"""k3s Job으로 배치 컨테이너를 실행한다.

Prefect의 Kubernetes work pool은 **flow 자체**를 컨테이너로 돌리는 기능이다.
이미지 안에 Prefect와 flow 코드가 들어 있어야 하므로, 임의의 이미지(Spring
Batch jar)를 돌리려는 여기와는 용도가 다르다. 그래서 work pool 대신 flow가
Job을 만들어 지켜본다.

스케줄, 순서, 재시도는 Prefect에 남고 계산은 컨테이너가 한다. k3s는 containerd를
쓰므로 Docker 소켓이 없다. `docker run` 방식은 쓸 수 없다.
"""

import os
import time
from functools import lru_cache
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prefect import get_run_logger, task
from prefect.runtime import flow_run

from flows.common import registry

NAMESPACE_ENV = "K8S_NAMESPACE"
DEFAULT_NAMESPACE = "default"

POLL_SECONDS = 2.0

# 이 상태들은 기다려도 나아지지 않는다. 로그를 기다리며 타임아웃까지 멈춰 있으면
# 원인이 드러나지 않으므로 즉시 실패시킨다.
FATAL_WAITING = {
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
}


@lru_cache(maxsize=128)
def _resolve_once(run_id: str, image: str) -> str:
    """flow run 하나 안에서는 같은 이미지를 쓴다.

    run_id는 캐시 키로만 쓰인다. 이게 없으면 task를 재시도할 때마다 태그를 다시
    해석하게 되고, 그 사이 새 이미지가 올라왔다면 1차 시도와 2차 시도가 다른
    코드를 돌게 된다. 한 번의 실행은 하나의 이미지로 끝나야 한다.

    다음 flow run은 run_id가 다르므로 그때 다시 최신을 집는다.
    """
    return registry.resolve(image)


def _pin(image: str) -> str:
    run_id = flow_run.id
    # flow run 밖에서 부른 경우다. 캐시하면 프로세스가 사는 동안 낡은 값이
    # 남으므로 그때그때 해석한다.
    return _resolve_once(run_id, image) if run_id else registry.resolve(image)


def _load_config() -> None:
    """pod 안이면 ServiceAccount를, 호스트면 kubeconfig를 쓴다.

    worker를 어디에 두든 같은 코드로 동작해야 한다. k3s의 kubeconfig는
    `/etc/rancher/k3s/k3s.yaml`에 있으며 KUBECONFIG로 가리킨다.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def namespace() -> str:
    return os.environ.get(NAMESPACE_ENV) or DEFAULT_NAMESPACE


def _manifest(
    *,
    name: str,
    image: str,
    args: list[str] | None,
    env: dict[str, str] | None,
    secrets: list[str] | None,
    service_account: str | None,
    resources: dict[str, dict[str, str]] | None,
) -> client.V1Job:
    container = client.V1Container(
        name="batch",
        image=image,
        # 다이제스트는 불변이므로 캐시를 믿어도 된다. 태그를 그대로 쓰는
        # 경우에만 매번 받아야 최신이 보장된다.
        image_pull_policy="IfNotPresent" if "@sha256:" in image else "Always",
        args=args,
        env=[client.V1EnvVar(name=k, value=v) for k, v in (env or {}).items()],
        env_from=[
            client.V1EnvFromSource(secret_ref=client.V1SecretEnvSource(name=secret))
            for secret in (secrets or [])
        ],
        resources=client.V1ResourceRequirements(**resources) if resources else None,
    )

    return client.V1Job(
        metadata=client.V1ObjectMeta(
            # 이름을 직접 짓지 않는다. k8s가 붙이는 접미사로 매 실행 고유해지고
            # DNS-1123 제약도 알아서 지켜진다.
            generate_name=f"{name}-",
            labels={"app.kubernetes.io/managed-by": "prefect"},
        ),
        spec=client.V1JobSpec(
            # 재시도는 Prefect가 맡는다. k8s가 같이 재시도하면 의미가 겹치고
            # 어느 시도의 로그인지 분간이 안 된다.
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    service_account_name=service_account,
                    containers=[container],
                )
            ),
        ),
    )


def _await_pod(core: client.CoreV1Api, job_name: str, space: str, deadline: float) -> str:
    """로그를 읽을 수 있는 pod가 뜰 때까지 기다린다."""
    while time.monotonic() < deadline:
        pods = core.list_namespaced_pod(
            space, label_selector=f"job-name={job_name}"
        ).items

        for pod in pods:
            for status in pod.status.container_statuses or []:
                waiting = status.state.waiting if status.state else None
                if waiting and waiting.reason in FATAL_WAITING:
                    raise RuntimeError(
                        f"컨테이너를 시작하지 못했습니다: {waiting.reason} "
                        f"({waiting.message})"
                    )

            if pod.status.phase in ("Running", "Succeeded", "Failed"):
                return pod.metadata.name

        time.sleep(POLL_SECONDS)

    raise TimeoutError(f"{job_name}: pod가 뜨기를 기다리다 시간이 지났습니다.")


def _stream_logs(core: client.CoreV1Api, pod: str, space: str, logger: Any) -> None:
    """컨테이너 stdout을 Prefect 로그로 옮긴다.

    이게 없으면 Prefect UI에는 실패 사실만 남고 원인은 사라진 pod 안에 있다.
    """
    response = core.read_namespaced_pod_log(
        name=pod, namespace=space, follow=True, _preload_content=False
    )

    buffer = b""
    for chunk in response.stream():
        buffer += chunk
        *lines, buffer = buffer.split(b"\n")
        for line in lines:
            logger.info("[%s] %s", pod, line.decode("utf-8", "replace"))

    if buffer:
        logger.info("[%s] %s", pod, buffer.decode("utf-8", "replace"))


def _await_job(batch: client.BatchV1Api, job_name: str, space: str, deadline: float) -> bool:
    while time.monotonic() < deadline:
        status = batch.read_namespaced_job_status(job_name, space).status

        if status.succeeded:
            return True
        if status.failed:
            return False

        time.sleep(POLL_SECONDS)

    raise TimeoutError(f"{job_name}: 완료를 기다리다 시간이 지났습니다.")


@task
def run_job(
    *,
    name: str,
    image: str,
    args: list[str] | None = None,
    env: dict[str, str] | None = None,
    secrets: list[str] | None = None,
    service_account: str | None = None,
    resources: dict[str, dict[str, str]] | None = None,
    pin_digest: bool = True,
    timeout_seconds: float = 3600.0,
) -> str:
    """Job을 만들어 끝날 때까지 지켜본다. 실패하면 예외를 낸다.

    Job은 성공하든 실패하든 지운다. 로그는 이미 Prefect로 옮겨왔으므로 남겨둘
    이유가 없고, 남기면 다음 실행 때 이름만 다른 Job이 쌓인다.

    pin_digest를 켜두면 태그가 지금 가리키는 다이제스트를 조회해 그것으로
    실행한다. 최신을 쓰면서 무엇이 돌았는지가 이력에 남는다. 해석 결과는 flow
    run 단위로 재사용하므로 재시도해도 같은 이미지가 돈다. 레지스트리를 부를
    수 없는 환경에서만 끈다.
    """
    logger = get_run_logger()

    if pin_digest:
        resolved = _pin(image)
        if resolved != image:
            logger.info("이미지 고정: %s -> %s", image, resolved)
        image = resolved

    _load_config()
    batch = client.BatchV1Api()
    core = client.CoreV1Api()

    space = namespace()
    deadline = time.monotonic() + timeout_seconds

    job = batch.create_namespaced_job(
        space,
        _manifest(
            name=name,
            image=image,
            args=args,
            env=env,
            secrets=secrets,
            service_account=service_account,
            resources=resources,
        ),
    )
    job_name = job.metadata.name
    logger.info("Job %s 생성 (namespace=%s, image=%s)", job_name, space, image)

    try:
        pod = _await_pod(core, job_name, space, deadline)
        _stream_logs(core, pod, space, logger)

        if not _await_job(batch, job_name, space, deadline):
            raise RuntimeError(f"Job {job_name} 이 실패했습니다. 위 로그를 보세요.")

        logger.info("Job %s 완료", job_name)
        return job_name
    finally:
        # 취소나 예외로 빠져나갈 때도 pod가 남으면 안 된다. Background 전파를
        # 지정해야 Job만 지워지고 pod가 고아로 남는 일이 없다.
        try:
            batch.delete_namespaced_job(
                job_name, space, propagation_policy="Background"
            )
        except ApiException as error:
            logger.warning("Job %s 정리 실패: %s", job_name, error.reason)
