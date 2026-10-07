"""서버 batch 잡(Team-Neki-Server apps/batch)을 k8s Job 으로 띄우고 끝나기를 기다린다.

search-index 와 알림 발송 3종(weekly-reminder, weekend-explore, holiday-explore)이 같이 쓴다.
띄우고 기다리는 것만 책임지고 잡의 규칙은 서버에 있다.

one-shot 계약(BACKEND-128): --spring.batch.job.name=<잡> businessDate=<사이클> 로 기동해 잡
하나를 돌리고 종료 코드로 성패를 알린다. 같은 businessDate 로 다시 돌려도 결과가 같다(멱등).
재시도는 처음부터 다시 돈다.

이미지는 GitOps overlays/prefect/images.env 의 NEKI_BATCH_IMAGE 를 flow run 파드 환경변수로
받는다(BACKEND-143). 서버 레포의 deploy-batch 가 그 줄을 갱신한다. 없으면 로컬이거나 GitOps 가
아직이므로 경고만 남기고 건너뛴다.

Job 은 flow run 과 같은 네임스페이스에 뜬다. flow run 파드의 SA prefect-worker 가 jobs 생성과
pods/log 조회를 허용한다. 실패한 Job 은 지우지 않는다. 파드 로그가 원인이고
ttlSecondsAfterFinished 가 하루 뒤 치운다.

알림 발송 잡은 Firebase 서비스계정 JSON 이 필요하다. firebase=True 면 Secret prefect-workflow 의
firebase-service-account.json 키를 /etc/firebase 에 마운트한다. 서버의 application-firebase.yaml
이 staging/prod 프로파일에서 그 경로를 읽는다. 키가 Secret 에 없으면 파드가
CreateContainerConfigError 로 뜨지 않아 타임아웃까지 기다린 뒤 flow 가 실패한다.
"""

import asyncio
import os
import re
from datetime import date
from typing import Any
from uuid import uuid4

from prefect import get_run_logger
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import KubernetesJob, KubernetesJobRun

IMAGE_ENV = "NEKI_BATCH_IMAGE"

NAMESPACE = "prefect"

# 배치가 앱 DB 에 붙을 때 쓰는 값이 있는 Secret. flow run 파드가 envFrom 으로 받는 것과 같은
# Secret 이다. 통째로 넘기지 않고 필요한 것만 꺼낸다. batch 파드에 Kakao 와 AWS 키까지 넘길 이유가 없다.
SECRET = "prefect-workflow"

FIREBASE_SECRET_KEY = "firebase-service-account.json"
FIREBASE_MOUNT_PATH = "/etc/firebase"

# 완료된 Job 을 k8s 가 치우는 시간. 실패 파드의 로그를 볼 여유다.
FINISHED_JOB_TTL = 86400

# k8s 이름 상한. prefix 가 길면 UUID 뒤쪽이 잘린다. 28자 이상 남으면 충돌 걱정이 없다.
MAX_NAME_LENGTH = 63


def manifest(
    prefix: str,
    job_name: str,
    image: str,
    cycle: date,
    *,
    run_at: str,
    timeout_seconds: int,
    firebase: bool = False,
) -> dict[str, Any]:
    """Job 매니페스트.

    이름이 유일해야 한다. prefect-kubernetes 가 metadata.name 으로 상태를 읽으므로
    generateName 은 못 쓴다. 실행 시각(YYYY-MM-DD_HHMMSS)의 밑줄은 k8s 이름에
    허용되지 않아 하이픈으로 바꾼다.
    """
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}_\d{6}", run_at) is None:
        raise ValueError("run_at은 YYYY-MM-DD_HHMMSS 형식이어야 합니다.")
    # 초 단위 시각이 같아도 실행마다 이름이 다르다.
    name = f"{prefix}-{run_at.replace('_', '-')}-{uuid4().hex}"[:MAX_NAME_LENGTH]
    container: dict[str, Any] = {
        "name": "neki-batch",
        "image": image,
        "args": [
            f"--spring.batch.job.name={job_name}",
            f"businessDate={cycle:%Y-%m-%d}",
        ],
        "env": [
            {"name": "TZ", "value": "Asia/Seoul"},
            {
                "name": "SPRING_PROFILES_ACTIVE",
                "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "SPRING_PROFILES_ACTIVE"}},
            },
            {
                "name": "JASYPT_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "JASYPT_PASSWORD"}},
            },
        ],
    }
    pod: dict[str, Any] = {"restartPolicy": "Never", "containers": [container]}
    if firebase:
        container["volumeMounts"] = [
            {"name": "firebase-credentials", "mountPath": FIREBASE_MOUNT_PATH, "readOnly": True}
        ]
        pod["volumes"] = [
            {
                "name": "firebase-credentials",
                "secret": {
                    "secretName": SECRET,
                    "items": [{"key": FIREBASE_SECRET_KEY, "path": FIREBASE_SECRET_KEY}],
                },
            }
        ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "backoffLimit": 0,
            # flow 가 타임아웃으로 끝나도 Job 은 남는다. 그 사이 flow 를 다시 돌리면
            # concurrency_limit 슬롯은 비어 있어 두 Job 이 겹친다. 같은 상한에서 k8s 가 파드를 끝낸다.
            "activeDeadlineSeconds": timeout_seconds,
            "ttlSecondsAfterFinished": FINISHED_JOB_TTL,
            "template": {"spec": pod},
        },
    }


async def wait_for_completion(run: KubernetesJobRun, timeout_seconds: int) -> None:
    """Pod 생성 전과 로그 읽기를 포함한 대기 전체를 실제 경과 시간으로 제한한다.

    prefect-kubernetes 0.7.12 의 내부 타이머는 active Pod 가 없으면 늘지 않는다.
    """
    async with asyncio.timeout(timeout_seconds):
        await run.await_for_completion(print_func=print)


def run_batch_job(
    prefix: str,
    job_name: str,
    cycle: date,
    *,
    run_at: str,
    timeout_seconds: int,
    firebase: bool = False,
) -> bool:
    """Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False.

    Job 실패는 RuntimeError, 대기 시간 초과는 TimeoutError 로 flow 가 실패한다.
    그것이 계약이다. 재시도를 붙이지 않는다. 잡은 멱등이라 flow 를 다시 돌리면 된다.
    각 flow 의 @task 가 이 함수를 감싼다. task 이름이 UI 에 보이도록 flow 마다 둔다.
    """
    logger = get_run_logger()

    image = os.environ.get(IMAGE_ENV)
    if not image:
        logger.warning(
            "%s 가 없어 %s Job 을 띄우지 않습니다. 로컬이거나 GitOps 의 neki-images "
            "ConfigMap 이 아직 없는 것입니다.",
            IMAGE_ENV,
            job_name,
        )
        return False

    job = KubernetesJob(
        v1_job=manifest(
            prefix,
            job_name,
            image,
            cycle,
            run_at=run_at,
            timeout_seconds=timeout_seconds,
            firebase=firebase,
        ),
        credentials=KubernetesCredentials(),
        namespace=NAMESPACE,
        timeout_seconds=timeout_seconds,
        delete_after_completion=False,
    )
    name = job.v1_job["metadata"]["name"]
    logger.info("%s Job 시작: %s (%s, businessDate=%s)", job_name, name, image, cycle)

    run = job.trigger()
    # 파드 로그를 줄 단위로 print 해 flow 로그(log_prints)에 남긴다.
    asyncio.run(wait_for_completion(run, timeout_seconds))

    logger.info("%s Job 완료: %s", job_name, name)
    return True
