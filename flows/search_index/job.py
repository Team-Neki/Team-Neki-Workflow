"""서버의 검색 색인 잡을 k8s Job 으로 띄우고 끝나기를 기다린다.

색인(BACKEND-65)은 Python 이 아니라 Team-Neki-Server apps/batch 의 Spring Batch
잡(searchIndexJob)이다. 정규화 규칙이 색인과 검색 질의에서 같은 Kotlin 함수여야
하기 때문이다. 이 모듈은 띄우고 기다리는 것만 책임진다. 그 성패가 flow 결과에
보여야 한다.

enrich flow 안에서 부르지 않는다. 묶으면 enrich 재시도가 색인을 되풀이하고 색인
실패가 enrich 를 실패로 만든다. collect 와 enrich 처럼 별도 deployment 로 두고
시각으로 순서를 맞춘다. 색인은 그 시점의 tb_photo_booth_enriched 현재 세대를
읽는다.

one-shot 계약(BACKEND-128): 인자 --spring.batch.job.name=searchIndexJob 과
businessDate=<사이클> 로 기동해 잡 하나를 돌리고 종료 코드로 성패를 알린다.
같은 businessDate 로 다시 돌려도 결과가 같다(멱등). 재시도는 처음부터 다시 돈다.

이미지는 GitOps overlays/prefect/images.env 의 NEKI_BATCH_IMAGE 를 flow run 파드
환경변수로 받는다(BACKEND-143). 서버 레포의 deploy-batch 가 그 줄을 갱신한다.
없으면 로컬이거나 GitOps 가 아직이므로 경고만 남기고 건너뛴다.

Job 은 flow run 과 같은 네임스페이스에 뜬다. flow run 파드의 SA prefect-worker
가 jobs 생성과 pods/log 조회를 허용한다. 실패한 Job 은 지우지 않는다. 파드
로그가 원인이고 ttlSecondsAfterFinished 가 하루 뒤 치운다.
"""

import os
from datetime import date
from typing import Any

from prefect import get_run_logger, task
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import KubernetesJob

IMAGE_ENV = "NEKI_BATCH_IMAGE"

NAMESPACE = "prefect"

# 배치가 앱 DB 에 붙을 때 쓰는 값이 있는 Secret. flow run 파드가 envFrom 으로
# 받는 것과 같은 Secret 이다. 통째로 넘기지 않고 필요한 둘만 꺼낸다. batch 파드에
# Kakao 와 AWS 키까지 넘길 이유가 없다.
SECRET = "prefect-workflow"

JOB_NAME = "searchIndexJob"

# 색인은 수천 건이라 초 단위다. 그래도 파드가 안 뜨는 경우(이미지 없음 등)에
# 무한정 기다리지 않게 상한을 둔다.
TIMEOUT_SECONDS = 1800

# 완료된 Job 을 k8s 가 치우는 시간. 실패 파드의 로그를 볼 여유다.
FINISHED_JOB_TTL = 86400


def manifest(image: str, cycle: date, *, run_at: str) -> dict[str, Any]:
    """Job 매니페스트.

    이름이 유일해야 한다. prefect-kubernetes 가 metadata.name 으로 상태를 읽으므로
    generateName 은 못 쓴다. 실행 시각(YYYY-MM-DD_HHMMSS)의 밑줄은 k8s 이름에
    허용되지 않아 하이픈으로 바꾼다.
    """
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"search-index-{run_at.replace('_', '-')}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": FINISHED_JOB_TTL,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "neki-batch",
                            "image": image,
                            "args": [
                                f"--spring.batch.job.name={JOB_NAME}",
                                f"businessDate={cycle:%Y-%m-%d}",
                            ],
                            "env": [
                                {"name": "TZ", "value": "Asia/Seoul"},
                                {
                                    "name": "SPRING_PROFILES_ACTIVE",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": SECRET,
                                            "key": "SPRING_PROFILES_ACTIVE",
                                        }
                                    },
                                },
                                {
                                    "name": "JASYPT_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": SECRET,
                                            "key": "JASYPT_PASSWORD",
                                        }
                                    },
                                },
                            ],
                        }
                    ],
                }
            },
        },
    }


@task
def run_search_index(cycle: date, *, run_at: str) -> bool:
    """색인 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False.

    Job 이 실패하면 wait_for_completion 이 RuntimeError 를 올려 flow 가 실패한다.
    그것이 계약이다. 재시도를 붙이지 않는다. 색인은 멱등이라 flow 를 다시 돌리면
    된다.
    """
    logger = get_run_logger()

    image = os.environ.get(IMAGE_ENV)
    if not image:
        logger.warning(
            "%s 가 없어 색인 Job 을 띄우지 않습니다. 로컬이거나 GitOps 의 neki-images "
            "ConfigMap 이 아직 없는 것입니다.",
            IMAGE_ENV,
        )
        return False

    job = KubernetesJob(
        v1_job=manifest(image, cycle, run_at=run_at),
        credentials=KubernetesCredentials(),
        namespace=NAMESPACE,
        timeout_seconds=TIMEOUT_SECONDS,
        delete_after_completion=False,
    )
    name = job.v1_job["metadata"]["name"]
    logger.info("색인 Job 시작: %s (%s, businessDate=%s)", name, image, cycle)

    run = job.trigger()
    # 파드 로그를 줄 단위로 print 해 flow 로그(log_prints)에 남긴다.
    run.wait_for_completion(print_func=print)

    logger.info("색인 Job 완료: %s", name)
    return True
