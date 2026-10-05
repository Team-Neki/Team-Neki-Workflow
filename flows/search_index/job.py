"""서버의 검색 색인 잡을 k8s Job 으로 띄운다. 띄우고 기다리는 것은 flows/common/batch_job 이 한다.

색인(BACKEND-65)은 Python 이 아니라 Team-Neki-Server apps/batch 의 Spring Batch
잡(searchIndexJob)이다. 정규화 규칙이 색인과 검색 질의에서 같은 Kotlin 함수여야
하기 때문이다.

enrich flow 안에서 부르지 않는다. 묶으면 enrich 재시도가 색인을 되풀이하고 색인
실패가 enrich 를 실패로 만든다. collect 와 enrich 처럼 별도 deployment 로 두고
시각으로 순서를 맞춘다. 색인은 그 시점의 tb_photo_booth_enriched 현재 세대를
읽는다.
"""

from datetime import date

from prefect import task

from flows.common.batch_job import run_batch_job

JOB_NAME = "searchIndexJob"

# 색인은 수천 건이라 초 단위다. 그래도 파드가 안 뜨는 경우(이미지 없음 등)에
# 무한정 기다리지 않게 상한을 둔다.
TIMEOUT_SECONDS = 1800


@task
def run_search_index(cycle: date, *, run_at: str) -> bool:
    """색인 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False."""
    return run_batch_job(
        "search-index", JOB_NAME, cycle, run_at=run_at, timeout_seconds=TIMEOUT_SECONDS
    )
