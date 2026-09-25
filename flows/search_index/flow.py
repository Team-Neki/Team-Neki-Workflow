"""서버의 검색 색인 잡(searchIndexJob)을 k8s Job 으로 띄운다.

enrich 와 별도 flow 다. enrich 안에서 색인을 부르면 enrich 재시도가 색인을
되풀이하고 색인 실패가 enrich 를 실패로 만든다. collect 와 enrich 를 나눈 것과
같은 이유로 나누고, 순서는 시각으로 맞춘다. 색인은 그 시점의
tb_photo_booth_enriched 현재 세대를 읽는다. enrich 가 늦으면 그날은 이전 세대를
색인하고, 색인은 멱등이라 다시 돌리면 된다.

이 flow 가 하는 일은 Job 을 만들고 끝나기를 기다리는 것뿐이다. 색인 규칙은
Team-Neki-Server apps/batch 에 있다(BACKEND-65). 이미지와 파드 권한은 GitOps 가
준다(BACKEND-143). NEKI_BATCH_IMAGE 가 없으면 경고만 남기고 끝난다.
"""

from datetime import date
from typing import Any

from prefect import flow, get_run_logger

from flows.common.manifest import target_date as cycle_date
from flows.common.storage import run_at
from flows.search_index.job import run_search_index


@flow(name="search-index", log_prints=True)
def search_index(target_date: date | None = None) -> dict[str, Any]:
    """색인 Job 을 띄우고 완료를 기다린다. Job 이 실패하면 flow 도 실패한다.

    target_date 는 batch 에 넘기는 businessDate 다. 비우면 이 run 의 예약
    시각(KST)이다. 같은 값으로 다시 돌려도 결과가 같다.
    """
    logger = get_run_logger()

    cycle = target_date or cycle_date()
    launched = run_search_index(cycle, run_at=run_at())
    if launched:
        logger.info("색인 완료 (businessDate=%s)", cycle)

    return {"cycle": f"{cycle:%Y-%m-%d}", "launched": launched}
