"""서버의 주말 전 탐색 잡(weekendExploreJob)을 k8s Job 으로 띄운다.

이 flow 가 하는 일은 Job 을 만들고 끝나기를 기다리는 것뿐이다. 누구에게 무엇을 보낼지는
Team-Neki-Server apps/batch 에 있다(BACKEND-135). 같은 businessDate 로 다시 돌려도 이미 보낸
유저는 서버가 걸러 중복 발송이 없다.
"""

from datetime import date
from typing import Any

from prefect import flow, get_run_logger

from flows.common.manifest import target_date as cycle_date
from flows.common.storage import run_at
from flows.weekend_explore.job import run_weekend_explore


@flow(name="weekend-explore", log_prints=True)
def weekend_explore(target_date: date | None = None) -> dict[str, Any]:
    """발송 Job 을 띄우고 완료를 기다린다. Job 이 실패하면 flow 도 실패한다.

    target_date 는 batch 에 넘기는 businessDate 다. 비우면 이 run 의 예약 시각(KST)이다.
    """
    logger = get_run_logger()

    cycle = target_date or cycle_date()
    launched = run_weekend_explore(cycle, run_at=run_at())
    if launched:
        logger.info("발송 완료 (businessDate=%s)", cycle)

    return {"cycle": f"{cycle:%Y-%m-%d}", "launched": launched}
