"""지점 법정동 보강 deployment.

매일 05:00 KST 에 돈다. stores-collect(04:00 KST)가 그 시점까지 남긴 적재물만
본다. 늦게 끝난 collect 는 그날 enrich 에 안 들어가고 전날 것으로 대신하며,
결과 테이블의 source_dt 가 그것을 드러낸다. collect 가 끝나야 하는 시각(SLO)은
아직 정하지 않았다. 정해지면 이 cron 을 그 뒤로 둔다.

Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다.

매월 1일 legal-dong(05:00 KST)과 겹친다. enrich 는 tb_legal_dong 을 대조용으로
SELECT 만 하고 그쪽 스왑은 원자적이라 해가 없다.

색인(search-index)은 여기 묶지 않고 별도 deployment 가 시각으로 뒤에 돈다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.stores_enrich import stores_enrich


def build() -> RunnerDeployment:
    return stores_enrich.to_deployment(
        name="stores-enrich",
        schedule=Cron("0 5 * * *", timezone="Asia/Seoul"),
    )
