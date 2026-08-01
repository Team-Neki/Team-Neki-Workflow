"""브랜드 통합 수집 스케줄 정의.

수집 스케줄은 여기 하나뿐이다. 브랜드별 deployment 는 스케줄 없이 남아 있어
백필과 단일 재수집에 쓴다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.stores_collect import stores_collect


def build() -> RunnerDeployment:
    return stores_collect.to_deployment(
        name="stores-collect",
        cron="0 4 * * *",
    )
