"""픽닷 지점 수집 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.picdot_stores import picdot_stores


def build() -> RunnerDeployment:
    # 다른 브랜드 수집(04:00, 04:30, 05:00)과 시간을 벌린다.
    return picdot_stores.to_deployment(
        name="picdot-stores",
        cron="30 5 * * *",
    )
