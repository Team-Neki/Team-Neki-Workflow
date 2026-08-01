"""모노맨션 지점 수집 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.monomansion_stores import monomansion_stores


def build() -> RunnerDeployment:
    # 다른 브랜드 수집(04:00, 04:30, 05:00, 05:30)과 시간을 벌린다.
    return monomansion_stores.to_deployment(
        name="monomansion-stores",
        cron="0 6 * * *",
    )
