"""플랜비스튜디오 지점 수집 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.planbstudio_stores import planbstudio_stores


def build() -> RunnerDeployment:
    # 다른 브랜드 수집(04:00, 04:30)과 시간을 벌린다.
    return planbstudio_stores.to_deployment(
        name="planbstudio-stores",
        cron="0 5 * * *",
    )
