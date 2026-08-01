"""인생네컷 지점 수집 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.lifefourcuts_stores import lifefourcuts_stores



def build() -> RunnerDeployment:
    # 지점 정보는 자주 바뀌지 않으므로 하루 한 번 새벽에 돌린다.
    return lifefourcuts_stores.to_deployment(
        name="lifefourcuts-stores",
        cron="0 4 * * *",
    )
