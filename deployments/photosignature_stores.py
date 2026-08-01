"""포토시그니처 지점 수집 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.photosignature_stores import photosignature_stores


def build() -> RunnerDeployment:
    # 인생네컷 수집(04:00)과 시간을 벌려 대상 사이트에 부하가 겹치지 않게 한다.
    return photosignature_stores.to_deployment(
        name="photosignature-stores",
        cron="30 4 * * *",
    )
