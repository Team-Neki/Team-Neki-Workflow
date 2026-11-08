"""브랜드 소식 수집 수동 실행 설정. 수집 주기 확정 전에는 cron을 두지 않는다."""

from prefect.deployments.runner import RunnerDeployment

from neki_workflow.flows.events.collect.photosignature import photosignature_events


def build() -> RunnerDeployment:
    return photosignature_events.to_deployment(name="photosignature-events")
