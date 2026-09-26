"""지점 동기화 deployment. 서버/조회 전환 검증 전까지 수동 실행만 등록한다."""

from prefect.deployments.runner import RunnerDeployment

from flows.stores_sync import stores_sync


def build() -> RunnerDeployment:
    return stores_sync.to_deployment(name="stores-sync", concurrency_limit=1)
