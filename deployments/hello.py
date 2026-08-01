"""hello 플로 스케줄 정의."""

from prefect.deployments.runner import RunnerDeployment

from flows.hello import hello


def build() -> RunnerDeployment:
    # 동작 확인용이라 스케줄 없이 수동 실행만 받는다.
    return hello.to_deployment(name="hello-local")
