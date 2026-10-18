"""패키지 경로를 옮겨도 등록된 flow가 실제 이미지 경로에서 열리는지 확인한다."""

from prefect.flows import load_flow_from_entrypoint

from neki_workflow.deployments import collect


def test_deployment_entrypoints_load() -> None:
    deployments = list(collect())
    assert len({(item.flow_name, item.name) for item in deployments}) == len(deployments)
    for item in deployments:
        assert load_flow_from_entrypoint(item.entrypoint).name == item.flow_name
