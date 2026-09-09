"""하루필름 지점 수집 deployment.

스케줄이 없다. 정기 수집은 stores-collect 가 묶어서 돌린다. 이 deployment 는
백필이나 단일 브랜드 재수집을 UI 에서 실행하기 위해 남겨둔다. 질의어가 아직
확정되지 않아 UI 에서 queries 를 바꿔가며 확인하는 데도 쓴다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.harufilm_stores import harufilm_stores


def build() -> RunnerDeployment:
    return harufilm_stores.to_deployment(name="harufilm-stores")
