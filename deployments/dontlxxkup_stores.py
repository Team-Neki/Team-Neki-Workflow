"""돈룩업 지점 수집 deployment.

스케줄이 없다. 정기 수집은 stores-collect 가 묶어서 돌린다. 이 deployment 는
백필이나 단일 브랜드 재수집을 UI 에서 실행하기 위해 남겨둔다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.dontlxxkup_stores import dontlxxkup_stores


def build() -> RunnerDeployment:
    return dontlxxkup_stores.to_deployment(name="dontlxxkup-stores")
