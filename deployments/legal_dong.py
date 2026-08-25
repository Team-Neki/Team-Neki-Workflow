"""법정동 코드 수집 deployment.

stores-collect 가 04:00 이므로 겹치지 않게 뒤로 둔다. 개편은 대개 월초에
시행되어 매일 도는 것이 과해 보이지만, 다운로드가 1MB 한 번이라 비용이 없고
반영 지연 상한이 하루로 고정된다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.legal_dong import legal_dong


def build() -> RunnerDeployment:
    return legal_dong.to_deployment(
        name="legal-dong",
        cron="0 5 * * *",
    )
