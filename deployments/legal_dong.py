"""법정동 코드 수집 deployment.

매월 1일에 돈다. stores-collect 가 04:00 이므로 겹치지 않게 뒤로 둔다.

**개편은 대개 1일에 시행되는데 원본이 그날 바로 갱신되지는 않는다.** 시행일 새벽에
받으면 아직 옛 스냅샷일 수 있고, 그러면 그 개편은 다음 달 1일에야 들어온다.
반영 지연 상한이 한 달이라는 뜻이다.

그 한 달을 감수하는 이유는 조인이 코드로 이루어지기 때문이다. 이름은 개편으로
움직이지만 코드로 조인하면 안전하고, Kakao 가 돌려준 서로 다른 법정동코드 701개가
전부 마스터에 존재하며 폐지된 것이 없음을 확인했다. 새로 생긴 코드가 마스터에
없는 동안에만 지연이 드러난다.

더 빨리 반영해야 할 일이 생기면 cron 을 당기는 것으로 끝난다. 다운로드가 1.4MB
한 번이라 비용이 없다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.legal_dong import legal_dong


def build() -> RunnerDeployment:
    return legal_dong.to_deployment(
        name="legal-dong",
        cron="0 5 1 * *",
    )
