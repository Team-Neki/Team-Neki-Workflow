"""주말 전 탐색 발송 스케줄. 금·토·일 18:00 KST.

Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다. 요일도 KST 기준이라야 금요일 저녁에 간다.

동시 실행은 concurrency_limit=1 로 막는다. 두 run 이 겹치면 같은 유저에게 두 번 보낼 수 있다.
서버의 중복 판정은 커밋된 이력만 보기 때문이다.

전환 뒤 Notification 앱이 내려가기 전까지는 UI 에서 pause 해 둔다. deploy.py 가 pause 를 보존한다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.weekend_explore import weekend_explore


def build() -> RunnerDeployment:
    return weekend_explore.to_deployment(
        name="weekend-explore",
        schedule=Cron("0 18 * * 5,6,0", timezone="Asia/Seoul"),
        concurrency_limit=1,
    )
