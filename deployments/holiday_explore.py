"""연휴·공휴일 탐색 발송 스케줄. 매일 09:00 KST.

매일 띄우고 발송일 판정은 서버가 한다(공휴일 CSV). 발송일이 아니면 0건으로 끝나므로 flow 는
대부분의 날 몇 초 만에 성공한다. Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다.

동시 실행은 concurrency_limit=1 로 막는다. 두 run 이 겹치면 같은 유저에게 두 번 보낼 수 있다.
서버의 중복 판정은 커밋된 이력만 보기 때문이다.

전환 뒤 Notification 앱이 내려가기 전까지는 UI 에서 pause 해 둔다. deploy.py 가 pause 를 보존한다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.holiday_explore import holiday_explore


def build() -> RunnerDeployment:
    return holiday_explore.to_deployment(
        name="holiday-explore",
        schedule=Cron("0 9 * * *", timezone="Asia/Seoul"),
        concurrency_limit=1,
    )
