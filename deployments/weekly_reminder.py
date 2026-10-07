"""아카이빙 1주일 리마인드 발송 스케줄. 매일 20:00 KST.

Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다. businessDate 는 run 예약 시각의
KST 날짜라 20:00 KST 가 11:00 UTC 여도 날짜가 밀리지 않는다.

동시 실행은 concurrency_limit=1 로 막는다. 두 run 이 겹치면 같은 유저에게 두 번 보낼 수 있다.
서버의 중복 판정은 커밋된 이력만 보기 때문이다.

전환 뒤 Notification 앱이 내려가기 전까지는 UI 에서 pause 해 둔다. deploy.py 가 pause 를 보존한다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.weekly_reminder import weekly_reminder


def build() -> RunnerDeployment:
    return weekly_reminder.to_deployment(
        name="weekly-reminder",
        schedule=Cron("0 20 * * *", timezone="Asia/Seoul"),
        concurrency_limit=1,
    )
