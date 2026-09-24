"""브랜드 통합 수집 스케줄 정의.

수집 스케줄은 여기 하나뿐이다. 브랜드별 deployment 는 스케줄 없이 남아 있어
백필과 단일 재수집에 쓴다.

매일 04:00 KST 에 돈다. **Prefect cron 은 timezone 을 주지 않으면 UTC 라
명시한다.** 빼면 04:00 UTC, 곧 13:00 KST 에 돌아 한낮에 사이트를 긁는다.

파티션 날짜(`dt=`)와 대상 일자(`target_date`)가 둘 다 KST 라 04:00 KST 가
19:00 UTC(전날)여도 날짜가 밀리지 않는다. 날짜를 UTC 로 끊었다면 새벽 실행이
전날 파티션에 들어갔을 자리다.

legal-dong(05:00 KST)과 subway-station(06:00 KST)은 매월 1일이라 같은 날 겹치는
날이 있지만 한 시간씩 벌어져 있다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.stores_collect import stores_collect


def build() -> RunnerDeployment:
    return stores_collect.to_deployment(
        name="stores-collect",
        schedule=Cron("0 4 * * *", timezone="Asia/Seoul"),
    )
