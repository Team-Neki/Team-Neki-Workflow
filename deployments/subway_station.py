"""지하철 역 정보 수집 deployment.

매월 1일 06:00 KST. 원본 갱신이 연 1회라(다음 등록 예정 2026-12-20) 더 자주 돌 이유가
없고, legal-dong 과 같은 날로 묶어 두면 마스터 둘의 기준일이 함께 움직인다. legal-dong 이
05:00 KST 이므로 뒤로 둔다. Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.subway_station import subway_station


def build() -> RunnerDeployment:
    return subway_station.to_deployment(
        name="subway-station",
        schedule=Cron("0 6 1 * *", timezone="Asia/Seoul"),
    )
