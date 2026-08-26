"""지하철 역 정보 수집 deployment.

원본 갱신이 연 1회다(국가철도공단 다음 등록 예정 2026-12-20). 법정동처럼 매일 돌
이유가 없어 주 1회로 둔다. legal-dong 이 05:00 이므로 겹치지 않게 뒤로 둔다.

연 1회 갱신이라 신규 개통역이 최대 1년 늦게 들어온다. 개통 직후 반영이 필요해지면
그때 TAGO API 나 Kakao 카테고리 검색을 보조 수집원으로 얹는 것을 검토한다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.subway_station import subway_station


def build() -> RunnerDeployment:
    return subway_station.to_deployment(
        name="subway-station",
        cron="0 6 * * 1",
    )
