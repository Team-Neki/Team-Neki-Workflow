"""지하철 역 정보 수집 deployment.

매월 1일에 돈다. 원본 갱신이 연 1회라(국가철도공단 다음 등록 예정 2026-12-20)
더 자주 돌 이유가 없고, 법정동과 같은 날 묶어 두면 마스터 둘의 기준일이 함께
움직여 어긋난 조합을 따질 일이 없다. legal-dong 이 05:00 이므로 뒤로 둔다.

**반영 지연 상한이 한 달이다.** 원본이 연 1회 갱신이라 신규 개통역은 어차피 최대
1년 늦게 들어오므로 이 한 달이 병목은 아니다. 개통 직후 반영이 필요해지면 그때
TAGO API 나 Kakao 카테고리 검색을 보조 수집원으로 얹는 것을 검토한다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.subway_station import subway_station


def build() -> RunnerDeployment:
    return subway_station.to_deployment(
        name="subway-station",
        cron="0 6 1 * *",
    )
