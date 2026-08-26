"""원문의 이름 표기를 고른다.

collect 와 enrich 를 가르는 기준을 그대로 쓴다. `source` 는 사이트가 준 것만 담고,
여기서부터는 우리 규칙이다. 그래서 표기 교정이 다운로드에 섞여 들지 않고, 규칙을
고칠 때 사이트를 다시 긁지 않아도 된다.

**행을 접지 않는다.** 원문 1,099행은 역이 아니라 (역, 노선)이고 그것이 곧 사용자가
고르는 단위다. 접으면 `강남역` 한 줄이 되어 2호선인지 신분당선인지 고를 수 없고,
노선마다 다른 좌표가 사라져 1km 반경 결과도 같아진다. 완전 중복 한 행만 뺀다.

고칠 것이 둘이다.

**하나, 역명에 `역` 이 붙은 것과 안 붙은 것이 섞여 있다.** 1,099행 중 358행에만
붙어 있어 그대로 두면 "강남" 검색과 "강남역" 검색의 결과가 달라진다. 뗀 쪽으로
맞추고 표시할 때 붙이는 것은 앱에 맡긴다.

**둘, 같은 노선이 다른 이름으로 온다.** 검색 결과에 그대로 보이는 값이라
`도시철도 7호선` 이나 공백이 둘 들어간 `수도권  도시철도 9호선` 이 사용자에게
노출된다.
"""

import re
from dataclasses import dataclass

from prefect import get_run_logger, task

from flows.subway_station.source import SourceStation

# 같은 노선을 가리키는 다른 표기. 공백을 고른 뒤에 본다.
#
# **노선번호로 다수결을 내지 않는다.** `I4108` 하나에 경의중앙선 51행과 경춘선 3행이
# 매달려 있는데 그 둘은 실제로 다른 노선이라, 다수결이면 경춘선 3행이 경의중앙선으로
# 둔갑한다. `I4101` 의 1호선 10행과 경부선 37행도 마찬가지다. 노선명은 47종뿐이라
# 눈으로 확인할 수 있으므로 합칠 것만 적어 둔다.
#
# 지역 접두는 떼지 않는다. `대구 도시철도 1호선` 에서 `대구` 를 떼면 서울 1호선과
# 같아진다.
LINE_ALIAS = {
    "도시철도 7호선": "7호선",
    "서울 도시철도 9호선": "9호선",
    "수도권 도시철도 9호선": "9호선",
    "수도권 광역철도 8호선": "8호선",
    "의정부": "의정부경전철",
}


@dataclass(frozen=True)
class Station:
    """적재할 (역, 노선) 하나.

    사용자는 "강남"을 검색해 `강남 2호선` 과 `강남 신분당선` 중 하나를 고르고, 고른
    행의 좌표로 1km 안의 부스를 받는다. 그래서 `name` 과 `line_name` 이 검색 결과 한
    줄을 이루고 `lat`/`lon` 이 그다음 질의의 입력이 된다. 담는 것은 그 넷과 원문
    식별자 둘이 전부다.
    """

    name: str
    line_name: str
    line_no: str | None
    station_no: str | None
    lat: float
    lon: float


def clean_name(value: str) -> str:
    """역명에서 공백과 끝의 `역` 을 뗀다.

    `개운포역` 은 `개운포` 가 되고 `신사` 는 그대로다. 끝 글자만 보므로 `역곡` 이나
    `역삼` 처럼 `역` 으로 시작하는 이름은 건드리지 않는다.

    한 글자 이름은 떼지 않는다. 전량에 그런 역은 없지만, 떼면 빈 문자열이 되어
    검색에서 사라지는 쪽이 남는 것보다 나쁘다.
    """
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) > 1 and text.endswith("역"):
        return text[:-1].strip()
    return text


def clean_line(value: str) -> str:
    """노선명의 공백을 고르고 같은 노선의 다른 표기를 합친다."""
    text = re.sub(r"\s+", " ", value).strip()
    return LINE_ALIAS.get(text, text)


def _blank_to_none(value: str) -> str | None:
    """빈 문자열을 NULL 로 맞춘다.

    `''` 가 그대로 담기면 다음 단계가 값이 있는 줄로 오해한다.
    """
    return value or None


@task
def normalize(rows: list[SourceStation]) -> list[Station]:
    """원문 행을 적재할 모양으로 고친다. 완전 중복만 빼고 행 수는 그대로다."""
    logger = get_run_logger()

    stations: list[Station] = []
    seen: set[tuple] = set()

    renamed = 0
    merged_lines = 0
    duplicated = 0

    for row in rows:
        name = clean_name(row.name)
        line_name = clean_line(row.line_name)

        if name != row.name:
            renamed += 1
        if line_name != row.line_name:
            merged_lines += 1

        station = Station(
            name=name,
            line_name=line_name,
            line_no=_blank_to_none(row.line_no),
            station_no=_blank_to_none(row.station_no),
            lat=row.lat,
            lon=row.lon,
        )

        # 원문에 모든 열이 같은 행이 하나 있다(경인선 주안역). 접는 것이 아니라
        # 같은 것을 두 번 담지 않는 것뿐이므로, 값이 하나라도 다르면 남는다.
        # 광운대의 경의중앙선과 경춘선은 좌표가 달라 둘 다 남는다.
        key = (
            station.name,
            station.line_name,
            station.line_no,
            station.station_no,
            station.lat,
            station.lon,
        )
        if key in seen:
            duplicated += 1
            continue
        seen.add(key)

        stations.append(station)

    logger.info(
        "역명 접미사 제거 %d행, 노선명 표기 통일 %d행, 완전 중복 제거 %d행",
        renamed,
        merged_lines,
        duplicated,
    )

    logger.info("정규화 %d행", len(stations))
    return stations
