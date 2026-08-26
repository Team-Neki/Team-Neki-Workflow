"""원문의 이름 표기를 고른다.

`source` 는 사이트가 준 것만 담고 여기서부터가 우리 규칙이다. 그래야 규칙을 고칠 때
사이트를 다시 긁지 않는다.

**같은 역의 여러 노선을 한 행으로 합치지 않는다.** 원문 한 행은 (역, 노선)이고
그것이 사용자가 고르는 단위다. 강남역을 한 행으로 만들면 2호선인지 신분당선인지
고를 수 없고, 노선마다 다른 좌표가 하나로 뭉개져 1km 반경 결과도 같아진다.
"""

import re
from collections import defaultdict
from dataclasses import dataclass

from prefect import get_run_logger, task

from flows.subway_station.source import SourceStation

# 같은 노선의 다른 표기. 검색 결과에 그대로 보이는 값이라 골라야 한다.
#
# **노선번호로 다수결을 내지 않는다.** `I4108` 에 경의중앙선 51행과 경춘선 3행이
# 매달려 있는데 실제로 다른 노선이라, 다수결이면 경춘선이 둔갑한다. 지역 접두도 떼지
# 않는다. `대구 도시철도 1호선` 에서 `대구` 를 떼면 서울 1호선과 같아진다.
LINE_ALIAS = {
    "도시철도 7호선": "7호선",
    "서울 도시철도 9호선": "9호선",
    "수도권 도시철도 9호선": "9호선",
    "수도권 광역철도 8호선": "8호선",
    "의정부": "의정부경전철",
}


@dataclass(frozen=True)
class Station:
    """적재할 (역, 노선) 하나. `(name, line_name)` 이 키다."""

    name: str
    line_name: str
    lat: float
    lon: float


def clean_name(value: str) -> str:
    """끝의 `역` 을 뗀다. 원문이 1,099행 중 358행에만 붙여 놔서 그대로 두면
    "강남" 검색과 "강남역" 검색의 결과가 달라진다.

    끝 글자만 보므로 `역곡`, `역삼` 은 건드리지 않는다. 한 글자 이름은 빈 문자열이
    되어 검색에서 사라지므로 떼지 않는다.
    """
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) > 1 and text.endswith("역"):
        return text[:-1].strip()
    return text


def clean_line(value: str) -> str:
    """공백을 고르고 같은 노선의 다른 표기를 합친다."""
    text = re.sub(r"\s+", " ", value).strip()
    return LINE_ALIAS.get(text, text)


@task
def normalize(rows: list[SourceStation]) -> list[Station]:
    """원문 행을 적재할 모양으로 고친다. 키가 겹치는 행만 뺀다."""
    logger = get_run_logger()

    groups: dict[tuple[str, str], list[Station]] = defaultdict(list)
    renamed = 0
    merged_lines = 0
    keyless = 0

    for row in rows:
        name = clean_name(row.name)
        line_name = clean_line(row.line_name)

        if not name or not line_name:
            keyless += 1
            continue

        renamed += name != row.name
        merged_lines += line_name != row.line_name

        groups[(name, line_name)].append(
            Station(name=name, line_name=line_name, lat=row.lat, lon=row.lon)
        )

    stations: list[Station] = []
    collapsed: list[str] = []

    for (name, line_name), candidates in groups.items():
        if len(candidates) > 1:
            # 원문 순서에 기대지 않는다. 첫 행을 집으면 원문 순서가 바뀔 때 남는
            # 행이 조용히 달라진다.
            candidates = sorted(candidates, key=lambda s: (s.lat, s.lon))
            collapsed.append(f"{name} {line_name} ({len(candidates)}행)")

        stations.append(candidates[0])

    logger.info(
        "역명 접미사 제거 %d행, 노선명 표기 통일 %d행, 키 중복 합침 %d건 %s",
        renamed,
        merged_lines,
        len(collapsed),
        collapsed or "",
    )

    if keyless:
        logger.warning("역명이나 노선명이 없어 건너뛴 행 %d개", keyless)

    logger.info("정규화 %d행", len(stations))
    return stations
