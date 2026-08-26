"""원문의 이름 표기를 고른다.

collect 와 enrich 를 가르는 기준을 그대로 쓴다. `source` 는 사이트가 준 것만 담고,
여기서부터는 우리 규칙이다. 그래서 표기 교정이 다운로드에 섞여 들지 않고, 규칙을
고칠 때 사이트를 다시 긁지 않아도 된다.

**행을 접지 않는다.** 원문 1,099행은 역이 아니라 (역, 노선)이고 그것이 곧 사용자가
고르는 단위다. 접으면 `강남역` 한 줄이 되어 2호선인지 신분당선인지 고를 수 없고,
노선마다 다른 좌표가 사라져 1km 반경 결과도 같아진다.

다만 키가 `(line_no, station_no)` 라 그 둘이 같은 행은 하나로 합친다. 원문이 한
노선번호에 두 노선명을 매달아 둔 탓에 생기는 4건이고, 합쳐지는 승강장 사이가 최대
167m 라 1km 반경에서는 같은 자리로 봐도 된다.

고칠 것이 둘이다.

**하나, 역명에 `역` 이 붙은 것과 안 붙은 것이 섞여 있다.** 1,099행 중 358행에만
붙어 있어 그대로 두면 "강남" 검색과 "강남역" 검색의 결과가 달라진다. 뗀 쪽으로
맞추고 표시할 때 붙이는 것은 앱에 맡긴다.

**둘, 같은 노선이 다른 이름으로 온다.** 검색 결과에 그대로 보이는 값이라
`도시철도 7호선` 이나 공백이 둘 들어간 `수도권  도시철도 9호선` 이 사용자에게
노출된다.
"""

import re
from collections import Counter, defaultdict
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
    줄을 이루고 `lat`/`lon` 이 그다음 질의의 입력이 된다.

    `(line_no, station_no)` 가 키다. 원문이 준 식별자라 스냅샷을 새로 받아도 같은
    역이 같은 키를 갖는다. 연번 대리키는 그렇지 않다. 매 실행이 테이블을 새로 만들어
    바꿔치므로 원문 앞쪽에 역이 하나만 생겨도 그 뒤 전부가 한 칸씩 밀리고, 앱이
    들고 있던 id 가 조용히 다른 역을 가리키게 된다.
    """

    line_no: str
    station_no: str
    name: str
    line_name: str
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


def _dominant_line_names(rows: list[SourceStation]) -> dict[str, str]:
    """노선번호마다 그 번호에서 가장 많이 쓰인 노선명.

    합칠 행 중 누구를 남길지 정하는 데만 쓴다. **노선명을 이걸로 갈아치우지
    않는다.** `I4108` 에 경의중앙선 51행과 경춘선 3행이 매달려 있는데 그 둘은 실제로
    다른 노선이라, 갈아치우면 경춘선 20행(`I41K2`)과 표기가 어긋난다.
    """
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        counts[row.line_no][clean_line(row.line_name)] += 1
    return {no: c.most_common(1)[0][0] for no, c in counts.items()}


@task
def normalize(rows: list[SourceStation]) -> list[Station]:
    """원문 행을 적재할 모양으로 고치고 키가 겹치는 행을 합친다."""
    logger = get_run_logger()

    dominant = _dominant_line_names(rows)

    groups: dict[tuple[str, str], list[Station]] = defaultdict(list)
    renamed = 0
    merged_lines = 0
    keyless = 0

    for row in rows:
        # 키가 없으면 담을 수 없다. 전량에서 0건이라 여기 걸리면 원문이 바뀐 것이다.
        if not row.line_no or not row.station_no:
            keyless += 1
            continue

        name = clean_name(row.name)
        line_name = clean_line(row.line_name)

        if name != row.name:
            renamed += 1
        if line_name != row.line_name:
            merged_lines += 1

        groups[(row.line_no, row.station_no)].append(
            Station(
                line_no=row.line_no,
                station_no=row.station_no,
                name=name,
                line_name=line_name,
                lat=row.lat,
                lon=row.lon,
            )
        )

    stations: list[Station] = []
    collapsed: list[str] = []

    for (line_no, station_no), candidates in groups.items():
        if len(candidates) > 1:
            # 대표 노선명을 가진 행을 남긴다. 그것도 갈리면 이름 순으로 정해 매
            # 실행 같은 행이 남게 한다. 임의로 첫 행을 집으면 원문 순서가 바뀔 때
            # 남는 행이 조용히 달라진다.
            candidates = sorted(
                candidates,
                key=lambda s: (s.line_name != dominant[line_no], s.name, s.line_name),
            )
            dropped = ", ".join(f"{s.name} {s.line_name}" for s in candidates[1:])
            collapsed.append(
                f"({line_no}, {station_no}) {candidates[0].name} "
                f"{candidates[0].line_name} <- {dropped}"
            )

        stations.append(candidates[0])

    logger.info(
        "역명 접미사 제거 %d행, 노선명 표기 통일 %d행, 키 중복 합침 %d건",
        renamed,
        merged_lines,
        len(collapsed),
    )

    # 합쳐진 것은 검색에서 사라진 이름이 있다는 뜻이라 전부 남긴다.
    for line in collapsed:
        logger.info("  합침 %s", line)

    if keyless:
        logger.warning("노선번호나 역번호가 없어 건너뛴 행 %d개", keyless)

    logger.info("정규화 %d행", len(stations))
    return stations
