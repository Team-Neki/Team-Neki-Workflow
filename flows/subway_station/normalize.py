"""원문의 이름 표기를 고르고 시도를 채운다.

collect 와 enrich 를 가르는 기준을 그대로 쓴다. `source` 는 사이트가 준 것만 담고,
여기서부터는 우리 규칙이다. 그래서 표기 교정이 다운로드에 섞여 들지 않고, 규칙을
고칠 때 사이트를 다시 긁지 않아도 된다.

**행을 접지 않는다.** 원문 1,099행은 역이 아니라 (역, 노선)이고 그것이 곧 사용자가
고르는 단위다. 접으면 `강남역` 한 줄이 되어 2호선인지 신분당선인지 고를 수 없고,
노선마다 다른 좌표가 사라져 1km 반경 결과도 같아진다. 완전 중복 한 행만 뺀다.

고칠 것이 넷이다.

**하나, 역명에 `역` 이 붙은 것과 안 붙은 것이 섞여 있다.** 1,099행 중 358행에만
붙어 있어 그대로 두면 "강남" 검색과 "강남역" 검색의 결과가 달라진다. 뗀 쪽으로
맞추고 표시할 때 붙이는 것은 앱에 맡긴다.

**둘, 같은 노선이 다른 이름으로 온다.** 검색 결과에 그대로 보이는 값이라
`도시철도 7호선` 이나 공백이 둘 들어간 `수도권  도시철도 9호선` 이 사용자에게
노출된다.

**셋, 주소가 시도로 시작하지 않는 행이 94개다.** 동명이역이 19쌍 있어 시도가 없으면
`송정역 5호선` 과 `송정역 동해선` 중 어느 쪽이 서울인지 알 수 없다.

**넷, 운영기관명에 시도가 접두로 붙은 것과 안 붙은 것이 섞여 있다.**
`부산광역시 부산교통공사` 와 `부산교통공사` 가 같이 있다.
"""

import re
from dataclasses import dataclass
from datetime import date

from prefect import get_run_logger, task

from flows.subway_station.source import SourceStation

# 정식 시도명. 주소와 운영기관명 앞머리를 여기에 맞춘다. 약칭(`서울시`)과 준말
# (`충남`)이 섞여 오므로 긴 이름부터 본다.
SIDO = (
    "서울특별시",
    "부산광역시",
    "대구광역시",
    "인천광역시",
    "광주광역시",
    "대전광역시",
    "울산광역시",
    "세종특별자치시",
    "경기도",
    "강원특별자치도",
    "충청북도",
    "충청남도",
    "전북특별자치도",
    "전라남도",
    "경상북도",
    "경상남도",
    "제주특별자치도",
)

# 원문에 실제로 나오는 표기들. 값이 정식 시도명이다.
SIDO_ALIAS = {
    "서울시": "서울특별시",
    "서울": "서울특별시",
    "부산시": "부산광역시",
    "부산": "부산광역시",
    "대구시": "대구광역시",
    "대구": "대구광역시",
    "인천시": "인천광역시",
    "인천": "인천광역시",
    "광주시": "광주광역시",
    "광주": "광주광역시",
    "대전시": "대전광역시",
    "대전": "대전광역시",
    "울산시": "울산광역시",
    "울산": "울산광역시",
    "세종시": "세종특별자치시",
    "세종": "세종특별자치시",
    "경기": "경기도",
    "강원도": "강원특별자치도",
    "강원": "강원특별자치도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전북특별자치도",
    "전라북도": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주도": "제주특별자치도",
    "제주": "제주특별자치도",
}

# 주소에 시도가 없을 때 기댈 곳. 원문에서 걸리는 것은 대구교통공사 91행과
# 구리도시공사 3행 둘뿐이라, 기관 이름에 시도가 드러나는 경우만 적어 둔다.
# **운영기관으로 시도를 정하는 것은 주소가 없을 때뿐이다.** 한국철도공사처럼
# 전국을 도는 기관이 있어 일반 규칙으로 쓰면 틀린다.
OPERATOR_SIDO = {
    "서울교통공사": "서울특별시",
    "부산교통공사": "부산광역시",
    "대구교통공사": "대구광역시",
    "인천교통공사": "인천광역시",
    "광주교통공사": "광주광역시",
    "대전교통공사": "대전광역시",
    "구리도시공사": "경기도",
}

# 같은 노선을 가리키는 다른 표기. 공백을 고른 뒤에 본다.
#
# **노선번호로 다수결을 내지 않는다.** `I4108` 하나에 경의중앙선 51행과 경춘선 3행이
# 매달려 있는데 그 둘은 실제로 다른 노선이라, 다수결이면 경춘선 3행이 경의중앙선으로
# 둔갑한다. `I4101` 의 1호선 10행과 경부선 37행도 마찬가지다. 노선명은 47종뿐이라
# 눈으로 확인할 수 있으므로 합칠 것만 적어 둔다.
LINE_ALIAS = {
    "도시철도 7호선": "7호선",
    "서울 도시철도 9호선": "9호선",
    "수도권 도시철도 9호선": "9호선",
    "수도권 광역철도 8호선": "8호선",
    "의정부": "의정부경전철",
}

# 환승역구분이 넷으로 온다. 도시철도 접두가 붙고 안 붙고의 차이뿐이다.
TRANSFER = "환승역"


@dataclass(frozen=True)
class Station:
    """적재할 (역, 노선) 하나. 조인 없이 이 한 줄로 검색과 표시가 되어야 한다.

    사용자는 "강남"을 검색해 `강남역 2호선` 과 `강남역 신분당선` 중 하나를 고르고,
    고른 행의 좌표로 1km 안의 부스를 받는다. 그래서 `name` 과 `line_name` 이 검색
    결과 한 줄을 이루고 `lat`/`lon` 이 그다음 질의의 입력이 된다.

    `region` 은 동명이역 때문에 있다. `송정` 처럼 서울과 부산에 같은 이름이 있는
    역이 19쌍이라 노선명만으로는 어디인지 알기 어렵다.
    """

    name: str
    name_en: str | None
    line_name: str
    line_no: str | None
    station_no: str | None
    lat: float
    lon: float
    region: str | None
    address: str | None
    operator: str | None
    phone: str | None
    is_transfer: bool
    base_on: date | None


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


def _strip_sido(value: str) -> str:
    """운영기관명 앞의 시도를 뗀다.

    `부산광역시 부산교통공사` 와 `부산교통공사` 가 섞여 있고, `서울특별시
    서울시메트로9호선㈜` 과 `서울시메트로9호선㈜` 도 마찬가지다. 뗀 쪽으로 맞춘다.
    """
    text = re.sub(r"\s+", " ", value).strip()
    for name in SIDO:
        prefix = f"{name} "
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text


def region_of(address: str, operator: str) -> str | None:
    """주소 앞머리에서 시도를 읽는다. 없으면 운영기관에서 찾는다.

    주소가 시도로 시작하지 않는 행이 94개 있고 전부 대구교통공사와 구리도시공사다.
    두 기관 다 한 시도 안에서만 운영하므로 기관 이름으로 메울 수 있다.
    """
    head = re.sub(r"\s+", " ", address).strip().split(" ")[0] if address else ""

    for name in SIDO:
        if head.startswith(name):
            return name

    if head in SIDO_ALIAS:
        return SIDO_ALIAS[head]

    return OPERATOR_SIDO.get(_strip_sido(operator))


def _blank_to_none(value: str) -> str | None:
    """빈 문자열을 NULL 로 맞춘다.

    `''` 가 그대로 담기면 다음 단계가 값이 있는 줄로 오해한다. 전화번호가 없는
    역을 "번호가 빈 문자열인 역"으로 보는 것과 같은 문제다.
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
    no_region: list[str] = []

    for row in rows:
        name = clean_name(row.name)
        line_name = clean_line(row.line_name)
        operator = _strip_sido(row.operator)
        region = region_of(row.address, row.operator)

        if name != row.name:
            renamed += 1
        if line_name != row.line_name:
            merged_lines += 1
        if region is None:
            no_region.append(f"{name} {line_name}")

        station = Station(
            name=name,
            name_en=_blank_to_none(row.name_en),
            line_name=line_name,
            line_no=_blank_to_none(row.line_no),
            station_no=_blank_to_none(row.station_no),
            lat=row.lat,
            lon=row.lon,
            region=region,
            address=_blank_to_none(row.address),
            operator=_blank_to_none(operator),
            phone=_blank_to_none(row.phone),
            is_transfer=TRANSFER in row.transfer,
            base_on=row.base_on,
        )

        # 원문에 모든 열이 같은 행이 하나 있다(경인선 주안역). 접는 것이 아니라
        # 같은 것을 두 번 담지 않는 것뿐이므로, 값이 하나라도 다르면 남는다.
        # 광운대역의 경의중앙선과 경춘선은 좌표가 달라 둘 다 남는다.
        key = (
            station.name,
            station.line_name,
            station.line_no,
            station.station_no,
            station.lat,
            station.lon,
            station.address,
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

    # 시도를 못 채운 행이 있으면 동명이역 구분이 안 된다. 원문 기준으로는 0이라
    # 여기 걸리면 주소 표기나 운영기관 이름이 바뀐 것이다.
    if no_region:
        logger.warning(
            "시도를 채우지 못한 역 %d개: %s",
            len(no_region),
            ", ".join(no_region[:10]),
        )

    logger.info("정규화 %d행", len(stations))
    return stations
