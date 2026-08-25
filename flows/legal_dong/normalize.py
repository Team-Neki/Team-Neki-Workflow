"""원문의 붙은 이름을 나누고 계층을 파생한다.

collect 와 enrich 를 가르는 기준을 그대로 쓴다. `source` 는 사이트가 준 것만
담고, 여기서부터는 우리 규칙이다. 그래서 이름 교정이 다운로드에 섞여 들지 않고,
규칙을 고칠 때 사이트를 다시 긁지 않아도 된다.

고쳐야 할 것이 둘이다. 둘 다 **추측이 아니라 같은 스냅샷 안의 다른 행을 근거로**
판정한다. 정규식으로 `시`와 `구` 사이를 자르는 방법은 쓰지 않는다. `군위군`,
`시흥시`, `군산시` 처럼 두 단위가 붙은 것처럼 보이는 정상 이름이 실제로 있어
그런 규칙은 멀쩡한 이름을 자른다.

**하나, 특례시 일반구의 공백이 빠져 있다.** `수원시영통구` 로 온다. 같은 시도 안에
`수원시` 라는 시군구 행이 따로 있으므로 그것을 접두로 떼면 경계가 나온다.
전량 20,561행에서 39종이 걸리고 1,755행이 영향을 받는다.

**둘, 세종특별자치시에 없는 시군구 `세종시` 가 채워져 있다.** 시군구 계층 행이
하나뿐인 시도는 실제로 시군구가 없으므로 그 행의 시군구명을 자리 채우기로 본다.
전량에서 세종 하나만 걸린다.
"""

from dataclasses import dataclass
from datetime import date

from prefect import get_run_logger, task

from flows.legal_dong.source import SourceDong

# 코드 자리수. 시도(2) + 시군구(3) + 읍면동(3) + 리(2) = 10.
SIDO = slice(0, 2)
SGG = slice(2, 5)
UMD = slice(5, 8)
RI = slice(8, 10)

LEVEL_SIDO = 1
LEVEL_SGG = 2
LEVEL_UMD = 3
LEVEL_RI = 4


@dataclass(frozen=True)
class LegalDong:
    """적재할 법정동 하나. 조인 없이 이 한 줄로 검색과 표시가 되어야 한다.

    계층 넷을 코드와 명칭으로 모두 담는다. 리 행의 `무장면` 처럼 상위 계층 이름이
    전체 명칭 안에만 있으면 앱이 문자열을 쪼개야 하므로, 원본이 열로 주는 것을
    버리지 않는다.

    `leaf_name` 과 `full_name` 은 그 계층들에서 뽑아낸 것이다. 둘을 다 담는 이유는
    검색과 표시가 서로 다른 값을 원하기 때문이다. 사용자가 "강남" 을 넣었을 때
    `full_name` 으로 찾으면 `강남구` 와 함께 그 아래 역삼동, 개포동까지 15건이
    딸려 나온다. `leaf_name` 으로 찾으면 `강남구`, (진주시) `강남동`,
    (고창군 무장면) `강남리` 셋만 남는다. 표시할 때는 어느 강남동인지 알려줘야
    하므로 다시 `full_name` 이 필요하다.
    """

    code: str
    level: int
    sido_code: str
    sido_name: str
    sgg_code: str
    sgg_name: str | None
    umd_code: str
    umd_name: str | None
    ri_code: str
    ri_name: str | None
    leaf_name: str
    full_name: str
    created_on: date | None


def level_of(code: str) -> int:
    """코드 자리수에서 계층을 읽는다.

    아래 자리가 비어 있으면 그 위 계층의 행이다. 채워진 이름 컬럼 개수로 세지
    않는 이유는 원문이 그 규칙을 지키지 않기 때문이다. 세종의 읍면동 행에는
    시군구명이 채워져 있는데 그것이 자리 채우기다.
    """
    if code[SGG] == "000":
        return LEVEL_SIDO
    if code[UMD] == "000":
        return LEVEL_SGG
    if code[RI] == "00":
        return LEVEL_UMD
    return LEVEL_RI


def _sgg_names_by_sido(rows: list[SourceDong]) -> dict[str, set[str]]:
    """시도별로 시군구 계층 행의 이름을 모은다. 분리 판정의 근거가 된다."""
    names: dict[str, set[str]] = {}
    for row in rows:
        if level_of(row.code) == LEVEL_SGG and row.sgg_name:
            names.setdefault(row.sido_name, set()).add(row.sgg_name)
    return names


def _split_sgg(name: str, siblings: set[str]) -> str:
    """같은 시도의 다른 시군구명이 접두면 그 경계에 공백을 넣는다.

    `수원시영통구` 는 형제 `수원시` 가 접두라서 `수원시 영통구` 가 된다.
    `군위군` 은 접두인 형제가 없어 그대로 둔다. 접두가 여럿이면 가장 긴 것을
    쓴다.
    """
    prefixes = [
        other
        for other in siblings
        if other != name and name.startswith(other)
    ]
    if not prefixes:
        return name

    base = max(prefixes, key=len)
    return f"{base} {name[len(base):]}"


@task
def normalize(rows: list[SourceDong]) -> list[LegalDong]:
    """붙은 시군구명을 나누고 자리 채우기를 걷어낸 뒤 계층을 붙인다."""
    logger = get_run_logger()

    siblings = _sgg_names_by_sido(rows)

    # 시군구 계층 행이 하나뿐인 시도는 실제로 시군구가 없다. 그 하나는 원문이
    # 빈 자리를 채우려고 넣은 이름이다.
    phantoms = {
        sido: next(iter(names))
        for sido, names in siblings.items()
        if len(names) == 1
    }
    if phantoms:
        logger.info("시군구가 없는 시도로 판정: %s", phantoms)
    if len(phantoms) > 1:
        # 전량에서 세종 하나만 걸렸다. 둘 이상이면 원문 구조가 바뀐 것이므로
        # 판정을 다시 봐야 한다.
        logger.warning(
            "시군구가 없는 시도가 %d개입니다. 원문 구조가 바뀌었을 수 "
            "있으니 확인하세요.",
            len(phantoms),
        )

    dongs: list[LegalDong] = []
    split_count = 0

    for row in rows:
        sgg_name: str | None = row.sgg_name or None

        if sgg_name and phantoms.get(row.sido_name) == sgg_name:
            sgg_name = None
        elif sgg_name:
            fixed = _split_sgg(sgg_name, siblings.get(row.sido_name, set()))
            if fixed != sgg_name:
                split_count += 1
                sgg_name = fixed

        parts = [
            part
            for part in (row.sido_name, sgg_name, row.umd_name, row.ri_name)
            if part
        ]

        dongs.append(
            LegalDong(
                code=row.code,
                level=level_of(row.code),
                sido_code=row.code[SIDO],
                sido_name=row.sido_name,
                sgg_code=row.code[SGG],
                sgg_name=sgg_name,
                umd_code=row.code[UMD],
                umd_name=row.umd_name or None,
                ri_code=row.code[RI],
                ri_name=row.ri_name or None,
                # 채워진 것 중 가장 아래가 자기 이름이다. 시도 행은 그것이 곧
                # 시도명이다.
                leaf_name=parts[-1],
                full_name=" ".join(parts),
                created_on=row.created_on,
            )
        )

    logger.info(
        "정규화 %d건 (시군구명 분리 %d건, 시도 %d, 시군구 %d, 읍면동 %d, 리 %d)",
        len(dongs),
        split_count,
        sum(1 for dong in dongs if dong.level == LEVEL_SIDO),
        sum(1 for dong in dongs if dong.level == LEVEL_SGG),
        sum(1 for dong in dongs if dong.level == LEVEL_UMD),
        sum(1 for dong in dongs if dong.level == LEVEL_RI),
    )

    return dongs
