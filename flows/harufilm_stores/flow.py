"""하루필름 지점 목록을 Kakao 장소검색으로 수집한다.

harufilm.com 에도 목록이 있지만 지역 페이지 여덟 곳에 흩어져 있고, 게시판이
아니라 갤러리 위젯 캡션이라 지역이 개편되면 조용히 어긋난다. 전화와 좌표도
주지 않는다. Kakao 는 좌표와 전화까지 한 번에 온다.
"""

import json
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 셋을 다 물어야 전량이 나온다. `크림필터 전포점`처럼 `하루필름` 표기가 아예
# 없는 지점이 있어 기본 질의에 걸리지 않는다.
QUERIES = ("하루필름", "크림필터", "하루에어")

# 계열 표기가 셋이라 브랜드명 하나로는 거를 수 없다. `하루필름`만 보면
# `크림필터 전포점`, `하루에어 홍대점`이 같이 날아간다.
BRAND_NAMES = ("하루필름", "크림필터", "하루에어")

# 이름과 업종 둘로 거른다. `하루필름` 질의에 경쟁사가 섞여 오는데 그중에는
# 우리가 따로 수집하는 인생네컷도 있다. 그대로 두면 같은 지점이 두 파티션에
# 다른 idx 로 들어간다. 반대로 `하나은행365 하루필름순천점`은 계열명을 달고
# 있지만 업종이 은행 ATM 이다.
#
# 빼는 쪽을 나열하지 않고 담을 쪽을 정한다. 나열하면 다음에 어떤 업종이 섞여
# 들어올지 알 수 없다.
BRANCH_CATEGORY = "문화,예술"


def _split_branches(
    documents: list[dict[str, Any]], *, names: Sequence[str], category: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """지점이 아닌 장소를 갈라낸다.

    이름에 계열 표기가 하나도 없거나, 업종이 사진이 아니면 뺀다. 뺀 것은 이유와
    함께 돌려준다. 조용히 사라지면 나중에 빠진 것이 지점인지 아닌지 알 수 없다.
    """
    branches: list[dict[str, Any]] = []
    dropped: list[str] = []

    for document in documents:
        name = (document.get("place_name") or "").strip() or "(이름 없음)"
        group = (document.get("category_name") or "").strip() or "(업종 없음)"

        if not any(brand in name for brand in names):
            dropped.append(f"{name} — 계열 아님")
        elif not group.startswith(category):
            dropped.append(f"{name} — {group}")
        else:
            branches.append(document)

    return branches, dropped


def _same_position(stores: list[CollectedStore]) -> list[list[CollectedStore]]:
    """좌표가 똑같은 지점끼리 묶어 둘 이상인 것만 돌려준다.

    Kakao 에 같은 가게가 두 번 등록된 경우가 있다. 주소도 좌표도 소수점까지
    같은데 장소 id 가 달라 id 로 합치는 것만으로는 걸러지지 않는다.

    합치지는 않는다. 어느 id 를 살릴지는 사이트가 준 값만으로 정할 수 없고 그
    판단은 enrich 의 일이다. 드러내기만 한다.
    """
    positions: dict[tuple[float, float], list[CollectedStore]] = defaultdict(list)

    for store in stores:
        if store.longitude is None or store.latitude is None:
            continue
        positions[(store.longitude, store.latitude)].append(store)

    return [group for group in positions.values() if len(group) > 1]


@flow(name="harufilm-stores", log_prints=True)
def harufilm_stores(
    queries: Sequence[str] = QUERIES,
    persist: bool = True,
) -> list[CollectedStore]:
    """질의 결과를 장소 id 로 합친다. 두 질의에 걸리는 지점이 있어도 중복이 없다.

    이름은 Kakao 가 준 그대로 담는다. `크림필터 전포점`에 `하루필름` 접두를
    붙여 맞추는 것은 해석이므로 enrich 의 일이다.

    persist 를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    if not queries:
        raise ValueError("질의어가 없습니다. 최소 하나는 있어야 합니다.")

    documents: dict[str, dict[str, Any]] = {}
    shortfalls: list[tuple[str, int, int]] = []

    for query in queries:
        found, expected = search_all(query)

        for document in found:
            documents[document["id"]] = document

        # 질의별로 대조한다. 질의끼리 결과가 겹쳐 total_count 를 합산할 수 없고,
        # 합계로 보면 한 질의의 미달을 다른 질의가 메워 가려버린다.
        if len(found) < expected:
            shortfalls.append((query, len(found), expected))

        logger.info("'%s' 검색 %d건 (total_count %d)", query, len(found), expected)

    merged = list(documents.values())

    branches, dropped = _split_branches(
        merged, names=BRAND_NAMES, category=BRANCH_CATEGORY
    )
    if dropped:
        logger.info("지점이 아니라 뺀 장소 %d건: %s", len(dropped), dropped)

    stores = parse_stores(branches, platform=Platform.HARU_FILM)

    if not stores:
        raise ValueError(
            f"{list(queries)} 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="하루필름")

    for group in _same_position(stores):
        logger.warning(
            "좌표가 같은 지점이 %d건 있습니다. Kakao 에 같은 가게가 여러 번 "
            "등록됐을 수 있습니다: %s",
            len(group),
            [(store.idx, store.name) for store in group],
        )

    for query, collected, expected in shortfalls:
        logger.warning(
            "'%s' 수집 %d건이 total_count %d건에 못 미칩니다. 분할이 부족했을 수 "
            "있습니다.",
            query,
            collected,
            expected,
        )

    if persist:
        # 걸러내기 전을 담는다. 필터가 과했는지 나중에 확인하려면 뺀 것이
        # 원문에 남아 있어야 한다.
        put_raw(
            json.dumps(merged, ensure_ascii=False),
            platform=Platform.HARU_FILM,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.HARU_FILM)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(queries))
    return stores
