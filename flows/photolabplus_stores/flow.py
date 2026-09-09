"""포토랩플러스 지점 목록을 Kakao 장소검색으로 수집한다.

photolabplus.co.kr 에 목록이 있는데도 쓰지 않는다. 지역 탭의 실제 목록이
무규칙한 이름의 iframe 안에 있고, 게시판이 아니라 사람이 손으로 만든 텍스트
위젯이라 주소가 두 줄로 쪼개진 항목이 섞인다.

무엇보다 사이트가 틀린 데이터를 준다. 제주 탭 두 지점의 주소가 서울 주소로
들어가 있고 지도보기 버튼은 제주 좌표를 가리켜 서로 어긋난다. 파서를 아무리
잘 짜도 걸러지지 않는다. 원본이 틀렸기 때문이다.
"""

import json
from collections.abc import Sequence
from typing import Any

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 표기에 공백이 섞여 있지만 질의는 하나면 된다. `포토랩 플러스` 로 물어도 같은
# 문서가 나온다.
QUERIES = ("포토랩플러스",)

# 이름과 업종 둘로 거른다. `포토랩플러스 본사`는 브랜드명을 달고 있지만 업종이
# `서비스,산업 > 기업` 인 사무실이라 이름으로는 걸러지지 않는다. 반대로
# 하루필름에는 이름이 다르면서 업종은 사진인 경쟁사가 섞였다.
#
# 빼는 쪽을 나열하지 않고 담을 쪽을 정한다. 나열하면 다음에 어떤 업종이 섞여
# 들어올지 알 수 없다.
BRAND_NAMES = ("포토랩플러스", "포토랩 플러스")
BRANCH_CATEGORY = "문화,예술"


def _split_branches(
    documents: list[dict[str, Any]], *, names: Sequence[str], category: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """지점이 아닌 장소를 갈라낸다. 뺀 것은 이유와 함께 돌려준다.

    조용히 사라지면 나중에 빠진 것이 지점인지 아닌지 알 수 없다.
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


@flow(name="photolabplus-stores", log_prints=True)
def photolabplus_stores(
    queries: Sequence[str] = QUERIES,
    persist: bool = True,
) -> list[CollectedStore]:
    """질의 결과를 장소 id 로 합친다. 두 질의에 걸리는 지점이 있어도 중복이 없다.

    이름과 주소는 Kakao 가 준 그대로 담는다. 상호명이 섞였는지 층수가 붙었는지를
    여기서 판정하지 않는다. 그 해석은 enrich 의 일이다.

    persist 를 끄면 S3 에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    if not queries:
        raise ValueError("질의어가 없습니다. 최소 하나는 있어야 합니다.")

    documents: dict[str, dict[str, Any]] = {}
    shortfalls: list[tuple[str, int, int]] = []

    for query in queries:
        found, expected = search_all(query)

        documents.update({document["id"]: document for document in found})

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

    stores = parse_stores(branches, platform=Platform.PHOTO_LAB_PLUS)

    if not stores:
        raise ValueError(
            f"{list(queries)} 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="포토랩플러스")

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
            platform=Platform.PHOTO_LAB_PLUS,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.PHOTO_LAB_PLUS)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(queries))
    return stores
