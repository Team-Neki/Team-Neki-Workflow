"""포토그레이 지점 목록을 Kakao 장소검색으로 수집한다.

사이트 게시판에도 목록이 있지만 좌표를 주지 않는다. 지도는 지점을 누를 때
브라우저가 주소를 Kakao 지오코더에 넣어 그리므로 사이트도 좌표를 모른다.
좌표가 후속 단계에서 가장 중요한 값이라 수집원 자체를 Kakao로 옮겼다.

대신 Kakao가 아는 지점이 사이트 목록보다 적다. 그만큼은 수집되지 않는다.
"""

import json
from typing import Any

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 표기를 바꿔도 더 나오지 않는다. PHOTOGRAY, 포토 그레이로도 돌려봤지만 전부
# 이 질의의 부분집합이었다.
QUERY = "포토그레이"

# 브랜드명으로 물어도 지점이 아닌 것이 섞인다. 운영사인 에이피알 본사가 그렇다.
#
# 이름과 업종 둘로 거른다. 한쪽만으로는 부족한 경우를 다른 브랜드에서 이미
# 만났다. 하루필름에는 이름이 다르면서 업종은 사진인 경쟁사가 섞였고,
# 포토랩플러스에는 브랜드명을 단 본사가 있었다.
BRAND_NAMES = ("포토그레이",)
BRANCH_CATEGORY = "문화,예술"


def _split_branches(
    documents: list[dict[str, Any]], *, names: tuple[str, ...], category: str
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


@flow(name="photogray-stores", log_prints=True)
def photogray_stores(query: str = QUERY, persist: bool = True) -> list[CollectedStore]:
    """persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다."""
    logger = get_run_logger()

    documents, expected = search_all(query)

    branches, dropped = _split_branches(
        documents, names=BRAND_NAMES, category=BRANCH_CATEGORY
    )
    if dropped:
        logger.info("지점이 아니라 뺀 장소 %d건: %s", len(dropped), dropped)

    stores = parse_stores(branches, platform=Platform.PHOTO_GRAY)

    if not stores:
        raise ValueError(
            f"'{query}' 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="포토그레이")

    # 일부러 뺀 것은 빠진 것이 아니므로 도로 더해서 비교한다. 그러지 않으면
    # 이 경고가 늘 떠 있게 된다.
    if len(stores) + len(dropped) < expected:
        logger.warning(
            "수집 %d건에 제외 %d건을 더해도 total_count %d건에 못 미칩니다. "
            "분할이 부족했을 수 있습니다.",
            len(stores),
            len(dropped),
            expected,
        )

    if persist:
        # 걸러내기 전을 담는다. 필터가 과했는지 나중에 확인하려면 뺀 것이
        # 원문에 남아 있어야 한다.
        put_raw(
            json.dumps(documents, ensure_ascii=False),
            platform=Platform.PHOTO_GRAY,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.PHOTO_GRAY)

    logger.info("수집 완료: 지점 %d건 (total_count %d)", len(stores), expected)
    return stores
