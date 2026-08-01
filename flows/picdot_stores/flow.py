"""픽닷 지점 목록을 Kakao 장소검색으로 수집한다."""

from prefect import flow, get_run_logger

from flows.common.output import log_stores
from flows.picdot_stores.stores import (
    MAX_EXPOSED,
    PAGE_SIZE,
    QUERY,
    Store,
    parse_stores,
    search_page,
)

# 노출 상한이 45건이라 4페이지면 충분하다. 상한이 바뀔 경우를 대비한 여유값이다.
MAX_PAGES = 45


@flow(name="picdot-stores", log_prints=True)
def picdot_stores(query: str = QUERY, max_pages: int = MAX_PAGES) -> list[Store]:
    """질의 하나로 전량을 받아온다.

    당초 45건 상한 때문에 행정구역 순회가 필요할 것으로 봤으나, 실측 결과
    전국 질의 하나에 전량이 들어 있어 순회하지 않는다. 지점이 늘어 상한에
    닿으면 순회 방식으로 바꿔야 하므로 그 시점을 경고로 알린다.
    """
    logger = get_run_logger()

    collected: dict[str, Store] = {}

    for page in range(1, max_pages + 1):
        payload = search_page(page, query=query)
        stores = parse_stores(payload)

        for store in stores:
            collected.setdefault(store.idx, store)

        meta = payload.get("meta") or {}
        if page == 1:
            logger.info(
                "total_count=%s pageable_count=%s",
                meta.get("total_count"),
                meta.get("pageable_count"),
            )

        if meta.get("is_end") or not stores:
            break

        if page * PAGE_SIZE >= MAX_EXPOSED:
            logger.warning(
                "노출 상한 %d건에 도달했습니다. 지점이 늘었다면 행정구역별 "
                "질의로 나눠야 합니다.",
                MAX_EXPOSED,
            )
            break

    if not collected:
        raise ValueError(
            f"'{query}' 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    stores = list(collected.values())
    log_stores(stores, label="픽닷")

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
