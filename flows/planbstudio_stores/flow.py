"""플랜비스튜디오 지점 목록을 페이지별로 수집한다."""

import time

from prefect import flow, get_run_logger

from flows.common.output import log_stores
from flows.planbstudio_stores.stores import (
    Store,
    fetch_store_page,
    parse_coordinates,
    parse_stores,
)

# 목록은 23페이지에서 끝나지만 지점이 늘 수 있어 여유를 둔다.
MAX_PAGES = 60


@flow(name="planbstudio-stores", log_prints=True)
def planbstudio_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
) -> list[Store]:
    """목록을 순회하며 지점을 모으고 좌표를 채운다.

    좌표는 지도 스크립트에 있고 페이지를 바꿔도 내용이 같다. 그래서 첫
    페이지에서 한 번만 뽑아 이후 페이지에 재사용한다.

    목록은 범위를 벗어나면 빈 응답을 준다. 인생네컷처럼 마지막 페이지를
    되돌려주지 않으므로 빈 응답으로 종료를 판정한다.
    """
    logger = get_run_logger()

    collected: dict[str, Store] = {}
    coordinates: dict[str, tuple[float, float]] = {}

    for page in range(1, max_pages + 1):
        if page > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)

        html = fetch_store_page(page)

        if page == 1:
            coordinates = parse_coordinates(html)

        stores = parse_stores(html, coordinates)

        if not stores:
            logger.info("page %d: 항목이 없어 종료합니다.", page)
            break

        for store in stores:
            collected.setdefault(store.idx, store)

        logger.info("page %d: %d건 (누적 %d건)", page, len(stores), len(collected))
    else:
        logger.warning(
            "상한 %d 페이지에 도달했습니다. 종료 조건을 확인해야 합니다.", max_pages
        )

    stores = list(collected.values())
    log_stores(stores, label="플랜비스튜디오")

    located = sum(1 for store in stores if store.latitude is not None)
    logger.info("수집 완료: 지점 %d건 (좌표 있음 %d건)", len(stores), located)

    return stores
