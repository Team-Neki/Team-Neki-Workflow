"""인생네컷 지점 목록을 페이지별로 수집한다."""

import time

from prefect import flow, get_run_logger

from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore
from flows.lifefourcuts_stores.stores import (
    PAGE_SIZE,
    fetch_store_page,
    parse_stores,
)

# 범위를 벗어난 페이지가 빈 응답 대신 마지막 페이지를 그대로 돌려준다.
# 종료 판정이 어긋나면 무한히 도는 구조라, 페이지 수에 상한을 둔다.
MAX_PAGES = 50


@flow(name="lifefourcuts-stores", log_prints=True)
def lifefourcuts_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
) -> list[CollectedStore]:
    """1페이지부터 순회하며 지점을 모은다.

    이 사이트는 마지막 페이지를 넘겨도 빈 목록을 주지 않고 마지막 페이지를
    반복해서 돌려준다. 따라서 "빈 응답이면 중단"으로는 끝나지 않는다.
    직전 페이지와 식별자 집합이 같으면 더 볼 것이 없다고 판단한다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    collected: dict[str, CollectedStore] = {}
    previous_ids: set[str] = set()
    pages: list[tuple[int, str]] = []

    for page in range(1, max_pages + 1):
        if page > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)

        html = fetch_store_page(page)
        pages.append((page, html))
        stores = parse_stores(html)

        if not stores:
            logger.info("page %d: 항목이 없어 종료합니다.", page)
            break

        current_ids = {store.idx for store in stores}

        if current_ids == previous_ids:
            logger.info("page %d: 직전 페이지와 동일해 종료합니다.", page)
            break

        for store in stores:
            collected.setdefault(store.idx, store)

        logger.info("page %d: %d건 (누적 %d건)", page, len(stores), len(collected))

        # 페이지가 덜 찼다면 마지막 페이지다. 담고 나서 끝낸다.
        if len(stores) < PAGE_SIZE:
            logger.info("page %d: 마지막 페이지입니다.", page)
            break

        previous_ids = current_ids
    else:
        logger.warning(
            "상한 %d 페이지에 도달했습니다. 종료 조건을 확인해야 합니다.", max_pages
        )

    stores = list(collected.values())
    log_stores(stores, label="인생네컷")

    if persist:
        for number, html in pages:
            put_raw(
                html,
                platform=Platform.LIFE_FOUR_CUT,
                name=f"page-{number:03d}.html",
            )
        put_stores(stores, platform=Platform.LIFE_FOUR_CUT)

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
