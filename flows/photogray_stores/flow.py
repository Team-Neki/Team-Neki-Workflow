"""포토그레이 지점 목록을 페이지별로 수집한다."""

import time
from collections import Counter

from prefect import flow, get_run_logger

from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore
from flows.photogray_stores.stores import (
    fetch_store_page,
    parse_stores,
    parse_total_count,
)

# 100건씩 3페이지면 끝나지만 지점이 늘 수 있어 여유를 둔다.
MAX_PAGES = 30


@flow(name="photogray-stores", log_prints=True)
def photogray_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
) -> list[CollectedStore]:
    """1페이지부터 순회하며 지점을 모은다.

    범위를 벗어난 페이지는 빈 목록을 준다. 인생네컷처럼 마지막 페이지를
    되돌려주지 않으므로 빈 응답으로 종료를 판정한다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    collected: list[CollectedStore] = []
    pages: list[tuple[int, str]] = []
    total: int | None = None

    for page in range(1, max_pages + 1):
        if page > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)

        html = fetch_store_page(page)
        pages.append((page, html))

        if page == 1:
            total = parse_total_count(html)

        stores = parse_stores(html)

        if not stores:
            logger.info("page %d: 항목이 없어 종료합니다.", page)
            break

        collected.extend(stores)
        logger.info("page %d: %d건 (누적 %d건)", page, len(stores), len(collected))
    else:
        logger.warning(
            "상한 %d 페이지에 도달했습니다. 종료 조건을 확인해야 합니다.", max_pages
        )

    # 원문을 먼저 남긴다. 아래 검사에 걸려 예외로 빠져나가더라도 그때 무엇을
    # 받았는지 볼 수 있어야 한다.
    if persist:
        for number, html in pages:
            put_raw(
                html,
                platform=Platform.PHOTO_GRAY,
                name=f"page-{number:03d}.html",
            )

    # idx가 지점명이라 이름이 겹치면 다음 단계에서 한 지점이 다른 지점을
    # 덮어쓴다. 조용히 사라지게 두지 않고 여기서 막는다.
    duplicated = sorted(
        name for name, count in Counter(s.idx for s in collected).items() if count > 1
    )
    if duplicated:
        raise ValueError(
            f"지점명이 겹칩니다: {duplicated}. 지점명을 식별자로 쓰므로 "
            "겹치면 지점이 사라집니다."
        )

    log_stores(collected, label="포토그레이")

    if total is not None and len(collected) < total:
        logger.warning(
            "목록은 총 %d건이라고 하는데 %d건만 모았습니다. 페이지 순회가 덜 "
            "내려갔거나 파서가 항목을 놓쳤을 수 있습니다.",
            total,
            len(collected),
        )

    if persist:
        put_stores(collected, platform=Platform.PHOTO_GRAY)

    logger.info("수집 완료: 지점 %d건 (목록 표기 %s건)", len(collected), total)
    return collected
