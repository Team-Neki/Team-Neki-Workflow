"""포토시그니처 지점 목록을 수집한다."""

from prefect import flow, get_run_logger

from flows.common.output import log_stores
from flows.photosignature_stores.stores import Store, fetch_store_page, parse_stores


@flow(name="photosignature-stores", log_prints=True)
def photosignature_stores() -> list[Store]:
    """전량을 한 번에 받아 파싱한다.

    인생네컷과 달리 페이지 순회가 없다. page 파라미터를 바꿔도 같은 전량이
    돌아오므로 순회하면 같은 데이터를 반복해서 받을 뿐이다.
    """
    logger = get_run_logger()

    html = fetch_store_page()
    stores = parse_stores(html)

    if not stores:
        raise ValueError(
            "지점을 하나도 찾지 못했습니다. 페이지 구조가 바뀌었을 수 있습니다."
        )

    log_stores(stores, label="포토시그니처")

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
