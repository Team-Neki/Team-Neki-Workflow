"""포토시그니처 지점 목록을 수집한다."""

from datetime import date

from prefect import flow, get_run_logger

from flows.common.geocode import fill_coordinates
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore
from flows.photosignature_stores.stores import fetch_store_page, parse_stores


@flow(name="photosignature-stores", log_prints=True)
def photosignature_stores(
    persist: bool = True,
    geocode: bool = True,
    target_date: date | None = None,
) -> list[CollectedStore]:
    """전량을 한 번에 받아 파싱한다.

    인생네컷과 달리 페이지 순회가 없다. page 파라미터를 바꿔도 같은 전량이
    돌아오므로 순회하면 같은 데이터를 반복해서 받을 뿐이다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다. geocode를
    끄면 좌표가 빈 지점을 Kakao로 채우지 않는다. 파싱만 볼 때는 둘 다 끈다.

    `target_date` 는 이 적재물이 어느 수집 사이클의 것인지다. `stores_collect`
    가 묶어 돌 때 내려보내며, 단독 실행이면 이 run 의 예약 시각에서 정해진다.
    """
    logger = get_run_logger()

    html = fetch_store_page()
    stores = parse_stores(html)

    # 원문을 먼저 남긴다. 파싱이 0건이면 예외로 빠져나가는데, 그때 원인을 볼
    # 수 있어야 한다. 이 브랜드는 정규식으로 마크업을 긁어 특히 잘 깨진다.
    if persist:
        put_raw(html, platform=Platform.PHOTO_SIGNATURE, name="store.html")

    if not stores:
        raise ValueError(
            "지점을 하나도 찾지 못했습니다. 페이지 구조가 바뀌었을 수 있습니다."
        )

    if geocode:
        stores = fill_coordinates(stores, label="포토시그니처")

    log_stores(stores, label="포토시그니처")

    if persist:
        put_stores(stores, platform=Platform.PHOTO_SIGNATURE, target_date=target_date)

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
