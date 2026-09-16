"""인생네컷 지점 목록을 페이지별로 수집한다."""

from prefect import flow, get_run_logger

from flows.common.geocode import fill_coordinates
from flows.common.imweb_map import MAX_PAGES, collect_board
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_stores
from flows.common.store import CollectedStore

BASE_URL = "https://lifefourcuts.com"
BOARD_CODE = "b20210114da9a94d63009f"
REFERER = "https://lifefourcuts.com/Store01/"


@flow(name="lifefourcuts-stores", log_prints=True)
def lifefourcuts_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
    geocode: bool = True,
) -> list[CollectedStore]:
    """imweb 지도 위젯 게시판 하나를 순회한다.

    수집과 파싱은 flows.common.imweb_map 이 맡는다. 포토이즘, 돈룩업과 같은
    위젯이라 브랜드마다 파서를 두면 사이트 개편 때 한 곳만 고치게 된다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다. geocode를
    끄면 좌표가 빈 지점을 Kakao로 채우지 않는다. 파싱만 볼 때는 둘 다 끈다.
    """
    logger = get_run_logger()

    stores = collect_board(
        base_url=BASE_URL,
        board_code=BOARD_CODE,
        referer=REFERER,
        platform=Platform.LIFE_FOUR_CUT,
        max_pages=max_pages,
        delay_seconds=delay_seconds,
        persist=persist,
    )

    if geocode:
        stores = fill_coordinates(stores, label="인생네컷")

    log_stores(stores, label="인생네컷")

    if persist:
        put_stores(stores, platform=Platform.LIFE_FOUR_CUT)

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
