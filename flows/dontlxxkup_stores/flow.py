"""돈룩업 지점 목록을 페이지별로 수집한다."""

from prefect import flow, get_run_logger

from flows.common.imweb_map import MAX_PAGES, collect_board
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_stores
from flows.common.store import CollectedStore

BASE_URL = "https://dontlxxkup.kr"
BOARD_CODE = "b202401090e0e7fd926f7a"
REFERER = "https://dontlxxkup.kr/store"


@flow(name="dontlxxkup-stores", log_prints=True)
def dontlxxkup_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
) -> list[CollectedStore]:
    """돈룩업 매장 안내 게시판을 순회한다.

    인생네컷, 포토이즘과 같은 imweb 지도 위젯이라 수집과 파싱은
    flows.common.imweb_map 이 그대로 맡는다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    stores = collect_board(
        base_url=BASE_URL,
        board_code=BOARD_CODE,
        referer=REFERER,
        platform=Platform.DONT_LXXK_UP,
        max_pages=max_pages,
        delay_seconds=delay_seconds,
        persist=persist,
    )

    log_stores(stores, label="돈룩업")

    if persist:
        put_stores(stores, platform=Platform.DONT_LXXK_UP)

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
