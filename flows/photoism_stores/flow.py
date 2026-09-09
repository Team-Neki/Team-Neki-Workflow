"""포토이즘 지점 목록을 페이지별로 수집한다."""

from prefect import flow, get_run_logger

from flows.common.imweb_map import MAX_PAGES, collect_board
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_stores
from flows.common.store import CollectedStore

BASE_URL = "https://photoism.co.kr"
REFERER = "https://photoism.co.kr/279"

# '지점 정보' 아래에 게시판이 셋 있다. 스튜디오(b2022071245de025e336e7)와
# 컬러드(b20220713ca5a26fde77a4)는 아직 수집 대상이 아니다.
BOARD_CODE = "b202207139aa9cbd453ce3"


@flow(name="photoism-stores", log_prints=True)
def photoism_stores(
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
) -> list[CollectedStore]:
    """포토이즘 박스 게시판을 순회한다.

    인생네컷과 같은 imweb 지도 위젯이라 수집과 파싱은
    flows.common.imweb_map 이 그대로 맡는다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    stores = collect_board(
        base_url=BASE_URL,
        board_code=BOARD_CODE,
        referer=REFERER,
        platform=Platform.PHOTOISM,
        max_pages=max_pages,
        delay_seconds=delay_seconds,
        persist=persist,
    )

    log_stores(stores, label="포토이즘")

    if persist:
        put_stores(stores, platform=Platform.PHOTOISM)

    logger.info("수집 완료: 지점 %d건", len(stores))
    return stores
