"""imweb 지도 위젯의 지점 목록 수집.

인생네컷, 포토이즘, 돈룩업이 같은 위젯을 쓴다. 목록 페이지 HTML에는 지점
데이터가 없고 AJAX 엔드포인트가 HTML 조각을 돌려주므로, 페이지를 그대로 긁지
않고 해당 엔드포인트를 직접 호출한다.

브랜드마다 다른 것은 base_url, board_code, referer, platform 넷뿐이다. 여기에
브랜드별 분기를 넣으면 사이트가 개편됐을 때 한 곳만 고치고 넘어가게 되므로
공용화한 의미가 사라진다. 브랜드가 늘어도 이 파일은 그대로여야 한다.
"""

import time
from typing import Any

import httpx
from bs4 import BeautifulSoup
from prefect import get_run_logger, task

from flows.common.platform import Platform
from flows.common.storage import put_raw
from flows.common.store import CollectedStore

# 위젯이 정한 경로다. 브랜드가 아니라 imweb 쪽 규약이므로 공용으로 둔다.
LIST_PATH = "/ajax/get_map_list.cm"

# 페이지당 항목 수. 이보다 적게 오면 마지막 페이지다.
PAGE_SIZE = 10

# 종료 판정이 어긋났을 때 무한히 도는 것을 막는 안전장치다. 실제 페이지 수에
# 가깝게 잡으면 지점이 조금만 늘어도 상한에 걸려 조용히 잘리므로 넉넉히 둔다.
# 포토이즘이 49페이지로 가장 길다.
MAX_PAGES = 100


def _text(node: Any) -> str | None:
    if node is None:
        return None
    value = node.get_text(strip=True)
    return value or None


def _phone(container: Any) -> str | None:
    """tel: 링크에서 번호를 꺼낸다.

    링크 본문에는 스크린리더용 <span class="sr-only">phone number</span>가
    붙어 있어 텍스트를 그대로 쓰면 번호 뒤에 딸려온다.

    포토이즘에는 링크는 있는데 href가 `tel:` 로 비어 있는 항목이 있다. 빈
    문자열이 그대로 담기면 다음 단계가 번호가 있는 줄로 오해하므로 None으로
    맞춘다.
    """
    node = container.select_one("p.tell a[href^='tel:']")
    if node is None:
        return None

    number = (node.get("href") or "").removeprefix("tel:").strip()
    return number or None


def _coordinate(container: Any, selector: str) -> float | None:
    node = container.select_one(selector)
    if node is None:
        return None

    raw = (node.get("value") or "").strip()
    if not raw:
        return None

    try:
        return float(raw)
    except ValueError:
        return None


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def fetch_page(
    base_url: str,
    board_code: str,
    page: int,
    *,
    referer: str,
    timeout: float = 20.0,
) -> str:
    """지점 목록 한 페이지를 HTML 조각으로 받아온다."""
    response = httpx.post(
        f"{base_url}{LIST_PATH}",
        data={
            "board_code": board_code,
            "search": "",
            "search_mod": "all",
            "page": page,
            "sort": "STREET",
            "status": "",
        },
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": referer,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def extract_stores(
    html: str, *, platform: Platform
) -> tuple[list[CollectedStore], list[str]]:
    """HTML 조각에서 지점을 추출한다. Prefect에 의존하지 않는 순수 함수다.

    지점명이 없는 항목은 건너뛰고 그 id를 함께 돌려준다. 나머지 필드는 없으면
    None으로 둔다. 한 항목의 결함으로 페이지 전체를 잃지 않기 위함이다.
    """
    soup = BeautifulSoup(html, "lxml")

    stores: list[CollectedStore] = []
    skipped: list[str] = []

    for container in soup.select("div.map_container[id^='list_']"):
        idx = container.get("id", "").removeprefix("list_")
        name = _text(container.select_one(".tit"))

        if not idx or not name:
            skipped.append(container.get("id") or "(id 없음)")
            continue

        stores.append(
            CollectedStore(
                platform=platform,
                idx=idx,
                name=name,
                # 사이트가 address를 adress로 표기한다. 오타지만 그대로 맞춰야 한다.
                address=_text(container.select_one("p.adress")),
                phone=_phone(container),
                longitude=_coordinate(container, "input._pos_x_temp"),
                latitude=_coordinate(container, "input._pos_y_temp"),
            )
        )

    return stores, skipped


def total_count(html: str) -> int | None:
    """조각 상단에 실려 오는 총 건수. Kakao의 total_count처럼 대조 기준으로 쓴다.

    위젯 설정에 따라 켜고 끌 수 있어 인생네컷처럼 자리만 있고 값이 빈 사이트가
    있다. 그래서 없을 수 있다는 것을 타입으로 드러내고, 있을 때만 비교한다.

    항목마다 있는 .tit 과 이름이 겹치므로 툴바 안으로 범위를 좁힌다.
    """
    node = BeautifulSoup(html, "lxml").select_one(
        "div.map-toolbar .tit span.text-brand"
    )
    if node is None:
        return None

    try:
        return int(node.get_text(strip=True).replace(",", ""))
    except ValueError:
        return None


@task
def parse_stores(html: str, *, platform: Platform) -> list[CollectedStore]:
    """extract_stores를 감싸고 건너뛴 항목을 남긴다."""
    stores, skipped = extract_stores(html, platform=platform)

    if skipped:
        get_run_logger().warning(
            "이름이나 식별자가 없어 건너뛴 항목 %d건: %s", len(skipped), skipped
        )

    return stores


def collect_board(
    *,
    base_url: str,
    board_code: str,
    referer: str,
    platform: Platform,
    max_pages: int = MAX_PAGES,
    delay_seconds: float = 0.5,
    persist: bool = True,
) -> list[CollectedStore]:
    """게시판 하나를 1페이지부터 순회하며 지점을 모은다.

    이 위젯은 마지막 페이지를 넘겨도 빈 목록을 주지 않고 마지막 페이지를
    반복해서 돌려준다. 따라서 "빈 응답이면 중단"으로는 끝나지 않는다. 직전
    페이지와 식별자 집합이 같으면 더 볼 것이 없다고 판단한다.

    페이지 원문은 여기서 남긴다. 순회 밖으로 빼면 페이지를 들고 나가야 하고,
    도중에 실패했을 때 이미 받아둔 원문까지 잃는다.

    적재는 원문까지만 맡는다. 지점 적재(put_stores)는 브랜드 flow가 한다.
    브랜드마다 게시판이 여럿일 수 있어 합친 뒤에 한 번만 올려야 하기 때문이다.
    """
    logger = get_run_logger()

    collected: dict[str, CollectedStore] = {}
    previous_ids: set[str] = set()
    expected: int | None = None

    for page in range(1, max_pages + 1):
        if page > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)

        html = fetch_page(base_url, board_code, page, referer=referer)

        if persist:
            put_raw(html, platform=platform, name=f"page-{page:03d}.html")

        if page == 1:
            expected = total_count(html)

        stores = parse_stores(html, platform=platform)

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

    if expected is not None and expected != len(stores):
        # 순회가 덜 끝났거나 사이트가 중간에 목록을 바꾼 것이다. 건수가 맞지
        # 않는다는 사실은 여기 말고는 드러나는 곳이 없다.
        logger.warning(
            "사이트가 알려준 %d건과 수집한 %d건이 다릅니다.", expected, len(stores)
        )

    return stores
