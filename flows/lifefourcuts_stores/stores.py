"""인생네컷 지점 목록 수집.

목록 페이지 HTML에는 지점 데이터가 없고 AJAX 엔드포인트가 HTML 조각을 돌려준다.
그래서 페이지를 그대로 긁지 않고 해당 엔드포인트를 직접 호출한다.
"""

from dataclasses import dataclass
from typing import Any

import httpx
from bs4 import BeautifulSoup
from prefect import get_run_logger, task

from flows.common.platform import Platform

LIST_URL = "https://lifefourcuts.com/ajax/get_map_list.cm"
BOARD_CODE = "b20210114da9a94d63009f"
REFERER = "https://lifefourcuts.com/Store01/"

# 페이지당 항목 수. 이보다 적게 오면 마지막 페이지다.
PAGE_SIZE = 10


@dataclass(frozen=True)
class Store:
    """지점 하나.

    idx는 사이트가 부여한 식별자다. 수집 대상 필드는 아니지만 페이지 경계에서
    중복을 걸러내고 종료를 판정하는 데 쓰인다.
    """

    idx: str
    name: str
    address: str | None
    phone: str | None
    longitude: float | None
    latitude: float | None
    platform: Platform = Platform.LIFE_FOUR_CUT


def _text(node: Any) -> str | None:
    if node is None:
        return None
    value = node.get_text(strip=True)
    return value or None


def _phone(container: Any) -> str | None:
    """tel: 링크에서 번호를 꺼낸다.

    링크 본문에는 스크린리더용 <span class="sr-only">phone number</span>가
    붙어 있어 텍스트를 그대로 쓰면 번호 뒤에 딸려온다.
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
def fetch_store_page(page: int, *, timeout: float = 20.0) -> str:
    """지점 목록 한 페이지를 HTML 조각으로 받아온다."""
    response = httpx.post(
        LIST_URL,
        data={
            "board_code": BOARD_CODE,
            "search": "",
            "search_mod": "all",
            "page": page,
            "sort": "STREET",
            "status": "",
        },
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": REFERER,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def extract_stores(html: str) -> tuple[list[Store], list[str]]:
    """HTML 조각에서 지점을 추출한다. Prefect에 의존하지 않는 순수 함수다.

    지점명이 없는 항목은 건너뛰고 그 id를 함께 돌려준다. 나머지 필드는 없으면
    None으로 둔다. 한 항목의 결함으로 페이지 전체를 잃지 않기 위함이다.
    """
    soup = BeautifulSoup(html, "lxml")

    stores: list[Store] = []
    skipped: list[str] = []

    for container in soup.select("div.map_container[id^='list_']"):
        idx = container.get("id", "").removeprefix("list_")
        name = _text(container.select_one(".tit"))

        if not idx or not name:
            skipped.append(container.get("id") or "(id 없음)")
            continue

        stores.append(
            Store(
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


@task
def parse_stores(html: str) -> list[Store]:
    """extract_stores를 감싸고 건너뛴 항목을 남긴다."""
    stores, skipped = extract_stores(html)

    if skipped:
        get_run_logger().warning(
            "이름이나 식별자가 없어 건너뛴 항목 %d건: %s", len(skipped), skipped
        )

    return stores
