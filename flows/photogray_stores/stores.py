"""포토그레이 지점 목록 수집.

그누보드 게시판이고 지점 데이터가 목록 HTML에 그대로 있다. 인생네컷처럼 AJAX
엔드포인트를 따로 찾을 필요가 없어 목록 URL을 그대로 부른다.

해외 지점 10곳은 게시판 밖에 하드코딩된 별도 블록(div.overseas_store)에 있고
여기서 담지 않는다. 국내 좌표계를 전제로 하는 후속 단계와 맞지 않는데, 담으려면
국내/해외 구분 필드가 필요하고 그것은 CollectedStore 스키마 변경이라 기존 브랜드
전체에 영향이 간다. 항목 선택자를 div.store_list 아래로 한정해 걸러낸다.
"""

import re
from typing import Any

import httpx
from bs4 import BeautifulSoup
from prefect import get_run_logger, task

from flows.common.platform import Platform
from flows.common.store import CollectedStore

STORE_URL = "https://photogray.com/photodb/board.php"
BO_TABLE = "store"

# 목록 상단의 "총 249건". 수집 건수를 대조할 유일한 근거다.
_TOTAL = re.compile(r"(\d[\d,]*)")


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def fetch_store_page(page: int, *, timeout: float = 25.0) -> str:
    """게시판 한 페이지를 받아온다."""
    response = httpx.get(
        STORE_URL,
        params={"bo_table": BO_TABLE, "page": page},
        timeout=timeout,
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def _text(node: Any) -> str | None:
    if node is None:
        return None
    value = node.get_text(strip=True)
    return value or None


def _phone(container: Any) -> str | None:
    """.binfo의 두 번째 <p>에서 전화번호를 꺼낸다.

    이 자리는 주소의 둘째 줄이 아니라 전화번호 슬롯이고 대부분 비어 있다.
    .binfo p 텍스트를 이어 붙여 주소로 쓰면 번호가 들어 있는 지점의 주소가
    오염된다. 그래서 주소는 data-addr에서, 전화는 여기서 따로 읽는다.
    """
    lines = container.select(".binfo p")
    if len(lines) < 2:
        return None
    return _text(lines[1])


def extract_total_count(html: str) -> int | None:
    """목록 상단이 알려주는 전체 건수를 뽑는다. 순수 함수다."""
    text = _text(BeautifulSoup(html, "lxml").select_one("div.total_count > span"))
    if not text:
        return None

    matched = _TOTAL.search(text)
    if matched is None:
        return None

    return int(matched.group(1).replace(",", ""))


def extract_stores(html: str) -> tuple[list[CollectedStore], list[str]]:
    """목록에서 지점을 추출한다. Prefect에 의존하지 않는 순수 함수다.

    지점명이 없는 항목은 건너뛰고 그 주소를 함께 돌려준다. 한 항목의 결함으로
    페이지 전체를 잃지 않기 위함이다.

    idx로 쓸 사이트 식별자가 없다. 마크업에 wr_id가 노출되지 않고 항목에 붙은
    속성은 data-addr뿐이라 지점명을 그대로 idx로 쓴다. 순번은 정렬이 바뀌면
    다른 지점을 가리키게 되어 더 나쁘다. 지점명이 개정되면 다음 단계에는
    삭제+신규로 보이지만, 사이트가 식별자를 주지 않으므로 대안이 없다.

    좌표는 사이트에 없다. 지도는 브라우저가 주소로 그리므로 None으로 둔다.
    """
    soup = BeautifulSoup(html, "lxml")

    stores: list[CollectedStore] = []
    skipped: list[str] = []

    for container in soup.select("div.store_list li > div.subject"):
        name = _text(container.select_one(".btitle"))

        if not name:
            skipped.append(container.get("data-addr") or "(주소 없음)")
            continue

        stores.append(
            CollectedStore(
                platform=Platform.PHOTO_GRAY,
                idx=name,
                name=name,
                address=(container.get("data-addr") or "").strip() or None,
                phone=_phone(container),
            )
        )

    return stores, skipped


@task
def parse_total_count(html: str) -> int | None:
    """extract_total_count를 감싸고 값을 못 찾으면 남긴다."""
    total = extract_total_count(html)

    if total is None:
        get_run_logger().warning(
            "목록 상단의 전체 건수를 찾지 못했습니다. 수집 건수를 대조할 수 없습니다."
        )

    return total


@task
def parse_stores(html: str) -> list[CollectedStore]:
    """extract_stores를 감싸고 건너뛴 항목을 남긴다."""
    stores, skipped = extract_stores(html)

    if skipped:
        get_run_logger().warning(
            "지점명이 없어 건너뛴 항목 %d건: %s", len(skipped), skipped
        )

    return stores
