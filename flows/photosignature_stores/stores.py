"""포토시그니처 지점 목록 수집.

지점 데이터가 목록 HTML이 아니라 Kakao 지도 마커를 생성하는 JavaScript 안에
들어 있다. 그래서 DOM을 훑지 않고 마커 블록을 잘라 값을 뽑는다.
"""

import re
from dataclasses import dataclass

import httpx
from prefect import get_run_logger, task

from flows.common.platform import Platform

STORE_URL = "https://photosignature.co.kr/bbs/board.php"
BO_TABLE = "store"

# 마커 하나가 지점 하나다. 이 문자열로 잘라 블록 단위로 처리한다.
MARKER_SPLIT = "markerPosition = new kakao.maps.LatLng("

_COORD = re.compile(r"([-\d.]+),\s*([-\d.]+)\)")
_NAME = re.compile(r'class="titles cut80"><a[^>]*>([^<]*)</a>')
_ADDRESS = re.compile(r'class="sub1 cut90">([^<]*)</div>')
_IDX = re.compile(r"wr_id=(\d+)")


@dataclass(frozen=True)
class Store:
    """지점 하나.

    포토시그니처는 지점별 전화번호를 노출하지 않는다. 문서 전체에 대표번호
    하나만 있어 phone 필드를 두지 않았다.
    """

    idx: str
    name: str
    address: str | None
    longitude: float | None
    latitude: float | None
    platform: Platform = Platform.PHOTO_SIGNATURE


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def fetch_store_page(*, timeout: float = 30.0) -> str:
    """매장 페이지를 받아온다.

    page 파라미터는 넘기지 않는다. 값을 바꿔도 같은 전량이 돌아오므로
    순회할 이유가 없다.
    """
    response = httpx.get(
        STORE_URL,
        params={"bo_table": BO_TABLE},
        timeout=timeout,
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def extract_stores(html: str) -> tuple[list[Store], int]:
    """마커 블록에서 지점을 추출한다. Prefect에 의존하지 않는 순수 함수다.

    이름이 없는 블록은 지점이 아니라 지도 중심 설정(map.setCenter)이므로
    건너뛴다. 건너뛴 개수를 함께 돌려준다.
    """
    stores: list[Store] = []
    skipped = 0

    for block in html.split(MARKER_SPLIT)[1:]:
        name = _NAME.search(block)
        idx = _IDX.search(block)

        if not (name and name.group(1).strip() and idx):
            skipped += 1
            continue

        coord = _COORD.match(block)
        address = _ADDRESS.search(block)

        stores.append(
            Store(
                idx=idx.group(1),
                name=name.group(1).strip(),
                address=address.group(1).strip() or None if address else None,
                # LatLng(위도, 경도) 순서다. 경도가 두 번째임에 주의한다.
                longitude=float(coord.group(2)) if coord else None,
                latitude=float(coord.group(1)) if coord else None,
            )
        )

    return stores, skipped


@task
def parse_stores(html: str) -> list[Store]:
    """extract_stores를 감싸고 건너뛴 블록 수를 남긴다."""
    stores, skipped = extract_stores(html)

    if skipped:
        get_run_logger().info(
            "지점이 아닌 마커 %d개를 건너뛰었습니다. 지도 중심 설정으로 보입니다.",
            skipped,
        )

    return stores
