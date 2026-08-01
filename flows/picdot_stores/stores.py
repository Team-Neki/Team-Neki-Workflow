"""픽닷 지점 목록 수집.

픽닷 사이트에는 지점 데이터가 없다. 매장 찾기가 Kakao 장소검색을 실시간으로
호출하고 결과를 그리기만 한다. 그래서 같은 데이터원을 서버에서 직접 부른다.
"""

import os
from dataclasses import dataclass
from typing import Any

import httpx
from prefect import get_run_logger, task

from flows.common.platform import Platform

SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
QUERY = "픽닷"

API_KEY_ENV = "KAKAO_API_KEY"

# Local API 사양. size는 최대 15, 한 질의로 노출되는 문서는 최대 45건이다.
PAGE_SIZE = 15
MAX_EXPOSED = 45


@dataclass(frozen=True)
class Store:
    """지점 하나.

    idx는 Kakao가 부여한 장소 id다. 중복 제거에 쓴다.

    address는 도로명을 우선하고 없으면 지번으로 채운다. jibun_address는 지번을
    그대로 둔다. kakao_place_url은 카카오맵 상세 링크이며, 영업시간이나 평점처럼
    검색 API가 주지 않는 정보를 나중에 붙일 때 진입점이 된다.
    """

    idx: str
    name: str
    address: str | None
    jibun_address: str | None
    phone: str | None
    longitude: float | None
    latitude: float | None
    kakao_place_url: str | None
    platform: Platform = Platform.PICDOT


def _api_key() -> str:
    key = os.environ.get(API_KEY_ENV)
    if not key:
        raise RuntimeError(
            f"{API_KEY_ENV} 환경변수가 없습니다. Kakao Developers에서 REST API 키를 "
            "발급받아 .env 또는 배포 환경에 넣으세요."
        )
    return key


def _coordinate(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_store(document: dict[str, Any]) -> Store | None:
    """장소 문서 하나를 Store로 옮긴다. 이름이나 id가 없으면 버린다."""
    idx = document.get("id")
    name = (document.get("place_name") or "").strip()

    if not idx or not name:
        return None

    jibun = (document.get("address_name") or "").strip()
    # 도로명 주소가 비어 있는 장소가 있어 지번 주소로 대체한다.
    address = (document.get("road_address_name") or "").strip() or jibun

    return Store(
        idx=str(idx),
        name=name,
        address=address or None,
        jibun_address=jibun or None,
        phone=(document.get("phone") or "").strip() or None,
        # Kakao는 x가 경도, y가 위도다.
        longitude=_coordinate(document.get("x")),
        latitude=_coordinate(document.get("y")),
        kakao_place_url=(document.get("place_url") or "").strip() or None,
    )


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def search_page(page: int, *, query: str = QUERY, timeout: float = 20.0) -> dict[str, Any]:
    """장소검색 한 페이지를 받아온다.

    키가 로그에 남지 않도록 헤더로만 넘긴다.
    """
    response = httpx.get(
        SEARCH_URL,
        params={"query": query, "page": page, "size": PAGE_SIZE},
        headers={"Authorization": f"KakaoAK {_api_key()}"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


@task
def parse_stores(payload: dict[str, Any]) -> list[Store]:
    """응답에서 지점을 추출한다."""
    logger = get_run_logger()

    documents = payload.get("documents") or []
    stores = [store for store in map(to_store, documents) if store is not None]

    dropped = len(documents) - len(stores)
    if dropped:
        logger.warning("id나 이름이 없는 문서 %d건을 건너뛰었습니다.", dropped)

    return stores
