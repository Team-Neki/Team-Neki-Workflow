"""플랜비스튜디오 지점 목록 수집.

sub4.html에는 지점 데이터가 없고 iframe으로 그누보드 게시판을 불러온다.
그래서 iframe 대상 URL을 직접 호출한다.

한 페이지 안에 데이터가 두 곳으로 나뉘어 있다. 목록(#gallery)에는 전체
지점이 페이지 단위로 담기고, 지도(positions 배열)에는 일부 지점의 좌표만
담긴다. 목록이 기준이고 좌표는 있는 것만 채운다.
"""

import re
from dataclasses import dataclass

import httpx
from prefect import get_run_logger, task

from flows.common.platform import Platform

STORE_URL = "http://planbstudio.co.kr/muse/bbs/board.php"
BO_TABLE = "store"

# 목록 영역의 시작점. 이 앞은 지도 스크립트라 항목 추출에서 제외해야 한다.
GALLERY_MARKER = '<div id="gallery">'

_ENTRY = re.compile(r'wr_id=(\d+)&amp;page=\d+">([^<]+)</a>')
_ADDRESS = re.compile(r"주소\s*:\s*([^<]*)<br>")
_PHONE = re.compile(r"TEL\s*:\s*([^<]*)")
_COORD = re.compile(
    r'wr_id=(\d+)"[^}]*?latlng: new daum\.maps\.LatLng\(([\d.]+), ([\d.]+)\)',
    re.S,
)


@dataclass(frozen=True)
class Store:
    """지점 하나.

    좌표는 지도에 표시되는 일부 지점에만 있다. 전화는 필드가 있으나 대부분
    비어 있다.
    """

    idx: str
    name: str
    address: str | None
    phone: str | None
    longitude: float | None
    latitude: float | None
    platform: Platform = Platform.PLANB_STUDIO


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


def extract_coordinates(html: str) -> dict[str, tuple[float, float]]:
    """지도 스크립트에서 wr_id별 좌표를 뽑는다.

    이 배열은 페이지를 바꿔도 항상 같다. 첫 페이지에서 한 번만 부르면 된다.
    """
    coordinates: dict[str, tuple[float, float]] = {}

    for idx, latitude, longitude in _COORD.findall(html):
        # LatLng(위도, 경도) 순서다. 반환은 (경도, 위도)로 맞춘다.
        coordinates[idx] = (float(longitude), float(latitude))

    return coordinates


def extract_stores(
    html: str, coordinates: dict[str, tuple[float, float]] | None = None
) -> list[Store]:
    """목록 영역에서 지점을 추출하고 좌표를 채운다.

    Prefect에 의존하지 않는 순수 함수다. 목록 영역 앞의 지도 스크립트에도
    wr_id가 있으므로 반드시 #gallery 이후만 본다.
    """
    marker = html.find(GALLERY_MARKER)
    if marker < 0:
        return []

    segment = html[marker:]
    coordinates = coordinates or {}

    entries = _ENTRY.findall(segment)
    addresses = _ADDRESS.findall(segment)
    phones = _PHONE.findall(segment)

    stores: list[Store] = []
    for position, (idx, name) in enumerate(entries):
        longitude, latitude = coordinates.get(idx, (None, None))

        stores.append(
            Store(
                idx=idx,
                name=name.strip(),
                address=_clean(addresses[position]) if position < len(addresses) else None,
                phone=_clean(phones[position]) if position < len(phones) else None,
                longitude=longitude,
                latitude=latitude,
            )
        )

    return stores


def _clean(value: str) -> str | None:
    cleaned = value.replace("\xa0", " ").strip()
    return cleaned or None


@task
def parse_coordinates(html: str) -> dict[str, tuple[float, float]]:
    """extract_coordinates를 감싸고 개수를 남긴다."""
    coordinates = extract_coordinates(html)
    get_run_logger().info("좌표를 가진 지점 %d개", len(coordinates))
    return coordinates


@task
def parse_stores(
    html: str, coordinates: dict[str, tuple[float, float]]
) -> list[Store]:
    """extract_stores를 감싼다."""
    return extract_stores(html, coordinates)
