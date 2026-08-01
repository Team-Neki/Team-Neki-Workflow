"""모노맨션 지점 수집.

픽닷과 같은 수집원(Kakao 장소검색)이지만 규모가 다르다. 지점이 104곳이라
한 질의로 꺼낼 수 있는 45건을 넘어서므로 좌표 사각형을 쪼개 받는다. 분할은
flows.common.kakao 가 맡고 이 모듈은 문서를 Store 로 옮기기만 한다.
"""

from typing import Any

from prefect import get_run_logger, task

from flows.common.platform import Platform
from flows.common.store import CollectedStore

QUERY = "모노맨션"


def _coordinate(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_store(document: dict[str, Any]) -> CollectedStore | None:
    """장소 문서 하나를 Store로 옮긴다. 이름이나 id가 없으면 버린다."""
    idx = document.get("id")
    name = (document.get("place_name") or "").strip()

    if not idx or not name:
        return None

    # 도로명 주소가 비어 있는 장소가 있어 지번 주소로 대체한다.
    jibun = (document.get("address_name") or "").strip()
    address = (document.get("road_address_name") or "").strip() or jibun

    return CollectedStore(
        platform=Platform.MONO_MANSION,
        idx=str(idx),
        name=name,
        address=address or None,
        phone=(document.get("phone") or "").strip() or None,
        # Kakao는 x가 경도, y가 위도다.
        longitude=_coordinate(document.get("x")),
        latitude=_coordinate(document.get("y")),
    )


@task
def parse_stores(documents: list[dict[str, Any]]) -> list[CollectedStore]:
    """문서 목록에서 지점을 추출한다."""
    logger = get_run_logger()

    stores = [store for store in map(to_store, documents) if store is not None]

    dropped = len(documents) - len(stores)
    if dropped:
        logger.warning("id나 이름이 없는 문서 %d건을 건너뛰었습니다.", dropped)

    return stores
