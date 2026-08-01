"""수집 결과를 로그로 남기는 공용 헬퍼.

브랜드마다 Store 정의가 다르므로 특정 타입에 의존하지 않는다. dataclass면
필드를 그대로 읽고, 없는 필드는 건너뛴다.
"""

from dataclasses import asdict, is_dataclass
from typing import Any

from prefect import get_run_logger

# 출력 순서. 브랜드마다 Store 필드가 달라 없는 것은 자동으로 빠진다.
FIELD_ORDER = (
    "platform",
    "idx",
    "name",
    "address",
    "jibun_address",
    "phone",
    "longitude",
    "latitude",
    "kakao_place_url",
)


def _as_dict(store: Any) -> dict[str, Any]:
    if is_dataclass(store):
        return asdict(store)
    if isinstance(store, dict):
        return store
    return vars(store)


def _format(store: Any) -> str:
    fields = _as_dict(store)
    parts = [str(fields[key]) if fields.get(key) is not None else "-"
             for key in FIELD_ORDER if key in fields]
    return " | ".join(parts)


def log_stores(stores: list[Any], *, label: str) -> None:
    """수집한 지점을 한 줄씩 로그로 남긴다."""
    logger = get_run_logger()

    logger.info("%s 수집 결과 %d건", label, len(stores))
    for position, store in enumerate(stores, start=1):
        logger.info("  %3d. %s", position, _format(store))
