"""지점 원천 키를 안정적인 locationId로 동기화한다. BACKEND-153."""

from datetime import date
from typing import Any

from prefect import flow, get_run_logger

from flows.common.manifest import target_date as cycle_date
from flows.stores_sync.table import MIN_EXPECTED, sync_locations


@flow(name="stores-sync", log_prints=True)
def stores_sync(
    target_date: date | None = None,
    persist: bool = True,
    min_expected: int = MIN_EXPECTED,
) -> dict[str, Any]:
    """enrich 현재 세대를 읽는다. persist=False는 DB 쓰기 없이 예상 건수를 반환한다."""
    if (
        isinstance(min_expected, bool)
        or not isinstance(min_expected, int)
        or min_expected < 1
    ):
        raise ValueError("min_expected는 양의 정수여야 합니다.")
    cycle = target_date or cycle_date()
    result = sync_locations(cycle, persist=persist, min_expected=min_expected)
    get_run_logger().info(
        "지점 동기화 (cycle=%s, persist=%s): %s", cycle, persist, result
    )
    return {"cycle": cycle.isoformat(), "persisted": persist, **result}
