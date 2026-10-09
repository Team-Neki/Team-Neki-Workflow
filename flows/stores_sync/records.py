"""수집 지점의 원천 키와 조회 가능한 입력을 판정한다."""

import math
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime

from flows.common.platform import Platform

SOURCE_TYPES = ("COLLECTED", "MANUAL")


@dataclass(frozen=True)
class SourceStore:
    platform: str
    idx: str
    name: str
    address: str | None
    longitude: float | None
    latitude: float | None
    source_dt: date
    collected_at: datetime
    b_code: str | None
    # COLLECTED 수집 지점 / MANUAL 관리자 등록 지점 (enrich 가 tb_photo_booth_manual 에서 담음)
    source_type: str = "COLLECTED"

    @property
    def key(self) -> tuple[str, str]:
        return self.platform, self.idx


def prepare(
    stores: list[SourceStore], *, cycle: date, min_expected: int
) -> tuple[list[SourceStore], dict[str, int]]:
    """키가 모호하면 실패한다. 조회할 수 없는 행만 제외하며 원문은 변경하지 않는다."""
    if (
        isinstance(min_expected, bool)
        or not isinstance(min_expected, int)
        or min_expected < 1
    ):
        raise ValueError("min_expected는 양의 정수여야 합니다.")
    seen: set[tuple[str, str]] = set()
    skipped: Counter[str] = Counter()
    eligible: list[SourceStore] = []
    for row in stores:
        if row.source_type not in SOURCE_TYPES:
            raise ValueError(f"잘못된 source_type: {row.key} {row.source_type}")
        # 관리자 등록 지점은 수집하지 않는 브랜드일 수 있어 Platform 대신 tb_brand.code 로 검사한다
        if (
            not row.platform
            or (row.source_type == "COLLECTED" and row.platform not in Platform)
            or not row.idx.strip()
            or len(row.idx) > 64
        ):
            raise ValueError(f"잘못된 원천 키: {row.key}")
        if row.key in seen:
            raise ValueError(f"중복 원천 키: {row.key}")
        seen.add(row.key)
        if not isinstance(row.source_dt, date) or isinstance(row.source_dt, datetime):
            raise TypeError(f"잘못된 수집 사이클: {row.key}")
        if (
            not isinstance(row.collected_at, datetime)
            or row.collected_at.tzinfo is not None
        ):
            raise ValueError(f"collected_at은 KST 벽시계여야 합니다: {row.key}")
        if row.b_code is not None and (
            len(row.b_code) != 10 or not row.b_code.isdecimal()
        ):
            raise ValueError(f"잘못된 법정동 코드: {row.key}")
        if row.source_dt > cycle:
            skipped["future_cycle"] += 1
        else:
            eligible.append(row)
    # 하한은 수집 지점만 센다. 관리자 등록 지점이 수집 장애를 가리면 안 된다 (enrich 와 같은 기준)
    collected = sum(1 for row in eligible if row.source_type == "COLLECTED")
    if collected < min_expected:
        raise ValueError(
            f"동기화 입력(수집 지점) {collected}건이 하한 {min_expected}건 미만입니다."
        )

    ready: list[SourceStore] = []
    for row in eligible:
        if not row.name or not row.name.strip() or len(row.name) > 255:
            skipped["invalid_name"] += 1
        elif not row.address or not row.address.strip() or len(row.address) > 255:
            skipped["invalid_address"] += 1
        elif row.longitude is None or row.latitude is None:
            skipped["no_coordinate"] += 1
        elif not (
            math.isfinite(row.longitude)
            and -180 <= row.longitude <= 180
            and math.isfinite(row.latitude)
            and -90 <= row.latitude <= 90
        ):
            skipped["invalid_coordinate"] += 1
        else:
            ready.append(row)
    if not ready:
        raise ValueError(
            "적재 가능한 지점이 없습니다. 지점 마스터를 변경하지 않습니다."
        )
    return ready, dict(skipped)
