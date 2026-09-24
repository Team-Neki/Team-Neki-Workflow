"""enrich 의 순수 판정만 확인한다. Kakao 와 DB 는 부르지 않는다."""

import threading
from datetime import date, datetime

from flows.stores_enrich.region import (
    EnrichedStore,
    from_collect,
    mismatched,
    resolve,
    reusable,
)

NOW = datetime(2026, 9, 25, 5, 0)


def store(**overrides) -> EnrichedStore:
    base = dict(
        platform="PHOTOISM",
        idx="1",
        name="포토이즘 역삼점",
        address="서울 강남구 역삼동 1",
        phone=None,
        longitude=127.03,
        latitude=37.5,
        coordinate_source="official",
        collected_at=NOW,
        source_dt=date(2026, 9, 25),
        b_code=None,
        region_1depth_name=None,
        region_2depth_name=None,
        region_3depth_name=None,
        geocode_status="failed",
        enriched_at=NOW,
    )
    return EnrichedStore(**{**base, **overrides})


def test_from_collect_strips_timezone_to_kst():
    record = {
        "platform": "PHOTOISM",
        "idx": "1",
        "name": "n",
        "address": None,
        "phone": None,
        "longitude": 127.0,
        "latitude": 37.0,
        "coordinate_source": None,
        "collected_at": "2026-09-24T19:23:36+00:00",
    }
    row = from_collect(record, source_dt=date(2026, 9, 25), enriched_at=NOW)
    assert row.collected_at == datetime(2026, 9, 25, 4, 23, 36)
    assert row.collected_at.tzinfo is None
    assert row.geocode_status == "failed"
    assert row.b_code is None


def test_reusable_only_when_same_coordinates_and_previous_has_code():
    previous = store(b_code="1168010100", geocode_status="ok")
    assert reusable(store(), previous)
    assert not reusable(store(longitude=127.04), previous)
    assert not reusable(store(), store(b_code=None, geocode_status="failed"))
    assert not reusable(store(longitude=None, latitude=None), previous)
    assert not reusable(store(), None)


def test_resolve_reuses_without_kakao():
    previous = store(
        b_code="1168010100",
        region_1depth_name="서울특별시",
        region_2depth_name="강남구",
        region_3depth_name="역삼동",
        geocode_status="ok",
    )
    stop = threading.Event()
    stop.set()  # Kakao 를 부르면 안 된다. 불리면 stop 뒤라 빈 채로 돌아온다
    row = resolve(store(), previous, stop=stop)
    assert row.geocode_status == "reused"
    assert row.b_code == "1168010100"
    assert row.region_3depth_name == "역삼동"


def test_resolve_marks_status_when_stopped():
    stop = threading.Event()
    stop.set()
    assert resolve(store(), None, stop=stop).geocode_status == "failed"
    assert (
        resolve(store(longitude=None, latitude=None), None, stop=stop).geocode_status
        == "no_coordinate"
    )


def test_mismatched_compares_first_token_of_sigungu():
    assert not mismatched(store(region_2depth_name="강남구"))
    assert mismatched(store(region_2depth_name="서초구"))
    assert not mismatched(
        store(address="경기 수원시 영통구 1", region_2depth_name="수원시 영통구")
    )
    assert not mismatched(store(address=None, region_2depth_name="강남구"))
    assert not mismatched(store(region_2depth_name=None))
