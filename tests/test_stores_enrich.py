"""enrich 의 순수 판정만 확인한다. Kakao 와 DB 는 부르지 않는다."""

import threading
from datetime import date, datetime

from flows.stores_enrich import region
from flows.stores_enrich.region import (
    EnrichedStore,
    from_collect,
    mismatched,
    next_failures,
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
    row, error = resolve(store(), previous, stop=stop)
    assert error is None
    assert row.geocode_status == "reused"
    assert row.b_code == "1168010100"
    assert row.region_3depth_name == "역삼동"


def test_resolve_marks_status_when_stopped():
    stop = threading.Event()
    stop.set()
    assert resolve(store(), None, stop=stop)[0].geocode_status == "failed"
    row, _ = resolve(store(longitude=None, latitude=None), None, stop=stop)
    assert row.geocode_status == "no_coordinate"


def _kakao_down(*args, **kwargs):
    raise RuntimeError("kakao down")


def test_resolve_keeps_fallback_coordinates_when_region_lookup_fails(monkeypatch):
    monkeypatch.setattr(
        region.geocode, "locate", lambda _store: (127.1, 37.6, "kakao_address")
    )
    monkeypatch.setattr(region.kakao, "coord2regioncode", _kakao_down)
    monkeypatch.setattr(region.time, "sleep", lambda _seconds: None)
    row, error = resolve(store(longitude=None, latitude=None), None, stop=threading.Event())
    assert isinstance(error, RuntimeError)
    assert row.geocode_status == "failed"
    assert row.b_code is None
    assert (row.longitude, row.latitude, row.coordinate_source) == (127.1, 37.6, "kakao")


def test_next_failures_resets_only_when_kakao_answered():
    boom = RuntimeError("kakao down")
    assert next_failures(2, error=boom, status="failed", skipped=False) == 3
    # 답은 왔지만 법정동 문서가 없거나 주소검색이 0건. Kakao 는 살아 있다
    assert next_failures(2, error=None, status="failed", skipped=False) == 0
    assert next_failures(2, error=None, status="no_coordinate", skipped=False) == 0
    assert next_failures(2, error=None, status="ok", skipped=False) == 0
    # Kakao 를 부르지 않은 지점은 근거가 못 된다
    assert next_failures(2, error=None, status="reused", skipped=False) == 2
    assert next_failures(2, error=None, status="failed", skipped=True) == 2


def test_mismatched_compares_first_token_of_sigungu():
    assert not mismatched(store(region_2depth_name="강남구"))
    assert mismatched(store(region_2depth_name="서초구"))
    assert not mismatched(
        store(address="경기 수원시 영통구 1", region_2depth_name="수원시 영통구")
    )
    assert not mismatched(store(address=None, region_2depth_name="강남구"))
    assert not mismatched(store(region_2depth_name=None))
