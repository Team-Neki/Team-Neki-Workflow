"""enrich 의 순수 판정만 확인한다. Kakao 와 DB 는 부르지 않는다."""

import threading
from dataclasses import replace
from datetime import date, datetime

import pytest

from flows.stores_enrich import region
from flows.stores_enrich.name import unify_brand
from flows.stores_enrich.region import (
    EnrichedStore,
    from_collect,
    from_manual,
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
        source_type="COLLECTED",
        b_code=None,
        sido_name=None,
        sgg_name=None,
        umd_name=None,
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


def test_from_collect_marks_collected():
    record = {
        "platform": "PHOTOISM",
        "idx": "1",
        "name": "n",
        "collected_at": "2026-09-24T19:23:36+00:00",
    }
    row = from_collect(record, source_dt=date(2026, 9, 25), enriched_at=NOW)
    assert row.source_type == "COLLECTED"


def test_from_manual_uses_brand_code_and_prefixed_idx():
    record = {
        "id": 7,
        "platform": "PHOTOISM",
        "branch_name": "포토이즘 박스 강남점",
        "address": "서울 강남구 역삼동 1",
        "phone": None,
        "longitude": 127.03,
        "latitude": 37.5,
        "updated_at": datetime(2026, 9, 20, 13, 0),
    }
    row = from_manual(record, source_dt=date(2026, 9, 25), enriched_at=NOW)
    assert (row.platform, row.idx) == ("PHOTOISM", "manual-7")
    assert row.source_type == "MANUAL"
    assert row.name == "포토이즘 강남점"
    assert row.coordinate_source == "manual"
    assert row.collected_at == datetime(2026, 9, 20, 13, 0)
    assert row.source_dt == date(2026, 9, 25)
    assert row.geocode_status == "failed"
    assert row.b_code is None


def test_manual_store_reuses_previous_b_code_when_coordinate_unchanged():
    manual = store(idx="manual-7", source_type="MANUAL", coordinate_source="manual")
    previous = replace(manual, b_code="1168010100", geocode_status="ok")
    result, error, lookup = resolve(manual, previous, stop=threading.Event())
    assert (result.b_code, result.geocode_status, error, lookup) == (
        "1168010100",
        "reused",
        None,
        "not_called",
    )


@pytest.mark.parametrize(
    "raw,unified",
    [
        ("포토이즘 박스 상록수역점", "포토이즘 상록수역점"),
        ("포토이즘박스 경기 양평점", "포토이즘 경기 양평점"),
        ("포토이즘  박스  강남점", "포토이즘 강남점"),
        ("포토이즘 강남점", "포토이즘 강남점"),
        ("포토이즘박스", "포토이즘"),
    ],
)
def test_unify_brand_photoism_prefix(raw, unified):
    assert unify_brand("PHOTOISM", raw) == unified


def test_unify_brand_leaves_other_brands_and_inner_text():
    assert unify_brand("LIFE_FOUR_CUT", "포토이즘 박스 강남점") == "포토이즘 박스 강남점"
    assert unify_brand("PHOTOISM", "강남 포토이즘 박스점") == "강남 포토이즘 박스점"


def test_from_collect_unifies_brand_in_name():
    record = {
        "platform": "PHOTOISM",
        "idx": "1",
        "name": "포토이즘박스 경기 양평점",
        "collected_at": "2026-09-24T19:23:36+00:00",
    }
    row = from_collect(record, source_dt=date(2026, 9, 25), enriched_at=NOW)
    assert row.name == "포토이즘 경기 양평점"


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
        sido_name="서울특별시",
        sgg_name="강남구",
        umd_name="역삼동",
        geocode_status="ok",
    )
    stop = threading.Event()
    stop.set()  # Kakao 를 부르면 안 된다. 불리면 stop 뒤라 빈 채로 돌아온다
    row, error, lookup_status = resolve(store(), previous, stop=stop)
    assert error is None
    assert lookup_status == "not_called"
    assert row.geocode_status == "reused"
    assert row.b_code == "1168010100"
    assert row.umd_name == "역삼동"


def test_resolve_marks_status_when_stopped():
    stop = threading.Event()
    stop.set()
    assert resolve(store(), None, stop=stop)[0].geocode_status == "failed"
    row, _, lookup_status = resolve(store(longitude=None, latitude=None), None, stop=stop)
    assert row.geocode_status == "no_coordinate"
    assert lookup_status == "not_called"


def _kakao_down(*args, **kwargs):
    raise RuntimeError("kakao down")


def test_resolve_keeps_fallback_coordinates_when_region_lookup_fails(monkeypatch):
    monkeypatch.setattr(
        region.geocode, "locate", lambda _store, **_kwargs: (127.1, 37.6, "kakao_address")
    )
    monkeypatch.setattr(region.kakao, "coord2regioncode", _kakao_down)
    monkeypatch.setattr(region.time, "sleep", lambda _seconds: None)
    row, error, lookup_status = resolve(store(longitude=None, latitude=None), None, stop=threading.Event())
    assert isinstance(error, RuntimeError)
    assert lookup_status == "failed"
    assert row.geocode_status == "failed"
    assert row.b_code is None
    assert (row.longitude, row.latitude, row.coordinate_source) == (127.1, 37.6, "kakao")


def test_next_failures_resets_only_when_kakao_answered():
    assert next_failures(2, lookup_status="failed") == 3
    assert next_failures(2, lookup_status="responded") == 0
    assert next_failures(2, lookup_status="not_called") == 2


def test_blank_input_does_not_call_kakao_or_reset_failure_counter(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("Kakao를 호출하면 안 됩니다")

    monkeypatch.setattr(region.kakao, "search_address", unexpected)
    monkeypatch.setattr(region.kakao, "search_keyword", unexpected)
    row, error, lookup_status = resolve(
        store(name=None, address=None, longitude=None, latitude=None),
        None, stop=threading.Event(),
    )
    assert row.geocode_status == "no_coordinate"
    assert error is None
    assert lookup_status == "not_called"
    assert next_failures(2, lookup_status=lookup_status) == 2


def test_actual_lookup_failure_is_counted(monkeypatch):
    monkeypatch.setattr(region.kakao, "search_address", _kakao_down)
    monkeypatch.setattr(region.geocode.time, "sleep", lambda _seconds: None)
    _, error, lookup_status = resolve(
        store(longitude=None, latitude=None), None, stop=threading.Event()
    )
    assert isinstance(error, RuntimeError)
    assert lookup_status == "failed"
    assert next_failures(2, lookup_status=lookup_status) == 3


def test_input_error_after_empty_response_is_not_a_kakao_failure(monkeypatch):
    monkeypatch.setattr(region.kakao, "search_address", lambda *_args, **_kwargs: [])
    _, error, lookup_status = resolve(
        store(name=object(), longitude=None, latitude=None), None, stop=threading.Event()
    )
    assert isinstance(error, AttributeError)
    assert lookup_status == "responded"
    assert next_failures(2, lookup_status=lookup_status) == 0


class Logger:
    def __init__(self):
        self.errors = []

    def warning(self, *args):
        pass

    def error(self, *args):
        self.errors.append(args)


def test_three_input_errors_do_not_stop_later_valid_lookup(monkeypatch):
    logger = Logger()
    calls = []
    monkeypatch.setattr(region, "get_run_logger", lambda: logger)
    monkeypatch.setattr(region, "WORKERS", 1)
    monkeypatch.setenv(region.kakao.API_KEY_ENV, "test-only")
    monkeypatch.setattr(region.kakao, "coord2regioncode", lambda *_args, **_kwargs: calls.append(1))
    invalid = [store(idx=str(i), name=object(), address=None, longitude=None, latitude=None) for i in range(3)]
    result = region.enrich_stores.fn([*invalid, store(idx="valid")], {})
    assert len(result) == 4
    assert calls == [1]
    assert not logger.errors


def test_give_up_log_survives_a_late_inflight_success(monkeypatch):
    logger = Logger()
    monkeypatch.setattr(region, "get_run_logger", lambda: logger)
    monkeypatch.setenv(region.kakao.API_KEY_ENV, "test-only")

    class OrderedPool:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def map(self, work, stores):
            return map(work, stores)

    def scripted_resolve(row, previous, *, stop):
        if row.idx == "late-success":
            # 다른 worker에서 진행 중이던 응답이 포기 이후 도착하는 상황.
            assert stop.is_set()
            return row, None, "responded"
        return row, RuntimeError("kakao down"), "failed"

    monkeypatch.setattr(region, "ThreadPoolExecutor", OrderedPool)
    monkeypatch.setattr(region, "resolve", scripted_resolve)
    region.enrich_stores.fn([*[store(idx=str(i)) for i in range(3)], store(idx="late-success")], {})
    assert len(logger.errors) == 1


def test_missing_api_key_is_not_logged_as_give_up(monkeypatch):
    logger = Logger()
    monkeypatch.setattr(region, "get_run_logger", lambda: logger)
    monkeypatch.delenv(region.kakao.API_KEY_ENV, raising=False)
    region.enrich_stores.fn([store()], {})
    assert not logger.errors


def test_mismatched_compares_first_token_of_sigungu():
    assert not mismatched(store(sgg_name="강남구"))
    assert mismatched(store(sgg_name="서초구"))
    assert not mismatched(
        store(address="경기 수원시 영통구 1", sgg_name="수원시 영통구")
    )
    assert not mismatched(store(address=None, sgg_name="강남구"))
    assert not mismatched(store(sgg_name=None))
