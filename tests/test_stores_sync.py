"""입력 판정과 flow 조립을 확인한다. 외부 DB와 Prefect 서버는 부르지 않는다."""

from datetime import UTC, date, datetime

import pytest

from deployments.stores_sync import build
from flows.stores_sync import flow as flow_module
from flows.stores_sync.records import SourceStore, prepare

CYCLE = date(2026, 9, 27)
NOW = datetime(2026, 9, 27, 4, 0, tzinfo=UTC).replace(tzinfo=None)


def store(**overrides):
    return SourceStore(
        **{
            "platform": "PHOTOISM",
            "idx": "official-1",
            "name": "포토이즘 강남점",
            "address": "서울 강남구 역삼동 1",
            "longitude": 127.03,
            "latitude": 37.5,
            "source_dt": CYCLE,
            "collected_at": NOW,
            "b_code": "1168010100",
            **overrides,
        }
    )


def test_same_idx_in_different_platforms_are_distinct_and_names_remain_raw():
    rows, skipped = prepare(
        [store(), store(platform="LIFE_FOUR_CUT")], cycle=CYCLE, min_expected=2
    )
    assert len(rows) == 2
    assert rows[0].name == "포토이즘 강남점"
    assert not skipped


def test_duplicate_source_key_is_not_silently_overwritten():
    with pytest.raises(ValueError, match="중복 원천 키"):
        prepare([store(), store(name="다른 지점")], cycle=CYCLE, min_expected=1)


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"address": None}, "invalid_address"),
        ({"address": "   "}, "invalid_address"),
        ({"name": ""}, "invalid_name"),
        ({"name": "x" * 256}, "invalid_name"),
        ({"longitude": None}, "no_coordinate"),
        ({"latitude": 91}, "invalid_coordinate"),
        ({"longitude": float("nan")}, "invalid_coordinate"),
        ({"latitude": float("inf")}, "invalid_coordinate"),
        ({"source_dt": date(2026, 9, 28)}, "future_cycle"),
    ],
)
def test_unusable_rows_are_reported_without_dropping_valid_rows(overrides, reason):
    rows, skipped = prepare(
        [store(), store(idx="2", **overrides)], cycle=CYCLE, min_expected=1
    )
    assert [r.idx for r in rows] == ["official-1"]
    assert skipped == {reason: 1}


@pytest.mark.parametrize(
    "overrides",
    [
        {"platform": "UNKNOWN"},
        {"idx": ""},
        {"idx": "x" * 65},
        {"b_code": "123"},
        {"source_dt": None},
        {"collected_at": None},
        {"collected_at": NOW.replace(tzinfo=UTC)},
    ],
)
def test_invalid_identity_or_version_fails_before_writes(overrides):
    with pytest.raises((TypeError, ValueError)):
        prepare([store(**overrides)], cycle=CYCLE, min_expected=1)


def test_input_floor_and_zero_valid_rows_fail():
    with pytest.raises(ValueError, match="하한"):
        prepare([store()], cycle=CYCLE, min_expected=2)
    with pytest.raises(ValueError, match="적재 가능한"):
        prepare([store(address=None)], cycle=CYCLE, min_expected=1)


def test_manual_rows_pass_but_do_not_count_toward_floor():
    manual = store(idx="manual-1", source_type="MANUAL")
    rows, _ = prepare([store(), manual], cycle=CYCLE, min_expected=1)
    assert [row.source_type for row in rows] == ["COLLECTED", "MANUAL"]
    with pytest.raises(ValueError, match="수집 지점"):
        prepare([store(), manual], cycle=CYCLE, min_expected=2)


def test_unknown_source_type_fails_before_writes():
    with pytest.raises(ValueError, match="source_type"):
        prepare([store(source_type="LEGACY")], cycle=CYCLE, min_expected=1)


@pytest.mark.parametrize("minimum", [0, -1, True, 1.5])
def test_minimum_must_be_a_positive_integer(minimum):
    with pytest.raises(ValueError, match="양의 정수"):
        prepare([store()], cycle=CYCLE, min_expected=minimum)


def test_dry_run_propagates_to_task_and_reports_cycle(monkeypatch):
    calls = []

    def sync(cycle, **kwargs):
        calls.append((cycle, kwargs))
        return {"input_count": 800, "inserted": 800, "updated": 0, "skipped": {}}

    class Logger:
        def info(self, *args):
            pass

    monkeypatch.setattr(flow_module, "sync_locations", sync)
    monkeypatch.setattr(flow_module, "get_run_logger", lambda: Logger())
    result = flow_module.stores_sync.fn(target_date=CYCLE, persist=False)
    assert calls == [(CYCLE, {"persist": False, "min_expected": 800})]
    assert result["cycle"] == "2026-09-27"
    assert result["persisted"] is False


def test_deployment_is_manual_and_serial():
    deployment = build()
    assert deployment.name == "stores-sync"
    assert not deployment.schedules
    assert deployment.concurrency_limit == 1
