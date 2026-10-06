"""enrich는 collect를 읽되 결과는 Postgres에만 저장한다. 외부 호출은 대역으로 검증한다."""

import importlib
import io
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from flows.common import storage

flow_module = importlib.import_module("flows.stores_enrich.flow")
CYCLE = date(2026, 9, 25)


@pytest.fixture
def pipeline(monkeypatch):
    rows = [SimpleNamespace(platform="PHOTOISM", idx="1", geocode_status="reused")]
    previous = {("PHOTOISM", "1"): object()}
    state = SimpleNamespace(
        rows=rows,
        previous=previous,
        logger=Mock(),
        read=Mock(return_value=rows),
        manual=Mock(return_value=([], 0)),
        current=Mock(return_value=previous),
        enrich=Mock(return_value=rows),
        swap=Mock(return_value={"unknown_codes": 0, "count": 1}),
        s3=Mock(side_effect=AssertionError("입력을 읽은 뒤 S3에 접근하면 안 됩니다")),
    )
    monkeypatch.setattr(flow_module, "MIN_EXPECTED", 1)
    monkeypatch.setattr(flow_module, "get_run_logger", lambda: state.logger)
    monkeypatch.setattr(flow_module, "_read_inputs", state.read)
    monkeypatch.setattr(flow_module, "read_manual", state.manual)
    monkeypatch.setattr(flow_module, "read_current", state.current)
    monkeypatch.setattr(flow_module, "enrich_stores", state.enrich)
    monkeypatch.setattr(flow_module, "swap_table", state.swap)
    monkeypatch.setattr(storage, "_client", state.s3)
    return state


@pytest.mark.parametrize("persist", [True, False])
def test_enrich_result_is_only_persisted_to_postgres(pipeline, persist):
    result = flow_module.stores_enrich.fn(target_date=CYCLE, persist=persist)
    assert result["cycle"] == "2026-09-25"
    assert result["count"] == 1
    assert result["status"] == {"reused": 1}
    assert "s3_path" not in result
    assert pipeline.read.call_args.args == (CYCLE,)
    pipeline.enrich.assert_called_once_with(
        pipeline.rows, pipeline.previous if persist else {}
    )
    pipeline.s3.assert_not_called()
    if persist:
        pipeline.current.assert_called_once_with()
        pipeline.swap.assert_called_once_with(pipeline.rows, cycle=CYCLE)
        assert result["table"] == pipeline.swap.return_value
    else:
        pipeline.current.assert_not_called()
        pipeline.swap.assert_not_called()
        assert "table" not in result


@pytest.mark.parametrize("unknown_codes", [2, -1])
def test_postgres_code_warnings_remain(pipeline, unknown_codes):
    pipeline.swap.return_value = {"unknown_codes": unknown_codes}
    result = flow_module.stores_enrich.fn(target_date=CYCLE)
    assert result["table"]["unknown_codes"] == unknown_codes
    pipeline.logger.warning.assert_called_once()
    pipeline.s3.assert_not_called()


def test_postgres_failure_is_propagated_without_s3_write(pipeline):
    pipeline.swap.side_effect = RuntimeError("Postgres unavailable")
    with pytest.raises(RuntimeError, match="Postgres unavailable"):
        flow_module.stores_enrich.fn(target_date=CYCLE)
    pipeline.s3.assert_not_called()


def test_too_few_rows_do_not_replace_postgres(pipeline, monkeypatch):
    monkeypatch.setattr(flow_module, "MIN_EXPECTED", 2)
    with pytest.raises(ValueError, match="하한 2건"):
        flow_module.stores_enrich.fn(target_date=CYCLE)
    pipeline.swap.assert_not_called()
    pipeline.s3.assert_not_called()


MANUAL_RECORD = {
    "id": 7,
    "platform": "PHOTOISM",
    "branch_name": "포토이즘 강남점",
    "address": "서울 강남구 역삼동 1",
    "phone": None,
    "longitude": 127.03,
    "latitude": 37.5,
    "updated_at": datetime(2026, 9, 20, 13, 0),
}


def test_manual_stores_are_enriched_with_collected(pipeline):
    pipeline.manual.return_value = ([MANUAL_RECORD], 2)
    pipeline.enrich.side_effect = lambda stores, _previous: stores
    result = flow_module.stores_enrich.fn(target_date=CYCLE)
    stores = pipeline.enrich.call_args.args[0]
    assert [(s.platform, s.idx) for s in stores] == [("PHOTOISM", "1"), ("PHOTOISM", "manual-7")]
    assert stores[1].source_type == "MANUAL"
    assert stores[1].source_dt == CYCLE
    assert result["manual"] == 1
    assert any("platform" in call.args[0] for call in pipeline.logger.warning.call_args_list)


def test_manual_stores_do_not_count_toward_minimum(pipeline, monkeypatch):
    monkeypatch.setattr(flow_module, "MIN_EXPECTED", 2)
    pipeline.manual.return_value = ([MANUAL_RECORD], 0)
    with pytest.raises(ValueError, match="수집 지점이 1건"):
        flow_module.stores_enrich.fn(target_date=CYCLE)
    pipeline.swap.assert_not_called()


def test_collect_input_still_comes_from_s3(monkeypatch):
    uri = "s3://test-bucket/collect/platform=PHOTOISM/dt=2026-09-25/input.csv"
    csv = (
        "platform,idx,name,address,phone,longitude,latitude,coordinate_source,collected_at\n"
        "PHOTOISM,1,역삼점,서울 강남구,,127.03,37.5,official,2026-09-25T04:00:00+09:00\n"
    )
    client = Mock()
    client.get_object.return_value = {"Body": io.BytesIO(csv.encode())}
    monkeypatch.setattr(storage, "_client", lambda: client)
    monkeypatch.setattr(
        storage, "read_manifest", lambda *_args: {"s3_path": uri, "store_count": 1}
    )
    monkeypatch.setattr(flow_module, "get_run_logger", Mock)
    monkeypatch.setattr(flow_module, "ensure_table", Mock())
    monkeypatch.setattr(
        flow_module,
        "read_cycle",
        lambda *_args, **_kwargs: {
            "PHOTOISM": {"status": "fresh", "source_target_date": CYCLE}
        },
    )
    rows = flow_module._read_inputs(
        CYCLE, max_stale_days=7, enriched_at=datetime(2026, 9, 25, 5)
    )
    assert len(rows) == 1
    assert rows[0].idx == "1"
    assert rows[0].source_dt == CYCLE
    assert rows[0].longitude == 127.03
    client.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="collect/platform=PHOTOISM/dt=2026-09-25/input.csv"
    )
    client.put_object.assert_not_called()
