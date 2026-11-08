"""CSV 완료 인덱스의 충돌·손상·복구 계약."""

import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from neki_workflow.flows.events.common import storage
from neki_workflow.flows.events.common.post import CollectedPost

STAMP = "2026-10-19T03:00:00Z"


def error(code):
    return ClientError({"Error": {"Code": code}}, "S3")


@pytest.fixture
def client(monkeypatch):
    client = Mock()
    monkeypatch.setenv("S3_BUCKET", "local-test")
    monkeypatch.setattr(storage.boto3, "Session", lambda: Mock(client=lambda _: client))
    return client


def response(csv):
    return {"Body": BytesIO(csv.encode()), "ETag": '"v1"'}


def test_missing_is_distinct_from_permission_failure(client):
    client.get_object.side_effect = error("NoSuchKey")
    assert storage.read_index.fn("test") == ({}, None)
    client.get_object.side_effect = error("AccessDenied")
    with pytest.raises(ClientError):
        storage.read_index.fn("test")


@pytest.mark.parametrize("csv", [
    "", "external_id\n1\n", "external_id,completed_at\n1,\n",
    f"external_id,completed_at\n1,{STAMP}\n1,{STAMP}\n",
    "external_id,completed_at\n1,2026-10-19T03:00:00\n",
    f"external_id,completed_at\n1,{STAMP},extra\n",
])
def test_corrupt_index_is_not_empty(client, csv):
    client.get_object.return_value = response(csv)
    with pytest.raises(ValueError):
        storage.read_index.fn("test")


def test_merge_conflict_rereads_and_preserves_other_writer(client):
    state = {"csv": None, "etag": None}
    def get(**kwargs):
        if state["csv"] is None:
            raise error("NoSuchKey")
        return {"Body": BytesIO(state["csv"]), "ETag": state["etag"]}
    def put(**kwargs):
        if state["csv"] is None:
            assert kwargs["IfNoneMatch"] == "*"
            state.update(csv=f"external_id,completed_at\nother,{STAMP}\n".encode(), etag='"other"')
            raise error("PreconditionFailed")
        assert kwargs["IfMatch"] == state["etag"]
        state.update(csv=kwargs["Body"], etag='"merged"')
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    with pytest.raises(ClientError):
        storage.merge_index.fn("test", {"mine": STAMP})
    # Prefect가 task 전체를 다시 실행하는 것과 동일한 경로.
    storage.merge_index.fn("test", {"mine": STAMP})
    assert storage.read_index.fn("test")[0] == {"mine": STAMP, "other": STAMP}
    client.put_object.reset_mock()
    storage.merge_index.fn("test", {"mine": "2026-10-20T00:00:00Z"})
    client.put_object.assert_not_called()
    assert storage.read_index.fn("test")[0]["mine"] == STAMP


def test_lost_put_response_does_not_write_again(client):
    state = {}
    def get(**kwargs):
        if not state:
            raise error("NoSuchKey")
        return {"Body": BytesIO(state["data"]), "ETag": '"saved"'}
    def put(**kwargs):
        state["data"] = kwargs["Body"]
        raise ReadTimeoutError(endpoint_url="http://localhost")
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    with pytest.raises(ReadTimeoutError):
        storage.merge_index.fn("test", {"001": STAMP})
    storage.merge_index.fn("test", {"001": STAMP})
    assert client.put_object.call_count == 1
    assert "001" in storage.read_index.fn("test")[0]


def test_completed_snapshot_validated_and_uses_s3_timestamp(client):
    post = CollectedPost(1, "test", "123", "https://example.com/123", STAMP,
                         {"detail": "<div>text</div>"}, image_list_complete=True)
    from dataclasses import asdict
    def reply(data):
        return {"Body": BytesIO(json.dumps(data).encode()),
                "LastModified": datetime(2026, 10, 19, 3, tzinfo=timezone.utc)}
    client.get_object.return_value = reply(asdict(post))
    assert storage.read_completed.fn(post) == STAMP
    damaged = asdict(post)
    damaged["image_list_complete"] = False
    client.get_object.return_value = reply(damaged)
    with pytest.raises(ValueError):
        storage.read_completed.fn(post)
    damaged = asdict(post)
    damaged["external_id"] = "other"
    client.get_object.return_value = reply(damaged)
    with pytest.raises(ValueError):
        storage.read_completed.fn(post)


@pytest.mark.parametrize("exception,retry", [
    (error("PreconditionFailed"), True), (error("ConditionalRequestConflict"), True),
    (error("AccessDenied"), False), (ValueError("bad CSV"), False),
    (ReadTimeoutError(endpoint_url="http://localhost"), True),
])
def test_retry_classification(exception, retry):
    state = Mock()
    state.result.side_effect = exception
    assert storage.retry_storage(None, None, state) is retry
