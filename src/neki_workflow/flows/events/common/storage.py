"""게시물별 이미지와 원문을 보존하고 완료 원문으로 재수집을 방지한다."""

import csv
import hashlib
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from io import StringIO

import boto3
from botocore.exceptions import ClientError, ConnectionClosedError, EndpointConnectionError, ReadTimeoutError
from prefect import task

from neki_workflow.flows.events.common.post import CollectedImage, CollectedPost


def bucket() -> str:
    value = os.environ.get("S3_BUCKET")
    if not value:
        raise RuntimeError("S3_BUCKET 환경변수가 없습니다.")
    return value


def put_object(data: bytes, *, key: str, content_type: str) -> str:
    """이미 존재하면 성공으로 취급한다. 다른 S3 오류를 중복으로 숨기지 않는다."""
    target = bucket()
    client = boto3.Session().client("s3")
    try:
        client.put_object(
            Bucket=target, Key=key, Body=data, ContentType=content_type, IfNoneMatch="*"
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "PreconditionFailed":
            raise
    return f"s3://{target}/{key}"


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_post(post: CollectedPost) -> str:
    """task 재시도는 같은 JSON 키를 사용하고 새 관측은 별도로 남긴다."""
    data = json.dumps(asdict(post), ensure_ascii=False, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(data).hexdigest()
    key = f"events/raw/{post.source_key}/{post.external_id}/{digest}.json"
    return put_object(data, key=key, content_type="application/json; charset=utf-8")


def post_prefix(post: CollectedPost) -> str:
    return f"events/raw/{post.source_key}/{post.external_id}"


def retry_storage(task, task_run, state) -> bool:
    try:
        state.result()
    except ClientError as error:
        code = error.response["Error"]["Code"]
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return code in {"PreconditionFailed", "ConditionalRequestConflict", "SlowDown", "RequestTimeout"} or status >= 500
    except (ConnectionClosedError, EndpointConnectionError, ReadTimeoutError):
        return True
    except Exception:
        return False
    return False


def utc_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_storage)
def read_completed(post: CollectedPost) -> str | None:
    """유효한 완료 원문의 저장 시각을 반환한다. 미존재만 None으로 처리한다."""
    client = boto3.Session().client("s3")
    try:
        response = client.get_object(Bucket=bucket(), Key=f"{post_prefix(post)}/post.json")
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    data = json.loads(response["Body"].read())
    data["images"] = [CollectedImage(**image) for image in data["images"]]
    saved = CollectedPost(**data)
    if (saved.brand_code, saved.source_key, saved.external_id) != (
        post.brand_code, post.source_key, post.external_id
    ):
        raise ValueError("저장된 게시물 식별자와 요청이 다릅니다.")
    return utc_string(response["LastModified"])


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_storage)
def complete_post(post: CollectedPost) -> str | None:
    """전체 저장 후 완료 원문을 생성하고 저장된 승자의 완료 시각을 반환한다."""
    if post.image_list_complete is not True or any(not image.s3_key for image in post.images):
        return None
    data = json.dumps(asdict(post), ensure_ascii=False, sort_keys=True).encode("utf-8")
    put_object(data, key=f"{post_prefix(post)}/post.json", content_type="application/json; charset=utf-8")
    completed_at = read_completed.fn(post)
    if completed_at is None:
        raise ValueError("저장한 완료 원문을 찾을 수 없습니다.")
    return completed_at


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_storage)
def read_index(source_key: str) -> tuple[dict[str, tuple[str, str]], str | None]:
    """출처별 CSV를 읽는다. 헤더가 다른 파일을 빈 목록으로 대체하지 않는다."""
    client = boto3.Session().client("s3")
    try:
        response = client.get_object(Bucket=bucket(), Key=f"events/raw/{source_key}/collected.csv")
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return {}, None
        raise
    reader = csv.DictReader(StringIO(response["Body"].read().decode("utf-8")), strict=True)
    if reader.fieldnames != ["external_id", "status", "recorded_at"]:
        raise ValueError("완료 CSV 헤더가 올바르지 않습니다.")
    return {row["external_id"]: (row["status"], row["recorded_at"]) for row in reader}, response["ETag"]


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_storage)
def merge_index(source_key: str, additions: dict[str, tuple[str, str]]) -> None:
    """재시도마다 최신 CSV를 읽어 병합한다. 낡은 ETag로 PUT만 반복하지 않는다."""
    if not additions:
        return
    entries, etag = read_index.fn(source_key)
    new = {key: value for key, value in additions.items() if key not in entries}
    if not new:
        return
    entries.update(new)
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["external_id", "status", "recorded_at"])
    writer.writerows((key, *value) for key, value in sorted(entries.items()))
    condition = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
    boto3.Session().client("s3").put_object(
        Bucket=bucket(), Key=f"events/raw/{source_key}/collected.csv",
        Body=output.getvalue().encode("utf-8"), ContentType="text/csv; charset=utf-8", **condition,
    )
