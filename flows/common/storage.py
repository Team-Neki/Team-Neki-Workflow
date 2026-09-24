"""수집 결과를 S3에 적재한다.

boto3 기본 자격증명 체인만 쓴다. endpoint나 프로파일을 코드에서 정하지 않는다.
로컬은 `aws/config`의 `neki-local` 프로파일이 LocalStack을 가리키고, 운영은
k8s Secret 이 넣는 자격증명이 실제 S3를 가리킨다. 이관은 환경변수 교체로 끝나며
코드는 바뀌지 않는다.

레이아웃은 collect 와 enrich 둘이다.

    collect/platform=<브랜드>/dt=<대상 일자>/<실행 시각>.csv
    collect/platform=<브랜드>/dt=<대상 일자>/_raw/<실행 시각>/<이름>
    enrich/dt=<사이클>/<실행 시각>.csv

사람이 콘솔에서 읽는 것과 Athena 를 붙이는 것을 둘 다 만족하도록 잡았다.

- 브랜드가 위다. 브랜드 폴더를 열면 `dt=` 가 이력 순으로 나열되고, 실패한 날은
  폴더가 없어 마지막 폴더가 곧 대신 쓰이는 것이다. 대신하기(stores_collect)가
  브랜드 이력을 읽는 흐름과 같다
- CSV 와 그것을 만든 원문(raw)이 같은 파티션에 있고 실행 시각으로 짝이 맞는다.
  다른 prefix 로 건너갈 일이 없다
- `_raw/` 는 Hive 와 Trino(Athena)가 `_` 나 `.` 로 시작하는 폴더와 파일을
  무시하는 규칙을 쓴 것이다. `collect/` 를 테이블로 잡으면 CSV 만 읽힌다.
  같은 날 재실행으로 CSV 가 둘이면 `max("$path") OVER (PARTITION BY platform, dt)`
  뷰로 최신만 남긴다
- `dt=` 는 적재일이 아니라 대상 일자(target_date)다. 늦게 집힌 월요일 run 은
  월요일 폴더에 들어가고 파일명이 실제 시각을 말한다. Athena 가 사이클로
  파티션을 건너뛰려면 파티션이 사이클이어야 한다

실행 시각은 `YYYY-MM-DD_HHMMSS`(KST)로 브랜드 flow run 의 시작 시각이다. CSV 와
raw 가 같은 값을 쓰므로 어느 원문이 어느 CSV 를 만들었는지 대조할 필요가 없다.
같은 날 다시 돌리면 CSV 도 raw 폴더도 하나 더 생기고 이전 것은 남는다.

어느 CSV 가 현재인지는 S3 가 아니라 Postgres 의 `tb_store_collect_manifest` 행이
가리킨다(`flows/common/manifest.py`). 읽는 쪽은 그 행의 `s3_path` 만 따라간다.
실행 하나의 요약(어느 브랜드가 성공하고 무엇으로 대신했나)은 S3 에 남기지 않는다.
실패 사유는 Prefect 로그에 있고, 무엇을 읽을지는 `manifest.read_cycle` 이 테이블에서
읽는 시점에 계산한다.

압축하지 않는다. collect 는 하루 전량이 수백 KB, raw 는 압축을 풀어도 5MB 안팎이라
줄여서 얻는 것이 없고, gzip 이면 콘솔에서 바로 열리지 않는다. 대신 Content-Type 을
넣어 콘솔의 "열기"가 브라우저에서 바로 보여주게 한다. raw 객체에는 `kind=raw`
태그를 달아 둔다. S3 lifecycle 은 prefix 나 태그로만 걸리는데 raw 가 파티션 안에
있어 prefix 로는 못 잡기 때문이다.

CSV는 타입이 없어 읽는 쪽이 되돌려야 한다. 열 목록이 곧 계약이므로 `COLUMNS`가
정본이고, 여기 없는 필드를 적재하면 `DictWriter`가 막는다.
"""

import csv
import io
import mimetypes
import os
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
from prefect import get_run_logger, task
from prefect.context import FlowRunContext
from prefect.runtime import flow_run

from flows.common.manifest import ensure_table, put_manifest, read_manifest
from flows.common.manifest import target_date as cycle_date
from flows.common.platform import Platform

BUCKET_ENV = "S3_BUCKET"

COLLECT_PREFIX = "collect"

# enrich 결과. 브랜드 11개가 한 파티션이라 platform= 이 없다.
ENRICH_PREFIX = "enrich"

# 원문이 들어가는 폴더. `_` 로 시작해야 Athena 가 무시한다. 이름을 바꾸면 그
# 성질이 사라진다.
RAW_DIR = "_raw"

RUN_AT_FORMAT = "%Y-%m-%d_%H%M%S"

# CSV 열 순서. 적재물의 스키마 계약이라 CollectedStore에 필드를 더하면 여기에도
# 넣어야 한다. 빠뜨리면 조용히 누락되지 않고 DictWriter가 ValueError로 막는다.
COLUMNS = (
    "platform",
    "idx",
    "name",
    "address",
    "phone",
    "longitude",
    "latitude",
    "coordinate_source",
    "collected_at",
)

# 숫자로 되돌릴 열. CSV는 전부 문자열로 나오므로 읽는 쪽이 복원한다.
FLOAT_COLUMNS = ("longitude", "latitude")

# 파티션 날짜는 KST를 쓴다. 새벽 3시 실행을 UTC로 끊으면 전날 파티션에 들어가
# 운영자가 보는 날짜와 어긋난다.
KST = timezone(timedelta(hours=9))


def _bucket() -> str:
    bucket = os.environ.get(BUCKET_ENV)
    if not bucket:
        raise RuntimeError(
            f"{BUCKET_ENV} 환경변수가 없습니다. 로컬은 .env.example을 복사하고 "
            "`make s3-init`으로 버킷을 만드세요."
        )
    return bucket


def _client():
    """S3 클라이언트. endpoint를 넘기지 않는 것이 핵심이다."""
    return boto3.Session().client("s3")


def partition(platform: Platform, target_date: date) -> str:
    """파티션 경로. 끝에 슬래시를 붙이지 않는다."""
    return f"{COLLECT_PREFIX}/platform={platform}/dt={target_date:%Y-%m-%d}"


def run_at() -> str:
    """이번 실행의 시각. 감싼 flow run 의 시작 시각(KST)이다.

    task 안에서 불러도 감싼 flow run 을 본다. CSV 와 raw 가 같은 값을 쓰려면
    적재 시점의 now() 가 아니라 run 에 박힌 시각이어야 한다. 재시도도 같은
    이름을 다시 쓰므로 실패한 시도의 파일이 따로 남지 않는다. flow run 밖이면
    지금이다.
    """
    context = FlowRunContext.get()
    started = context.flow_run.start_time if context else None
    return (started or datetime.now(KST)).astimezone(KST).strftime(RUN_AT_FORMAT)


def _content_type(name: str) -> str:
    """콘솔의 "열기"가 브라우저에서 바로 보여주도록 확장자로 정한다."""
    guessed = mimetypes.guess_type(name)[0] or "text/plain"
    return f"{guessed}; charset=utf-8" if guessed.startswith("text/") else guessed


def _split_uri(uri: str) -> tuple[str, str]:
    """`s3://버킷/키` 를 나눈다.

    manifest 가 전체 경로를 들고 있으므로 읽는 쪽은 버킷을 환경변수에서
    다시 찾지 않는다. 행만 보고 파일에 닿을 수 있어야 한다.
    """
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        raise ValueError(f"S3 경로가 아닙니다: {uri}")
    return bucket, key


def _record(store: Any, *, collected_at: datetime) -> dict[str, Any]:
    """dataclass를 CSV 한 줄로 옮긴다.

    collected_at을 여기서 붙인다. enrich가 여러 브랜드를 한 파일로 합치고 나면
    브랜드마다 수집 시각이 다를 수 있어(한 브랜드만 실패해 어제 것을 쓰는 경우)
    줄마다 들고 있어야 구분된다.
    """
    fields = asdict(store) if is_dataclass(store) else dict(store)
    fields["platform"] = str(fields["platform"])
    fields["collected_at"] = collected_at.isoformat()
    return fields


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_stores(
    stores: list[Any],
    *,
    platform: Platform,
    target_date: date | None = None,
) -> str:
    """수집 결과를 collect 파티션에 적재하고 manifest 행을 남긴다.

    `target_date` 는 이 적재물이 어느 수집 사이클의 것인지이며 파티션과 manifest
    의 키가 된다. 비우면 감싼 flow run 에서 읽는다(`manifest.target_date`).

    파일명이 실행 시각이라 같은 날 다시 실행해도 이전 CSV를 덮어쓰지 않고,
    manifest 도 행을 하나 더 쌓는다. 읽는 쪽은 사이클의 마지막 행이 가리키는
    `s3_path` 를 따라가므로 언제 읽어도 완결된 실행 하나를 본다.

    본문을 먼저 올리고 manifest 를 나중에 쓴다. 순서가 뒤집히면 manifest 만 있고
    데이터가 없는 창이 생겨 다음 단계가 없는 파일을 읽으러 간다.

    다만 DB 가 닿는지는 올리기 전에 확인한다. 이 task 는 재시도가 셋이라
    manifest 쪽에서 처음 막히면 행 없는 CSV 가 파티션에 남는다.
    """
    logger = get_run_logger()

    target_date = target_date or cycle_date()
    collected_at = datetime.now(KST)

    bucket = _bucket()
    client = _client()

    # 적재물을 올리기 전에 부른다. DATABASE_URL 이 없거나 DB 가 죽어 있으면
    # 여기서 끝나므로 가리킬 행이 없는 CSV 를 남기지 않는다.
    ensure_table()

    buffer = io.StringIO()
    # lineterminator를 지정한다. 기본값이 CRLF라 그대로 두면 Postgres COPY가
    # 마지막 열 끝에 \r을 붙여 읽는다.
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    for store in stores:
        writer.writerow(_record(store, collected_at=collected_at))

    body = buffer.getvalue().encode("utf-8")
    key = f"{partition(platform, target_date)}/{run_at()}.csv"
    uri = f"s3://{bucket}/{key}"

    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=_content_type(key))

    manifest_id = put_manifest(
        platform=platform,
        target_date=target_date,
        s3_path=uri,
        store_count=len(stores),
        collected_at=collected_at,
        # task 안에서도 감싸고 있는 flow run을 가리킨다. 적재물에서 실행 로그로
        # 되짚어갈 수 있어야 원인을 찾는다.
        flow_run_id=flow_run.id,
    )

    logger.info(
        "%s 에 %d건 적재 (사이클 %s, manifest #%d)",
        uri,
        len(stores),
        f"{target_date:%Y-%m-%d}",
        manifest_id,
    )
    return uri


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_raw(
    content: str,
    *,
    platform: Platform,
    name: str,
    target_date: date | None = None,
) -> str:
    """응답 원문을 CSV 와 같은 파티션의 `_raw/<실행 시각>/` 에 남긴다.

    파싱이 조용히 깨졌을 때 소급해서 고치기 위한 것이다. 포토시그니처처럼
    정규식으로 마크업을 긁는 경우 사이트가 조금만 바뀌어도 결과가 0건이 되는데,
    원문이 있으면 사이트를 다시 긁지 않고 파서만 고쳐 재생성할 수 있다.

    수집기 깊숙이서 불리므로 `target_date` 를 인자로 받을 길이 없다. 비우면
    감싼 브랜드 flow run 의 `target_date` 파라미터를 읽어 CSV 와 같은 파티션에
    들어간다(`manifest.target_date`).

    보존은 S3 lifecycle에 맡긴다. 코드가 지우지 않는다. `kind=raw` 태그가
    lifecycle 의 손잡이다.
    """
    target_date = target_date or cycle_date()

    bucket = _bucket()
    key = f"{partition(platform, target_date)}/{RAW_DIR}/{run_at()}/{name}"

    _client().put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType=_content_type(name),
        Tagging="kind=raw",
    )
    return f"s3://{bucket}/{key}"


def _restore(row: dict[str, str]) -> dict[str, Any]:
    """CSV 한 줄을 적재 전 타입으로 되돌린다.

    CSV에는 null이 없어 빈 칸과 빈 문자열을 구분하지 못한다. 수집 단계는 값이
    없을 때만 None을 넣고 빈 문자열을 담지 않으므로, 빈 칸은 None으로 읽는다.
    """
    record: dict[str, Any] = {key: value or None for key, value in row.items()}
    for key in FLOAT_COLUMNS:
        if record.get(key) is not None:
            record[key] = float(record[key])
    return record


def read_stores(*, platform: Platform, target_date: date) -> list[dict[str, Any]]:
    """한 사이클의 collect 적재물을 읽는다. enrich와 검증이 쓴다.

    파일은 manifest 행의 `s3_path` 하나만 읽는다. 같은 파티션에 남은 이전
    실행의 CSV는 이력일 뿐 현재가 아니다.

    manifest의 `store_count`와 실제 건수가 다르면 적재가 중간에 끊긴 것이므로
    막는다. 줄이 아니라 CSV 레코드를 센다. 주소에 줄바꿈이 섞이면 한 레코드가
    여러 줄로 인용되므로 줄 수로 세면 건수가 부풀려진다.
    """
    manifest = read_manifest(platform, target_date)
    if manifest is None:
        raise FileNotFoundError(
            f"{platform} 의 {target_date:%Y-%m-%d} 사이클 적재 기록이 없습니다."
        )

    bucket, key = _split_uri(manifest["s3_path"])
    body = _client().get_object(Bucket=bucket, Key=key)["Body"].read()
    records = [_restore(row) for row in csv.DictReader(io.StringIO(body.decode("utf-8")))]

    if manifest["store_count"] != len(records):
        raise ValueError(
            f"{manifest['s3_path']} manifest 의 {manifest['store_count']} 건과 "
            f"실제 {len(records)}건이 다릅니다. 적재가 중간에 끊겼을 수 있습니다."
        )

    return records


def enrich_partition(target_date: date) -> str:
    """enrich 파티션. 끝에 슬래시를 붙이지 않는다."""
    return f"{ENRICH_PREFIX}/dt={target_date:%Y-%m-%d}"


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_enriched(
    rows: list[Any], *, columns: tuple[str, ...], target_date: date
) -> str:
    """enrich 결과를 사이클 파티션에 CSV 로 남긴다.

    manifest 행을 남기지 않는다. index 는 S3 가 아니라 Postgres 의 현재 세대를
    읽고, S3 는 이력과 재실행 원천이다. 파티션 안에서 파일명이 실행 시각이라
    가장 나중 파일이 곧 그 사이클의 마지막 실행이다.

    열 목록은 호출부가 준다. 이 모듈은 collect 의 스키마만 알고 enrich 의
    스키마는 flows/stores_enrich/region.py 가 정본이다. 날짜와 시각은 ISO 로
    쓴다. datetime 은 date 의 하위 타입이라 isinstance 하나로 둘 다 걸린다.
    """
    logger = get_run_logger()
    bucket = _bucket()

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        fields = asdict(row) if is_dataclass(row) else dict(row)
        writer.writerow(
            {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in fields.items()
            }
        )

    key = f"{enrich_partition(target_date)}/{run_at()}.csv"
    uri = f"s3://{bucket}/{key}"
    _client().put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType=_content_type(key),
    )
    logger.info("%s 에 %d건 적재 (사이클 %s)", uri, len(rows), f"{target_date:%Y-%m-%d}")
    return uri
