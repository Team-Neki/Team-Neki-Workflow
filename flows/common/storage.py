"""수집 결과를 S3에 적재한다.

boto3 기본 자격증명 체인만 쓴다. endpoint나 프로파일을 코드에서 정하지 않는다.
로컬은 `aws/config`의 `neki-local` 프로파일이 LocalStack을 가리키고, 운영은
worker의 IAM role이 실제 S3를 가리킨다. 이관은 프로파일 교체로 끝나며 코드는
바뀌지 않는다.

레이아웃은 다음과 같다.

    raw/     platform=<브랜드>/dt=<날짜>/<이름>.gz
    collect/ platform=<브랜드>/dt=<날짜>/stores.jsonl.gz
                                       /_manifest.json

`dt=` Hive 파티션이라 이후 Glue나 Athena를 그대로 붙일 수 있다. 포맷은
JSONL+gzip이다. 아직 스키마가 흔들리고 있어 Parquet은 이르다.
"""

import gzip
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
from prefect import get_run_logger, task
from prefect.runtime import flow_run

from flows.common.platform import Platform

BUCKET_ENV = "S3_BUCKET"

RAW_PREFIX = "raw"
COLLECT_PREFIX = "collect"

STORES_NAME = "stores.jsonl.gz"
MANIFEST_NAME = "_manifest.json"

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


def today() -> date:
    return datetime.now(KST).date()


def partition(prefix: str, *, platform: Platform, dt: date) -> str:
    """파티션 경로. 끝에 슬래시를 붙이지 않는다."""
    return f"{prefix}/platform={platform}/dt={dt:%Y-%m-%d}"


def _record(store: Any, *, collected_at: datetime) -> dict[str, Any]:
    """dataclass를 JSON 한 줄로 옮긴다.

    collected_at을 여기서 붙인다. enrich가 4개 브랜드를 한 파일로 합치고 나면
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
    dt: date | None = None,
) -> str:
    """수집 결과를 collect 파티션에 적재하고 manifest를 남긴다.

    같은 날 다시 실행하면 같은 키를 덮어쓴다. 단일 객체 PUT은 원자적이라
    안전하고, 이렇게 해야 재실행이 멱등해진다.

    manifest가 없으면 다음 단계가 부분 실패한 파티션을 정상으로 오해한다.
    그래서 본문을 먼저 올리고 manifest를 나중에 올린다. 순서가 뒤집히면
    manifest만 있고 데이터가 없는 창이 생긴다.
    """
    logger = get_run_logger()

    dt = dt or today()
    collected_at = datetime.now(KST)

    bucket = _bucket()
    client = _client()
    base = partition(COLLECT_PREFIX, platform=platform, dt=dt)

    lines = [
        json.dumps(_record(store, collected_at=collected_at), ensure_ascii=False)
        for store in stores
    ]
    body = gzip.compress("\n".join(lines).encode("utf-8"))

    client.put_object(Bucket=bucket, Key=f"{base}/{STORES_NAME}", Body=body)

    manifest = {
        "platform": str(platform),
        "dt": f"{dt:%Y-%m-%d}",
        "count": len(lines),
        "collected_at": collected_at.isoformat(),
        # task 안에서도 부모 flow run을 가리킨다. 적재물에서 실행 로그로
        # 되짚어갈 수 있어야 원인을 찾는다.
        "flow_run_id": flow_run.id,
    }
    client.put_object(
        Bucket=bucket,
        Key=f"{base}/{MANIFEST_NAME}",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
    )

    logger.info("s3://%s/%s 에 %d건 적재", bucket, base, len(lines))
    return f"s3://{bucket}/{base}"


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_raw(
    content: str,
    *,
    platform: Platform,
    name: str,
    dt: date | None = None,
) -> str:
    """응답 원문을 raw 파티션에 남긴다.

    파싱이 조용히 깨졌을 때 소급해서 고치기 위한 것이다. 포토시그니처처럼
    정규식으로 마크업을 긁는 경우 사이트가 조금만 바뀌어도 결과가 0건이 되는데,
    원문이 있으면 사이트를 다시 긁지 않고 파서만 고쳐 재생성할 수 있다.

    보존은 S3 lifecycle에 맡긴다. 코드가 지우지 않는다.
    """
    dt = dt or today()

    bucket = _bucket()
    base = partition(RAW_PREFIX, platform=platform, dt=dt)
    key = f"{base}/{name}.gz"

    _client().put_object(
        Bucket=bucket, Key=key, Body=gzip.compress(content.encode("utf-8"))
    )
    return f"s3://{bucket}/{key}"


def read_stores(*, platform: Platform, dt: date) -> list[dict[str, Any]]:
    """collect 파티션을 읽는다. enrich와 검증이 쓴다.

    manifest의 count와 실제 줄 수가 다르면 적재가 중간에 끊긴 것이므로 막는다.
    """
    bucket = _bucket()
    client = _client()
    base = partition(COLLECT_PREFIX, platform=platform, dt=dt)

    body = client.get_object(Bucket=bucket, Key=f"{base}/{STORES_NAME}")["Body"].read()
    text = gzip.decompress(body).decode("utf-8")
    records = [json.loads(line) for line in text.splitlines() if line]

    manifest = json.loads(
        client.get_object(Bucket=bucket, Key=f"{base}/{MANIFEST_NAME}")["Body"].read()
    )
    if manifest["count"] != len(records):
        raise ValueError(
            f"{base} manifest count {manifest['count']} 와 실제 {len(records)}건이 "
            "다릅니다. 적재가 중간에 끊겼을 수 있습니다."
        )

    return records
