"""수집 결과를 S3에 적재한다.

boto3 기본 자격증명 체인만 쓴다. endpoint나 프로파일을 코드에서 정하지 않는다.
로컬은 `aws/config`의 `neki-local` 프로파일이 LocalStack을 가리키고, 운영은
worker의 IAM role이 실제 S3를 가리킨다. 이관은 프로파일 교체로 끝나며 코드는
바뀌지 않는다.

레이아웃은 다음과 같다.

    raw/     platform=<브랜드>/dt=<날짜>/<이름>.gz
    collect/ platform=<브랜드>/dt=<날짜>/<HHMMSS>.csv
    runs/    dt=<날짜>/collect.json

`dt=` Hive 파티션이라 이후 Glue나 Athena를 그대로 붙일 수 있다. 포맷은 헤더
있는 CSV다. 다음 단계가 Postgres COPY로 그대로 받고, 사람이 볼 때도 S3 콘솔과
스프레드시트에서 바로 열린다. 스키마가 아직 흔들리고 있어 Parquet은 이르다.

collect는 압축하지 않는다. 하루 전량이 수백 KB라 줄여서 얻는 것이 없고,
압축하면 바로 열린다는 이점이 사라진다. raw/는 HTML 원문이라 gzip으로 둔다.

CSV 파일명은 적재 시각(KST)이다. 같은 날 다시 돌리면 파일이 하나 더 생기고
이전 것은 남는다. **어느 파일이 현재인지는 S3 가 아니라 Postgres 가 안다.**
`flows/common/manifest.py` 의 `tb_store_collect_manifest` 가 적재 한 번마다 한
행으로 경로와 건수를 들고 있고, 읽는 쪽은 그 사이클의 마지막 행을 따라간다.
파일과 행이 1:1 로 쌓이므로 재실행 이력이 양쪽에 같은 모양으로 남는다.
파티션 안에 `_manifest.json` 을 두지 않으므로 `collect/` 에는 Hive 파티션과
CSV 만 남는다.

CSV는 타입이 없어 읽는 쪽이 되돌려야 한다. 열 목록이 곧 계약이므로 `COLUMNS`가
정본이고, 여기 없는 필드를 적재하면 `DictWriter`가 막는다.
"""

import csv
import gzip
import io
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
from prefect import get_run_logger, task
from prefect.runtime import flow_run

from flows.common.manifest import put_manifest, read_manifest
from flows.common.manifest import target_date as cycle_date
from flows.common.platform import Platform

BUCKET_ENV = "S3_BUCKET"

RAW_PREFIX = "raw"
COLLECT_PREFIX = "collect"

# 실행 하나를 설명하는 manifest. collect/ 안에 두지 않는다. 그쪽은 Hive 파티션만
# 있어야 나중에 Glue 를 그대로 붙일 수 있고, 다른 것이 섞이면 파티션 인식이
# 깨진다. 브랜드마다의 적재 위치와 달리 `brands` 가 중첩 구조라 표로 펼칠 수
# 없어 테이블로 옮기지 않고 JSON 으로 남긴다.
RUNS_PREFIX = "runs"

STORES_NAME_FORMAT = "%H%M%S.csv"
RUN_MANIFEST_NAME = "collect.json"

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


def today() -> date:
    return datetime.now(KST).date()


def partition(prefix: str, *, platform: Platform, dt: date) -> str:
    """파티션 경로. 끝에 슬래시를 붙이지 않는다."""
    return f"{prefix}/platform={platform}/dt={dt:%Y-%m-%d}"


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
    dt: date | None = None,
    target_date: date | None = None,
) -> str:
    """수집 결과를 collect 파티션에 적재하고 manifest 행을 남긴다.

    `dt` 는 파티션 날짜, 즉 우리가 언제 받았는지다. `target_date` 는 이 적재물이
    어느 수집 사이클의 것인지이며 manifest 의 키가 된다. 둘은 보통 같고, 예약된
    run 이 늦게 집혔을 때만 갈린다. 자세한 것은 `flows/common/manifest.py` 에
    남겼다.

    파일명이 적재 시각이라 같은 날 다시 실행해도 이전 CSV를 덮어쓰지 않고,
    manifest 도 행을 하나 더 쌓는다. 읽는 쪽은 사이클의 마지막 행이 가리키는
    `s3_path` 를 따라가므로 언제 읽어도 완결된 실행 하나를 본다.

    본문을 먼저 올리고 manifest 를 나중에 쓴다. 순서가 뒤집히면 manifest 만 있고
    데이터가 없는 창이 생겨 다음 단계가 없는 파일을 읽으러 간다.
    """
    logger = get_run_logger()

    dt = dt or today()
    target_date = target_date or cycle_date()
    collected_at = datetime.now(KST)

    bucket = _bucket()
    client = _client()
    base = partition(COLLECT_PREFIX, platform=platform, dt=dt)

    buffer = io.StringIO()
    # lineterminator를 지정한다. 기본값이 CRLF라 그대로 두면 Postgres COPY가
    # 마지막 열 끝에 \r을 붙여 읽는다.
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    for store in stores:
        writer.writerow(_record(store, collected_at=collected_at))

    body = buffer.getvalue().encode("utf-8")
    name = collected_at.strftime(STORES_NAME_FORMAT)
    uri = f"s3://{bucket}/{base}/{name}"

    client.put_object(Bucket=bucket, Key=f"{base}/{name}", Body=body)

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


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_run_manifest(
    brands: dict[str, dict[str, Any]], *, target_date: date | None = None
) -> str:
    """수집 실행 하나를 설명하는 manifest 를 남긴다.

    브랜드 하나가 실패해도 나머지는 적재하므로, 다음 단계는 "이 사이클에 무엇이
    쓸 수 있는가"를 알아야 한다. 브랜드별 manifest 행으로는 답할 수 없다.
    없는 행은 없다는 사실 자체가 기록되지 않기 때문이다.

    파티션을 `target_date` 로 끊는다. 브랜드별 manifest 와 같은 사이클을 가리켜야
    다음 단계가 둘을 맞붙일 수 있다.
    """
    logger = get_run_logger()

    target_date = target_date or cycle_date()
    bucket = _bucket()
    key = f"{RUNS_PREFIX}/dt={target_date:%Y-%m-%d}/{RUN_MANIFEST_NAME}"

    succeeded = sorted(k for k, v in brands.items() if v.get("status") == "ok")
    stale = sorted(k for k, v in brands.items() if v.get("status") == "stale")
    failed = sorted(k for k, v in brands.items() if v.get("status") == "failed")

    manifest = {
        "target_date": f"{target_date:%Y-%m-%d}",
        "finished_at": datetime.now(KST).isoformat(),
        "flow_run_id": flow_run.id,
        "succeeded": succeeded,
        # 이번 사이클 수집은 실패했지만 이전 것으로 대신하는 브랜드다. 다음
        # 단계는 이들도 처리하되 데이터가 오래됐음을 알아야 한다.
        "stale": stale,
        # 대신할 것조차 없는 브랜드다. 다음 단계가 다룰 수 없다.
        "failed": failed,
        "total": sum(v.get("count") or 0 for v in brands.values()),
        # 브랜드마다 manifest 테이블의 어느 사이클 행을 읽어야 하는지 담는다.
        # 다음 단계는 이것만 보면 되고 신선한지 여부를 따로 판단할 필요가 없다.
        "brands": brands,
    }

    _client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
    )

    logger.info("s3://%s/%s 기록 (성공 %d, 실패 %d)", bucket, key, len(succeeded), len(failed))
    return f"s3://{bucket}/{key}"


def read_run_manifest(target_date: date) -> dict[str, Any]:
    """수집 실행 manifest 를 읽는다. enrich 가 무엇을 처리할지 여기서 정한다."""
    key = f"{RUNS_PREFIX}/dt={target_date:%Y-%m-%d}/{RUN_MANIFEST_NAME}"
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return json.loads(body)


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
