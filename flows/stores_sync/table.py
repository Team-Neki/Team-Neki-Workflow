"""지점 마스터를 원천 키로 upsert한다. 스키마 변경은 서버 Flyway가 맡는다."""

from datetime import date, datetime
from typing import Any

from prefect import task
from psycopg.rows import dict_row

from flows.common.manifest import KST
from flows.common.postgres import connect
from flows.stores_sync.records import SourceStore, prepare

TABLE = "tb_photo_booth_location"
MIN_EXPECTED = 800
SYNC_LOCK = 0x6E656B69_153
REQUIRED_COLUMNS = {
    "source_type",
    "source_platform",
    "source_idx",
    "source_name",
    "source_address",
    "source_location",
    "source_b_code",
    "source_dt",
    "source_collected_at",
    "override_branch_name",
    "override_address",
    "override_location",
    "override_b_code",
    "b_code",
    "admin_hidden",
}

# override는 DB가 행 잠금을 획득한 뒤 현재 값으로 읽는다. Python에서 미리 읽어
# 새 수집 값과 합치면 동시 어드민 수정이 유실될 수 있다.
UPSERT = """
INSERT INTO tb_photo_booth_location AS current (
    map_id, brand_id, branch_name, address, location, created_at, updated_at,
    source_type, source_platform, source_idx, source_name, source_address,
    source_location, source_b_code, source_dt, source_collected_at, b_code
) VALUES (
    %(map_id)s, %(brand_id)s, %(name)s, %(address)s,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326), %(now)s, %(now)s,
    %(source_type)s, %(platform)s, %(idx)s, %(name)s, %(address)s,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
    %(b_code)s, %(source_dt)s, %(collected_at)s, %(b_code)s
)
ON CONFLICT (source_platform, source_idx) DO UPDATE SET
    brand_id = EXCLUDED.brand_id,
    source_name = EXCLUDED.source_name,
    source_address = EXCLUDED.source_address,
    source_location = EXCLUDED.source_location,
    source_b_code = EXCLUDED.source_b_code,
    source_dt = EXCLUDED.source_dt,
    source_collected_at = EXCLUDED.source_collected_at,
    branch_name = COALESCE(current.override_branch_name, EXCLUDED.source_name),
    address = COALESCE(current.override_address, EXCLUDED.source_address),
    location = COALESCE(current.override_location, EXCLUDED.source_location),
    b_code = CASE
        WHEN current.override_b_code IS NOT NULL THEN current.override_b_code
        WHEN current.override_location IS NOT NULL THEN NULL
        ELSE EXCLUDED.source_b_code
    END,
    updated_at = EXCLUDED.updated_at
WHERE current.source_type = EXCLUDED.source_type
    AND (current.source_dt, current.source_collected_at)
        <= (EXCLUDED.source_dt, EXCLUDED.source_collected_at)
RETURNING id
"""


def require_schema(cursor) -> None:
    cursor.execute(
        "SELECT attname FROM pg_attribute WHERE attrelid = to_regclass(%s) "
        "AND attnum > 0 AND NOT attisdropped",
        (TABLE,),
    )
    missing = REQUIRED_COLUMNS - {r["attname"] for r in cursor.fetchall()}
    if missing:
        raise RuntimeError(
            "서버 Flyway V34를 먼저 적용해야 합니다. 지점 컬럼 누락: "
            + ", ".join(sorted(missing))
        )
    cursor.execute(
        "SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass(%s) "
        "AND conname = 'uq_photo_booth_location_source' AND contype = 'u'",
        (TABLE,),
    )
    if cursor.fetchone() is None:
        raise RuntimeError("서버 Flyway V34의 원천 키 유일성 제약이 없습니다.")
    # V34 제약은 MANUAL 에 원천 키를 허용하지 않는다. V36 전이면 관리자 지점 INSERT 가 실패한다
    cursor.execute(
        "SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass(%s) "
        "AND conname = 'ck_photo_booth_location_source_keyed' AND contype = 'c'",
        (TABLE,),
    )
    if cursor.fetchone() is None:
        raise RuntimeError(
            "서버 Flyway V36을 먼저 적용해야 합니다. 관리자 등록 지점의 원천 키를 허용하는 제약이 없습니다."
        )


def synchronize(
    connection, *, cycle: date, persist: bool, min_expected: int
) -> dict[str, Any]:
    """호출자가 트랜잭션을 소유한다. 테스트도 같은 SQL 경로를 사용한다."""
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        if persist:
            cursor.execute(
                "SELECT pg_try_advisory_xact_lock(%s) AS locked", (SYNC_LOCK,)
            )
            if not cursor.fetchone()["locked"]:
                raise RuntimeError(
                    "다른 지점 동기화가 실행 중입니다. 완료 후 다시 실행하세요."
                )
        require_schema(cursor)
        # 관리자 등록 지점(MANUAL)도 (platform, manual-<id>) 원천 키로 함께 동기화한다
        cursor.execute(
            "SELECT platform, idx, name, address, longitude, latitude, source_dt, "
            "collected_at, b_code, source_type FROM tb_photo_booth_enriched "
            "ORDER BY platform, idx"
        )
        stores = [SourceStore(**row) for row in cursor.fetchall()]
        ready, skipped = prepare(stores, cycle=cycle, min_expected=min_expected)
        cursor.execute(
            "SELECT id, platform FROM tb_brand WHERE deleted_at IS NULL AND platform IS NOT NULL"
        )
        brands: dict[str, int] = {}
        for brand in cursor.fetchall():
            if brand["platform"] in brands:
                raise ValueError(f"중복 브랜드 platform: {brand['platform']}")
            brands[brand["platform"]] = brand["id"]
        missing_brands = {s.platform for s in ready} - brands.keys()
        if missing_brands:
            raise ValueError(
                "tb_brand.platform 매핑 누락: " + ", ".join(sorted(missing_brands))
            )

        cursor.execute(
            "SELECT source_platform, source_idx, source_dt, source_collected_at "
            "FROM tb_photo_booth_location WHERE source_platform IS NOT NULL"
        )
        versions = {
            (row["source_platform"], row["source_idx"]): (
                row["source_dt"],
                row["source_collected_at"],
            )
            for row in cursor.fetchall()
        }
        now = datetime.now(KST).replace(tzinfo=None)
        inserted = updated = older = 0
        for row in ready:
            version = versions.get(row.key)
            if version is not None and version > (row.source_dt, row.collected_at):
                older += 1
                continue
            if persist:
                cursor.execute(
                    UPSERT,
                    {
                        "map_id": f"source:{row.platform}:{row.idx}",
                        "source_type": row.source_type,
                        "brand_id": brands[row.platform],
                        "platform": row.platform,
                        "idx": row.idx,
                        "name": row.name,
                        "address": row.address,
                        "longitude": row.longitude,
                        "latitude": row.latitude,
                        "b_code": row.b_code,
                        "source_dt": row.source_dt,
                        "collected_at": row.collected_at,
                        "now": now,
                    },
                )
                if cursor.fetchone() is None:
                    older += 1
                    continue
            if version is None:
                inserted += 1
            else:
                updated += 1
        return {
            "input_count": len(stores),
            "inserted": inserted,
            "updated": updated,
            "skipped_older": older,
            "skipped": skipped,
        }


@task
def sync_locations(cycle: date, *, persist: bool, min_expected: int) -> dict[str, Any]:
    with connect() as connection:
        if not persist:
            # dry-run에서 실수로 DML을 추가해도 DB가 막는다.
            connection.execute("SET TRANSACTION READ ONLY")
        return synchronize(
            connection, cycle=cycle, persist=persist, min_expected=min_expected
        )
