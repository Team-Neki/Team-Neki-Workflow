"""전용 PostGIS DB의 임시 schema에서 실제 서버 마이그레이션과 upsert를 검증한다.

STORES_SYNC_TEST_DATABASE_URL을 명시해야 실행한다. 앱 DATABASE_URL은 읽지 않는다.
서버 checkout이 형제 디렉토리에 없으면 STORES_SYNC_MIGRATION_FILE로 V34를 지정한다.
"""

import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from flows.stores_sync.table import synchronize

CYCLE = date(2026, 9, 27)
TEST_DSN = os.environ.get("STORES_SYNC_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="전용 PostGIS 테스트 DB가 지정되지 않았습니다"
)


@pytest.fixture
def db():
    migration = Path(
        os.environ.get(
            "STORES_SYNC_MIGRATION_FILE",
            str(
                Path(__file__).resolve().parents[2]
                / "Team-Neki-Server/modules/postgres/src/main/resources/db/migration"
                / "V34__add_photo_booth_location_source_and_overrides.sql"
            ),
        )
    )
    assert migration.is_file(), (
        "STORES_SYNC_MIGRATION_FILE로 서버 V34 마이그레이션을 지정하세요"
    )
    schema = "test_stores_sync_" + uuid4().hex
    with psycopg.connect(TEST_DSN, autocommit=True) as connection:
        assert connection.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'postgis'"
        ).fetchone(), "테스트 DB에 PostGIS가 설치되어 있어야 합니다"
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            connection.execute("""
                CREATE TABLE tb_brand (
                    id BIGSERIAL PRIMARY KEY, platform VARCHAR(32), deleted_at TIMESTAMP
                );
                CREATE TABLE tb_photo_booth_location (
                    id BIGSERIAL PRIMARY KEY, map_id VARCHAR(100) NOT NULL,
                    brand_id BIGINT NOT NULL REFERENCES tb_brand(id),
                    branch_name VARCHAR(100) NOT NULL, address VARCHAR(255) NOT NULL,
                    location geometry(Point, 4326) NOT NULL,
                    created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL
                );
                CREATE TABLE tb_photo_booth_enriched (
                    platform VARCHAR(32), idx VARCHAR(64), name VARCHAR(255), address VARCHAR(255),
                    longitude DOUBLE PRECISION, latitude DOUBLE PRECISION,
                    source_dt DATE, collected_at TIMESTAMP, b_code CHAR(10)
                );
                INSERT INTO tb_brand(platform) VALUES ('PHOTOISM');
                INSERT INTO tb_photo_booth_location (
                    map_id, brand_id, branch_name, address, location, created_at, updated_at
                ) VALUES ('kakao-old', 1, '기존 지점', '기존 주소', ST_SetSRID(ST_MakePoint(127, 37), 4326),
                    '2026-09-01', '2026-09-01');
            """)
            connection.execute(migration.read_text())
            connection.execute("""
                INSERT INTO tb_photo_booth_enriched VALUES
                    ('PHOTOISM', 'official-1', '포토이즘 강남점', '서울 강남구 역삼동 1',
                     127.03, 37.5, '2026-09-27', '2026-09-27 04:00', '1168010100');
            """)
            yield connection
        finally:
            # 이 fixture가 만든 정확한 UUID schema만 정리한다.
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def run(db, persist=True):
    with db.transaction():
        if not persist:
            db.execute("SET TRANSACTION READ ONLY")
        return synchronize(db, cycle=CYCLE, persist=persist, min_expected=1)


def collected(db):
    return db.execute("""
        SELECT id, created_at, branch_name, address, ST_X(location), ST_Y(location),
               b_code, admin_hidden, source_name, source_address
        FROM tb_photo_booth_location WHERE source_type = 'COLLECTED'
    """).fetchone()


def test_same_source_keeps_id_created_at_and_changes_raw_values(db):
    assert run(db)["inserted"] == 1
    before = collected(db)
    db.execute(
        "UPDATE tb_photo_booth_enriched SET name='포토이즘 변경점', collected_at='2026-09-27 04:10'"
    )
    assert run(db)["updated"] == 1
    after = collected(db)
    assert after[:2] == before[:2]
    assert after[2] == after[8] == "포토이즘 변경점"
    assert db.execute(
        "SELECT source_type FROM tb_photo_booth_location WHERE map_id='kakao-old'"
    ).fetchone() == ("LEGACY",)


def test_overrides_hidden_and_manual_rows_survive_sync(db):
    run(db)
    db.execute("""
        UPDATE tb_photo_booth_location SET override_branch_name='수동 보정',
            override_address='수동 주소', override_location=ST_SetSRID(ST_MakePoint(128, 36), 4326),
            admin_hidden=TRUE WHERE source_type='COLLECTED';
        INSERT INTO tb_photo_booth_location (
            map_id, brand_id, branch_name, address, location, created_at, updated_at, source_type
        ) VALUES ('manual-1', 1, '수동 지점', '수동 주소', ST_SetSRID(ST_MakePoint(127, 37), 4326),
            '2026-09-01', '2026-09-01', 'MANUAL');
        UPDATE tb_photo_booth_enriched SET name='수집 변경', address='수집 주소';
    """)
    run(db)
    row = collected(db)
    assert row[2:8] == ("수동 보정", "수동 주소", 128.0, 36.0, None, True)
    assert row[8:] == ("수집 변경", "수집 주소")
    assert db.execute(
        "SELECT branch_name FROM tb_photo_booth_location WHERE source_type='MANUAL'"
    ).fetchone() == ("수동 지점",)
    db.execute(
        "UPDATE tb_photo_booth_location SET override_b_code='2611010100' WHERE source_type='COLLECTED'"
    )
    run(db)
    assert collected(db)[6] == "2611010100"
    db.execute("""UPDATE tb_photo_booth_location SET override_branch_name=NULL, override_address=NULL,
        override_location=NULL, override_b_code=NULL WHERE source_type='COLLECTED'""")
    run(db)
    assert collected(db)[2:8] == (
        "수집 변경",
        "수집 주소",
        127.03,
        37.5,
        "1168010100",
        True,
    )


def test_older_input_does_not_replace_newer_source(db):
    run(db)
    before = collected(db)
    db.execute(
        "UPDATE tb_photo_booth_enriched SET name='옛 이름', collected_at='2026-09-27 03:00'"
    )
    assert run(db)["skipped_older"] == 1
    assert collected(db) == before


def test_dry_run_is_read_only_and_predicts_inserts(db):
    assert run(db, persist=False)["inserted"] == 1
    assert collected(db) is None


def test_missing_brand_fails_without_partial_writes(db):
    db.execute("""INSERT INTO tb_photo_booth_enriched
        SELECT 'LIFE_FOUR_CUT', '1', name, address, longitude, latitude, source_dt, collected_at, b_code
        FROM tb_photo_booth_enriched""")
    with pytest.raises(ValueError, match="매핑 누락"):
        run(db)
    assert collected(db) is None


def test_write_error_rolls_back_prior_insert(db):
    db.execute("""INSERT INTO tb_photo_booth_enriched
        SELECT platform, 'official-2', '거부 지점', address, longitude, latitude,
            source_dt, collected_at, b_code FROM tb_photo_booth_enriched;
        ALTER TABLE tb_photo_booth_location ADD CONSTRAINT reject_fixture CHECK (branch_name != '거부 지점');""")
    with pytest.raises(psycopg.errors.CheckViolation):
        run(db)
    assert collected(db) is None


def test_missing_migration_fails_before_writes(db):
    db.execute(
        "ALTER TABLE tb_photo_booth_location DROP CONSTRAINT uq_photo_booth_location_source"
    )
    with pytest.raises(RuntimeError, match="유일성 제약"):
        run(db)
    assert collected(db) is None


def test_missing_source_does_not_delete_stable_location(db):
    run(db)
    before = collected(db)
    db.execute("DELETE FROM tb_photo_booth_enriched")
    with pytest.raises(ValueError, match="하한"):
        run(db)
    assert collected(db) == before
