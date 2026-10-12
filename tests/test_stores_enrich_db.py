"""전용 PostGIS DB의 임시 schema에서 enrich 적재와 직전 세대 읽기를 검증한다.

재사용 판정(region.reusable)은 float 동등 비교라 COPY(EWKT) -> ST_X / ST_Y 왕복이
원래 float 를 그대로 돌려줘야 한다. 깨져도 실패하지 않고 재사용이 0건이 되어 매일
전 지점을 Kakao 에 물으므로 실제 PostGIS 로 확인한다.

STORES_SYNC_TEST_DATABASE_URL을 명시해야 실행한다(test_stores_sync_db 와 같은 DB).
앱 DATABASE_URL은 읽지 않는다.
"""

import logging
import os
import random
from contextlib import contextmanager
from datetime import date, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from flows.stores_enrich import table
from flows.stores_enrich.region import EnrichedStore

CYCLE = date(2026, 10, 12)
NOW = datetime(2026, 10, 12, 5, 0)
TEST_DSN = os.environ.get("STORES_SYNC_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="전용 PostGIS 테스트 DB가 지정되지 않았습니다"
)


@pytest.fixture
def schema(monkeypatch):
    """table 의 연결을 임시 schema 로 돌린다.

    search_path 에 PostGIS 가 설치된 schema 를 남겨야 geometry 타입이 풀린다. 그런데
    swap_table 의 DROP / RENAME 은 이름만 쓰므로, 임시 schema 에 없는 테이블은 그
    schema 의 같은 이름 테이블을 잡아 지우거나 옮긴다. 그래서 그쪽에 enriched 계열
    테이블이 하나라도 있으면 아무것도 건드리기 전에 멈춘다.
    """
    name = "test_stores_enrich_" + uuid4().hex
    with psycopg.connect(TEST_DSN, autocommit=True) as connection:
        found = connection.execute(
            "SELECT n.nspname FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'postgis'"
        ).fetchone()
        assert found, "테스트 DB에 PostGIS가 설치되어 있어야 합니다"
        postgis = found[0]
        clash = connection.execute(
            "SELECT string_agg(tablename, ', ') FROM pg_tables "
            "WHERE schemaname = %s AND tablename LIKE %s",
            (postgis, table.TABLE + "%"),
        ).fetchone()[0]
        if clash:
            pytest.fail(
                f"테스트 DB의 {postgis} schema 에 {clash} 가 있어 swap_table 이 그것을 지울 수 있습니다. "
                "전용 테스트 DB를 쓰세요"
            )
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    @contextmanager
    def connect():
        with psycopg.connect(TEST_DSN, options=f"-c search_path={name},{postgis}") as connection:
            yield connection

    monkeypatch.setattr(table, "connect", connect)
    # task 밖에서 부르므로 Prefect 실행 컨텍스트가 없다
    monkeypatch.setattr(table, "get_run_logger", lambda: logging.getLogger(__name__))
    try:
        yield
    finally:
        with psycopg.connect(TEST_DSN, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
            )


def store(idx: str, longitude: float | None, latitude: float | None) -> EnrichedStore:
    return EnrichedStore(
        platform="PHOTOISM",
        idx=idx,
        name=f"포토이즘 {idx}점",
        address="서울 강남구 역삼동 1",
        phone=None,
        longitude=longitude,
        latitude=latitude,
        coordinate_source="official",
        collected_at=NOW,
        source_dt=CYCLE,
        source_type="COLLECTED",
        b_code="1168010100",
        sido_name="서울특별시",
        sgg_name="강남구",
        umd_name="역삼동",
        geocode_status="ok",
        enriched_at=NOW,
    )


def test_coordinates_survive_copy_and_read_back_exactly(schema):
    rng = random.Random(244)
    stores = [
        store(str(i), rng.uniform(124, 132), rng.uniform(33, 39)) for i in range(500)
    ]
    stores += [store("no-coordinate", None, None), store("nan", float("nan"), 37.5)]

    assert table.swap_table.fn(stores, cycle=CYCLE)["loaded"] == len(stores)
    current = table.read_current()

    for row in stores[:500]:
        assert current[(row.platform, row.idx)] == row
    for idx in ("no-coordinate", "nan"):
        row = current[("PHOTOISM", idx)]
        assert (row.longitude, row.latitude) == (None, None)


def test_location_column_is_point_4326(schema):
    table.swap_table.fn([store("1", 127.0276, 37.4979)], cycle=CYCLE)

    with table.connect() as connection:
        assert connection.execute(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attname = 'location'",
            (table.TABLE,),
        ).fetchone() == ("geometry(Point,4326)",)
        assert connection.execute(
            f"SELECT ST_AsEWKT(location) FROM {table.TABLE}"
        ).fetchone() == ("SRID=4326;POINT(127.0276 37.4979)",)
