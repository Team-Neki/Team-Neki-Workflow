"""enrich 결과로 테이블을 새로 만들어 기존 것과 바꿔치운다.

법정동(flows/legal_dong/table.py)과 같은 바꿔치기다. 매 실행이 전량이라 증분을
계산하지 않고, 사이클 날짜를 붙인 테이블을 채워 이름만 맞바꾸며 직전 세대는
_prev 로 남긴다. 한 트랜잭션, lock_timeout 을 첫 문장으로, _prev 를 맨 앞에서
치우는 이유, 인덱스 이름을 되돌리지 않는 이유는 그쪽 docstring 과 같다.

index(서버 batch, BACKEND-65)는 현재 세대 이름 tb_photo_booth_enriched 하나만
안다. 그쪽과의 계약은 platform, idx, name, address, longitude, latitude,
source_dt, b_code 여덟 열이다. 나머지는 운영 확인용이라 바꿔도 index 를 깨지
않는다. b_code 가 NULL 인 행도 남긴다. index 가 region_ids 만 비우고 카드는
만든다.

직전 세대는 스왑 전에 읽어 재사용 판정에 쓴다(read_current). 같은 트랜잭션일
필요가 없다. 읽은 뒤 누가 스왑해도 답이 낡을 뿐 틀리지는 않는다.
"""

from dataclasses import astuple
from datetime import date, datetime

from prefect import get_run_logger, task
from psycopg import errors, sql

from flows.common.manifest import KST
from flows.common.postgres import connect
from flows.stores_enrich.region import COLUMNS, EnrichedStore

TABLE = "tb_photo_booth_enriched"
PREV_TABLE = f"{TABLE}_prev"

# 법정동 마스터. 여기 없는 b_code 를 세어 개편 뒤 마스터가 낡은 것을 드러낸다.
LEGAL_DONG_TABLE = "tb_legal_dong"

# DROP 과 스왑이 락을 못 잡을 때 물러나는 시간. 길게 잡으면 그만큼 앱의 읽기가
# 우리 뒤에 줄 서므로 짧아야 한다.
LOCK_TIMEOUT = "5s"


def staging_name(cycle: date) -> str:
    """tb_photo_booth_enriched_20260925. 오늘이 아니라 사이클 날짜다.

    어느 수집분으로 만든 세대인지 이름이 말한다. 백필로 지난 날짜를 돌리면
    그 날짜가 붙는다.
    """
    return f"{TABLE}_{cycle:%Y%m%d}"


def _ddl(staging: str) -> str:
    """컬럼 순서는 region.COLUMNS(EnrichedStore 필드 순서)와 같아야 한다. COPY 가
    그 순서로 값을 받는다.
    """
    return f"""
CREATE TABLE {staging} (
    -- collect 가 준 것 (tb_store_collect_manifest 가 가리키는 CSV 그대로)
    platform            VARCHAR(32)      NOT NULL,
    idx                 VARCHAR(64)      NOT NULL,
    name                VARCHAR(255)     NOT NULL,
    address             VARCHAR(255),
    phone               VARCHAR(32),
    longitude           DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    coordinate_source   VARCHAR(16),
    collected_at        TIMESTAMP        NOT NULL,
    source_dt           DATE             NOT NULL,

    -- enrich 가 더하는 것
    b_code              CHAR(10),
    sido_name  VARCHAR(32),
    sgg_name  VARCHAR(32),
    umd_name  VARCHAR(32),
    geocode_status      VARCHAR(16)      NOT NULL,
    enriched_at         TIMESTAMP        NOT NULL,

    PRIMARY KEY (platform, idx)
);
"""


def _indexes(staging: str) -> str:
    """COPY 를 끝낸 다음에 건다. 이름에 날짜가 붙은 채로 둔다."""
    return f"""
-- index 가 법정동으로 부스를 모을 때.
CREATE INDEX ON {staging} (b_code);
"""


def _comments(staging: str) -> str:
    return f"""
COMMENT ON COLUMN {staging}.platform IS '브랜드 (flows.common.platform.Platform)';
COMMENT ON COLUMN {staging}.idx IS '사이트가 준 지점 식별자. platform 안에서만 유일';
COMMENT ON COLUMN {staging}.name IS '지점 이름, 사이트 원문';
COMMENT ON COLUMN {staging}.address IS '주소, 사이트 원문 (해석하지 않음)';
COMMENT ON COLUMN {staging}.coordinate_source IS '좌표 출처. official / kakao / NULL (좌표 없음)';
COMMENT ON COLUMN {staging}.collected_at IS '수집 시각 (KST 벽시계, 시간대 없음)';
COMMENT ON COLUMN {staging}.source_dt IS '어느 수집 사이클에서 왔나. 오늘이 아니면 그 브랜드는 이전 사이클로 대신한 것';
COMMENT ON COLUMN {staging}.b_code IS '법정동 코드 10자리 (Kakao coord2regioncode). 실패하면 NULL';
COMMENT ON COLUMN {staging}.sido_name IS 'Kakao 가 준 시도 이름 (예: 서울특별시). b_code 앞 2자리. 운영 확인용, 정본은 tb_legal_dong';
COMMENT ON COLUMN {staging}.sgg_name IS 'Kakao 가 준 시군구 이름 (예: 강남구, 수원시 영통구). b_code 앞 5자리. 세종은 NULL. 운영 확인용';
COMMENT ON COLUMN {staging}.umd_name IS 'Kakao 가 준 읍면동 이름 (예: 역삼동). b_code 앞 8자리. 운영 확인용';
COMMENT ON COLUMN {staging}.geocode_status IS 'ok Kakao 응답 / reused 직전 세대 재사용 / no_coordinate 좌표 없음 / failed 좌표는 있으나 Kakao 실패';
COMMENT ON COLUMN {staging}.enriched_at IS '보강 시각 (KST 벽시계, 시간대 없음)';
"""


def read_current() -> dict[tuple[str, str], EnrichedStore]:
    """현재 세대를 (platform, idx) 로 읽는다. 테이블이 없으면 빈 dict.

    첫 실행이거나 손으로 지운 뒤라면 없는 것이 정상이다. 그때는 전 지점을
    Kakao 에 묻는다. 컬럼이 지금 스키마와 다른 세대(컬럼 이름을 바꾸기 전 것)도
    같게 다룬다. 한 번 전 지점을 물면 다음 세대부터 다시 재사용된다.
    """
    logger = get_run_logger()

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (TABLE,))
            if cursor.fetchone()[0] is None:
                return {}
            try:
                cursor.execute(f"SELECT {', '.join(COLUMNS)} FROM {TABLE}")
            except errors.UndefinedColumn as error:
                logger.warning(
                    "%s 의 컬럼이 지금 스키마와 달라 재사용하지 않습니다: %s", TABLE, error
                )
                connection.rollback()
                return {}
            rows = [
                EnrichedStore(**dict(zip(COLUMNS, values)))
                for values in cursor.fetchall()
            ]
    return {(row.platform, row.idx): row for row in rows}


@task
def swap_table(stores: list[EnrichedStore], *, cycle: date) -> dict[str, int]:
    """새 테이블을 채워 기존 것과 바꿔치우고 건수를 돌려준다.

    빈 목록이면 막는다. 하한(MIN_EXPECTED)은 호출부(flow)가 먼저 본다. 여기서는
    0건만 막아 다른 경로로 불려도 빈 테이블을 들이지 않게 한다.

    돌려주는 unknown_codes 는 tb_legal_dong 에 없는 b_code 의 건수다. 개편 뒤
    마스터가 낡았거나 Kakao 가 새 코드를 먼저 쓰는 경우이고, 그 부스는 검색에서
    조용히 빠지므로 flow 가 경고로 남긴다. 마스터 테이블이 없으면 -1 이다.
    """
    logger = get_run_logger()

    if not stores:
        raise ValueError(
            "적재할 지점이 없습니다. 0건짜리 테이블로 바꿔치우면 검색이 죽으므로 멈춥니다."
        )

    staging = staging_name(cycle)
    now = datetime.now(KST)

    with connect() as connection:
        with connection.cursor() as cursor:
            # 트랜잭션 첫 문장이어야 한다. 아래 DROP 도 ACCESS EXCLUSIVE 락을 잡는다.
            cursor.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")

            cursor.execute(f"DROP TABLE IF EXISTS {staging}")

            # 이전 세대를 여기서 치운다. 스왑 직전에 치우면 같은 날 재실행할 때 새
            # 인덱스 이름이 _prev 쪽과 부딪혀 번호가 붙고 실행마다 올라간다.
            cursor.execute(f"DROP TABLE IF EXISTS {PREV_TABLE}")

            cursor.execute(_ddl(staging))

            with cursor.copy(
                f"COPY {staging} ({', '.join(COLUMNS)}) FROM STDIN"
            ) as copy:
                for store in stores:
                    copy.write_row(astuple(store))

            cursor.execute(_indexes(staging))
            cursor.execute(_comments(staging))

            # COMMENT 는 유틸리티 문이라 파라미터를 받지 못한다. 리터럴 조립은
            # psycopg 에 맡긴다.
            cursor.execute(
                sql.SQL("COMMENT ON TABLE {} IS {}").format(
                    sql.Identifier(staging),
                    sql.Literal(
                        f"지점 법정동 보강 결과 (enrich). 사이클 {cycle}, "
                        f"{now:%Y-%m-%d %H:%M} 적재"
                    ),
                )
            )

            cursor.execute(f"SELECT count(*) FROM {staging}")
            loaded = cursor.fetchone()[0]

            cursor.execute("SELECT to_regclass(%s)", (LEGAL_DONG_TABLE,))
            unknown_codes = -1
            if cursor.fetchone()[0] is not None:
                cursor.execute(
                    f"SELECT count(*) FROM {staging} s "
                    f"LEFT JOIN {LEGAL_DONG_TABLE} d ON d.code = s.b_code "
                    "WHERE s.b_code IS NOT NULL AND d.code IS NULL"
                )
                unknown_codes = cursor.fetchone()[0]

            # 첫 실행에는 바꿔칠 대상이 없다. 없는 테이블을 세면 트랜잭션이 죽는다.
            cursor.execute("SELECT to_regclass(%s)", (TABLE,))
            existed = cursor.fetchone()[0] is not None

            before = 0
            if existed:
                cursor.execute(f"SELECT count(*) FROM {TABLE}")
                before = cursor.fetchone()[0]

            # 여기서부터가 스왑이다. 첫머리의 lock_timeout 이 살아 있다.
            cursor.execute(f"ALTER TABLE IF EXISTS {TABLE} RENAME TO {PREV_TABLE}")
            cursor.execute(f"ALTER TABLE {staging} RENAME TO {TABLE}")

    if existed:
        logger.info(
            "바꿔치기 완료: %s -> %s (%d행). 직전 %d행은 %s 로 밀어 두었습니다.",
            staging,
            TABLE,
            loaded,
            before,
            PREV_TABLE,
        )
    else:
        logger.info(
            "첫 적재: %s -> %s (%d행). 바꿔칠 테이블이 없어 %s 는 만들지 않았습니다.",
            staging,
            TABLE,
            loaded,
            PREV_TABLE,
        )

    return {
        "loaded": loaded,
        "before": before,
        "swapped": int(existed),
        "unknown_codes": unknown_codes,
    }
