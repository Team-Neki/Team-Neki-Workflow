"""역 스냅샷으로 테이블을 새로 만들어 기존 것과 바꿔치운다.

매 실행이 전량이라 증분 갱신을 하지 않는다. 무엇을 넣고 지울지 계산하지 않으면 그
계산이 틀릴 일도 없다.

```text
1. tb_subway_station_prev 를 버린다
2. tb_subway_station_20260826 을 만들고 COPY 로 채운다
3. 인덱스를 건다                       <- 채운 다음이어야 빠르다
4. tb_subway_station -> tb_subway_station_prev
5. tb_subway_station_20260826 -> tb_subway_station
```

전부 한 트랜잭션이다. Postgres 는 DDL 도 트랜잭션에 들어가므로 4~5 가 원자적으로
일어나고, 읽는 쪽은 중간 상태를 볼 수 없다. `_prev` 를 한 세대 남기는 것이 값이다.
새 스냅샷이 이상하면 되돌리면 끝난다.

**인덱스 이름은 되돌리지 않는다.** 테이블 이름을 바꿔도 인덱스 이름은 따라오지
않으므로, 표준 이름으로 맞추려 들면 세대마다 `_prev` 쪽과 부딪힌다.

**스왑에 `lock_timeout` 이 필요하다.** ACCESS EXCLUSIVE 를 기다리는 요청은 뒤이어
오는 읽기까지 자기 뒤에 줄 세운다. 못 잡으면 물러나고 다음 실행에 맡긴다.
"""

from datetime import date, datetime, timedelta, timezone

from prefect import get_run_logger, task
from psycopg import sql

from flows.common.postgres import connect
from flows.subway_station.normalize import Station

TABLE = "tb_subway_station"
PREV_TABLE = f"{TABLE}_prev"

# KST 로 끊는다. UTC 로 끊으면 새벽 실행이 전날 이름을 갖는다.
KST = timezone(timedelta(hours=9))

LOCK_TIMEOUT = "5s"

# DDL 의 컬럼 순서와 같아야 한다. COPY 가 여기 순서로 값을 받는다.
COLUMNS = ("name", "line_name", "location")


def staging_name(day: date) -> str:
    return f"{TABLE}_{day:%Y%m%d}"


def _ddl(staging: str) -> str:
    """**PK 는 `(name, line_name)` 이다.** 사용자가 고르는 단위가 그대로 키다.

    연번 대리키는 쓰지 않는다. 매 실행이 테이블을 새로 만들므로 연번은 COPY 순서로
    매겨지고, 원문 앞쪽에 역이 하나 생기면 그 뒤 전부가 밀려 앱이 들고 있던 id 가
    조용히 다른 역을 가리킨다.

    원문 식별자(역번호, 노선번호)도 키가 못 된다. `I4108` 하나에 경의중앙선과
    경춘선이 매달려 있어 부딪히고, 그것으로 키를 잡으면 `광운대 경춘선` 이 검색에서
    사라진다.

    위경도를 컬럼으로 두지 않는 것은 `tb_photo_booth_location` 이 `location` 하나만
    들고 있어서다. 반경 계산에서 두 테이블을 섞어 쓰는데 한쪽만 타입이 다르면 변환이
    끼고, 변환이 끼면 인덱스가 죽는다.
    """
    return f"""
CREATE TABLE {staging} (
    name      VARCHAR(60)  NOT NULL,
    line_name VARCHAR(40)  NOT NULL,
    location  geometry(POINT, 4326) NOT NULL,

    PRIMARY KEY (name, line_name)
);
"""


def _indexes(staging: str) -> str:
    """COPY 를 끝낸 다음에 건다.

    `location` 에는 걸지 않는다. 반경 질의는 이 테이블이 아니라 부스 테이블에 건다.
    """
    return f"""
-- PK 인덱스는 기본 연산자 클래스라 LIKE '강남%' 가 타지 않는다.
CREATE INDEX ON {staging} (name text_pattern_ops);
"""


def _comments(staging: str) -> str:
    return f"""
COMMENT ON COLUMN {staging}.name IS '역명, 끝의 역 을 뗀 형태 (예: 강남). 검색용';
COMMENT ON COLUMN {staging}.line_name IS '노선명 (예: 2호선, 신분당선)';
COMMENT ON COLUMN {staging}.location IS '역 좌표 (SRID 4326). 근처 부스를 찾는 기준';
"""


@task
def swap_table(stations: list[Station], *, dataset: str = "") -> dict[str, int]:
    """새 테이블을 채워 기존 것과 바꿔치우고 건수를 돌려준다.

    빈 목록이면 막는다. 0건짜리로 바꿔치우면 역 검색이 죽고 쓸 만한 테이블이
    `_prev` 로 밀려난다.
    """
    logger = get_run_logger()

    if not stations:
        raise ValueError("적재할 역이 없습니다. 다운로드나 파싱을 확인해야 합니다.")

    today = datetime.now(KST).date()
    staging = staging_name(today)

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")

            # 이전 세대를 스왑 직전이 아니라 여기서 치운다. 나중에 치우면 같은 날
            # 재실행할 때 새 인덱스 이름이 _prev 쪽과 부딪혀 뒤에 번호가 붙고 그
            # 번호가 실행마다 올라간다. 한 트랜잭션이라 실패하면 함께 되돌아간다.
            cursor.execute(f"DROP TABLE IF EXISTS {PREV_TABLE}")

            cursor.execute(_ddl(staging))

            with cursor.copy(
                f"COPY {staging} ({', '.join(COLUMNS)}) FROM STDIN"
            ) as copy:
                for station in stations:
                    copy.write_row(
                        (
                            station.name,
                            station.line_name,
                            f"SRID=4326;POINT({station.lon} {station.lat})",
                        )
                    )

            cursor.execute(_indexes(staging))
            cursor.execute(_comments(staging))

            note = f"지하철 역 정보 (역 x 노선). {today} 적재"
            if dataset:
                note += f", 원본 {dataset}"

            # COMMENT 는 유틸리티 문이라 파라미터를 받지 못한다. 직접 이어붙이지
            # 않고 psycopg 에 맡긴다.
            cursor.execute(
                sql.SQL("COMMENT ON TABLE {} IS {}").format(
                    sql.Identifier(staging), sql.Literal(note)
                )
            )

            cursor.execute(f"SELECT count(*) FROM {staging}")
            loaded = cursor.fetchone()[0]

            # 첫 실행에는 바꿔칠 대상이 없다. 없는 테이블을 세면 트랜잭션이 죽으므로
            # 카탈로그로 먼저 확인한다.
            cursor.execute("SELECT to_regclass(%s)", (TABLE,))
            existed = cursor.fetchone()[0] is not None

            before = 0
            if existed:
                cursor.execute(f"SELECT count(*) FROM {TABLE}")
                before = cursor.fetchone()[0]

            cursor.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            cursor.execute(f"ALTER TABLE IF EXISTS {TABLE} RENAME TO {PREV_TABLE}")
            cursor.execute(f"ALTER TABLE {staging} RENAME TO {TABLE}")

    if existed:
        logger.info(
            "바꿔치기 완료: %s -> %s (%d행). 직전 %d행은 %s 로 밀어 두었습니다.",
            staging, TABLE, loaded, before, PREV_TABLE,
        )
    else:
        logger.info(
            "첫 적재: %s -> %s (%d행). 바꿔칠 테이블이 없어 %s 는 만들지 않았습니다.",
            staging, TABLE, loaded, PREV_TABLE,
        )

    return {"loaded": loaded, "before": before, "swapped": int(existed)}
