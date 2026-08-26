"""역 스냅샷으로 테이블을 새로 만들어 기존 것과 바꿔치운다.

증분 갱신을 하지 않는다. 매 실행이 전량이므로 무엇을 넣고 무엇을 지울지 계산할
이유가 없고, 계산하지 않으면 그 계산이 틀릴 일도 없다. 대신 날짜를 붙인 테이블을
따로 만들어 채우고 이름만 맞바꾼다.

```text
1. tb_subway_station_prev 를 버린다
2. tb_subway_station_20260826 을 만들고 COPY 로 채운다
3. 인덱스를 건다                       <- 채운 다음이어야 빠르다
4. tb_subway_station -> tb_subway_station_prev
5. tb_subway_station_20260826 -> tb_subway_station
```

전부 한 트랜잭션이다. Postgres 는 DDL 도 트랜잭션에 들어가므로 4~5 가 원자적으로
일어난다. 읽는 쪽은 이전 테이블을 끝까지 보다가 커밋 시점에 새 테이블로 넘어가고,
중간 상태를 볼 수 없다.

**`_prev` 를 남기는 것이 바꿔치기의 값이다.** 새 스냅샷이 이상하면
`tb_subway_station` 을 버리고 `_prev` 를 되돌리면 끝난다.

**인덱스 이름은 되돌리지 않는다.** Postgres 에서 테이블 이름을 바꿔도 인덱스
이름은 따라오지 않으므로, 표준 이름으로 맞추려 들면 세대마다 `_prev` 쪽 인덱스와
이름이 부딪힌다. 날짜가 붙은 채로 두면 부딪힐 일이 없고, `\\d tb_subway_station` 이
어느 스냅샷으로 만든 테이블인지 알려준다.

**스왑에는 `lock_timeout` 이 필요하다.** 이름을 바꾸려면 ACCESS EXCLUSIVE 락이
필요한데, 이 락을 기다리는 요청은 뒤이어 오는 읽기까지 자기 뒤에 줄 세운다.
앱이 긴 조회를 물고 있으면 스왑이 기다리는 동안 앱 전체가 멈춘다. 그래서 몇 초
안에 못 잡으면 실패하고 다음 실행에 맡긴다.

법정동(`flows/legal_dong/table.py`)과 같은 절차다. 같은 마스터라서가 아니라 매
실행이 전량 스냅샷이라 같은 답이 나온다.
"""

from datetime import date, datetime, timedelta, timezone

from prefect import get_run_logger, task
from psycopg import sql

from flows.common.postgres import connect
from flows.subway_station.normalize import Station

TABLE = "tb_subway_station"
PREV_TABLE = f"{TABLE}_prev"

# 날짜는 KST 로 끊는다. UTC 로 끊으면 새벽 실행이 전날 이름을 갖는다. 지점 수집의
# 파티션 날짜와 같은 규칙이다.
KST = timezone(timedelta(hours=9))

# 스왑이 락을 못 잡을 때 물러나는 시간. 길게 잡으면 그만큼 앱의 읽기가 우리 뒤에
# 줄 서므로 짧아야 한다.
LOCK_TIMEOUT = "5s"

# DDL 의 컬럼 순서와 같아야 한다. COPY 가 여기 순서로 값을 받으므로 한쪽만 고치면
# 값이 엉뚱한 컬럼에 들어간다. id 는 BIGSERIAL 이라 넘기지 않는다.
COLUMNS = (
    "name",
    "name_en",
    "line_name",
    "line_no",
    "station_no",
    "location",
    "region",
    "address",
    "operator",
    "phone",
    "is_transfer",
    "base_on",
)


def staging_name(day: date) -> str:
    """`tb_subway_station_20260826`. 63자 제한에 한참 못 미친다."""
    return f"{TABLE}_{day:%Y%m%d}"


def _ddl(staging: str) -> str:
    """길이는 실측에 여유를 둔 값이다. 1,098행에서 name 9자, name_en 22자,
    line_name 12자, address 33자, operator 16자였다.

    **PK 를 대리키로 둔다.** 원문 어느 열 조합도 유일하지 않다. 역번호는 중복이
    146건이고 가장 나은 `(노선번호, 역번호)` 조차 5건이 부딪힌다. 원문 열에 PK 를
    걸면 다음 스냅샷에 중복이 하나만 늘어도 적재가 통째로 죽는다.
    """
    return f"""
CREATE TABLE {staging} (
    id          BIGSERIAL,

    -- 검색 결과 한 줄을 이루는 값. 사용자는 "강남"을 검색해 강남 2호선과
    -- 강남 신분당선 중 하나를 고른다.
    name        VARCHAR(60)   NOT NULL,
    name_en     VARCHAR(80),
    line_name   VARCHAR(40)   NOT NULL,

    -- 원문 식별자. 유일하지 않아 키로 쓰지 못하고 값으로만 담는다.
    line_no     VARCHAR(10),
    station_no  VARCHAR(10),

    -- 고른 역 1km 안의 부스를 찾는 기준. 이 데이터의 쓸모다.
    --
    -- 위경도를 컬럼으로 두지 않는다. tb_photo_booth_location 이 location 하나만
    -- 들고 ST_X / ST_Y 로 돌려주고 있어 같은 모양으로 맞춘다. 반경 계산에서 두
    -- 테이블을 섞어 쓰는데 한쪽만 타입이 다르면 변환이 끼고, 변환이 끼면 인덱스가
    -- 죽는다.
    location    geometry(POINT, 4326) NOT NULL,

    -- 동명이역이 19쌍 있다. 노선명만으로는 어느 송정인지 알기 어렵다.
    region      VARCHAR(20),

    address     VARCHAR(200),
    operator    VARCHAR(60),
    phone       VARCHAR(30),
    is_transfer BOOLEAN       NOT NULL,

    -- 원문이 행마다 들고 있는 기준일자
    base_on     DATE,

    PRIMARY KEY (id)
);
"""


def _indexes(staging: str) -> str:
    """COPY 를 끝낸 다음에 건다. 채우면서 갱신하는 것보다 빠르다.

    이름에 날짜가 붙은 채로 둔다. 모듈 docstring 에 이유를 남겼다.
    """
    return f"""
-- 앱 검색의 주 경로. text_pattern_ops 여야 LIKE '강남%' 가 인덱스를 탄다.
-- 기본 연산자 클래스는 콜레이션에 묶여 접두 검색에 쓰이지 않는다.
CREATE INDEX ON {staging} (name text_pattern_ops);

-- location 에는 인덱스를 걸지 않는다. 반경 질의는 이 테이블이 아니라 부스 테이블에
-- 건다. 사용자가 고른 역의 좌표는 여기서 읽어 상수로 넘어가고, 그 상수로 반경 안의
-- 부스를 찾는 것은 tb_photo_booth_location 의 GiST 가 한다. 실행 계획에서
-- idx_photo_booth_location_location 이 잡히는 것을 확인했다.
--
-- 반대 방향(부스마다 가까운 역 찾기)이 생기면 그때 GiST 한 줄을 더한다. 없는 질의를
-- 미리 최적화하면 스왑마다 인덱스를 만드는 값만 낸다.
"""


def _comments(staging: str) -> str:
    return f"""
COMMENT ON COLUMN {staging}.id IS '대리키. 원문에 유일한 열 조합이 없어 둔다';
COMMENT ON COLUMN {staging}.name IS '역명, 끝의 역 을 뗀 형태 (예: 강남). 검색용';
COMMENT ON COLUMN {staging}.name_en IS '영문 역명 (예: Gangnam)';
COMMENT ON COLUMN {staging}.line_name IS '노선명 (예: 2호선, 신분당선). 검색 결과 표시용';
COMMENT ON COLUMN {staging}.line_no IS '원문 노선번호. 노선을 유일하게 식별하지 못한다';
COMMENT ON COLUMN {staging}.station_no IS '원문 역번호. 노선 안에서도 유일하지 않다';
COMMENT ON COLUMN {staging}.location IS '역 좌표 (SRID 4326). 근처 부스를 찾는 기준';
COMMENT ON COLUMN {staging}.region IS '시도 (예: 서울특별시). 동명이역 구분용';
COMMENT ON COLUMN {staging}.address IS '역사 주소';
COMMENT ON COLUMN {staging}.operator IS '운영기관명, 시도 접두를 뗀 형태';
COMMENT ON COLUMN {staging}.phone IS '역사 전화번호';
COMMENT ON COLUMN {staging}.is_transfer IS '환승역 여부';
COMMENT ON COLUMN {staging}.base_on IS '원문 데이터기준일자';
"""


@task
def swap_table(stations: list[Station], *, dataset: str = "") -> dict[str, int]:
    """새 테이블을 채워 기존 것과 바꿔치우고 건수를 돌려준다.

    dataset 은 어느 스냅샷으로 만들었는지다. 테이블 코멘트에 남겨 스왑 뒤에도
    기준일자를 알 수 있게 한다.

    빈 목록이면 막는다. 0건짜리 테이블을 바꿔치우면 역 검색이 통째로 죽고,
    쓸 만한 테이블이 `_prev` 로 밀려난다.
    """
    logger = get_run_logger()

    if not stations:
        raise ValueError(
            "적재할 역이 없습니다. 0건짜리 테이블로 바꿔치우면 검색이 죽으므로 "
            "멈춥니다. 다운로드나 파싱을 확인해야 합니다."
        )

    today = datetime.now(KST).date()
    staging = staging_name(today)

    with connect() as connection:
        with connection.cursor() as cursor:
            # 지난 실행이 스왑 전에 죽었으면 이 이름이 남아 있다. 같은 날 다시
            # 돌릴 수 있어야 하므로 먼저 치운다.
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")

            # 이전 세대도 여기서 치운다. 스왑 직전에 치우면 같은 날 재실행할 때
            # 새 인덱스 이름이 _prev 쪽 이름과 부딪혀 Postgres 가 뒤에 번호를
            # 붙이고, 그 번호가 실행마다 올라간다. 먼저 비우면 이름이 돌아온다.
            # 한 트랜잭션이므로 뒤에서 실패하면 이 삭제도 되돌아간다.
            cursor.execute(f"DROP TABLE IF EXISTS {PREV_TABLE}")

            cursor.execute(_ddl(staging))

            with cursor.copy(
                f"COPY {staging} ({', '.join(COLUMNS)}) FROM STDIN"
            ) as copy:
                for station in stations:
                    copy.write_row(
                        (
                            station.name,
                            station.name_en,
                            station.line_name,
                            station.line_no,
                            station.station_no,
                            f"SRID=4326;POINT({station.lon} {station.lat})",
                            station.region,
                            station.address,
                            station.operator,
                            station.phone,
                            station.is_transfer,
                            station.base_on,
                        )
                    )

            cursor.execute(_indexes(staging))
            cursor.execute(_comments(staging))

            note = f"지하철 역 정보 (역 x 노선). {today} 적재"
            if dataset:
                note += f", 원본 {dataset}"

            # COMMENT 는 유틸리티 문이라 파라미터를 받지 못한다. 값을 리터럴로
            # 조립해야 하며, 직접 문자열을 이어붙이지 않고 psycopg 에 맡긴다.
            cursor.execute(
                sql.SQL("COMMENT ON TABLE {} IS {}").format(
                    sql.Identifier(staging), sql.Literal(note)
                )
            )

            cursor.execute(f"SELECT count(*) FROM {staging}")
            loaded = cursor.fetchone()[0]

            # 첫 실행에는 바꿔칠 대상이 없다. 없는 테이블을 세면 예외가 나고
            # 트랜잭션이 통째로 죽으므로 카탈로그로 먼저 확인한다.
            cursor.execute("SELECT to_regclass(%s)", (TABLE,))
            existed = cursor.fetchone()[0] is not None

            before = 0
            if existed:
                cursor.execute(f"SELECT count(*) FROM {TABLE}")
                before = cursor.fetchone()[0]

            # 여기서부터가 스왑이다. 락을 못 잡으면 앱을 세우지 않고 물러난다.
            cursor.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")

            cursor.execute(f"ALTER TABLE IF EXISTS {TABLE} RENAME TO {PREV_TABLE}")
            cursor.execute(f"ALTER TABLE {staging} RENAME TO {TABLE}")

    # 첫 실행에는 밀려날 테이블이 없어 _prev 가 만들어지지 않는다. 그런데도
    # "이전 0행을 남겼다"고 적으면 있지도 않은 테이블을 가리키게 된다.
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
            "첫 적재: %s -> %s (%d행). 바꿔칠 테이블이 없어 %s 는 만들지 "
            "않았습니다.",
            staging,
            TABLE,
            loaded,
            PREV_TABLE,
        )

    return {"loaded": loaded, "before": before, "swapped": int(existed)}
