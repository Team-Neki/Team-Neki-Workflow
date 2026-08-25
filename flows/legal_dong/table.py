"""법정동 스냅샷으로 테이블을 새로 만들어 기존 것과 바꿔치운다.

증분 갱신을 하지 않는다. 매 실행이 전량이므로 무엇을 넣고 무엇을 지울지 계산할
이유가 없고, 계산하지 않으면 그 계산이 틀릴 일도 없다. 대신 날짜를 붙인 테이블을
따로 만들어 채우고 이름만 맞바꾼다.

```text
1. tb_legal_dong_prev 를 버린다
2. tb_legal_dong_20260825 를 만들고 COPY 로 채운다
3. 인덱스를 건다                       <- 채운 다음이어야 빠르다
4. tb_legal_dong -> tb_legal_dong_prev
5. tb_legal_dong_20260825 -> tb_legal_dong
```

전부 한 트랜잭션이다. Postgres 는 DDL 도 트랜잭션에 들어가므로 4~5 가 원자적으로
일어난다. 읽는 쪽은 이전 테이블을 끝까지 보다가 커밋 시점에 새 테이블로 넘어가고,
중간 상태를 볼 수 없다.

**`_prev` 를 남기는 것이 바꿔치기의 값이다.** 새 스냅샷이 이상하면
`tb_legal_dong` 을 버리고 `_prev` 를 되돌리면 끝난다. 증분 갱신은 되돌릴 것이
남지 않는다.

**인덱스 이름은 되돌리지 않는다.** Postgres 에서 테이블 이름을 바꿔도 인덱스
이름은 따라오지 않으므로, 표준 이름으로 맞추려 들면 세대마다 `_prev` 쪽 인덱스와
이름이 부딪힌다. 날짜가 붙은 채로 두면 부딪힐 일이 없고, `\\d tb_legal_dong` 이
어느 스냅샷으로 만든 테이블인지 알려준다.

`collected_at` 컬럼을 두지 않는다. 20,561행에 같은 값을 반복하는 것이고, 스왑
뒤에는 테이블 이름에서 날짜가 사라지므로 그 사실은 테이블 코멘트에 남긴다.

**스왑에는 `lock_timeout` 이 필요하다.** 이름을 바꾸려면 ACCESS EXCLUSIVE 락이
필요한데, 이 락을 기다리는 요청은 뒤이어 오는 읽기까지 자기 뒤에 줄 세운다.
앱이 긴 조회를 물고 있으면 스왑이 기다리는 동안 앱 전체가 멈춘다. 그래서 몇 초
안에 못 잡으면 실패하고 다음 실행에 맡긴다.
"""

from datetime import date, datetime, timedelta, timezone

from prefect import get_run_logger, task
from psycopg import sql

from flows.common.postgres import connect
from flows.legal_dong.normalize import LegalDong

TABLE = "tb_legal_dong"
PREV_TABLE = f"{TABLE}_prev"

# 날짜는 KST 로 끊는다. UTC 로 끊으면 새벽 실행이 전날 이름을 갖는다. 지점 수집의
# 파티션 날짜와 같은 규칙이다.
KST = timezone(timedelta(hours=9))

# 스왑이 락을 못 잡을 때 물러나는 시간. 길게 잡으면 그만큼 앱의 읽기가 우리 뒤에
# 줄 서므로 짧아야 한다.
LOCK_TIMEOUT = "5s"

# DDL 의 컬럼 순서와 같아야 한다. COPY 가 여기 순서로 값을 받으므로 한쪽만 고치면
# 값이 엉뚱한 컬럼에 들어간다.
COLUMNS = (
    "code",
    "level",
    "sido_code",
    "sido_name",
    "sgg_code",
    "sgg_name",
    "umd_code",
    "umd_name",
    "ri_code",
    "ri_name",
    "leaf_name",
    "full_name",
    "created_on",
)


def staging_name(day: date) -> str:
    """`tb_legal_dong_20260825`. 63자 제한에 한참 못 미친다."""
    return f"{TABLE}_{day:%Y%m%d}"


def _ddl(staging: str) -> str:
    """길이는 실측에 여유를 둔 값이다. 20,561행에서 full_name 24자, sido_name 9자,
    sgg_name 8자, umd_name 6자, ri_name 7자였다.
    """
    return f"""
CREATE TABLE {staging} (
    -- 무엇인가
    code       CHAR(10)     NOT NULL,
    level      SMALLINT     NOT NULL,

    -- 계층을 위에서 아래로. 코드는 code 의 자리수를 잘라낸 것이라 늘 있고,
    -- 명칭은 그 계층이 없으면 NULL 이다. 그래서 시도 행의 sgg_code 는 000 인데
    -- sgg_name 은 NULL 이다.
    sido_code  CHAR(2)      NOT NULL,   -- 시도
    sido_name  VARCHAR(30)  NOT NULL,
    sgg_code   CHAR(3)      NOT NULL,   -- 시군구
    sgg_name   VARCHAR(30),
    umd_code   CHAR(3)      NOT NULL,   -- 읍면동
    umd_name   VARCHAR(30),
    ri_code    CHAR(2)      NOT NULL,   -- 리
    ri_name    VARCHAR(30),

    -- 위 계층에서 뽑아낸 이름. leaf_name 은 가장 아래 계층의 명칭이고
    -- full_name 은 있는 것을 전부 이은 것이다.
    leaf_name  VARCHAR(30)  NOT NULL,
    full_name  VARCHAR(60)  NOT NULL,

    -- 언제 생겼나
    created_on DATE,

    PRIMARY KEY (code)
);
"""


def _indexes(staging: str) -> str:
    """COPY 를 끝낸 다음에 건다. 채우면서 갱신하는 것보다 빠르다.

    이름에 날짜가 붙은 채로 둔다. 모듈 docstring 에 이유를 남겼다.
    """
    return f"""
-- 앱 검색의 주 경로. text_pattern_ops 여야 LIKE '강남%' 가 인덱스를 탄다.
-- 기본 연산자 클래스는 콜레이션에 묶여 접두 검색에 쓰이지 않는다.
CREATE INDEX ON {staging} (leaf_name text_pattern_ops);

-- 전체 경로로 찾을 때. "서울특별시 강남%" 같은 질의를 받는다.
CREATE INDEX ON {staging} (full_name text_pattern_ops);
"""


def _comments(staging: str) -> str:
    return f"""
COMMENT ON COLUMN {staging}.code IS '법정동코드 10자리 (시도2+시군구3+읍면동3+리2)';
COMMENT ON COLUMN {staging}.level IS '계층 (1 시도, 2 시군구, 3 읍면동, 4 리)';
COMMENT ON COLUMN {staging}.sido_code IS '시도 코드 2자리';
COMMENT ON COLUMN {staging}.sido_name IS '시도 명칭 (예: 서울특별시)';
COMMENT ON COLUMN {staging}.sgg_code IS '시군구 코드 3자리. 시도 행은 000';
COMMENT ON COLUMN {staging}.sgg_name IS '시군구 명칭 (예: 강남구). 세종특별자치시는 NULL';
COMMENT ON COLUMN {staging}.umd_code IS '읍면동 코드 3자리. 시군구 이상 행은 000';
COMMENT ON COLUMN {staging}.umd_name IS '읍/면/동 명칭 (예: 역삼동, 무장면). 시군구 이상 행은 NULL';
COMMENT ON COLUMN {staging}.ri_code IS '리 코드 2자리. 리 아닌 행은 00';
COMMENT ON COLUMN {staging}.ri_name IS '리 명칭 (예: 강남리). 리 아닌 행은 NULL';
COMMENT ON COLUMN {staging}.leaf_name IS '가장 아래 계층의 명칭 (예: 역삼동, 강남구, 무장면). 검색용';
COMMENT ON COLUMN {staging}.full_name IS '전체 명칭 (예: 서울특별시 강남구 역삼동). 표시용';
COMMENT ON COLUMN {staging}.created_on IS '법정동 생성일';
"""


@task
def swap_table(dongs: list[LegalDong], *, dataset: str = "") -> dict[str, int]:
    """새 테이블을 채워 기존 것과 바꿔치우고 건수를 돌려준다.

    dataset 은 어느 스냅샷으로 만들었는지다. 테이블 코멘트에 남겨 스왑 뒤에도
    기준일자를 알 수 있게 한다.

    빈 목록이면 막는다. 0건짜리 테이블을 바꿔치우면 앱 검색이 통째로 죽고,
    쓸 만한 테이블이 `_prev` 로 밀려난다.
    """
    logger = get_run_logger()

    if not dongs:
        raise ValueError(
            "적재할 법정동이 없습니다. 0건짜리 테이블로 바꿔치우면 검색이 "
            "죽으므로 멈춥니다. 다운로드나 파싱을 확인해야 합니다."
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
                for dong in dongs:
                    copy.write_row(
                        (
                            dong.code,
                            dong.level,
                            dong.sido_code,
                            dong.sido_name,
                            dong.sgg_code,
                            dong.sgg_name,
                            dong.umd_code,
                            dong.umd_name,
                            dong.ri_code,
                            dong.ri_name,
                            dong.leaf_name,
                            dong.full_name,
                            dong.created_on,
                        )
                    )

            cursor.execute(_indexes(staging))
            cursor.execute(_comments(staging))

            note = f"법정동 코드 테이블 (현존만). {today} 적재"
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
