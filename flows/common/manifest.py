"""collect 적재물의 위치를 Postgres 에 기록한다.

파티션마다 `_manifest.json` 을 두는 대신 테이블 한 곳에 모은다. 객체로 두면
"이 사이클에 어느 브랜드가 무엇을 남겼나" 를 묻는 데 파티션을 전부 나열해야
하고, 브랜드가 늘수록 목록 호출이 따라 는다. 다음 단계(enrich)가 가장 자주
던지는 질문이 그것이므로 조회 한 번으로 끝나는 쪽에 둔다.

법정동, 지하철 역과 달리 테이블 바꿔치기를 하지 않는다. 그쪽은 매 실행이 전량
스냅샷이라 통째로 갈아끼우는 것이 맞지만, 여기는 실행마다 한 브랜드의 한 줄이
늘 뿐이라 누적이 곧 이력이다.

## 키

실행할 때마다 행을 새로 쌓는다. 덮어쓰지 않으므로 같은 사이클을 두 번 돌리면
행이 둘이고, 그 둘이 곧 이력이다. S3 의 CSV 도 실행 시각 이름으로 전부 남으므로
행과 파일이 1:1 로 맞아 어느 실행이 무엇을 남겼는지 되짚을 수 있다.

따라서 `(platform, target_date)` 는 유일하지 않아 키가 되지 못하고, PK 는 연번
대리키 `id` 다. enrich 가 특정 적재를 가리켜야 할 때 들고 다닐 것도 이 번호다.

**읽는 쪽은 "가장 최근 행" 을 집어야 한다.** 한 사이클에 행이 여럿일 수 있으므로
조건만 걸고 첫 행을 쓰면 오래된 적재를 읽는다. 정렬은 `collected_at` 이 아니라
`id` 로 한다. 같은 초에 두 번 적재되면 시각이 같아 순서가 갈리지 않는다.

## target_date

`collected_at` 은 우리가 언제 받았는지고, `target_date` 는 이번 적재물이 어느
수집 사이클의 것인지다. 둘은 보통 같지만 갈릴 때가 있다.

    월요일 04:00 예약 run 이 워커 부재로 쌓였다가 수요일에 집힘
        collected_at  2026-09-23 15:02 (수)
        target_date   2026-09-21       (월)

    수요일에 손으로 실행
        collected_at  2026-09-23 15:02
        target_date   2026-09-23

지금 도는 flow run 의 예약 시각(`scheduled_start_time`) 을 KST 로 끊어 정한다.
cron 을 역산하지 않는 이유는 `flows/` 가 스케줄을 알면 안 되기 때문이다. 예약
시각은 Prefect 가 run 레코드에 이미 박아둔 값이라 스케줄을 몰라도 읽을 수 있다.

**묶어 도는 쪽은 이 값을 인자로 내려보내야 한다.** `stores_collect` 는 브랜드
flow 를 `ThreadPoolExecutor` 로 부르는데, 스레드를 건너면 Prefect 의 flow run
컨텍스트가 따라가지 않아 브랜드 run 이 서브플로우가 아니라 독립 run 으로 뜬다
(`parent_flow_run_id` 가 `None`). 그래서 브랜드 flow 안에서 이 함수를 부르면
자기 시작 시각이 나오고, 월요일 사이클이 수요일로 기록된다. root 를 따라
올라가는 방법도 부모 연결이 없어 쓸 수 없다. `stores_collect` 가 한 번 정해
`target_date` 인자로 내려보내는 것이 유일하게 맞는 길이다.
"""

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from typing import Any

from prefect.context import FlowRunContext
from prefect.runtime import flow_run

from flows.common.platform import Platform
from flows.common.postgres import connect

TABLE = "tb_store_collect_manifest"

# 이보다 오래된 적재물로는 대신하지 않는다. 무한정 대신하면 파서가 깨진 채로
# 몇 주가 지나도 아무도 눈치채지 못한다. best effort 가 고장을 감추는 장치가
# 되면 안 된다.
MAX_STALE_DAYS = 7

# 파티션 날짜와 같은 규칙이다. UTC 로 끊으면 새벽 실행이 전날 사이클로 들어간다.
KST = timezone(timedelta(hours=9))

# CREATE TABLE IF NOT EXISTS 는 경합에 안전하지 않다. 브랜드 11개가 스레드로
# 겹쳐 도는데 테이블이 없는 첫 실행이면 동시에 만들려다 pg_type 유니크 위반이
# 난다. 권고 락으로 한 줄로 세운다. 값은 이 모듈 전용이라 아무 상수여도 된다.
_DDL_LOCK_KEY = 0x6E656B69_C0115C7

# INSERT 가 채우는 열. 읽을 때는 앞에 id 가 붙는다.
COLUMNS = (
    "platform",
    "target_date",
    "s3_path",
    "store_count",
    "collected_at",
    "flow_run_id",
)

READ_COLUMNS = ("id", *COLUMNS)

_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    -- 적재 한 번을 가리키는 번호. 실행마다 행이 쌓이므로
    -- (platform, target_date) 는 유일하지 않아 키가 되지 못한다.
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- 어느 브랜드의 어느 수집 사이클인가. 조회는 늘 이 둘로 들어온다.
    platform     VARCHAR(30)  NOT NULL,
    target_date  DATE         NOT NULL,

    -- 무엇을 어디에 남겼나. s3_path 는 s3://<버킷>/<키> 전체다. 버킷을 빼면
    -- 읽는 쪽이 환경변수를 알아야 하고, 그러면 행만 보고는 파일을 못 찾는다.
    s3_path      TEXT         NOT NULL,
    store_count  INTEGER      NOT NULL,

    -- 언제 받았나. target_date 와 갈릴 수 있다. 모듈 docstring 참고.
    -- 앱 DB (Team-Neki-Server) 의 다른 테이블처럼 시간대 없는 TIMESTAMP 에 KST
    -- 벽시계를 넣는다. TIMESTAMPTZ 로 두면 세션 시간대(운영 파드는 UTC)로 보여
    -- 앱 쪽 테이블과 나란히 읽을 때 9시간이 어긋난다.
    collected_at TIMESTAMP    NOT NULL,

    -- 적재물에서 실행 로그로 되짚어 가는 고리. 서버 없이 도는 로컬 실행에는
    -- run 이 없을 수 있어 NULL 을 허용한다.
    flow_run_id  UUID
);
"""

# PK 가 연번이라 조회는 둘 다 따로 받쳐야 한다. 앞은 사이클 하나를 통째로 받는
# enrich 의 길이고, 뒤는 브랜드의 최신/직전을 찾는 stores_collect 의 길이다.
# 둘 다 id 를 꼬리에 달아 같은 사이클의 여러 적재 중 최신이 먼저 나오게 한다.
_INDEXES = f"""
CREATE INDEX IF NOT EXISTS ix_{TABLE}_cycle
    ON {TABLE} (target_date, platform, id DESC);
CREATE INDEX IF NOT EXISTS ix_{TABLE}_platform
    ON {TABLE} (platform, target_date DESC, id DESC);
"""

_COMMENTS = f"""
COMMENT ON TABLE {TABLE} IS '지점 수집(collect) 적재물의 위치. 적재 한 번이 한 행이고 덮어쓰지 않는다';
COMMENT ON COLUMN {TABLE}.id IS '적재 일련번호. 같은 (platform, target_date) 안에서 큰 값이 최신';
COMMENT ON COLUMN {TABLE}.platform IS '수집 브랜드 (flows.common.platform.Platform)';
COMMENT ON COLUMN {TABLE}.target_date IS '수집 사이클 일자(KST). 예약 시각 기준이라 늦게 집힌 run 은 collected_at 과 다르다';
COMMENT ON COLUMN {TABLE}.s3_path IS 'CSV 전체 경로 (s3://<버킷>/collect/platform=.../dt=.../<YYYY-MM-DD_HHMMSS>.csv)';
COMMENT ON COLUMN {TABLE}.store_count IS 'CSV 레코드 수. 읽는 쪽이 실제 건수와 대조한다';
COMMENT ON COLUMN {TABLE}.collected_at IS '적재 시각(KST 벽시계, 시간대 없음). CSV 파일명과 같은 시각';
COMMENT ON COLUMN {TABLE}.flow_run_id IS '적재한 Prefect flow run';
"""

# 덮어쓰지 않는다. ON CONFLICT 가 없는 것이 이 테이블의 성격이다.
_INSERT = f"""
INSERT INTO {TABLE} ({", ".join(COLUMNS)})
VALUES (%s, %s, %s, %s, %s, %s)
RETURNING id
"""

_SELECT = f"SELECT {', '.join(READ_COLUMNS)} FROM {TABLE}"


def target_date() -> date:
    """지금 도는 run 이 채우는 수집 사이클의 날짜.

    감싼 flow run 에 `target_date` 파라미터가 있으면 그것이다. `stores_collect`
    가 브랜드 flow 로 내려보낸 값이며, 원문을 남기는 `put_raw` 는 수집기 깊숙이서
    불려 인자로 받을 길이 없어 여기서 읽는다. 그래야 raw 가 CSV 와 같은 파티션에
    들어간다.

    없으면 flow run 의 예약 시각을 KST 로 끊는다. flow run 이 없으면 Prefect 가
    현재 시각을 돌려주므로 그대로 쓴다.

    묶어 도는 flow 는 이 값을 한 번 정해 아래로 내려보내야 한다. 이유는 모듈
    docstring 에 남겼다.
    """
    context = FlowRunContext.get()
    given = context.parameters.get("target_date") if context else None
    if given:
        return given if isinstance(given, date) else date.fromisoformat(str(given))
    return flow_run.scheduled_start_time.astimezone(KST).date()


def ensure_table() -> None:
    """테이블과 인덱스를 만든다. 이미 있으면 아무것도 하지 않는다.

    법정동처럼 매 실행 새로 만들지 않으므로 `DROP` 하지 않는다. 컬럼을 바꿀
    때는 `CREATE TABLE IF NOT EXISTS` 가 기존 테이블을 고치지 않으므로 손으로
    `DROP TABLE` 하거나 마이그레이션을 따로 내야 한다.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (_DDL_LOCK_KEY,))
            cursor.execute(_DDL)
            cursor.execute(_INDEXES)
            cursor.execute(_COMMENTS)


def put_manifest(
    *,
    platform: Platform,
    target_date: date,
    s3_path: str,
    store_count: int,
    collected_at: datetime,
    flow_run_id: str | None = None,
) -> int:
    """적재물 위치를 한 행으로 남기고 그 번호를 돌려준다.

    덮어쓰지 않는다. 같은 사이클을 다시 돌리면 행이 하나 더 쌓이고 이전 행은
    그대로 남아 이력이 된다.

    테이블은 호출부가 `ensure_table()` 로 먼저 마련한다. 여기서 만들면 본문을
    올린 뒤에야 DB 에 처음 닿게 되어, DB 가 죽어 있을 때 행 없는 CSV 만 쌓인다.

    collected_at 은 시간대를 떼고 KST 벽시계로 넣는다. aware 값을 그대로 넣으면
    Postgres 가 세션 시간대로 바꿔 TIMESTAMP 에 담으므로 운영에서는 UTC 가 들어간다.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                _INSERT,
                (
                    str(platform),
                    target_date,
                    s3_path,
                    store_count,
                    collected_at.astimezone(KST).replace(tzinfo=None),
                    flow_run_id,
                ),
            )
            return cursor.fetchone()[0]


def _row(values: tuple[Any, ...] | None) -> dict[str, Any] | None:
    return dict(zip(READ_COLUMNS, values)) if values else None


def read_manifest(platform: Platform, target_date: date) -> dict[str, Any] | None:
    """브랜드 하나의 한 사이클에서 가장 최근 적재를 읽는다. 없으면 None.

    없는 것이 정상이다. 이번 수집이 실패했거나 아직 돌지 않은 브랜드를 묻는
    자리가 그대로 이 함수다. 예외로 막으면 호출부가 매번 감싸야 한다.

    같은 사이클을 여러 번 돌렸으면 행이 여럿이므로 마지막 것을 집는다.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"{_SELECT} WHERE platform = %s AND target_date = %s "
                "ORDER BY id DESC LIMIT 1",
                (str(platform), target_date),
            )
            return _row(cursor.fetchone())


def read_cycle(
    target_date: date,
    *,
    max_stale_days: int = MAX_STALE_DAYS,
    platforms: Iterable[Platform] = Platform,
) -> dict[str, dict[str, Any]]:
    """사이클 하나에서 브랜드마다 무엇을 읽을지 정한다.

    enrich 와 `stores_collect` 가 같이 쓴다. 규칙이 한 곳이어야 수집이 정한 것과
    다음 단계가 보는 것이 어긋나지 않는다.

    브랜드마다 `target_date` 이하의 가장 최근 적재를 집고 며칠 지났는지로 상태를
    매긴다. 결과는 platform 값을 키로 한다.

        ok      이 사이클에 적재됨. manifest 가 그 행, age_days 0
        stale   이전 사이클로 대신함. manifest 가 그 행, age_days 가 며칠 전인지
        failed  max_stale_days 안에 쓸 것이 없음. manifest 는 None

    실행 시점에 기록해 두지 않고 읽을 때 계산한다. 아침에 실패한 브랜드를 오후에
    단독으로 다시 수집하면 기록은 아침 상태에 굳어 있지만 계산은 새 행을 바로
    집는다. 쓰지 않기로 한 경우에도 `stale_target_date` 와 `age_days` 를 돌려줘
    왜 버렸는지 남긴다.

    S3 파티션을 나열하지 않는다. manifest 가 테이블로 옮겨진 뒤로는 파티션이
    있어도 행이 없을 수 있어(적재 중간에 끊긴 경우) 목록과 기록이 어긋난다.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            # DISTINCT ON 이 정렬 첫 열의 값마다 첫 행만 남기므로, 사이클과 id
            # 내림차순이면 브랜드별 최신 적재가 남는다.
            cursor.execute(
                f"SELECT DISTINCT ON (platform) {', '.join(READ_COLUMNS)} "
                f"FROM {TABLE} WHERE target_date <= %s "
                "ORDER BY platform, target_date DESC, id DESC",
                (target_date,),
            )
            latest = {
                row["platform"]: row
                for row in (dict(zip(READ_COLUMNS, values)) for values in cursor.fetchall())
            }

    cycle: dict[str, dict[str, Any]] = {}
    for platform in platforms:
        name = str(platform)
        row = latest.get(name)
        if row is None:
            cycle[name] = {"status": "failed", "manifest": None}
            continue

        age = (target_date - row["target_date"]).days
        if age > max_stale_days:
            cycle[name] = {
                "status": "failed",
                "manifest": None,
                "stale_target_date": row["target_date"],
                "age_days": age,
            }
            continue

        cycle[name] = {
            "status": "ok" if age == 0 else "stale",
            "manifest": row,
            "source_target_date": row["target_date"],
            "age_days": age,
        }
    return cycle
