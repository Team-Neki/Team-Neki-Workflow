"""좌표를 법정동 코드로 바꾼다.

검색 API 가 부스에서 쓰는 값은 법정동 코드 10자리와 1km 안 역 둘뿐이다. 역은
좌표만으로 index(서버 batch)가 계산하므로, enrich 가 만드는 값은 법정동 코드
하나다. 주소 문자열은 해석하지 않는다. 계층은 코드의 자리수(시도2+시군구3+
읍면동3+리2)에 있고 이름의 정본은 tb_legal_dong 이라, 여기서 시군구를 잘라내거나
"서울" 과 "서울특별시" 를 맞추는 규칙을 만들면 같은 정보를 두 곳에 두게 된다.

Kakao coord2regioncode 를 부스당 한 번 부른다. 절약은 직전 세대 재사용이다.
같은 (platform, idx) 의 좌표가 어제와 같으면 어제 답을 그대로 쓴다. 좌표가 안
바뀐 날은 호출이 변경분만큼만 나고, Kakao 가 죽은 날에도 안 바뀐 지점은 어제
답으로 채워진다.

지오코딩 실패가 flow 실패가 되지 않는다. 그 지점만 b_code 를 비우고 완주한다.
연속으로 GIVE_UP_AFTER 번 실패하면 Kakao 가 죽은 것으로 보고 남은 지점은 묻지
않는다. 재시도 횟수와 timeout, 포기 규칙은 collect 의 좌표 보정(geocode.py)과
같은 상수를 쓴다.
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, fields, replace
from datetime import date, datetime
from typing import Any, Callable, Literal

from prefect import get_run_logger, task

from flows.common import geocode, kakao
from flows.common.manifest import KST
from flows.common.platform import Platform
from flows.common.store import CollectedStore

GeocodeStatus = Literal["ok", "reused", "no_coordinate", "failed"]

# 동시에 보내는 조회 수. QPS 가 미공개라 낮게 잡는다. 재사용이 대부분을 걸러
# 하루 호출이 변경분 수십 건이라 이 값이 실행 시간을 좌우하지 않는다.
WORKERS = 4


@dataclass(frozen=True)
class EnrichedStore:
    """enrich 결과 한 행.

    필드 순서가 곧 CSV 열 순서이자 COPY 열 순서다 (COLUMNS). table.py 의 DDL 도
    같은 순서다. 한쪽만 고치면 값이 엉뚱한 컬럼에 들어간다.

    시각은 앱 DB 규약대로 시간대 없는 KST 벽시계다.
    """

    # collect 가 준 것
    platform: str
    idx: str
    name: str
    address: str | None
    phone: str | None
    longitude: float | None
    latitude: float | None
    coordinate_source: str | None
    collected_at: datetime
    source_dt: date

    # enrich 가 더하는 것
    b_code: str | None
    region_1depth_name: str | None
    region_2depth_name: str | None
    region_3depth_name: str | None
    geocode_status: GeocodeStatus
    enriched_at: datetime


COLUMNS = tuple(field.name for field in fields(EnrichedStore))

# (platform, idx) -> 직전 세대의 행
Previous = dict[tuple[str, str], EnrichedStore]


def from_collect(
    record: dict[str, Any], *, source_dt: date, enriched_at: datetime
) -> EnrichedStore:
    """collect CSV 한 줄을 판정 전 행으로 옮긴다.

    collected_at 은 CSV 에 시간대가 붙은 ISO 문자열로 있다. 앱 DB 규약대로 KST
    벽시계로 바꾸고 시간대를 뗀다. 판정 전이라 status 는 failed 로 둔다.
    """
    collected_at = (
        datetime.fromisoformat(record["collected_at"])
        .astimezone(KST)
        .replace(tzinfo=None)
    )
    return EnrichedStore(
        platform=record["platform"],
        idx=record["idx"],
        name=record["name"],
        address=record.get("address"),
        phone=record.get("phone"),
        longitude=record.get("longitude"),
        latitude=record.get("latitude"),
        coordinate_source=record.get("coordinate_source"),
        collected_at=collected_at,
        source_dt=source_dt,
        b_code=None,
        region_1depth_name=None,
        region_2depth_name=None,
        region_3depth_name=None,
        geocode_status="failed",
        enriched_at=enriched_at,
    )


def reusable(store: EnrichedStore, previous: EnrichedStore | None) -> bool:
    """직전 세대의 답을 그대로 써도 되는가.

    좌표가 같고 직전에 코드가 있어야 한다. 직전이 failed 였으면 좌표가 같아도
    다시 묻는다. 그날 Kakao 가 죽어서 비었을 수 있다. float 동등 비교다. CSV 와
    DOUBLE PRECISION 왕복이 정확하므로 오차를 두지 않는다.
    """
    return (
        previous is not None
        and previous.b_code is not None
        and store.longitude is not None
        and store.latitude is not None
        and store.longitude == previous.longitude
        and store.latitude == previous.latitude
    )


def _retry(call: Callable[[], Any]) -> Any:
    """Kakao 조회 하나를 몇 번 다시 해보고 끝내 실패하면 예외를 올린다.

    geocode._lookup 과 같은 규칙이다. 예외를 삼키지 않는 이유도 같다. "안 잡힌
    지점" 과 "조회가 안 되는 상황" 이 다르고, 뒤는 호출부가 세어 포기한다.
    """
    for attempt in range(1, geocode.LOOKUP_ATTEMPTS + 1):
        try:
            return call()
        except Exception:
            if attempt == geocode.LOOKUP_ATTEMPTS:
                raise
            time.sleep(attempt * 2)
    return None


def resolve(
    store: EnrichedStore, previous: EnrichedStore | None, *, stop: threading.Event
) -> tuple[EnrichedStore, Exception | None]:
    """지점 하나의 법정동을 정한다. 조회가 끝내 실패하면 그 예외를 행과 함께 돌려준다.

    예외를 올리지 않고 돌려주는 이유는 폴백 좌표 때문이다. 좌표가 없던 지점이
    geocode.locate 로 좌표를 얻은 뒤 법정동 조회에서 실패하면 그 좌표는 행에
    남아야 한다. 올려 버리면 호출부는 조회 전 행밖에 몰라 좌표를 잃는다.

    stop 이 걸려 있으면 Kakao 를 부르지 않고 빈 채로 돌려준다. 키가 없거나
    연속 실패로 포기한 뒤의 지점이 여기로 온다. 재사용은 Kakao 없이도 된다.
    """
    if reusable(store, previous):
        return replace(
            store,
            b_code=previous.b_code,
            region_1depth_name=previous.region_1depth_name,
            region_2depth_name=previous.region_2depth_name,
            region_3depth_name=previous.region_3depth_name,
            geocode_status="reused",
        ), None

    longitude, latitude = store.longitude, store.latitude
    coordinate_source = store.coordinate_source
    missing = longitude is None or latitude is None

    if stop.is_set():
        status = "no_coordinate" if missing else "failed"
        return replace(store, geocode_status=status), None

    if missing:
        # collect 가 못 채운 좌표를 한 번 더 찾는다. 수집 때 Kakao 가 죽어 있던
        # 경우다. 규칙(주소검색 뒤 키워드검색)은 collect 의 것을 그대로 쓴다.
        try:
            found = geocode.locate(
                CollectedStore(
                    platform=Platform(store.platform),
                    idx=store.idx,
                    name=store.name,
                    address=store.address,
                )
            )
        except Exception as error:
            return replace(store, geocode_status="no_coordinate"), error
        if found is None:
            return replace(store, geocode_status="no_coordinate"), None
        longitude, latitude, _ = found
        coordinate_source = "kakao"

    coordinates = {
        "longitude": longitude,
        "latitude": latitude,
        "coordinate_source": coordinate_source,
    }
    try:
        document = _retry(
            lambda: kakao.coord2regioncode(
                longitude, latitude, timeout=geocode.LOOKUP_TIMEOUT
            )
        )
    except Exception as error:
        return replace(store, geocode_status="failed", **coordinates), error
    if document is None:
        return replace(store, geocode_status="failed", **coordinates), None

    return replace(
        store,
        b_code=document["code"],
        region_1depth_name=document.get("region_1depth_name") or None,
        region_2depth_name=document.get("region_2depth_name") or None,
        region_3depth_name=document.get("region_3depth_name") or None,
        geocode_status="ok",
        **coordinates,
    ), None


def mismatched(store: EnrichedStore) -> bool:
    """원문 주소에 Kakao 시군구의 첫 토큰이 없으면 True.

    사이트 좌표가 옆 건물이나 옆 동네를 찍은 경우를 드러내는 경고용이다. 느슨한
    비교이고 주소를 해석하지 않는다. 특례시 일반구("수원시 영통구")는 첫 토큰인
    시 이름으로 비교한다.
    """
    if not store.address or not store.region_2depth_name:
        return False
    return store.region_2depth_name.split()[0] not in store.address


def next_failures(
    failures: int, *, error: Exception | None, status: GeocodeStatus, skipped: bool
) -> int:
    """연속 실패 수의 다음 값.

    조회가 끝내 실패했으면 하나 는다. Kakao 가 실제로 답했으면 0 이다. 법정동
    문서가 없어 failed 이거나 주소검색이 0건이라 no_coordinate 여도 Kakao 는 살아
    있는 것이다. reused 와 stop 뒤에 건너뛴 지점은 Kakao 를 부르지 않았으므로
    그대로 둔다. 여기서 0 으로 되돌리면 장애를 가린다.
    """
    if error is not None:
        return failures + 1
    if skipped or status == "reused":
        return failures
    return 0


@task
def enrich_stores(stores: list[EnrichedStore], previous: Previous) -> list[EnrichedStore]:
    """전 지점의 법정동을 정해 새 목록을 돌려준다. 입력 순서를 지킨다.

    task 하나다. 지점마다 task 를 만들면 1,653개의 task run 이 Prefect API 를
    누르고 UI 에서 flow run 이 묻힌다. 재시도도 붙이지 않는다. 지점마다 이미
    다시 해보고, task 를 통째로 다시 돌리면 호출을 처음부터 되풀이한다.

    쓰레드 안에서는 로그를 남기지 않는다. Prefect 의 run 컨텍스트가 쓰레드를
    따라가지 않는다. 결과와 예외를 모아 여기서 남긴다.
    """
    logger = get_run_logger()

    stop = threading.Event()
    has_key = bool(os.environ.get(kakao.API_KEY_ENV))
    if not has_key:
        logger.warning(
            "%s 가 없어 Kakao 를 부르지 않습니다. 직전 세대와 좌표가 같은 지점만 채웁니다.",
            kakao.API_KEY_ENV,
        )
        stop.set()

    failures = 0
    lock = threading.Lock()

    def work(store: EnrichedStore) -> tuple[EnrichedStore, Exception | None]:
        nonlocal failures
        # resolve 가 stop 을 보기 전에 읽는다. 이미 걸려 있었으면 Kakao 를 부르지
        # 않은 지점이라 연속 실패를 끊는 근거가 못 된다.
        skipped = stop.is_set()
        key = (store.platform, store.idx)
        result, error = resolve(store, previous.get(key), stop=stop)
        with lock:
            failures = next_failures(
                failures, error=error, status=result.geocode_status, skipped=skipped
            )
            if failures >= geocode.GIVE_UP_AFTER:
                stop.set()
        return result, error

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        outcomes = list(pool.map(work, stores))

    for store, error in outcomes:
        if error is not None:
            logger.warning("%s %s 법정동 조회 실패: %s", store.platform, store.name, error)
    if has_key and stop.is_set():
        logger.error(
            "연속 %d번 실패해 남은 지점은 조회하지 않았습니다. Kakao 상태를 확인하세요.",
            geocode.GIVE_UP_AFTER,
        )

    return [store for store, _ in outcomes]
