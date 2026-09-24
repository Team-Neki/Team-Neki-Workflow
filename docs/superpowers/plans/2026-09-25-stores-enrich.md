# 지점 법정동 보강(enrich) flow 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** collect 가 남긴 지점 좌표에 Kakao `coord2regioncode` 로 법정동 코드를 붙여 S3 `enrich/dt=` 파티션과 Postgres `tb_photo_booth_enriched` 세대로 남기고, 끝나면 서버 batch 의 색인 잡을 k8s Job 으로 띄우는 `stores-enrich` flow 를 만든다.

**Architecture:** collect 와 별도 deployment(05:00 KST)로 돌며 `manifest.read_cycle` 이 정한 브랜드별 최신 CSV 를 읽는다. 직전 세대와 좌표가 같은 지점은 Kakao 없이 답을 재사용하고, 나머지만 쓰레드 4개로 조회한다. 적재는 `legal_dong/table.py` 와 같은 테이블 바꿔치기이고 800건 미만이면 바꿔치지 않는다. 색인 Job 은 `NEKI_BATCH_IMAGE` 환경변수가 있을 때만 띄운다.

**Tech Stack:** Python 3.13, Prefect 3.8.5, prefect-kubernetes 0.7.12 (베이스 이미지와 같은 버전), httpx, psycopg 3, boto3, pytest (dev)

**Spec:** `docs/superpowers/specs/2026-09-25-stores-enrich-design.md`. 정책 정본은 Task 9 에서 `docs/spec/enrich-pipeline.md` 로 옮긴다.

**Tickets:** BACKEND-64 (이 저장소), BACKEND-143 (GitOps, Task 11). 브랜치 `feature/BACKEND-64-stores-enrich` 는 이미 만들어져 있다.

**Conventions (AGENTS.md):** 코드에 이모지 없음. flow.py 만 task 모듈을 import 하고 반대는 없음. 시각은 시간대 없는 `TIMESTAMP` 에 KST 벽시계. cron 은 `timezone="Asia/Seoul"`. 새 환경변수는 GitOps `workflow-secret.example.yaml` 에도 적음. 명령은 `make` 를 거치며 `.env` 가 있어야 API 키가 들어감.

---

## 파일 구조

| 파일 | 역할 |
|---|---|
| `pyproject.toml` (수정) | `prefect-kubernetes==0.7.12`, dev 그룹 `pytest` |
| `Makefile` (수정) | `check` 에 pytest, `enrich` 타깃 |
| `.env.example` (수정) | `NEKI_BATCH_IMAGE` 안내 |
| `flows/common/kakao.py` (수정) | `REGION_URL`, `coord2regioncode()` |
| `flows/common/storage.py` (수정) | `ENRICH_PREFIX`, `enrich_partition()`, `put_enriched()` |
| `flows/stores_enrich/__init__.py` (생성) | flow 재노출 |
| `flows/stores_enrich/region.py` (생성) | `EnrichedStore`, `COLUMNS`, `from_collect`, `reusable`, `resolve`, `mismatched`, `enrich_stores` |
| `flows/stores_enrich/table.py` (생성) | `read_current`, `swap_table` |
| `flows/stores_enrich/index_job.py` (생성) | `manifest`, `run_search_index` |
| `flows/stores_enrich/flow.py` (생성) | `stores_enrich` flow, `MIN_EXPECTED` |
| `deployments/stores_enrich.py` (생성) | cron 05:00 KST, `run_index=False` 파라미터 |
| `tests/test_stores_enrich.py` (생성) | 순수 판정 테스트 |
| `docs/spec/enrich-pipeline.md` (생성) | 정책 정본, anchor |
| `docs/spec/collect-pipeline.md` (수정) | "enrich 가 아직 없다" 서술 셋 |
| `AGENTS.md`, `README.md` (수정) | 구조, 검증, 실행 목록 |

---

### Task 1: 의존성 추가

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: pyproject.toml 에 의존성을 더한다**

`dependencies` 의 `"openpyxl>=3.1.0",` 줄 뒤에 추가:

```toml
    # enrich 가 색인 Job 을 k8s 에 띄운다. Dockerfile 베이스 이미지
    # (prefecthq/prefect:3.8.5-*-kubernetes)에 든 것과 같은 버전이어야 한다.
    # 다르면 이미지 안의 prefect-kubernetes 가 교체되어 worker 가 깨진다.
    "prefect-kubernetes==0.7.12",
```

`[build-system]` 앞에 추가:

```toml
[dependency-groups]
# 운영 이미지에는 들어가지 않는다 (Dockerfile 이 --no-dev 로 export 한다).
dev = ["pytest>=8.0"]
```

- [ ] **Step 2: lock 을 갱신하고 설치한다**

Run: `uv lock && uv sync`
Expected: `uv.lock` 에 `name = "prefect-kubernetes"` 와 `version = "0.7.12"`, `name = "pytest"` 가 생긴다. 지우고 다시 만들지 않는다.

Run: `grep -n -A1 'name = "prefect-kubernetes"' uv.lock | head -3`
Expected: `version = "0.7.12"`

- [ ] **Step 3: 임포트를 확인한다**

Run: `uv run python -c "import prefect_kubernetes, pytest; print(prefect_kubernetes.__version__)"`
Expected: `0.7.12`

Run: `make check`
Expected: `deployment 15 건` 과 `spec anchor N개 중 N개 일치`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: enrich 가 쓸 prefect-kubernetes 와 dev 의존 pytest 를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Kakao coord2regioncode 클라이언트

**Files:**
- Modify: `flows/common/kakao.py`

- [ ] **Step 1: URL 상수를 더한다**

`ADDRESS_URL = ...` 줄 뒤에:

```python
REGION_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
```

- [ ] **Step 2: 함수를 더한다**

`search_keyword` 함수 뒤, `_drain` 앞에:

```python
def coord2regioncode(
    longitude: float, latitude: float, *, timeout: float = 20.0
) -> dict[str, Any] | None:
    """좌표가 속한 법정동 문서 하나. 바다처럼 행정구역이 없으면 None.

    응답에는 법정동(B)과 행정동(H) 문서가 같이 오는데 검색이 쓰는 것은 법정동
    코드라 B 만 돌려준다. 문서의 code 가 10자리 법정동 코드이고
    region_1depth_name 부터 3depth 까지가 시도, 시군구, 읍면동 이름이다.

    search_address 와 같은 이유로 @task 로 감싸지 않는다. 지점 수만큼 호출된다.
    """
    payload = get(REGION_URL, {"x": longitude, "y": latitude}, timeout=timeout)
    for document in payload.get("documents") or []:
        if document.get("region_type") == "B":
            return document
    return None
```

- [ ] **Step 3: 실제로 한 번 불러 본다 (.env 에 KAKAO_API_KEY 필요)**

Run:
```bash
uv run --env-file .env python -c "
from flows.common.kakao import coord2regioncode
d = coord2regioncode(127.0276, 37.4979)
print(d['code'], d['region_1depth_name'], d['region_2depth_name'], d['region_3depth_name'])"
```
Expected: `1168010100 서울특별시 강남구 역삼동`

- [ ] **Step 4: Commit**

```bash
git add flows/common/kakao.py
git commit -m "feat: Kakao coord2regioncode 클라이언트를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: 판정 로직 region.py (테스트 먼저)

**Files:**
- Create: `tests/test_stores_enrich.py`
- Create: `flows/stores_enrich/region.py`
- Modify: `Makefile` (`check` 에 pytest)

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`tests/test_stores_enrich.py`:

```python
"""enrich 의 순수 판정만 확인한다. Kakao 와 DB 는 부르지 않는다."""

import threading
from datetime import date, datetime

from flows.stores_enrich.region import (
    EnrichedStore,
    from_collect,
    mismatched,
    resolve,
    reusable,
)

NOW = datetime(2026, 9, 25, 5, 0)


def store(**overrides) -> EnrichedStore:
    base = dict(
        platform="PHOTOISM",
        idx="1",
        name="포토이즘 역삼점",
        address="서울 강남구 역삼동 1",
        phone=None,
        longitude=127.03,
        latitude=37.5,
        coordinate_source="official",
        collected_at=NOW,
        source_dt=date(2026, 9, 25),
        b_code=None,
        region_1depth_name=None,
        region_2depth_name=None,
        region_3depth_name=None,
        geocode_status="failed",
        enriched_at=NOW,
    )
    return EnrichedStore(**{**base, **overrides})


def test_from_collect_strips_timezone_to_kst():
    record = {
        "platform": "PHOTOISM",
        "idx": "1",
        "name": "n",
        "address": None,
        "phone": None,
        "longitude": 127.0,
        "latitude": 37.0,
        "coordinate_source": None,
        "collected_at": "2026-09-24T19:23:36+00:00",
    }
    row = from_collect(record, source_dt=date(2026, 9, 25), enriched_at=NOW)
    assert row.collected_at == datetime(2026, 9, 25, 4, 23, 36)
    assert row.collected_at.tzinfo is None
    assert row.geocode_status == "failed"
    assert row.b_code is None


def test_reusable_only_when_same_coordinates_and_previous_has_code():
    previous = store(b_code="1168010100", geocode_status="ok")
    assert reusable(store(), previous)
    assert not reusable(store(longitude=127.04), previous)
    assert not reusable(store(), store(b_code=None, geocode_status="failed"))
    assert not reusable(store(longitude=None, latitude=None), previous)
    assert not reusable(store(), None)


def test_resolve_reuses_without_kakao():
    previous = store(
        b_code="1168010100",
        region_1depth_name="서울특별시",
        region_2depth_name="강남구",
        region_3depth_name="역삼동",
        geocode_status="ok",
    )
    stop = threading.Event()
    stop.set()  # Kakao 를 부르면 안 된다. 불리면 stop 뒤라 빈 채로 돌아온다
    row = resolve(store(), previous, stop=stop)
    assert row.geocode_status == "reused"
    assert row.b_code == "1168010100"
    assert row.region_3depth_name == "역삼동"


def test_resolve_marks_status_when_stopped():
    stop = threading.Event()
    stop.set()
    assert resolve(store(), None, stop=stop).geocode_status == "failed"
    assert (
        resolve(store(longitude=None, latitude=None), None, stop=stop).geocode_status
        == "no_coordinate"
    )


def test_mismatched_compares_first_token_of_sigungu():
    assert not mismatched(store(region_2depth_name="강남구"))
    assert mismatched(store(region_2depth_name="서초구"))
    assert not mismatched(
        store(address="경기 수원시 영통구 1", region_2depth_name="수원시 영통구")
    )
    assert not mismatched(store(address=None, region_2depth_name="강남구"))
    assert not mismatched(store(region_2depth_name=None))
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run pytest -q tests`
Expected: `ModuleNotFoundError: No module named 'flows.stores_enrich'`

- [ ] **Step 3: region.py 를 쓴다**

`flows/stores_enrich/region.py`:

```python
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
) -> EnrichedStore:
    """지점 하나의 법정동을 정한다. 조회가 끝내 실패하면 예외를 올린다.

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
        )

    longitude, latitude = store.longitude, store.latitude
    coordinate_source = store.coordinate_source
    missing = longitude is None or latitude is None

    if stop.is_set():
        return replace(store, geocode_status="no_coordinate" if missing else "failed")

    if missing:
        # collect 가 못 채운 좌표를 한 번 더 찾는다. 수집 때 Kakao 가 죽어 있던
        # 경우다. 규칙(주소검색 뒤 키워드검색)은 collect 의 것을 그대로 쓴다.
        found = geocode.locate(
            CollectedStore(
                platform=Platform(store.platform),
                idx=store.idx,
                name=store.name,
                address=store.address,
            )
        )
        if found is None:
            return replace(store, geocode_status="no_coordinate")
        longitude, latitude, _ = found
        coordinate_source = "kakao"

    coordinates = {
        "longitude": longitude,
        "latitude": latitude,
        "coordinate_source": coordinate_source,
    }
    document = _retry(
        lambda: kakao.coord2regioncode(
            longitude, latitude, timeout=geocode.LOOKUP_TIMEOUT
        )
    )
    if document is None:
        return replace(store, geocode_status="failed", **coordinates)

    return replace(
        store,
        b_code=document["code"],
        region_1depth_name=document.get("region_1depth_name") or None,
        region_2depth_name=document.get("region_2depth_name") or None,
        region_3depth_name=document.get("region_3depth_name") or None,
        geocode_status="ok",
        **coordinates,
    )


def mismatched(store: EnrichedStore) -> bool:
    """원문 주소에 Kakao 시군구의 첫 토큰이 없으면 True.

    사이트 좌표가 옆 건물이나 옆 동네를 찍은 경우를 드러내는 경고용이다. 느슨한
    비교이고 주소를 해석하지 않는다. 특례시 일반구("수원시 영통구")는 첫 토큰인
    시 이름으로 비교한다.
    """
    if not store.address or not store.region_2depth_name:
        return False
    return store.region_2depth_name.split()[0] not in store.address


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
    if not os.environ.get(kakao.API_KEY_ENV):
        logger.warning(
            "%s 가 없어 Kakao 를 부르지 않습니다. 직전 세대와 좌표가 같은 지점만 채웁니다.",
            kakao.API_KEY_ENV,
        )
        stop.set()

    failures = 0
    lock = threading.Lock()

    def work(store: EnrichedStore) -> tuple[EnrichedStore, Exception | None]:
        nonlocal failures
        try:
            result = resolve(store, previous.get((store.platform, store.idx)), stop=stop)
        except Exception as error:
            with lock:
                failures += 1
                if failures >= geocode.GIVE_UP_AFTER:
                    stop.set()
            missing = store.longitude is None or store.latitude is None
            return replace(store, geocode_status="no_coordinate" if missing else "failed"), error
        if result.geocode_status == "ok":
            # Kakao 가 실제로 답한 경우에만 연속 실패를 끊는다. reused 는 Kakao 를
            # 부르지 않아 장애를 가릴 수 있다.
            with lock:
                failures = 0
        return result, None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        outcomes = list(pool.map(work, stores))

    for store, error in outcomes:
        if error is not None:
            logger.warning("%s %s 법정동 조회 실패: %s", store.platform, store.name, error)
    if failures >= geocode.GIVE_UP_AFTER:
        logger.error(
            "연속 %d번 실패해 남은 지점은 조회하지 않았습니다. Kakao 상태를 확인하세요.",
            failures,
        )

    return [store for store, _ in outcomes]
```

- [ ] **Step 4: 테스트를 통과시킨다**

Run: `uv run pytest -q tests`
Expected: `5 passed`

- [ ] **Step 5: Makefile check 에 pytest 를 건다**

`Makefile` 의 `check` 타깃을 이렇게 바꾼다 (help 설명도):

```makefile
check: spec-check ## 임포트와 deployment 수집, spec anchor, 단위 테스트를 확인한다
	@$(UV) run python -c "\
	from deployments import collect; \
	found = list(collect()); \
	print('deployment', len(found), '건'); \
	[print('  ', d.flow_name + '/' + d.name) for d in found]"
	@$(UV) run pytest -q tests
```

Run: `make check`
Expected: 끝에 `5 passed`

- [ ] **Step 6: Commit**

```bash
git add tests/test_stores_enrich.py flows/stores_enrich/region.py Makefile
git commit -m "feat: enrich 판정 로직과 직전 세대 재사용 규칙을 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: 테이블 바꿔치기 table.py

**Files:**
- Create: `flows/stores_enrich/table.py`

- [ ] **Step 1: table.py 를 쓴다**

```python
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
from psycopg import sql

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
    region_1depth_name  VARCHAR(32),
    region_2depth_name  VARCHAR(32),
    region_3depth_name  VARCHAR(32),
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
COMMENT ON COLUMN {staging}.region_1depth_name IS 'Kakao 가 준 시도 이름 (예: 서울특별시). 운영 확인용, 정본은 tb_legal_dong';
COMMENT ON COLUMN {staging}.region_2depth_name IS 'Kakao 가 준 시군구 이름 (예: 강남구, 수원시 영통구). 운영 확인용';
COMMENT ON COLUMN {staging}.region_3depth_name IS 'Kakao 가 준 읍면동 이름 (예: 역삼동). 운영 확인용';
COMMENT ON COLUMN {staging}.geocode_status IS 'ok Kakao 응답 / reused 직전 세대 재사용 / no_coordinate 좌표 없음 / failed 좌표는 있으나 Kakao 실패';
COMMENT ON COLUMN {staging}.enriched_at IS '보강 시각 (KST 벽시계, 시간대 없음)';
"""


def read_current() -> dict[tuple[str, str], EnrichedStore]:
    """현재 세대를 (platform, idx) 로 읽는다. 테이블이 없으면 빈 dict.

    첫 실행이거나 손으로 지운 뒤라면 없는 것이 정상이다. 그때는 전 지점을
    Kakao 에 묻는다.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (TABLE,))
            if cursor.fetchone()[0] is None:
                return {}
            cursor.execute(f"SELECT {', '.join(COLUMNS)} FROM {TABLE}")
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
```

- [ ] **Step 2: DDL 과 COLUMNS 의 순서가 같은지 눈으로 대조한다**

Run: `uv run python -c "from flows.stores_enrich.region import COLUMNS; print(COLUMNS)"`
Expected: `('platform', 'idx', 'name', 'address', 'phone', 'longitude', 'latitude', 'coordinate_source', 'collected_at', 'source_dt', 'b_code', 'region_1depth_name', 'region_2depth_name', 'region_3depth_name', 'geocode_status', 'enriched_at')`. `_ddl` 의 컬럼 순서와 같아야 한다.

- [ ] **Step 3: Commit**

```bash
git add flows/stores_enrich/table.py
git commit -m "feat: enrich 결과를 tb_photo_booth_enriched 세대로 바꿔치우는 적재를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: S3 적재 put_enriched

**Files:**
- Modify: `flows/common/storage.py`
- Modify: `docs/spec/collect-pipeline.md` (anchor 줄 번호만)

- [ ] **Step 1: docstring 과 상수를 더한다**

모듈 docstring 의 "레이아웃은 이것 하나다." 문단을 이렇게 바꾼다:

```text
레이아웃은 collect 와 enrich 둘이다.

    collect/platform=<브랜드>/dt=<대상 일자>/<실행 시각>.csv
    collect/platform=<브랜드>/dt=<대상 일자>/_raw/<실행 시각>/<이름>
    enrich/dt=<사이클>/<실행 시각>.csv
```

`COLLECT_PREFIX = "collect"` 줄 뒤에:

```python
# enrich 결과. 브랜드 11개가 한 파티션이라 platform= 이 없다.
ENRICH_PREFIX = "enrich"
```

- [ ] **Step 2: 파티션 함수와 적재 task 를 파일 끝에 더한다**

`read_stores` 뒤에:

```python
def enrich_partition(target_date: date) -> str:
    """enrich 파티션. 끝에 슬래시를 붙이지 않는다."""
    return f"{ENRICH_PREFIX}/dt={target_date:%Y-%m-%d}"


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def put_enriched(
    rows: list[Any], *, columns: tuple[str, ...], target_date: date
) -> str:
    """enrich 결과를 사이클 파티션에 CSV 로 남긴다.

    manifest 행을 남기지 않는다. index 는 S3 가 아니라 Postgres 의 현재 세대를
    읽고, S3 는 이력과 재실행 원천이다. 파티션 안에서 파일명이 실행 시각이라
    가장 나중 파일이 곧 그 사이클의 마지막 실행이다.

    열 목록은 호출부가 준다. 이 모듈은 collect 의 스키마만 알고 enrich 의
    스키마는 flows/stores_enrich/region.py 가 정본이다. 날짜와 시각은 ISO 로
    쓴다.
    """
    logger = get_run_logger()
    bucket = _bucket()

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        fields = asdict(row) if is_dataclass(row) else dict(row)
        writer.writerow(
            {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in fields.items()
            }
        )

    key = f"{enrich_partition(target_date)}/{run_at()}.csv"
    uri = f"s3://{bucket}/{key}"
    _client().put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType=_content_type(key),
    )
    logger.info("%s 에 %d건 적재 (사이클 %s)", uri, len(rows), f"{target_date:%Y-%m-%d}")
    return uri
```

`datetime` 은 `date` 의 하위 타입이라 `isinstance(value, date)` 하나로 둘 다 걸린다.

- [ ] **Step 3: 밀린 anchor 를 고친다**

Run: `make spec-check`
Expected: `docs/spec/collect-pipeline.md: flows/common/storage.py:NN 에 ... 이 없습니다, 지금은 MM줄` 이 몇 줄 나온다. 각 anchor 의 줄 번호를 "지금은 MM줄" 값으로 고친다. 심볼은 바뀌지 않았으므로 본문은 그대로 둔다.

Run: `make spec-check`
Expected: `spec anchor N개 중 N개 일치`

- [ ] **Step 4: Commit**

```bash
git add flows/common/storage.py docs/spec/collect-pipeline.md
git commit -m "feat: enrich 결과를 enrich/dt= 파티션에 CSV 로 남기는 적재를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: 색인 Job index_job.py

**Files:**
- Create: `flows/stores_enrich/index_job.py`

- [ ] **Step 1: index_job.py 를 쓴다**

```python
"""enrich 가 끝나면 서버의 검색 색인 잡을 k8s Job 으로 띄우고 끝나기를 기다린다.

색인(BACKEND-65)은 Python 이 아니라 Team-Neki-Server apps/batch 의 Spring Batch
잡(searchIndexJob)이다. 정규화 규칙이 색인과 검색 질의에서 같은 Kotlin 함수여야
하기 때문이다. 이 모듈은 순서만 책임진다. enrich 세대가 바뀐 뒤에 색인이
돌아야 하고, 그 성패가 flow 결과에 보여야 한다.

one-shot 계약(BACKEND-128): 인자 --spring.batch.job.name=searchIndexJob 과
businessDate=<사이클> 로 기동해 잡 하나를 돌리고 종료 코드로 성패를 알린다.
같은 businessDate 로 다시 돌려도 결과가 같다(멱등). 재시도는 처음부터 다시 돈다.

이미지는 GitOps overlays/prefect/images.env 의 NEKI_BATCH_IMAGE 를 flow run 파드
환경변수로 받는다(BACKEND-143). 서버 레포의 deploy-batch 가 그 줄을 갱신한다.
없으면 로컬이거나 GitOps 가 아직이므로 경고만 남기고 건너뛴다.

Job 은 flow run 과 같은 네임스페이스에 뜬다. flow run 파드의 SA prefect-worker
가 jobs 생성과 pods/log 조회를 허용한다. 실패한 Job 은 지우지 않는다. 파드
로그가 원인이고 ttlSecondsAfterFinished 가 하루 뒤 치운다.
"""

import os
from datetime import date
from typing import Any

from prefect import get_run_logger, task
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import KubernetesJob

IMAGE_ENV = "NEKI_BATCH_IMAGE"

NAMESPACE = "prefect"

# 배치가 앱 DB 에 붙을 때 쓰는 값이 있는 Secret. flow run 파드가 envFrom 으로
# 받는 것과 같은 Secret 이다. 통째로 넘기지 않고 필요한 둘만 꺼낸다. batch 파드에
# Kakao 와 AWS 키까지 넘길 이유가 없다.
SECRET = "prefect-workflow"

JOB_NAME = "searchIndexJob"

# 색인은 수천 건이라 초 단위다. 그래도 파드가 안 뜨는 경우(이미지 없음 등)에
# 무한정 기다리지 않게 상한을 둔다.
TIMEOUT_SECONDS = 1800

# 완료된 Job 을 k8s 가 치우는 시간. 실패 파드의 로그를 볼 여유다.
FINISHED_JOB_TTL = 86400


def manifest(image: str, cycle: date, *, run_at: str) -> dict[str, Any]:
    """Job 매니페스트.

    이름이 유일해야 한다. prefect-kubernetes 가 metadata.name 으로 상태를 읽으므로
    generateName 은 못 쓴다. 실행 시각(YYYY-MM-DD_HHMMSS)의 밑줄은 k8s 이름에
    허용되지 않아 하이픈으로 바꾼다.
    """
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"search-index-{run_at.replace('_', '-')}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": FINISHED_JOB_TTL,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "neki-batch",
                            "image": image,
                            "args": [
                                f"--spring.batch.job.name={JOB_NAME}",
                                f"businessDate={cycle:%Y-%m-%d}",
                            ],
                            "env": [
                                {"name": "TZ", "value": "Asia/Seoul"},
                                {
                                    "name": "SPRING_PROFILES_ACTIVE",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": SECRET,
                                            "key": "SPRING_PROFILES_ACTIVE",
                                        }
                                    },
                                },
                                {
                                    "name": "JASYPT_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": SECRET,
                                            "key": "JASYPT_PASSWORD",
                                        }
                                    },
                                },
                            ],
                        }
                    ],
                }
            },
        },
    }


@task
def run_search_index(cycle: date, *, run_at: str) -> bool:
    """색인 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False.

    Job 이 실패하면 wait_for_completion 이 RuntimeError 를 올려 flow 가 실패한다.
    그것이 계약이다. 재시도를 붙이지 않는다. 색인은 멱등이라 flow 를 다시 돌리면
    되고, 그때 enrich 는 재사용으로 Kakao 없이 스왑까지 간다.
    """
    logger = get_run_logger()

    image = os.environ.get(IMAGE_ENV)
    if not image:
        logger.warning(
            "%s 가 없어 색인 Job 을 띄우지 않습니다. 로컬이거나 GitOps 의 neki-images "
            "ConfigMap 이 아직 없는 것입니다.",
            IMAGE_ENV,
        )
        return False

    job = KubernetesJob(
        v1_job=manifest(image, cycle, run_at=run_at),
        credentials=KubernetesCredentials(),
        namespace=NAMESPACE,
        timeout_seconds=TIMEOUT_SECONDS,
        delete_after_completion=False,
    )
    name = job.v1_job["metadata"]["name"]
    logger.info("색인 Job 시작: %s (%s, businessDate=%s)", name, image, cycle)

    run = job.trigger()
    # 파드 로그를 줄 단위로 print 해 flow 로그(log_prints)에 남긴다.
    run.wait_for_completion(print_func=print)

    logger.info("색인 Job 완료: %s", name)
    return True
```

- [ ] **Step 2: 매니페스트 모양을 확인한다**

Run:
```bash
uv run python -c "
import json
from datetime import date
from flows.stores_enrich.index_job import manifest
m = manifest('ghcr.io/team-neki/neki-batch:x', date(2026, 9, 25), run_at='2026-09-25_050112')
print(m['metadata']['name'])
print(m['spec']['template']['spec']['containers'][0]['args'])"
```
Expected:
```text
search-index-2026-09-25-050112
['--spring.batch.job.name=searchIndexJob', 'businessDate=2026-09-25']
```

- [ ] **Step 3: Commit**

```bash
git add flows/stores_enrich/index_job.py
git commit -m "feat: enrich 뒤에 서버 색인 잡을 k8s Job 으로 띄우는 단계를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: flow, deployment, Makefile, .env.example

**Files:**
- Create: `flows/stores_enrich/flow.py`
- Create: `flows/stores_enrich/__init__.py`
- Create: `deployments/stores_enrich.py`
- Modify: `Makefile` (`enrich` 타깃)
- Modify: `.env.example`

- [ ] **Step 1: flow.py 를 쓴다**

```python
"""collect 가 남긴 지점 좌표에 법정동 코드를 붙인다.

collect 와 별도 flow 다. 합치면 enrich 가 죽었을 때 부모 run 을 재시도하는 순간
사이트를 다시 긁는다. 나눠 두면 재시도가 S3 만 다시 읽는다. 순서는 시각으로
맞춘다. collect 04:00 KST, enrich 05:00 KST 이고, enrich 는 그 시점까지 적재된
것만 본다.

무엇을 읽을지는 collect 의 읽기 계약(docs/spec/collect-pipeline.md)대로
manifest.read_cycle 이 정한다. 브랜드마다 대상 일자 이하의 최신 적재를 집고,
7일 넘게 낡은 브랜드는 버린다. 늦게 끝난 collect 는 그날 enrich 에 안 들어가고
전날 것으로 대신하며 source_dt 가 그것을 드러낸다.

산출물은 둘이다. S3 enrich/dt=<사이클>/<실행 시각>.csv 는 이력과 재실행 원천이고,
Postgres tb_photo_booth_enriched 는 index(서버 batch)가 읽는 현재 세대다.
성공하면 색인 Job 을 띄우고 그 종료 코드가 flow 결과가 된다.
"""

from collections import Counter
from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger

from flows.common.manifest import KST, MAX_STALE_DAYS, ensure_table, read_cycle
from flows.common.manifest import target_date as cycle_date
from flows.common.platform import Platform
from flows.common.storage import put_enriched, read_stores, run_at
from flows.stores_enrich.index_job import run_search_index
from flows.stores_enrich.region import (
    COLUMNS,
    EnrichedStore,
    enrich_stores,
    from_collect,
    mismatched,
)
from flows.stores_enrich.table import read_current, swap_table

# 브랜드 11개가 1,653건이다(2026-09-25). 절반 넘게 사라졌다면 S3 를 잘못 읽었거나
# 브랜드 대부분이 7일 넘게 낡아 버려진 것이므로 바꿔치우지 않는다. 브랜드 하나가
# 빠지는 것은 막지 않는다. 그것은 read_cycle 이 일부러 떨어뜨리는 동작이고, 그때
# 스왑을 멈추면 나머지 브랜드까지 갱신이 멈춘다. 브랜드가 늘면 올린다.
MIN_EXPECTED = 800


def _read_inputs(
    cycle: date, *, max_stale_days: int, enriched_at: datetime
) -> list[EnrichedStore]:
    """사이클의 브랜드별 CSV 를 읽어 판정 전 행으로 모은다.

    failed 브랜드는 경고 후 건너뛴다. (platform, idx) 중복은 첫 것만 남긴다.
    결과 테이블의 PK 라 둘 수 없다.
    """
    logger = get_run_logger()

    # 브랜드 전부가 한 번도 적재되지 않았으면 테이블이 없다. 그대로 조회하면
    # UndefinedTable 이 "읽을 것이 없다" 는 진짜 이유를 가린다.
    ensure_table()

    stores: list[EnrichedStore] = []
    seen: set[tuple[str, str]] = set()
    for name, outcome in read_cycle(cycle, max_stale_days=max_stale_days).items():
        if outcome["status"] == "failed":
            logger.warning("%s: %d일 안에 쓸 적재물이 없어 뺍니다.", name, max_stale_days)
            continue

        source_dt = outcome["source_target_date"]
        if outcome["status"] == "stale":
            logger.warning(
                "%s: %s 사이클(%d일 전)로 대신합니다.", name, source_dt, outcome["age_days"]
            )

        for record in read_stores(platform=Platform(name), target_date=source_dt):
            key = (record["platform"], record["idx"])
            if key in seen:
                logger.warning("%s idx=%s 가 중복이라 첫 것만 남깁니다.", *key)
                continue
            seen.add(key)
            stores.append(from_collect(record, source_dt=source_dt, enriched_at=enriched_at))

    return stores


@flow(name="stores-enrich", log_prints=True)
def stores_enrich(
    target_date: date | None = None,
    persist: bool = True,
    run_index: bool = True,
    max_stale_days: int = MAX_STALE_DAYS,
) -> dict[str, Any]:
    """최신 collect 적재물에 법정동 코드를 붙여 S3 와 Postgres 에 남기고 색인을 띄운다.

    target_date 는 사이클 날짜다. 비우면 이 run 의 예약 시각(KST)이고 백필은
    지난 날짜를 준다. persist 를 끄면 S3 와 Postgres 에 쓰지 않고 색인도 띄우지
    않는다. 직전 세대도 읽지 않으므로 전 지점을 Kakao 에 묻는다. run_index 를
    끄면 적재까지만 한다.
    """
    logger = get_run_logger()

    cycle = target_date or cycle_date()
    enriched_at = datetime.now(KST).replace(tzinfo=None)
    started = run_at()

    stores = _read_inputs(cycle, max_stale_days=max_stale_days, enriched_at=enriched_at)
    if not stores:
        raise RuntimeError(
            f"{cycle:%Y-%m-%d} 사이클에 보강할 지점이 없습니다. collect 가 돌았는지 확인하세요."
        )

    previous = read_current() if persist else {}
    enriched = enrich_stores(stores, previous)

    counts = Counter(store.geocode_status for store in enriched)
    logger.info(
        "법정동 보강 %d건: ok %d, reused %d, no_coordinate %d, failed %d",
        len(enriched),
        counts["ok"],
        counts["reused"],
        counts["no_coordinate"],
        counts["failed"],
    )
    for store in enriched:
        if store.geocode_status in ("failed", "no_coordinate"):
            logger.warning(
                "법정동 없음 (%s): %s %s / %s",
                store.geocode_status,
                store.platform,
                store.name,
                store.address,
            )

    suspicious = [s for s in enriched if s.geocode_status == "ok" and mismatched(s)]
    for store in suspicious:
        logger.warning(
            "시군구 불일치: %s %s 주소 '%s' 인데 Kakao 는 '%s'",
            store.platform,
            store.name,
            store.address,
            store.region_2depth_name,
        )

    if len(enriched) < MIN_EXPECTED:
        raise ValueError(
            f"보강 결과가 {len(enriched)}건으로 하한 {MIN_EXPECTED}건에 못 미칩니다. "
            "S3 나 manifest 를 확인해야 합니다. 테이블은 바꿔치우지 않았습니다."
        )

    result: dict[str, Any] = {
        "cycle": f"{cycle:%Y-%m-%d}",
        "count": len(enriched),
        "status": dict(counts),
        "mismatched": len(suspicious),
    }
    if not persist:
        return result

    result["s3_path"] = put_enriched(enriched, columns=COLUMNS, target_date=cycle)

    swapped = swap_table(enriched, cycle=cycle)
    if swapped["unknown_codes"] > 0:
        logger.warning(
            "tb_legal_dong 에 없는 법정동 코드가 %d건입니다. 마스터가 낡았을 수 "
            "있습니다 (legal-dong flow).",
            swapped["unknown_codes"],
        )
    elif swapped["unknown_codes"] < 0:
        logger.warning("tb_legal_dong 이 없어 법정동 코드를 대조하지 못했습니다.")
    result["table"] = swapped

    if run_index:
        result["indexed"] = run_search_index(cycle, run_at=started)

    return result
```

- [ ] **Step 2: __init__.py 를 쓴다**

`flows/stores_enrich/__init__.py`:

```python
"""지점 법정동 코드 보강(enrich) 워크플로."""

from flows.stores_enrich.flow import stores_enrich

__all__ = ["stores_enrich"]
```

- [ ] **Step 3: deployment 를 쓴다**

`deployments/stores_enrich.py`:

```python
"""지점 법정동 보강 deployment.

매일 05:00 KST 에 돈다. stores-collect(04:00 KST)가 그 시점까지 남긴 적재물만
본다. 늦게 끝난 collect 는 그날 enrich 에 안 들어가고 전날 것으로 대신하며,
결과 테이블의 source_dt 가 그것을 드러낸다. collect 가 끝나야 하는 시각(SLO)은
아직 정하지 않았다. 정해지면 이 cron 을 그 뒤로 둔다.

Prefect cron 은 timezone 을 주지 않으면 UTC 라 명시한다.

매월 1일 legal-dong(05:00 KST)과 겹친다. enrich 는 tb_legal_dong 을 대조용으로
SELECT 만 하고 그쪽 스왑은 원자적이라 해가 없다.

flow run 파드가 색인 Job 을 만들려면 SA prefect-worker 가 필요하다. 그 값은
GitOps 의 base job template 기본값에 있어(BACKEND-143) deployment 마다 지정하지
않는다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.stores_enrich import stores_enrich


def build() -> RunnerDeployment:
    return stores_enrich.to_deployment(
        name="stores-enrich",
        schedule=Cron("0 5 * * *", timezone="Asia/Seoul"),
        # BACKEND-65 가 searchIndexJob 을 넣기 전까지 색인 단계를 끈다. 없는 잡
        # 이름이면 batch 가 종료 코드 1 로 죽어 enrich 가 매일 실패한다. 65 가
        # 들어오면 이 줄을 지운다. 수동 실행은 UI 에서 run_index 를 켤 수 있다.
        parameters={"run_index": False},
    )
```

- [ ] **Step 4: Makefile 에 enrich 타깃을 더한다**

`.PHONY` 의 `collect` 뒤에 `enrich` 를 넣고, `collect` 타깃 뒤에:

```makefile
enrich: ## 최신 collect 적재물에 법정동 코드를 붙여 S3 와 Postgres 에 적재한다 (KAKAO_API_KEY, S3, DATABASE_URL 필요)
	@$(UV) run python -c "\
	from flows.stores_enrich import stores_enrich; \
	result = stores_enrich(); \
	print('보강', result['count'], '건', result['status'])"
```

- [ ] **Step 5: .env.example 에 NEKI_BATCH_IMAGE 를 적는다**

파일 끝에:

```bash

# enrich 가 끝나면 띄우는 서버 batch 이미지 (ghcr.io/team-neki/neki-batch:<tag>).
# 운영은 GitOps 의 neki-images ConfigMap 이 flow run 파드에 넣는다. 비우면 색인
# Job 을 띄우지 않고 경고만 남긴다. 로컬은 k8s 가 없으므로 비워 둔다.
NEKI_BATCH_IMAGE=
```

- [ ] **Step 6: 임포트와 deployment 수집을 확인한다**

Run: `make check`
Expected: `deployment 16 건` 에 `stores-enrich/stores-enrich` 가 있고 `5 passed`

- [ ] **Step 7: Commit**

```bash
git add flows/stores_enrich/flow.py flows/stores_enrich/__init__.py deployments/stores_enrich.py Makefile .env.example
git commit -m "feat: 지점 법정동 보강 flow stores-enrich 와 05:00 KST deployment 를 더한다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: 로컬 검증

LocalStack 과 Postgres(Team-Neki-Server local)가 떠 있고 `.env` 에 `KAKAO_API_KEY`, `DATABASE_URL`, `S3_BUCKET` 이 있어야 한다. `psql` 은 `.env` 의 `DATABASE_URL` 로 붙는다.

- [ ] **Step 1: 테이블이 없는 상태에서 시작한다**

```bash
make localstack
psql "$(grep ^DATABASE_URL .env | cut -d= -f2-)" -c 'DROP TABLE IF EXISTS tb_photo_booth_enriched, tb_photo_booth_enriched_prev'
make collect
```
Expected: `make collect` 가 `성공 11 건`, `합계 1600` 안팎

- [ ] **Step 2: 첫 실행. 전 지점을 Kakao 에 묻는다**

Run: `make enrich`
Expected: 로그에 `법정동 보강 N건: ok N-α, reused 0, ...` (reused 가 0), `첫 적재: tb_photo_booth_enriched_YYYYMMDD -> tb_photo_booth_enriched`, `NEKI_BATCH_IMAGE 가 없어 색인 Job 을 띄우지 않습니다` 경고. 1~2분 걸린다.

- [ ] **Step 3: 둘째 실행. 전부 재사용이어야 한다**

Run: `make enrich`
Expected: `ok 0, reused N` 에 가깝고 몇 초 안에 끝난다. `바꿔치기 완료 ... 직전 N행은 tb_photo_booth_enriched_prev 로`.

- [ ] **Step 4: 적재물을 확인한다**

```bash
make s3-ls | grep enrich/
psql "$(grep ^DATABASE_URL .env | cut -d= -f2-)" -c "
select geocode_status, count(*) from tb_photo_booth_enriched group by 1;
select count(*) from tb_photo_booth_enriched_prev;
select tablename, indexname from pg_indexes where tablename like 'tb_photo_booth_enriched%';
select platform, source_dt, count(*) from tb_photo_booth_enriched group by 1, 2 order by 1;
select platform, name, address, b_code, region_2depth_name from tb_photo_booth_enriched where b_code is null limit 20;"
```
Expected: `enrich/dt=<오늘>/` 에 CSV 둘. 두 테이블이 같은 건수. 인덱스 이름에 `1` 같은 번호가 붙지 않음(`_b_code_idx` 로 끝남). `source_dt` 가 브랜드마다 오늘. `b_code` NULL 행이 있으면 주소를 보고 좌표가 바다나 해외가 아닌지 확인.

- [ ] **Step 5: 키 없이 완주하는지**

Run: `KAKAO_API_KEY= make enrich`
Expected: `KAKAO_API_KEY 가 없어 Kakao 를 부르지 않습니다` 경고, `reused` 가 전 지점(직전 세대가 있으므로), flow 성공.

- [ ] **Step 6: 판정만 도는지**

Run: `uv run --env-file .env python -c "from flows.stores_enrich import stores_enrich; print(stores_enrich(persist=False)['status'])"`
Expected: `{'ok': N, ...}` 이고 S3 와 테이블은 늘지 않는다 (`make s3-ls | grep -c enrich/` 가 그대로).

- [ ] **Step 7: 하한이 막는지**

Run: `uv run --env-file .env python -c "
import flows.stores_enrich.flow as f; f.MIN_EXPECTED = 10**6
from flows.stores_enrich import stores_enrich; stores_enrich()"`
Expected: `ValueError: 보강 결과가 N건으로 하한 1000000건에 못 미칩니다`. 테이블은 그대로다.

---

### Task 9: 문서

**Files:**
- Create: `docs/spec/enrich-pipeline.md`
- Modify: `docs/spec/collect-pipeline.md`
- Modify: `AGENTS.md`
- Modify: `README.md`

- [ ] **Step 1: 정책 정본 docs/spec/enrich-pipeline.md 를 쓴다**

anchor 의 줄 번호는 아래처럼 `:0` 으로 두고 Step 3 에서 `make spec-check` 가 알려주는 값으로 고친다.

````markdown
# 지점 법정동 보강(enrich) 파이프라인 정책

이 문서는 collect 가 남긴 지점 좌표에 법정동 코드를 붙이는 enrich 단계의 정책을
다룹니다. 무엇을 읽고, 어떻게 판정하며, 어디에 어떤 모양으로 남기고, 끝나면
무엇을 띄우는지를 정합니다.

**이 문서가 정본입니다.** 코드와 이 문서가 어긋나면 문서가 맞고 코드가 틀린
것입니다. 동작을 바꾸려면 이 문서를 먼저 고치고 같은 PR 에서 코드를 맞춥니다.
항목마다 `구현` 줄이 구현 위치를 가리키며 `make spec-check` 가 확인합니다. 설계에
이른 이유는 `docs/superpowers/specs/2026-09-25-stores-enrich-design.md` 에 있습니다.

## 범위

다루는 것은 enrich 입니다. 검색 API 가 부스에서 쓰는 값은 법정동 코드 10자리와
1km 안 역 둘뿐이고, 역은 좌표만으로 index(Team-Neki-Server `apps/batch` 의
`searchIndexJob`, BACKEND-65)가 계산하므로 enrich 가 만드는 값은 법정동 코드
하나입니다. 다루지 않는 것은 주소 문자열 해석과 표시용 주소 정리입니다.

## collect 와 별도 flow 이고 순서는 시각으로 맞춘다

`stores_collect` 안에서 부르지 않습니다. 한 flow 면 enrich 가 죽었을 때 재시도가
사이트를 다시 긁습니다. 나눠 두면 재시도가 S3 만 다시 읽습니다.

- collect 04:00 KST, enrich 05:00 KST. enrich 는 그 시점까지 적재된 것만 봄
- 무엇을 읽을지는 collect 의 읽기 계약대로 `read_cycle(target_date)` 가 정함.
  브랜드마다 대상 일자 이하의 최신 manifest 행 하나. `failed` 브랜드는 경고 후 뺌
- 늦게 끝난 collect 는 그날 enrich 에 안 들어가고 전날 것으로 `stale` 대신함.
  결과의 `source_dt` 가 어느 사이클에서 왔는지 드러냄
- collect 가 끝나야 하는 시각(SLO)은 아직 없음. 정해지면 cron 을 그 뒤로 둠
- `(platform, idx)` 중복은 첫 것만 남기고 경고. 결과 테이블의 PK 임
- `collected_at` 은 CSV 의 시간대 붙은 ISO 문자열을 KST 벽시계로 바꾸고 시간대를 뗌

구현 : `deployments/stores_enrich.py:0` `schedule=Cron(`,
`flows/stores_enrich/flow.py:0` `def _read_inputs`,
`flows/stores_enrich/region.py:0` `def from_collect`

## 판정

부스당 한 번 Kakao `coord2regioncode` 를 부르고 법정동(`region_type = B`) 문서의
`code` 와 시도, 시군구, 읍면동 이름을 받습니다.

- 재사용 : 직전 세대(현재 `tb_photo_booth_enriched`)와 좌표가 같고 직전에
  `b_code` 가 있으면 Kakao 없이 그 답을 씀. 직전이 `failed` 면 다시 물음
- 폴백 : 좌표가 없는 지점만 collect 의 `geocode.locate` 로 좌표를 얻어 같은
  길로 보냄. 얻은 좌표는 `coordinate_source = kakao` 로 결과에 넣음
- 상태 : `ok` Kakao 응답 / `reused` 재사용 / `no_coordinate` 좌표 없고 폴백 실패 /
  `failed` 좌표는 있으나 Kakao 실패
- 쓰레드 4개. 조회 하나는 3번까지 다시 해보고 연속 3번 실패하면 남은 지점은 묻지
  않음. 상수는 `geocode.py` 것을 씀
- `KAKAO_API_KEY` 가 없으면 재사용만 하고 나머지는 비움. flow 는 완주함
- task 는 하나. 지점마다 task 를 만들지 않음

구현 : `flows/common/kakao.py:0` `def coord2regioncode`,
`flows/stores_enrich/region.py:0` `def reusable`,
`flows/stores_enrich/region.py:0` `def resolve`,
`flows/stores_enrich/region.py:0` `def enrich_stores`,
`flows/stores_enrich/region.py:0` `WORKERS`

## 주소는 해석하지 않는다

주소 문자열을 시도, 시군구 컬럼으로 나누거나 "서울" 과 "서울특별시" 를 맞추지
않습니다. 계층은 코드 10자리(시도2+시군구3+읍면동3+리2)에 있고 이름의 정본은
`tb_legal_dong` 이라 코드로 조인하면 됩니다. Kakao 도 API 마다 표기가 다릅니다.
결과의 `region_*depth_name` 은 Kakao 가 준 문자열 그대로이고 운영 확인용입니다.

경고 둘만 남깁니다. `ok` 행에서 `region_2depth_name` 첫 토큰이 원문 주소에
없으면 시군구 불일치, 스왑 트랜잭션 안에서 `tb_legal_dong` 에 없는 `b_code`
건수. 둘 다 flow 를 막지 않습니다.

구현 : `flows/stores_enrich/region.py:0` `def mismatched`,
`flows/stores_enrich/table.py:0` `LEGAL_DONG_TABLE`

## 산출물 : S3

```text
enrich/dt=<사이클>/<실행 시각>.csv        e.g. enrich/dt=2026-09-25/2026-09-25_050112.csv
```

- 브랜드 11개가 한 파티션. `platform=` 없음
- 열은 Postgres 테이블과 같고 순서는 `EnrichedStore` 필드 순서가 정본
- manifest 행 없음. index 는 Postgres 를 읽고 S3 는 이력과 재실행 원천. 파티션의
  가장 나중 파일이 그 사이클의 마지막 실행

구현 : `flows/common/storage.py:0` `ENRICH_PREFIX`,
`flows/common/storage.py:0` `def put_enriched`,
`flows/stores_enrich/region.py:0` `COLUMNS = tuple(`

## 산출물 : Postgres `tb_photo_booth_enriched`

index 가 읽는 현재 세대입니다. 세대 교체는 `tb_legal_dong` 과 같은 바꿔치기입니다.
사이클 날짜를 붙인 테이블을 COPY 로 채우고 인덱스를 건 뒤 이름을 맞바꾸며 직전은
`_prev` 로 남깁니다. 한 트랜잭션, `lock_timeout 5s`, `_prev` 는 맨 앞에서 치움.

- 하한 800 미만이면 바꿔치우지 않고 예외. 브랜드 하나가 빠지는 것은 막지 않고
  거의 빈 테이블만 막음
- index 와의 계약은 `platform`, `idx`, `name`, `address`, `longitude`, `latitude`,
  `source_dt`, `b_code` 여덟 열. `b_code` 가 NULL 인 행도 남김
- 시각 컬럼은 시간대 없는 `TIMESTAMP` 에 KST 벽시계

구현 : `flows/stores_enrich/table.py:0` `TABLE`,
`flows/stores_enrich/table.py:0` `def swap_table`,
`flows/stores_enrich/table.py:0` `def read_current`,
`flows/stores_enrich/flow.py:0` `MIN_EXPECTED`

## 색인 Job

성공하면 `NEKI_BATCH_IMAGE` 이미지를 `--spring.batch.job.name=searchIndexJob
businessDate=<사이클>` 인자로 k8s Job 에 띄우고 완료를 기다립니다. Job 실패는
flow 실패입니다. 환경변수가 없으면 경고 후 건너뜁니다.

- 이미지는 GitOps `overlays/prefect/images.env` (ConfigMap `neki-images`)
- env 는 `TZ`, 그리고 Secret `prefect-workflow` 의 `SPRING_PROFILES_ACTIVE`,
  `JASYPT_PASSWORD` 둘만
- 완료된 Job 은 지우지 않고 `ttlSecondsAfterFinished` 로 하루 뒤 정리
- BACKEND-65 전까지 deployment 파라미터 `run_index=False`

구현 : `flows/stores_enrich/index_job.py:0` `IMAGE_ENV`,
`flows/stores_enrich/index_job.py:0` `def manifest`,
`flows/stores_enrich/index_job.py:0` `def run_search_index`,
`deployments/stores_enrich.py:0` `parameters={"run_index": False}`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| Postgres | flow 실패 |
| S3 (읽기) | 그 브랜드 예외로 flow 실패. 건수 불일치도 같음 |
| Kakao | 그 지점만 `failed`. 연속 3회면 남은 지점은 묻지 않음. flow 완주 |
| S3 (쓰기) | 재시도 셋 뒤 flow 실패. 스왑 전이라 테이블 그대로 |
| k8s / batch 이미지 | 색인 단계에서 flow 실패. 테이블은 새 세대. 다시 돌리면 재사용으로 스왑 뒤 색인만 다시 |
| `NEKI_BATCH_IMAGE` 없음 | 경고 후 건너뜀. flow 성공 |

## 변경 검증

- `make check` : 임포트, deployment 수집, anchor, pytest
- 판정을 건드렸다면 `make enrich` 를 두 번. 첫 실행은 전부 `ok`, 둘째는 전부
  `reused`. `KAKAO_API_KEY` 를 비우고도 완주해야 함
- 적재를 건드렸다면 테이블이 없는 상태부터. 두 번 돌려 두 테이블 건수가 같고
  인덱스 이름에 번호가 안 붙어야 함
- `persist=False` 로 S3 와 테이블이 늘지 않아야 함

## 정리

enrich 는 05:00 KST 에 manifest 가 가리키는 브랜드별 최신 CSV 를 읽어 좌표를
법정동 코드로 바꿉니다. 좌표가 안 바뀐 지점은 직전 답을 재사용하고 Kakao 가
죽어도 완주합니다. S3 파티션 하나와 Postgres 세대 하나를 남기고 800건 미만이면
바꿔치우지 않으며, 끝나면 서버 색인 잡을 k8s Job 으로 띄웁니다.
````

- [ ] **Step 2: collect-pipeline.md 의 "아직 없다" 서술 셋을 고친다**

"## 범위" 의 `뒤 단계인 enrich 와 index 는 아직 없으며, 이 문서는 그 단계들이 지켜야 할 읽기 계약만 정합니다.` 를:

```markdown
뒤 단계 enrich 는 `docs/spec/enrich-pipeline.md` 가 정본이고 index 는
Team-Neki-Server 의 batch 잡입니다. 이 문서는 그 단계들이 지켜야 할 읽기 계약을
정합니다.
```

"### 좌표 보정만 collect 안에서 합니다" 의 `좌표가 없으면 그 지점이 색인에서 통째로 빠지는데 enrich 가 아직 없어 결측이 방치되기 때문입니다.` 를:

```markdown
좌표가 없으면 그 지점이 색인에서 통째로 빠지기 때문입니다. enrich 가 생긴 뒤에도
유지합니다(BACKEND-64). enrich 의 폴백은 여기서 못 채운 지점만 다시 시도합니다.
```

"## 읽는 쪽 계약 (enrich, index)" 의 `enrich 와 index 는 아직 없습니다. 만들 때 지킬 계약은 셋입니다.` 를:

```markdown
enrich(`flows/stores_enrich`)가 지키는 계약은 셋입니다.
```

- [ ] **Step 3: anchor 를 채운다**

Run: `make spec-check`
Expected: `enrich-pipeline.md` 의 anchor 마다 `... :0 줄이 없습니다` 가 나온다. 각 심볼의 줄을 찾아 넣는다:

```bash
grep -n 'schedule=Cron(\|parameters={"run_index": False}' deployments/stores_enrich.py
grep -n 'def _read_inputs\|MIN_EXPECTED = ' flows/stores_enrich/flow.py
grep -n 'def from_collect\|def reusable\|def resolve\|def enrich_stores\|^WORKERS\|def mismatched\|COLUMNS = tuple(' flows/stores_enrich/region.py
grep -n 'def coord2regioncode' flows/common/kakao.py
grep -n '^LEGAL_DONG_TABLE\|^TABLE\|def swap_table\|def read_current' flows/stores_enrich/table.py
grep -n '^ENRICH_PREFIX\|def put_enriched' flows/common/storage.py
grep -n '^IMAGE_ENV\|def manifest\|def run_search_index' flows/stores_enrich/index_job.py
```

Run: `make spec-check`
Expected: `spec anchor N개 중 N개 일치`

- [ ] **Step 4: AGENTS.md 를 고친다**

"## 정본은 docs/spec 입니다" 의 목록에 한 줄:

```markdown
- `docs/spec/enrich-pipeline.md` : 지점 법정동 보강(enrich). 입력, 재사용, 상태,
  두 산출물, 색인 Job
```

"## 구조" 의 트리에서 `flows/` 아래 `common/` 앞에:

```text
  stores_enrich/          법정동 보강. collect 의 최신 CSV 를 읽어 Kakao 로 b_code 를 붙임
    region.py             판정 (재사용, 폴백, 상태)
    table.py              tb_photo_booth_enriched 바꿔치기
    index_job.py          서버 색인 잡을 k8s Job 으로
```

트리의 `deploy.py` 줄 뒤에:

```text
tests/                    순수 판정 테스트. make check 가 돌림. Kakao 와 DB 는 부르지 않음
```

"## 수집 파이프라인" 절 뒤, "## 마스터 데이터" 앞에 새 절:

```markdown
## 보강 파이프라인 (enrich)

정책은 `docs/spec/enrich-pipeline.md` 가 정본입니다. 코드를 만질 때 바로 부딪히는
것만 추립니다.

- collect 와 별도 flow. `stores_collect` 안에서 부르지 않음. 합치면 enrich 실패
  재시도가 사이트를 다시 긁음. 순서는 cron 시각(04:00 / 05:00 KST)으로 맞춤
- 무엇을 읽을지는 `manifest.read_cycle` 이 정함. enrich 가 따로 정하지 않음
- `EnrichedStore` 의 필드 순서가 CSV 열이자 COPY 열이자 DDL 순서. 필드를 더하면
  `table._ddl` 도 같은 자리에 넣어야 함. 한쪽만 고치면 값이 엉뚱한 컬럼에 들어감
- 주소 문자열을 해석하지 않음. 계층은 `b_code` 자리수에, 이름은 `tb_legal_dong` 에
  있음. 시군구 비교는 경고용 느슨한 토큰 비교 하나뿐
- Kakao 조회는 task 하나 안의 쓰레드. 지점마다 task 를 만들지 않음. 쓰레드 안에서
  `get_run_logger` 를 부르지 않음 (컨텍스트가 따라가지 않음)
- 색인 Job 은 `NEKI_BATCH_IMAGE` 가 있을 때만. 매니페스트는 `generateName` 이 아니라
  `metadata.name` 이어야 함. prefect-kubernetes 가 이름으로 상태를 읽음
- `prefect-kubernetes` 버전은 Dockerfile 베이스와 같아야 함. 다르면 이미지 안의
  것이 교체됨
- BACKEND-65 가 `searchIndexJob` 을 넣기 전까지 deployment 파라미터
  `run_index=False`. 들어오면 그 줄을 지움
```

"## 변경 검증" 의 make 목록에 `make collect` 뒤 `make enrich` 를 넣고, `make legal-dong` 문단 앞에:

```markdown
`enrich` 는 `make collect` 뒤에 돕니다. 판정을 건드렸다면 두 번 돌려 첫 실행이
`ok`, 둘째가 `reused` 인지, `KAKAO_API_KEY` 를 비우고도 완주하는지 봅니다. 적재를
건드렸다면 테이블이 없는 상태부터 확인하고 두 번 돌려 `tb_photo_booth_enriched`
와 `_prev` 가 같은 건수인지, 인덱스 이름에 번호가 붙지 않는지 봅니다. 색인 단계는
로컬에서 `NEKI_BATCH_IMAGE` 가 없어 경고 후 건너뛰는 것이 정상입니다.

```bash
psql -c 'DROP TABLE IF EXISTS tb_photo_booth_enriched, tb_photo_booth_enriched_prev'
make enrich
make enrich
psql -c "select geocode_status, count(*) from tb_photo_booth_enriched group by 1"
```
```

- [ ] **Step 5: README.md 를 고친다**

"## 디렉토리 구조" 트리에 AGENTS.md 와 같은 `stores_enrich/` 와 `tests/` 줄을 넣는다. "### 워크플로 실행" 의 make 목록에 `make collect` 뒤 `make enrich` 를 넣고 한 줄 설명을 붙인다:

```markdown
`enrich` 는 `collect` 가 남긴 브랜드별 최신 CSV 에 Kakao 로 법정동 코드를 붙여
`enrich/dt=` 파티션과 `tb_photo_booth_enriched` 테이블에 적재합니다. 정책은
`docs/spec/enrich-pipeline.md` 에 있습니다.
```

- [ ] **Step 6: Commit**

```bash
make check
git add docs/spec/enrich-pipeline.md docs/spec/collect-pipeline.md AGENTS.md README.md
git commit -m "docs: enrich 파이프라인 정책을 docs/spec 에 정본으로 두고 collect 의 서술을 맞춘다 (BACKEND-64)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: staging 검증과 PR

절차는 `docs/runbook.md` "브랜치를 머지 전에 올려 보기" 와 메모리의 staging 접근 방법을 따른다. staging Prefect 는 SSH 터널(포트 2024, 비밀번호는 Sprint 위키 "prefect tunneling")로만 닿는다.

- [ ] **Step 1: 이미지 빌드와 배포를 확인한다**

```bash
make image
git push -u origin feature/BACKEND-64-stores-enrich
gh workflow run build.yml --ref main -f ref=feature/BACKEND-64-stores-enrich
```
GitOps `worker.yaml` 의 태그가 `0.1.1-<sha7>` 로 바뀌면 노드에서 `kubectl -n argocd annotate app prefect argocd.argoproj.io/refresh=normal --overwrite`. 팀원에게 staging 을 이 브랜치로 돌린다고 알린다.

- [ ] **Step 2: 수동 run 을 만든다**

터널을 열고 `PREFECT_API_URL=http://localhost:30420/api uv run prefect deployment run 'stores-enrich/stores-enrich'`. UI 에서 로그를 본다.
Expected: `법정동 보강 1,6xx건: ok ...`, `첫 적재: ...`, `NEKI_BATCH_IMAGE 가 없어 색인 Job 을 띄우지 않습니다`, flow Completed. 한 번 더 돌려 `reused` 가 전부인지 본다.

- [ ] **Step 3: 노드에서 테이블을 본다**

```bash
psql "$(kubectl -n prefect get secret prefect-workflow -o jsonpath='{.data.DATABASE_URL}' | base64 -d)" -c "
select geocode_status, count(*) from tb_photo_booth_enriched group by 1;
select platform, source_dt, count(*) from tb_photo_booth_enriched group by 1,2 order by 1;
select count(*) from tb_photo_booth_enriched e left join tb_legal_dong d on d.code = e.b_code where e.b_code is not null and d.code is null;"
```
Expected: `failed` 와 `no_coordinate` 가 한 자릿수. `source_dt` 가 오늘. 마지막 count 가 0.

- [ ] **Step 4: main 으로 되돌리고 PR 을 연다**

```bash
gh workflow run build.yml --ref main
gh pr create --title "feat: 지점 법정동 코드 보강 enrich flow 를 더한다 (BACKEND-64)" --body-file - <<'EOF'
## Summary
- collect 의 브랜드별 최신 CSV 를 읽어 Kakao coord2regioncode 로 법정동 코드를 붙이는 `stores-enrich` flow. 05:00 KST
- 직전 세대와 좌표가 같으면 재사용. Kakao 가 죽어도 완주
- S3 `enrich/dt=` CSV 와 Postgres `tb_photo_booth_enriched` 세대 (800건 미만이면 스왑 안 함)
- 끝나면 서버 색인 잡을 k8s Job 으로. `NEKI_BATCH_IMAGE` 가 없으면 건너뜀. BACKEND-65 전까지 `run_index=False`
- 정책 정본 `docs/spec/enrich-pipeline.md`. 설계 `docs/superpowers/specs/2026-09-25-stores-enrich-design.md`

## Test plan
- [ ] `make check` (pytest 5건, anchor)
- [ ] 로컬 `make enrich` 두 번: 첫 실행 ok, 둘째 reused
- [ ] staging 수동 run 두 번, 테이블 상태 확인 (Task 10)

## Follow-ups
- BACKEND-143 GitOps (images.env, SA, Secret 키)
- BACKEND-65 뒤 `run_index=False` 제거

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

---

### Task 11: GitOps PR (BACKEND-143, Team-Neki-GitOps)

`../Team-Neki-GitOps` 에서 브랜치 `feat/BACKEND-143-prefect-batch-job` 을 만든다. 워크플로 PR 이 머지된 뒤에 한다.

**Files:**
- Create: `overlays/prefect/images.env`
- Modify: `overlays/prefect/kustomization.yaml`
- Modify: `overlays/prefect/worker-base-job-template.json`
- Modify: `overlays/prefect/workflow-secret.example.yaml`

- [ ] **Step 1: images.env**

주석 없이 한 줄. Team-Neki-Server 의 deploy-batch 가 `^NEKI_BATCH_IMAGE=ghcr.io/team-neki/neki-batch:` 로 시작하는 줄이 정확히 하나여야 돈다.

```text
NEKI_BATCH_IMAGE=ghcr.io/team-neki/neki-batch:main
```

- [ ] **Step 2: kustomization.yaml 의 configMapGenerator 에 항목을 더한다**

```yaml
  # 서버 batch 이미지 태그. flow run 파드가 envFrom 으로 NEKI_BATCH_IMAGE 를 받아
  # enrich 끝에 색인 Job 을 띄운다 (Team-Neki-Workflow flows/stores_enrich/index_job.py).
  # Team-Neki-Server 의 deploy-batch 워크플로가 images.env 의 그 줄을 갱신한다. 손으로 고치지 않는다.
  # base job template 이 이름을 그대로 참조하므로 해시 접미가 없어야 한다.
  - name: neki-images
    namespace: prefect
    envs:
      - images.env
    options:
      disableNameSuffixHash: true
```

- [ ] **Step 3: worker-base-job-template.json**

컨테이너의 `envFrom` 을:

```json
"envFrom": [
  {
    "secretRef": {
      "name": "prefect-workflow"
    }
  },
  {
    "configMapRef": {
      "name": "neki-images"
    }
  }
],
```

`variables.properties.service_account_name.default` 를 `null` 에서 `"prefect-worker"` 로. flow run 파드가 색인 Job 을 만들려면 Role `prefect-worker`(jobs CRUD, pods/log)가 필요하다.

- [ ] **Step 4: workflow-secret.example.yaml**

`S3_BUCKET` 앞에:

```yaml
  # enrich 가 띄우는 서버 batch(색인 Job) 파드가 secretKeyRef 로 꺼내는 둘.
  # api 배포(overlays/staging/deployment.yaml)와 같은 값이다. 나머지 키는
  # batch 파드에 넘기지 않는다.
  SPRING_PROFILES_ACTIVE: "staging"
  JASYPT_PASSWORD: ""
```

- [ ] **Step 5: 노드에서 실제 Secret 에 키를 넣는다**

```bash
JASYPT=$(kubectl -n staging get secret yapp-secrets -o jsonpath='{.data.jasypt-password}' | base64 -d)
kubectl -n prefect patch secret prefect-workflow -p "{\"stringData\":{\"SPRING_PROFILES_ACTIVE\":\"staging\",\"JASYPT_PASSWORD\":\"$JASYPT\"}}"
unset JASYPT
```
값을 출력하지 않는다.

- [ ] **Step 6: PR, 머지, 반영**

PR 제목 `feat(prefect): 색인 Job 을 위한 neki-images ConfigMap, flow run SA, batch Secret 키 (BACKEND-143)`. 머지 뒤:

```bash
kubectl -n argocd annotate app prefect argocd.argoproj.io/refresh=normal --overwrite
kubectl -n prefect rollout restart deployment prefect-worker   # initContainer 가 base job template 을 pool 에 다시 민다
```
그다음 Team-Neki-Server 에서 `gh workflow run deploy-batch.yml -f ref=<128 브랜치 또는 main>` 으로 images.env 가 실제 태그를 가리키게 한다.

- [ ] **Step 7: 확인**

enrich 를 UI 에서 `run_index=True` 로 수동 실행. `kubectl -n prefect get jobs` 에 `search-index-<시각>` 이 생기고, flow 로그에 `색인 Job 시작` 과 batch 파드 로그가 보이며, BACKEND-65 전이면 없는 잡 이름으로 종료 코드 1 이라 flow 가 `RuntimeError: Job ... failed` 로 끝나는 것이 맞다. flow run 파드의 `serviceAccountName` 이 `prefect-worker` 인지 `kubectl -n prefect get pod <flow run 파드> -o jsonpath='{.spec.serviceAccountName}'` 로 본다.
