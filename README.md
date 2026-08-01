# Team Neki Workflow

이 문서는 Prefect 기반 워크플로 저장소의 구조와 실행 방법을 다룹니다.

## 디렉토리 구조

워크플로 하나가 디렉토리 하나입니다. `@flow`와 `@task`를 같은 디렉토리에 두되
파일은 나눕니다. 스케줄만 따로 모읍니다.

```text
deployments/              언제 돌리나
  daily_sync.py           build()
flows/
  daily_sync/             무엇을 어떤 순서로 + 실제로 하는 일
    __init__.py           flow 재노출
    flow.py               @flow
    orders.py             @task
  common/                 여러 워크플로가 함께 쓰는 task
serve.py                  로컬 개발용 - 한 프로세스로 서빙
deploy.py                 운영 배포용 - work pool에 스케줄 등록
```

의존 방향은 단방향입니다.

```text
deployments/  ->  flows/<name>/flow.py  ->  flows/<name>/<task 모듈>
```

`flows/`는 스케줄을 모르고, 워크플로 안에서 task 모듈은 `flow.py`를 import하지
않습니다. 이 방향이 깨지면 분리의 의미가 없어집니다.

## 실행 모델

UI나 cron이 워크플로를 직접 실행하지는 않습니다. flow run 레코드를 만들어둘 뿐이고,
`serve.py`나 worker가 API를 주기적으로 확인하다 집어갑니다.

```text
[UI / cron]  ->  [Prefect API]  <-  폴링  <-  [serve.py / worker]  ->  [flow] -> [task]
```

따라서 실행 프로세스가 없으면 run이 `SCHEDULED`에 쌓이기만 하고, 버튼을 눌러도
폴링 주기만큼(기본 10초) 지연이 생깁니다.

## 네이밍 규약

- 파일, 모듈 : snake_case (e.g. `daily_sync.py`)
- flow 함수 : snake_case, 접미사 없음 (e.g. `def daily_sync(...)`)
- `@flow(name=)` : kebab-case, UI 표시명 (e.g. `"daily-sync"`)
- deployment name : kebab-case (e.g. `"daily-sync"`, `"daily-sync-backfill"`)
- task 함수 : 동사로 시작 (e.g. `fetch_orders`, `upload_report`)

`deployments/<name>.py`와 `flows/<name>/`은 이름을 1:1로 맞춥니다. deployment
파일을 열었을 때 대응하는 워크플로 위치를 이름만 보고 알 수 있어야 합니다.

## 워크플로 추가하기

`serve.py`와 `deploy.py`는 건드리지 않습니다. 두 곳만 손대면 됩니다.

먼저 `flows/<name>/`을 만들고 task를 역할별 모듈에 담습니다. 여러 워크플로가
함께 쓰는 task는 `flows/common/`에 둡니다.

```python
# flows/daily_sync/orders.py
from prefect import task


@task(retries=2)
def fetch_orders() -> list[dict]:
    ...
```

같은 디렉토리의 `flow.py`에서 순서를 조립합니다.

```python
# flows/daily_sync/flow.py
from prefect import flow

from flows.daily_sync.orders import fetch_orders


@flow(name="daily-sync")
def daily_sync() -> None:
    fetch_orders()
```

`__init__.py`에서 flow만 재노출합니다.

```python
# flows/daily_sync/__init__.py
from flows.daily_sync.flow import daily_sync

__all__ = ["daily_sync"]
```

마지막으로 `deployments/<name>.py`에 `RunnerDeployment`를 반환하는 `build()`를
정의합니다. `serve.py`와 `deploy.py`가 이 이름으로 찾아가므로 함수명은 반드시
`build`여야 합니다.

```python
from prefect.deployments.runner import RunnerDeployment

from flows.daily_sync import daily_sync


def build() -> RunnerDeployment:
    return daily_sync.to_deployment(name="daily-sync", cron="0 3 * * *")
```

## 로컬 실행

`make`가 진입점입니다. 인자 없이 실행하면 명령 목록이 나옵니다.

```bash
make
```

```text
  help            명령 목록을 출력한다
  setup           의존성을 uv.lock 기준으로 설치한다
  check           임포트와 deployment 수집을 확인한다
  hello           hello 워크플로를 실행한다
  lifefourcuts    인생네컷 지점을 수집한다
  photosignature  포토시그니처 지점을 수집한다
  planbstudio     플랜비스튜디오 지점을 수집한다
  picdot          픽닷 지점을 수집한다 (KAKAO_API_KEY 필요)
  serve           로컬 개발용으로 deployment를 서빙한다
  server          Prefect 서버를 띄운다
  deploy          work pool에 스케줄을 등록한다
  build           wheel을 빌드하고 포함된 패키지를 확인한다
  clean           빌드 산출물과 캐시를 지운다
```

### 준비

`uv`만 있으면 됩니다. 가상환경을 따로 만들거나 활성화하지 않아도 됩니다.

```bash
make setup
```

내부적으로 `uv run`을 씁니다. `uv run`은 `.venv`가 없으면 만들고 `uv.lock`에 맞춰
채운 뒤 실행하므로, 클론 직후 `make hello`를 바로 실행해도 동작합니다.

**`uv.lock`은 반드시 커밋된 것을 그대로 씁니다.** 지우고 다시 만들면 팀원마다 다른
버전이 설치되어 로컬에서만 재현되는 문제가 생깁니다.

### 워크플로 실행

서버 없이 flow만 돌려볼 때 씁니다.

```bash
make hello
make lifefourcuts
make photosignature
make planbstudio
make picdot
```

`picdot`은 Kakao Local API를 호출하므로 `KAKAO_API_KEY`가 필요합니다. `.env`에 넣어두면
`make`가 알아서 읽습니다. `uv run`은 `.env`를 자동으로 읽지 않으므로, Makefile을 거치지
않고 직접 실행할 때는 `uv run --env-file .env ...`로 지정해야 합니다.

```bash
# .env
KAKAO_API_KEY=발급받은_REST_API_키
```

Kakao Developers에서 앱을 만들고 `앱` > `플랫폼 키` > **REST API 키**를 씁니다.
서버 호출용이라 플랫폼 등록이나 비즈 앱 전환은 필요 없습니다.

`make check`는 임포트와 deployment 수집만 확인합니다. 구조를 바꾼 뒤 회귀를 빠르게
잡을 때 유용합니다.

### UI로 확인하기

터미널 두 개가 필요합니다. 먼저 서버를 띄웁니다.

```bash
make server
```

다른 터미널에서 deployment를 서빙합니다.

```bash
make serve
```

`http://127.0.0.1:4200/deployments`에서 Run 버튼으로 실행할 수 있습니다. **두
프로세스가 모두 떠 있어야 합니다.** 하나라도 없으면 목록에 안 뜨거나 눌러도
`SCHEDULED`에서 멈춥니다.

`make serve`는 `PREFECT_API_URL`을 대신 넣어줍니다. 직접 `python serve.py`를 실행할
때는 이 값을 지정해야 합니다. 기본 프로파일이 `ephemeral`이라 지정하지 않으면
프로세스 안에 임시 서버가 떴다 사라져 UI로 접근할 수 없습니다.

포트를 바꾸려면 변수를 넘깁니다.

```bash
make server PORT=4300
make serve PORT=4300
```

## 운영 배포

운영에서는 `serve.py`를 쓰지 않습니다. 스케줄 등록과 실행을 분리합니다.

```bash
make deploy WORK_POOL=neki-pool
uv run prefect worker start --pool neki-pool
```

`make deploy`는 스케줄만 등록하고 끝납니다. 실제 실행은 worker가 담당하므로 둘 다
필요합니다.

### pause가 배포에 지워지는 문제

Prefect는 deployment를 등록할 때 기존 정의를 통째로 덮어씁니다. 따라서 운영자가
UI에서 꺼둔 스케줄이 배포할 때마다 되살아납니다. Airflow에서 DAG를 pause하면 그
상태가 유지되는 것과 다릅니다.

`serve.py`는 프로세스가 뜰 때마다 재등록하므로 재시작할 때마다 풀리고, `deploy()`를
직접 호출하는 방식도 배포할 때마다 풀립니다. 스케줄의 `active`를 대신 꺼도
마찬가지로 되살아납니다. 반면 Prefect 서버나 worker의 재시작은 정의를 건드리지
않으므로 pause가 유지됩니다.

`deploy.py`는 배포 전 `paused`와 각 스케줄의 `active`를 읽어두고 등록 후 되돌려 이
문제를 막습니다. 반대로 UI에서 다시 켠 것을 배포가 도로 끄지도 않습니다.

**따라서 운영 스케줄 등록은 반드시 `deploy.py`를 거쳐야 합니다.** `prefect deploy`나
`flow.deploy()`를 직접 호출하면 보존 로직을 건너뛰어 꺼둔 스케줄이 되살아납니다.
