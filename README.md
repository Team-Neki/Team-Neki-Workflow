# Team Neki Workflow

이 문서는 Prefect 기반 워크플로 저장소의 구조와 실행 방법을 다룹니다.

## 디렉토리 구조

```text
deployments/   스케줄 정의 - "언제 돌리나"
flows/         @flow - "무엇을 어떤 순서로"
tasks/         @task - "실제로 하는 일"
serve.py       로컬 개발용 - 한 프로세스로 서빙
deploy.py      운영 배포용 - work pool에 스케줄 등록
```

세 디렉토리의 의존 방향은 단방향입니다.

```text
deployments/  ->  flows/  ->  tasks/
```

`tasks/`는 `flows/`를 import하지 않고, `flows/`는 스케줄을 모릅니다. 이 방향이
깨지면 계층 분리의 의미가 없어집니다.

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

`deployments/<name>.py`와 `flows/<name>.py`는 파일명을 1:1로 맞춥니다. deployment
파일을 열었을 때 대응하는 flow 위치를 이름만 보고 알 수 있어야 합니다.

## 워크플로 추가하기

`serve.py`와 `deploy.py`는 건드리지 않습니다. 아래 세 파일만 추가하면 됩니다.

먼저 `tasks/<domain>/<module>.py`에 `@task`로 실제 작업을 정의합니다. 여러
워크플로가 함께 쓰는 task는 `tasks/common/`에 둡니다.

다음으로 `flows/<name>.py`에서 task를 import해 순서를 조립합니다.

```python
from prefect import flow

from tasks.orders.sync import fetch_orders


@flow(name="daily-sync")
def daily_sync() -> None:
    fetch_orders()
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

의존성을 설치합니다. 저장소를 editable로 설치해야 `flows`, `tasks` import가 실행
위치와 무관하게 동작합니다.

```bash
uv venv
uv pip install -e . --python .venv/bin/python
```

flow만 단독으로 돌려볼 때는 서버가 필요 없습니다.

```bash
.venv/bin/python -c "from flows.hello import hello; hello()"
```

### UI로 확인하기

UI를 쓰려면 서버를 먼저 띄우고 API 주소를 지정해야 합니다. 기본 프로파일이
`ephemeral`이라 이 과정을 건너뛰면 `serve.py`가 프로세스 안에 임시 서버를 띄우고
종료와 함께 없애므로 UI로 접근할 수 없습니다.

터미널 하나에서 서버를 띄웁니다.

```bash
.venv/bin/prefect server start --host 127.0.0.1 --port 4200
```

다른 터미널에서 deployment를 서빙합니다.

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
.venv/bin/python serve.py
```

`http://127.0.0.1:4200/deployments`에서 등록된 deployment를 보고 Run 버튼으로
실행할 수 있습니다. 두 프로세스가 모두 떠 있어야 하며, 하나라도 없으면 목록에
안 뜨거나 눌러도 `SCHEDULED`에서 멈춥니다.

## 운영 배포

운영에서는 `serve.py`를 쓰지 않습니다. 스케줄 등록과 실행을 분리합니다.

```bash
PREFECT_WORK_POOL=neki-pool .venv/bin/python deploy.py
.venv/bin/prefect worker start --pool neki-pool
```

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
