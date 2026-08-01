# CLAUDE.md

이 문서는 이 저장소에서 코드를 작성할 때 지켜야 할 규약과 빠지기 쉬운 함정을 다룹니다.

## 구조

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
    store.py              수집 공통 스키마 (CollectedStore)
    storage.py            S3 적재
aws/config                로컬 개발용 AWS 프로파일
compose.yaml              로컬 S3 (LocalStack)
```

의존은 `deployments -> flows` 단방향입니다. `flows/`가 스케줄을 알면 안 됩니다.

워크플로 안에서는 `flow.py`가 task 모듈을 import하고 그 반대는 없습니다. task
모듈이 `flow.py`를 import해야 할 것 같다면 대개 인자로 받아야 할 값을 import로
끌어온 것이므로, 시그니처를 먼저 의심해볼 수 있습니다.

`__init__.py`는 flow 함수만 재노출합니다. 그래야 `deployments/`가
`from flows.daily_sync import daily_sync`로 짧게 가져올 수 있습니다.

## 실행 모델

UI나 cron이 워크플로를 직접 실행하지 않습니다. flow run 레코드를 만들어둘 뿐이고,
`serve.py`나 worker가 API를 주기적으로 들여다보다 집어갑니다. pull 모델입니다.

```text
[UI / cron / API]
      |  1. flow run 레코드 생성 (SCHEDULED)
      v
[Prefect API]  <-----+
                     |  2. 10초마다 폴링
              [serve.py / worker]
                     |  3. entrypoint import 후 실행
                     v
                 [flow] -> [task]
```

여기서 따라오는 성질들이 있습니다.

- 실행 프로세스가 없으면 run이 `SCHEDULED`에 쌓임. 등록만으로는 실행되지 않음
- 버튼을 눌러도 최대 10초 지연이 있음 (`runner.poll_frequency`, `worker.query_seconds`)
- 서버가 워커에 접근할 필요 없음. 워커의 outbound만 열리면 됨
- task는 flow와 같은 프로세스에서 실행됨. `@task`는 재시도와 상태 추적 단위이지
  실행 격리 단위가 아님

**UI에서 실행하려면 deployment 등록과 실행 프로세스가 둘 다 필요합니다.** 하나만
있으면 UI에 보이지만 눌러도 진행되지 않거나, 아예 목록에 뜨지 않습니다.

## 수집 파이프라인의 경계

지점 수집은 collect, enrich, index 세 단계입니다. 단계를 섞으면 재실행 단위가
무너지므로 어디에 코드를 둘지 헷갈릴 때는 두 질문으로 가릅니다.

- 다시 돌리려면 사이트를 또 긁어야 하나. 아니면 collect 바깥임
- 값이 틀렸을 때 누구 잘못인가. 사이트면 collect, Kakao면 enrich, 우리 규칙이면 index

**collect에서 주소를 해석하지 않습니다.** 도로명인지 지번인지 판정하거나 상호명,
층수를 떼는 것은 전부 enrich의 일입니다. 픽닷은 수집원이 Kakao라 지번 주소를
공짜로 받을 수 있지만 담지 않습니다. 다른 브랜드에 없는 값을 담으면 collect
출력이 브랜드마다 달라지고, 그러면 enrich가 4개를 한 번에 받지 못합니다.

외부 API 의존은 enrich에 모읍니다. 수집 flow는 `KAKAO_API_KEY` 없이도 동작해야
합니다. 지오코딩을 수집 flow에 붙이면 Kakao 장애가 수집 실패가 되고, 색인 규칙을
고칠 때마다 크롤링이 따라 돕니다.

### 공통 스키마

브랜드별 `Store` dataclass를 두지 않습니다. `flows/common/store.py`의
`CollectedStore` 하나를 4개 브랜드가 공유하고, 사이트가 주지 않는 필드는 `None`으로
둡니다. 브랜드마다 모양이 다르면 enrich가 공통으로 받을 수 없습니다.

`collected_at`은 `CollectedStore`에 없습니다. 사이트가 준 값이 아니라 우리가 언제
받았는지이므로 적재 시점에 `storage`가 붙입니다.

### S3 적재

```text
raw/     platform=<브랜드>/dt=<날짜>/<이름>.gz
collect/ platform=<브랜드>/dt=<날짜>/stores.jsonl.gz
                                   /_manifest.json
```

- 파티션 날짜는 KST임. UTC로 끊으면 새벽 실행이 전날 파티션에 들어감
- 같은 날 재실행은 같은 키를 덮어씀. 단일 객체 PUT은 원자적이라 멱등함
- `_manifest.json`을 본문보다 **나중에** 올림. 순서가 뒤집히면 manifest만 있고
  데이터가 없는 창이 생김
- `read_stores`가 manifest의 `count`와 실제 줄 수를 대조함. 다르면 예외임

`boto3.Session().client("s3")`에 `endpoint_url`을 넘기지 않습니다. 로컬과 운영의
차이는 `AWS_PROFILE` 하나여야 합니다. 코드에 분기를 넣으면 이 성질이 깨집니다.

## build() 규약

`deployments/` 아래 모든 모듈은 `RunnerDeployment`를 반환하는 `build()`를 노출해야
합니다. `deployments/__init__.py`의 `collect()`가 이 이름으로 모듈을 찾습니다.

```python
from prefect.deployments.runner import RunnerDeployment

from flows.daily_sync import daily_sync


def build() -> RunnerDeployment:
    return daily_sync.to_deployment(name="daily-sync", cron="0 3 * * *")
```

- 함수명은 `build`로 고정. 다른 이름을 쓰면 `AttributeError`로 실패함
- 워크플로를 추가할 때 `serve.py`와 `deploy.py`는 건드리지 않음
- `deployments/<name>.py`와 `flows/<name>.py`는 파일명을 1:1로 맞춤

### 이름을 바꾸면 고아가 남습니다

deployment는 flow 이름과 deployment 이름의 조합(`hello/hello-local`)으로 식별됩니다.
따라서 flow 함수명이나 `@flow(name=)`을 바꾸면 새 deployment가 만들어지고 **예전
것은 지워지지 않은 채 남습니다.** entrypoint가 이미 없는 함수를 가리키므로 실행하면
실패하지만, UI 목록에는 계속 보입니다.

이름을 바꿀 때는 옛 deployment를 직접 지워야 합니다.

```bash
prefect deployment delete '<옛-flow-이름>/<deployment-이름>'
```

## 네이밍

- 파일, 모듈 : snake_case (e.g. `daily_sync.py`)
- flow 함수 : snake_case, 접미사 없음 (e.g. `def daily_sync(...)`)
- `@flow(name=)` : kebab-case (e.g. `"daily-sync"`). UI 표시명임
- deployment name : kebab-case (e.g. `"daily-sync"`, `"daily-sync-backfill"`)
- task 함수 : 동사로 시작 (e.g. `fetch_orders`, `upload_report`)

flow 함수에 `_flow` 접미사를 붙이지 않는 이유는 `flows.daily_sync.daily_sync`처럼
경로가 이미 역할을 말해주기 때문입니다. import 시 이름이 충돌하면 `as`로 호출부에서
해결합니다.

## serve.py vs. deploy.py

두 진입점은 용도가 다르며 섞어 쓰면 안 됩니다.

- `serve.py` : 로컬 개발용. 서버나 work pool 없이 한 프로세스로 즉시 확인함
- `deploy.py` : 운영용. 스케줄만 등록하고 실행은 worker가 담당함

**운영 스케줄 등록은 반드시 `deploy.py`를 거쳐야 합니다.** `prefect deploy`나
`flow.deploy()`를 직접 호출하면 아래 보존 로직을 건너뜁니다.

## pause는 배포가 덮어씁니다

Prefect는 deployment를 등록할 때 기존 정의를 통째로 덮어씁니다. 따라서 운영자가
UI에서 꺼둔 스케줄이 배포할 때마다 되살아납니다. Airflow에서 DAG를 pause하면 그
상태가 메타DB에 남아 유지되는 것과 다릅니다.

재등록이 일어나는 시점은 다음과 같습니다.

- `serve.py` 재시작 : 매번 재등록하므로 프로세스가 뜰 때마다 풀림
- `deploy()` 재실행 : 배포할 때마다 풀림
- Prefect 서버 재시작 : 유지됨 (DB에 남음)
- worker 재시작 : 유지됨 (worker는 정의를 건드리지 않음)

deployment의 `paused` 대신 스케줄의 `active`를 꺼도 마찬가지로 되살아납니다.
`deploy()`가 cron 인자를 그대로 다시 쓰므로 스케줄 객체가 통째로 교체되기
때문입니다.

`deploy.py`는 배포 전 `paused`와 각 스케줄의 `active`를 읽어두고 등록 후 되돌려
이를 막습니다. 반대로 UI에서 다시 켠 것을 배포가 도로 끄지도 않습니다. 이 파일을
수정할 때는 양방향이 모두 유지되는지 확인해야 합니다.

## 패키지 설정

저장소는 editable로 설치해야 합니다. 설치하지 않으면 `flows` import가 실행
위치(CWD)에 의존하게 되어 worker를 다른 경로에서 띄울 때 깨집니다.

```bash
make setup
```

- 명령은 `make`를 거치며 내부적으로 `uv run`을 씀. venv를 활성화하지 않음
- `uv run`은 `.venv`가 없으면 만들고 `uv.lock`에 맞춰 채운 뒤 실행함
- `uv.lock`은 커밋된 것을 그대로 씀. 지우고 다시 만들면 팀원 간 버전이 갈림
- venv에 `pip` 실행 파일이 없음. 직접 다뤄야 한다면 `uv pip`를 사용함

`uv run`은 `.env`를 자동으로 읽지 않습니다. Makefile이 `.env`가 있을 때만
`UV_ENV_FILE`을 지정해 이를 대신합니다. Makefile을 거치지 않고 실행할 때는
`uv run --env-file .env`로 직접 지정해야 하며, 그러지 않으면 API 키를 쓰는
워크플로가 키 없음 오류로 실패합니다.

최상위 패키지를 새로 추가하면 `pyproject.toml`의
`[tool.hatch.build.targets.wheel] packages`에 반드시 등록해야 합니다. 등록을
빠뜨려도 **로컬에서는 아무 문제가 없어 알아채기 어렵습니다.** editable 설치는
프로젝트 루트를 통째로 `sys.path`에 넣기 때문에 `packages` 목록과 무관하게 전부
import되기 때문입니다.

반면 wheel을 빌드하면 나열된 패키지만 포함됩니다. 즉, 로컬과 CI 테스트는
통과하는데 컨테이너 배포에서만 `ModuleNotFoundError`가 나는 형태로 드러납니다.
`packages`를 수정했다면 wheel 내용을 직접 확인하는 것이 좋습니다.

```bash
uv build --wheel --out-dir /tmp/dist
python -c "import zipfile,glob; w=sorted(glob.glob('/tmp/dist/*.whl'))[-1]; \
print(sorted({n.split('/')[0] for n in zipfile.ZipFile(w).namelist() if '/' in n}))"
```

## Prefect 3 API 함정

Prefect 3.8 기준입니다.

- `Flow.deploy()`와 `prefect.serve()`는 동기 함수임. 이벤트 루프 안에서 호출하면
  실패하므로 `asyncio.run()` 밖에서 불러야 함
- API가 돌려주는 `DeploymentResponse`에는 `flow_name`이 없음.
  `flow_name`은 `RunnerDeployment`에만 있음
- `apply()`는 스케줄을 새로 만들어 ID가 바뀜. 배포 전후로 스케줄을 대응시킬 때는
  ID가 아니라 `slug`를 쓰고, `slug`가 없으면 순번으로 맞춰야 함
- `to_deployment(paused=...)`로 등록 시점에 pause 상태를 정할 수 있음.
  등록 후 되돌리는 것보다 경합이 없어 안전함

## 변경 검증

구조를 바꿨다면 임포트와 deployment 수집부터 확인합니다.

```bash
make check
```

flow 로직만 바꿨다면 단독 실행으로 충분합니다. 수집 flow는 S3에 적재하므로
LocalStack이 먼저 떠 있어야 합니다.

```bash
make localstack
make hello
make lifefourcuts
make photosignature
make planbstudio
make picdot
make s3-ls
```

적재를 건드렸다면 실행 결과가 아니라 적재물을 봐야 합니다. `make s3-ls`로 키가
빠짐없이 올라갔는지 보고, 같은 flow를 두 번 돌려 키 수가 늘지 않는지 확인합니다.
늘어난다면 파티션 경로에 실행마다 바뀌는 값이 섞인 것입니다.

파싱만 확인하고 싶으면 적재를 끕니다.

```bash
uv run --env-file .env python -c \
  "from flows.picdot_stores import picdot_stores; picdot_stores(persist=False)"
```

`pyproject.toml`의 `packages`를 건드렸다면 wheel 내용을 확인합니다.

```bash
make build
```

`deploy.py`나 `deployments/`를 건드렸다면 로컬 서버를 띄워 배포 사이클을 확인해야
합니다. 실제 스케줄을 다루는 코드라 import 성공만으로는 회귀를 잡을 수 없습니다.

```bash
PREFECT_HOME=/tmp/pf-test uv run prefect server start --host 127.0.0.1 --port 4301
export PREFECT_API_URL=http://127.0.0.1:4301/api
uv run prefect work-pool create neki-pool --type process
make deploy PORT=4301
```

검증에는 `PREFECT_HOME`을 임시 경로로 지정해 격리해야 합니다. 지정하지 않으면
`~/.prefect`의 실제 상태를 건드리게 되고, 테스트로 만든 deployment가 로컬 UI에
남습니다.

pause 관련 코드를 고쳤다면 양방향을 모두 확인해야 합니다. 끈 것이 배포 후에도
꺼져 있는지, 그리고 다시 켠 것을 배포가 도로 끄지는 않는지 둘 다 봐야 합니다.

## 로컬 UI

기본 프로파일이 `ephemeral`이라 `PREFECT_API_URL`이 비어 있습니다. 이 상태로
`serve.py`를 실행하면 프로세스 안에서 임시 서버가 뜨고 종료와 함께 사라지므로
**UI로 접근할 수 없습니다.** UI를 보려면 서버를 따로 띄우고 API 주소를 지정해야
합니다.

터미널 두 개가 필요합니다. `make serve`가 `PREFECT_API_URL`을 대신 넣어줍니다.

```bash
make server
make serve
```

UI는 `http://127.0.0.1:4200`이고, deployment 목록은 `/deployments`입니다.

## 작업 순서

team-neki 저장소이므로 코드를 건드리기 전에 Sprint 앱에 티켓을 먼저 만듭니다.
어느 에픽 하위에 둘지 불명확하면 임의로 정하지 않고 확인을 받아야 합니다.
