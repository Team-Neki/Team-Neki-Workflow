# 워크플로 코드 구조와 개발 규약

## 구조

도메인 아래에 워크플로를 둡니다. 지점은 `stores` 아래 collect, enrich, sync 로
나누고 브랜드별 수집기는 collect 아래에 둡니다. `@flow`와 `@task`는 같은
워크플로 디렉토리에 두되 파일을 나눕니다. 배포 설정은 별도로 모읍니다.

```text
neki_workflow/
  deployments/
    stores/               지점 flow 의 build() 설정
      collect.py           정기 수집 스케줄
      photoism.py          브랜드 단독 실행 설정
      enrich.py
      sync.py
    search_index.py
  flows/
    stores/
      collect/             지점 수집 총괄
        flow.py
        photoism/          브랜드 수집 flow
          flow.py
      enrich/              법정동 보강
      sync/                지점 마스터 동기화
      common/              지점 스키마·S3 적재·manifest·수집 공통 코드
    search_index/          서버 색인 Job 실행
    common/                도메인 공통 DB 연결·알림
aws/config                로컬 개발용 AWS 프로파일
compose.yaml              로컬 S3 (LocalStack)
Dockerfile                운영 이미지. worker 와 flow run 이 같이 씀
.github/workflows/        ci.yml (PR 검증), build.yml (main merge 시 이미지 푸시, GitOps 갱신)
docs/spec/                수집 파이프라인 정책 (정본). 항목마다 구현 위치 anchor
docs/runbook.md           배포 절차와 장애 대응
spec_check.py             docs/spec 의 anchor 가 코드 줄과 맞는지 확인 (make check 에 포함)
tests/                    순수 판정 테스트. make check 가 돌림. Kakao 와 DB 는 부르지 않음
```

의존은 `neki_workflow.deployments -> neki_workflow.flows` 단방향입니다. `neki_workflow/flows/`가 스케줄을 알면 안 됩니다.

워크플로 안에서는 `flow.py`가 task 모듈을 import하고 그 반대는 없습니다. task
모듈이 `flow.py`를 import해야 할 것 같다면 대개 인자로 받아야 할 값을 import로
끌어온 것이므로, 시그니처를 먼저 의심해볼 수 있습니다.

`__init__.py`는 flow 함수만 재노출합니다. 그래야 `neki_workflow/deployments/`가
`from neki_workflow.flows.example.daily_sync import daily_sync`로 짧게 가져올 수 있습니다.
## build() 규약

`neki_workflow/deployments/` 아래 모든 Python 모듈은 `RunnerDeployment`를 반환하는
`build()`를 노출해야 합니다. `neki_workflow/deployments/__init__.py`의 `collect()`가
하위 패키지까지 탐색합니다. 패키지의 `__init__.py`는 탐색 대상에서 제외합니다.

```python
from prefect.deployments.runner import RunnerDeployment

from neki_workflow.flows.example.daily_sync import daily_sync


def build() -> RunnerDeployment:
    return daily_sync.to_deployment(name="daily-sync", cron="0 3 * * *")
```

- 함수명은 `build`로 고정. 다른 이름을 쓰면 `AttributeError`로 실패함
- 워크플로를 추가할 때 `serve.py`와 `deploy.py`는 건드리지 않음
- deployment는 대응 flow와 같은 도메인에 둠. 브랜드 단독 실행은 `stores/photoism.py`처럼 둠

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

flow 함수에 `_flow` 접미사를 붙이지 않는 이유는 `neki_workflow.flows.example.daily_sync.daily_sync`처럼
경로가 이미 역할을 말해주기 때문입니다. import 시 이름이 충돌하면 `as`로 호출부에서
해결합니다.
## 패키지 설정

저장소는 editable로 설치해야 합니다. 설치하지 않으면 `neki_workflow.flows` import가 실행
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

반면 wheel을 빌드하면 나열된 패키지만 포함됩니다. 현재 패키지 정본은
`neki_workflow` 하나입니다. 운영 이미지는 wheel 대신 `Dockerfile`이 패키지
전체와 `deploy.py`를 직접 복사합니다. 새 파일을 추가하면 `make check`와
`make image`에서 import와 deployment 수집을 확인합니다.

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
