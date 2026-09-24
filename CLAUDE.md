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
    geocode.py            좌표가 빈 지점을 Kakao로 보정
    imweb_map.py          imweb 지도 위젯 수집 (인생네컷, 포토이즘, 돈룩업)
aws/config                로컬 개발용 AWS 프로파일
compose.yaml              로컬 S3 (LocalStack)
Dockerfile                운영 이미지. worker 와 flow run 이 같이 씀
.github/workflows/        ci.yml (PR 검증), build.yml (main merge 시 이미지 푸시, GitOps 갱신)
docs/runbook.md           배포 절차와 장애 대응
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

### 운영 배포 파이프라인

Actions는 Prefect 서버에 접근하지 않습니다. 서버가 외부에 노출되어 있지 않기
때문입니다. 이미지를 밀고 GitOps 태그를 바꾸는 것까지만 하고, deployment 등록은
클러스터 안에서 일어납니다.

```text
main merge
  -> build.yml      이미지 빌드, ghcr.io/team-neki/team-neki-workflow:<version>-<sha7> 와 :main 푸시
  -> build.yml      Team-Neki-GitOps overlays/prefect/worker.yaml 의 image 태그 커밋
  -> ArgoCD         worker Deployment 롤링
  -> initContainer  /opt/prefect 에서 python deploy.py (등록 갱신, pause 보존)
  -> 다음 flow run 부터 새 이미지
```

worker와 flow run(Job 파드)이 같은 이미지입니다. 이미지에 `WORKFLOW_IMAGE`로 자기
참조가 구워져 있고 `deploy.py`가 이를 `job_variables.image`에 넣습니다. 이게 없으면
flow run이 work pool 기본 이미지(베이스 prefect 이미지)로 떠서 `flows`를 찾지
못합니다.

- entrypoint가 cwd 기준 상대경로라 worker와 Job 모두 `/opt/prefect`에서 실행됨
- 파드는 uid 1001, 루트 파일시스템 읽기 전용일 수 있음. 이미지 경로에 쓰지 않고
  파일은 root 소유 644로 둠
- 자격증명은 GitOps의 k8s Secret `prefect-workflow`가 flow run Job 파드 환경변수로
  넣음 (`worker-base-job-template.json`의 envFrom). worker 파드가 아님. IAM role 없음.
  코드는 환경변수만 봄. 새 환경변수를 읽는 flow를 추가하면 GitOps의
  `workflow-secret.example.yaml`에 키를 같이 추가함
- `pyproject.toml`의 prefect 버전은 Dockerfile 베이스와 같아야 함. 다르면 uv.lock
  설치가 베이스의 prefect를 덮어써 prefect-kubernetes가 깨지고 worker가 뜨지 않음.
  `make image`가 버전을 assert함
- 매니페스트와 RBAC은 GitOps 레포 `overlays/prefect/`에 있음. 여기 두지 않음

## 수집 파이프라인의 경계

지점 수집은 collect, enrich, index 세 단계입니다. 단계를 섞으면 재실행 단위가
무너지므로 어디에 코드를 둘지 헷갈릴 때는 두 질문으로 가릅니다.

- 다시 돌리려면 사이트를 또 긁어야 하나. 아니면 collect 바깥임
- 값이 틀렸을 때 누구 잘못인가. 사이트면 collect, Kakao면 enrich, 우리 규칙이면 index

**collect에서 주소를 해석하지 않습니다.** 도로명인지 지번인지 판정하거나 상호명,
층수를 떼는 것은 전부 enrich의 일입니다. 픽닷은 수집원이 Kakao라 지번 주소를
공짜로 받을 수 있지만 담지 않습니다. 다른 브랜드에 없는 값을 담으면 collect
출력이 브랜드마다 달라지고, 그러면 enrich가 전 브랜드를 한 번에 받지 못합니다.

지오코딩 같은 외부 API 보강은 enrich에 모읍니다. 수집 flow에 붙이면 Kakao 장애가
수집 실패가 되고, 색인 규칙을 고칠 때마다 크롤링이 따라 돕니다. 픽닷, 모노맨션,
포토그레이, 하루필름, 포토랩플러스, 비룸스튜디오는 수집원 자체가 Kakao라 예외지만,
이 경우에도 받아온 값을 해석하지는 않습니다.

### 좌표 보정만 collect 안에서 합니다

이 규약의 예외가 하나 있습니다. **좌표가 빈 지점은 수집 flow 안에서 Kakao로
채웁니다**(`flows/common/geocode.py`). 좌표가 없으면 그 지점은 색인에서 통째로
빠지는데, enrich 단계가 아직 없어 그때까지 결측이 방치되기 때문입니다.

규약이 막으려던 것은 코드로 막습니다. 이 셋 중 하나라도 깨면 예외를 유지할
이유가 없어지므로 함께 봐야 합니다.

- **Kakao 장애가 수집 실패가 되지 않습니다.** `KAKAO_API_KEY`가 없으면 경고만
  남기고 건너뛰고, 조회 예외는 그 지점만 좌표 없이 넘깁니다. 연속 세 번 실패하면
  남은 지점은 아예 조회하지 않습니다. 장애일 때 지점 수만큼 재시도를 되풀이하면
  수집이 몇십 분 늘어집니다
- **사이트가 준 값과 구분합니다.** `coordinate_source`는 공식 사이트 좌표면
  `official`, Kakao 직접 수집이나 보정 좌표면 `kakao`로 남습니다.
  `None`은 좌표를 얻지 못했다는 뜻입니다. 좌표는 collect 출력에서 유일하게 우리가 만든 값이 섞이는
  필드이므로, 이 표시가 없으면 경계가 코드에서 사라집니다
- **끌 수 있습니다.** flow의 `geocode` 파라미터를 끄면 보정하지 않습니다

**주소는 여전히 해석하지 않습니다.** 원문을 그대로 질의에 넣을 뿐 층수나 상호명을
떼지 않습니다. 주소검색이 0건이면 `"<주소 앞 2토큰> <상호명>"`으로 다시 묻는데,
`서울 강남구 압구정로50길 27 1F`처럼 접미사가 붙어 주소검색이 실패하는 지점이
실제로 있어 넣은 폴백입니다. 키워드검색을 먼저 쓰지 않는 이유는 질의가 주소 하나로
닫혀 있지 않아 전국의 동명 가게를 집을 수 있기 때문입니다.

보정을 부르는 브랜드는 좌표가 비는 다섯(플랜비스튜디오, 포토시그니처, 인생네컷,
포토이즘, 돈룩업)뿐입니다. 나머지는 수집원이 Kakao라 좌표가 항상 옵니다.

하루필름은 사이트에도 목록이 있지만 Kakao를 씁니다. 전체 목록 페이지에 목록이
없어 지역 페이지 8개를 순회해야 하고, 그 목록마저 게시판이 아니라 갤러리 위젯
캡션이라 주소가 마지막 `<br>` 뒤 텍스트로 들어 있습니다. 전화와 좌표도 주지
않습니다. 자세한 근거는 `flows/harufilm_stores/flow.py` 모듈 docstring에
남겼습니다.

포토랩플러스는 사이트에 목록이 있는데도 Kakao를 씁니다. 탭이 무규칙한 iframe으로
나뉘고 손으로 만든 텍스트 위젯이라 주소가 두 줄로 쪼개진 항목이 있으며, 제주 지점에
서울 주소가 들어가 있는 등 **사이트가 틀린 값을 줍니다.** 파서로는 걸러지지 않는
문제라 수집원 자체를 바꾼 것입니다.

비룸스튜디오는 브랜드 도메인이 NXDOMAIN이라 Kakao 말고 고를 수집원이 아예
없습니다.

### imweb 지도 위젯은 한 모듈이 담당합니다

인생네컷과 포토이즘, 돈룩업은 같은 imweb 위젯을 씁니다. 목록 페이지 HTML에는 지점이 없고
AJAX 엔드포인트가 HTML 조각을 돌려주므로, 페이지를 긁지 않고 엔드포인트를 직접
호출합니다. 엔드포인트와 셀렉터가 사이트를 넘나들며 그대로 먹습니다.

그래서 `flows/common/imweb_map.py` 하나가 순회와 파싱을 맡고 브랜드 flow는
`collect_board`에 넷만 넘깁니다.

```text
base_url  board_code  referer  platform
```

**브랜드별 분기를 이 모듈에 넣지 않습니다.** 넣으면 사이트가 개편됐을 때 한 곳만
고치고 넘어가게 되어 공용화한 의미가 사라집니다. 위 넷 말고 다른 것이 필요해
보이면 대개 collect가 아니라 enrich에서 해야 할 일입니다.

**범위를 벗어난 페이지가 빈 응답이 아니라 마지막 페이지로 고정됩니다.** 따라서
"빈 페이지면 중단"으로는 끝나지 않고, 직전 페이지와 항목 id 집합을 비교해야
합니다. `MAX_PAGES`는 이 판정이 어긋났을 때를 막는 안전장치일 뿐이므로 실제
페이지 수에 가깝게 잡지 않습니다. 가깝게 잡으면 지점이 조금 늘었을 때 상한에
걸려 조용히 잘립니다.

조각 상단 `.tit > span.text-brand`에 총 건수가 실려 옵니다. Kakao의
`total_count`처럼 대조 기준으로 쓰되, 위젯 설정에 따라 꺼져 있어(인생네컷) 값이
없으므로 파서는 `int | None`을 돌려주고 있을 때만 비교합니다.

전화번호는 `tel:` 링크가 있어도 href가 비어 있는 경우가 있습니다. 포토이즘은 488곳
전부가 그렇습니다. `""`가 그대로 담기면 다음 단계가 번호가 있는 줄로 오해하므로
`None`으로 맞춥니다.

### Kakao 장소검색의 45건 상한

한 질의로 꺼낼 수 있는 문서는 45건까지입니다. `total_count`는 상한과 무관하게
실제 개수를 알려주므로, `total_count > pageable_count`면 잘린 것입니다.

**행정구역 이름으로 쪼개지 않습니다.** 개편을 따라다녀야 하고, 실제 데이터에
`전남광주통합특별시`처럼 예상 못 한 표기가 섞입니다. 대신 `flows/common/kakao.py`의
`search_all`이 좌표 사각형을 넷으로 나눠 재귀합니다. 모노맨션 기준으로 호출 26회에
104건 전량이 나오며, 행정구역 순회(43회)보다 적습니다.

수집 후에는 건수를 `total_count`와 대조해야 합니다. 분할이 덜 내려갔을 때 이것
말고는 드러나는 곳이 없습니다.

### 공통 스키마

브랜드별 `Store` dataclass를 두지 않습니다. `flows/common/store.py`의
`CollectedStore` 하나를 모든 브랜드가 공유하고, 사이트가 주지 않는 필드는 `None`으로
둡니다. 브랜드마다 모양이 다르면 enrich가 공통으로 받을 수 없습니다.

`collected_at`은 `CollectedStore`에 없습니다. 사이트가 준 값이 아니라 우리가 언제
받았는지이므로 적재 시점에 `storage`가 붙입니다.

적재 포맷은 헤더가 있는 CSV입니다. 다음 단계가 Postgres `COPY`로 그대로 받고
사람이 볼 때도 S3 콘솔과 스프레드시트에서 바로 열립니다. 열 순서는
`storage.COLUMNS`가 정본이며, `CollectedStore`에 필드를 더하면 여기에도 넣어야
합니다. 빠뜨리면 조용히 누락되지 않고 `DictWriter`가 `ValueError`로 막습니다.

**압축하지 않습니다.** collect는 하루 전량이 수백 KB, raw는 압축을 풀어도 5MB
안팎이라 gzip으로 줄여서 얻는 것이 없고, 압축하면 콘솔에서 바로 열린다는 이점이
사라집니다. 대신 Content-Type을 넣어 콘솔의 "열기"가 브라우저에서 바로 보여주게
합니다.

**CSV에는 타입도 null도 없습니다.** 빈 칸과 빈 문자열이 구분되지 않으므로
수집 단계는 값이 없을 때 빈 문자열이 아니라 `None`을 넣어야 하고, 읽는 쪽인
`read_stores`가 빈 칸을 `None`으로, 좌표를 `float`으로 되돌립니다. 이 규칙이
깨지면 좌표가 문자열인 채로 enrich에 넘어갑니다.

### 수집 스케줄은 한 곳에만 있습니다

정기 수집 스케줄은 `deployments/stores_collect.py` 하나뿐입니다. 브랜드별
deployment는 **cron 없이** 남겨둡니다. 스케줄이 없으면 자동으로 돌지 않지만 UI
목록에는 남아 실행 버튼이 동작하므로, 백필과 단일 브랜드 재수집에 그대로 씁니다.

`paused=True` 대신 스케줄을 아예 두지 않는 이유는, pause는 배포가 덮어쓰는 문제를
계속 신경 써야 하지만 스케줄이 없으면 그럴 일이 없기 때문입니다.

브랜드를 추가하면 `flows/stores_collect/flow.py`의 `BRANDS`에도 등록해야 합니다.
빠뜨리면 단독 실행은 되는데 정기 수집에서만 조용히 누락됩니다.

`stores_collect`가 브랜드 flow를 직접 import합니다. 워크플로 디렉토리끼리 서로를
모르는 것이 기본이지만, 묶는 쪽은 묶이는 쪽을 알아야 합니다. 반대 방향은 없습니다.

### 한 브랜드가 실패해도 멈추지 않습니다

Prefect 3에서 동기 서브플로우 호출은 순차입니다. `ThreadPoolExecutor`로 감싸야
실제로 겹쳐 돕니다. 실측으로 45.6초에서 22.1초가 됩니다.

각 브랜드의 예외는 `future.result()` 자리에서 잡아 결과에 담고 넘어갑니다.
**여기서 예외를 다시 던지면 나머지 브랜드의 결과까지 버리게 됩니다.** 사이트
하나가 개편돼 파싱이 깨졌을 때 나머지까지 멈추는 것은 과합니다.

실패한 브랜드는 이전 사이클의 적재물로 대신합니다. 그래야 사이트 하나가 깨졌다고
색인에서 브랜드가 통째로 사라지지 않습니다.

**이전 데이터를 이번 사이클로 복사하지 않습니다.** 수집한 적 없는 것이 이번 것처럼
보이면 collect 계층이 거짓말을 하게 되고, 며칠이 지나도 신선도를 알 수 없습니다.

**무엇을 대신 읽을지는 기록하지 않고 읽는 시점에 계산합니다.** `manifest.read_cycle`이
브랜드마다 사이클 이하의 가장 최근 행을 집어 며칠 지났는지로 상태를 매기고,
`stores_collect`와 enrich가 같은 함수를 씁니다. 규칙이 한 곳이어야 수집이 정한
것과 다음 단계가 보는 것이 어긋나지 않습니다. 실행 요약을 S3에 따로 남기지 않는
이유가 이것입니다. 실패 사유는 Prefect 로그에 있고, 무엇을 읽을지는 테이블에서
나오며, 아침에 실패한 브랜드를 오후에 단독 재수집하면 기록은 아침 상태에 굳어
있지만 계산은 새 행을 바로 집습니다.

`MAX_STALE_DAYS`(기본 7일)를 넘으면 대신하지 않고 `failed`로 둡니다. 무한정
대신하면 파서가 깨진 채로 몇 주가 지나도 아무도 눈치채지 못합니다. **best effort가
고장을 감추는 장치가 되면 안 됩니다.** 쓰지 않기로 한 경우에도 `stale_target_date`와
`age_days`는 돌려줘 왜 버렸는지 남깁니다.

상태는 셋입니다.

- `ok` : 이 사이클에 적재됨. 이번 시도가 실패해도 이 사이클 행이 이미 있으면 여기 속함
- `stale` : 이전 사이클로 대신함. `age_days`가 며칠 전인지 알려줌
- `failed` : 쓸 수 있는 것이 없음

`ok`와 `stale`이 하나도 없을 때만 flow를 실패시킵니다. 다음 단계로 넘길 것이 없기
때문입니다. 신선한 것이 하나도 없으면(전부 `stale`) 개별 사이트 문제가 아니라
네트워크나 배포 쪽을 의심해야 하므로 별도로 에러 로그를 남깁니다.

### S3 적재

```text
collect/platform=<브랜드>/dt=<대상 일자>/<실행 시각>.csv
collect/platform=<브랜드>/dt=<대상 일자>/_raw/<실행 시각>/<이름>
```

prefix는 `collect/` 하나뿐입니다. 사람이 콘솔에서 읽는 것과 Athena를 붙이는 것을
둘 다 만족하도록 잡았습니다.

- **브랜드가 위입니다.** 브랜드 폴더를 열면 `dt=`가 이력 순으로 나열되고, 실패한
  날은 폴더가 없어 마지막 폴더가 곧 대신 쓰이는 것입니다. 대신하기가 브랜드
  이력을 읽는 흐름과 같습니다. "오늘 무엇이 돌았나"는 폴더가 아니라 Prefect
  run과 `read_cycle`이 답합니다
- **CSV와 그것을 만든 원문이 같은 파티션에 있습니다.** `_raw/<실행 시각>/` 아래이고
  실행 시각이 CSV와 같아 짝이 맞습니다. 다른 prefix로 건너갈 일이 없습니다
- **`_raw/`는 Athena가 무시합니다.** Hive와 Trino가 `_`나 `.`로 시작하는 폴더와
  파일을 숨김으로 보는 규칙입니다. 이름을 바꾸면 그 성질이 사라집니다
- **`dt=`는 적재일이 아니라 대상 일자(`target_date`)입니다.** 늦게 집힌 월요일
  run은 월요일 폴더에 들어가고 파일명이 실제 시각을 말합니다. Athena가 사이클로
  파티션을 건너뛰려면 파티션이 사이클이어야 합니다
- 실행 시각은 `YYYY-MM-DD_HHMMSS`(KST)로 브랜드 flow run의 시작 시각입니다.
  `storage.run_at`이 Prefect 컨텍스트에서 읽으므로 수집기에 인자로 나를 필요가
  없고, 재시도도 같은 이름을 다시 씁니다. 같은 날 재실행하면 CSV도 raw 폴더도
  하나 더 생기고 이전 것은 남습니다
- 어느 CSV가 현재인지는 S3가 아니라 Postgres가 압니다. `tb_store_collect_manifest`가
  적재 한 번마다 한 행으로 경로와 건수를 들고 있고, 읽는 쪽은 그 행의 `s3_path`만
  따라갑니다. 아래 "수집 manifest는 Postgres에 있습니다"를 보세요
- manifest 행을 본문보다 **나중에** 씁니다. 순서가 뒤집히면 행만 있고 데이터가
  없는 창이 생깁니다
- `read_stores`가 manifest의 `store_count`와 실제 건수를 대조합니다. 다르면
  예외입니다. 줄이 아니라 CSV 레코드를 셉니다. 주소에 줄바꿈이 섞이면 한 건이 여러
  줄로 인용됩니다
- raw 객체에는 `kind=raw` 태그가 붙습니다. S3 lifecycle은 prefix나 태그로만
  걸리는데 raw가 파티션 안에 있어 prefix로는 못 잡습니다. 코드는 지우지 않습니다

`raw/`나 `runs/` prefix는 없습니다. 원문은 위처럼 CSV 옆에 있고, 실행 요약은
Prefect 로그와 `read_cycle`이 대신합니다. 파티션 안에 `_manifest.json`도 없습니다.

#### Athena를 붙이려면

지금 붙이지는 않습니다. 붙이는 날 `collect/`를 테이블 위치로 잡고 `platform`(enum
11개)과 `dt`(date)를 partition projection으로 정의하면 크롤러도 파티션 등록도
필요 없습니다. 같은 날 재실행으로 CSV가 둘이면 파일명이 시각이라 정렬되므로 뷰
하나로 최신만 남깁니다.

```sql
SELECT * FROM (
  SELECT *, max("$path") OVER (PARTITION BY platform, dt) AS latest FROM collect
) WHERE "$path" = latest
```

`_raw/`가 정말 무시되는지는 그날 `SELECT count(*)` 한 번으로 확인합니다. 어긋나면
`_raw/`를 별도 prefix로 빼는 것이라 되돌리기 쉽습니다.

`boto3.Session().client("s3")`에 `endpoint_url`을 넘기지 않습니다. 로컬과 운영의
차이는 환경변수뿐이어야 합니다. 로컬은 `AWS_PROFILE=neki-local`, 운영은 k8s Secret이
넣는 자격증명 변수입니다. 코드에 분기를 넣으면 이 성질이 깨집니다.

### 수집 manifest는 Postgres에 있습니다

파티션마다 `_manifest.json`을 두지 않습니다. 객체로 두면 "이 사이클에 어느 브랜드가
무엇을 남겼나"를 묻는 데 파티션을 전부 나열해야 하고, 브랜드가 늘수록 목록 호출이
따라 늡니다. enrich가 가장 자주 던지는 질문이 그것이므로 조회 한 번으로 끝나는
쪽에 둡니다. `flows/common/manifest.py`가 DDL과 조회를 함께 들고 있습니다.

```text
id           적재 일련번호 (PK)
platform     브랜드
target_date  대상 일자
s3_path      s3://<버킷>/collect/platform=.../dt=.../<YYYY-MM-DD_HHMMSS>.csv
store_count  건수
collected_at 적재 시각 (KST 벽시계, 시간대 없는 TIMESTAMP)
flow_run_id  적재한 flow run
```

`collected_at`은 앱 DB(Team-Neki-Server)의 다른 테이블처럼 시간대 없는 `TIMESTAMP`에
KST 벽시계를 넣습니다. `TIMESTAMPTZ`로 두면 세션 시간대(운영 파드는 UTC)로 보여
앱 쪽 테이블과 나란히 읽을 때 9시간이 어긋납니다. aware 값을 그대로 넣어도 세션
시간대로 바뀌어 들어가므로 `put_manifest`가 시간대를 떼고 넣습니다.

**실행할 때마다 행을 새로 쌓습니다. 덮어쓰지 않습니다.** 같은 사이클을 두 번
돌리면 행이 둘이고 그 둘이 곧 이력입니다. S3의 CSV도 적재 시각 이름으로 전부
남으므로 행과 파일이 1:1로 맞습니다.

그래서 `(platform, target_date)`는 유일하지 않아 키가 되지 못하고, PK는 연번
대리키 `id`입니다. **읽는 쪽은 "가장 최근 행"을 집어야 합니다.** 조건만 걸고 첫
행을 쓰면 오래된 적재를 읽습니다. 정렬은 `collected_at`이 아니라 `id`로 합니다.
같은 초에 두 번 적재되면 시각이 같아 순서가 갈리지 않습니다.

인덱스는 둘입니다. `(target_date, platform, id DESC)`는 한 사이클의 정확한 행을
집는 `read_manifest`(`read_stores`)의 길이고, `(platform, target_date DESC, id DESC)`는
브랜드마다 사이클 이하의 최신을 찾는 `read_cycle`의 길입니다. 둘 다 `id`를 꼬리에
달아 같은 사이클의 여러 적재 중 최신이 먼저 나오게 합니다.

법정동, 지하철 역과 달리 테이블 바꿔치기를 하지 않습니다. 그쪽은 매 실행이 전량
스냅샷이지만 여기는 실행마다 한 브랜드의 한 줄이 늘 뿐이라 누적이 곧 이력입니다.
`CREATE TABLE IF NOT EXISTS`는 기존 테이블을 고치지 않으므로 컬럼을 바꾸면
`DROP TABLE` 후 다시 돌려야 합니다.

**`ensure_table`이 권고 락을 먼저 잡습니다.** 브랜드 11개가 스레드로 겹쳐 도는데
테이블이 없는 첫 실행이면 동시에 만들려다 `pg_type` 유니크 위반이 납니다.
`CREATE TABLE IF NOT EXISTS`는 경합에 안전하지 않습니다.

**이 때문에 수집 flow에 `DATABASE_URL`이 필요해졌습니다.** 이전에는 S3만 있으면
돌았습니다. `persist=False`로 끄면 여전히 DB 없이 파싱만 볼 수 있습니다.

#### 대상 일자(`target_date`)

`collected_at`은 우리가 언제 받았는지고, `target_date`는 이 적재물이 어느 수집
사이클의 것인지입니다. 둘은 보통 같지만 갈릴 때가 있습니다.

```text
월요일 04:00 예약 run 이 워커 부재로 쌓였다가 수요일에 집힘
    collected_at  2026-09-23 15:02 (수)
    target_date   2026-09-21       (월)

수요일에 손으로 실행
    collected_at  2026-09-23 15:02
    target_date   2026-09-23
```

지금 도는 flow run의 예약 시각(`scheduled_start_time`)을 KST로 끊어 정합니다.
**cron을 역산하지 않습니다.** `flows/`가 스케줄을 알면 안 되기 때문입니다. 예약
시각은 Prefect가 run 레코드에 이미 박아둔 값이라 스케줄을 몰라도 읽힙니다.

**`stores_collect`가 이 값을 한 번 정해 브랜드 flow로 내려보냅니다.** 브랜드를
`ThreadPoolExecutor`로 부르는데, 스레드를 건너면 Prefect의 flow run 컨텍스트가
따라가지 않아 브랜드 run이 서브플로우가 아니라 독립 run으로 뜹니다
(`parent_flow_run_id`가 `None`). 그래서 브랜드 안에서 `target_date()`를 부르면
자기 시작 시각이 나오고 월요일 사이클이 수요일로 기록됩니다. root를 따라 올라가는
방법도 부모 연결이 없어 쓸 수 없으므로, 인자로 내려보내는 것이 유일한 길입니다.
브랜드 flow의 `target_date` 파라미터가 그것이며 백필에도 그대로 씁니다.

### 법정동 코드는 Postgres에 직접 적재합니다

지점 수집과 달리 S3를 거치지 않습니다. `collect/`는 `platform=`
파티션을 쓰고 그 값은 브랜드입니다. 법정동을 넣으려면 `Platform`에 브랜드가 아닌
값을 더해야 하고, 그러면 `stores_collect`의 `BRANDS` 순회에 섞여 들어갑니다.

접속은 `DATABASE_URL` 하나입니다. Prefect Block을 쓰지 않습니다. Block은 UI에서
바꿀 수 있어 편하지만 설정이 Prefect 서버 상태에 얹히므로 서버를 갈아치우거나
다른 환경에서 같은 flow를 돌릴 때 값이 따라오지 않습니다.

DDL은 `flows/legal_dong/table.py`가 들고 있습니다. 원래 스키마 주인은
Team-Neki-Server의 Flyway이고 `TB_` 접두와 `COMMENT ON`은 그쪽 규약을 따른
것입니다. Spring이 이 테이블에 엔티티를 붙일 때는 DDL을 마이그레이션으로 떠가면
됩니다.

수집원은 공공데이터포털의 국토교통부 전국 법정동(`15063424`)입니다. 인증이 필요
없고 utf-8 CSV 1.4MB에 20,561행이 옵니다. 행안부 법정동코드 **API(`15077871`)는
쓰지 않습니다.** 서비스키 신청과 승인이 필요한데 한 달에 한 번 전량을 갈아끼우는
용도에 키 관리를 얹을 이유가 없습니다. 실시간 단건 조회가 필요해지면 그때가 API
차례입니다.

세 단계입니다. **이름 교정을 다운로드에 섞지 않습니다.**

```text
source.py     CSV 를 받아 원문 행으로            (사이트가 준 것만)
normalize.py  붙은 시군구명을 나누고 계층을 파생   (우리 규칙)
table.py      Postgres 에 반영                   (적재)
```

교정을 `source`에 넣으면 규칙을 고칠 때마다 사이트를 다시 긁어야 하고, 사이트가
준 값과 우리가 만든 값을 구분할 수 없게 됩니다. 지점 수집의 collect/enrich 경계와
같은 기준입니다 — 값이 틀렸을 때 `source`까지는 사이트 잘못이고 그 다음은 우리
잘못입니다.

### 붙은 이름은 정규식으로 자르지 않습니다

원문의 `시군구명`이 특례시 일반구 39종에서 공백 없이 옵니다. 20,561행 중
1,755행이 영향을 받습니다.

```text
원문 : 수원시영통구      →  정규화 : 수원시 영통구
원문 : 고양시덕양구      →  정규화 : 고양시 덕양구
```

**`^(.+시)(.+구)$` 같은 정규식을 쓰면 안 됩니다.** `군위군`, `시흥시`, `군포시`,
`군산시`처럼 두 단위가 붙은 것처럼 보이는 정상 이름이 실제로 있어서 멀쩡한 이름을
자릅니다. 대신 같은 시도 안의 다른 시군구 행 이름을 접두로 떼면 경계가 나옵니다.
`수원시영통구`는 형제 `수원시`가 접두라서 나뉘고, `군위군`은 접두인 형제가 없어
그대로 남습니다.

**세종특별자치시에는 없는 시군구 `세종시`가 채워져 있습니다.** 시군구 계층 행이
하나뿐인 시도는 실제로 시군구가 없으므로 그 이름을 자리 채우기로 보고 `None`으로
둡니다. 전량에서 세종 하나만 걸리며, 둘 이상이면 원문 구조가 바뀐 것이므로
`normalize`가 경고를 남깁니다.

계층은 이름이 아니라 **10자리 코드의 자리수**(시도2+시군구3+읍면동3+리2)로
읽습니다. 채워진 이름 컬럼 개수로 세면 안 됩니다. 세종의 읍면동 행에는 시군구명이
채워져 있는데 그것이 자리 채우기입니다.

### 테이블 하나에 조인 없이 담습니다

앱에서 사용자가 "강남"을 검색하면 `강남구`와 `강남동`이 같은 목록에 나와야 하고
어느 강남동인지도 함께 보여야 합니다. 상위 계층을 따라가는 조인이 끼면 검색 한
번에 조인이 셋 붙으므로 상위 이름을 같은 행에 펼쳐 담습니다.

컬럼은 계층 순서를 그대로 따릅니다. 코드와 명칭이 계층마다 짝으로 붙고, 그
계층들에서 뽑아낸 이름 둘이 뒤에 옵니다.

```text
code  level
sido_code  sido_name    시도
sgg_code   sgg_name     시군구
umd_code   umd_name     읍면동
ri_code    ri_name      리
leaf_name  full_name    뽑아낸 이름
created_on
```

**상위 계층 명칭을 컬럼으로 담습니다.** 리 행의 `무장면`이 `full_name` 안에만
있으면 앱이 문자열을 쪼개야 합니다. 원본이 열로 주는 것을 버리지 않습니다.
코드는 `code`의 자리수를 잘라낸 것이라 늘 있고 명칭은 그 계층이 없으면 NULL이라,
시도 행의 `sgg_code`는 `000`인데 `sgg_name`은 NULL입니다.

`leaf_name`과 `full_name`을 둘 다 담는 이유가 여기 있습니다.

- `leaf_name` : 가장 아래 계층의 명칭(`역삼동`, `강남구`, `무장면`). **검색용**
- `full_name` : 전체 경로(`서울특별시 강남구 역삼동`). 표시용

`full_name`으로 검색하면 안 됩니다. "강남"에 `강남구`와 함께 그 아래 역삼동,
개포동까지 15건이 딸려 나옵니다. `leaf_name`으로 찾으면 `강남구`, (진주시)
`강남동`, (고창군 무장면) `강남리` 셋만 남습니다.

**특례시 일반구는 `sgg_name`과 `leaf_name`이 갈립니다.** 원문이 `수원시장안구`
한 덩어리로 오므로, 표기에 공백을 넣는 것으로 끝내지 않고 나눈 경계를
`leaf_name`에도 써야 합니다.

```text
sgg_name   수원시 장안구
full_name  경기도 수원시 장안구
leaf_name  장안구
```

`leaf_name`이 `수원시 장안구`면 접두 검색이라 "장안"으로 39개 일반구가 하나도
나오지 않고 `수원시`로 찾아야만 나옵니다. 경계를 아는 곳은 이름을 나누는
`_split_sgg` 하나뿐이므로 뒷부분을 거기서 함께 돌려줍니다. 호출부에서 공백으로
다시 쪼개면 원문이 처음부터 공백을 갖고 온 이름과 우리가 넣은 공백을 구분할 수
없습니다.

인덱스는 `text_pattern_ops`여야 합니다. 기본 연산자 클래스는 콜레이션에 묶여
`LIKE '강남%'`가 인덱스를 타지 않습니다. 중간 일치(`'%강남%'`)가 필요해지면
`pg_trgm` GIN이 필요한데 확장 설치 권한이 걸리므로, 지금은 접두 검색만 지원합니다.

이름이 겹치는 행이 **하나 있습니다.** 원문이 세종에 시도 행(`3600000000`)과
시군구 행(`3611000000`)을 둘 다 두어 `세종특별자치시`가 두 번 나옵니다. 앱에서
"세종"을 검색하면 같아 보이는 두 행이 나오므로 `level`로 걸러야 합니다.

행정동은 담지 않습니다. 필요해지면 법정동과 다대다라 같은 행에 넣을 수 없고,
행 종류를 나누거나 중복을 감수해야 합니다. 지점 매핑 자체에는 필요하지 않습니다.
**Kakao가 좌표든 주소든 법정동코드와 행정동코드를 한 번에 주기 때문입니다**
(`coord2regioncode`의 `B`/`H`, `search/address`의 `b_code`/`h_code`).

### 적재는 증분이 아니라 테이블 바꿔치기입니다

매 실행이 전량이므로 무엇을 넣고 무엇을 지울지 계산할 이유가 없고, 계산하지 않으면
그 계산이 틀릴 일도 없습니다. 날짜를 붙인 테이블을 따로 만들어 채우고 이름만
맞바꿉니다.

```text
1. tb_legal_dong_prev 를 버린다
2. tb_legal_dong_20260825 를 만들고 COPY 로 채운다
3. 인덱스를 건다                       <- 채운 다음이어야 빠르다
4. tb_legal_dong -> tb_legal_dong_prev
5. tb_legal_dong_20260825 -> tb_legal_dong
```

전부 한 트랜잭션입니다. Postgres는 DDL도 트랜잭션에 들어가므로 4~5가 원자적으로
일어납니다. 읽는 쪽은 이전 테이블을 끝까지 보다가 커밋 시점에 새 테이블로 넘어가고
중간 상태를 볼 수 없습니다.

**`_prev`를 남기는 것이 바꿔치기의 값입니다.** 새 스냅샷이 이상하면
`tb_legal_dong`을 버리고 `_prev`를 되돌리면 끝납니다. 증분 갱신은 되돌릴 것이
남지 않습니다.

```sql
DROP TABLE tb_legal_dong;
ALTER TABLE tb_legal_dong_prev RENAME TO tb_legal_dong;
```

날짜는 KST로 끊습니다. UTC로 끊으면 새벽 실행이 전날 이름을 갖습니다. 지점 수집의
파티션 날짜와 같은 규칙입니다.

**스왑에는 `lock_timeout`이 필요합니다.** 이름을 바꾸려면 ACCESS EXCLUSIVE 락이
필요한데, 이 락을 기다리는 요청은 뒤이어 오는 읽기까지 자기 뒤에 줄 세웁니다.
앱이 긴 조회를 물고 있으면 스왑이 기다리는 동안 앱 전체가 멈추므로, 5초 안에 못
잡으면 실패하고 다음 실행에 맡깁니다.

**`_prev` 정리를 스왑 직전이 아니라 맨 앞에서 합니다.** Postgres는 테이블 이름을
바꿔도 인덱스 이름을 따라 바꾸지 않습니다. 스왑 직전에 치우면 같은 날 재실행할 때
새 인덱스 이름이 `_prev` 쪽 이름과 부딪혀 뒤에 번호가 붙고, 그 번호가 실행마다
올라갑니다. 먼저 비우면 이름이 돌아와 번호가 `1`에서 묶입니다. 한 트랜잭션이므로
뒤에서 실패하면 이 삭제도 되돌아갑니다.

**인덱스 이름을 표준 이름으로 맞추려 들지 않습니다.** 날짜가 붙은 채로 두면
세대끼리 부딪히지 않고, `\d tb_legal_dong`이 어느 스냅샷으로 만든 테이블인지
알려줍니다.

`collected_at` 컬럼을 두지 않습니다. 20,561행에 같은 값을 반복하는 것이고, 스왑
뒤에는 테이블 이름에서 날짜가 사라지므로 그 사실은 테이블 코멘트에 남깁니다.
원본 데이터셋 이름도 함께 넣어 기준일자를 알 수 있게 합니다.

```text
법정동 코드 테이블 (현존만). 2026-08-25 적재, 원본 국토교통부_전국 법정동_20260630
```

`COMMENT`는 유틸리티 문이라 파라미터를 받지 못합니다. 값을 직접 이어붙이지 않고
`psycopg.sql.Literal`에 맡깁니다.

파싱이 0건이면 바꿔치우지 않고 예외를 냅니다. 0건짜리 테이블을 들이면 앱 검색이
통째로 죽고 쓸 만한 테이블이 `_prev`로 밀려납니다.

### 수집원 쪽에서 걸리는 것들

- 두 단계입니다. 상세 페이지로 세션을 얻고, POST로 첨부파일 id를 받은 뒤 그
  id로 내려받습니다. 로그인은 필요 없습니다
- **첨부파일 id를 코드에 박지 않습니다.** 데이터셋이 갱신되면 id가 바뀌므로
  박아두면 갱신된 뒤에도 옛 파일을 계속 받습니다
- 핸들 응답의 `Content-Type`이 `text/html`인데 본문은 JSON입니다
- CSV가 BOM을 달고 옵니다. `utf-8`로 읽으면 첫 열 이름에 BOM이 붙어 헤더 조회가
  조용히 빗나가므로 `utf-8-sig`로 읽습니다
- 실패해도 200에 HTML이 옵니다. 헤더 첫 열 이름으로 걸러야 파싱까지 끌고 가지 않음

### 정기 수집이 필요한 이유

법정동은 2015~2022년에 연 50~230건 바뀌다가 2023년 3,148 / 2024년 3,908 /
2026년 7,045건으로 늘었습니다. 가장 최근인 2026-07-01에는 6,576건이 한 번에
바뀌며 광주광역시와 전라남도가 폐지되고 전남광주통합특별시(코드 접두 `12`)가
생겼습니다. **손으로 넣은 파일은 이런 개편을 놓치고, 놓치면 조인이 실패하는 대신
지점이 검색에서 조용히 사라집니다.**

이름은 개편으로 움직이지만 코드로 조인하면 안전합니다. Kakao가 돌려준 서로 다른
법정동코드 701개가 전부 마스터에 존재하고 폐지된 것이 없음을 확인했습니다.

주기는 매월 1일입니다. **개편이 대개 1일에 시행되지만 원본이 그날 바로 갱신되지는
않으므로, 시행일 새벽에 받으면 아직 옛 스냅샷일 수 있습니다.** 그 개편은 다음 달
1일에야 들어오니 반영 지연 상한이 한 달입니다.

그 한 달을 감수하는 이유는 조인이 코드로 이루어지기 때문입니다. 새로 생긴 코드가
마스터에 없는 동안에만 지연이 드러납니다. 더 빨리 반영해야 할 일이 생기면 cron을
당기는 것으로 끝납니다. 다운로드가 1.4MB 한 번이라 비용이 없습니다.

### 지하철 역은 Postgres 에 직접 적재합니다

법정동과 같은 이유로 S3 를 거치지 않습니다. `platform=` 파티션은 브랜드라, 역을
넣으면 `stores_collect`의 브랜드 순회에 섞여 들어갑니다.

수집원은 국가철도공단 표준데이터이고 **레일포털에서 로그인도 서비스키도 없이**
받아집니다. 공공데이터포털 사본(`15093755`)은 `atachFileYn`이 `N`인 링크형이라
첨부파일 다운로드가 404 로 끝나고, 사본이 원본보다 1년 반 낡았습니다.

```text
GET https://data.kric.go.kr/rips/dataset/download.file?type=filedata&id=32&operation=1
  -> 전체_도시철도역사정보_20260630.xlsx  1,099행 × 15열
```

- xlsx 만 옵니다. `openpyxl` 의존이 여기서 늘었습니다
- 실패해도 200 에 HTML 이 옵니다. xlsx 는 zip 이라 앞 두 바이트(`PK`)로 가릅니다
- **`Content-Disposition`의 파일 이름이 utf-8 바이트인데 HTTP 헤더는 latin-1 로
  디코딩됩니다.** 되돌리지 않으면 한글이 깨진 채로 테이블 코멘트에 박힙니다

#### 같은 역의 여러 노선을 한 행으로 합치지 않습니다

원문 한 행은 역이 아니라 **역 × 노선**입니다. 강남역이 2호선과 신분당선 두 행으로
들어 있습니다.

사용자가 "강남"을 검색하면 `강남 2호선`과 `강남 신분당선`이 각각 나오고, 그중 하나를
눌러 **그 좌표 1km 안의 포토부스**를 받습니다. 강남역을 한 행으로 만들면 고를 수가
없고, 노선마다 다른 좌표가 하나로 뭉개져 반경 결과도 같아집니다. 광운대는 노선별로
좌표가 실제로 다릅니다.

**1km 반경 매핑은 이 flow 가 하지 않습니다.** 부스 쪽에 붙으므로 포토부스 flow 의
몫입니다.

#### 담는 것은 셋뿐입니다

```text
name  line_name  location    PRIMARY KEY (name, line_name)
```

쓰임이 역명 검색과 좌표뿐이라 영문명, 주소, 운영기관, 전화번호, 환승역구분,
데이터기준일자, 역번호, 노선번호를 담지 않습니다. 환승 여부는 같은 `name`의 행
수로 드러납니다. 적재 결과는 1,098행입니다.

**시도를 담지 않는 것이 중요합니다.** 담으려면 시도 목록을 코드에 박아야 하는데,
2026-07-01 에 광주광역시와 전라남도가 폐지되어 `tb_legal_dong`의 시도는 이미
16개입니다. 동명이역(`송정` 서울 5호선 / 부산 동해선)은 `line_name`과 좌표로 갈립니다.

#### 키는 `(name, line_name)` 입니다

사용자가 고르는 단위가 그대로 키입니다. 1,098행 전부 유일합니다.

**연번 대리키를 쓰지 않습니다.** 매 실행이 테이블을 새로 만들어 바꿔치므로 연번은
COPY 순서로 매겨지는데, 원문 앞쪽에 역이 하나 생기면 그 뒤 전부가 밀립니다. 앱이
들고 있던 id 가 조용히 다른 역을 가리키게 되고, 터지지 않아서 더 나쁩니다.

**원문 식별자(역번호, 노선번호)도 키가 못 됩니다.** `I4108` 하나에 경의중앙선과
경춘선이 매달려 있어 4건이 부딪히고, 그것으로 키를 잡으면 `광운대 경춘선`이
검색에서 사라집니다.

`normalize`가 합치는 것은 모든 열이 같은 한 행(경인선 주안역)뿐입니다.

#### 노선명은 다수결로 합치지 않습니다

같은 노선이 `7호선`과 `도시철도 7호선`으로 오고 공백이 둘 들어간 표기도 있습니다.
검색 결과에 그대로 보이는 값이라 골라야 합니다.

**노선번호로 다수결을 내면 안 됩니다.** `I4108`에 경의중앙선 51행과 경춘선 3행이
매달려 있는데 실제로 다른 노선이라, 다수결이면 경춘선이 둔갑합니다. 47종뿐이라
합칠 것만 `LINE_ALIAS`에 적습니다. 지역 접두는 떼지 않습니다. `대구 도시철도 1호선`
에서 `대구`를 떼면 서울 1호선과 같아집니다.

#### 좌표는 부스 테이블과 같은 모양으로 담습니다

위경도를 컬럼으로 두지 않고 `location geometry(POINT, 4326)` 하나로 담습니다.
`tb_photo_booth_location`이 이미 그렇게 하고 `ST_X` / `ST_Y`로 돌려줍니다. PostGIS 는
Team-Neki-Server 의 Flyway 가 이미 들여놓았습니다.

반경 계산에서 두 테이블을 섞어 쓰는데 한쪽만 타입이 다르면 변환이 끼고, **변환이
끼면 인덱스가 죽습니다.**

**`location`에는 인덱스를 걸지 않습니다.** 반경 질의는 이 테이블이 아니라 부스
테이블에 겁니다. 인덱스는 `name`의 `text_pattern_ops` 하나뿐입니다. PK 인덱스는
기본 연산자 클래스라 `LIKE '강남%'`가 타지 않습니다.

**부스 컬럼이 `geography`가 아니라 SRID 4326 `geometry`입니다.** 거리 단위가 미터가
아니라 도(degree)라서 도 단위로 걸러낸 뒤 미터를 따로 확인해야 합니다.

```sql
WHERE ST_DWithin(b.location, s.location, 0.012)            -- 인덱스가 타는 자리
  AND ST_DistanceSphere(b.location, s.location) <= 1000    -- 정확한 미터
```

`::geography`로 캐스팅하면 미터가 바로 나오지만 기존 geometry GiST 를 못 씁니다.

적재는 법정동과 같은 테이블 바꿔치기입니다. 같은 마스터라서가 아니라 매 실행이 전량
스냅샷이라 같은 답이 나옵니다.

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
- `deploy.py` : 운영용. 스케줄만 등록하고 실행은 worker가 담당함. 운영에서는 worker
  파드의 initContainer가 이미지 안에서 실행함

**운영 스케줄 등록은 반드시 `deploy.py`를 거쳐야 합니다.** `prefect deploy`나
`flow.deploy()`를 직접 호출하면 아래 보존 로직을 건너뜁니다.

## pause는 배포가 덮어씁니다

Prefect는 deployment를 등록할 때 기존 정의를 통째로 덮어씁니다. 따라서 운영자가
UI에서 꺼둔 스케줄이 배포할 때마다 되살아납니다. Airflow에서 DAG를 pause하면 그
상태가 메타DB에 남아 유지되는 것과 다릅니다.

재등록이 일어나는 시점은 다음과 같습니다.

- `serve.py` 재시작 : 매번 재등록하므로 프로세스가 뜰 때마다 풀림
- `deploy()` 재실행 : 배포할 때마다 풀림
- 이미지 태그 갱신으로 worker 파드가 재시작될 때 : initContainer가 `deploy.py`를
  돌리므로 재등록되지만 아래 보존 로직 덕에 pause는 유지됨
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

반면 wheel을 빌드하면 나열된 패키지만 포함됩니다. 운영 이미지는 wheel 대신
`Dockerfile`이 `deployments/`, `flows/`, `deploy.py`를 직접 복사하므로, 최상위
패키지를 추가하면 `packages`와 Dockerfile의 `COPY` 둘 다에 넣어야 합니다. 빠뜨리면
`make check`는 통과하고 `make image`에서만 `ModuleNotFoundError`가 납니다.

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

flow 로직만 바꿨다면 단독 실행으로 충분합니다. 수집 flow는 S3와 Postgres에
적재하므로 LocalStack이 떠 있고 `DATABASE_URL`이 있어야 합니다.

```bash
make localstack
make hello
make lifefourcuts
make photoism
make dontlxxkup
make photosignature
make photogray
make planbstudio
make picdot
make monomansion
make harufilm
make photolabplus
make broomstudio
make collect
make s3-ls
make subway-station
make legal-dong
```

`subway-station`은 `DATABASE_URL`이 있어야 돕니다. 파싱과 정규화만 볼 때는
`subway_station(persist=False)`로 끄면 `DATABASE_URL` 없이도 됩니다.

적재를 건드렸다면 **테이블이 없는 상태부터** 확인해야 합니다. 첫 실행에는 바꿔칠
대상이 없어 경로가 다릅니다. 그다음 여러 번 돌려 `tb_subway_station`과 `_prev`가
둘 다 1,098행인지, 인덱스 이름의 번호가 계속 올라가지 않는지 봅니다.

`normalize.py`를 건드렸다면 실행 로그의 "역명 접미사 제거"가 359, "노선명 표기
통일"이 66에서 움직이지 않는지, "시도를 채우지 못한 역" 경고가 없는지 봐야 합니다.

`legal-dong`은 `DATABASE_URL`이 있어야 돕니다. 파싱과 정규화만 볼 때는
`legal_dong(persist=False)`로 끄면 `DATABASE_URL` 없이도 됩니다.

적재를 건드렸다면 **테이블이 없는 상태부터** 확인해야 합니다. 첫 실행에는 바꿔칠
대상이 없어 경로가 다릅니다. 그다음 여러 번 돌려 `tb_legal_dong`과 `_prev`가 둘 다
20,561행인지, 인덱스 이름의 번호가 계속 올라가지 않는지 봅니다.

```bash
psql -c 'DROP TABLE IF EXISTS tb_legal_dong, tb_legal_dong_prev'
make legal-dong
make legal-dong
psql -c "select tablename, indexname from pg_indexes where tablename like 'tb_legal_dong%'"
```

`normalize.py`를 건드렸다면 실행 로그의 "시군구명 분리" 건수가 1,755에서 움직이지
않는지, "시군구가 없는 시도로 판정"이 세종 하나인지 봐야 합니다. 둘 중 하나가
바뀌면 멀쩡한 이름을 자르고 있을 수 있습니다.

DDL을 바꿨다면 `CREATE TABLE IF NOT EXISTS`가 기존 테이블을 고치지 않으므로
`DROP TABLE tb_legal_dong` 후 다시 돌려야 합니다.

`picdot`, `monomansion`, `photogray`, `harufilm`, `photolabplus`, `broomstudio`는
`KAKAO_API_KEY`가 있어야 돕니다. 키가 없으면 `flows/common/kakao.py`의 `api_key()`가
`RuntimeError`로 막습니다.

`planbstudio`, `photosignature`, `lifefourcuts`, `photoism`, `dontlxxkup`은 키가
없어도 돌지만 좌표 보정만 건너뜁니다. `geocode.py`를 고쳤다면 이 다섯을 키가 있는
상태와 없는 상태로 모두 돌려, 없을 때 수집 자체는 끝까지 가는지 봐야 합니다.
보정 자체는 요약 로그(`없던 N건 중 주소로 …`)와 적재물의 `coordinate_source`로
확인합니다.

적재를 건드렸다면 실행 결과가 아니라 적재물을 봐야 합니다. `make s3-ls`로 키가
빠짐없이 올라갔는지 보고, 같은 flow를 두 번 돌려 `collect/`에 CSV가 하나 늘고
`_raw/<실행 시각>/` 폴더도 하나 늘며 `tb_store_collect_manifest`에 행도 하나
느는지, 그 행의 `s3_path`가 나중 파일을 가리키는지 확인합니다. CSV와 raw 폴더의
실행 시각이 같아야 합니다. `.gz`, `raw/`, `runs/`, `_manifest.json`이 다시 생기면
안 됩니다. 객체의 Content-Type이 비어 있어도 안 됩니다.

manifest 테이블을 건드렸다면 **테이블이 없는 상태부터** 확인해야 합니다.
`ensure_table`이 처음 만드는 경로가 따로입니다.

```bash
psql -c 'DROP TABLE IF EXISTS tb_store_collect_manifest'
make collect
make collect
psql -c "select platform, count(*) from tb_store_collect_manifest group by 1"
```

브랜드마다 행이 2건이어야 합니다. 1건이면 덮어쓰고 있는 것이고, 인덱스 이름에
번호가 붙으면 `IF NOT EXISTS`가 빠진 것입니다.

`flows/common/`의 수집 모듈을 고쳤다면 그것을 쓰는 브랜드를 모두 돌려 건수가
전과 같은지 봐야 합니다. `imweb_map.py`는 인생네컷과 포토이즘, 돈룩업이 함께 씁니다.

파싱만 확인하고 싶으면 적재를 끕니다.

```bash
uv run --env-file .env python -c \
  "from flows.picdot_stores import picdot_stores; picdot_stores(persist=False)"
```

`pyproject.toml`의 `packages`를 건드렸다면 wheel 내용을 확인합니다.

```bash
make build
```

`Dockerfile`이나 의존성을 건드렸다면 이미지를 빌드해 안에서 확인합니다. 운영과 같은
uid와 읽기 전용 루트로 돌리므로 파일 권한 문제도 여기서 드러납니다.

```bash
make image
```

`deploy.py`나 `deployments/`를 건드렸다면 로컬 서버를 띄워 배포 사이클을 확인해야
합니다. 실제 스케줄을 다루는 코드라 import 성공만으로는 회귀를 잡을 수 없습니다.

```bash
PREFECT_HOME=/tmp/pf-test uv run prefect server start --host 127.0.0.1 --port 4301
export PREFECT_API_URL=http://127.0.0.1:4301/api
uv run prefect work-pool create neki-pool --type process
WORKFLOW_IMAGE=ghcr.io/team-neki/team-neki-workflow:local make deploy PORT=4301
uv run prefect deployment inspect hello/hello-local | grep image
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
