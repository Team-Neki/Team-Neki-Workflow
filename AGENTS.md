# AGENTS.md

이 문서는 이 저장소에서 코드를 작성할 때 지켜야 할 규약과 빠지기 쉬운 함정을 다룹니다.
사람과 코딩 에이전트가 같이 읽습니다.

## 정본은 docs/spec 입니다

파이프라인의 **정책**은 `docs/spec/` 아래 문서가 정본입니다. 이 문서와 코드는
그것을 따릅니다.

- `docs/spec/collect-pipeline.md` : 지점 수집(collect). 산출물 레이아웃, manifest
  테이블, 대상 일자, 대신하기 규칙, 스케줄, 읽는 쪽 계약
- `docs/spec/enrich-pipeline.md` : 지점 법정동 보강(enrich). 입력, 재사용, 상태,
  두 산출물
- `docs/spec/search-index.md` : 검색 색인 실행(search-index). 서버 batch 잡을
  k8s Job 으로 띄우는 계약

지켜야 할 것은 셋입니다.

- **수집 코드를 건드리기 전에 해당 spec 을 읽습니다.** 이 문서의 수집 절은 요약이고
  근거와 세부는 spec 에 있습니다
- **동작을 바꾸면 spec 을 같은 PR 에서 먼저 고칩니다.** spec 과 코드가 어긋나면 spec
  이 맞고 코드가 틀린 것입니다. 코드를 먼저 바꿔야 할 사정이 있으면 PR 본문에 spec
  의 어느 항목이 왜 바뀌는지 적습니다
- **spec 의 항목은 구현 위치를 anchor 로 가리킵니다.** 형식은 `` `경로:줄` `심볼` ``
  이고, `make spec-check`(`make check` 에 포함)가 그 줄에 그 심볼이 있는지 확인합니다.
  코드를 옮겨 줄이 밀리면 CI 가 막으므로 anchor 의 줄 번호를 같이 고칩니다. 심볼이
  사라졌다면 정책이 바뀐 것이니 문서 본문을 고칩니다. anchor 만 맞추고 본문을
  그대로 두면 안 됩니다

spec 에 없는 규약(구조, 네이밍, 배포, 검증 절차)과 함정은 이 문서가 정본입니다.

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
  stores_enrich/          법정동 보강. collect 의 최신 CSV 를 읽어 Kakao 로 b_code 를 붙임
    region.py             판정 (재사용, 폴백, 상태)
    table.py              tb_photo_booth_enriched 바꿔치기
  search_index/           서버 색인 잡을 k8s Job 으로 띄우고 기다림
    job.py                매니페스트와 실행
  common/                 여러 워크플로가 함께 쓰는 task
    store.py              수집 공통 스키마 (CollectedStore)
    storage.py            S3 적재
    geocode.py            좌표가 빈 지점을 Kakao로 보정
    imweb_map.py          imweb 지도 위젯 수집 (인생네컷, 포토이즘, 돈룩업)
aws/config                로컬 개발용 AWS 프로파일
compose.yaml              로컬 S3 (LocalStack)
Dockerfile                운영 이미지. worker 와 flow run 이 같이 씀
.github/workflows/        ci.yml (PR 검증), build.yml (main merge 시 이미지 푸시, GitOps 갱신)
docs/spec/                수집 파이프라인 정책 (정본). 항목마다 구현 위치 anchor
docs/runbook.md           배포 절차와 장애 대응
spec_check.py             docs/spec 의 anchor 가 코드 줄과 맞는지 확인 (make check 에 포함)
tests/                    순수 판정 테스트. make check 가 돌림. Kakao 와 DB 는 부르지 않음
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

## 수집 파이프라인

정책은 `docs/spec/collect-pipeline.md` 가 정본입니다. 여기서는 코드를 만질 때 바로
부딪히는 것만 추립니다.

- collect 는 사이트가 준 것만 담고 해석하지 않음. 주소 판정, 상호명 떼기는 enrich
  의 일. 예외는 좌표 보정 하나이고 `coordinate_source` 로 구분함
- 브랜드별 `Store` 를 두지 않음. `flows/common/store.py` 의 `CollectedStore` 하나.
  필드를 더하면 `storage.COLUMNS` 에도 넣어야 함. 빠뜨리면 `DictWriter` 가 막음
- 브랜드를 더하면 `flows/stores_collect/flow.py` 의 `BRANDS` 에도 등록. 빠뜨리면
  정기 수집에서만 조용히 빠짐
- imweb 브랜드(인생네컷, 포토이즘, 돈룩업)는 `flows/common/imweb_map.py` 하나가
  맡음. 브랜드별 분기를 이 모듈에 넣지 않음
- S3 는 `collect/platform=<브랜드>/dt=<대상 일자>/<실행 시각>.csv` 와 같은 파티션의
  `_raw/<실행 시각>/`. `raw/`, `runs/`, `_manifest.json`, `.gz` 는 없음. 다시
  생기면 회귀
- 어느 CSV 가 현재인지는 Postgres `tb_store_collect_manifest` 가 앎. 행은 쌓기만
  하고 덮어쓰지 않음. 읽는 쪽은 `id` 내림차순 최신 행의 `s3_path` 만 따라감
- `collected_at` 은 앱 DB 규약대로 시간대 없는 `TIMESTAMP` 에 KST 벽시계. 넣기 전에
  시간대를 뗌
- `target_date` 는 적재일이 아니라 사이클 날짜. `stores_collect` 가 한 번 정해
  브랜드 flow 로 내려보냄. 스레드를 건너면 Prefect 컨텍스트가 따라가지 않기 때문
- 실패한 브랜드는 `manifest.read_cycle` 이 읽는 시점에 7일 안의 최근 사이클로
  대신함. 규칙은 그 함수 한 곳. `stores_collect` 와 enrich 가 같은 함수를 씀
- 적재 task 는 S3 에 올리기 전에 `ensure_table` 로 DB 에 먼저 닿음. 순서를 바꾸면
  DB 장애 때 가리킬 행 없는 CSV 가 남음
- cron 은 `Cron(..., timezone="Asia/Seoul")`. 빠뜨리면 UTC 라 13:00 KST 에 돎.
  브랜드별 deployment 는 cron 없이 둠
- 수집 flow 는 `DATABASE_URL` 이 있어야 돎. `persist=False` 면 DB 도 S3 도 없이
  파싱만 봄

## 보강 파이프라인 (enrich)

정책은 `docs/spec/enrich-pipeline.md` 가 정본입니다. 코드를 만질 때 바로 부딪히는
것만 추립니다.

- collect 와 별도 flow. `stores_collect` 안에서 부르지 않음. 합치면 enrich 실패
  재시도가 사이트를 다시 긁음. 순서는 cron 시각(04:00 / 05:00 KST)으로 맞춤
- 무엇을 읽을지는 `manifest.read_cycle` 이 정함. enrich 가 따로 정하지 않음
- `EnrichedStore` 의 필드 순서가 CSV 열이자 COPY 열이자 DDL 순서. 필드를 더하면
  `table._ddl` 도 같은 자리에 넣어야 함. 한쪽만 고치면 값이 엉뚱한 컬럼에 들어감
- 주소 문자열을 해석하지 않음. 계층은 `b_code` 자리수에, 이름은 `tb_legal_dong` 에
  있음. 시군구 비교는 경고용 느슨한 토큰 비교 하나뿐. 인천 개편 뒤 사이트 주소가
  옛 구 이름이라 인천 20여 건이 늘 경고로 뜨는데 코드는 맞음
- Kakao 조회는 task 하나 안의 쓰레드. 지점마다 task 를 만들지 않음. 쓰레드 안에서
  `get_run_logger` 를 부르지 않음 (컨텍스트가 따라가지 않음)
- 색인은 여기서 띄우지 않음. 묶으면 enrich 재시도가 색인을 되풀이하고 색인 실패가
  enrich 를 실패로 만듦. 별도 flow `search-index` 가 시각으로 뒤에 돎

## 검색 색인 (search-index)

정책은 `docs/spec/search-index.md` 가 정본입니다.

- 하는 일은 서버 batch 의 `searchIndexJob` 을 k8s Job 으로 띄우고 기다리는 것뿐.
  `NEKI_BATCH_IMAGE` 가 없으면 경고 후 끝남
- 매니페스트는 `generateName` 이 아니라 `metadata.name` 이어야 함.
  prefect-kubernetes 가 이름으로 상태를 읽음
- `prefect-kubernetes` 버전은 Dockerfile 베이스와 같아야 함. 다르면 이미지 안의
  것이 교체됨
- BACKEND-65 가 `searchIndexJob` 을 넣기 전까지 스케줄 없음. 들어오면
  `deployments/search_index.py` 에 05:30 KST cron 을 붙임

## 마스터 데이터 (법정동, 지하철 역)

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

`make check` 는 spec anchor 검사(`make spec-check`)를 포함합니다. 코드를 옮겨 줄이
밀렸으면 `docs/spec/` 의 anchor 를 같이 고칩니다. 수집 쪽 검증 절차는
`docs/spec/collect-pipeline.md` 의 "변경 검증" 절이 정본이고 아래는 그 요약입니다.

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
make enrich
make search-index
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

`enrich` 는 `make collect` 뒤에 돕니다. 판정을 건드렸다면 두 번 돌려 첫 실행이
`ok`, 둘째가 `reused` 인지, `KAKAO_API_KEY` 를 비우고도 완주하는지 봅니다. 적재를
건드렸다면 테이블이 없는 상태부터 확인하고 두 번 돌려 `tb_photo_booth_enriched`
와 `_prev` 가 같은 건수인지, 인덱스 이름의 번호가 `1` 을 넘지 않는지 봅니다.

```bash
psql -c 'DROP TABLE IF EXISTS tb_photo_booth_enriched, tb_photo_booth_enriched_prev'
make enrich
make enrich
psql -c "select geocode_status, count(*) from tb_photo_booth_enriched group by 1"
```

`search-index` 는 로컬에서 `NEKI_BATCH_IMAGE` 가 없어 경고 후 끝나는 것이
정상입니다. Job 을 실제로 띄우는 확인은 staging 에서 합니다.

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
