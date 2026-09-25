# 지점 법정동 보강(enrich) flow 설계

- **작성일**: 2026-09-25
- **티켓**: BACKEND-64 (에픽 BACKEND-40 파이프라인 설계). 클러스터 쪽 준비는 BACKEND-143 (GitOps, 같은 에픽)
- **브랜치**: `feature/BACKEND-64-stores-enrich`
- **목표**: collect 가 남긴 지점 좌표를 Kakao `coord2regioncode` 로 법정동 코드로 바꿔 S3 와 Postgres 에 세대로 남기고, 끝나면 서버 batch 의 색인 잡을 k8s Job 으로 띄움
- **결정 이력**: 처음에는 `stores_collect` 끝에서 enrich 를 직접 부르는 안이었으나, enrich 실패 때 부모 run 재시도가 사이트를 다시 긁게 되므로 별도 deployment 로 분리하고 시각(컷오프)으로 순서를 맞추는 것으로 바꿈. 주소 문자열을 컬럼으로 나누고 이름을 정규화하는 안은 코드와 마스터 테이블이 이미 그 정보를 갖고 있어 하지 않기로 함

이 문서는 enrich 단계의 경계, 입력과 판정 규칙, 두 산출물의 모양, 색인 Job 실행 계약, 스케줄과 장애 동작에 대해 다룹니다. 정책의 정본은 구현 뒤 `docs/spec/enrich-pipeline.md` 로 옮기며, 이 문서는 그 결정에 이른 이유를 남깁니다.

> **갱신 (2026-09-25, 구현 뒤)**: 색인 Job 은 enrich 안에서 띄우지 않고 별도 flow `search-index` 로 분리했습니다 (`docs/spec/search-index.md`). 색인 Job 절과 `run_index` 서술은 분리 전 설계입니다. 결과 테이블의 `region_1/2/3depth_name` 은 `sido_name / sgg_name / umd_name` 으로 바꿨습니다 (BACKEND-151). 현재 정책은 `docs/spec/enrich-pipeline.md` 가 정본입니다.

---

## 1. 배경과 경계

파이프라인은 collect, enrich, index 세 단계입니다. 어느 단계에 코드를 둘지는 `docs/spec/collect-pipeline.md` 의 두 질문으로 가릅니다. 다시 돌리려면 사이트를 또 긁어야 하는가, 값이 틀렸을 때 누구 잘못인가.

- collect : 사이트가 준 것만 S3 CSV 와 manifest 행으로 남김. 좌표 보정 하나만 예외
- enrich : Kakao 를 불러 법정동 코드를 붙임. 값이 틀리면 Kakao 잘못. **이 문서의 범위**
- index : 우리 규칙으로 검색 카드를 만듦. Team-Neki-Server `apps/batch` 의 Spring Batch 잡(BACKEND-65). Python 이 아님

검색 API 가 부스에서 쓰는 값은 둘뿐입니다. 법정동 코드 10자리와 1km 안 역. 역은 좌표만으로 index 가 PostGIS 로 계산하므로, enrich 가 만드는 값은 법정동 코드 하나입니다.

---

## 2. collect 와 왜 나누고 어떻게 순서를 맞추는가?

`stores_collect` 끝에서 enrich 를 부르지 않습니다. 한 flow 안에 있으면 enrich 가 죽었을 때 run 을 재시도하는 순간 사이트 11곳을 다시 긁습니다. 나눠 두면 enrich 재시도는 S3 만 다시 읽습니다.

순서는 시각으로 맞춥니다. collect 04:00 KST, enrich 05:00 KST 이고 enrich 는 그 시점까지 적재된 것만 봅니다.

- 무엇을 읽을지는 collect 의 읽기 계약 그대로 `manifest.read_cycle(target_date)` 가 정함. 브랜드마다 대상 일자 이하의 최신 manifest 행 하나
- 대상 일자는 enrich run 의 예약 시각을 KST 로 끊은 것. 05:00 run 이면 그날
- collect 가 늦게 집혀 06:00 에 끝나면 그날 enrich 는 전날 행을 `stale` 로 씀. 결과의 `source_dt` 가 그것을 드러냄
- 7일 넘게 낡은 브랜드는 `read_cycle` 이 버림. best effort 가 고장을 감추면 안 되기 때문

collect 가 언제까지 끝나야 하는지(SLO)는 아직 정하지 않았습니다. 정해지면 이 cron 을 그 뒤에 두면 되고, `source_dt` 가 오늘이 아닌 브랜드 수가 곧 SLO 위반 지표입니다.

---

## 3. 입력

`tb_store_collect_manifest` 가 가리키는 `collect/platform=<브랜드>/dt=<사이클>/<실행 시각>.csv` 입니다. 브랜드 11개, 1,653건(2026-09-25 기준)입니다.

- `read_cycle` 결과가 `failed` 인 브랜드는 경고를 남기고 뺌. 있는 것만 처리
- 파일은 `storage.read_stores(platform, target_date)` 로 읽음. manifest 의 `store_count` 와 실제 건수가 다르면 예외 (읽기 계약)
- `(platform, idx)` 중복은 첫 것만 남기고 경고. 결과 테이블의 PK 라 둘 수 없음
- `collected_at` 은 CSV 에 시간대 붙은 ISO 문자열. 앱 DB 규약대로 KST 벽시계로 바꾸고 시간대를 뗌

---

## 4. 법정동 판정

### 1차 : coord2regioncode

부스당 한 번 `GET /v2/local/geo/coord2regioncode.json?x=<경도>&y=<위도>` 를 부릅니다. 응답에 법정동(`region_type = B`)과 행정동(`H`) 문서가 같이 오는데 `B` 만 씁니다. 거기서 `code`(법정동 코드 10자리), `region_1depth_name`, `region_2depth_name`, `region_3depth_name` 을 받습니다. 바다처럼 행정구역이 없으면 문서가 비어 `failed` 입니다.

### 재사용 : 좌표가 안 바뀌면 Kakao 를 부르지 않는다

직전 세대(현재 `tb_photo_booth_enriched`)를 `(platform, idx)` 로 읽어 두고, 좌표가 같고 직전에 `b_code` 가 있으면 그 답을 그대로 씁니다 (`reused`). 직전이 `failed` 였으면 좌표가 같아도 다시 묻습니다. 그날 Kakao 가 죽어서 비었을 수 있기 때문입니다.

좌표 비교는 float 동등 비교입니다. collect 가 `str(float)` 로 쓰고 `float()` 로 읽으며 Postgres `DOUBLE PRECISION` 도 Python float 을 그대로 돌려주므로 왕복이 정확합니다.

**이 재사용이 절약이자 안전장치입니다.** 매일 1,653회가 변경분 수십 회로 줄고, Kakao 가 죽은 날에도 좌표가 안 바뀐 지점은 어제 답으로 채워져 피해가 변경분에 갇힙니다. 하루 한도는 10만 건이라 첫 실행의 1,653회도 문제가 없습니다.

### 폴백 : 좌표가 없는 지점

collect 가 좌표 보정을 이미 하므로 거의 오지 않습니다. 오는 경우는 수집 때 Kakao 가 죽어 있었거나 키가 없었을 때입니다. collect 의 `geocode.locate`(주소검색 뒤 키워드검색)를 그대로 재사용해 좌표를 얻고 1차 경로로 보냅니다. 얻은 좌표는 결과 행에 `coordinate_source = kakao` 로 넣습니다. index 가 역 매핑에 좌표를 쓰기 때문입니다. 못 얻으면 `no_coordinate` 입니다.

티켓 원안의 "상호명·층수 접미를 뗀 주소로 질의" 는 하지 않습니다. 주소를 해석하는 규칙이 되고(5절), collect 의 보정이 이미 원문 그대로 질의해서 채우고 있습니다.

### 상태

| `geocode_status` | 뜻 |
|---|---|
| `ok` | Kakao 응답으로 채움 |
| `reused` | 좌표가 직전 세대와 같아 이전 답 재사용 |
| `no_coordinate` | 좌표가 없고 폴백도 실패 |
| `failed` | 좌표는 있으나 Kakao 실패 (조회 예외, 빈 응답, 연속 실패로 포기, 키 없음) |

### 동시성과 포기

쓰레드 4개로 보냅니다. QPS 가 미공개라 낮게 잡고, 재사용이 대부분을 걸러 이 값이 실행 시간을 좌우하지 않습니다. 조회 하나는 3번까지 다시 해보고(2초, 4초 간격), 연속 3번 실패하면 Kakao 가 죽은 것으로 보고 남은 지점은 묻지 않습니다. 규칙과 상수(`LOOKUP_ATTEMPTS`, `LOOKUP_TIMEOUT`, `GIVE_UP_AFTER`)는 collect 의 `geocode.py` 것을 그대로 씁니다.

task 는 하나입니다. 지점마다 task 를 만들면 1,653개의 task run 이 Prefect API 를 누르고 UI 에서 flow run 이 묻힙니다. task 재시도도 붙이지 않습니다. 지점마다 이미 다시 해보고, task 를 통째로 다시 돌리면 호출을 처음부터 되풀이합니다. 쓰레드 안에서는 로그를 남기지 않습니다. Prefect 의 run 컨텍스트가 쓰레드를 따라가지 않으므로 결과와 예외를 모아 task 본문에서 남깁니다.

### 지오코딩 실패는 flow 실패가 아니다

`KAKAO_API_KEY` 가 없거나 Kakao 가 죽어도 flow 는 완주합니다. 그 지점만 `b_code` 를 비우고 넘어갑니다. index 는 `b_code` 가 NULL 인 행도 카드로 만들되 `region_ids` 만 비웁니다.

---

## 5. 주소를 해석하지 않는 이유

주소 문자열을 시도, 시군구, 읍면동 컬럼으로 나누거나 "서울" 과 "서울특별시" 를 맞추는 일은 enrich 에서 하지 않습니다.

- 계층은 이미 코드 안에 있음. 법정동 코드 10자리가 시도 2 + 시군구 3 + 읍면동 3 + 리 2 라 잘라 쓰면 되고, index 의 `region_ids[]` 가 정확히 그렇게 만듦. 컬럼으로 또 나누면 같은 정보를 두 곳에 두고 어긋날 자리만 생김
- 이름의 정본은 `tb_legal_dong`. 표기 차이는 이름을 맞춰서가 아니라 코드로 조인해서 피함. Kakao 도 API 마다 표기가 다름 (`coord2regioncode` 는 "서울특별시", `search/address` 는 "서울"). 우리가 이름 규칙을 또 만들면 `legal_dong/normalize.py`(일반구 분리, 세종 처리)와 두 곳이 됨
- 사이트 주소는 collect 계약상 원문. 그것을 쪼개 시군구를 뽑는 것은 사이트 11곳의 표기를 파싱하는 일이고 좌표를 Kakao 에 넣어 받는 답보다 못함
- 뒤 단계도 주소를 키로 쓰지 않음. index 는 `b_code` 와 좌표로 `region_ids` 와 `station_ids` 를 만들고 검색 API 는 그 둘만 씀. 주소는 카드의 표시 문자열

표시용 주소 정리(도로명과 지번 통일)는 이번 범위 밖입니다. 필요해지면 부스당 `coord2address` 한 번으로 정본 형태를 받는 별도 티켓이 맞습니다.

결과 테이블의 `region_1depth_name`, `region_2depth_name`, `region_3depth_name` 은 Kakao 가 준 문자열 그대로이고 운영 확인용입니다. 검색 키가 아니며 index 계약(8절)에도 들어가지 않습니다.

---

## 6. 경고

flow 를 막지 않고 로그만 남기는 것 둘입니다.

- **시군구 불일치** : `geocode_status = ok` 인 행에서 `region_2depth_name` 의 첫 토큰이 원문 `address` 에 없으면 경고. 사이트 좌표가 옆 건물이나 옆 동네를 찍은 경우를 드러냄. 특례시 일반구("수원시 영통구")는 첫 토큰인 시 이름으로 비교. `reused` 행은 처음 판정될 때 이미 경고했으므로 매일 되풀이하지 않음
- **마스터에 없는 코드** : 스왑 트랜잭션 안에서 `tb_legal_dong` 에 없는 `b_code` 건수를 셈. 개편 뒤 마스터가 낡았거나 Kakao 가 새 코드를 먼저 쓰는 경우이고, 그 부스는 검색에서 조용히 빠지므로 여기서 드러냄. `tb_legal_dong` 이 없으면 대조하지 못했다고 경고

그리고 `geocode_status` 별 건수와 `failed`, `no_coordinate` 목록(브랜드, 이름, 주소)을 로그에 남깁니다. 완료 조건입니다.

---

## 7. 산출물 : S3

```text
s3://<bucket>/
  enrich/
    dt=<사이클>/
      <실행 시각>.csv                       e.g. 2026-09-25_050112.csv
```

- 브랜드 11개가 한 파티션. `platform=` 이 없음
- `dt=` 는 사이클(대상 일자), 파일명은 실행 시각 `YYYY-MM-DD_HHMMSS`(KST). collect 와 같은 규칙이라 티켓의 `<HHMMSS>` 표기 대신 이것을 씀
- 열은 Postgres 테이블(8절)과 같고 순서도 같음. `EnrichedStore` dataclass 의 필드 순서가 정본
- manifest 행을 남기지 않음. index 는 S3 가 아니라 Postgres 의 현재 세대를 읽고, S3 는 이력과 재실행 원천. 파티션 안에서 파일명이 시각이라 가장 나중 파일이 곧 그 사이클의 마지막 실행
- 압축과 Content-Type 은 collect 와 같음 (압축 없음, `text/csv; charset=utf-8`)

---

## 8. 산출물 : Postgres `tb_photo_booth_enriched`

index 가 읽는 현재 세대입니다. BACKEND-114 의 `tb_temp_photo_booth` 와는 별도 테이블입니다. enrich 는 S3 를 읽으므로 114 를 기다리지 않고, index 는 "현재 세대 이름" 하나만 압니다.

```sql
CREATE TABLE tb_photo_booth_enriched (
    -- collect 가 준 것 (manifest 가 가리키는 CSV 그대로)
    platform            VARCHAR(32)      NOT NULL,
    idx                 VARCHAR(64)      NOT NULL,
    name                VARCHAR(255)     NOT NULL,
    address             VARCHAR(255),
    phone               VARCHAR(32),
    longitude           DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    coordinate_source   VARCHAR(16),                -- official / kakao / NULL
    collected_at        TIMESTAMP        NOT NULL,  -- KST 벽시계, 시간대 없음
    source_dt           DATE             NOT NULL,  -- 어느 수집 사이클에서 왔나

    -- enrich 가 더하는 것
    b_code              CHAR(10),                   -- 실패하면 NULL
    region_1depth_name  VARCHAR(32),
    region_2depth_name  VARCHAR(32),
    region_3depth_name  VARCHAR(32),
    geocode_status      VARCHAR(16)      NOT NULL,  -- ok / reused / no_coordinate / failed
    enriched_at         TIMESTAMP        NOT NULL,  -- KST 벽시계, 시간대 없음

    PRIMARY KEY (platform, idx)
);
CREATE INDEX ON tb_photo_booth_enriched_<YYYYMMDD> (b_code);
```

### 세대 교체는 legal_dong 과 같다

날짜 붙인 테이블 `tb_photo_booth_enriched_<사이클>` 을 COPY 로 채우고 인덱스를 건 뒤 이름을 맞바꾸며, 직전 세대는 `_prev` 로 남깁니다. 한 트랜잭션, `lock_timeout 5s` 를 첫 문장으로, `_prev` 는 맨 앞에서 치움, 인덱스 이름은 날짜 붙은 채로 둠. 이유는 `flows/legal_dong/table.py` 의 docstring 과 같습니다. 이름에 붙는 날짜는 오늘이 아니라 사이클입니다. 어느 수집분으로 만든 세대인지 이름이 말합니다.

직전 세대 읽기(재사용용)는 스왑과 같은 트랜잭션일 필요가 없습니다. 읽은 뒤 누가 스왑해도 답이 낡을 뿐 틀리지는 않습니다.

### 하한 800

결과가 800건 미만이면 바꿔치우지 않고 예외를 냅니다. 지금 전체가 1,653건이라 대략 절반입니다.

- 막으려는 것은 파국. S3 를 잘못 읽었거나 CSV 파싱이 깨졌거나 브랜드 대부분이 7일 넘게 낡아 버려진 경우처럼 거의 빈 테이블이 검색을 통째로 비우는 상황
- 브랜드 하나가 빠지는 것은 막지 않음. 그것은 `read_cycle` 이 일부러 떨어뜨리는 동작이고, 그때 스왑을 멈추면 나머지 브랜드까지 갱신이 멈춤
- 직전 세대 대비 비율은 쓰지 않음. 첫 실행에 기준이 없고 천천히 줄어드는 것은 못 잡음
- 브랜드가 늘어 전체가 크게 바뀌면 그때 숫자를 올림

### index(BACKEND-65)와의 계약

`platform`, `idx`, `name`, `address`, `longitude`, `latitude`, `source_dt`, `b_code` 여덟 열입니다. 나머지는 운영 확인용이라 바꿔도 index 를 깨지 않습니다. `b_code` 가 NULL 인 행도 남깁니다.

---

## 9. 색인 Job

enrich 가 성공하면 `prefect-kubernetes` 의 `KubernetesJob` 으로 `ghcr.io/team-neki/neki-batch` 를 띄우고 완료를 기다립니다. BACKEND-128 의 one-shot 계약입니다.

```text
args  : --spring.batch.job.name=searchIndexJob  businessDate=<사이클>
env   : TZ=Asia/Seoul
        SPRING_PROFILES_ACTIVE  <- Secret prefect-workflow 의 같은 키
        JASYPT_PASSWORD         <- Secret prefect-workflow 의 같은 키
spec  : backoffLimit 0, restartPolicy Never, ttlSecondsAfterFinished 86400
name  : search-index-<실행 시각>   (prefect-kubernetes 가 metadata.name 으로 상태를 읽으므로 generateName 은 못 씀)
```

- 이미지는 flow run 파드 환경변수 `NEKI_BATCH_IMAGE`. GitOps `overlays/prefect/images.env` 를 ConfigMap `neki-images` 로 만들어 base job template 의 `envFrom` 에 걸고, 서버 레포의 deploy-batch 가 그 줄을 갱신함
- 환경변수가 없으면 경고만 남기고 건너뜀. 로컬이거나 GitOps 가 아직인 경우. 그래서 워크플로 PR 은 GitOps 없이 머지할 수 있음
- Job 이 실패하면 `wait_for_completion` 이 RuntimeError 를 올려 flow 가 실패함. 종료 코드가 flow 결과에 반영되는 것이 계약
- 완료된 Job 을 지우지 않음(`delete_after_completion=False`). 실패 파드의 로그가 원인이고 `ttlSecondsAfterFinished` 가 하루 뒤 치움. 성공한 파드의 로그는 대기 중에 읽어 flow 로그에 남김
- 타임아웃 1,800초. 색인은 초 단위지만 파드가 안 뜨는 경우(이미지 없음)에 무한정 기다리지 않게
- flow run 파드는 SA `prefect-worker` 로 뜸. Role 이 jobs 생성과 pods/log 조회를 이미 허용함. base job template 의 `service_account_name` 기본값으로 두어 deployment 마다 지정하지 않음
- Secret 을 통째로 `envFrom` 하지 않음. batch 파드에 Kakao 와 AWS 키까지 넘길 이유가 없음
- **BACKEND-65 가 `searchIndexJob` 을 넣기 전까지는 deployment 파라미터 `run_index=False` 로 꺼 둠.** 없는 잡 이름이면 batch 가 종료 코드 1 로 죽어 enrich 가 매일 실패하기 때문. 65 가 들어오면 그 줄을 지움

---

## 10. 스케줄, 재실행, 백필

- deployment `stores-enrich`, `Cron("0 5 * * *", timezone="Asia/Seoul")`
- 매월 1일 legal-dong(05:00 KST)과 겹침. enrich 는 `tb_legal_dong` 을 대조용으로 SELECT 만 하고 그쪽 스왑은 원자적이라 해가 없음
- 재실행 : 같은 deployment 를 UI 에서 다시 실행. 재사용 덕에 Kakao 호출이 거의 없음. 규칙만 고쳐 색인만 다시 돌릴 때는 65 가 정한 대로 Prefect UI 에서 Job 을 다시 만들거나 enrich 를 `run_index=True` 로 다시 돌림
- 백필 : `stores_enrich(target_date=<지난 날짜>)`. 그 날짜 이하의 최신 적재물을 읽고 파티션 `dt=` 와 staging 테이블 이름에 그 날짜가 붙음
- `persist=False` : S3 와 Postgres 에 쓰지 않고 색인도 안 띄움. 직전 세대도 안 읽으므로 전 지점을 Kakao 에 물음. 판정만 볼 때

---

## 11. 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| Postgres (manifest, 직전 세대, 스왑) | flow 실패. `DATABASE_URL` 이 없으면 시작 직후 RuntimeError |
| S3 (collect 읽기) | 그 브랜드 read_stores 예외로 flow 실패. 건수 불일치도 같음 |
| Kakao | 그 지점만 `failed`. 연속 3회 실패면 남은 지점은 묻지 않음. 키가 없으면 재사용만 함. flow 는 완주 |
| S3 (enrich 쓰기) | task 재시도 셋 뒤 flow 실패. 스왑 전이라 테이블은 그대로 |
| k8s API / batch 이미지 | 색인 단계에서 flow 실패. 테이블은 이미 새 세대. 다시 돌리면 재사용으로 Kakao 없이 스왑 뒤 색인만 다시 시도 |
| `NEKI_BATCH_IMAGE` 없음 | 경고 후 색인 건너뜀. flow 성공 |

`KAKAO_API_KEY` 의존은 enrich 로 모입니다. 수집은 키 없이도 돕니다 (좌표 보정만 예외, 티켓 결정). `docs/spec/collect-pipeline.md` 의 "enrich 가 아직 없어" 라는 근거 서술은 enrich 가 생긴 뒤에도 보정을 유지한다는 서술로 고칩니다.

---

## 12. 코드 구조

```text
flows/stores_enrich/
  __init__.py       flow 재노출
  flow.py           stores_enrich(target_date, persist, run_index, max_stale_days). 순서와 로그, 하한
  region.py         EnrichedStore, from_collect, reusable, resolve, mismatched, enrich_stores(@task)
  table.py          read_current, swap_table(@task). legal_dong/table.py 와 같은 모양
  index_job.py      manifest, run_search_index(@task)
flows/common/kakao.py       REGION_URL, coord2regioncode()
flows/common/storage.py     ENRICH_PREFIX, enrich_partition, put_enriched(@task)
deployments/stores_enrich.py
tests/test_stores_enrich.py 순수 판정(from_collect, reusable, resolve 재사용 경로, mismatched)만. Kakao 와 DB 없음
docs/spec/enrich-pipeline.md
```

- `EnrichedStore` 의 필드 순서가 CSV 열 순서이자 COPY 열 순서. DDL 도 같은 순서. 한쪽만 고치면 값이 엉뚱한 컬럼에 들어감
- `region.py` 와 `table.py` 는 서로를 부르지 않고 `flow.py` 가 순서를 정함. `table.py` 가 `region.EnrichedStore` 를 import 하는 것은 `legal_dong/table.py` 가 `normalize.LegalDong` 을 쓰는 것과 같음
- 스왑 코드는 `legal_dong/table.py` 를 복사함. 세 벌째이지만 DDL, 인덱스, 코멘트, 하한이 다 달라 공통화하면 콜백이 됨. 공통화는 이 티켓 밖
- 의존 추가 : `prefect-kubernetes==0.7.12` (베이스 이미지와 같은 버전으로 고정. 다르면 이미지 안의 것이 교체됨), dev 그룹에 `pytest`. Dockerfile 은 `--no-dev` 라 이미지에 안 들어감. `make check` 가 pytest 를 돌림

---

## 13. 저장소 바깥의 변경

### Team-Neki-GitOps `overlays/prefect` (후속 티켓, 같은 에픽)

- `images.env` : `NEKI_BATCH_IMAGE=ghcr.io/team-neki/neki-batch:<tag>` 한 줄
- `kustomization.yaml` : configMapGenerator `neki-images` (envs, disableNameSuffixHash)
- `worker-base-job-template.json` : 컨테이너 `envFrom` 에 `configMapRef neki-images`, `service_account_name` 기본값 `prefect-worker`
- `workflow-secret.example.yaml` : `JASYPT_PASSWORD`, `SPRING_PROFILES_ACTIVE` 키. 실제 Secret 은 노드에서 patch
- base job template 은 worker initContainer 가 기동 때만 pool 에 밀어넣으므로 머지 뒤 worker 를 롤링해야 함

### Team-Neki-Server

- BACKEND-128 (apps/batch, deploy-batch) 머지. images.env 줄이 있어야 deploy-batch 가 돎
- BACKEND-65 (searchIndexJob). 들어오면 enrich deployment 의 `run_index=False` 를 지움

순서는 워크플로 PR → GitOps PR 과 128 → 65 입니다. 워크플로 PR 은 앞의 둘 없이도 staging 에서 적재까지 검증됩니다.

---

## 14. 검증

- `make check` : 임포트, deployment 수집, spec anchor, pytest
- 로컬 : LocalStack 과 Postgres 를 띄우고 `make collect` 뒤 `make enrich` 를 두 번. 첫 실행은 전 지점 `ok`, 둘째는 전 지점 `reused` 이고 Kakao 호출이 0에 가까워야 함. `enrich/dt=` 에 CSV 가 둘, `tb_photo_booth_enriched` 와 `_prev` 가 같은 건수, 인덱스 이름에 번호가 안 붙어야 함
- 테이블이 없는 상태부터 확인 (첫 실행은 바꿔칠 대상이 없어 경로가 다름)
- `KAKAO_API_KEY` 를 비우고 돌려 전 지점 `reused` 로 완주하는지
- `persist=False` 로 판정만 도는지
- staging : 브랜치를 build.yml `ref` 로 올려 05:00 예약 run 또는 수동 run. 노드에서 psql 로 `geocode_status` 분포와 `source_dt` 확인. 색인 단계는 `NEKI_BATCH_IMAGE` 가 없어 건너뛰는 경고가 남아야 함. 확인 뒤 `ref` 를 비워 main 으로 되돌림

---

## 15. 열어 둔 것

- SLO. collect 가 몇 시까지 끝나야 하는지. 정해지면 cron 을 그 뒤로
- 하한 800 은 브랜드 수가 크게 바뀌면 다시 봄
- 114 의 `tb_temp_photo_booth` 를 이 테이블로 흡수할지. 어느 쪽이든 index 는 현재 세대 이름 하나만 알면 됨
- 표시용 주소 정리 (`coord2address`). 별도 티켓

---

## 정리

enrich 는 collect 와 별도 flow 로 05:00 KST 에 돌며, 그 시점까지 manifest 가 가리키는 브랜드별 최신 CSV 를 읽어 좌표를 법정동 코드로 바꿉니다. 좌표가 안 바뀐 지점은 직전 세대의 답을 재사용해 Kakao 호출을 변경분으로 줄이고, Kakao 가 죽어도 완주합니다. 결과는 S3 파티션 하나와 Postgres 세대 하나로 남기고, 800건 미만이면 바꿔치우지 않습니다. 주소 문자열은 해석하지 않으며 계층은 코드에, 이름은 `tb_legal_dong` 에 있습니다. 끝나면 서버 batch 의 색인 잡을 k8s Job 으로 띄우고 그 종료 코드가 flow 결과가 됩니다.
