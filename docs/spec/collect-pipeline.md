# 지점 수집 파이프라인 정책

이 문서는 포토부스 지점 수집 파이프라인의 정책을 다룹니다. 무엇을 어디에 어떤
모양으로 남기고, 실패했을 때 무엇으로 대신하며, 다음 단계가 무엇을 읽어야 하는지를
정합니다.

**이 문서가 정본입니다.** 코드와 이 문서가 어긋나면 문서가 맞고 코드가 틀린
것입니다. 동작을 바꾸려면 이 문서를 먼저 고치고 같은 PR 에서 코드를 맞춥니다.
항목마다 `구현` 줄이 구현 위치를 가리키며, 형식은 `` `경로:줄` `심볼` `` 입니다.
`make spec-check` 가 그 줄에 그 심볼이 있는지 확인하므로 줄이 밀리면 CI 가
막습니다. 그때는 줄 번호를 고치면 되고, 심볼이 사라졌다면 정책이 바뀐 것이니 이
문서를 같이 고쳐야 합니다.

## 범위

다루는 것은 지점 수집(collect)입니다. 브랜드 11개의 지점 목록을 받아 S3 와
Postgres 에 남기는 데까지입니다. 뒤 단계인 enrich 와 index 는 아직 없으며, 이
문서는 그 단계들이 지켜야 할 읽기 계약만 정합니다.

다루지 않는 것은 법정동 코드와 지하철 역 마스터입니다. 별도 flow 이고 S3 를 거치지
않으며, 규약은 `AGENTS.md` 에 있습니다.

## 세 단계와 경계

파이프라인은 collect, enrich, index 세 단계입니다. 어느 단계에 코드를 둘지는 두
질문으로 가릅니다.

- 다시 돌리려면 사이트를 또 긁어야 하나. 아니면 collect 바깥임
- 값이 틀렸을 때 누구 잘못인가. 사이트면 collect, Kakao 면 enrich, 우리 규칙이면
  index

**collect 는 사이트가 준 것만 담고 해석하지 않습니다.** 주소가 도로명인지 지번인지
판정하지 않고 상호명이나 층수를 떼지 않습니다. 수집원이 Kakao 인 브랜드도 받아온
값을 해석하지 않습니다. 모든 브랜드가 한 스키마를 쓰고, 사이트가 주지 않는 필드는
`None` 입니다.

구현 : `flows/common/store.py:29` `class CollectedStore`

### 좌표 보정만 collect 안에서 합니다

이 경계의 유일한 예외입니다. 좌표가 빈 지점은 수집 flow 안에서 Kakao 주소검색으로
채웁니다. 좌표가 없으면 그 지점이 색인에서 통째로 빠지는데 enrich 가 아직 없어
결측이 방치되기 때문입니다. 예외를 유지하는 조건은 셋이고 하나라도 깨지면 예외를
거둡니다.

- Kakao 장애가 수집 실패가 되지 않음. 키가 없으면 건너뛰고, 연속 세 번 실패하면
  남은 지점은 조회하지 않음
- 사이트가 준 좌표와 우리가 채운 좌표를 `coordinate_source` 로 구분함. `official`,
  `kakao`, `None`(못 얻음)
- flow 의 `geocode` 파라미터로 끌 수 있음

주소는 원문 그대로 질의에 넣고, 0건이면 `<주소 앞 2토큰> <상호명>` 으로 한 번 더
묻습니다. 키워드검색을 먼저 쓰지 않는 이유는 전국의 동명 가게를 집을 수 있어서입니다.

구현 : `flows/common/geocode.py:130` `def fill_coordinates`,
`flows/common/geocode.py:43` `GIVE_UP_AFTER`,
`flows/common/geocode.py:30` `REGION_TOKENS`,
`flows/common/store.py:47` `coordinate_source`

## 대상 브랜드와 수집원

| 브랜드 | 수집원 | 좌표 |
|---|---|---|
| 인생네컷, 포토이즘, 돈룩업 | imweb 지도 위젯 AJAX (공용 모듈 하나) | 보정 |
| 포토시그니처, 플랜비스튜디오 | 사이트 HTML | 보정 |
| 픽닷, 모노맨션, 포토그레이, 하루필름, 포토랩플러스, 비룸스튜디오 | Kakao 장소검색 | 항상 옴 |

브랜드를 더하면 `BRANDS` 에도 등록해야 합니다. 빠뜨리면 단독 실행은 되는데 정기
수집에서만 조용히 빠집니다.

imweb 위젯은 범위를 벗어난 페이지가 빈 응답이 아니라 마지막 페이지로 고정되므로
직전 페이지와 항목 id 집합을 비교해 끝을 판정합니다. Kakao 장소검색은 한 질의에
45건까지만 꺼낼 수 있어 좌표 사각형을 넷으로 나눠 재귀하고, 수집 뒤 `total_count`
와 건수를 대조합니다.

구현 : `flows/stores_collect/flow.py:30` `BRANDS`,
`flows/common/imweb_map.py:180` `def collect_board`,
`flows/common/imweb_map.py:32` `MAX_PAGES`,
`flows/common/kakao.py:143` `def search_all`,
`flows/common/kakao.py:29` `MAX_EXPOSED`

## 산출물 : S3

```text
s3://<bucket>/
  collect/
    platform=<브랜드>/
      dt=<대상 일자>/
        <실행 시각>.csv                       e.g. 2026-09-25_042336.csv
        _raw/<실행 시각>/<이름>               e.g. _raw/2026-09-25_042336/page-001.html
```

- prefix 는 `collect/` 하나. `raw/`, `runs/`, `_manifest.json` 은 없음
- 브랜드가 위. 브랜드 폴더를 열면 `dt=` 가 이력이고, 실패한 날은 폴더가 없어
  마지막 폴더가 곧 대신 쓰이는 것임
- `dt=` 는 적재일이 아니라 대상 일자(`target_date`)임. 파티션이 사이클이어야 사람도
  Athena 도 사이클로 찾음
- 실행 시각은 `YYYY-MM-DD_HHMMSS`(KST). CSV 와 raw 가 같은 값을 써서 짝이 맞음.
  같은 날 재실행하면 둘 다 하나 더 생기고 이전 것은 남음
- 원문(raw)은 CSV 와 같은 파티션의 `_raw/` 아래. `_` 로 시작해 Hive 와
  Trino(Athena) 가 무시함. 이름을 바꾸면 그 성질이 사라짐
- 압축하지 않음. collect 는 하루 수백 KB, raw 는 압축을 풀어도 5MB 안팎이라 줄여서
  얻는 것이 없고 콘솔에서 바로 열리는 이점이 사라짐
- Content-Type 을 확장자로 정해 붙임. `text/csv`, `text/html`, `application/json`
- raw 객체에 태그 `kind=raw`. lifecycle 은 prefix 나 태그로만 걸리는데 raw 가
  파티션 안에 있어 prefix 로는 못 잡음. 코드는 지우지 않음

구현 : `flows/common/storage.py:67` `COLLECT_PREFIX`,
`flows/common/storage.py:71` `RAW_DIR`,
`flows/common/storage.py:73` `RUN_AT_FORMAT`,
`flows/common/storage.py:112` `def partition`,
`flows/common/storage.py:162` `def put_stores`,
`flows/common/storage.py:231` `def put_raw`,
`flows/common/storage.py:130` `def _content_type`,
`flows/common/storage.py:261` `Tagging="kind=raw"`

### CSV 계약

헤더가 있는 CSV 이고 열 순서는 `COLUMNS` 가 정본입니다. `CollectedStore` 에 필드를
더하면 여기에도 넣어야 하며, 빠뜨리면 `DictWriter` 가 `ValueError` 로 막습니다.
`collected_at` 은 `CollectedStore` 에 없고 적재 시점에 붙습니다. 우리가 언제 받았는지이지
사이트가 준 값이 아니기 때문입니다.

CSV 에는 타입도 null 도 없습니다. 수집 단계는 값이 없을 때 빈 문자열이 아니라 `None`
을 넣고, 읽는 쪽이 빈 칸을 `None` 으로, 좌표를 `float` 으로 되돌립니다. 줄 끝은
`\n` 입니다. 기본값인 CRLF 로 두면 Postgres `COPY` 가 마지막 열에 `\r` 을 붙여
읽습니다.

구현 : `flows/common/storage.py:77` `COLUMNS`,
`flows/common/storage.py:90` `FLOAT_COLUMNS`,
`flows/common/storage.py:148` `def _record`,
`flows/common/storage.py:266` `def _restore`

## 산출물 : manifest 테이블

어느 CSV 가 현재인지는 S3 가 아니라 Postgres 가 압니다. 적재 한 번이 한 행입니다.

```text
tb_store_collect_manifest
  id            BIGINT identity PK     적재 일련번호. 같은 (platform, target_date) 안에서 큰 값이 최신
  platform      VARCHAR(30)            브랜드
  target_date   DATE                   대상 일자 (KST)
  s3_path       TEXT                   s3://<버킷>/<키> 전체. 행만 보고 파일에 닿아야 함
  store_count   INTEGER                CSV 레코드 수. 읽는 쪽이 대조
  collected_at  TIMESTAMP              적재 시각. 시간대 없는 컬럼에 KST 벽시계
  flow_run_id   UUID                   적재한 Prefect flow run
```

- 실행할 때마다 행을 새로 쌓고 덮어쓰지 않음. `(platform, target_date)` 는
  유일하지 않아 키가 못 되고 PK 는 연번 `id`
- 읽는 쪽은 `id` 내림차순으로 최신 행을 집음. `collected_at` 으로 정렬하면 같은
  초에 두 번 적재됐을 때 순서가 갈리지 않음
- 인덱스 둘. `(target_date, platform, id DESC)` 는 사이클 하나의 정확한 행을 집는
  길이고 `(platform, target_date DESC, id DESC)` 는 브랜드마다 사이클 이하의 최신을
  찾는 길
- `collected_at` 은 앱 DB(Team-Neki-Server)의 다른 테이블처럼 시간대 없는
  `TIMESTAMP` 에 KST 벽시계. `TIMESTAMPTZ` 면 세션 시간대(운영은 UTC)로 보여 앱
  테이블과 9시간 어긋남. aware 값을 그대로 넣어도 세션 시간대로 바뀌므로 넣기 전에
  시간대를 뗌
- 테이블은 첫 실행이 `CREATE TABLE IF NOT EXISTS` 로 만듦. 브랜드 11개가 스레드로
  겹쳐 도는 첫 실행에서 동시에 만들려다 `pg_type` 유니크 위반이 나므로 권고 락을
  먼저 잡음. 컬럼을 바꾸면 기존 테이블을 고치지 않으므로 `DROP TABLE` 뒤 다시 돌림
- 접속은 `DATABASE_URL` 하나. Prefect Block 을 쓰지 않음

구현 : `flows/common/manifest.py:61` `TABLE`,
`flows/common/manifest.py:88` `_DDL`,
`flows/common/manifest.py:118` `_INDEXES`,
`flows/common/manifest.py:74` `_DDL_LOCK_KEY`,
`flows/common/manifest.py:167` `def ensure_table`,
`flows/common/manifest.py:182` `def put_manifest`,
`flows/common/postgres.py:22` `def dsn`

### 본문과 행의 순서

DB 에 닿는지는 본문을 올리기 전에 확인하고, 행은 본문을 올린 뒤에 씁니다.

- 올리기 전에 `ensure_table`. 적재 task 는 재시도가 셋이라 DB 가 죽어 있는데 본문부터
  올리면 시도마다 가리킬 행 없는 CSV 가 남음. 재시도는 같은 실행 시각을 다시 쓰므로
  이름이 같아 파일도 늘지 않음
- 그래도 창은 남음. 본문을 올린 직후 DB 가 끊기면 그 실행의 CSV 하나가 행 없이
  남을 수 있음. 어느 행도 가리키지 않는 파일은 읽는 쪽이 집지 않으므로 데이터가
  섞이지는 않음. 완전히 없애려면 트랜잭션 밖의 S3 를 되돌려야 하는데 그 비용이
  고아 파일 하나보다 큼
- 행은 본문 뒤에. 순서가 뒤집히면 행만 있고 데이터가 없는 창이 생겨 다음 단계가
  없는 파일을 읽으러 감
- `DATABASE_URL` 이 없으면 수집 flow 는 시작 직후 실패함. `persist=False` 로 끄면 DB
  없이 파싱만 볼 수 있음

구현 : `flows/common/storage.py:162` `def put_stores`,
`flows/common/postgres.py:39` `def connect`

## 대상 일자와 실행 시각

`target_date` 는 이번 적재물이 어느 수집 사이클의 것인지이고, 실행 시각은 실제로
언제 돌았는지입니다. 둘은 보통 같은 날이지만 갈릴 때가 있습니다.

```text
월요일 04:00 예약 run 이 worker 부재로 쌓였다가 수요일에 집힘
    target_date   2026-09-21 (월)      -> dt=2026-09-21 파티션
    실행 시각      2026-09-23_154002    -> 파일명

수요일에 손으로 실행
    target_date   2026-09-23
    실행 시각      2026-09-23_154002
```

- `target_date` 는 감싼 flow run 의 `target_date` 파라미터가 있으면 그 값, 없으면
  flow run 의 예약 시각(`scheduled_start_time`)을 KST 로 끊은 것. cron 을 역산하지
  않음. `flows/` 가 스케줄을 알면 안 되기 때문
- `stores_collect` 가 값을 한 번 정해 브랜드 flow 의 `target_date` 파라미터로
  내려보냄. 브랜드를 스레드로 부르면 Prefect 컨텍스트가 따라가지 않아 브랜드 run
  이 독립 run 으로 뜨고, 그 안에서 읽으면 자기 시작 시각이 나오기 때문
- 원문을 남기는 `put_raw` 는 수집기 깊숙이서 불려 인자로 받을 길이 없으므로 감싼
  브랜드 run 의 파라미터를 컨텍스트에서 읽음. 그래서 raw 가 CSV 와 같은 파티션에
  들어감
- 실행 시각은 브랜드 flow run 의 시작 시각(KST). CSV 와 raw 가 같은 값을 씀. 재시도도
  같은 값
- 백필은 `stores_collect(target_date=...)` 또는 브랜드 flow 의 `target_date` 로 함

구현 : `flows/common/manifest.py:146` `def target_date`,
`flows/stores_collect/flow.py:138` `cycle = target_date()`,
`flows/stores_collect/flow.py:144` `target_date=cycle`,
`flows/photoism_stores/flow.py:28` `target_date: date | None = None`,
`flows/common/storage.py:117` `def run_at`

## 실패한 브랜드는 무엇으로 대신하나

한 브랜드가 실패해도 나머지는 적재합니다. 실패한 브랜드는 이전 사이클의 적재물로
대신하되, **이전 데이터를 이번 사이클로 복사하지 않습니다.** 수집한 적 없는 것이
이번 것처럼 보이면 collect 계층이 거짓말을 하게 되고 며칠이 지나도 신선도를 알 수
없습니다.

무엇을 대신 읽을지는 기록하지 않고 읽는 시점에 계산합니다. 브랜드마다 대상 일자
이하의 가장 최근 행을 집고 며칠 지났는지로 상태를 매깁니다.

- `ok` : 이 사이클에 적재됨. 이번 시도가 실패했어도 이 사이클 행이 이미 있으면 여기
- `stale` : 이전 사이클로 대신함. `age_days` 가 며칠 전인지
- `failed` : `MAX_STALE_DAYS`(7일) 안에 쓸 것이 없음. 버린 행의 `stale_target_date`
  와 `age_days` 는 남겨 왜 버렸는지 드러냄

무한정 대신하지 않는 이유는 파서가 깨진 채로 몇 주가 지나도 아무도 눈치채지
못하기 때문입니다. best effort 가 고장을 감추는 장치가 되면 안 됩니다.

같은 함수를 `stores_collect` 와 enrich 가 씁니다. 규칙이 한 곳이어야 수집이 정한
것과 다음 단계가 보는 것이 어긋나지 않습니다. 실행 요약을 S3 에 남기지 않는 이유가
여기 있습니다. 실패 사유는 Prefect 로그에 있고, 무엇을 읽을지는 테이블에서 나오며,
아침에 실패한 브랜드를 오후에 단독 재수집하면 계산이 새 행을 바로 집습니다.

`stores_collect` 는 `ok` 와 `stale` 이 하나도 없을 때만 실패합니다. 다음 단계로
넘길 것이 없기 때문입니다. `ok` 가 하나도 없으면(전부 `stale`) 개별 사이트 문제가
아니라 네트워크나 배포를 의심해야 하므로 에러 로그를 따로 남깁니다.

구현 : `flows/common/manifest.py:66` `MAX_STALE_DAYS`,
`flows/common/manifest.py:240` `def read_cycle`,
`flows/stores_collect/flow.py:44` `def _fill_from_previous`,
`flows/stores_collect/flow.py:171` `raise RuntimeError(`

## 병렬 실행과 격리

Prefect 3 에서 동기 서브플로우 호출은 순차라 `ThreadPoolExecutor` 로 감싸 겹쳐
돌립니다. 각 브랜드의 예외는 `future.result()` 자리에서 잡아 결과에 담고 넘어갑니다.
여기서 다시 던지면 나머지 브랜드의 결과까지 버리게 됩니다. 브랜드 11개는 별도
파드가 아니라 한 flow run 파드 안의 스레드입니다.

구현 : `flows/stores_collect/flow.py:142` `ThreadPoolExecutor`,
`flows/stores_collect/flow.py:151` `except Exception as error`

## 스케줄

정기 수집은 `stores-collect` deployment 하나이고 매일 04:00 KST 입니다. Prefect cron
은 시간대를 주지 않으면 UTC 라 반드시 명시합니다. 빠뜨리면 13:00 KST 한낮에
사이트를 긁습니다.

브랜드별 deployment 는 cron 없이 둡니다. 스케줄이 없어도 UI 에 남아 실행 버튼이
동작하므로 백필과 단일 브랜드 재수집에 씁니다. `paused=True` 가 아니라 스케줄을
아예 두지 않는 이유는 pause 는 배포가 덮어쓰는 문제를 계속 신경 써야 하기
때문입니다.

UI 에서 끈 스케줄은 `deploy.py` 가 배포 전에 읽어 두었다가 되돌립니다. 반대로 다시
켠 것을 배포가 끄지도 않습니다.

구현 : `deployments/stores_collect.py:26` `schedule=Cron(`,
`deployments/__init__.py:14` `def collect`,
`deploy.py:47` `async def read_states`,
`deploy.py:69` `async def restore_states`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| 수집 사이트 | 그 브랜드만 `failed`. 나머지는 적재. 대신하기 규칙 적용 |
| Kakao (수집원) | 그 브랜드만 `failed`. `KAKAO_API_KEY` 가 없으면 시작 직후 `RuntimeError` |
| Kakao (좌표 보정) | 보정만 건너뜀. 수집은 끝까지 감 |
| Postgres | 수집 flow 실패. 올리기 전 확인(`ensure_table`)에서 막히면 S3 에 아무것도 남지 않음. 본문을 올린 뒤 행 기록에서 막히면 가리킬 행 없는 CSV 가 브랜드당 하나 남을 수 있음. 재시도는 같은 이름을 덮어써 더 늘지 않고, 그 파일은 어느 행도 가리키지 않으므로 읽는 쪽이 집지 않음 |
| S3 | 적재 task 실패 (재시도 셋). `S3_BUCKET` 이 없으면 시작 직후 `RuntimeError` |

자격증명은 환경변수뿐입니다. 로컬은 `.env` 와 `aws/config` 의 프로파일, 운영은
GitOps 의 k8s Secret `prefect-workflow` 가 flow run Job 파드에 넣습니다. worker
파드가 아닙니다. 코드에 endpoint 나 프로파일 분기를 두지 않습니다. 로컬과 운영의
차이가 환경변수 하나여야 코드에 분기가 생기지 않습니다.

구현 : `flows/common/kakao.py:41` `def api_key`,
`flows/common/storage.py:97` `def _bucket`,
`flows/common/storage.py:107` `def _client`

## 읽는 쪽 계약 (enrich, index)

enrich 와 index 는 아직 없습니다. 만들 때 지킬 계약은 셋입니다.

- 무엇을 읽을지는 `read_cycle(target_date)` 가 정함. 브랜드마다 `status` 와
  manifest 행을 돌려주고, `failed` 는 건너뜀
- 파일은 `read_stores(platform, target_date)` 로 읽음. manifest 행의 `s3_path` 하나만
  따라가고 파티션의 다른 CSV 는 이력일 뿐 현재가 아님. `store_count` 와 실제
  레코드 수가 다르면 예외
- S3 파티션을 나열해 최신을 찾지 않음. 파티션이 있어도 행이 없을 수 있어 목록과
  기록이 어긋남

구현 : `flows/common/manifest.py:240` `def read_cycle`,
`flows/common/storage.py:279` `def read_stores`,
`flows/common/storage.py:136` `def _split_uri`

### Athena 를 붙이려면

지금 붙이지는 않습니다. 붙이는 날 `collect/` 를 테이블 위치로 잡고 `platform`(enum
11개)과 `dt`(date)를 partition projection 으로 정의하면 크롤러도 파티션 등록도
필요 없습니다. 같은 날 재실행으로 CSV 가 둘이면 파일명이 시각이라 정렬되므로 뷰
하나로 최신만 남깁니다.

```sql
SELECT * FROM (
  SELECT *, max("$path") OVER (PARTITION BY platform, dt) AS latest FROM collect
) WHERE "$path" = latest
```

`_raw/` 가 정말 무시되는지는 그날 `SELECT count(*)` 한 번으로 확인합니다. 어긋나면
`_raw/` 를 별도 prefix 로 빼는 것이라 되돌리기 쉽습니다.

## 운영

이미지 하나를 worker 와 flow run 이 같이 씁니다. main 에 머지되면 Actions 가
이미지를 `ghcr.io/team-neki/team-neki-workflow:<version>-<sha7>` 로 밀고 GitOps
`overlays/prefect/worker.yaml` 의 태그를 커밋합니다. ArgoCD 가 worker 를 롤링하고
initContainer 가 `deploy.py` 로 deployment 를 재등록하며, 다음 run 부터 새
이미지가 씁니다. 이미지에 구운 `WORKFLOW_IMAGE` 가 `job_variables.image` 로
들어가지 않으면 flow run 이 베이스 prefect 이미지로 떠서 `flows` 를 찾지 못합니다.

머지 전에 브랜치를 올려 보려면 Actions build 를 `ref` 에 브랜치명을 적어 실행합니다.
Prefect 환경이 하나라 그 동안 staging worker 전체가 그 브랜치 코드로 돌고, 04:00
예약 run 도 그 이미지로 돕니다. 팀원과 겹치면 서로 되돌리게 되므로 올리기 전에
알리고, 확인이 끝나면 `ref` 를 비우고 다시 실행해 main 으로 되돌립니다. 절차는
`docs/runbook.md` 에 있습니다.

레이아웃이나 테이블 모양이 바뀐 배포는 이전 산출물과 섞이면 안 됩니다. staging 은
버킷을 비우고 `tb_store_collect_manifest` 를 지운 뒤 한 번 돌려 확인합니다.

구현 : `.github/workflows/build.yml:22` `ref:`,
`.github/workflows/build.yml:39` `GITOPS_PATH:`,
`Dockerfile:27` `ENV WORKFLOW_IMAGE`,
`deploy.py:118` `WORKFLOW_IMAGE`

## 변경 검증

- `make check` : 임포트, deployment 수집, 이 문서의 anchor
- 적재를 건드렸다면 같은 flow 를 두 번 돌려 `collect/` 에 CSV 와 `_raw/<실행 시각>/`
  가 하나씩 늘고 행도 하나 느는지, 그 행의 `s3_path` 가 나중 파일인지 봄. CSV 와
  raw 폴더의 실행 시각이 같아야 하고 `.gz`, `raw/`, `runs/`, `_manifest.json` 이
  생기면 안 됨
- manifest 를 건드렸다면 테이블이 없는 상태부터. `make collect` 두 번에 브랜드마다
  행이 2건, 인덱스 이름에 번호가 붙지 않아야 함
- 대신하기를 건드렸다면 `read_cycle` 네 갈래(ok, stale, 너무 오래됨, 없음)를 과거
  사이클로 격리해 확인
- 실행 시각과 대상 일자를 건드렸다면 브랜드 flow 를 `target_date=어제` 로 돌려 CSV 와
  raw 가 어제 파티션에 들어가고 파일명은 오늘 시각인지 봄

## 정리

collect 는 사이트가 준 것만 S3 CSV 로 남기고 어디에 남겼는지를 Postgres 행으로
남깁니다. 파티션은 대상 일자, 파일명은 실행 시각이며 원문은 같은 파티션의 `_raw/`
아래에 있습니다. 실패한 브랜드는 읽는 시점에 7일 안의 최근 사이클로 대신하고, 그
규칙은 `read_cycle` 한 곳에만 있습니다. 이 문서와 코드가 어긋나면 문서를 기준으로
코드를 고칩니다.
