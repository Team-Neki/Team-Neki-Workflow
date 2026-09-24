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

구현 : `deployments/stores_enrich.py:27` `schedule=Cron(`,
`flows/stores_enrich/flow.py:45` `def _read_inputs`,
`flows/stores_enrich/region.py:79` `def from_collect`

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
- task 는 하나. 지점마다 task 를 만들지 않음. 쓰레드 안에서 로그를 남기지 않음

구현 : `flows/common/kakao.py:111` `def coord2regioncode`,
`flows/stores_enrich/region.py:112` `def reusable`,
`flows/stores_enrich/region.py:145` `def resolve`,
`flows/stores_enrich/region.py:223` `def enrich_stores`,
`flows/stores_enrich/region.py:39` `WORKERS`

## 주소는 해석하지 않는다

주소 문자열을 시도, 시군구 컬럼으로 나누거나 "서울" 과 "서울특별시" 를 맞추지
않습니다. 계층은 코드 10자리(시도2+시군구3+읍면동3+리2)에 있고 이름의 정본은
`tb_legal_dong` 이라 코드로 조인하면 됩니다. Kakao 도 API 마다 표기가 다릅니다.
결과의 `region_*depth_name` 은 Kakao 가 준 문자열 그대로이고 운영 확인용입니다.

경고 둘만 남깁니다. `ok` 행에서 `region_2depth_name` 첫 토큰이 원문 주소에
없으면 시군구 불일치, 스왑 트랜잭션 안에서 `tb_legal_dong` 에 없는 `b_code`
건수. 둘 다 flow 를 막지 않습니다. 시군구 불일치는 좌표가 틀린 경우 말고도
행정구역 개편 뒤 사이트 주소가 옛 이름인 경우에 뜹니다. 2026-07 인천 개편(중구,
서구가 제물포구, 영종구, 서해구, 검단구로) 뒤 인천 지점 20여 건이 그렇고, 이때
코드는 맞으므로 경고만 보고 넘어가면 됩니다. `reused` 행은 처음 판정될 때 이미
경고했으므로 매일 되풀이하지 않습니다.

구현 : `flows/stores_enrich/region.py:210` `def mismatched`,
`flows/stores_enrich/table.py:32` `LEGAL_DONG_TABLE`

## 산출물 : S3

```text
enrich/dt=<사이클>/<실행 시각>.csv        e.g. enrich/dt=2026-09-25/2026-09-25_050112.csv
```

- 브랜드 11개가 한 파티션. `platform=` 없음
- 열은 Postgres 테이블과 같고 순서는 `EnrichedStore` 필드 순서가 정본
- manifest 행 없음. index 는 Postgres 를 읽고 S3 는 이력과 재실행 원천. 파티션의
  가장 나중 파일이 그 사이클의 마지막 실행
- 압축하지 않고 `text/csv; charset=utf-8`. collect 와 같음

구현 : `flows/common/storage.py:71` `ENRICH_PREFIX`,
`flows/common/storage.py:318` `def put_enriched`,
`flows/stores_enrich/region.py:73` `COLUMNS = tuple(`

## 산출물 : Postgres `tb_photo_booth_enriched`

index 가 읽는 현재 세대입니다. 세대 교체는 `tb_legal_dong` 과 같은 바꿔치기입니다.
사이클 날짜를 붙인 테이블을 COPY 로 채우고 인덱스를 건 뒤 이름을 맞바꾸며 직전은
`_prev` 로 남깁니다. 한 트랜잭션, `lock_timeout 5s`, `_prev` 는 맨 앞에서 치움.
같은 날 다시 돌리면 인덱스 이름이 현재 세대와 부딪혀 `1` 이 붙었다 다음 실행에
돌아오며, `2` 이상으로 올라가면 `_prev` 정리가 빠진 것입니다.

- 하한 800 미만이면 바꿔치우지 않고 예외. 브랜드 하나가 빠지는 것은 막지 않고
  거의 빈 테이블만 막음
- index 와의 계약은 `platform`, `idx`, `name`, `address`, `longitude`, `latitude`,
  `source_dt`, `b_code` 여덟 열. `b_code` 가 NULL 인 행도 남김
- 시각 컬럼은 시간대 없는 `TIMESTAMP` 에 KST 벽시계
- BACKEND-114 의 `tb_temp_photo_booth` 와 별도 테이블. enrich 는 S3 를 읽으므로
  그쪽을 기다리지 않음

구현 : `flows/stores_enrich/table.py:28` `TABLE`,
`flows/stores_enrich/table.py:125` `def swap_table`,
`flows/stores_enrich/table.py:105` `def read_current`,
`flows/stores_enrich/flow.py:42` `MIN_EXPECTED`

## 색인 Job

성공하면 `NEKI_BATCH_IMAGE` 이미지를 `--spring.batch.job.name=searchIndexJob
businessDate=<사이클>` 인자로 k8s Job 에 띄우고 완료를 기다립니다. Job 실패는
flow 실패입니다. 환경변수가 없으면 경고 후 건너뜁니다.

- 이미지는 GitOps `overlays/prefect/images.env` (ConfigMap `neki-images`, BACKEND-143)
- Job 이름은 `search-index-<실행 시각>`. prefect-kubernetes 가 `metadata.name` 으로
  상태를 읽으므로 `generateName` 은 못 씀
- env 는 `TZ`, 그리고 Secret `prefect-workflow` 의 `SPRING_PROFILES_ACTIVE`,
  `JASYPT_PASSWORD` 둘만. Secret 을 통째로 넘기지 않음
- 완료된 Job 은 지우지 않고 `ttlSecondsAfterFinished` 로 하루 뒤 정리
- flow run 파드는 SA `prefect-worker`. base job template 기본값이라 deployment 는
  지정하지 않음
- BACKEND-65 가 `searchIndexJob` 을 넣기 전까지 deployment 파라미터 `run_index=False`

구현 : `flows/stores_enrich/index_job.py:29` `IMAGE_ENV`,
`flows/stores_enrich/index_job.py:48` `def manifest`,
`flows/stores_enrich/index_job.py:106` `def run_search_index`,
`deployments/stores_enrich.py:31` `parameters={"run_index": False}`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| Postgres | flow 실패. `DATABASE_URL` 이 없으면 시작 직후 `RuntimeError` |
| S3 (읽기) | 그 브랜드 예외로 flow 실패. 건수 불일치도 같음 |
| Kakao | 그 지점만 `failed`. 연속 3회면 남은 지점은 묻지 않음. flow 완주 |
| S3 (쓰기) | 재시도 셋 뒤 flow 실패. 스왑 전이라 테이블 그대로 |
| k8s / batch 이미지 | 색인 단계에서 flow 실패. 테이블은 새 세대. 다시 돌리면 재사용으로 스왑 뒤 색인만 다시 |
| `NEKI_BATCH_IMAGE` 없음 | 경고 후 건너뜀. flow 성공 |

## 재실행과 백필

- 같은 deployment 를 다시 실행. 재사용 덕에 Kakao 호출이 거의 없음
- 백필은 `stores_enrich(target_date=<지난 날짜>)`. 그 날짜 이하의 최신 적재물을
  읽고 파티션과 staging 테이블 이름에 그 날짜가 붙음
- `persist=False` 는 S3 와 Postgres 에 쓰지 않고 색인도 안 띄움. 직전 세대도 안
  읽으므로 전 지점을 Kakao 에 물음

## 변경 검증

- `make check` : 임포트, deployment 수집, anchor, pytest
- 판정을 건드렸다면 `make enrich` 를 두 번. 첫 실행은 전부 `ok`, 둘째는 전부
  `reused`. `KAKAO_API_KEY` 를 비우고도 완주해야 함
- 적재를 건드렸다면 테이블이 없는 상태부터. 두 번 돌려 두 테이블 건수가 같고
  인덱스 이름의 번호가 `1` 을 넘지 않아야 함
- `persist=False` 로 S3 와 테이블이 늘지 않아야 함

## 정리

enrich 는 05:00 KST 에 manifest 가 가리키는 브랜드별 최신 CSV 를 읽어 좌표를
법정동 코드로 바꿉니다. 좌표가 안 바뀐 지점은 직전 답을 재사용하고 Kakao 가
죽어도 완주합니다. S3 파티션 하나와 Postgres 세대 하나를 남기고 800건 미만이면
바꿔치우지 않으며, 끝나면 서버 색인 잡을 k8s Job 으로 띄웁니다.
