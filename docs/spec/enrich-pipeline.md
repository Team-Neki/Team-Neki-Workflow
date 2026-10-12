# 지점 법정동 보강(enrich) 파이프라인 정책

이 문서는 collect 가 남긴 지점 좌표에 법정동 코드를 붙이는 enrich 단계의 정책을
다룹니다. 무엇을 읽고, 어떻게 판정하며, 어디에 어떤 모양으로 남기는지를
정합니다. 색인을 띄우는 일은 `docs/spec/search-index.md` 가 따로 다룹니다.

**이 문서가 정본입니다.** 코드와 이 문서가 어긋나면 문서가 맞고 코드가 틀린
것입니다. 동작을 바꾸려면 이 문서를 먼저 고치고 같은 PR 에서 코드를 맞춥니다.
항목마다 `구현` 줄이 구현 위치를 가리키며 `make spec-check` 가 확인합니다. 설계에
이른 이유는 `docs/superpowers/specs/2026-09-25-stores-enrich-design.md` 에 있습니다.

## 범위

다루는 것은 enrich 입니다. 검색 API 가 부스에서 쓰는 값은 법정동 코드 10자리와
1km 안 역 둘뿐이고, 역은 좌표만으로 index(Team-Neki-Server `apps/batch` 의
`searchIndexJob`, BACKEND-65)가 계산하므로 enrich 가 만드는 값은 법정동 코드
하나입니다. 다루지 않는 것은 주소 문자열 해석과 표시용 주소 정리입니다. 이름은
앞머리 브랜드 표기 통일 하나만 합니다.

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

구현 : `deployments/stores_enrich.py:25` `schedule=Cron(`,
`flows/stores_enrich/flow.py:56` `def _read_inputs`,
`flows/stores_enrich/region.py:90` `def from_collect`

## 판정

부스당 한 번 Kakao `coord2regioncode` 를 부르고 법정동(`region_type = B`) 문서의
`code` 와 시도, 시군구, 읍면동 이름을 받습니다.

- 재사용 : 직전 세대(현재 `tb_photo_booth_enriched`)와 좌표가 같고 직전에
  `b_code` 가 있으면 Kakao 없이 그 답을 씀. 직전이 `failed` 면 다시 물음. 직전 세대의
  컬럼이 지금 스키마와 다르면(컬럼 이름을 바꾼 직후) 재사용 없이 전 지점을 물음
- 폴백 : 좌표가 없는 지점만 collect 의 `geocode.locate` 로 좌표를 얻어 같은
  길로 보냄. 얻은 좌표는 `coordinate_source = kakao` 로 결과에 넣고, 그 뒤 법정동
  조회가 실패해도 좌표는 남김
- 상태 : `ok` Kakao 응답 / `reused` 재사용 / `no_coordinate` 좌표 없고 폴백 실패 /
  `failed` 좌표는 있으나 Kakao 실패
- 쓰레드 4개. 조회 하나는 3번까지 다시 해보고 연속 3번 실패하면 남은 지점은 묻지
  않음. 상수는 `geocode.py` 것을 씀. Kakao 가 답하면 법정동 문서가 없어도 연속
  실패는 0 으로 돌아감. 재사용과 건너뛴 지점은 Kakao 를 부르지 않아 세지 않음
- 조회 결과에 실제 Kakao 호출 결과(not_called/responded/failed)를 함께 전달합니다.
  빈 이름 등 입력 처리 오류는 Kakao 장애로 세지 않고, 실제 호출이 응답했을 때만
  실패 수를 초기화합니다. 이미 포기한 뒤 진행 중인 조회가 성공해도 포기 로그는 남깁니다.
- `KAKAO_API_KEY` 가 없으면 재사용만 하고 나머지는 비움. flow 는 완주함
- task 는 하나. 지점마다 task 를 만들지 않음. 쓰레드 안에서 로그를 남기지 않음

구현 : `flows/common/kakao.py:111` `def coord2regioncode`,
`flows/stores_enrich/region.py:156` `def reusable`,
`flows/stores_enrich/region.py:189` `def resolve`,
`flows/stores_enrich/region.py:308` `def enrich_stores`,
`flows/stores_enrich/region.py:45` `WORKERS`

## 주소는 해석하지 않는다

주소 문자열을 시도, 시군구 컬럼으로 나누거나 "서울" 과 "서울특별시" 를 맞추지
않습니다. 계층은 코드 10자리(시도2+시군구3+읍면동3+리2)에 있고 이름의 정본은
`tb_legal_dong` 이라 코드로 조인하면 됩니다. Kakao 도 API 마다 표기가 다릅니다.
결과의 `sido_name`, `sgg_name`, `umd_name` 은 Kakao 가 준 시도, 시군구, 읍면동 문자열
그대로이고 운영 확인용입니다. 이름은 `tb_legal_dong` 과 같고 각각 `b_code` 앞 2, 5, 8자리에
대응합니다. 특례시 일반구는 시군구 한 자리라 `sgg_name` 이 "수원시 영통구" 이고, 세종은
시군구가 없어 `sgg_name` 이 비어 있습니다.

경고 둘만 남깁니다. `ok` 행에서 `sgg_name` 첫 토큰이 원문 주소에
없으면 시군구 불일치, 스왑 트랜잭션 안에서 `tb_legal_dong` 에 없는 `b_code`
건수. 둘 다 flow 를 막지 않습니다. 시군구 불일치는 좌표가 틀린 경우 말고도
행정구역 개편 뒤 사이트 주소가 옛 이름인 경우에 뜹니다. 2026-07 인천 개편(중구,
서구가 제물포구, 영종구, 서해구, 검단구로) 뒤 인천 지점 20여 건이 그렇고, 이때
코드는 맞으므로 경고만 보고 넘어가면 됩니다. `reused` 행은 처음 판정될 때 이미
경고했으므로 매일 되풀이하지 않습니다.

구현 : `flows/stores_enrich/region.py:278` `def mismatched`,
`flows/stores_enrich/table.py:40` `LEGAL_DONG_TABLE`

## 이름은 앞머리 브랜드 표기만 통일한다

사이트가 한 브랜드를 여러 표기로 줍니다. 포토이즘은 `포토이즘 박스 OO점` 과
`포토이즘박스 OO점` 이 섞여 오고, 같은 브랜드가 검색과 표시에서 갈립니다. enrich 는
`from_collect` 에서 이름 앞머리의 표기만 하나로 바꿉니다.

- 포토이즘: `포토이즘 박스` / `포토이즘박스` -> `포토이즘`. `포토이즘 박스 상록수역점`
  은 `포토이즘 상록수역점`
- 브랜드(`platform`)별로 등록한 표기만 바꿈. 등록되지 않은 브랜드와 맞지 않는 이름은 그대로
- 원문은 collect CSV(S3)에 남음. collect 는 여전히 해석하지 않음
- 브랜드와 지점명을 나누는 검색용 정규화는 여전히 서버 `SearchNormalizer` 몫

구현 : `flows/stores_enrich/name.py:16` `PREFIX_ALIASES`,
`flows/stores_enrich/region.py:107` `unify_brand(`

## 관리자 등록 지점도 함께 담는다

수집 경로에 없는 지점은 관리자가 서버의 `tb_photo_booth_manual` 에 넣습니다. enrich 는
매 실행 이 테이블을 전량 읽어 수집 지점과 같은 판정(재사용 또는 Kakao)을 거쳐 함께
담습니다. 테이블 스키마는 Team-Neki-Server Flyway(`V35__create_photo_booth_manual_table.sql`)가
소유하고 enrich 는 읽기만 합니다. BACKEND-222 작업입니다.

- `deleted_at` 이 NULL 이고 브랜드가 삭제되지 않은 행만 읽음
- `platform` 은 브랜드의 `tb_brand.code`. 서버 색인이 그 값으로 브랜드를 찾음.
  수집하지 않는 브랜드의 지점도 담음 (BACKEND-228 전에는 `tb_brand.platform` 이 없는 브랜드를 뺐음)
- `idx` 는 `manual-<id>`. 사이트 idx 와 겹치지 않게 접두를 붙임. 겹치면 수집 지점을 남김
- `source_type` 은 수집 지점 `COLLECTED`, 관리자 등록 지점 `MANUAL`. 지점 마스터와 같은 어휘
- `source_dt` 는 이번 사이클, `collected_at` 은 관리자가 마지막으로 고친 시각(`updated_at`),
  `coordinate_source` 는 `manual`
- 이름은 수집 지점과 같이 앞머리 브랜드 표기만 통일함
- 하한(800)은 수집 지점만 셈. 관리자 등록 지점이 수집 장애를 가리지 않게 함
- 테이블이 없으면(서버 마이그레이션 전) 경고 후 수집 지점만으로 진행함

구현 : `flows/stores_enrich/table.py:145` `def read_manual`,
`flows/stores_enrich/region.py:125` `def from_manual`,
`flows/stores_enrich/region.py:41` `MANUAL_IDX_PREFIX`,
`flows/stores_enrich/flow.py:99` `def _read_manual`

## enrich 결과는 Postgres 에만 저장한다

collect 원본 CSV 는 계속 S3 에서 읽지만 enrich 결과는 S3 에 쓰지 않습니다.
결과의 유일한 저장소는 `tb_photo_booth_enriched` 이고 flow 반환값에 `s3_path` 는
없습니다. 기존 S3 enrich 객체는 삭제하지 않습니다. COPY 열 순서는
`EnrichedStore` 필드 순서가 정본입니다.

구현 : `flows/stores_enrich/region.py:84` `COLUMNS = tuple(`

## 산출물 : Postgres `tb_photo_booth_enriched`

index 가 읽는 현재 세대입니다. 세대 교체는 `tb_legal_dong` 과 같은 바꿔치기입니다.
사이클 날짜를 붙인 테이블을 COPY 로 채우고 인덱스를 건 뒤 이름을 맞바꾸며 직전은
`_prev` 로 남깁니다. 한 트랜잭션, `lock_timeout 5s`, `_prev` 는 맨 앞에서 치움.
같은 날 다시 돌리면 인덱스 이름이 현재 세대와 부딪혀 `1` 이 붙었다 다음 실행에
돌아오며, `2` 이상으로 올라가면 `_prev` 정리가 빠진 것입니다.

- 수집 지점이 하한 800 미만이면 바꿔치우지 않고 예외. 브랜드 하나가 빠지는 것은
  막지 않고 거의 빈 테이블만 막음
- index 와의 계약은 `platform`, `idx`, `name`, `address`, `longitude`, `latitude`,
  `source_dt`, `b_code` 여덟 열. `b_code` 가 NULL 인 행도 남김. 관리자 등록 지점도
  같은 여덟 열로 담겨 index 는 구분 없이 카드를 만듦
- `source_type` 으로 수집 지점과 관리자 등록 지점을 구분함. stores-sync 는 이 값을 그대로
  지점 마스터의 `source_type` 으로 씀 (`docs/spec/stores-sync.md`). 이 열이 없는 직전 세대는 재사용하지 않으므로
  배포 직후 첫 실행은 전 지점을 Kakao 에 물음
- 시각 컬럼은 시간대 없는 `TIMESTAMP` 에 KST 벽시계
- BACKEND-114 의 `tb_temp_photo_booth` 와 별도 테이블. enrich 는 S3 를 읽으므로
  그쪽을 기다리지 않음

구현 : `flows/stores_enrich/table.py:33` `TABLE`,
`flows/stores_enrich/table.py:179` `def swap_table`,
`flows/stores_enrich/table.py:116` `def read_current`,
`flows/stores_enrich/flow.py:48` `MIN_EXPECTED`

## 색인은 별도 flow 가 띄운다

enrich 가 끝나도 색인 Job 을 띄우지 않습니다. 묶으면 enrich 재시도가 색인을
되풀이하고 색인 실패가 enrich 를 실패로 만듭니다. `search-index` flow 가 시각으로
뒤에 돌며 그 시점의 `tb_photo_booth_enriched` 현재 세대를 읽습니다. 정책은
`docs/spec/search-index.md` 에 있습니다.

## 결과는 Discord 로 알린다

적재가 끝나면(`persist=True`) 결과를 Discord webhook 으로 보냅니다. 실패하거나
크래시한 run 도 사유와 함께 보냅니다. `legal-dong`, `subway-station` 도 같은 모듈로
알립니다.

- 주소는 `DISCORD_WEBHOOK_URL`. 비어 있으면 보내지 않음. 로컬 실행이 채널을 울리지 않게
- 제목 앞에 `SPRING_PROFILES_ACTIVE` 를 붙여 환경을 가름. 비어 있으면 `local`
- 본문은 브랜드마다 S3 에서 읽은 행 수(`s3`), enriched 에 담긴 수집 지점(`enriched`),
  관리자 등록 지점(`manual`), 법정동 코드를 못 붙인 지점(`no_bcode`). `s3` 와
  `enriched` 가 다르면 `(platform, idx)` 중복으로 버린 것
- `failed` 로 빠진 브랜드와 `stale` 로 대신한 브랜드, `tb_legal_dong` 에 없는 코드를 경고로 붙임
- **알림 실패로 flow 를 실패시키지 않음.** 알림은 적재 커밋 뒤에 불리고, 발송·본문 조립의 어떤 예외도 경고로만 남김. 새면 이미 끝난 적재가 실패로 기록되고 재시도가 적재를 되풀이함

구현 : `flows/common/discord.py:53` `def notify`,
`flows/common/discord.py:96` `def notify_failure`,
`flows/stores_enrich/flow.py:120` `def _report`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| Postgres | flow 실패. `DATABASE_URL` 이 없으면 시작 직후 `RuntimeError` |
| S3 (읽기) | 그 브랜드 예외로 flow 실패. 건수 불일치도 같음 |
| Kakao | 그 지점만 `failed`. 연속 3회면 남은 지점은 묻지 않음. flow 완주 |
| Discord | 경고만 남김. flow 완주 |

## 재실행과 백필

- 같은 deployment 를 다시 실행. 재사용 덕에 Kakao 호출이 거의 없음
- 백필은 `stores_enrich(target_date=<지난 날짜>)`. 그 날짜 이하의 최신 적재물을
  읽고 staging 테이블 이름에 그 날짜가 붙음
- `persist=False` 는 Postgres 에 쓰지 않음. 직전 세대도 안 읽으므로 전
  지점을 Kakao 에 물음

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
죽어도 완주합니다. Postgres 세대 하나만 남기고 800건 미만이면
바꿔치우지 않습니다. 색인은 별도 flow `search-index` 가 띄웁니다.
