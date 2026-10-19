# 파이프라인 구현 메모

## 수집 파이프라인

정책은 `docs/spec/collect-pipeline.md` 가 정본입니다. 여기서는 코드를 만질 때 바로
부딪히는 것만 추립니다.

- collect 는 사이트가 준 것만 담고 해석하지 않음. 예외는 좌표 보정 하나이고
  `coordinate_source` 로 구분함. 주소는 enrich 도 해석하지 않고, 이름은 enrich 가
  앞머리 브랜드 표기만 통일함(`포토이즘 박스` -> `포토이즘`). 브랜드·지점명 분리 같은
  검색용 정규화는 서버 batch(index)가 함
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
- enrich 결과는 Postgres 에만 저장. S3 는 collect 원본을 읽는 용도로만 사용
- `EnrichedStore` 의 필드 순서가 COPY 열이자 DDL 순서. 필드를 더하면
  `table._ddl` 도 같은 자리에 넣어야 함. 한쪽만 고치면 값이 엉뚱한 컬럼에 들어감.
  경도·위도만 예외로 테이블에서는 `location geometry(Point, 4326)` 한 열이고
  `table._row` 가 COPY 때 합침
- 이름은 앞머리 브랜드 표기 통일(`name.py` 의 `PREFIX_ALIASES`) 하나만 함. 같은 브랜드의
  다른 표기가 보이면 거기에 더함. 주소는 해석하지 않음. 계층은 `b_code` 자리수에, 이름은 `tb_legal_dong` 에
  있음. 브랜드·지점명 분리 같은 검색용 정규화는 서버 batch(`SearchNormalizer`)가 함.
  시군구 비교는 경고용 느슨한 토큰 비교 하나뿐. 인천 개편 뒤 사이트 주소가 옛 구
  이름이라 인천 20여 건이 늘 경고로 뜨는데 코드는 맞음
- 지역 이름 컬럼 `sido_name / sgg_name / umd_name` 은 Kakao 값 그대로이고 `tb_legal_dong`
  과 같은 계층(`b_code` 앞 2 / 5 / 8자리). 직전 세대에 이 컬럼이 없으면 재사용 없이 전
  지점을 물음
- Kakao 조회는 task 하나 안의 쓰레드. 지점마다 task 를 만들지 않음. 쓰레드 안에서
  `get_run_logger` 를 부르지 않음 (컨텍스트가 따라가지 않음)
- 관리자 등록 지점(서버 `tb_photo_booth_manual`, Flyway V35 소유)도 매 실행 읽어 함께
  담음(`source_type = MANUAL`, `idx = manual-<id>`). 하한은 수집 지점만 셈. stores-sync 는
  `source_type` 을 그대로 지점 마스터에 쓰므로 enriched 에 행을 더하는 경로가 생기면 꼭 채움
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
- 스케줄은 `deployments/search_index.py` 의 05:30 KST cron. UI 에서 붙인 스케줄은
  배포가 지우므로 스케줄은 코드에서만 바꿈
