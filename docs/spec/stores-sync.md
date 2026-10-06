# 지점 마스터 동기화(stores-sync) 정책

수집·보강 결과를 서버 지점 마스터 `tb_photo_booth_location`에 반영합니다.
원천 키는 `(platform, idx)`이고 서비스 식별자는 지점 테이블의 `id`입니다.
주소·좌표로 기존 카카오 지점을 추정 연결하지 않습니다. BACKEND-153 작업입니다.

이 문서가 정본입니다. 동작을 바꾸면 문서를 먼저 고치고 코드를 맞춥니다.

## 실행과 입력

- enrich와 별도 flow입니다. 현재 `tb_photo_booth_enriched`를 한 번 읽습니다.
  `source_type = 'COLLECTED'`인 수집 지점만 읽고, enrich가 함께 담은 관리자 등록
  지점(`MANUAL`, `docs/spec/enrich-pipeline.md`)은 동기화하지 않습니다.
  enriched에 `source_type` 열이 생기기 전(새 enrich가 한 번 돌기 전)에는 실패합니다.
- 외부 사이트, Kakao, S3는 부르지 않습니다. 접속은 기존 `DATABASE_URL`을 씁니다.
- `target_date`는 허용할 수집 사이클의 상한이며 기본값은 run 예약일(KST)입니다.
  미래 `source_dt`는 제외합니다. 과거 S3 스냅샷을 복원하는 백필은 아닙니다.
- 기본 입력 하한은 800건입니다. `min_expected`는 양수만 허용합니다.
  원천 키 중복·잘못된 날짜·브랜드 매핑 누락은 전체 실행을 실패시킵니다.
- 좌표 누락·범위 오류·빈 이름·빈 주소는 해당 행만 제외하고 사유별 건수를 남깁니다.
  적재 가능한 지점이 0건이면 쓰지 않습니다.
- 지점 동기화 deployment는 스케줄 없이 등록합니다. 서버 마이그레이션, 기존 카카오
  수집 중지, 지도 조회 범위 전환과 검색 색인의 지점 참조 전환이 준비된 뒤 스케줄을 정합니다.

구현 : `flows/stores_sync/flow.py:13` `def stores_sync`,
`flows/stores_sync/table.py:112` `WHERE source_type = 'COLLECTED'`,
`flows/stores_sync/records.py:28` `def prepare`,
`deployments/stores_sync.py:9` `to_deployment(name="stores-sync"`

## 안정적인 ID와 수동 관리

- 스키마는 Team-Neki-Server Flyway의
  `V34__add_photo_booth_location_source_and_overrides.sql`이 소유합니다.
  flow는 테이블을 만들거나 ALTER하지 않습니다. 선행 스키마가 없으면 실패합니다.
- 기존 행은 `LEGACY`로 남습니다. 수동 등록은 `MANUAL`, 배치 생성은 `COLLECTED`입니다.
  배치는 `COLLECTED` 원천 키에만 INSERT/UPDATE합니다. LEGACY/MANUAL은 건드리지 않습니다.
- 수집 지점은 `(source_platform, source_idx)` 유일성 제약으로 upsert합니다.
  갱신할 때 `id`, `created_at`, `map_id`를 바꾸지 않습니다.
- 수집 지점의 `map_id`는 호환 컬럼을 채우기 위한 `source:<platform>:<idx>`입니다.
  카카오 장소 ID가 아니며 원천 키 매칭에 쓰지 않습니다.
- 원문은 `source_name/address/location/b_code`에 담고, 조회용
  `branch_name/address/location/b_code`에는 override가 있으면 그 값을 씁니다.
  지점 이름은 enrich 의 `name` 그대로입니다. enrich 가 앞머리 브랜드 표기만 통일하며
  (`docs/spec/enrich-pipeline.md`) 브랜드 접두 제거는 서버 `SearchNormalizer`가 맡습니다.
- 어드민은 `override_branch_name/address/location/b_code`와 조회용 컬럼을 같은
  트랜잭션에서 갱신해야 즉시 조회에 반영됩니다. NULL로 override를 해제할 때도
  원문으로 조회용 값을 복원해야 합니다. 어드민 API 자체는 이 flow의 범위 밖입니다.
- 좌표 override가 있고 법정동 override가 없으면 조회용 `b_code`는 NULL입니다.
  다른 위치의 수집 법정동을 잘못 사용하지 않습니다. 좌표 보정 시 법정동도 함께
  보정하거나 서버에서 다시 계산해야 합니다.
- `admin_hidden`은 배치가 변경하지 않습니다. 노출 중지는 관리자만 해제합니다.
- 수집에서 빠진 지점도 삭제하거나 비활성화하지 않습니다. 현재 enrich는 실패한
  브랜드를 제외하므로 부재만으로 폐점을 확정할 수 없습니다. 폐점은 관리자 판단입니다.

구현 : `flows/stores_sync/table.py:36` `UPSERT`,
`flows/stores_sync/table.py:72` `def require_schema`

## 트랜잭션과 재실행

- 입력 조회와 지점 갱신은 한 트랜잭션입니다. 실패하면 전체 롤백합니다.
- 동기화끼리는 트랜잭션 advisory lock을 사용하며 중복 실행은 즉시 실패합니다.
- 행 잠금은 5초까지만 기다립니다. 어드민 override는 upsert SQL에서 현재 행의
  값을 읽으므로 배치가 앞서 읽어둔 값으로 덮어쓰지 않습니다.
- `(source_dt, source_collected_at)`가 기존 값보다 오래되면 갱신하지 않습니다.
  같은 입력을 다시 실행해도 locationId와 어드민 설정은 유지됩니다.
- `persist=False`는 입력·브랜드·스키마를 읽고 예상 건수만 반환합니다.
  지점 INSERT/UPDATE, S3 업로드, 검색 색인 실행은 하지 않습니다.

구현 : `flows/stores_sync/table.py:93` `def synchronize`,
`flows/stores_sync/table.py:183` `def sync_locations`

## 조회와 색인 연결의 후속 계약

서버 검색 배치는 수집 지점을 `(source_platform, source_idx)`로 조인해 `id`를
검색 카드의 `location_id`로 저장해야 합니다. 수동 지점도 같은 마스터에서 읽습니다.
조회·색인은 `source_type != 'LEGACY' AND NOT admin_hidden`인 최종 지점 값을 사용해야
단일 원천으로 전환됩니다. 현재 서버 코드는 이 계약으로 바뀌지 않았습니다.

기존 지점과 즐겨찾기의 이관·초기화는 별도 결정입니다. 이 flow는 기존 행이나
즐겨찾기를 지우지 않습니다. 기존 카카오 수집을 계속 실행하면 새 수집 지점도
지우거나 덮어쓸 수 있으므로 staging 실행 전에 그 경로를 중지해야 합니다.

## 검증

- `make check`: spec anchor, deployment 수집, 입력 판정·flow 테스트
- `make stores-sync-dry-run`: 현재 환경의 입력 및 스키마 확인, 쓰기 없음
- `make test-stores-sync-db`: 전용 PostGIS DB의 격리 schema에서 실제 upsert 검증
  (`STORES_SYNC_TEST_DATABASE_URL` 필수, 앱 DATABASE_URL을 대신 사용하지 않음)
- 마이그레이션 적용 전 실패, 동일 입력 두 번의 ID 유지, override 유지, MANUAL/LEGACY
  유지, admin_hidden 유지, 오래된 입력 차단, 실패 시 롤백을 확인합니다.
- 운영/staging 배포와 어드민 API·검색 색인 연결은 이 변경에서 실행하지 않습니다.
