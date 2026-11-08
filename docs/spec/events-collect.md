# 브랜드 소식 수집

설계: [브랜드 소식 수집 HLD](https://sprint.suitestudy.com:4641/wiki/cmve1qomb005hka01j1l6xpm9).
구현 티켓: BACKEND-258. 이 문서는 collect의 구현 계약이며 공개·정렬은 enrich 이후 책임이다.

## 범위

- 포토시그니처 frame 게시판과 하루필름 301 공지 게시판의 목록·상세·이미지 수집.
- 브랜드별 파일은 `flows/events/collect/<브랜드>.py`. 공통 HTTP·이미지·저장은 events 내부에서 공유.
- `brand_id`는 실제 운영 브랜드 ID를 실행 인자로 받음. PoC의 brand-04/05를 운영 ID로 간주하지 않음.
- `pages=1`은 초기 실행 범위이며 전체 이력 수집을 의미하지 않음. 지정 페이지를 순차 조회하고 게시물 ID로 중복 제거. 같은 목록이 반복되면 중단. 수집 일정은 미정이므로 deployment에 cron을 두지 않음.
- `max_posts` 지정 시 목록 앞쪽 일부만 실행(로컬 검증용). 기본값은 제한 없음. 목록 미발견을 삭제로 해석하지 않음.
- 배치 시작 시 출처별 collected.csv를 읽어 완료 ID를 메모리에 유지. 완료 ID는 상세·이미지·post.json 요청을 생략. CSV에 없는 ID만 완료 원문으로 복구하거나 신규 수집. 게시 후 수정·삭제는 자동 감지하지 않음.
- 반환값은 discovered/skipped/collected/recovered/incomplete 건수 요약. persist=False는 S3 읽기·쓰기를 모두 생략하고 검증 완료 건수를 collected로 반환.
- CSV는 external_id,completed_at(완료 원문의 S3 LastModified, UTC) 두 열. NoSuchKey만 빈 인덱스로 처리. 손상·권한 오류는 실패.
- 정상 종료 시 새 완료·복구 ID가 있을 때만 CSV를 전체 병합 교체. 기존 ID와 시각 유지. 최초 생성 If-None-Match, 갱신 If-Match 사용. Prefect 재시도는 최신 CSV부터 다시 읽어 병합.
- post.json은 원문 식별자·필수 이미지 정보·상세 완료 조건 검증 후 복구. CSV에 이미 있는 ID의 원문 유실은 일반 배치에서 감지하지 않음.
- 상세 확인과 발견 이미지 전체 저장이 완료된 경우에만 post.json 생성. 누락 또는 실패한 글은 다음 실행에서 다시 수집. persist=False는 완료 여부 조회 없이 원문 검증.

## 원문과 이미지

- 게시물 JSON: schema_version, brand_id, source_type, source_key, external_id, source_url,
  observed_at, raw_content, title, body, published_at_raw, frame_period_raw,
  image_list_complete, images. 미제공 정보는 null, 게시일 미상도 수집.
- raw_content에는 목록 항목과 상세 본문 HTML을 보존. 요청 쿠키·인증 헤더는 저장하지 않으며 HTML의 스크립트·입력 필드는 제외.
- 기간 문자열은 원문 그대로 보존. 하루필름 이미지 OCR, 연도 보완, 프레임 기간 판정은 enrich 책임.
- 이미지 순서는 원문 순서 유지. URL을 발견했으나 다운로드 실패 시 해당 순서·URL을 남기고 파일 정보는 null.
- 상세 본문을 찾지 못하면 image_list_complete=null. 본문을 확인했더라도 읽을 수 없는 이미지 주소가 있으면 false. 이미지 0장도 메타데이터 보존.
- 원문 메타데이터를 이미지 처리 전에 먼저 저장하고 처리 후 보완본 저장. 보완본이 생겨도 이전 산출물 유지.
- enrich는 상세를 확인한 단계별 원문 중 observed_at 최대를 입력으로 쓰고, 미확보 이미지는 같은 게시물의 이전 원문에서 source_url로 보완([enrich 입력 원문 선택 ADR](https://sprint.suitestudy.com:4641/wiki/cmw6t0otv006nl601zproyv70)). 이를 위해 collect는 단계별 원문을 덮어쓰지 않고, observed_at을 상세 요청 시각으로 두며(같은 실행의 상세·이미지 단계 원문은 같은 값), 같은 관측 안에서 이미지 정보를 제거하지 않고, 게시물 폴더의 이미지를 지우지 않음.
- HTTPS 출처·CDN 허용 호스트만 요청하고 리디렉션도 매번 검증. 요청 간 기본 1초 간격.
- 이미지 최대 20 MiB, 최대 4천만 픽셀. Pillow로 JPEG·PNG·WebP·GIF 파일을 검증하고 원래 바이트를 보존. 형식 변환은 하지 않음.

## S3와 실패

- 기존 S3_BUCKET 및 boto3 기본 자격증명 사용. 지점 CSV/manifest와 별개인 events prefix 사용.
- 게시물 경로: `events/raw/<source_key>/<external_id>/`. 이미지: `images/<SHA-256>.<확장자>` (검증된 실제 포맷 기준 jpg/png/webp/gif). 단계별 원문: `<JSON SHA-256>.json`, 완료 원문: `post.json`.
- 제목 변경에 따른 경로 변경을 피하기 위해 S3 폴더명은 ID만 사용. 같은 이미지는 게시물 내에서 재사용하며 게시물 간에는 별도 저장.
- 원문·이미지는 조건부 생성으로 같은 키를 덮어쓰지 않음. 동시 실행·S3 버전 관리에서도 동일 이미지의 중복 버전 생성 방지.
- metadata의 observed_at은 실제 원문 관측 시각. 재실행 관측은 새 이력, 동일 task 재시도는 같은 파일 재사용.
- 일시 HTTP/S3 오류는 Prefect task 재시도. 401/403/429와 로그인·challenge 응답은 해당 flow 중단. 일반 상세·이미지 실패는 원문과 누락 정보를 남기고 다음 게시물 진행.
- S3 쓰기 실패는 flow 실패. 전 게시물 상세 실패 시 원문 보존 후 flow 실패로 표시.
- `persist=False`는 원문·이미지 다운로드/검증까지 수행하지만 S3에는 쓰지 않음. DB·enrich·공개 상태 변경 없음.

## 검증

`make check`: 출처별 파싱, 부분 실패, 차단 중단, 실제 파일 검증, S3 중복 생성 방지와 deployment entrypoint 확인.
실제 사이트 확인은 pages=1/max_posts=1/persist=False로 제한. 운영 S3·DB 쓰기 없이 수행.

## 구현 위치

- `src/neki_workflow/flows/events/collect/photosignature.py:47` `def photosignature_events(`
- `src/neki_workflow/flows/events/collect/harufilm.py:47` `def harufilm_events(`
- `src/neki_workflow/flows/events/common/web.py:144` `def collect_posts(`
- `src/neki_workflow/flows/events/common/storage.py:25` `def put_object(`
- `src/neki_workflow/flows/events/common/post.py:17` `class CollectedPost:`
