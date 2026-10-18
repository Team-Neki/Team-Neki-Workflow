# 변경 검증

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
`psql` 은 `make enrich` 가 읽는 `.env` 의 `DATABASE_URL` 로 붙어야 같은 DB 를
봅니다. 기본 접속은 다른 DB 일 수 있어 아래 블록 첫 줄이 그 값을 꺼냅니다.

```bash
DATABASE_URL=$(sed -n 's/^DATABASE_URL=//p' .env)
psql "$DATABASE_URL" -c 'DROP TABLE IF EXISTS tb_photo_booth_enriched, tb_photo_booth_enriched_prev'
make enrich
make enrich
psql "$DATABASE_URL" -c "select geocode_status, count(*) from tb_photo_booth_enriched group by 1"
```

`search-index` 는 로컬에서 `NEKI_BATCH_IMAGE` 가 없어 경고 후 끝나는 것이
정상입니다. Job 을 실제로 띄우는 확인은 staging 에서 합니다.

`legal-dong`은 `DATABASE_URL`이 있어야 돕니다. 파싱과 정규화만 볼 때는
`legal_dong(persist=False)`로 끄면 `DATABASE_URL` 없이도 됩니다.

적재를 건드렸다면 **테이블이 없는 상태부터** 확인해야 합니다. 첫 실행에는 바꿔칠
대상이 없어 경로가 다릅니다. 그다음 여러 번 돌려 `tb_legal_dong`과 `_prev`가 둘 다
20,561행인지, 인덱스 이름의 번호가 계속 올라가지 않는지 봅니다.

```bash
DATABASE_URL=$(sed -n 's/^DATABASE_URL=//p' .env)
psql "$DATABASE_URL" -c 'DROP TABLE IF EXISTS tb_legal_dong, tb_legal_dong_prev'
make legal-dong
make legal-dong
psql "$DATABASE_URL" -c "select tablename, indexname from pg_indexes where tablename like 'tb_legal_dong%'"
```

`normalize.py`를 건드렸다면 실행 로그의 "시군구명 분리" 건수가 1,755에서 움직이지
않는지, "시군구가 없는 시도로 판정"이 세종 하나인지 봐야 합니다. 둘 중 하나가
바뀌면 멀쩡한 이름을 자르고 있을 수 있습니다.

DDL을 바꿨다면 `CREATE TABLE IF NOT EXISTS`가 기존 테이블을 고치지 않으므로
`DROP TABLE tb_legal_dong` 후 다시 돌려야 합니다.

`picdot`, `monomansion`, `photogray`, `harufilm`, `photolabplus`, `broomstudio`는
`KAKAO_API_KEY`가 있어야 돕니다. 키가 없으면 `neki_workflow/flows/stores/common/kakao.py`의 `api_key()`가
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
DATABASE_URL=$(sed -n 's/^DATABASE_URL=//p' .env)
psql "$DATABASE_URL" -c 'DROP TABLE IF EXISTS tb_store_collect_manifest'
make collect
make collect
psql "$DATABASE_URL" -c "select platform, count(*) from tb_store_collect_manifest group by 1"
```

브랜드마다 행이 2건이어야 합니다. 1건이면 덮어쓰고 있는 것이고, 인덱스 이름에
번호가 붙으면 `IF NOT EXISTS`가 빠진 것입니다.

`neki_workflow/flows/stores/common/`의 수집 모듈을 고쳤다면 그것을 쓰는 브랜드를 모두 돌려 건수가
전과 같은지 봐야 합니다. `imweb_map.py`는 인생네컷과 포토이즘, 돈룩업이 함께 씁니다.

파싱만 확인하고 싶으면 적재를 끕니다.

```bash
uv run --env-file .env python -c \
  "from neki_workflow.flows.stores.collect.picdot import picdot_stores; picdot_stores(persist=False)"
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

`deploy.py`나 `neki_workflow/deployments/`를 건드렸다면 로컬 서버를 띄워 배포 사이클을 확인해야
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
