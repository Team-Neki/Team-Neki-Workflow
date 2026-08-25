# uv run 을 감싸는 진입점. 명령마다 uv run 을 치지 않도록 한다.
#
# 변수는 실행할 때 덮어쓸 수 있다.
#   make server PORT=4300
#   make deploy WORK_POOL=other-pool

UV ?= uv
HOST ?= 127.0.0.1
PORT ?= 4200
API_URL ?= http://$(HOST):$(PORT)/api
WORK_POOL ?= neki-pool

# uv run 은 .env 를 자동으로 읽지 않는다. 파일이 있을 때만 지정한다.
# 없는 파일을 가리키면 uv 가 실패하므로 wildcard 로 존재를 확인한다.
ifneq ($(wildcard .env),)
export UV_ENV_FILE := .env
endif

# UI를 쓰려면 API 주소를 지정해야 한다. 지정하지 않으면 프로세스 안에
# 임시 서버가 떴다 사라져 UI로 접근할 수 없다.
PREFECT_ENV = PREFECT_API_URL=$(API_URL)

.DEFAULT_GOAL := help
.PHONY: help setup check hello lifefourcuts photoism dontlxxkup photosignature \
	photogray planbstudio picdot monomansion harufilm photolabplus broomstudio \
	collect localstack localstack-down s3-init s3-ls serve server deploy build clean

help: ## 명령 목록을 출력한다
	@echo "사용법: make <명령>"
	@echo
	@grep -E '^[a-z][a-z-]*:.*## ' $(MAKEFILE_LIST) \
		| awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
	@echo
	@echo "변수: HOST=$(HOST) PORT=$(PORT) WORK_POOL=$(WORK_POOL)"
	@echo "UI:   http://$(HOST):$(PORT)/deployments"

setup: ## 의존성을 uv.lock 기준으로 설치한다
	$(UV) sync

check: ## 임포트와 deployment 수집을 확인한다
	@$(UV) run python -c "\
	from deployments import collect; \
	found = list(collect()); \
	print('deployment', len(found), '건'); \
	[print('  ', d.flow_name + '/' + d.name) for d in found]"

hello: ## hello 워크플로를 실행한다
	$(UV) run python -c "from flows.hello import hello; hello()"

lifefourcuts: ## 인생네컷 지점을 수집한다
	@$(UV) run python -c "\
	from flows.lifefourcuts_stores import lifefourcuts_stores; \
	stores = lifefourcuts_stores(); \
	print('수집', len(stores), '건')"

photoism: ## 포토이즘 지점을 수집한다
	@$(UV) run python -c "\
	from flows.photoism_stores import photoism_stores; \
	stores = photoism_stores(); \
	print('수집', len(stores), '건')"

dontlxxkup: ## 돈룩업 지점을 수집한다
	@$(UV) run python -c "\
	from flows.dontlxxkup_stores import dontlxxkup_stores; \
	stores = dontlxxkup_stores(); \
	print('수집', len(stores), '건')"

photosignature: ## 포토시그니처 지점을 수집한다
	@$(UV) run python -c "\
	from flows.photosignature_stores import photosignature_stores; \
	stores = photosignature_stores(); \
	print('수집', len(stores), '건')"

photogray: ## 포토그레이 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.photogray_stores import photogray_stores; \
	stores = photogray_stores(); \
	print('수집', len(stores), '건')"

planbstudio: ## 플랜비스튜디오 지점을 수집한다
	@$(UV) run python -c "\
	from flows.planbstudio_stores import planbstudio_stores; \
	stores = planbstudio_stores(); \
	print('수집', len(stores), '건')"

picdot: ## 픽닷 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.picdot_stores import picdot_stores; \
	stores = picdot_stores(); \
	print('수집', len(stores), '건')"

monomansion: ## 모노맨션 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.monomansion_stores import monomansion_stores; \
	stores = monomansion_stores(); \
	print('수집', len(stores), '건')"

harufilm: ## 하루필름 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.harufilm_stores import harufilm_stores; \
	stores = harufilm_stores(); \
	print('수집', len(stores), '건')"

photolabplus: ## 포토랩플러스 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.photolabplus_stores import photolabplus_stores; \
	stores = photolabplus_stores(); \
	print('수집', len(stores), '건')"

broomstudio: ## 비룸스튜디오 지점을 수집한다 (KAKAO_API_KEY 필요)
	@$(UV) run python -c "\
	from flows.broomstudio_stores import broomstudio_stores; \
	stores = broomstudio_stores(); \
	print('수집', len(stores), '건')"

collect: ## 전체 브랜드를 병렬로 수집한다 (KAKAO_API_KEY, S3 필요)
	@$(UV) run python -c "\
	from flows.stores_collect import stores_collect; \
	results = stores_collect(); \
	print('성공', sum(1 for r in results.values() if r['status'] == 'ok'), '건'); \
	print('합계', sum(r.get('count', 0) for r in results.values()), '건')"

# docker compose 는 .env 를 알아서 읽으므로 LOCALSTACK_PORT 가 그대로 먹는다.
localstack: ## 로컬 S3(LocalStack)를 띄운다
	docker compose up -d --wait
	@$(MAKE) --no-print-directory s3-init

localstack-down: ## 로컬 S3를 내린다 (버킷 내용도 사라진다)
	docker compose down

s3-init: ## 로컬 버킷을 만든다 (이미 있으면 넘어간다)
	@$(UV) run python -c "\
	import os, boto3; \
	s3 = boto3.Session().client('s3'); \
	bucket = os.environ['S3_BUCKET']; \
	names = {b['Name'] for b in s3.list_buckets()['Buckets']}; \
	print('버킷', bucket, '이미 있음') if bucket in names else ( \
		s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={ \
			'LocationConstraint': s3.meta.region_name}), \
		print('버킷', bucket, '생성'))"

s3-ls: ## 적재된 키를 나열한다
	@$(UV) run python -c "\
	import os, boto3; \
	s3 = boto3.Session().client('s3'); \
	bucket = os.environ['S3_BUCKET']; \
	pages = s3.get_paginator('list_objects_v2').paginate(Bucket=bucket); \
	keys = sorted(o['Key'] for p in pages for o in p.get('Contents', [])); \
	print('s3://' + bucket, len(keys), '개'); \
	[print('  ', k) for k in keys]"

serve: ## 로컬 개발용으로 deployment를 서빙한다 (server가 먼저 떠 있어야 함)
	$(PREFECT_ENV) $(UV) run python serve.py

server: ## Prefect 서버를 띄운다 (UI 주소는 아래 변수 참고)
	$(UV) run prefect server start --host $(HOST) --port $(PORT)

deploy: ## work pool에 스케줄을 등록한다 (pause 상태 보존)
	$(PREFECT_ENV) PREFECT_WORK_POOL=$(WORK_POOL) $(UV) run python deploy.py

build: ## wheel을 빌드하고 포함된 패키지를 확인한다
	$(UV) build --wheel --out-dir dist
	@$(UV) run python -c "\
	import glob, zipfile; \
	w = sorted(glob.glob('dist/*.whl'))[-1]; \
	names = zipfile.ZipFile(w).namelist(); \
	print('wheel 최상위:', sorted({n.split('/')[0] for n in names if '/' in n}))"

clean: ## 빌드 산출물과 캐시를 지운다
	rm -rf dist build *.egg-info
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} +
