# 검색 색인 실행(search-index) 정책

이 문서는 서버의 검색 색인 잡(`searchIndexJob`)을 Prefect 가 k8s Job 으로 띄우는
`search-index` flow 의 정책을 다룹니다. 색인 규칙 자체는 Team-Neki-Server
`apps/batch` 의 것이고(BACKEND-65), 여기서는 언제 무엇을 어떻게 띄우고 성패를
어디에 남기는지만 정합니다.

**이 문서가 정본입니다.** 코드와 이 문서가 어긋나면 문서가 맞고 코드가 틀린
것입니다. 동작을 바꾸려면 이 문서를 먼저 고치고 같은 PR 에서 코드를 맞춥니다.
항목마다 `구현` 줄이 구현 위치를 가리키며 `make spec-check` 가 확인합니다.

## enrich 와 별도 flow 이고 순서는 시각으로 맞춘다

enrich flow 끝에서 색인을 부르지 않습니다. 묶으면 enrich 재시도가 색인을
되풀이하고 색인 실패가 enrich 를 실패로 만듭니다. collect 와 enrich 를 나눈
것과 같은 이유로 나누고, 순서는 시각으로 맞춥니다.

- 색인은 그 시점의 `tb_photo_booth_enriched` 현재 세대를 읽음. enrich 가 늦으면
  그날은 이전 세대를 색인하고, 다음 날 따라잡음
- 색인은 멱등. 같은 `businessDate` 로 다시 돌려도 결과가 같으므로 수동 재실행에
  부담이 없음 (BACKEND-128 의 `RunIdIncrementer` 계약)
- `businessDate` 는 이 run 의 예약 시각(KST). 백필은 `target_date` 로 줌
- 스케줄은 BACKEND-65 가 들어올 때 `Cron("30 5 * * *", timezone="Asia/Seoul")`
  로 붙임. 그 전에 정기로 돌리면 없는 잡 이름으로 종료 코드 1 이라 매일 실패함.
  스케줄이 없어도 UI 실행 버튼과 `prefect deployment run` 으로 띄울 수 있음
- 동시 실행은 `concurrency_limit=1` 로 막음. 서버 잡은 build 와 swap 이 다른
  트랜잭션이라 두 실행의 build 가 swap 보다 먼저 끝나면 두 번째 swap 이 첫 번째를
  되돌려 직전 세대를 서빙하고, 둘 다 COMPLETED 라 알림도 없음

구현 : `flows/search_index/flow.py:25` `def search_index`,
`flows/search_index/flow.py:33` `cycle = target_date or cycle_date()`,
`deployments/search_index.py:25` `to_deployment(name="search-index", concurrency_limit=1)`

## Job 계약

`NEKI_BATCH_IMAGE` 이미지를 `--spring.batch.job.name=searchIndexJob
businessDate=<사이클>` 인자로 k8s Job 에 띄우고 완료를 기다립니다. Job 실패는
flow 실패입니다. 환경변수가 없으면 경고 후 끝납니다. 로컬이거나 GitOps 가 아직인
경우입니다.

- 이미지는 GitOps 의 환경별 `overlays/prefect/images.env` 또는
  `overlays/prefect-stg/images.env` (ConfigMap `neki-images`, BACKEND-143).
  Team-Neki-Server 의 deploy-batch 가 선택한 환경의 줄을 갱신함
- Job 은 flow run 과 같은 Kubernetes 네임스페이스에 만듭니다. 클러스터 안에서는
  service account 의 namespace 파일을 읽고, 파일을 읽지 못하면 실패합니다. 로컬
  매니페스트 확인에서만 `prefect` 를 기본값으로 씁니다. 같은 네임스페이스의
  `prefect-workflow` Secret 을 참조하므로 staging 과 prod 의 DB 접속 정보가 섞이지 않습니다.
- Job 이름은 `search-index-<실행 시각>-<UUID hex>`. 같은 초의 실행도 다른 이름입니다.
  prefect-kubernetes 가 `metadata.name` 으로 상태를 읽으므로 `generateName` 은 못 씁니다.
  실행 시각은 YYYY-MM-DD_HHMMSS 형식만 허용하고 밑줄을 하이픈으로 바꿉니다.
  UUID 32자리를 포함한 이름 길이는 63자입니다.
- env 는 `TZ`, 그리고 Secret `prefect-workflow` 의 `SPRING_PROFILES_ACTIVE`,
  `JASYPT_PASSWORD` 둘만. Secret 을 통째로 넘기지 않음
- `backoffLimit 0`, `restartPolicy Never`. 재시도는 k8s 가 아니라 사람이 flow 를
  다시 돌려서
- 완료된 Job 은 지우지 않고 `ttlSecondsAfterFinished` 로 하루 뒤 정리. 실패 파드의
  로그가 원인임. 성공한 파드의 로그는 대기 중에 읽어 flow 로그에 남김
- 대기 전체에 실제 경과 시간 기준 타임아웃 1,800초를 적용합니다. Pod가 아직
  생성되지 않은 상태와 로그 읽기도 포함합니다. prefect-kubernetes 0.7.12의
  내부 타이머는 active Pod가 없으면 늘지 않으므로 비동기 대기를 별도 타이머로
  감쌉니다. 타임아웃은 flow 실패로 전파하며 Job은 삭제하지 않습니다.
- Job 에도 `activeDeadlineSeconds` 1,800초를 둡니다. flow 가 타임아웃으로 끝나면
  concurrency 슬롯이 비므로, 남은 Job 이 계속 돌면 재실행한 Job 과 겹칩니다.
  같은 상한에서 k8s 가 파드를 끝냅니다. swap 도중에 끝나도 트랜잭션이 롤백돼
  서빙 중 테이블은 그대로입니다
- flow run 파드는 SA `prefect-worker`. base job template 기본값이라 deployment 는
  지정하지 않음. Role 이 jobs 생성과 pods/log 조회를 허용함

구현 : `flows/search_index/job.py:38` `IMAGE_ENV`,
`flows/search_index/job.py:67` `def manifest`,
`flows/search_index/job.py:133` `async def _wait_for_completion`,
`flows/search_index/job.py:140` `def run_search_index`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| `NEKI_BATCH_IMAGE` 없음 | 경고 후 끝남. flow 성공 |
| k8s API (권한, 연결) | Job 생성 실패로 flow 실패 |
| batch 이미지 없음 | 파드가 안 떠 타임아웃까지 기다린 뒤 flow 실패 |
| 잡 실패 (종료 코드 1) | Job Failed 로 flow 실패. 파드 로그를 봄 |
| enrich 가 아직 안 돎 | 이전 세대를 색인. flow 성공 |

## 변경 검증

- `make check` : 임포트, deployment 수집, anchor, pytest (매니페스트 이름과 인자)
- 로컬 `make search-index` 는 `NEKI_BATCH_IMAGE` 가 없어 경고 후 끝나는 것이 정상
- staging 에서 `search-index-stg` 를 수동 실행해 `kubectl -n prefect-stg get jobs` 에
  `search-index-<시각>` 이 생기고 flow 로그에 batch 파드 로그가 보이는지 봄

## 정리

search-index 는 enrich 와 무관하게 정해진 시각에 그 시점의 현재 세대를 색인하도록
서버 batch 를 k8s Job 으로 띄우고 종료 코드를 flow 결과로 삼습니다. 이미지와
권한은 GitOps 가 주고, 잡은 멱등이라 언제든 다시 돌릴 수 있습니다.
