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

구현 : `flows/search_index/flow.py:25` `def search_index`,
`flows/search_index/flow.py:33` `cycle = target_date or cycle_date()`,
`deployments/search_index.py:21` `to_deployment(name="search-index")`

## Job 계약

`NEKI_BATCH_IMAGE` 이미지를 `--spring.batch.job.name=searchIndexJob
businessDate=<사이클>` 인자로 k8s Job 에 띄우고 완료를 기다립니다. Job 실패는
flow 실패입니다. 환경변수가 없으면 경고 후 끝납니다. 로컬이거나 GitOps 가 아직인
경우입니다.

- 이미지는 GitOps `overlays/prefect/images.env` (ConfigMap `neki-images`, BACKEND-143).
  Team-Neki-Server 의 deploy-batch 가 그 줄을 갱신함
- Job 이름은 `search-index-<실행 시각>`. prefect-kubernetes 가 `metadata.name` 으로
  상태를 읽으므로 `generateName` 은 못 씀. 실행 시각의 밑줄은 하이픈으로
- env 는 `TZ`, 그리고 Secret `prefect-workflow` 의 `SPRING_PROFILES_ACTIVE`,
  `JASYPT_PASSWORD` 둘만. Secret 을 통째로 넘기지 않음
- `backoffLimit 0`, `restartPolicy Never`. 재시도는 k8s 가 아니라 사람이 flow 를
  다시 돌려서
- 완료된 Job 은 지우지 않고 `ttlSecondsAfterFinished` 로 하루 뒤 정리. 실패 파드의
  로그가 원인임. 성공한 파드의 로그는 대기 중에 읽어 flow 로그에 남김
- 타임아웃 1,800초. 파드가 안 뜨는 경우(이미지 없음)에 무한정 기다리지 않게
- flow run 파드는 SA `prefect-worker`. base job template 기본값이라 deployment 는
  지정하지 않음. Role 이 jobs 생성과 pods/log 조회를 허용함

구현 : `flows/search_index/job.py:34` `IMAGE_ENV`,
`flows/search_index/job.py:53` `def manifest`,
`flows/search_index/job.py:111` `def run_search_index`

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
- staging 에서 `search-index` 를 수동 실행해 `kubectl -n prefect get jobs` 에
  `search-index-<시각>` 이 생기고 flow 로그에 batch 파드 로그가 보이는지 봄

## 정리

search-index 는 enrich 와 무관하게 정해진 시각에 그 시점의 현재 세대를 색인하도록
서버 batch 를 k8s Job 으로 띄우고 종료 코드를 flow 결과로 삼습니다. 이미지와
권한은 GitOps 가 주고, 잡은 멱등이라 언제든 다시 돌릴 수 있습니다.
