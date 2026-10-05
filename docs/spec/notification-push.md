# 알림 발송 실행(weekly-reminder, weekend-explore, holiday-explore) 정책

이 문서는 서버의 알림 발송 잡 3종(`weeklyReminderJob`, `weekendExploreJob`,
`holidayExploreJob`)을 Prefect 가 k8s Job 으로 띄우는 flow 3개의 정책을 다룹니다.
누구에게 무엇을 보내는지는 Team-Neki-Server `apps/batch` 의 것이고(BACKEND-135,
`docs/lld/notification-push/`), 여기서는 언제 무엇을 어떻게 띄우고 성패를 어디에
남기는지만 정합니다.

**이 문서가 정본입니다.** 코드와 이 문서가 어긋나면 문서가 맞고 코드가 틀린
것입니다. 동작을 바꾸려면 이 문서를 먼저 고치고 같은 PR 에서 코드를 맞춥니다.
항목마다 `구현` 줄이 구현 위치를 가리키며 `make spec-check` 가 확인합니다.

## 잡 하나에 flow 하나, deployment 하나

세 발송은 서버 쪽 청크 파이프라인이 같고 대상 조건과 스케줄만 다릅니다. 잡 이름이
곧 발송 종류이므로 flow 와 deployment 도 종류마다 둡니다. 한 flow 에 `job_name`
파라미터를 두면 Prefect UI 와 실패 알림에서 어느 발송인지 한 단계 더 들어가야
보이고, 종류를 더할 때 분기를 고치게 됩니다.

| flow / deployment | 서버 잡 | cron (Asia/Seoul) | 대상 |
|---|---|---|---|
| `weekly-reminder` | `weeklyReminderJob` | `0 20 * * *` 매일 20:00 | 7일 전 사진 업로드 동의자 |
| `weekend-explore` | `weekendExploreJob` | `0 18 * * 5,6,0` 금·토·일 18:00 | 동의자 전원 |
| `holiday-explore` | `holidayExploreJob` | `0 9 * * *` 매일 09:00 | 발송일이면 최근 1달 업로드 동의자, 아니면 0건 |

- `businessDate` 는 이 run 의 예약 시각(KST). 백필은 `target_date` 로 줌
- 잡은 멱등. 같은 `businessDate` 로 다시 돌려도 이미 보낸 유저는 서버의 중복
  판정(`notification_log`)이 걸러 발송이 0건 (BACKEND-128 의 `RunIdIncrementer` 계약)
- 동시 실행은 deployment 마다 `concurrency_limit=1`. 두 run 이 겹치면 서버의 중복
  판정이 커밋된 이력만 보므로 같은 유저에게 두 번 갈 수 있음. 종류가 다른 flow
  끼리는 중복 키가 달라 겹쳐도 됨
- `holiday-explore` 는 매일 뜨고 발송일 판정은 서버의 공휴일 CSV 가 함. 대부분의
  날은 0건으로 몇 초 만에 끝남
- 전환 중(Notification 앱이 아직 떠 있는 동안)에는 등록 직후 UI 에서 셋 다 pause.
  `deploy.py` 가 pause 를 보존하므로 다음 배포에 풀리지 않음

구현 : `flows/weekly_reminder/flow.py:19` `def weekly_reminder`,
`flows/weekend_explore/flow.py:19` `def weekend_explore`,
`flows/holiday_explore/flow.py:19` `def holiday_explore`,
`flows/weekly_reminder/flow.py:26` `cycle = target_date or cycle_date()`,
`deployments/weekly_reminder.py:21` `schedule=Cron("0 20 * * *", timezone="Asia/Seoul")`,
`deployments/weekend_explore.py:20` `schedule=Cron("0 18 * * 5,6,0", timezone="Asia/Seoul")`,
`deployments/holiday_explore.py:21` `schedule=Cron("0 9 * * *", timezone="Asia/Seoul")`,
`deployments/weekly_reminder.py:22` `concurrency_limit=1`

## Job 계약

`NEKI_BATCH_IMAGE` 이미지를 `--spring.batch.job.name=<잡> businessDate=<사이클>`
인자로 k8s Job 에 띄우고 완료를 기다립니다. search-index 와 같은 `flows/common/batch_job`
을 쓰며 다른 점은 둘입니다.

- Firebase 서비스계정 JSON 을 마운트함. Secret `prefect-workflow` 의
  `firebase-service-account.json` 키를 `/etc/firebase/firebase-service-account.json`
  에 파일로 둠. 서버의 `application-firebase.yaml` 이 staging/prod 프로파일에서 그
  경로를 읽음. search-index 는 이 키가 없어도 떠야 하므로 `firebase=True` 일 때만
  볼륨을 붙임
- 타임아웃이 3,600초. 발송은 청크 1건마다 FCM 왕복과 커밋이라 대상 수에 비례함.
  대상 1만 명이면 수십 분. 이 상한에 걸리기 시작하면 서버 쪽이 `sendEach` 로
  묶을 차례. 세 flow 가 같은 값을 씀

나머지는 search-index 와 같습니다.

- 이미지는 GitOps `overlays/prefect/images.env` (ConfigMap `neki-images`).
  Team-Neki-Server 의 deploy-batch 가 그 줄을 갱신함
- Job 이름은 `<flow 이름>-<실행 시각>-<UUID hex>` 를 63자에서 자른 것. prefix 가
  search-index 보다 길어 UUID 뒤쪽이 몇 자 잘리지만 28자 이상 남아 같은 초의
  실행도 겹치지 않음
- env 는 `TZ`, 그리고 Secret `prefect-workflow` 의 `SPRING_PROFILES_ACTIVE`,
  `JASYPT_PASSWORD` 둘만. 어느 앱 DB 를 보는지가 `SPRING_PROFILES_ACTIVE` 로 정해짐
- `backoffLimit 0`, `restartPolicy Never`. 재시도는 사람이 flow 를 다시 돌려서
- 완료된 Job 은 `ttlSecondsAfterFinished` 로 하루 뒤 정리. 성공한 파드의 로그는
  대기 중에 읽어 flow 로그에 남김
- Job 에도 `activeDeadlineSeconds` 3,600초. flow 타임아웃으로 끝난 뒤 남은 Job 이
  재실행한 Job 과 겹치지 않게 같은 상한에서 k8s 가 파드를 끝냄. 보낸 만큼은
  서버가 건별로 커밋해 두었으므로 재실행하면 나머지만 보냄

구현 : `flows/weekly_reminder/job.py:17` `TIMEOUT_SECONDS = 3600`,
`flows/weekly_reminder/job.py:29` `firebase=True`,
`flows/common/batch_job.py:43` `FIREBASE_SECRET_KEY`,
`flows/common/batch_job.py:93` `if firebase:`,
`flows/common/batch_job.py:72` `[:MAX_NAME_LENGTH]`

## 외부 의존과 장애

| 의존 | 없거나 죽었을 때 |
|---|---|
| `NEKI_BATCH_IMAGE` 없음 | 경고 후 끝남. flow 성공 |
| Secret 에 `firebase-service-account.json` 키 없음 | 파드가 `CreateContainerConfigError` 로 안 떠 타임아웃까지 기다린 뒤 flow 실패 |
| 키 파일은 있으나 서버가 FCM 을 못 씀 | 서버가 첫 발송에서 잡을 FAILED 로 끝냄(종료 코드 1). flow 실패 |
| 토큰 무효 등 건별 FCM 실패 | 서버가 그 건을 FAILED 로 적재하고 계속. Job 성공, flow 성공 |
| 잡 실패 (종료 코드 1) | Job Failed 로 flow 실패. 파드 로그를 봄 |
| 대상이 많아 3,600초 초과 | flow 실패. 서버가 건별 커밋해 둔 만큼은 보내졌고 재실행하면 나머지만 |

## 변경 검증

- `make check` : 임포트, deployment 수집(3개 추가), anchor, pytest (매니페스트와 스케줄)
- 로컬 실행은 `NEKI_BATCH_IMAGE` 가 없어 경고 후 끝나는 것이 정상. Job 을 실제로 띄우는
  확인은 staging 에서 `prefect deployment run weekend-explore/weekend-explore` 로
  하고 `kubectl -n prefect get jobs` 에 `weekend-explore-<시각>` 이 생기는지,
  flow 로그에 batch 파드 로그가 보이는지 봄

## 정리

알림 발송 3종은 search-index 와 같은 방식으로 서버 batch 를 k8s Job 으로 띄우고
종료 코드를 flow 결과로 삼습니다. 종류마다 flow 와 deployment 를 두어 이름으로
구분하고, Firebase 키 마운트와 긴 타임아웃만 다릅니다. 스케줄은 여기 있고 발송
규칙은 서버에 있습니다.
