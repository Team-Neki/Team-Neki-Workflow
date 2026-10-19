# Prefect 배포와 로컬 실행 메모

## serve.py vs. deploy.py

두 진입점은 용도가 다르며 섞어 쓰면 안 됩니다.

- `serve.py` : 로컬 개발용. 서버나 work pool 없이 한 프로세스로 즉시 확인함
- `deploy.py` : 운영용. 스케줄만 등록하고 실행은 worker가 담당함. 운영에서는 worker
  파드의 initContainer가 이미지 안에서 실행함

**운영 스케줄 등록은 반드시 `deploy.py`를 거쳐야 합니다.** `prefect deploy`나
`flow.deploy()`를 직접 호출하면 아래 보존 로직을 건너뜁니다.
## pause는 배포가 덮어씁니다

Prefect는 deployment를 등록할 때 기존 정의를 통째로 덮어씁니다. 따라서 운영자가
UI에서 꺼둔 스케줄이 배포할 때마다 되살아납니다. Airflow에서 DAG를 pause하면 그
상태가 메타DB에 남아 유지되는 것과 다릅니다.

재등록이 일어나는 시점은 다음과 같습니다.

- `serve.py` 재시작 : 매번 재등록하므로 프로세스가 뜰 때마다 풀림
- `deploy()` 재실행 : 배포할 때마다 풀림
- 이미지 태그 갱신으로 worker 파드가 재시작될 때 : initContainer가 `deploy.py`를
  돌리므로 재등록되지만 아래 보존 로직 덕에 pause는 유지됨
- Prefect 서버 재시작 : 유지됨 (DB에 남음)
- worker 재시작 : 유지됨 (worker는 정의를 건드리지 않음)

deployment의 `paused` 대신 스케줄의 `active`를 꺼도 마찬가지로 되살아납니다.
`deploy()`가 cron 인자를 그대로 다시 쓰므로 스케줄 객체가 통째로 교체되기
때문입니다.

`deploy.py`는 배포 전 `paused`와 각 스케줄의 `active`를 읽어두고 등록 payload 에 반영해
이를 막습니다. 반대로 UI에서 다시 켠 것을 배포가 도로 끄지도 않습니다. 이 파일을
수정할 때는 양방향이 모두 유지되는지 확인해야 합니다.
## 로컬 UI

기본 프로파일이 `ephemeral`이라 `PREFECT_API_URL`이 비어 있습니다. 이 상태로
`serve.py`를 실행하면 프로세스 안에서 임시 서버가 뜨고 종료와 함께 사라지므로
**UI로 접근할 수 없습니다.** UI를 보려면 서버를 따로 띄우고 API 주소를 지정해야
합니다.

터미널 두 개가 필요합니다. `make serve`가 `PREFECT_API_URL`을 대신 넣어줍니다.

```bash
make server
make serve
```

UI는 `http://127.0.0.1:4200`이고, deployment 목록은 `/deployments`입니다.
