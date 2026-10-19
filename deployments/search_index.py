"""검색 색인 잡 실행 deployment.

매일 05:30 KST 에 돈다. stores-enrich(05:00 KST) 뒤 30분이고, 그 시점의
tb_photo_booth_enriched 현재 세대를 색인한다. Prefect cron 은 timezone 을 주지
않으면 UTC 라 명시한다.

스케줄은 여기서만 바꾼다. UI 에서 붙인 스케줄은 worker 롤링 때 deploy.py 가
deployment 를 다시 등록하면서 지워진다 (BACKEND-244 에서 임시 서버로 확인). UI 에서
끈 것은 deploy.py 가 되살리지 않는다. 수동 실행은 UI 실행 버튼으로 한다.

동시 실행은 concurrency_limit=1 로 막는다. 색인 잡은 build 와 swap 이 서로 다른
트랜잭션이라, 두 실행의 build 가 swap 보다 먼저 끝나면 두 번째 swap 이 첫 번째를
되돌려 직전 세대를 서빙한다. 둘 다 COMPLETED 라 알림도 없다.
NEKI_BATCH_IMAGE 가 없으면(로컬 등) 경고 후 끝난다.

flow run 파드가 Job 을 만들려면 SA prefect-worker 가 필요하다. 그 값은 GitOps 의
base job template 기본값에 있어(BACKEND-143) deployment 마다 지정하지 않는다.
"""

from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron

from flows.search_index import search_index


def build() -> RunnerDeployment:
    return search_index.to_deployment(
        name="search-index",
        concurrency_limit=1,
        schedule=Cron("30 5 * * *", timezone="Asia/Seoul"),
    )
