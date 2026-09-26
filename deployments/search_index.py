"""검색 색인 잡 실행 deployment.

지금은 스케줄이 없다. Team-Neki-Server 의 searchIndexJob(BACKEND-65)이 아직 없어
정기로 돌리면 배치가 종료 코드 1 로 죽어 매일 실패한다. 65 가 들어오면
`schedule=Cron("30 5 * * *", timezone="Asia/Seoul")` 을 붙인다. enrich(05:00 KST)
뒤 30분이고, 그 시점의 tb_photo_booth_enriched 현재 세대를 색인한다.

스케줄이 없어도 UI 에 남아 실행 버튼이 동작하므로 수동 실행과 배관 확인에 쓴다.
GitOps(BACKEND-143)가 아직이면 NEKI_BATCH_IMAGE 가 없어 경고 후 끝난다.

flow run 파드가 Job 을 만들려면 SA prefect-worker 가 필요하다. 그 값은 GitOps 의
base job template 기본값에 있어(BACKEND-143) deployment 마다 지정하지 않는다.
"""

from prefect.deployments.runner import RunnerDeployment

from flows.search_index import search_index


def build() -> RunnerDeployment:
    return search_index.to_deployment(name="search-index")
