"""서버의 weekendExploreJob 을 k8s Job 으로 띄운다. 띄우고 기다리는 것은 flows/common/batch_job 이 한다.

대상 조건과 문구는 Team-Neki-Server docs/lld/notification-push/weekend-explore-job.md 에 있다.
세 발송 중 대상이 가장 많다(동의자 전원).
"""

from datetime import date

from prefect import task

from flows.common.batch_job import run_batch_job
from flows.weekly_reminder.job import TIMEOUT_SECONDS

JOB_NAME = "weekendExploreJob"


@task
def run_weekend_explore(cycle: date, *, run_at: str) -> bool:
    """발송 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False."""
    return run_batch_job(
        "weekend-explore",
        JOB_NAME,
        cycle,
        run_at=run_at,
        timeout_seconds=TIMEOUT_SECONDS,
        firebase=True,
    )
