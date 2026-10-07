"""서버의 holidayExploreJob 을 k8s Job 으로 띄운다. 띄우고 기다리는 것은 flows/common/batch_job 이 한다.

대상 조건과 문구, 공휴일 CSV 는 Team-Neki-Server docs/lld/notification-push/holiday-explore-job.md 에
있다. 매일 띄우되 businessDate 가 발송일이 아니면 서버가 0건으로 끝낸다(종료 코드 0).
"""

from datetime import date

from prefect import task

from flows.common.batch_job import run_batch_job
from flows.weekly_reminder.job import TIMEOUT_SECONDS

JOB_NAME = "holidayExploreJob"


@task
def run_holiday_explore(cycle: date, *, run_at: str) -> bool:
    """발송 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False."""
    return run_batch_job(
        "holiday-explore",
        JOB_NAME,
        cycle,
        run_at=run_at,
        timeout_seconds=TIMEOUT_SECONDS,
        firebase=True,
    )
