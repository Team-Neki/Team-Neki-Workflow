"""서버의 weeklyReminderJob 을 k8s Job 으로 띄운다. 띄우고 기다리는 것은 flows/common/batch_job 이 한다.

대상 조건과 문구는 Team-Neki-Server docs/lld/notification-push/weekly-reminder-job.md 에 있다.
"""

from datetime import date

from prefect import task

from flows.common.batch_job import run_batch_job

JOB_NAME = "weeklyReminderJob"

# 발송은 청크 1건마다 FCM 왕복과 커밋이라 대상 수에 비례한다. 색인(1,800초)보다 길게 둔다.
# 대상 1만 명이면 수십 분이다. 이 상한에 걸리기 시작하면 서버 쪽이 sendEach 로 묶을 차례다.
# 알림 발송 3종이 같은 값을 쓴다 (weekend_explore, holiday_explore 가 여기서 가져간다).
TIMEOUT_SECONDS = 3600


@task
def run_weekly_reminder(cycle: date, *, run_at: str) -> bool:
    """발송 Job 을 띄우고 끝나기를 기다린다. 띄웠으면 True, 이미지가 없어 건너뛰면 False."""
    return run_batch_job(
        "weekly-reminder",
        JOB_NAME,
        cycle,
        run_at=run_at,
        timeout_seconds=TIMEOUT_SECONDS,
        firebase=True,
    )
