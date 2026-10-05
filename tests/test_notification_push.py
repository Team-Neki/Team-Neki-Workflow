"""알림 발송 flow 3종의 매니페스트와 스케줄의 순수한 부분만 확인한다. k8s 는 부르지 않는다."""

from datetime import date

import pytest

from deployments import holiday_explore, weekend_explore, weekly_reminder
from flows.common.batch_job import FIREBASE_MOUNT_PATH, FIREBASE_SECRET_KEY, SECRET, manifest
from flows.holiday_explore.job import JOB_NAME as HOLIDAY_JOB
from flows.weekend_explore.job import JOB_NAME as WEEKEND_JOB
from flows.weekly_reminder.job import JOB_NAME as WEEKLY_JOB
from flows.weekly_reminder.job import TIMEOUT_SECONDS

CASES = [
    (weekly_reminder, "weekly-reminder", WEEKLY_JOB, "weeklyReminderJob", "0 20 * * *"),
    (weekend_explore, "weekend-explore", WEEKEND_JOB, "weekendExploreJob", "0 18 * * 5,6,0"),
    (holiday_explore, "holiday-explore", HOLIDAY_JOB, "holidayExploreJob", "0 9 * * *"),
]


@pytest.mark.parametrize("module, name, job_name, expected_job, cron", CASES)
def test_manifest_runs_the_right_job_with_firebase_mounted(module, name, job_name, expected_job, cron):
    m = manifest(
        name,
        job_name,
        "img",
        date(2026, 10, 6),
        run_at="2026-10-06_200000",
        timeout_seconds=TIMEOUT_SECONDS,
        firebase=True,
    )
    assert len(m["metadata"]["name"]) <= 63
    assert m["metadata"]["name"].startswith(f"{name}-2026-10-06-200000-")
    pod = m["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["args"] == [f"--spring.batch.job.name={expected_job}", "businessDate=2026-10-06"]
    assert container["volumeMounts"] == [
        {"name": "firebase-credentials", "mountPath": FIREBASE_MOUNT_PATH, "readOnly": True}
    ]
    assert pod["volumes"][0]["secret"] == {
        "secretName": SECRET,
        "items": [{"key": FIREBASE_SECRET_KEY, "path": FIREBASE_SECRET_KEY}],
    }
    assert m["spec"]["activeDeadlineSeconds"] == TIMEOUT_SECONDS


@pytest.mark.parametrize("module, name, job_name, expected_job, cron", CASES)
def test_deployment_is_serial_and_scheduled_in_kst(module, name, job_name, expected_job, cron):
    deployment = module.build()
    assert deployment.name == name
    assert deployment.concurrency_limit == 1
    [schedule] = deployment.schedules
    assert schedule.schedule.cron == cron
    assert schedule.schedule.timezone == "Asia/Seoul"


def test_push_jobs_wait_longer_than_search_index():
    from flows.search_index.job import TIMEOUT_SECONDS as SEARCH_TIMEOUT

    assert TIMEOUT_SECONDS > SEARCH_TIMEOUT
