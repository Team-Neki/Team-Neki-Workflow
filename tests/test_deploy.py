"""재등록의 부분 실패에도 기존 pause/active가 풀리지 않는지 확인한다."""

from unittest.mock import AsyncMock

import pytest
from prefect.deployments.runner import RunnerDeployment
from prefect.client.schemas.actions import DeploymentScheduleCreate
from prefect.client.schemas.schedules import CronSchedule

import deploy
from deployments.search_index import build


@pytest.mark.parametrize("environment", ["prod", "stg"])
@pytest.mark.parametrize("paused", [False, True])
def test_registration_preserves_state_before_a_later_failure(monkeypatch, environment, paused):
    first, second = build(), build()
    second.name = "second"
    suffix = "-stg" if environment == "stg" else ""
    name = f"search-index/search-index{suffix}"
    monkeypatch.setattr("sys.argv", ["deploy.py", "--environment", environment, "--pool", "test"])
    monkeypatch.setattr(deploy, "collect", lambda: iter([first, second]))
    monkeypatch.setattr(deploy, "read_states", AsyncMock(return_value={
        name: deploy.ScheduleState(paused=paused, schedule_active={0: paused}),
    }))
    applied = []

    def apply(deployment, **kwargs):
        if applied:
            raise RuntimeError("second registration failed")
        applied.append((deploy.full_name(deployment), deployment.model_dump(exclude_unset=True)))

    monkeypatch.setattr(RunnerDeployment, "apply", apply)
    with pytest.raises(RuntimeError, match="second registration failed"):
        deploy.main()
    assert applied[0][0] == name
    assert applied[0][1]["paused"] is paused
    assert applied[0][1]["schedules"][0]["active"] is paused


@pytest.mark.parametrize("environment,paused", [("prod", False), ("stg", True)])
def test_new_deployment_pause_depends_on_environment(monkeypatch, environment, paused):
    monkeypatch.setattr("sys.argv", ["deploy.py", "--environment", environment, "--pool", "test"])
    monkeypatch.setattr(deploy, "collect", lambda: iter([build()]))
    monkeypatch.setattr(deploy, "read_states", AsyncMock(return_value={}))
    applied = []
    monkeypatch.setattr(RunnerDeployment, "apply", lambda d, **kw: applied.append(d.paused))
    deploy.main()
    assert applied == [paused]


def test_schedule_slug_survives_reordering_and_new_schedule_keeps_default():
    deployment = build()

    deployment.schedules = [
        DeploymentScheduleCreate(schedule=CronSchedule(cron="0 1 * * *"), slug="second"),
        DeploymentScheduleCreate(schedule=CronSchedule(cron="0 2 * * *"), slug="first"),
        DeploymentScheduleCreate(schedule=CronSchedule(cron="0 3 * * *"), slug="new", active=False),
    ]
    deploy.preserve_state(deployment, deploy.ScheduleState(schedule_active={"first": False, "second": True}))
    assert [s.active for s in deployment.schedules] == [True, False, False]
