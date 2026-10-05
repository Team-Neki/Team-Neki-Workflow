"""색인 Job 매니페스트의 순수한 부분만 확인한다. k8s 는 부르지 않는다."""

from datetime import date
import re

import pytest

from deployments.search_index import build
from flows.common.batch_job import manifest
from flows.search_index.job import JOB_NAME, TIMEOUT_SECONDS


def search_manifest(image: str = "ghcr.io/team-neki/neki-batch:x", run_at: str = "2026-09-25_053012"):
    return manifest(
        "search-index", JOB_NAME, image, date(2026, 9, 25), run_at=run_at, timeout_seconds=TIMEOUT_SECONDS
    )


def test_manifest_name_is_a_valid_k8s_name_and_args_follow_the_contract():
    m = search_manifest()
    assert re.fullmatch(r"search-index-2026-09-25-053012-[0-9a-f]{32}", m["metadata"]["name"])
    assert len(m["metadata"]["name"]) == 63
    assert "_" not in m["metadata"]["name"]
    container = m["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == [
        "--spring.batch.job.name=searchIndexJob",
        "businessDate=2026-09-25",
    ]
    assert m["spec"]["backoffLimit"] == 0
    assert m["spec"]["activeDeadlineSeconds"] == TIMEOUT_SECONDS
    assert {e["name"] for e in container["env"]} == {
        "TZ",
        "SPRING_PROFILES_ACTIVE",
        "JASYPT_PASSWORD",
    }
    # 색인은 Firebase 가 필요 없다. 키가 없는 Secret 에도 떠야 한다.
    assert "volumeMounts" not in container
    assert "volumes" not in m["spec"]["template"]["spec"]


def test_same_second_runs_get_distinct_job_names():
    names = {search_manifest(image="image")["metadata"]["name"] for _ in range(10)}
    assert len(names) == 10


@pytest.mark.parametrize("run_at", ["x" * 100, "2026-09-25_053012/invalid", ""])
def test_invalid_run_timestamp_is_rejected(run_at):
    with pytest.raises(ValueError, match="run_at"):
        search_manifest(image="image", run_at=run_at)


def test_deployment_is_serial():
    deployment = build()
    assert deployment.name == "search-index"
    assert deployment.concurrency_limit == 1
