"""색인 Job 매니페스트의 순수한 부분만 확인한다. k8s 는 부르지 않는다."""

from datetime import date
import re

import pytest

from deployments.search_index import build
from flows.search_index import job
from flows.search_index.job import TIMEOUT_SECONDS, manifest


def test_manifest_name_is_a_valid_k8s_name_and_args_follow_the_contract():
    m = manifest("ghcr.io/team-neki/neki-batch:x", date(2026, 9, 25), run_at="2026-09-25_053012")
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


def test_same_second_runs_get_distinct_job_names():
    names = {
        manifest("image", date(2026, 9, 25), run_at="2026-09-25_053012")["metadata"]["name"]
        for _ in range(10)
    }
    assert len(names) == 10


@pytest.mark.parametrize("run_at", ["x" * 100, "2026-09-25_053012/invalid", ""])
def test_invalid_run_timestamp_is_rejected(run_at):
    with pytest.raises(ValueError, match="run_at"):
        manifest("image", date(2026, 9, 25), run_at=run_at)


def test_deployment_is_serial():
    deployment = build()
    assert deployment.name == "search-index"
    assert deployment.concurrency_limit == 1


def test_staging_batch_job_uses_its_pod_namespace(tmp_path, monkeypatch):
    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("prefect-stg\n")
    monkeypatch.setattr(job, "NAMESPACE_FILE", namespace_file)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    m = manifest("image", date(2026, 9, 25), run_at="2026-09-25_053012")
    assert m["metadata"]["namespace"] == "prefect-stg"

    namespace_file.unlink()
    with pytest.raises(FileNotFoundError):
        manifest("image", date(2026, 9, 25), run_at="2026-09-25_053012")
