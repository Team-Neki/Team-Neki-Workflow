"""색인 Job 매니페스트의 순수한 부분만 확인한다. k8s 는 부르지 않는다."""

from datetime import date

from flows.search_index.job import manifest


def test_manifest_name_is_a_valid_k8s_name_and_args_follow_the_contract():
    m = manifest("ghcr.io/team-neki/neki-batch:x", date(2026, 9, 25), run_at="2026-09-25_053012")
    assert m["metadata"]["name"] == "search-index-2026-09-25-053012"
    assert "_" not in m["metadata"]["name"]
    container = m["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == [
        "--spring.batch.job.name=searchIndexJob",
        "businessDate=2026-09-25",
    ]
    assert m["spec"]["backoffLimit"] == 0
    assert {e["name"] for e in container["env"]} == {
        "TZ",
        "SPRING_PROFILES_ACTIVE",
        "JASYPT_PASSWORD",
    }
