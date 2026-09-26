"""검색 색인 대기는 Pod 생성 여부와 무관하게 실제 경과 시간으로 제한한다."""

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import prefect_kubernetes.jobs as kubernetes_jobs
from prefect_kubernetes.jobs import KubernetesJob, KubernetesJobRun

from flows.search_index import job


def test_no_pods_wait_times_out_and_is_cancelled(monkeypatch):
    """라이브러리의 active 없는 분기를 실제로 돌려 취소까지 확인한다."""
    definition = KubernetesJob(
        v1_job=job.manifest("image", date(2026, 9, 27), run_at="2026-09-27_053000"),
        timeout_seconds=1800,
        delete_after_completion=False,
    )
    run = KubernetesJobRun(definition, definition.v1_job)
    reads = []

    async def read_job(**kwargs):
        reads.append(kwargs)
        await asyncio.sleep(0.001)
        return SimpleNamespace(
            spec=SimpleNamespace(
                template=SimpleNamespace(
                    metadata=SimpleNamespace(labels={"controller-uid": "test"})
                )
            ),
            status=SimpleNamespace(active=None, conditions=None),
        )

    read = SimpleNamespace(fn=read_job)
    pods = SimpleNamespace(fn=AsyncMock(return_value=SimpleNamespace(items=[])))
    monkeypatch.setattr(kubernetes_jobs, "read_namespaced_job", read)
    monkeypatch.setattr(kubernetes_jobs, "list_namespaced_pod", pods)
    monkeypatch.setattr(job, "TIMEOUT_SECONDS", 0.03)

    async def verify():
        started = asyncio.get_running_loop().time()
        with pytest.raises(TimeoutError):
            # 바깥쪽 상한으로 테스트 자체의 무한 대기도 막는다.
            await asyncio.wait_for(job._wait_for_completion(run), timeout=1)
        assert asyncio.get_running_loop().time() - started < 0.5
        assert reads
        assert not run._completed
        count = len(reads)
        await asyncio.sleep(0.01)
        assert len(reads) == count  # 백그라운드에 폴링을 남기지 않는다.

    asyncio.run(verify())


def test_successful_wait_streams_logs():
    """완료한 Job은 로그 출력 콜백을 받은 뒤 정상 종료한다."""
    run = SimpleNamespace(await_for_completion=AsyncMock())
    asyncio.run(job._wait_for_completion(run))
    run.await_for_completion.assert_awaited_once_with(print_func=print)


def test_failed_job_error_is_preserved():
    """서버 배치 실패를 타임아웃으로 바꾸지 않는다."""
    run = SimpleNamespace(
        await_for_completion=AsyncMock(side_effect=RuntimeError("failed"))
    )
    with pytest.raises(RuntimeError, match="failed"):
        asyncio.run(job._wait_for_completion(run))


def test_task_timeout_fails_without_deleting_job(monkeypatch):
    """대기 시간 초과가 동기 task까지 전파되고 Job은 조사용으로 남는다."""

    async def never_complete(**kwargs):
        await asyncio.sleep(10)

    run = SimpleNamespace(await_for_completion=never_complete)
    definition = SimpleNamespace(
        v1_job={"metadata": {"name": "test-job"}}, trigger=Mock(return_value=run)
    )
    factory = Mock(return_value=definition)
    monkeypatch.setenv(job.IMAGE_ENV, "test-image")
    monkeypatch.setattr(job, "KubernetesJob", factory)
    monkeypatch.setattr(job, "KubernetesCredentials", Mock())
    monkeypatch.setattr(job, "get_run_logger", Mock)
    monkeypatch.setattr(job, "TIMEOUT_SECONDS", 0)
    with pytest.raises(TimeoutError):
        job.run_search_index.fn(date(2026, 9, 27), run_at="2026-09-27_053000")
    assert factory.call_args.kwargs["delete_after_completion"] is False
