"""deployments/ 의 스케줄을 Prefect API에 등록한다.

실행은 하지 않는다. 워크플로를 실제로 돌리는 것은 work pool에 붙은
worker이고, 이 스크립트는 정의만 갱신한다.

등록은 기존 deployment를 덮어쓴다. 그래서 운영자가 UI에서 꺼둔 스케줄이
배포할 때마다 되살아난다. Prefect는 이를 보존해주지 않으므로 배포 전
상태를 읽어두고 배포 후 되돌린다.

사용법:
    PREFECT_WORK_POOL=neki-pool python deploy.py
"""

import argparse
import asyncio
import os
from dataclasses import dataclass, field

from prefect.client.orchestration import get_client
from prefect.deployments.runner import RunnerDeployment
from prefect.exceptions import ObjectNotFound

from deployments import collect


@dataclass
class ScheduleState:
    """배포 전 스케줄 활성 상태. 슬러그가 없으면 순서로 대응시킨다."""

    paused: bool = False
    schedule_active: dict[str | int, bool] = field(default_factory=dict)

    @property
    def has_disabled(self) -> bool:
        return self.paused or not all(self.schedule_active.values())


def full_name(deployment: RunnerDeployment) -> str:
    return f"{deployment.flow_name}/{deployment.name}"


async def read_states(names: list[str]) -> dict[str, ScheduleState]:
    """배포 전 pause 상태를 읽는다. 아직 없는 deployment는 건너뛴다."""
    states: dict[str, ScheduleState] = {}

    async with get_client() as client:
        for name in names:
            try:
                existing = await client.read_deployment_by_name(name)
            except ObjectNotFound:
                continue

            states[name] = ScheduleState(
                paused=existing.paused,
                schedule_active={
                    schedule.slug or index: schedule.active
                    for index, schedule in enumerate(existing.schedules)
                },
            )

    return states


async def restore_states(states: dict[str, ScheduleState]) -> list[str]:
    """배포로 되살아난 pause 상태를 원래대로 되돌린다."""
    restored: list[str] = []

    async with get_client() as client:
        for name, state in states.items():
            if not state.has_disabled:
                continue

            current = await client.read_deployment_by_name(name)

            if state.paused:
                await client.pause_deployment(current.id)

            for index, schedule in enumerate(current.schedules):
                key = schedule.slug or index
                # 배포 전에 없던 스케줄은 켜진 채로 둔다.
                if state.schedule_active.get(key, True):
                    continue

                await client.update_deployment_schedule(
                    current.id, schedule.id, active=False
                )

            restored.append(name)

    return restored


def resolve_pool(deployment: RunnerDeployment, default_pool: str | None) -> str:
    pool = deployment.work_pool_name or default_pool
    if not pool:
        raise SystemExit(
            f"{full_name(deployment)}: work pool이 지정되지 않았습니다. "
            "PREFECT_WORK_POOL 환경변수를 설정하거나 build()에서 "
            "work_pool_name을 넘기세요."
        )
    return pool


def main() -> None:
    parser = argparse.ArgumentParser(description="deployments/ 를 Prefect API에 등록")
    parser.add_argument(
        "--pool",
        default=os.environ.get("PREFECT_WORK_POOL"),
        help="기본 work pool. build()가 지정한 pool이 우선한다.",
    )
    args = parser.parse_args()

    found = list(collect())
    if not found:
        raise SystemExit("등록할 deployment가 없습니다.")

    names = [full_name(deployment) for deployment in found]
    states = asyncio.run(read_states(names))

    for deployment in found:
        deployment.apply(work_pool_name=resolve_pool(deployment, args.pool))
        print(f"등록: {full_name(deployment)}")

    restored = asyncio.run(restore_states(states))
    for name in restored:
        print(f"pause 상태 복원: {name}")


if __name__ == "__main__":
    main()
