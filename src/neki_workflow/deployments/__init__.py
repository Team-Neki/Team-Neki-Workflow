"""스케줄 정의. 도메인별 패키지의 build()를 모두 수집한다.

각 모듈은 RunnerDeployment를 반환하는 build()를 노출해야 한다.
serve.py와 deploy.py가 이 규약으로 모듈을 찾아간다.
"""

import importlib
import pkgutil
from collections.abc import Iterator

from prefect.deployments.runner import RunnerDeployment


def collect() -> Iterator[RunnerDeployment]:
    """하위 패키지를 포함해 모든 deployment 모듈을 수집한다."""
    for module_info in pkgutil.walk_packages(__path__, prefix=f"{__name__}."):
        if module_info.ispkg:
            continue
        module = importlib.import_module(module_info.name)

        build = getattr(module, "build", None)
        if build is None:
            raise AttributeError(
                f"{module_info.name} 에 build() 가 없습니다. "
                "RunnerDeployment 를 반환하는 build() 를 정의하세요."
            )

        yield build()
