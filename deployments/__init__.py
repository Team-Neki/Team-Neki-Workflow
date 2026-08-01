"""스케줄 정의. flow 하나당 파일 하나로 두고 flows/와 파일명을 맞춘다.

각 모듈은 RunnerDeployment를 반환하는 build()를 노출해야 한다.
serve.py와 deploy.py가 이 규약으로 모듈을 찾아간다.
"""

import importlib
import pkgutil
from collections.abc import Iterator

from prefect.deployments.runner import RunnerDeployment


def collect() -> Iterator[RunnerDeployment]:
    """deployments/ 아래 모든 모듈의 build() 결과를 모은다."""
    for module_info in pkgutil.iter_modules(__path__):
        module = importlib.import_module(f"{__name__}.{module_info.name}")

        build = getattr(module, "build", None)
        if build is None:
            raise AttributeError(
                f"{__name__}.{module_info.name} 에 build() 가 없습니다. "
                "RunnerDeployment 를 반환하는 build() 를 정의하세요."
            )

        yield build()
