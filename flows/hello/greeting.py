"""인사말 생성 task."""

from prefect import task


@task(retries=2, retry_delay_seconds=1)
def fetch_names() -> list[str]:
    return ["neki", "workflow", "prefect"]


@task
def greet(name: str, greeting: str) -> str:
    return f"{greeting}, {name}!"
