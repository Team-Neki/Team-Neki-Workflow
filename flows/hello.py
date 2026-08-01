"""Prefect 동작 확인용 최소 워크플로."""

from prefect import flow, get_run_logger, unmapped

from tasks.hello.greeting import fetch_names, greet


@flow(name="hello", log_prints=True)
def hello(greeting: str = "Hello") -> list[str]:
    logger = get_run_logger()

    names = fetch_names()
    greetings = greet.map(names, greeting=unmapped(greeting)).result()

    for line in greetings:
        logger.info(line)

    return greetings
