"""Postgres 연결 공용 헬퍼.

접속 정보는 `DATABASE_URL` 하나로 받는다. 호스트와 포트, 사용자를 따로 나누지
않는 이유는 `KAKAO_API_KEY`나 `S3_BUCKET`과 같다. 로컬과 운영의 차이가 환경변수
하나여야 코드에 분기가 생기지 않는다.

Prefect Block 을 쓰지 않는다. Block 은 UI 에서 값을 바꿀 수 있어 편하지만
설정이 Prefect 서버 상태에 얹히므로, 서버를 갈아치우거나 다른 환경에서 같은
flow 를 돌릴 때 값이 따라오지 않는다. 환경변수는 worker 를 띄우는 쪽이 들고
있으므로 그럴 일이 없다.
"""

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg

DSN_ENV = "DATABASE_URL"


def dsn() -> str:
    """접속 문자열을 돌려준다. 없으면 막는다.

    없는 채로 진행하면 psycopg 가 로컬 소켓에 붙으려 하다 엉뚱한 오류를 내므로,
    무엇이 빠졌는지 여기서 말해준다.
    """
    value = os.environ.get(DSN_ENV)
    if not value:
        raise RuntimeError(
            f"{DSN_ENV} 환경변수가 없습니다. 로컬은 .env.example 을 복사해 값을 "
            "채우고, 운영은 worker 환경변수로 넣으세요."
        )
    return value


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    """트랜잭션 하나를 감싼 연결을 넘긴다.

    `autocommit` 을 켜지 않는다. psycopg3 는 `with` 블록을 벗어날 때 커밋하고
    예외가 나면 롤백하므로, 적재를 통째로 되돌릴 수 있는 단위가 그대로 생긴다.
    """
    with psycopg.connect(dsn()) as connection:
        yield connection
