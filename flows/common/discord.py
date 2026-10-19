"""flow 결과를 Discord webhook 으로 알린다.

주소는 `DISCORD_WEBHOOK_URL` 하나로 받는다. 비어 있으면 보내지 않고 넘어간다.
로컬에서 flow 를 돌릴 때마다 채널이 울리면 안 되기 때문이다.

어느 환경의 결과인지는 `SPRING_PROFILES_ACTIVE` 로 가른다. 서버와 같은 Secret
(prefect-workflow)의 같은 키라 환경 이름이 서버와 어긋날 일이 없다.

**알림 실패로 flow 를 실패시키지 않는다.** 적재는 이미 끝났는데 Discord 가 죽었다고
run 이 실패로 남으면 재시도가 적재를 되풀이한다. 실패는 경고로만 남긴다.
"""

import logging
import os
from typing import Any, Callable

import httpx
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"
PROFILE_ENV = "SPRING_PROFILES_ACTIVE"

# embed 왼쪽 띠 색. 성공 초록, 실패 빨강.
COLOR_OK = 0x2ECC71
COLOR_FAILED = 0xE74C3C

# Discord embed description 상한은 4096자다. 넘으면 400 으로 통째로 거절된다.
MAX_DESCRIPTION = 4000

TIMEOUT_SECONDS = 10


def _logger() -> Any:
    """run 안이면 Prefect 로거, 밖이면 모듈 로거. 훅과 테스트에서도 불린다."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def profile() -> str:
    """알림에 붙일 환경 이름. 비어 있으면 local 로 본다."""
    return os.environ.get(PROFILE_ENV) or "local"


def _truncate(text: str) -> str:
    if len(text) <= MAX_DESCRIPTION:
        return text
    return text[: MAX_DESCRIPTION - 4] + "\n..."


def notify(
    title: str, lines: list[str] | Callable[[], list[str]], *, ok: bool = True
) -> bool:
    """embed 하나를 보낸다. 보냈으면 True.

    webhook 이 없거나 발송이 실패하면 False 를 돌려주고 예외를 내지 않는다.
    lines 에 함수를 넘기면 여기서 부른다. 본문을 만들다 터져도 flow 로 새지 않게
    하려는 것이다. 알림은 적재가 커밋된 뒤에 불리므로 여기서 예외가 나가면 이미
    끝난 적재가 실패로 기록되고 재시도가 적재를 되풀이한다.
    """
    url = os.environ.get(WEBHOOK_ENV)
    if not url:
        _logger().info("%s 이 없어 Discord 알림을 건너뜁니다.", WEBHOOK_ENV)
        return False

    try:
        body = lines() if callable(lines) else lines
        payload: dict[str, Any] = {
            "embeds": [
                {
                    "title": f"[{profile()}] {title}",
                    "description": _truncate("\n".join(body)),
                    "color": COLOR_OK if ok else COLOR_FAILED,
                }
            ]
        }
        response = httpx.post(url, json=payload, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        # 오류 문자열에 webhook 주소(토큰 포함)가 들어가므로 상태 코드만 남긴다.
        _logger().warning("Discord 알림이 거절됐습니다: HTTP %d", error.response.status_code)
        return False
    except httpx.HTTPError as error:
        _logger().warning("Discord 알림을 보내지 못했습니다: %s", type(error).__name__)
        return False
    except Exception as error:
        # 본문 조립이나 잘못된 주소(httpx.InvalidURL 은 HTTPError 가 아니다) 등.
        # 메시지에 주소가 섞일 수 있어 종류만 남긴다.
        _logger().warning("Discord 알림 중 예외가 나 건너뜁니다: %s", type(error).__name__)
        return False
    return True


def notify_failure(flow: Any, flow_run: Any, state: Any) -> None:
    """`@flow(on_failure=..., on_crashed=...)` 훅. 실패한 run 과 그 사유를 알린다."""
    notify(
        f"{flow.name} 실패",
        [
            f"run: `{flow_run.name}`",
            f"상태: {state.type.value}",
            "```",
            str(state.message or "(메시지 없음)")[:1500],
            "```",
        ],
        ok=False,
    )
