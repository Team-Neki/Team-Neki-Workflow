"""Kakao 장소검색 공용 클라이언트.

한 질의로 꺼낼 수 있는 문서는 45건까지다. 지점이 그보다 많은 브랜드는 질의를
쪼개야 하는데, 행정구역 이름으로 쪼개면 개편을 따라다녀야 한다. 대신 좌표
사각형(rect)을 넷으로 나눠 재귀한다. 이름을 몰라도 되고 지점이 늘어도 알아서
깊어진다.

잘렸는지는 `total_count > pageable_count`로 판정한다. total_count는 상한과
무관하게 실제 개수를 알려주므로, 다 받았는지 대조하는 기준으로도 쓴다.
"""

import os
from typing import Any

import httpx
from prefect import get_run_logger, task

SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

API_KEY_ENV = "KAKAO_API_KEY"

# Local API 사양. size는 최대 15, 한 질의로 노출되는 문서는 최대 45건이다.
PAGE_SIZE = 15
MAX_EXPOSED = 45

# 대한민국을 넉넉히 덮는 사각형. (서경, 남위, 동경, 북위)
KOREA = (124.5, 33.0, 132.0, 38.7)

# 사각형이 계속 잘릴 때 멈추는 깊이. 한 점에 45곳이 몰려 있으면 아무리 쪼개도
# 벗어나지 못하므로 무한 재귀를 막는다. 8단계면 한 변이 대한민국의 1/256이다.
MAX_DEPTH = 8

Rect = tuple[float, float, float, float]


def api_key() -> str:
    key = os.environ.get(API_KEY_ENV)
    if not key:
        raise RuntimeError(
            f"{API_KEY_ENV} 환경변수가 없습니다. Kakao Developers에서 REST API 키를 "
            "발급받아 .env 또는 배포 환경에 넣으세요."
        )
    return key


def quarters(rect: Rect) -> list[Rect]:
    """사각형을 넷으로 나눈다. 경계가 겹치지만 id로 중복을 거르므로 무해하다."""
    x1, y1, x2, y2 = rect
    mx, my = (x1 + x2) / 2, (y1 + y2) / 2
    return [
        (x1, y1, mx, my),
        (mx, y1, x2, my),
        (x1, my, mx, y2),
        (mx, my, x2, y2),
    ]


@task(retries=3, retry_delay_seconds=[2, 5, 10])
def search_page(
    query: str,
    *,
    page: int = 1,
    rect: Rect | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    """장소검색 한 페이지를 받아온다.

    키가 로그에 남지 않도록 헤더로만 넘긴다.
    """
    params: dict[str, Any] = {"query": query, "page": page, "size": PAGE_SIZE}
    if rect:
        params["rect"] = ",".join(str(value) for value in rect)

    response = httpx.get(
        SEARCH_URL,
        params=params,
        headers={"Authorization": f"KakaoAK {api_key()}"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def _drain(query: str, rect: Rect, meta: dict[str, Any]) -> list[dict[str, Any]]:
    """상한에 걸리지 않은 사각형의 남은 페이지를 마저 받는다."""
    documents: list[dict[str, Any]] = []

    page = 1
    while not meta.get("is_end") and page * PAGE_SIZE < MAX_EXPOSED:
        page += 1
        payload = search_page(query, page=page, rect=rect)
        meta = payload.get("meta") or {}
        documents.extend(payload.get("documents") or [])

    return documents


def search_all(query: str, *, rect: Rect = KOREA) -> tuple[list[dict[str, Any]], int]:
    """사각형을 쪼개가며 전량을 받는다.

    문서 목록과 최상위 사각형의 total_count를 함께 돌려준다. 호출부는 이 둘을
    대조해 다 받았는지 확인해야 한다.
    """
    logger = get_run_logger()

    found: dict[str, dict[str, Any]] = {}
    expected = 0
    truncated_leaves: list[Rect] = []

    def walk(current: Rect, depth: int) -> None:
        nonlocal expected

        payload = search_page(query, page=1, rect=current)
        meta = payload.get("meta") or {}
        total = meta.get("total_count") or 0

        if depth == 0:
            expected = total

        if total == 0:
            return

        for document in payload.get("documents") or []:
            found[document["id"]] = document

        # 상한에 걸렸으면 이 사각형 안을 다 못 본 것이다. 넷으로 쪼개 다시 본다.
        if total > (meta.get("pageable_count") or 0):
            if depth < MAX_DEPTH:
                for quarter in quarters(current):
                    walk(quarter, depth + 1)
                return
            truncated_leaves.append(current)

        for document in _drain(query, current, meta):
            found[document["id"]] = document

    walk(rect, 0)

    if truncated_leaves:
        logger.warning(
            "깊이 상한 %d에서도 잘린 사각형이 %d개 있습니다. 일부 지점이 빠졌을 "
            "수 있습니다: %s",
            MAX_DEPTH,
            len(truncated_leaves),
            truncated_leaves,
        )

    return list(found.values()), expected
