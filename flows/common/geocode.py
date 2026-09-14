"""좌표가 없는 지점을 Kakao 로 채운다.

사이트가 좌표를 주지 않는 지점이 있다. imweb 위젯은 `_pos_x_temp` 값이 빈
항목이 있고, 포토시그니처는 정규식이 빗나가면 비고, 플랜비스튜디오는 좌표 표에
없는 idx 가 있다. 좌표가 없으면 색인에서 지점이 통째로 빠진다.

지오코딩 같은 외부 API 보강은 원래 enrich 의 일이다. 그럼에도 수집 flow 에서
부르는 이유는 enrich 단계가 아직 없어 결측이 그때까지 방치되기 때문이며, 그
규약이 막으려던 것을 대신 여기서 막는다.

- Kakao 장애가 수집 실패가 되지 않는다. 키가 없으면 건너뛰고, 조회 예외는
  삼킨다. 수집 결과는 언제나 남는다
- 사이트가 준 좌표와 우리가 채운 좌표를 coordinate_source 로 구분한다
- 주소는 해석하지 않는다. 원문을 그대로 질의에 넣을 뿐 층수나 상호명을 떼지
  않는다. 그것은 여전히 enrich 의 일이다
"""

import os
import time
from dataclasses import replace
from typing import Any, Callable, Literal

from prefect import get_run_logger, task

from flows.common import kakao
from flows.common.store import CollectedStore

# 키워드검색 폴백에 붙이는 지역 접두의 토큰 수. 주소 앞 두 토큰이면 시/도와
# 시군구다. 더 붙이면 사이트가 틀리게 적은 번지까지 질의에 섞여 들어간다.
REGION_TOKENS = 2

# 한 지점의 조회를 몇 번까지 다시 해보는가. 여기서 실패해도 그 지점만 좌표 없이
# 넘어가면 되므로 크게 잡지 않는다.
LOOKUP_ATTEMPTS = 3

# 조회 하나에 매달리는 시간. 수집 전체가 30초대인데 지점마다 20초를 기다리면
# 보정이 수집보다 오래 걸린다. 정상 응답은 1초 안에 온다.
LOOKUP_TIMEOUT = 5.0

# 연속으로 이만큼 실패하면 남은 지점은 조회하지 않는다. Kakao 가 통째로 죽었을
# 때 지점 수만큼 재시도를 되풀이하면 수집이 몇십 분 늘어진다. 개별 실패는
# 넘어가되 장애는 빨리 포기하는 것이 낫다.
GIVE_UP_AFTER = 3

# 질의와 timeout 을 받는 Kakao 조회. kakao.search_address / search_keyword 다.
Search = Callable[..., list[dict[str, Any]]]

# 조회 방식은 요약 로그에만 사용하고 저장 출처는 kakao로 통일한다.
Point = tuple[float, float, Literal["kakao_address", "kakao_keyword"]]


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _point(documents: list[dict[str, Any]]) -> tuple[float, float] | None:
    """문서 목록에서 첫 좌표를 꺼낸다. Kakao 는 x 가 경도, y 가 위도다."""
    for document in documents:
        longitude = _number(document.get("x"))
        latitude = _number(document.get("y"))
        if longitude is not None and latitude is not None:
            return longitude, latitude

    return None


def _missing(stores: list[CollectedStore]) -> int:
    return sum(
        1 for store in stores if store.longitude is None or store.latitude is None
    )


def region(address: str | None) -> str:
    """주소 앞 두 토큰. 키워드검색이 전국에서 같은 상호를 집지 않게 한다."""
    if not address:
        return ""

    return " ".join(address.split()[:REGION_TOKENS])


def _lookup(query: str, search: Search) -> tuple[float, float] | None:
    """Kakao 조회 하나. 못 찾으면 None 이고, 끝내 실패하면 예외를 올린다.

    여기서 예외를 삼키지 않는 이유는 "안 잡힌 지점"과 "조회가 안 되는 상황"이
    다르기 때문이다. 앞은 그 지점만 넘어가면 되지만 뒤는 남은 지점을 물어봐야
    소용이 없다. 호출부가 그것을 세어 포기한다.
    """
    for attempt in range(1, LOOKUP_ATTEMPTS + 1):
        try:
            return _point(search(query, timeout=LOOKUP_TIMEOUT))
        except Exception:
            if attempt == LOOKUP_ATTEMPTS:
                raise
            time.sleep(attempt * 2)

    return None


def locate(store: CollectedStore) -> Point | None:
    """지점 하나의 좌표를 찾는다. 못 찾으면 None 이다.

    주소검색을 먼저 두는 이유는 질의가 주소 하나로 닫혀 있어 엉뚱한 가게를 집을
    일이 없기 때문이다. 키워드검색은 그러지 못하므로, 주소가 손으로 적혀 있어
    주소검색이 0건을 주는 지점의 폴백으로만 쓴다.
    """
    address = (store.address or "").strip()

    if address:
        found = _lookup(address, kakao.search_address)
        if found:
            return found[0], found[1], "kakao_address"

    name = store.name.strip()
    if not name:
        return None

    found = _lookup(f"{region(address)} {name}".strip(), kakao.search_keyword)
    if found:
        return found[0], found[1], "kakao_keyword"

    return None


@task
def fill_coordinates(
    stores: list[CollectedStore], *, label: str
) -> list[CollectedStore]:
    """좌표가 빈 지점만 채운 새 목록을 돌려준다.

    이미 좌표가 있는 지점은 건드리지 않는다. 사이트가 준 값을 Kakao 값으로
    바꾸는 것은 보정이 아니라 수집원 교체이고, 그 판단은 브랜드 flow 가 이미
    했다.

    같은 주소는 한 번만 조회한다. 한 건물에 지점이 둘 있거나 목록에 같은 항목이
    두 번 실린 경우가 있어 그대로 두면 호출이 그만큼 는다.

    재시도를 붙이지 않는다. 조회는 지점마다 이미 다시 해보고, task 를 통째로
    다시 돌리면 수백 번의 호출을 처음부터 되풀이하게 된다.
    """
    logger = get_run_logger()

    missing = _missing(stores)
    if not missing:
        logger.info("%s 좌표 보정: 좌표가 빈 지점이 없습니다.", label)
        return stores

    if not os.environ.get(kakao.API_KEY_ENV):
        logger.warning(
            "%s 좌표 보정: %s 가 없어 %d건을 좌표 없이 둡니다.",
            label,
            kakao.API_KEY_ENV,
            missing,
        )
        return stores

    found: dict[tuple[str, str], Point | None] = {}
    filled = {"kakao_address": 0, "kakao_keyword": 0}
    result: list[CollectedStore] = []
    failures = 0

    for position, store in enumerate(stores):
        if (
            store.longitude is not None and store.latitude is not None
        ) or failures >= GIVE_UP_AFTER:
            result.append(store)
            continue

        key = ((store.address or "").strip(), store.name.strip())
        if key not in found:
            try:
                found[key] = locate(store)
            except Exception as error:
                failures += 1
                logger.warning("%s 좌표 조회 실패 (%s): %s", label, store.name, error)
                if failures >= GIVE_UP_AFTER:
                    logger.error(
                        "%s 좌표 보정: 연속 %d번 실패해 남은 %d건은 조회하지 "
                        "않습니다. Kakao 상태를 확인하세요.",
                        label,
                        failures,
                        _missing(stores[position + 1 :]),
                    )
                result.append(store)
                continue
            failures = 0

        point = found[key]
        if point is None:
            result.append(store)
            continue

        longitude, latitude, source = point
        filled[source] += 1
        result.append(
            replace(
                store,
                longitude=longitude,
                latitude=latitude,
                coordinate_source="kakao",
            )
        )

    still_missing = missing - filled["kakao_address"] - filled["kakao_keyword"]
    log = logger.warning if still_missing else logger.info
    log(
        "%s 좌표 보정: 없던 %d건 중 주소로 %d건, 상호로 %d건을 채우고 %d건이 "
        "남았습니다 (조회 %d회).",
        label,
        missing,
        filled["kakao_address"],
        filled["kakao_keyword"],
        still_missing,
        len(found),
    )
    return result
