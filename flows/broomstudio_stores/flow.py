"""비룸스튜디오 지점 목록을 Kakao 장소검색으로 수집한다.

다른 브랜드와 달리 수집원을 고를 여지가 없다. 브랜드 도메인이 죽었다.

    broomstudio.co.kr        NXDOMAIN
    www.broomstudio.co.kr    NXDOMAIN
    m.broomstudio.co.kr      NXDOMAIN

예전에는 `broomstudio.co.kr/myboard/menu_list/789295`(전국 매장현황)와
`m.broomstudio.co.kr/page/page16`에 목록이 있었지만 지금은 DNS 가 응답하지
않는다. 사이트 복구를 기다릴 이유가 없으므로 Kakao 장소검색을 수집원으로 삼는다.
픽닷과 같은 모양이 되고, 새로 쓰는 코드는 이 flow 하나다.
"""

import json
from collections.abc import Sequence
from typing import Any

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 표기에 공백이 섞여 있지만 질의는 하나면 된다. `비룸스튜디오` 와 `비룸 스튜디오`
# 가 total_count 22 로 같고 문서도 완전히 같았다 (2026-08-18 실측).
#
# `비룸`만으로 질의하지는 않는다. 상호에 이 두 글자가 들어가는 무관한 장소가
# 섞일 가능성이 높다.
QUERIES = ("비룸스튜디오",)

# 비룸스튜디오의 상호 표기. `비룸 스튜디오 Broom Studio`처럼 공백이 들어간
# 것이 있어 한 표기로는 거를 수 없다.
BRAND_NAMES = ("비룸스튜디오", "비룸 스튜디오")

# 이름과 업종 둘로 거른다. 22건 중 2건이 파티룸 대여업이었다.
#
#   플랜비스튜디오 N파티룸    서비스,산업 > 전문대행 > 공간대여
#   렌탈스튜디오&파티룸 비온   서비스,산업 > 전문대행 > 공간대여
#
# 플랜비스튜디오는 우리가 따로 수집하는 브랜드라 그대로 두면 같은 지점이 두
# 파티션에 다른 idx 로 들어간다.
#
# 지금은 이름만으로도 둘 다 걸리지만 업종도 함께 본다. 하루필름에서는 경쟁사가
# 사진 업종으로 등록돼 있어 이름으로만 걸러야 했고, 포토랩플러스에서는 본사가
# 브랜드명을 달고 있어 업종으로만 걸러야 했다. 어느 쪽이 올지 알 수 없다.
BRANCH_CATEGORY = "문화,예술"


def _split_branches(
    documents: list[dict[str, Any]], *, names: Sequence[str], category: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """지점이 아닌 장소를 갈라낸다.

    이름에 계열 표기가 하나도 없거나, 업종이 사진이 아니면 뺀다. 뺀 것은 이유와
    함께 돌려준다. 조용히 사라지면 나중에 빠진 것이 지점인지 아닌지 알 수 없다.
    """
    branches: list[dict[str, Any]] = []
    dropped: list[str] = []

    for document in documents:
        name = (document.get("place_name") or "").strip() or "(이름 없음)"
        group = (document.get("category_name") or "").strip() or "(업종 없음)"

        if not any(brand in name for brand in names):
            dropped.append(f"{name} — 계열 아님")
        elif not group.startswith(category):
            dropped.append(f"{name} — {group}")
        else:
            branches.append(document)

    return branches, dropped


@flow(name="broomstudio-stores", log_prints=True)
def broomstudio_stores(
    queries: Sequence[str] = QUERIES,
    persist: bool = True,
) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    전국 질의의 total_count 는 22 다. 45건 상한 아래라 사각형을 쪼개지 않고 한
    질의로 끝난다. 지점이 늘어 상한에 닿으면 common.kakao 가 알아서 사분할로
    내려가므로 그때도 코드는 달라지지 않는다.

    질의어를 하나가 아니라 목록으로 받는다. 표기가 갈려 있어(`비룸스튜디오` /
    `비룸 스튜디오`) 한쪽 질의로 전량이 모이지 않으면 둘 다 질의해 합쳐야 할 수
    있었기 때문이다. 실측해보니 두 표기가 같은 22건을 준다. 기본값은 하나만 두고
    시그니처는 그대로 둔다. 합치는 기준은 장소 id 라 질의를 늘려도 같은 지점이
    두 번 담기지 않는다.

    이름과 주소는 Kakao 가 준 그대로 담는다. 다만 지점이 아닌 것은 뺀다.
    BRAND_NAMES, BRANCH_CATEGORY 의 주석을 참고한다.

    persist 를 끄면 S3 에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    if not queries:
        raise ValueError("질의어가 없습니다. 최소 하나는 있어야 합니다.")

    found: dict[str, dict[str, Any]] = {}
    shortfalls: list[tuple[str, int, int]] = []

    for query in queries:
        matched, expected = search_all(query)
        for document in matched:
            found[document["id"]] = document

        # total_count 는 노출 상한과 무관하게 실제 개수를 알려준다. 분할이
        # 어딘가에서 덜 내려갔다면 여기서 드러난다. 질의별로 대조하는 이유는
        # 질의끼리 결과가 겹칠 수 있어 total_count 를 합산할 수 없기 때문이다.
        # 합계로 보면 한 질의의 미달을 다른 질의가 메워 가려버린다.
        if len(matched) < expected:
            shortfalls.append((query, len(matched), expected))

        logger.info("'%s' 검색 %d건 (total_count %d)", query, len(matched), expected)

    documents = list(found.values())

    branches, dropped = _split_branches(
        documents, names=BRAND_NAMES, category=BRANCH_CATEGORY
    )
    if dropped:
        logger.info("지점이 아니라 뺀 장소 %d건: %s", len(dropped), dropped)

    stores = parse_stores(branches, platform=Platform.BROOM_STUDIO)

    if not stores:
        raise ValueError(
            f"{list(queries)} 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="비룸스튜디오")

    for query, collected, expected in shortfalls:
        logger.warning(
            "'%s' 수집 %d건이 total_count %d건에 못 미칩니다. 분할이 부족했을 수 "
            "있습니다.",
            query,
            collected,
            expected,
        )

    if persist:
        # 원문은 사각형마다 나뉜 응답을 id로 합친 것이다. 우리가 쓰지 않는
        # category_name이나 place_url까지 들어 있어 나중에 되짚을 수 있다.
        # 걸러내기 전을 담는다. 뺀 것이 무엇이었는지 원문에 남아 있어야
        # 필터가 과했는지 나중에 확인할 수 있다.
        put_raw(
            json.dumps(documents, ensure_ascii=False),
            platform=Platform.BROOM_STUDIO,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.BROOM_STUDIO)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(queries))
    return stores
