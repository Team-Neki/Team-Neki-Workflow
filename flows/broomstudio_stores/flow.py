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

# 표기에 공백이 섞여 있다. 외부에서 확인된 것만 해도 `비룸스튜디오 대학로점`과
# `비룸 스튜디오 홍대 점`이 둘 다 있어, 어느 표기로 질의해야 전량이 잡히는지
# 아직 확정하지 못했다. KAKAO_API_KEY 가 없어 total_count 를 대조할 수 없기
# 때문이다. 키를 확보하면 `비룸 스튜디오`로도 돌려 total_count 를 비교하고,
# 한쪽으로 모이면 그 표기 하나만 기본값으로 남긴다.
#
# `비룸`만으로 질의하지는 않는다. 상호에 이 두 글자가 들어가는 무관한 장소가
# 섞일 가능성이 높고, 그것을 걸러내려면 브랜드 판별 규칙이 필요해진다. 그 규칙은
# 픽닷과 모노맨션에도 같은 기준으로 들어가야 하므로 여기서 혼자 정할 일이 아니다.
QUERIES = ("비룸스튜디오",)


@flow(name="broomstudio-stores", log_prints=True)
def broomstudio_stores(
    queries: Sequence[str] = QUERIES,
    persist: bool = True,
) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    지점이 몇 곳인지 아직 모른다. KAKAO_API_KEY 가 없어 전국 질의의 total_count
    를 찍어보지 못했다. **그래도 코드는 달라지지 않는다.** 45건 이하면 픽닷(30곳)
    처럼 사각형을 쪼개지 않고 한 질의로 끝나고, 45건을 넘으면 모노맨션(104곳)처럼
    common.kakao 가 알아서 사분할로 내려간다. 분할 여부는 total_count 와
    pageable_count 의 비교로 정해지므로 우리가 규모를 미리 알 필요가 없다.

    질의어를 하나가 아니라 목록으로 받는다. 표기가 갈려 있어(`비룸스튜디오` /
    `비룸 스튜디오`) 한쪽 질의로 전량이 모이지 않으면 둘 다 질의해 합쳐야 할 수
    있는데, 그때 시그니처를 고치지 않고 파라미터만 바꿔 확인할 수 있어야 한다.
    합치는 기준은 장소 id 다. search_all 이 사각형별 응답을 id 로 합치는 것과
    같은 방식이라, 질의를 늘려도 같은 지점이 두 번 담기지 않는다.

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
    stores = parse_stores(documents, platform=Platform.BROOM_STUDIO)

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
        put_raw(
            json.dumps(documents, ensure_ascii=False),
            platform=Platform.BROOM_STUDIO,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.BROOM_STUDIO)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(queries))
    return stores
