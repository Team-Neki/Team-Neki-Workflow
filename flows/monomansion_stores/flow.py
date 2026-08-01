"""모노맨션 지점 목록을 Kakao 장소검색으로 수집한다."""

import json

from prefect import flow, get_run_logger

from flows.common.kakao import search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore
from flows.monomansion_stores.stores import QUERY, parse_stores


@flow(name="monomansion-stores", log_prints=True)
def monomansion_stores(
    query: str = QUERY,
    persist: bool = True,
) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    지점이 104곳이라 한 질의로 꺼낼 수 있는 45건을 넘는다. 픽닷처럼 질의 하나로
    끝나지 않으므로 사각형 분할이 필요하다. 분할 자체는 common.kakao가 맡는다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    documents, expected = search_all(query)
    stores = parse_stores(documents)

    if not stores:
        raise ValueError(
            f"'{query}' 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="모노맨션")

    # total_count는 노출 상한과 무관하게 실제 개수를 알려준다. 분할이 어딘가에서
    # 덜 내려갔다면 여기서 드러난다.
    if len(stores) < expected:
        logger.warning(
            "수집 %d건이 total_count %d건에 못 미칩니다. 분할이 부족했을 수 "
            "있습니다.",
            len(stores),
            expected,
        )

    if persist:
        # 원문은 사각형마다 나뉜 응답을 id로 합친 것이다. 우리가 쓰지 않는
        # category_name이나 place_url까지 들어 있어 나중에 되짚을 수 있다.
        put_raw(
            json.dumps(documents, ensure_ascii=False),
            platform=Platform.MONO_MANSION,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.MONO_MANSION)

    logger.info("수집 완료: 지점 %d건 (total_count %d)", len(stores), expected)
    return stores
