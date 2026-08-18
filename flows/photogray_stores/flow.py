"""포토그레이 지점 목록을 Kakao 장소검색으로 수집한다.

사이트 게시판(photogray.com/photodb/board.php)에도 249곳이 있지만 좌표를 주지
않는다. 목록 HTML에는 주소 문자열(data-addr)뿐이고, 지도는 지점을 누를 때
브라우저가 그 주소를 Kakao 지오코더에 넣어 그린다. 사이트도 좌표를 모른다.

좌표가 후속 단계에서 가장 중요한 값이라 수집원 자체를 Kakao로 옮겼다. 픽닷,
모노맨션과 같은 경로이고 여기서도 받아온 값을 해석하지 않는다.

Kakao가 아는 포토그레이는 223곳으로 사이트 목록보다 26곳 적다. 그만큼은
수집되지 않는다. 이 차이를 감수하는 대신 전 건에 좌표가 붙는다.
"""

import json

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 표기를 바꿔도 더 나오지 않는다. PHOTOGRAY(217), 포토 그레이(223)를 각각
# 돌려 합집합을 내봤지만 222건 그대로였고 전부 이 질의의 부분집합이었다.
QUERY = "포토그레이"


@flow(name="photogray-stores", log_prints=True)
def photogray_stores(query: str = QUERY, persist: bool = True) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    223곳이라 한 질의로 꺼낼 수 있는 45건을 넘으므로 search_all이 사각형을
    나눠 내려간다.

    persist를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    documents, expected = search_all(query)
    stores = parse_stores(documents, platform=Platform.PHOTO_GRAY)

    if not stores:
        raise ValueError(
            f"'{query}' 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="포토그레이")

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
            platform=Platform.PHOTO_GRAY,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.PHOTO_GRAY)

    logger.info("수집 완료: 지점 %d건 (total_count %d)", len(stores), expected)
    return stores
