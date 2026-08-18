"""포토랩플러스 지점 목록을 Kakao 장소검색으로 수집한다.

픽닷과 모노맨션은 사이트에 지점 데이터가 없어서 Kakao 를 불렀지만, 포토랩플러스는
사이트에 목록이 **있는데도** 부르지 않는다. 사이트가 수집원으로 쓰기에 너무
약하기 때문이다.

- 지역 탭(`/17`)의 실제 목록은 `<iframe src="tabN">` 안에 있는데 탭 이름이
  `tab1, tab2, tab000, tab00, tab4, tab5` 로 무규칙이라 유추할 수 없다. 마크업에
  버튼 없이 남아 있는 `tab6`, `tab7` 은 둘 다 404 다
- 게시판이 아니라 사람이 손으로 만든 텍스트 위젯이다. 지점 하나가 `<h6>` 이름과
  `<p>` 몇 줄이고, `발산점` 처럼 주소가 두 줄로 쪼개진 항목이 있어 첫 줄만 읽으면
  동·호수가 날아간다
- 전화번호와 좌표를 주지 않는다
- 주소 문자열에 `... 1층 포토랩플러스 혜화점` 처럼 상호명이 섞여 있다

무엇보다 **사이트가 틀린 데이터를 준다.** 제주 탭의 `이호테우해변점`과
`함덕해수욕장점` 주소가 서울(압구정점·홍대점) 주소로 들어가 있고, 같은 항목의
지도보기 버튼은 제주 좌표(lng 126.45 / lat 33.49)를 가리켜 서로 어긋난다. 파서를
아무리 잘 짜도 이건 걸러지지 않는다. 원본이 틀렸기 때문이다.

Kakao 는 좌표와 전화까지 한 번에 오고 위 오류 데이터도 타지 않는다. 대신 Kakao 에
없는 신규 지점은 늦게 들어올 수 있으므로, 사이트 목록과 건수를 주기적으로
대조해야 한다.
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

# 표기에 공백이 섞여 있지만 질의는 하나면 된다. `포토랩플러스` 와 `포토랩 플러스`
# 가 total_count 76 으로 같고 문서도 완전히 같았다 (2026-08-18 실측).
QUERIES = ("포토랩플러스",)

# 이름에 브랜드가 있어도 지점이 아닌 것이 섞인다. `포토랩플러스 본사`는 업종이
# `서비스,산업 > 기업` 인 사무실이다. 이름으로는 걸러지지 않는다.
#
# 빼는 쪽을 나열하지 않고 담을 쪽을 정한다. 나열하면 다음에 어떤 업종이 섞여
# 들어올지 알 수 없다. 76건 중 75건이 이 아래였다.
BRANCH_CATEGORY = "문화,예술"


def _split_branches(
    documents: list[dict[str, Any]], *, category: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """업종이 사진이 아닌 장소를 갈라낸다. 뺀 것은 이유와 함께 돌려준다.

    조용히 사라지면 나중에 빠진 것이 지점인지 아닌지 알 수 없다.
    """
    branches: list[dict[str, Any]] = []
    dropped: list[str] = []

    for document in documents:
        name = (document.get("place_name") or "").strip() or "(이름 없음)"
        group = (document.get("category_name") or "").strip() or "(업종 없음)"

        if group.startswith(category):
            branches.append(document)
        else:
            dropped.append(f"{name} — {group}")

    return branches, dropped


@flow(name="photolabplus-stores", log_prints=True)
def photolabplus_stores(
    queries: Sequence[str] = QUERIES,
    persist: bool = True,
) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    사이트 탭 합계가 72곳(2026-08-13 실측)이라 한 질의로 꺼낼 수 있는 45건을
    넘는다. 모노맨션처럼 사각형 분할이 필요하며 분할 자체는 common.kakao 가
    맡는다.

    질의어는 문자열 하나가 아니라 목록으로 받는다. 상호 표기가 `포토랩플러스` 와
    `포토랩 플러스` 로 갈려 둘 다 물어야 할 수 있었기 때문인데, 실측해보니 두
    표기가 같은 76건을 준다. 기본값은 하나만 두고 시그니처는 그대로 둔다.
    표기가 갈리기 시작하면 인자만 바꾸면 된다.

    이름과 주소는 Kakao 가 준 그대로 담는다. 상호명이 섞였는지 층수가 붙었는지를
    여기서 판정하지 않는다. 그 해석은 enrich 의 일이다. 다만 지점이 아닌 것은
    뺀다. BRANCH_CATEGORY 의 주석을 참고한다.

    persist 를 끄면 S3 에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    if not queries:
        raise ValueError("질의어가 없습니다. 최소 하나는 있어야 합니다.")

    # 질의가 둘 이상이면 id 로 합친다. search_all 이 사각형별 응답을 이미 dict 로
    # 합치므로, 질의 사이의 중복도 같은 열쇠로 사라진다.
    documents: dict[str, dict[str, Any]] = {}
    shortfalls: list[tuple[str, int, int]] = []

    for query in queries:
        found, expected = search_all(query)

        documents.update({document["id"]: document for document in found})

        # total_count 는 노출 상한과 무관하게 실제 개수를 알려준다. 분할이
        # 어딘가에서 덜 내려갔다면 여기서 드러난다. 질의별로 대조하는 이유는
        # 질의끼리 결과가 겹칠 수 있어 total_count 를 합산할 수 없기 때문이다.
        # 합계로 보면 한 질의의 미달을 다른 질의가 메워 가려버린다.
        if len(found) < expected:
            shortfalls.append((query, len(found), expected))

        logger.info("'%s' 검색 %d건 (total_count %d)", query, len(found), expected)

    merged = list(documents.values())

    branches, dropped = _split_branches(merged, category=BRANCH_CATEGORY)
    if dropped:
        logger.info("지점이 아니라 뺀 장소 %d건: %s", len(dropped), dropped)

    stores = parse_stores(branches, platform=Platform.PHOTO_LAB_PLUS)

    if not stores:
        raise ValueError(
            f"{list(queries)} 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="포토랩플러스")

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
            json.dumps(merged, ensure_ascii=False),
            platform=Platform.PHOTO_LAB_PLUS,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.PHOTO_LAB_PLUS)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(queries))
    return stores
