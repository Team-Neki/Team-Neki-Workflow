"""하루필름 지점 목록을 Kakao 장소검색으로 수집한다.

harufilm.com 에서도 받을 수 있지만 수집원으로 삼기에 취약하다.

- 사용자가 보는 전체 목록 페이지(`/201`)에는 목록이 없다. 검색창과 이미지뿐이고
  실제 목록은 지역 페이지 `/202`~`/209` 8개에 흩어져 있다. 지역이 개편되면 이
  목록이 조용히 어긋난다
- 목록이 게시판이 아니라 갤러리 위젯 캡션이다. `#caption_<id>` 안의 `<h4>`가
  이름이고 주소는 같은 `<p>`의 마지막 `<br>` 뒤 텍스트다. 앞쪽은 기능 아이콘
  `<img>` 여러 개이고, 주소가 `<span>`으로 한 번 더 감싸인 항목도 섞여 있다
- 항목 링크 117개 중 10개는 링크가 없고, 1개는 `href="/경기 수원시 팔달구
  화서문로 28"`처럼 주소가 상대경로로 박힌 깨진 링크다
- 전화와 좌표를 주지 않아 enrich 가 전부 보강해야 한다

Kakao 는 좌표와 전화까지 한 번에 온다. 픽닷·모노맨션과 같은 수집원이라 새로 쓰는
코드는 이 flow 하나뿐이다.
"""

import json
from typing import Any

from prefect import flow, get_run_logger

from flows.common.kakao import parse_stores, search_all
from flows.common.output import log_stores
from flows.common.platform import Platform
from flows.common.storage import put_raw, put_stores
from flows.common.store import CollectedStore

# 기본 질의어. 아직 확정된 값이 아니다. 아래 docstring 참고.
QUERIES = ["하루필름"]


@flow(name="harufilm-stores", log_prints=True)
def harufilm_stores(
    queries: list[str] | None = None,
    persist: bool = True,
) -> list[CollectedStore]:
    """좌표 사각형을 쪼개가며 전량을 받아온다.

    사이트 지역 페이지 합계는 117곳(2026-08-13 기준)이라 한 질의로 꺼낼 수 있는
    45건을 넘는다. 분할 자체는 common.kakao 가 맡는다.

    픽닷·모노맨션과 달리 질의어를 목록으로 받는다. 지점명에 서브 라인이 섞여
    있어 질의 하나로 전량이 나올지 확실하지 않기 때문이다. 사이트 목록에는
    `하루필름 크림필터 연트럴파크점`, `하루필름 하루에어 강남점`처럼 접두가 붙은
    것과 `크림필터 신촌점`, `크림필터 성수점`처럼 `하루필름`이 아예 없는 표기가
    함께 있다. 후자가 Kakao 에서도 `하루필름` 질의에 걸리지 않는다면 질의를
    늘려야 하는데, **KAKAO_API_KEY 가 없어 아직 실측하지 못했다.** 키를 받으면
    기본값만 고치면 되도록 지금 목록으로 열어둔다.

    질의를 여러 개 주면 결과를 장소 id 로 합친다. `하루필름 크림필터 성수점`처럼
    두 질의에 모두 걸리는 지점이 있어도 중복이 생기지 않는다.

    이름은 Kakao 가 준 그대로 담는다. 서브 라인 접두를 떼거나 붙여 맞추는 것은
    해석이므로 enrich 의 일이다. 브랜드와 무관한 장소를 걸러내는 필터도 두지
    않는다. 넣기로 하면 픽닷·모노맨션에도 같은 규칙으로 넣어야 하고, 그 판단은
    실제 응답을 봐야 할 수 있다.

    persist 를 끄면 S3에 적재하지 않는다. 파싱만 확인할 때 쓴다.
    """
    logger = get_run_logger()

    targets = list(queries) if queries else list(QUERIES)

    # 질의마다 사각형을 쪼개 받고 id 로 합친다.
    documents: dict[str, dict[str, Any]] = {}
    shortfalls: list[tuple[str, int, int]] = []

    for query in targets:
        found, expected = search_all(query)

        for document in found:
            documents[document["id"]] = document

        # total_count 는 노출 상한과 무관하게 실제 개수를 알려준다. 분할이
        # 어딘가에서 덜 내려갔다면 여기서 드러난다. 질의별로 대조하는 이유는
        # 질의끼리 결과가 겹칠 수 있어 total_count 를 합산할 수 없기 때문이다.
        if len(found) < expected:
            shortfalls.append((query, len(found), expected))

        logger.info("'%s' 검색 %d건 (total_count %d)", query, len(found), expected)

    merged = list(documents.values())
    stores = parse_stores(merged, platform=Platform.HARU_FILM)

    if not stores:
        raise ValueError(
            f"{targets} 검색 결과가 없습니다. 질의어나 API 키를 확인해야 합니다."
        )

    log_stores(stores, label="하루필름")

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
            json.dumps(merged, ensure_ascii=False),
            platform=Platform.HARU_FILM,
            name="documents.json",
        )
        put_stores(stores, platform=Platform.HARU_FILM)

    logger.info("수집 완료: 지점 %d건 (질의 %d개)", len(stores), len(targets))
    return stores
