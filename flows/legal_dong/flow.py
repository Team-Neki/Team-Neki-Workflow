"""법정동 코드를 수집해 Postgres 에 적재한다.

세 단계다. 국토교통부 CSV 를 받고(`source`), 붙어 있는 이름을 나누고
계층을 붙인 뒤(`normalize`), 날짜 붙인 테이블을 만들어 기존 것과 바꿔친다(`table`).

**이름 교정을 다운로드에 섞지 않는다.** 원문은 특례시 일반구 39종에서 공백이
빠져 `수원시영통구` 로 오고, 세종특별자치시에는 없는 시군구 `세종시` 가 채워져
있다. 그 교정을 `source` 에 넣으면 규칙을 고칠 때마다 사이트를 다시 긁어야 하고,
사이트가 준 값과 우리가 만든 값을 구분할 수 없게 된다. 값이 틀렸을 때 누구
잘못인가로 가르면 `source` 까지는 사이트 잘못이고 그 다음은 우리 잘못이다.

지점을 법정동에 매핑하는 일은 여기서 하지 않는다. Kakao 를 부르는 일이므로
enrich 단계의 몫이다.

**파일로 한 번 밀어넣지 않는 이유가 있다.** 법정동은 2015~2022년에 연 50~230건
바뀌다가 2023년 3,148 / 2024년 3,908 / 2026년 7,045건으로 늘었다. 가장 최근인
2026-07-01 에는 6,576건이 한 번에 바뀌면서 광주광역시와 전라남도가 폐지되고
전남광주통합특별시(코드 접두 12)가 생겼다. 손으로 넣은 파일은 이런 개편을
놓치고, 놓치면 조인이 실패하는 대신 지점이 검색에서 조용히 사라진다.

S3 에 적재하지 않는다. `raw/` 와 `collect/` 는 `platform=` 파티션을 쓰고 그
값은 브랜드다. 법정동을 넣으려면 `Platform` 에 브랜드가 아닌 값을 더해야 하고,
그러면 `stores_collect` 의 브랜드 순회에 섞여 들어간다.
"""

from prefect import flow, get_run_logger

from flows.legal_dong.normalize import LegalDong, normalize
from flows.legal_dong.source import fetch_rows, parse_rows
from flows.legal_dong.table import swap_table

# 현존 법정동은 20,561개다. 개편으로 오르내리지만 절반으로 줄 일은 없으므로,
# 이보다 적게 파싱됐다면 다운로드가 잘렸거나 CSV 열 이름이 바뀐 것이다.
MIN_EXPECTED = 15000


@flow(name="legal-dong", log_prints=True)
def legal_dong(persist: bool = True) -> list[LegalDong]:
    """현존 법정동 전량을 받아 정규화하고 테이블에 반영한다.

    persist 를 끄면 Postgres 에 쓰지 않는다. 파싱과 정규화만 확인할 때 쓰며
    이때는 DATABASE_URL 이 없어도 된다.
    """
    logger = get_run_logger()

    body, dataset = fetch_rows()
    dongs = normalize(parse_rows(body))

    if len(dongs) < MIN_EXPECTED:
        raise ValueError(
            f"정규화 결과가 {len(dongs)}건으로 예상 하한 {MIN_EXPECTED}건에 "
            "못 미칩니다. 다운로드가 잘렸거나 CSV 열 이름이 바뀌었을 수 있습니다."
        )

    if persist:
        swap_table(dongs, dataset=dataset)

    logger.info("법정동 %d건", len(dongs))
    return dongs
