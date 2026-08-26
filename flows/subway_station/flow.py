"""전국 지하철 역 정보를 수집해 Postgres 에 적재한다.

레일포털 xlsx 를 받고(`source`), 이름 표기를 고른 뒤(`normalize`), 날짜 붙인
테이블을 만들어 기존 것과 바꿔친다(`table`).

사용자가 "강남"을 검색하면 `강남 2호선` 과 `강남 신분당선` 이 각각 나오고, 그중
하나를 누르면 그 좌표 1km 안의 포토부스를 받는다. **1km 반경 매핑은 여기서 하지
않는다.** 부스 쪽에 붙는 일이라 포토부스 flow 의 몫이고, 여기서는 좌표를 결측 없이
담는 데까지 한다.

S3 에 적재하지 않는다. `raw/` 와 `collect/` 는 `platform=` 파티션이 브랜드라, 역을
넣으면 `stores_collect` 의 브랜드 순회에 섞여 들어간다. 법정동과 같은 이유다.
"""

from prefect import flow, get_run_logger

from flows.subway_station.normalize import Station, normalize
from flows.subway_station.source import fetch_rows, parse_rows
from flows.subway_station.table import swap_table

# 전량이 1,098행이다. 절반으로 줄 일은 없으므로 이보다 적으면 다운로드가 잘렸거나
# 시트 열 이름이 바뀐 것이다.
MIN_EXPECTED = 1000


@flow(name="subway-station", log_prints=True)
def subway_station(persist: bool = True) -> list[Station]:
    """역 전량을 받아 정규화하고 테이블에 반영한다.

    persist 를 끄면 Postgres 에 쓰지 않는다. 이때는 DATABASE_URL 이 없어도 된다.
    """
    logger = get_run_logger()

    body, dataset = fetch_rows()
    stations = normalize(parse_rows(body))

    if len(stations) < MIN_EXPECTED:
        raise ValueError(
            f"정규화 결과가 {len(stations)}건으로 하한 {MIN_EXPECTED}건에 못 "
            "미칩니다. 다운로드가 잘렸거나 시트 열 이름이 바뀌었을 수 있습니다."
        )

    if persist:
        swap_table(stations, dataset=dataset)

    logger.info("지하철 역 %d건", len(stations))
    return stations
