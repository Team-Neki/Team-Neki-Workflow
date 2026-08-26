"""전국 지하철 역 정보를 수집해 Postgres 에 적재한다.

세 단계다. 레일포털 xlsx 를 받고(`source`), 이름 표기를 고르고 시도를 채운 뒤
(`normalize`), 날짜 붙인 테이블을 만들어 기존 것과 바꿔친다(`table`).

쓰임이 둘이고 둘 다 이 모양을 요구한다. 사용자가 "강남"을 검색하면
`강남역 2호선` 과 `강남역 신분당선` 이 각각 나오고, 그중 하나를 누르면 **그 역·노선
좌표 1km 안의 포토부스**를 보여준다. 그래서 원문의 (역, 노선) 행을 접지 않는다.
접으면 고를 수가 없고, 노선마다 다른 좌표가 사라져 반경 결과도 같아진다.

**1km 반경 매핑은 여기서 하지 않는다.** 역 좌표만 있으면 되는 일이고 지점 쪽에
붙는 것이므로 포토부스 flow 의 몫이다. 여기서는 매핑이 가능한 좌표를 결측 없이
담는 데까지 한다.

**표기 교정을 다운로드에 섞지 않는다.** 원문은 역명에 `역` 을 358/1,099 행에만
붙여 놓았고 같은 노선을 `7호선` 과 `도시철도 7호선` 으로 부른다. 그 교정을
`source` 에 넣으면 규칙을 고칠 때마다 사이트를 다시 긁어야 하고, 사이트가 준 값과
우리가 만든 값을 구분할 수 없게 된다. 값이 틀렸을 때 누구 잘못인가로 가르면
`source` 까지는 사이트 잘못이고 그 다음은 우리 잘못이다.

S3 에 적재하지 않는다. `raw/` 와 `collect/` 는 `platform=` 파티션을 쓰고 그 값은
브랜드다. 역을 넣으려면 `Platform` 에 브랜드가 아닌 값을 더해야 하고, 그러면
`stores_collect` 의 브랜드 순회에 섞여 들어간다. 법정동과 같은 이유다.
"""

from prefect import flow, get_run_logger

from flows.subway_station.normalize import Station, normalize
from flows.subway_station.source import fetch_rows, parse_rows
from flows.subway_station.table import swap_table

# 전국 도시광역철도 역사는 (역, 노선) 기준 1,098개다. 신규 개통으로 오르내리지만
# 절반으로 줄 일은 없으므로, 이보다 적게 파싱됐다면 다운로드가 잘렸거나 시트 열
# 이름이 바뀐 것이다.
MIN_EXPECTED = 1000


@flow(name="subway-station", log_prints=True)
def subway_station(persist: bool = True) -> list[Station]:
    """역 전량을 받아 정규화하고 테이블에 반영한다.

    persist 를 끄면 Postgres 에 쓰지 않는다. 파싱과 정규화만 확인할 때 쓰며
    이때는 DATABASE_URL 이 없어도 된다.
    """
    logger = get_run_logger()

    body, dataset = fetch_rows()
    stations = normalize(parse_rows(body))

    if len(stations) < MIN_EXPECTED:
        raise ValueError(
            f"정규화 결과가 {len(stations)}건으로 예상 하한 {MIN_EXPECTED}건에 "
            "못 미칩니다. 다운로드가 잘렸거나 시트 열 이름이 바뀌었을 수 있습니다."
        )

    if persist:
        swap_table(stations, dataset=dataset)

    logger.info("지하철 역 %d건", len(stations))
    return stations
