"""국토교통부 전국 법정동 CSV 를 공공데이터포털에서 받는다.

`시도명 / 시군구명 / 읍면동명 / 리명` 이 열로 나뉘어 오고 `생성일자` 가
20,561행 전부에 있다.

**대신 이름이 제대로 나뉘어 있지 않다.** 특례시 일반구 39종에서 공백이 빠져
`수원시영통구` 로 오고, 세종특별자치시에는 없는 시군구 `세종시` 가 채워져 있다.
그 교정은 이 모듈이 하지 않는다. 여기는 사이트가 준 것만 담고 `normalize` 가
나눈다. 값이 틀렸을 때 누구 잘못인가로 가르면 여기까지는 사이트 잘못이다.

받는 방식이 두 단계다. 상세 페이지를 한 번 열어 세션을 얻고, POST 로 첨부파일
id 를 받은 다음 그 id 로 내려받는다. 로그인은 필요 없다. **id 를 코드에 박아두지
않는다.** 데이터셋이 갱신되면 id 가 바뀌므로 박아두면 갱신된 뒤에도 옛 파일을
계속 받는다.
"""

import csv
import io
from dataclasses import dataclass
from datetime import date

import httpx
from prefect import get_run_logger, task

DETAIL_URL = "https://www.data.go.kr/data/15063424/fileData.do"
HANDLE_URL = "https://www.data.go.kr/tcs/dss/selectFileDataDownload.do"
DOWNLOAD_URL = "https://www.data.go.kr/cmm/cmm/fileDownload.do"

PUBLIC_DATA_PK = "15063424"
PUBLIC_DATA_DETAIL_PK = "uddi:5176efd5-da6e-42a0-b2cf-8512f74503ea"
PUBLIC_DATA_TY_CODE = "PR0051"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

# 파일이 BOM 을 달고 온다. utf-8 로 읽으면 첫 열 이름에 BOM 이 붙어 헤더 조회가
# 조용히 빗나간다.
ENCODING = "utf-8-sig"

HEADER = "법정동코드"


@dataclass(frozen=True)
class SourceDong:
    """국토부가 준 한 행. 붙어 있는 이름도 그대로 담는다.

    `sgg_name` 에 `수원시영통구` 가 들어올 수 있다. 나누는 것은 `normalize` 의
    일이므로 여기서 손대지 않는다.
    """

    code: str
    sido_name: str
    sgg_name: str
    umd_name: str
    ri_name: str
    created_on: date | None


def _parse_created_on(value: str) -> date | None:
    """`1988-04-23` 형태를 날짜로. 20,561행 전부에 값이 있지만 빈 값도 견딘다."""
    text = (value or "").strip()
    try:
        year, month, day = text.split("-")
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


@task(retries=3, retry_delay_seconds=[10, 30, 60])
def fetch_rows() -> tuple[bytes, str]:
    """CSV 본문과 데이터셋 이름을 받는다.

    이름(예: `국토교통부_전국 법정동_20260630`)에 기준일자가 들어 있다. 적재할
    테이블 코멘트에 남겨 어느 스냅샷으로 만든 테이블인지 알 수 있게 한다.
    """
    logger = get_run_logger()

    with httpx.Client(
        timeout=180.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        # 세션 쿠키를 얻는다. 이것 없이 POST 하면 핸들이 오지 않는다.
        client.get(DETAIL_URL)

        handle = client.post(
            HANDLE_URL,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": DETAIL_URL,
                "Origin": "https://www.data.go.kr",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            data={
                "publicDataDetailPk": PUBLIC_DATA_DETAIL_PK,
                "publicDataPk": PUBLIC_DATA_PK,
                "atchFileId": "",
                "fileDetailSn": "1",
                "publicDataTyCode": PUBLIC_DATA_TY_CODE,
                "url": "/tcs/dss/selectFileDataDownload.do",
            },
        )
        handle.raise_for_status()

        # 응답 Content-Type 이 text/html 이지만 본문은 JSON 이다.
        payload = handle.json()
        file_id = payload.get("atchFileId")
        if not payload.get("status") or not file_id:
            raise ValueError(
                "첨부파일 id 를 받지 못했습니다. 데이터셋 식별자가 바뀌었을 수 "
                f"있습니다: status={payload.get('status')!r}"
            )

        dataset = (
            (payload.get("fileDataRegistVO") or {}).get("dataNm")
            or (payload.get("dataSetFileDetailInfo") or {}).get("dataNm")
            or "(이름 없음)"
        )
        logger.info("스냅샷 %s (파일 id %s)", dataset, file_id)

        body = client.get(
            DOWNLOAD_URL,
            params={"atchFileId": file_id, "fileDetailSn": "1"},
            headers={"Referer": DETAIL_URL},
        )

    body.raise_for_status()

    # 실패하면 HTML 이 200 으로 온다. 헤더 첫 열 이름으로 가른다.
    if HEADER.encode("utf-8") not in body.content[:64]:
        raise ValueError(
            "CSV 가 아닌 응답을 받았습니다. 다운로드 경로가 바뀌었을 수 "
            f"있습니다: {body.content[:200]!r}"
        )

    return body.content, dataset


@task
def parse_rows(body: bytes) -> list[SourceDong]:
    """CSV 를 행 목록으로. 코드나 시도명이 없는 줄은 버린다."""
    logger = get_run_logger()

    reader = csv.DictReader(io.StringIO(body.decode(ENCODING)))

    rows: list[SourceDong] = []
    skipped = 0

    for record in reader:
        code = (record.get("법정동코드") or "").strip()
        sido = (record.get("시도명") or "").strip()

        # 코드는 10자리여야 한다. 자리수가 계층을 담고 있어 짧으면 계층을 읽을 수
        # 없다.
        if len(code) != 10 or not code.isdigit() or not sido:
            skipped += 1
            continue

        rows.append(
            SourceDong(
                code=code,
                sido_name=sido,
                sgg_name=(record.get("시군구명") or "").strip(),
                umd_name=(record.get("읍면동명") or "").strip(),
                ri_name=(record.get("리명") or "").strip(),
                created_on=_parse_created_on(record.get("생성일자") or ""),
            )
        )

    if skipped:
        logger.warning("코드나 시도명이 없어 건너뛴 줄 %d개", skipped)

    logger.info("원문 %d행", len(rows))
    return rows
