"""국가철도공단 전국 도시철도 역사 정보를 레일포털에서 받는다.

로그인도 서비스키도 필요 없다.

**공공데이터포털 사본(`15093755`)은 파일을 들고 있지 않다.** `atachFileYn` 이 `N` 인
링크형이라 법정동에 쓴 첨부파일 다운로드가 404 로 끝나고, 사본이 원본보다 1년 반 낡았다.

쓰임이 역명 검색과 좌표뿐이라 원문 15열 중 넷만 읽는다. 안 담을 열을 읽어 두면
원문이 그 열을 바꿨을 때 쓰지도 않는 값 때문에 파싱이 깨진다.
"""

import io
import re
from dataclasses import dataclass

import httpx
import openpyxl
from prefect import get_run_logger, task

DETAIL_URL = "https://data.kric.go.kr/rips/M_01_01/detail.do?id=32"
DOWNLOAD_URL = "https://data.kric.go.kr/rips/dataset/download.file"
DATASET_ID = "32"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

# 실패해도 200 에 HTML 이 온다. xlsx 는 zip 이라 앞 두 바이트로 가른다.
ZIP_MAGIC = b"PK"

# 열 순서에 기대지 않는다. 가운데에 열이 하나 끼면 값이 한 칸씩 밀린 채로 적재된다.
FIELDS = ("역사명", "노선명", "역위도", "역경도")


@dataclass(frozen=True)
class SourceStation:
    """원문 한 행. 역이 아니라 (역, 노선) 하나다."""

    name: str
    line_name: str
    lat: float
    lon: float


def _text(value: object) -> str:
    """셀 하나를 문자열로. `1.0` 이 `'1.0'` 으로 새지 않게 한다."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _filename(disposition: str) -> str:
    """`Content-Disposition` 에서 파일 이름을 꺼낸다.

    **HTTP 헤더는 latin-1 로 디코딩되는데 이 서버는 utf-8 바이트를 그대로 싣는다.**
    되돌리지 않으면 한글이 깨진 채로 테이블 코멘트에 박힌다.
    """
    match = re.search(r'filename="?([^";]+)"?', disposition)
    if not match:
        return ""

    name = match.group(1).strip()
    try:
        return name.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return name


@task(retries=3, retry_delay_seconds=[10, 30, 60])
def fetch_rows() -> tuple[bytes, str]:
    """xlsx 본문과 파일 이름을 받는다. 이름에 기준일자가 들어 있다."""
    logger = get_run_logger()

    with httpx.Client(
        timeout=180.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        body = client.get(
            DOWNLOAD_URL,
            params={"type": "filedata", "id": DATASET_ID, "operation": "1"},
            headers={"Referer": DETAIL_URL},
        )

    body.raise_for_status()

    if not body.content.startswith(ZIP_MAGIC):
        raise ValueError(
            f"xlsx 가 아닌 응답을 받았습니다: {body.content[:200]!r}"
        )

    dataset = _filename(body.headers.get("content-disposition", ""))
    logger.info("스냅샷 %s (%d bytes)", dataset or "(이름 없음)", len(body.content))

    return body.content, dataset


@task
def parse_rows(body: bytes) -> list[SourceStation]:
    """xlsx 를 행 목록으로. 이름이나 좌표가 없는 줄은 버린다."""
    logger = get_run_logger()

    workbook = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = sheet.iter_rows(values_only=True)

    header = [_text(cell) for cell in next(rows, ())]
    missing = [name for name in FIELDS if name not in header]
    if missing:
        raise ValueError(f"원문에 없는 열: {missing}. 헤더는 {header[:8]}")

    index = {name: header.index(name) for name in FIELDS}

    stations: list[SourceStation] = []
    skipped = 0

    for row in rows:
        cell = lambda name: row[index[name]] if index[name] < len(row) else None  # noqa: E731

        name = _text(cell("역사명"))
        lat, lon = _text(cell("역위도")), _text(cell("역경도"))

        # 좌표가 이 데이터의 쓸모다. 전량에서 결측이 0건이라 여기 걸리면 원문이 바뀐 것이다.
        if not name or not lat or not lon:
            skipped += 1
            continue

        try:
            stations.append(
                SourceStation(
                    name=name,
                    line_name=_text(cell("노선명")),
                    lat=float(lat),
                    lon=float(lon),
                )
            )
        except ValueError:
            skipped += 1

    workbook.close()

    if skipped:
        logger.warning("이름이나 좌표가 없어 건너뛴 줄 %d개", skipped)

    logger.info("원문 %d행", len(stations))
    return stations
