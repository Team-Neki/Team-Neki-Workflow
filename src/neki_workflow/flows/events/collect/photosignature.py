"""포토시그니처 프레임 게시판의 원문과 이미지를 수집한다."""

import re
from urllib.parse import parse_qs, urljoin, urlsplit

from bs4 import BeautifulSoup
from prefect import flow

from neki_workflow.flows.events.common.post import CollectedPost
from neki_workflow.flows.events.common.web import checked_url, clean_html, collect_posts

LIST_URL = "https://photosignature.co.kr/bbs/board.php?bo_table=frame"
HOSTS = ("photosignature.co.kr", "www.photosignature.co.kr")
SOURCE_KEY = "photosignature-frames"


def parse_posts(html: str, brand_id: int, observed_at: str) -> list[CollectedPost]:
    """목록의 기간 문자열과 주석 안 게시일을 해석 없이 담는다."""
    soup = BeautifulSoup(html, "html.parser")
    posts: dict[str, CollectedPost] = {}
    for row in soup.select("#bo_gall .listBox > li"):
        link = row.select_one("a[href*='wr_id=']")
        if link is None:
            continue
        url = checked_url(urljoin(LIST_URL, link["href"]), HOSTS)
        params = parse_qs(urlsplit(url).query)
        external_id = params.get("wr_id", [""])[0]
        if not external_id.isdigit() or params.get("bo_table") != ["frame"]:
            continue
        title = row.select_one(".txtBox h3")
        period = row.select_one(".txtBox .bo_info span")
        published = re.search(
            r'<!--\s*<div class="bo_info">\s*<span>(\d{4}-\d{2}-\d{2})</span>', str(row)
        )
        posts[external_id] = CollectedPost(
            brand_id=brand_id, source_key=SOURCE_KEY, external_id=external_id,
            source_url=f"{LIST_URL}&wr_id={external_id}", observed_at=observed_at,
            raw_content={"listing": clean_html(row), "detail": None},
            title=title.get_text(strip=True) or None if title else None,
            published_at_raw=published.group(1) if published else None,
            frame_period_raw=period.get_text(strip=True) or None if period else None,
        )
    return list(posts.values())


@flow(name="photosignature-events", log_prints=True)
def photosignature_events(
    brand_id: int, pages: int = 1, max_posts: int | None = None,
    persist: bool = True, delay_seconds: float = 1.0,
) -> dict[str, int]:
    """지정한 목록 범위의 게시물을 수집한다. 날짜 판정은 enrich에 맡긴다.

    brand_id는 운영 브랜드 ID다. persist=False는 S3 쓰기만 끄고 이미지 검증까지
    수행한다. pages/max_posts는 실행 범위이며 사이트 전체 수집 완료를 뜻하지 않는다.
    """
    return collect_posts(
        brand_id=brand_id, source_key=SOURCE_KEY, list_url=LIST_URL, source_hosts=HOSTS, image_hosts=HOSTS,
        parser=parse_posts, detail_selector=".bo_v_con", pages=pages,
        max_posts=max_posts, persist=persist, delay_seconds=delay_seconds,
    )
