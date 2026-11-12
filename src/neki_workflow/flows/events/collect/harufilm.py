"""하루필름 공지의 원문과 이미지를 수집한다. 이미지 OCR은 enrich 책임이다."""

import re
from urllib.parse import parse_qs, urljoin, urlsplit

from bs4 import BeautifulSoup
from prefect import flow

from neki_workflow.flows.events.common.post import CollectedPost
from neki_workflow.flows.events.common.web import checked_url, clean_html, collect_posts
from neki_workflow.flows.stores.common.platform import Platform

LIST_URL = "https://harufilm.com/301/"
HOSTS = ("harufilm.com", "www.harufilm.com")
IMAGE_HOSTS = (*HOSTS, "cdn.imweb.me")
SOURCE_KEY = "harufilm-news"


def parse_posts(html: str, observed_at: str) -> list[CollectedPost]:
    """onclick 안의 URL 문자열만 읽는다. 원문 JavaScript는 실행하지 않는다."""
    soup = BeautifulSoup(html, "html.parser")
    posts: dict[str, CollectedPost] = {}
    for row in soup.select(".card[onclick*='bmode=view']"):
        match = re.search(r'''location\.href\s*=\s*['"]([^'"]+)['"]''', row.get("onclick", ""))
        if match is None:
            continue
        url = checked_url(urljoin(LIST_URL, match.group(1)), HOSTS)
        external_id = parse_qs(urlsplit(url).query).get("idx", [""])[0]
        if not external_id.isdigit():
            continue
        title = row.select_one(".title")
        if title:
            title = BeautifulSoup(str(title), "html.parser")
            for decoration in title.select("em, .icons"):
                decoration.decompose()
        published = row.select_one(".date")
        posts[external_id] = CollectedPost(
            brand_code=Platform.HARU_FILM, source_key=SOURCE_KEY, external_id=external_id,
            source_url=f"{LIST_URL}?bmode=view&idx={external_id}", observed_at=observed_at,
            raw_content={"listing": clean_html(row), "detail": None},
            title=title.get_text(strip=True) or None if title else None,
            published_at_raw=published.get_text(strip=True) or None if published else None,
        )
    return list(posts.values())


@flow(name="harufilm-events", log_prints=True)
def harufilm_events(
    pages: int = 1, max_posts: int | None = None,
    persist: bool = True, delay_seconds: float = 1.0,
) -> dict[str, int | dict[str, str]]:
    """공지 전체를 수집 대상으로 삼는다. 기간 미표기나 이미지 0장도 제외하지 않는다."""
    return collect_posts(
        source_key=SOURCE_KEY, list_url=LIST_URL, source_hosts=HOSTS, image_hosts=IMAGE_HOSTS,
        parser=parse_posts, detail_selector=".board_txt_area", pages=pages,
        max_posts=max_posts, persist=persist, delay_seconds=delay_seconds,
    )
