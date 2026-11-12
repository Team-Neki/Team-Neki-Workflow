"""웹 수집의 HTTP·이미지 검증과 부분 실패 처리. 출처별 파서는 collect에 둔다."""

import hashlib
import time
import warnings
from collections.abc import Callable
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup, Tag
from PIL import Image
from prefect import get_run_logger, task

from neki_workflow.flows.events.common.post import CollectedImage, CollectedPost
from neki_workflow.flows.events.common.storage import (
    bucket, complete_post, post_prefix, put_object, put_post, read_completed, read_index, merge_index, utc_string,
)

MAX_BYTES = 20 * 1024 * 1024
MAX_PIXELS = 40_000_000
IMAGE_EXTENSIONS = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif"}
IMAGE_TYPES = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp", "GIF": "image/gif"}


class SourceUnavailable(RuntimeError):
    """차단·로그인 요구는 해당 출처의 후속 요청까지 중단한다."""


def retry_request(task, task_run, state) -> bool:
    try:
        state.result()
    except (SourceUnavailable, ValueError):
        return False
    except httpx.HTTPStatusError as error:
        return error.response.status_code >= 500
    except Exception:
        return True
    return False


def permanent(error: Exception) -> bool:
    """재시도해도 결과가 같은 실패. 5xx·네트워크 오류는 다음 실행에서 다시 시도한다."""
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code < 500
    return isinstance(error, ValueError)


def checked_url(url: str, hosts: tuple[str, ...]) -> str:
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in hosts
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError("허용되지 않은 수집 URL입니다.")
    return url


def request(url: str, *, hosts: tuple[str, ...], delay_seconds: float) -> bytes:
    """본문 크기를 제한하고 리디렉션의 목적지도 요청 전에 검사한다."""
    with httpx.Client(timeout=30, follow_redirects=False) as client:
        for _ in range(6):
            checked_url(url, hosts)
            time.sleep(delay_seconds)
            with client.stream("GET", url) as response:
                if response.status_code in (401, 403, 429):
                    raise SourceUnavailable(f"출처 접근 중단: HTTP {response.status_code}")
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        raise ValueError("리디렉션 주소가 없습니다.")
                    url = urljoin(url, location)
                    continue
                response.raise_for_status()
                data = bytearray()
                for chunk in response.iter_bytes():
                    data.extend(chunk)
                    if len(data) > MAX_BYTES:
                        raise ValueError("수집 파일이 20 MiB 상한을 넘었습니다.")
                return bytes(data)
    raise ValueError("리디렉션 횟수가 상한을 넘었습니다.")


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_request)
def fetch_page(url: str, *, hosts: tuple[str, ...], delay_seconds: float) -> str:
    html = request(url, hosts=hosts, delay_seconds=delay_seconds).decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text().lower() if soup.title else ""
    if soup.select_one('input[type="password"], #challenge-form') or "just a moment" in title:
        raise SourceUnavailable("로그인 또는 challenge 페이지가 반환됐습니다.")
    return html


def clean_html(node: Tag) -> str:
    """본문만 남긴다. 실행 코드와 입력 값은 재처리 원문에 필요하지 않다."""
    copy = BeautifulSoup(str(node), "html.parser")
    for el in copy.select("script, style, input"):
        el.decompose()
    return str(copy)


def validate_image(data: bytes) -> str:
    """확장자·HTTP 헤더를 믿지 않고 실제 이미지 구조와 디코딩을 확인한다."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(BytesIO(data)) as image:
            content_type = IMAGE_TYPES.get(image.format)
            if not content_type or image.width * image.height > MAX_PIXELS:
                raise ValueError("지원하지 않는 이미지 형식 또는 크기입니다.")
            image.verify()
        with Image.open(BytesIO(data)) as image:
            image.load()
    return content_type


@task(retries=3, retry_delay_seconds=[2, 5, 10], retry_condition_fn=retry_request)
def collect_image(
    image: CollectedImage, *, prefix: str, hosts: tuple[str, ...], persist: bool, delay_seconds: float
) -> CollectedImage:
    data = request(image.source_url, hosts=hosts, delay_seconds=delay_seconds)
    try:
        content_type = validate_image(data)
    except (OSError, SyntaxError, Image.DecompressionBombError, Image.DecompressionBombWarning) as error:
        raise ValueError("유효한 이미지 파일이 아닙니다.") from error
    digest = hashlib.sha256(data).hexdigest()
    extension = IMAGE_EXTENSIONS[content_type]
    key = f"{prefix}/images/{digest}.{extension}"
    if persist:
        put_object(data, key=key, content_type=content_type)
    return CollectedImage(
        image.image_order, image.source_url, key if persist else None,
        digest, content_type, len(data),
    )


def read_detail(post: CollectedPost, html: str, *, selector: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one(selector)
    if content is None:
        raise ValueError("상세 본문을 찾지 못했습니다. 페이지 구조를 확인하세요.")
    post.raw_content["detail"] = clean_html(content)
    post.body = content.get_text("\n", strip=True) or None
    post.image_list_complete = True
    for order, node in enumerate(content.select("img")):
        src = node.get("data-original") or node.get("data-src") or node.get("src")
        if not src or src.startswith("data:"):
            post.image_list_complete = False
            continue
        post.images.append(CollectedImage(order, urljoin(post.source_url, src)))


def collect_posts(
    *, source_key: str, list_url: str, source_hosts: tuple[str, ...],
    image_hosts: tuple[str, ...], parser: Callable[[str, str], list[CollectedPost]],
    detail_selector: str, pages: int, max_posts: int | None, persist: bool,
    delay_seconds: float,
) -> dict[str, int | dict[str, str]]:
    """지정한 목록 범위를 처리한다. 이미지 일부 실패는 누락 참조로 보존한다."""
    if pages <= 0 or (max_posts is not None and max_posts <= 0):
        raise ValueError("pages, max_posts는 양수여야 합니다.")
    if delay_seconds < 0:
        raise ValueError("delay_seconds는 0 이상이어야 합니다.")
    if persist:
        bucket()
    completed, _ = read_index(source_key) if persist else ({}, None)
    logger = get_run_logger()
    records: dict[str, CollectedPost] = {}
    for page in range(1, pages + 1):
        url = str(httpx.URL(list_url).copy_merge_params({"page": page}))
        html = fetch_page(url, hosts=source_hosts, delay_seconds=delay_seconds)
        observed_at = datetime.now(timezone.utc).isoformat()
        found = parser(html, observed_at)
        if not found:
            if page == 1:
                raise ValueError("목록 게시물이 없습니다. 빈 목록 또는 파서 변경 확인이 필요합니다.")
            break
        new = [post for post in found if post.external_id not in records]
        if not new:
            break
        for post in new:
            records[post.external_id] = post
        if max_posts is not None and len(records) >= max_posts:
            break
    posts = list(records.values())[:max_posts]
    detailed = 0
    summary = dict(discovered=len(posts), skipped=0, collected=0, recovered=0, incomplete=0, failed=0)
    reasons: dict[str, str] = {}
    additions: dict[str, tuple[str, str]] = {}
    for post in posts:
        if post.source_key != source_key:
            raise ValueError("목록 게시물의 출처가 요청과 다릅니다.")
        if post.external_id in completed:
            summary["skipped"] += 1
            continue
        saved_at = read_completed(post) if persist else None
        if saved_at is not None:
            additions[post.external_id] = ("complete", saved_at)
            summary["recovered"] += 1
            continue
        # 상세 요청 실패 시에도 목록에서 확보한 원문은 보존한다.
        if persist:
            put_post(post)
        try:
            html = fetch_page(post.source_url, hosts=source_hosts, delay_seconds=delay_seconds)
            post.observed_at = datetime.now(timezone.utc).isoformat()
            read_detail(post, html, selector=detail_selector)
            detailed += 1
        except SourceUnavailable:
            raise
        except (httpx.HTTPError, ValueError) as error:
            logger.warning("%s 상세 수집 실패: %s", post.external_id, error)
            summary["incomplete"] += 1
            reasons[post.external_id] = f"상세 수집 실패: {error}"
            continue
        if persist:
            put_post(post)
        missing = [] if post.image_list_complete else ["읽을 수 없는 이미지 주소"]
        retryable = False
        try:
            for position, image in enumerate(post.images):
                try:
                    post.images[position] = collect_image(
                        image, prefix=post_prefix(post), hosts=image_hosts, persist=persist, delay_seconds=delay_seconds
                    )
                except (httpx.HTTPError, ValueError) as error:
                    logger.warning("%s 이미지 %d 미확보: %s", post.external_id, image.image_order, error)
                    missing.append(f"이미지 {image.image_order}: {error}")
                    retryable = retryable or not permanent(error)
        finally:
            # 중간 차단에도 이미 확보한 파일 참조는 잃지 않는다. S3 오류는 전파한다.
            if persist:
                uri = put_post(post)
                logger.info("원문 저장: %s", uri)
        if persist:
            completed_at = complete_post(post)
            if completed_at is not None:
                additions[post.external_id] = ("complete", completed_at)
            complete = completed_at is not None
        else:
            complete = post.image_list_complete is True and all(image.sha256 for image in post.images)
        if complete:
            summary["collected"] += 1
        elif retryable:
            summary["incomplete"] += 1
            reasons[post.external_id] = "; ".join(missing)
        else:
            # 재시도해도 같은 원인만 남았다. 매 실행 다시 받지 않도록 CSV에 남긴다.
            summary["failed"] += 1
            reasons[post.external_id] = "영구 실패: " + "; ".join(missing)
            if persist:
                additions[post.external_id] = ("failed", utc_string(datetime.now(timezone.utc)))
    if not detailed and len(posts) > summary["skipped"] + summary["recovered"]:
        raise RuntimeError("상세 수집이 전부 실패했습니다. 저장한 목록 원문을 확인하세요.")
    if persist and additions:
        merge_index(source_key, additions)
    logger.info("수집 결과: %s, 사유: %s", summary, reasons)
    return {**summary, "reasons": reasons}
