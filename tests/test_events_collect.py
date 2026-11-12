"""원문 보존·부분 이미지 실패·중복 저장 계약. 외부 사이트나 DB에 쓰지 않는다."""

from copy import deepcopy
from io import BytesIO
from unittest.mock import Mock

import httpx
import pytest
from botocore.exceptions import ClientError
from PIL import Image

from neki_workflow.flows.events.collect.harufilm import parse_posts as parse_harufilm
from neki_workflow.flows.events.collect.photosignature import parse_posts as parse_signature
from neki_workflow.flows.events.common import storage, web
from neki_workflow.flows.events.common.post import CollectedImage, CollectedPost

OBSERVED = "2026-10-19T03:00:00+00:00"
SIGNATURE = '''<div id="bo_gall"><ul class="listBox"><li>
<a href="/bbs/board.php?bo_table=frame&amp;wr_id=580"><div class="txtBox">
<h3>10월 프레임</h3><div class="bo_info"><span>10.01~12.31</span></div>
<!--<div class="bo_info"><span>2026-10-01</span></div>--></div></a></li></ul></div>'''
HARUFILM = '''<div class="card" onclick="location.href='/301/?bmode=view&amp;idx=123'">
<div class="title"><em>공지</em>하루필름 소식</div><small class="date">2026-10-06</small>
<input value="not-for-storage"><script>ignore()</script></div>'''


def png() -> bytes:
    output = BytesIO()
    Image.new("RGB", (2, 2), "red").save(output, format="PNG")
    return output.getvalue()


def test_parsers_keep_raw_dates_and_missing_values():
    post = parse_signature(SIGNATURE, OBSERVED)[0]
    assert (post.external_id, post.published_at_raw, post.frame_period_raw) == (
        "580", "2026-10-01", "10.01~12.31"
    )
    assert post.source_url.endswith("bo_table=frame&wr_id=580")
    assert post.raw_content["detail"] is None
    post = parse_harufilm(HARUFILM + HARUFILM, OBSERVED)[0]
    assert len(parse_harufilm(HARUFILM + HARUFILM, OBSERVED)) == 1
    assert post.title == "하루필름 소식"
    assert post.frame_period_raw is None
    assert "not-for-storage" not in post.raw_content["listing"]
    assert "<script" not in post.raw_content["listing"]
    missing = SIGNATURE.replace("2026-10-01", "unknown")
    assert parse_signature(missing, OBSERVED)[0].published_at_raw is None


def test_detail_keeps_original_order_and_marks_unknown_images():
    post = parse_harufilm(HARUFILM, OBSERVED)[0]
    web.read_detail(post, '''<div class="board_txt_area"><img src="/a.png">
    <img><img data-original="https://cdn.imweb.me/b.png" src="placeholder"></div>''',
                    selector=".board_txt_area")
    assert [i.image_order for i in post.images] == [0, 2]
    assert post.image_list_complete is False
    assert post.images[1].source_url == "https://cdn.imweb.me/b.png"
    assert "board_txt_area" in post.raw_content["detail"]


def test_empty_detail_is_not_a_missing_container():
    post = parse_harufilm(HARUFILM, OBSERVED)[0]
    web.read_detail(post, '<div class="board_txt_area">텍스트만</div>', selector=".board_txt_area")
    assert post.images == [] and post.image_list_complete is True
    with pytest.raises(ValueError, match="상세 본문"):
        web.read_detail(post, "<html></html>", selector=".board_txt_area")


@pytest.mark.parametrize("url", ["http://harufilm.com/a", "https://127.0.0.1/a",
                                 "https://harufilm.com.evil/a", "https://user@harufilm.com/a"])
def test_urls_are_restricted(url):
    with pytest.raises(ValueError):
        web.checked_url(url, ("harufilm.com",))


def test_redirect_is_checked_before_request(monkeypatch):
    calls = []
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})
    client = httpx.Client
    monkeypatch.setattr(web.httpx, "Client", lambda **kw: client(transport=httpx.MockTransport(handler), **kw))
    with pytest.raises(ValueError):
        web.request("https://harufilm.com/301", hosts=("harufilm.com",), delay_seconds=0)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [401, 403, 429])
def test_blocked_source_does_not_retry(monkeypatch, status):
    client = httpx.Client
    monkeypatch.setattr(web.httpx, "Client", lambda **kw: client(
        transport=httpx.MockTransport(lambda r: httpx.Response(status)), **kw))
    with pytest.raises(web.SourceUnavailable):
        web.request("https://harufilm.com/301", hosts=("harufilm.com",), delay_seconds=0)
    state = Mock()
    state.result.side_effect = web.SourceUnavailable("blocked")
    assert web.retry_request(None, None, state) is False


def test_image_validation_and_content_addressed_storage(monkeypatch):
    objects = {}
    def put_object(**kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        key = kwargs["Key"]
        if key in objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        objects[key] = kwargs["Body"]
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setattr(storage.boto3, "Session", lambda: Mock(client=lambda _: Mock(put_object=put_object)))
    data = png()
    monkeypatch.setattr(web, "request", lambda *a, **k: data)
    args = dict(prefix="events/raw/harufilm-news/123", hosts=("cdn.imweb.me",), persist=True, delay_seconds=0)
    a = web.collect_image.fn(CollectedImage(0, "https://cdn.imweb.me/a.jpg"), **args)
    b = web.collect_image.fn(CollectedImage(2, "https://cdn.imweb.me/b.png"), **args)
    assert a.s3_key == b.s3_key and len(objects) == 1
    assert a.s3_key == f"events/raw/harufilm-news/123/images/{a.sha256}.png"
    assert a.content_type == "image/png" and a.byte_size == len(data)
    post = parse_harufilm(HARUFILM, OBSERVED)[0]
    post.images = [a, b]
    assert storage.put_post.fn(post) == storage.put_post.fn(post)
    assert len(objects) == 2
    monkeypatch.setattr(web, "request", lambda *a, **k: b"<html>not an image</html>")
    with pytest.raises(ValueError, match="유효"):
        web.collect_image.fn(CollectedImage(0, "https://cdn.imweb.me/bad.png"), **args)
    assert len(objects) == 2


def test_s3_errors_are_not_treated_as_duplicates(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test")
    client = Mock()
    client.put_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
    monkeypatch.setattr(storage.boto3, "Session", lambda: Mock(client=lambda _: client))
    with pytest.raises(ClientError):
        storage.put_object(b"data", key="key", content_type="image/png")


def run_collect(monkeypatch, image_error=None):
    snapshots = []
    monkeypatch.setenv("S3_BUCKET", "test")
    monkeypatch.setattr(web, "read_completed", lambda post: None)
    monkeypatch.setattr(web, "complete_post", Mock(return_value=None))
    monkeypatch.setattr(web, "read_index", lambda _: ({}, None))
    monkeypatch.setattr(web, "merge_index", Mock())
    monkeypatch.setattr(web, "get_run_logger", lambda: Mock())
    detail = '<div class="board_txt_area"><img src="https://cdn.imweb.me/a.jpg"><img src="https://cdn.imweb.me/b.png"></div>'
    monkeypatch.setattr(web, "fetch_page", lambda url, **kw: detail if "bmode=view" in url else HARUFILM)
    def save(post):
        snapshots.append(deepcopy(post))
        return "s3://test/raw.json"
    monkeypatch.setattr(web, "put_post", save)
    def collect_image(image, **kw):
        if image.image_order == 0:
            raise image_error or ValueError("bad file")
        return CollectedImage(1, image.source_url, "events/images/hash.png", "hash", "image/png", 50)
    monkeypatch.setattr(web, "collect_image", collect_image)
    kwargs = dict(source_key="harufilm-news", list_url="https://harufilm.com/301/", source_hosts=("harufilm.com",),
                  image_hosts=("cdn.imweb.me",), parser=parse_harufilm, detail_selector=".board_txt_area",
                  pages=1, max_posts=None, persist=True, delay_seconds=0)
    return snapshots, kwargs


def test_partial_failure_keeps_raw_and_image_position(monkeypatch):
    snapshots, kwargs = run_collect(monkeypatch, httpx.ConnectError("timeout"))
    merge = Mock()
    monkeypatch.setattr(web, "merge_index", merge)
    summary = web.collect_posts(**kwargs)
    assert (summary["incomplete"], summary["failed"]) == (1, 0)
    assert "timeout" in summary["reasons"]["123"]
    merge.assert_not_called()
    post = snapshots[-1]
    assert snapshots[0].raw_content["detail"] is None
    assert snapshots[1].raw_content["detail"] is not None
    assert snapshots[1].images[1].s3_key is None
    assert [i.image_order for i in post.images] == [0, 1]
    assert post.images[0].s3_key is None
    assert snapshots[-1].images[1].s3_key == "events/images/hash.png"


def test_permanent_image_failure_is_recorded_and_skipped_later(monkeypatch):
    _, kwargs = run_collect(monkeypatch, ValueError("bad file"))
    merge = Mock()
    monkeypatch.setattr(web, "merge_index", merge)
    summary = web.collect_posts(**kwargs)
    assert (summary["incomplete"], summary["failed"]) == (0, 1)
    assert summary["reasons"]["123"].startswith("영구 실패")
    [(source_key, additions)] = [call.args for call in merge.call_args_list]
    assert source_key == "harufilm-news" and additions["123"][0] == "failed"


@pytest.mark.parametrize("error,expected", [
    (ValueError("bad file"), True),
    (httpx.HTTPStatusError("", request=httpx.Request("GET", "https://a"), response=httpx.Response(404)), True),
    (httpx.HTTPStatusError("", request=httpx.Request("GET", "https://a"), response=httpx.Response(503)), False),
    (httpx.ConnectError("timeout"), False),
])
def test_permanent_failure_classification(error, expected):
    assert web.permanent(error) is expected


def test_image_block_aborts_and_preserves_snapshot(monkeypatch):
    snapshots, kwargs = run_collect(monkeypatch, web.SourceUnavailable("blocked"))
    with pytest.raises(web.SourceUnavailable):
        web.collect_posts(**kwargs)
    assert snapshots[-1].raw_content["detail"] is not None
    assert len(snapshots[-1].images) == 2


def test_all_details_failed_is_visible(monkeypatch):
    snapshots, kwargs = run_collect(monkeypatch)
    monkeypatch.setattr(web, "fetch_page", lambda url, **kw: "<html>changed</html>" if "bmode=view" in url else HARUFILM)
    with pytest.raises(RuntimeError, match="전부 실패"):
        web.collect_posts(**kwargs)
    assert snapshots[0].raw_content["listing"]
    assert snapshots[0].image_list_complete is None


def test_preview_never_writes_s3(monkeypatch):
    monkeypatch.setattr(web, "request", lambda *a, **kw: png())
    write = Mock(side_effect=AssertionError("preview must not write"))
    monkeypatch.setattr(web, "put_object", write)
    image = web.collect_image.fn(
        CollectedImage(0, "https://cdn.imweb.me/a.jpg"),
        prefix="events/raw/harufilm-news/123", hosts=("cdn.imweb.me",), persist=False, delay_seconds=0,
    )
    assert image.sha256 is not None and image.s3_key is None
    write.assert_not_called()


def test_repeated_list_page_stops_without_duplicate_posts(monkeypatch):
    _, kwargs = run_collect(monkeypatch)
    calls = []
    def fetch(url, **kw):
        calls.append(url)
        return '<div class="board_txt_area">본문</div>' if "bmode=view" in url else HARUFILM
    monkeypatch.setattr(web, "fetch_page", fetch)
    kwargs["pages"] = 5
    posts = web.collect_posts(**kwargs)
    assert posts["discovered"] == 1
    assert len([url for url in calls if "page=" in url]) == 2


def test_image_size_limit_is_enforced(monkeypatch):
    monkeypatch.setattr(web, "MAX_PIXELS", 1)
    with pytest.raises(ValueError, match="크기"):
        web.validate_image(png())


def test_completed_post_skips_detail_and_images(monkeypatch):
    _, kwargs = run_collect(monkeypatch)
    saved = parse_harufilm(HARUFILM, OBSERVED)[0]
    monkeypatch.setattr(web, "read_index", lambda _: ({saved.external_id: ("complete", OBSERVED)}, "etag"))
    monkeypatch.setattr(web, "read_completed", Mock(side_effect=AssertionError("indexed post read")))
    fetch = Mock(return_value=HARUFILM)
    image = Mock(side_effect=AssertionError("completed image requested"))
    monkeypatch.setattr(web, "fetch_page", fetch)
    monkeypatch.setattr(web, "collect_image", image)
    assert web.collect_posts(**kwargs)["skipped"] == 1
    assert fetch.call_count == 1
    image.assert_not_called()


def test_completion_requires_all_images(monkeypatch):
    post = parse_harufilm(HARUFILM, OBSERVED)[0]
    write = Mock()
    monkeypatch.setattr(storage, "put_object", write)
    storage.complete_post.fn(post)
    post.image_list_complete = True
    post.images = [CollectedImage(0, "https://cdn.imweb.me/a.png")]
    storage.complete_post.fn(post)
    write.assert_not_called()
    monkeypatch.setattr(storage.read_completed, "fn", lambda _: OBSERVED)
    post.images[0].s3_key = "events/raw/harufilm-news/123/images/hash.png"
    storage.complete_post.fn(post)
    assert write.call_args.kwargs["key"] == "events/raw/harufilm-news/123/post.json"


def test_missing_index_recovers_without_detail_requests(monkeypatch):
    _, kwargs = run_collect(monkeypatch)
    monkeypatch.setattr(web, "read_completed", lambda _: OBSERVED)
    fetch = Mock(return_value=HARUFILM)
    merge = Mock()
    monkeypatch.setattr(web, "fetch_page", fetch)
    monkeypatch.setattr(web, "merge_index", merge)
    result = web.collect_posts(**kwargs)
    assert result == dict(discovered=1, skipped=0, collected=0, recovered=1, incomplete=0, failed=0, reasons={})
    assert fetch.call_count == 1
    merge.assert_called_once_with("harufilm-news", {"123": ("complete", OBSERVED)})


def test_new_post_is_added_without_reading_existing_post(monkeypatch):
    _, kwargs = run_collect(monkeypatch)
    monkeypatch.setattr(web, "read_index", lambda _: ({"123": ("complete", OBSERVED)}, "etag"))
    reads = []
    def read(post):
        reads.append(post.external_id)
        return None
    monkeypatch.setattr(web, "read_completed", read)
    listing = HARUFILM + HARUFILM.replace("idx=123", "idx=124")
    monkeypatch.setattr(web, "fetch_page", lambda url, **kw:
                        '<div class="board_txt_area">본문</div>' if 'bmode=view' in url else listing)
    monkeypatch.setattr(web, "complete_post", lambda post: OBSERVED)
    merge = Mock()
    monkeypatch.setattr(web, "merge_index", merge)
    assert web.collect_posts(**kwargs) == dict(
        discovered=2, skipped=1, collected=1, recovered=0, incomplete=0, failed=0, reasons={})
    assert reads == ["124"]
    merge.assert_called_once_with("harufilm-news", {"124": ("complete", OBSERVED)})


def test_index_error_stops_before_source_requests(monkeypatch):
    _, kwargs = run_collect(monkeypatch)
    monkeypatch.setattr(web, "read_index", Mock(side_effect=ValueError("broken CSV")))
    fetch = Mock()
    monkeypatch.setattr(web, "fetch_page", fetch)
    with pytest.raises(ValueError, match="broken CSV"):
        web.collect_posts(**kwargs)
    fetch.assert_not_called()
