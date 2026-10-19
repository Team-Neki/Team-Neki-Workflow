"""Discord 알림은 환경을 표시하고, 없거나 실패해도 flow 를 막지 않는다."""

from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from flows.common import discord


@pytest.fixture
def post(monkeypatch):
    response = Mock()
    sender = Mock(return_value=response)
    monkeypatch.setattr(discord.httpx, "post", sender)
    return sender


def test_skips_without_webhook(monkeypatch, post):
    monkeypatch.delenv(discord.WEBHOOK_ENV, raising=False)
    assert discord.notify("t", ["a"]) is False
    post.assert_not_called()


def test_title_carries_spring_profile(monkeypatch, post):
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")
    monkeypatch.setenv(discord.PROFILE_ENV, "staging")
    assert discord.notify("법정동 적재 완료", ["a", "b"]) is True
    embed = post.call_args.kwargs["json"]["embeds"][0]
    assert embed["title"] == "[staging] 법정동 적재 완료"
    assert embed["description"] == "a\nb"
    assert embed["color"] == discord.COLOR_OK


def test_profile_defaults_to_local(monkeypatch):
    monkeypatch.delenv(discord.PROFILE_ENV, raising=False)
    assert discord.profile() == "local"


def test_long_description_is_truncated(monkeypatch, post):
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")
    discord.notify("t", ["x" * 5000])
    embed = post.call_args.kwargs["json"]["embeds"][0]
    assert len(embed["description"]) <= discord.MAX_DESCRIPTION


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("down"),
        httpx.HTTPStatusError(
            "bad", request=Mock(), response=SimpleNamespace(status_code=404)
        ),
    ],
)
def test_send_failure_does_not_raise(monkeypatch, post, error):
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")
    post.side_effect = error
    assert discord.notify("t", ["a"]) is False


def test_failure_hook_sends_red_embed_with_message(monkeypatch, post):
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")
    monkeypatch.setenv(discord.PROFILE_ENV, "prod")
    flow = SimpleNamespace(name="legal-dong")
    flow_run = SimpleNamespace(name="brave-otter")
    state = SimpleNamespace(type=SimpleNamespace(value="FAILED"), message="하한 미달")
    discord.notify_failure(flow, flow_run, state)
    embed = post.call_args.kwargs["json"]["embeds"][0]
    assert embed["title"] == "[prod] legal-dong 실패"
    assert embed["color"] == discord.COLOR_FAILED
    assert "하한 미달" in embed["description"]
    assert "brave-otter" in embed["description"]


def test_report_error_does_not_raise(monkeypatch, post):
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")

    def broken():
        raise KeyError("loaded")

    assert discord.notify("t", broken) is False
    post.assert_not_called()


def test_non_http_error_does_not_raise(monkeypatch, post):
    # httpx.InvalidURL 은 HTTPError 의 하위가 아니다.
    monkeypatch.setenv(discord.WEBHOOK_ENV, "https://discord.test/hook")
    post.side_effect = httpx.InvalidURL("bad")
    assert discord.notify("t", ["a"]) is False
