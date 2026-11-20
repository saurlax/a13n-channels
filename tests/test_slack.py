import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from a13n_channels.channels import slack
from a13n_channels.channels.adapter import ChannelError

APP = "xapp-test-token"
BOT = "xoxb-test-token"
URL = "wss-primary.slack.com/link/?ticket=test"
TS = "1712345678.010203"
PARENT = "1712345600.000001"


def payload(event_type="message", **event):
    return {
        "type": "event_callback",
        "team_id": "T1",
        "authorizations": [{"team_id": "T1", "user_id": "U1", "is_bot": True}],
        "event_id": "Ev1",
        "event": {"type": event_type, "channel": "C1", "user": "U2", "ts": TS, "text": "<@U1> hi", **event},
    }


def test_slack_bind_checks_both_tokens_and_validates_the_socket_destination():
    async def exercise():
        calls = []

        def respond(request):
            method = request.url.path.removeprefix("/api/")
            calls.append((method, request.headers["Authorization"]))
            if method == "auth.test":
                return httpx.Response(200, json={"ok": True, "user_id": "U1", "team_id": "T1", "bot_id": "B1"})
            return httpx.Response(200, json={"ok": True, "url": f"wss://{URL}"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            assert await slack.ADAPTER.bind(http, {"app_token": APP, "bot_token": BOT}) == "T1:U1"
        assert calls == [("auth.test", f"Bearer {BOT}"), ("apps.connections.open", f"Bearer {APP}")]
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            with pytest.raises(ChannelError, match="app-level token"):
                await slack.ADAPTER.bind(http, {"app_token": BOT, "bot_token": APP})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"ok": True, "url": "wss://wss.slack.com.evil.example/"})
            )
        ) as http:
            with pytest.raises(ChannelError, match="invalid Socket Mode URL"):
                await slack._socket_url(http, APP)

    asyncio.run(exercise())


def test_slack_messages_are_stable_across_event_types_and_scoped_to_the_bot_installation():
    connection = slack.SlackConnection(None, "account", "T1:U1", {})
    message, context = connection.message(payload(channel_type="group"))
    mentioned, _ = connection.message({**payload("app_mention"), "event_id": "Ev2"})
    assert message == mentioned
    assert (message.id, message.kind, message.conversation_id, message.sender_id) == (f"C1:{TS}", "channel", "C1", "U2")
    assert message.bot_mentioned
    assert message.timestamp == "2024-04-05T19:34:38.010203+00:00"
    assert context == {"thread_ts": TS}

    direct, context = connection.message(payload(channel="D1", channel_type="im", text="hey"))
    assert direct.kind == "private" and direct.bot_mentioned is False
    assert context == {}

    _, context = connection.message(payload(thread_ts=PARENT))
    assert context == {"thread_ts": PARENT}

    assert connection.message(payload(user="U1")) is None
    assert connection.message(payload(bot_id="B1")) is None
    assert connection.message(payload(subtype="message_changed")) is None
    assert connection.message({**payload(), "team_id": "T2"}) is None
    assert (
        connection.message({**payload(), "authorizations": [{"team_id": "T1", "user_id": "U3", "is_bot": True}]})
        is None
    )


@pytest.mark.parametrize("fail_ingest", [False, True])
def test_slack_socket_acknowledges_only_after_ingest(monkeypatch, fail_ingest):
    async def exercise():
        operations = []

        class Socket:
            async def recv(self):
                return json.dumps({"type": "hello"})

            async def __aiter__(self):
                yield json.dumps({"type": "events_api", "envelope_id": "ignored", "payload": payload(user="U1")})
                yield json.dumps(
                    {"type": "events_api", "envelope_id": "accepted", "payload": payload(thread_ts=PARENT)}
                )
                yield json.dumps({"type": "disconnect", "reason": "refresh_requested"})
                raise AssertionError("The old connection should close on refresh")

            async def send(self, value):
                operations.append(("ack", json.loads(value)["envelope_id"]))

        @asynccontextmanager
        async def open_socket(url, **kwargs):
            assert url == f"wss://{URL}"
            yield Socket()

        async def ingest(message, *, context):
            assert (message.id, context) == (f"C1:{TS}", {"thread_ts": PARENT})
            operations.append(("ingest", message.id))
            if fail_ingest:
                raise RuntimeError("persistence failed")

        monkeypatch.setattr(slack, "connect", open_socket)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "url": f"wss://{URL}"}))
        ) as http:
            connection = slack.SlackConnection(http, "account", "T1:U1", {"app_token": APP})
            status = lambda state: operations.append(("status", state))
            if fail_ingest:
                with pytest.raises(RuntimeError, match="persistence failed"):
                    await connection._serve(ingest, status)
            else:
                await connection._serve(ingest, status)
        assert operations[:3] == [("status", "connected"), ("ack", "ignored"), ("ingest", f"C1:{TS}")]
        assert operations[3:] == ([] if fail_ingest else [("ack", "accepted")])

    asyncio.run(exercise())


def test_slack_reconnects_with_fresh_socket_urls_and_respects_rate_limits(monkeypatch):
    async def exercise():
        calls = []
        sleeps = []
        statuses = []
        opened = []

        def respond(request):
            assert request.url.path == "/api/apps.connections.open"
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(429, headers={"Retry-After": "10"})
            return httpx.Response(200, json={"ok": True, "url": f"wss://{URL}{len(calls)}"})

        class Socket:
            async def recv(self):
                return '{"type": "hello"}'

            async def __aiter__(self):
                yield '{"type": "disconnect", "reason": "refresh_requested"}'

        @asynccontextmanager
        async def socket(url, **kwargs):
            opened.append(url)
            yield Socket()

        async def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 3:
                raise asyncio.CancelledError

        monkeypatch.setattr(slack, "connect", socket)
        monkeypatch.setattr(slack.asyncio, "sleep", sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            connection = slack.SlackConnection(http, "account", "T1:U1", {"app_token": APP})
            with pytest.raises(asyncio.CancelledError):
                await connection.listen("", None, statuses.append)
        assert opened == [f"wss://{URL}2", f"wss://{URL}3"]
        assert sleeps == [10, 1, 1]
        assert statuses == ["reconnecting", "connected", "reconnecting", "connected", "reconnecting"]

    asyncio.run(exercise())


@pytest.mark.parametrize("channel,thread", [("C1", PARENT), ("C1", None), ("D1", None)])
def test_slack_replies_follow_the_source_thread(channel, thread):
    async def exercise():
        def respond(request):
            assert request.url == "https://slack.com/api/chat.postMessage"
            assert request.headers["Authorization"] == f"Bearer {BOT}"
            expected = {
                "channel": channel,
                "text": "reply",
                "mrkdwn": False,
                "unfurl_media": False,
                "unfurl_links": False,
            }
            if thread or channel == "C1":
                expected["thread_ts"] = thread or TS
            assert json.loads(request.content) == expected
            return httpx.Response(200, json={"ok": True, "channel": channel, "ts": "1712345679.000001"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            connection = slack.SlackConnection(http, "account", "T1:U1", {"bot_token": BOT})
            message, context = connection.message(payload(channel=channel, thread_ts=thread))
            assert await connection.reply(message, "reply", context) == f"{channel}:1712345679.000001"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "response,retryable",
    [
        (httpx.Response(200, json={"ok": False, "error": "missing_scope"}), False),
        (httpx.Response(429, headers={"Retry-After": "7"}), False),
        (httpx.Response(503), True),
        (httpx.Response(200, json={"ok": True}), True),
    ],
)
def test_slack_replies_require_confirmed_success(response, retryable):
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as http:
            connection = slack.SlackConnection(http, "account", "T1:U1", {"bot_token": BOT})
            message, context = connection.message(payload())
            with pytest.raises(ChannelError) as error:
                await connection.reply(message, "reply", context)
            assert error.value.retryable is retryable
            if response.status_code == 429:
                assert error.value.retry_after == 7

    asyncio.run(exercise())
