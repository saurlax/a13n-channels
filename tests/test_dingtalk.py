import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import httpx
import pytest

from a13n_channels.channels import dingtalk
from a13n_channels.channels.adapter import ChannelError

CONFIG = {"client_id": "example-client", "client_secret": "example-secret"}
TOPIC = "/v1.0/im/bot/messages/get"
ENDPOINT = "wss://wss-open-connection.dingtalk.com:443/connect"
SESSION = "opaque-private-session"
WEBHOOK = f"https://oapi.dingtalk.com/robot/sendBySession?session={SESSION}"
EXPIRES = 1_800_000_000_000
CREATED = 1_700_000_000_123


def message(**fields):
    return {
        "msgtype": "text",
        "msgId": "message==",
        "conversationId": "conversation==",
        "conversationType": "2",
        "senderId": "sender",
        "chatbotUserId": "bot",
        "text": {"content": " hello "},
        "isInAtList": True,
        "createAt": CREATED,
        "sessionWebhook": WEBHOOK,
        "sessionWebhookExpiredTime": EXPIRES,
        **fields,
    }


def frame(topic, data, kind="CALLBACK", message_id="ack-id"):
    return json.dumps(
        {
            "type": kind,
            "headers": {"topic": topic, "messageId": message_id, "time": str(CREATED)},
            "data": json.dumps(data),
        }
    )


class Socket:
    def __init__(self, frames, operations):
        self.frames = iter(frames)
        self.operations = operations

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.frames)
        except StopIteration:
            raise StopAsyncIteration from None

    async def send(self, data):
        self.operations.append(("ack", json.loads(data)))


def test_bind_uses_the_application_identity_and_subscribes_to_robot_callbacks():
    def respond(request):
        assert str(request.url) == "https://api.dingtalk.com/v1.0/gateway/connections/open"
        assert json.loads(request.content) == {
            "clientId": CONFIG["client_id"],
            "clientSecret": CONFIG["client_secret"],
            "subscriptions": [{"type": "CALLBACK", "topic": TOPIC}],
        }
        return httpx.Response(200, json={"endpoint": ENDPOINT, "ticket": "one-time+ticket"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            assert await dingtalk.ADAPTER.bind(http, CONFIG) == CONFIG["client_id"]
            url = urlsplit(await dingtalk._stream_url(http, CONFIG))
            assert parse_qs(url.query) == {"ticket": ["one-time+ticket"]}

    asyncio.run(run())


def test_bind_rejects_an_untrusted_websocket_destination():
    async def run():
        response = httpx.Response(200, json={"endpoint": "wss://dingtalk.com.example/connect", "ticket": "private"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as http:
            with pytest.raises(ChannelError, match="invalid Stream endpoint") as failure:
                await dingtalk.ADAPTER.bind(http, CONFIG)
        assert failure.value.retryable
        assert "private" not in str(failure.value)

    asyncio.run(run())


def test_stream_consumes_text_and_acknowledges_after_ingest():
    operations = []
    socket = Socket(
        [
            frame(TOPIC, message(), message_id="group-ack"),
            frame(
                TOPIC,
                message(msgId="private==", conversationType="1", isInAtList=False, createAt=None),
                message_id="private-ack",
            ),
            frame(TOPIC, message(senderId="bot"), message_id="self-ack"),
            frame(TOPIC, message(msgtype="picture"), message_id="picture-ack"),
            frame("ping", {"opaque": "challenge"}, "SYSTEM", "ping-ack"),
            frame("disconnect", {"reason": "connection expired"}, "SYSTEM", ""),
            frame(TOPIC, message(), message_id="ignored-ack"),
        ],
        operations,
    )

    async def ingest(incoming, *, context):
        operations.append(("ingest", (incoming, context)))

    asyncio.run(dingtalk.DingTalkConnection(None, "channel", "app", CONFIG)._consume(socket, ingest))
    assert [kind for kind, _ in operations] == ["ingest", "ack", "ingest", "ack", "ack", "ack", "ack"]
    group, context = operations[0][1]
    assert group.model_dump(exclude={"attachments", "structured"}) == {
        "id": "message==",
        "channel_id": "channel",
        "kind": "group",
        "sender_id": "sender",
        "conversation_id": "conversation==",
        "timestamp": "2023-11-14T22:13:20.123000+00:00",
        "text": "hello",
        "bot_mentioned": True,
    }
    assert SESSION not in group.model_dump_json()
    assert context == {"session_webhook": WEBHOOK, "expires_at_ms": str(EXPIRES)}
    assert operations[1][1] == {
        "code": 200,
        "message": "OK",
        "headers": {"messageId": "group-ack", "contentType": "application/json"},
        "data": '{"response": null}',
    }
    direct, _ = operations[2][1]
    assert direct.kind == "private" and direct.bot_mentioned is False and direct.timestamp == group.timestamp
    assert json.loads(operations[-1][1]["data"]) == {"opaque": "challenge"}


def test_stream_reports_persistence_failure_without_acknowledging_success():
    operations = []
    socket = Socket([frame(TOPIC, message())], operations)

    async def ingest(_message, *, context):
        raise sqlite3.OperationalError(context["session_webhook"])

    asyncio.run(dingtalk.DingTalkConnection(None, "channel", "app", CONFIG)._consume(socket, ingest))
    assert len(operations) == 1 and operations[0][1]["code"] == 500
    assert SESSION not in str(operations)


def test_stream_renews_one_time_ticket_after_disconnect(monkeypatch):
    tickets = iter(["first", "second"])
    states = []
    sockets = []

    def respond(_):
        return httpx.Response(200, json={"endpoint": ENDPOINT, "ticket": next(tickets)})

    @asynccontextmanager
    async def connect(url, **_options):
        sockets.append(url)
        if len(sockets) == 2:
            raise asyncio.CancelledError
        yield Socket([frame("disconnect", {}, "SYSTEM", "")], [])

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            with pytest.raises(asyncio.CancelledError):
                await dingtalk.DingTalkConnection(http, "channel", "app", CONFIG).listen("", None, states.append)

    monkeypatch.setattr(dingtalk, "connect", connect)
    asyncio.run(run())
    assert states == ["connected", "reconnecting"]
    assert sockets == [f"{ENDPOINT}?ticket=first", f"{ENDPOINT}?ticket=second"]


@pytest.mark.parametrize(
    ("status", "payload", "retryable"),
    [(200, {"errcode": 0}, None), (200, {"errcode": 400}, False), (200, {}, True), (429, {}, True)],
)
def test_reply_uses_session_webhook_and_checks_platform_confirmation(status, payload, retryable, caplog, monkeypatch):
    def respond(request):
        assert request.url.host == "oapi.dingtalk.com"
        assert request.url.params["session"] == SESSION
        assert SESSION not in repr(request)
        assert json.loads(request.content) == {"msgtype": "text", "text": {"content": "answer"}}
        return httpx.Response(status, json=payload)

    async def run():
        incoming, context = dingtalk._normalize("channel", message(), CREATED)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            connection = dingtalk.DingTalkConnection(http, "channel", "app", CONFIG)
            if retryable is None:
                assert UUID(await connection.reply(incoming, "answer", context)).version == 4
            else:
                with pytest.raises(ChannelError) as failure:
                    await connection.reply(incoming, "answer", context)
                assert failure.value.retryable == retryable and SESSION not in str(failure.value)

    monkeypatch.setattr(dingtalk.time, "time", lambda: CREATED / 1000)
    with caplog.at_level(logging.INFO, logger="httpx"):
        asyncio.run(run())
    assert "sendBySession" in caplog.text and SESSION not in caplog.text


def test_reply_rejects_expired_or_untrusted_sessions_before_sending(monkeypatch):
    def respond(_):
        pytest.fail("Invalid reply sessions must not make network requests")

    async def run():
        incoming, context = dingtalk._normalize("channel", message(), CREATED)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            connection = dingtalk.DingTalkConnection(http, "channel", "app", CONFIG)
            with pytest.raises(ChannelError, match="session expired"):
                await connection.reply(incoming, "answer", context)
            _, bad_context = dingtalk._normalize("channel", message(sessionWebhook="https://invalid.example"), CREATED)
            with pytest.raises(ChannelError, match="no reply session"):
                await connection.reply(incoming, "answer", bad_context)

    monkeypatch.setattr(dingtalk.time, "time", lambda: EXPIRES / 1000)
    asyncio.run(run())
