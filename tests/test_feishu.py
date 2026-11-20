import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from a13n_channels.channels import feishu
from a13n_channels.channels.adapter import ChannelError

CONFIG = {"app_id": "cli_example", "app_secret": "fake-secret"}


def message_event(**overrides):
    message = {
        "message_id": "om_example",
        "create_time": "1700000000000",
        "chat_id": "oc_example",
        "chat_type": "group",
        "message_type": "text",
        "content": json.dumps({"text": "@_user_1 hi @_user_10"}),
        "mentions": [
            {"key": "@_user_1", "name": "Bot", "id": {"open_id": "ou_bot"}},
            {"key": "@_user_10", "name": "Ada", "id": {"open_id": "ou_ada"}},
        ],
        **overrides,
    }
    return {
        "schema": "2.0",
        "header": {"event_id": "event-example", "event_type": "im.message.receive_v1", "app_id": "cli_example"},
        "event": {"sender": {"sender_type": "user", "sender_id": {"open_id": "ou_user"}}, "message": message},
    }


def test_normalizes_text_and_only_human_events_for_this_bot():
    event = message_event(thread_id="omt_thread", root_id="om_root")
    result = feishu.normalize(event, "channel", "cli_example", "ou_bot")
    assert result.model_dump() == {
        "id": "om_example",
        "channel_id": "channel",
        "kind": "group",
        "conversation_id": "oc_example",
        "sender_id": "ou_user",
        "text": "hi @Ada",
        "timestamp": "2023-11-14T22:13:20+00:00",
        "bot_mentioned": True,
        "attachments": (),
        "structured": {"thread_id": "omt_thread", "root_id": "om_root"},
    }
    assert feishu.normalize(event, "channel", "cli_other", "ou_bot") is None
    assert feishu.normalize(event, "channel", "cli_example", "ou_user") is None
    event["event"]["sender"]["sender_type"] = "app"
    assert feishu.normalize(event, "channel", "cli_example", "ou_bot") is None


def test_renders_post_and_direct_chat():
    event = message_event(
        chat_type="p2p",
        mentions=[],
        message_type="post",
        content=json.dumps({"title": "Title", "content": [[{"tag": "text", "text": "hello"}]]}),
    )
    result = feishu.normalize(event, "channel", "cli_example", "ou_bot")
    assert result.kind == "private"
    assert result.text == "Title\nhello"
    assert not result.bot_mentioned


def event_frame(payload, count=1, index=0):
    frame = feishu.Frame(SeqID=1, LogID=2, service=17, method=feishu.DATA, payload=payload)
    for key, value in {"type": "event", "sum": str(count), "seq": str(index), "message_id": "wire-message"}.items():
        frame.headers.add(key=key, value=value)
    return frame.SerializeToString()


def test_protobuf_frame_ack_after_durable_ingest_and_replays_failure():
    async def exercise():
        frames = feishu.EventFrames("channel", "cli_example", "ou_bot")
        log = []
        fails = True

        class Socket:
            async def send(self, data):
                frame = feishu.Frame.FromString(data)
                assert (frame.SeqID, frame.LogID, frame.service, frame.method) == (1, 2, 17, feishu.DATA)
                assert any(header.key == "biz_rt" for header in frame.headers)
                log.append(json.loads(frame.payload)["code"])

        async def ingest(message):
            log.append(message.id)
            if fails:
                raise sqlite3.OperationalError("busy")

        socket = Socket()
        payload = json.dumps(message_event()).encode()
        midpoint = len(payload) // 2
        await frames.receive(event_frame(payload[midpoint:], 2, 1), socket, ingest)
        assert log == []
        await frames.receive(event_frame(payload[:midpoint], 2, 0), socket, ingest)
        assert log == ["om_example", 500]
        fails = False
        await frames.receive(event_frame(payload), socket, ingest)
        assert log == ["om_example", 500, "om_example", 200]
        await frames.receive(event_frame(b'{"header":{"event_type":"unrelated"}}'), socket, ingest)
        assert log[-1] == 200 and log.count("om_example") == 2

    asyncio.run(exercise())


def test_feishu_bind_reply_and_long_connection_lifecycle(monkeypatch):
    async def exercise():
        requests = []
        sockets = []
        ack_codes = []
        states = []
        ticket_url = "wss://msg-frontier.feishu.cn/ws/v2?service_id=17&ticket=test"

        def handler(request):
            requests.append(request)
            if request.url.path == "/open-apis/auth/v3/tenant_access_token/internal":
                assert json.loads(request.content) == CONFIG
                return httpx.Response(200, json={"code": 0, "tenant_access_token": "test-token", "expire": 7200})
            if request.url.path == "/open-apis/bot/v3/info":
                assert request.headers["Authorization"] == "Bearer test-token"
                return httpx.Response(200, json={"code": 0, "bot": {"open_id": "ou_bot"}})
            if request.url.path == "/callback/ws/endpoint":
                assert json.loads(request.content) == {"AppID": "cli_example", "AppSecret": "fake-secret"}
                return httpx.Response(200, json={"code": 0, "data": {"URL": ticket_url}})
            assert request.url.path == "/open-apis/im/v1/messages/om_example/reply"
            assert request.headers["Authorization"] == "Bearer test-token"
            body = json.loads(request.content)
            assert body["reply_in_thread"] is True and body["msg_type"] == "text"
            assert json.loads(body["content"]) == {"text": "回复"}
            return httpx.Response(200, json={"code": 0, "data": {"message_id": "om_reply"}})

        class Socket:
            async def send(self, data):
                frame = feishu.Frame.FromString(data)
                if frame.method == feishu.CONTROL:
                    assert frame.service == 17
                    assert [(header.key, header.value) for header in frame.headers] == [("type", "ping")]
                else:
                    ack_codes.append(json.loads(frame.payload)["code"])

            async def recv(self):
                if ack_codes:
                    raise asyncio.CancelledError
                return event_frame(json.dumps(message_event()).encode())

        @asynccontextmanager
        async def connect(uri, **kwargs) -> AsyncIterator[Socket]:
            assert uri == ticket_url and kwargs["max_size"] == feishu.FRAME_LIMIT
            sockets.append("open")
            try:
                yield Socket()
            finally:
                sockets.append("closed")

        monkeypatch.setattr(feishu, "connect", connect)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            identity = await feishu.ADAPTER.bind(http, CONFIG)
            assert identity == "ou_bot"
            connection = feishu.ADAPTER.connect(http, "channel", identity, CONFIG)
            message = feishu.normalize(message_event(thread_id="omt_thread"), "channel", CONFIG["app_id"], identity)
            assert await connection.reply(message, "回复", {}) == "om_reply"
            assert await connection.reply(message, "回复", {}) == "om_reply"
            assert sum(request.url.path.endswith("/internal") for request in requests) == 2

            async def ingest(incoming):
                assert incoming.id == "om_example"
                assert not ack_codes

            with pytest.raises(asyncio.CancelledError):
                await connection.listen("", ingest, states.append)
        assert states == ["connected"]
        assert sockets == ["open", "closed"]
        assert ack_codes == [200]

    asyncio.run(exercise())


def test_feishu_rejects_foreign_websocket_and_unconfirmed_reply():
    async def exercise():
        def handler(request):
            path = request.url.path
            if path == "/callback/ws/endpoint":
                return httpx.Response(200, json={"code": 0, "data": {"URL": "wss://foreign.example/?service_id=17"}})
            if path.endswith("/internal"):
                return httpx.Response(200, json={"code": 0, "tenant_access_token": "test-token", "expire": 7200})
            return httpx.Response(200, json={"code": 0, "data": {}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            connection = feishu.FeishuConnection(http, "channel", "ou_bot", CONFIG)
            with pytest.raises(ChannelError, match="invalid long connection endpoint"):
                await connection._endpoint()
            message = feishu.normalize(message_event(), "channel", CONFIG["app_id"], "ou_bot")
            with pytest.raises(ChannelError, match="did not confirm") as error:
                await connection.reply(message, "回复", {})
            assert error.value.retryable

    asyncio.run(exercise())


def test_feishu_rediscovers_endpoint_after_disconnect(monkeypatch):
    async def exercise():
        states = []
        endpoints = []

        def discover(request):
            assert request.url.path == "/callback/ws/endpoint"
            url = f"wss://msg-frontier.feishu.cn/ws/v2?service_id=17&ticket={len(endpoints)}"
            endpoints.append(url)
            return httpx.Response(200, json={"code": 0, "data": {"URL": url, "ClientConfig": {"ReconnectInterval": 1}}})

        class Socket:
            async def send(self, _data):
                pass

            async def recv(self):
                if len(endpoints) == 1:
                    raise OSError("connection closed")
                raise asyncio.CancelledError

        @asynccontextmanager
        async def connect(uri, **_kwargs):
            assert uri == endpoints[-1]
            yield Socket()

        async def ingest(_message):
            pytest.fail("No event was received")

        monkeypatch.setattr(feishu, "connect", connect)
        async with httpx.AsyncClient(transport=httpx.MockTransport(discover)) as http:
            connection = feishu.FeishuConnection(http, "channel", "ou_bot", CONFIG)
            with pytest.raises(asyncio.CancelledError):
                await connection.listen("", ingest, states.append)
        assert len(endpoints) == 2
        assert states == ["connected", "reconnecting", "connected"]

    asyncio.run(exercise())
