import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from a13n_channels.channels import qq


def test_qq_identifies_receives_resumes_and_replies(monkeypatch):
    outbound = []
    statuses = []
    received = []
    announce_ready = True

    def http_request(request):
        if str(request.url) == qq.QQ_TOKEN:
            assert json.loads(request.content) == {"appId": "123", "clientSecret": "fake"}
            return httpx.Response(200, json={"access_token": "test-token", "expires_in": 7200})
        assert request.headers["Authorization"] == "QQBot test-token"
        if request.url.path == "/gateway":
            return httpx.Response(200, json={"url": "wss://gateway.qq.com/"})
        assert request.url.path == "/v2/groups/open-id/messages"
        assert json.loads(request.content) == {"content": "world", "msg_id": "m", "msg_seq": 1, "msg_type": 0}
        return httpx.Response(200, json={"id": "sent"})

    class Socket:
        close_code = 1000

        async def recv(self):
            return '{"op": 10, "d": {"heartbeat_interval": 10000}}'

        async def send(self, raw):
            outbound.append(json.loads(raw))

        async def __aiter__(self):
            if not announce_ready:
                yield '{"op": 7}'
                return
            yield json.dumps({"op": 0, "t": "READY", "s": 1, "d": {"session_id": "sid"}})
            yield json.dumps(
                {
                    "op": 0,
                    "t": "GROUP_AT_MESSAGE_CREATE",
                    "s": 2,
                    "d": {
                        "author": {"member_openid": "sender"},
                        "id": "m",
                        "group_openid": "open-id",
                        "timestamp": "2026-11-18T00:00:00Z",
                        "content": "hello",
                    },
                }
            )
            yield '{"op": 7}'

    @asynccontextmanager
    async def connect(url, **_kwargs):
        assert url == "wss://gateway.qq.com/"
        yield Socket()

    monkeypatch.setattr(qq, "connect", connect)

    async def exercise():
        nonlocal announce_ready
        async with httpx.AsyncClient(transport=httpx.MockTransport(http_request)) as http:
            connection = qq.QQConnection(http, "qq_test", qq.QQCreate(name="QQ", app_id="123", app_secret="fake"))

            async def receive(incoming):
                received.append(incoming)

            await connection._session(receive, statuses.append)
            await connection._session(receive, statuses.append)
            assert outbound[0] == {
                "op": 2,
                "d": {"token": "QQBot test-token", "intents": qq.QQ_INTENTS, "shard": [0, 1]},
            }
            assert outbound[1] == {"op": 6, "d": {"token": "QQBot test-token", "session_id": "sid", "seq": 2}}
            assert statuses == ["connected", "connected"]
            assert received[0].text == "hello"
            assert await connection.reply(received[0], "world", 1) == "sent"

            probe = qq.QQConnection(http, "qq_probe", connection.credentials)
            await probe.test_connection()
            assert outbound[-1]["op"] == 2
            announce_ready = False
            with pytest.raises(ValueError, match="before sending READY"):
                await qq.QQConnection(http, "qq_probe", connection.credentials).test_connection()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("event", "mentions", "expected"),
    [
        ("GROUP_AT_MESSAGE_CREATE", [], True),
        ("GROUP_MESSAGE_CREATE", [{"is_you": True}], True),
        ("GROUP_MESSAGE_CREATE", [{"is_you": 1}], False),
        ("AT_MESSAGE_CREATE", [], True),
        ("MESSAGE_CREATE", [{"is_you": False}], False),
        ("C2C_MESSAGE_CREATE", [], None),
    ],
)
def test_qq_message_uses_platform_mention_metadata(event, mentions, expected):
    incoming = qq.message(
        "qq_test",
        {
            "t": event,
            "d": {
                "id": "message",
                "author": {"id": "sender", "member_openid": "sender", "user_openid": "sender"},
                "group_openid": "group",
                "channel_id": "guild-channel",
                "timestamp": "2026-11-18T00:00:00Z",
                "content": "<@bot> hello",
                "mentions": mentions,
            },
        },
    )
    assert incoming.bot_mentioned is expected
