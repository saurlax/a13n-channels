import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager

import httpx
import pytest

from a13n_channels.channels import discord
from a13n_channels.channels.adapter import ChannelError

BOT_ID = "123"
SENDER_ID = "456"
CHANNEL_ID = "789"
TOKEN = "test-discord-token"
GATEWAY = "wss://gateway.discord.gg/?v=10&encoding=json"
RESUME = "wss://gateway-abc.discord.gg/?v=10&encoding=json"


def dispatch(event, sequence, payload):
    return {"op": 0, "t": event, "s": sequence, "d": payload}


def ready(sequence=1):
    return dispatch(
        "READY",
        sequence,
        {"session_id": "test-session", "resume_gateway_url": "wss://gateway-abc.discord.gg", "user": {"id": BOT_ID}},
    )


def incoming(message_id="101", *, sender_id=SENDER_ID, guild_id="900"):
    return {
        "type": 0,
        "id": message_id,
        "author": {"id": sender_id},
        "channel_id": CHANNEL_ID,
        "guild_id": guild_id,
        "content": "hello",
        "mentions": [{"id": BOT_ID}],
        "timestamp": "2026-11-20T01:00:00.000Z",
    }


class Socket:
    def __init__(self, *frames, interval=10000):
        self.frames = iter(({"op": 10, "d": {"heartbeat_interval": interval}}, *frames))
        self.sent = []
        self.close_codes = []

    async def recv(self):
        frame = next(self.frames, None)
        if frame is None:
            await asyncio.Future()
        return json.dumps(frame)

    async def send(self, frame):
        self.sent.append(json.loads(frame))

    async def close(self, *, code):
        self.close_codes.append(code)


def test_gateway_receives_reconnects_and_replies_in_the_original_channel(monkeypatch):
    async def exercise():
        requests = []
        sockets = [
            Socket(
                ready(),
                dispatch("MESSAGE_CREATE", 2, incoming()),
                dispatch("MESSAGE_CREATE", 3, incoming("102", sender_id=BOT_ID)),
                {"op": 1},
                {"op": 11},
                {"op": 7},
            ),
            Socket(
                dispatch("MESSAGE_CREATE", 4, incoming("103", guild_id=None)),
                dispatch("RESUMED", 5, {}),
                {"op": 9, "d": False},
            ),
            Socket(ready(), {"op": 7}),
        ]
        addresses = []

        @asynccontextmanager
        async def connect(url, **kwargs):
            assert kwargs["proxy"] is None
            addresses.append(url)
            yield sockets[len(addresses) - 1]

        def handler(request):
            requests.append(request)
            assert request.headers["Authorization"] == f"Bot {TOKEN}"
            if request.url.path == "/api/v10/users/@me":
                return httpx.Response(200, json={"id": BOT_ID, "bot": True})
            if request.url.path == "/api/v10/gateway/bot":
                return httpx.Response(200, json={"url": "wss://gateway.discord.gg"})
            assert request.url.path == f"/api/v10/channels/{CHANNEL_ID}/messages"
            assert json.loads(request.content) == {
                "content": "reply @everyone",
                "message_reference": {"type": 0, "message_id": "101", "fail_if_not_exists": True},
                "allowed_mentions": {"parse": [], "replied_user": False},
            }
            return httpx.Response(200, json={"id": "201"})

        messages = []
        status = []

        async def ingest(message):
            messages.append(message)

        monkeypatch.setattr(discord, "connect", connect)
        monkeypatch.setattr(discord.random, "random", lambda: 1)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            config = {"bot_token": TOKEN, "message_content": "true"}
            identity = await discord.ADAPTER.bind(http, config)
            connection = discord.ADAPTER.connect(http, "account", identity, config)
            await connection._socket(ingest, status.append)
            await connection._socket(ingest, status.append)
            await connection._socket(ingest, status.append)
            assert await connection.reply(messages[0], "reply @everyone", {}) == "201"
        assert identity == BOT_ID
        assert addresses == [GATEWAY, RESUME, GATEWAY]
        assert sockets[0].sent[0]["op"] == 2
        assert (
            sockets[0].sent[0]["d"]["intents"]
            == discord.GUILD_MESSAGES | discord.DIRECT_MESSAGES | discord.MESSAGE_CONTENT
        )
        assert sockets[0].sent[1] == {"op": 1, "d": 3}
        assert sockets[1].sent[0] == {"op": 6, "d": {"token": TOKEN, "session_id": "test-session", "seq": 3}}
        assert sockets[2].sent[0]["op"] == 2
        assert all(socket.close_codes == [4000] for socket in sockets)
        assert status == ["connected", "connected", "connected"]
        assert [(message.id, message.kind, message.bot_mentioned) for message in messages] == [
            ("101", "channel", True),
            ("103", "private", None),
        ]
        assert all(message.channel_id == "account" and message.conversation_id == CHANNEL_ID for message in messages)
        assert [request.method for request in requests] == ["GET", "GET", "POST"]

    asyncio.run(exercise())


def test_missing_heartbeat_ack_keeps_the_last_ingested_sequence(monkeypatch):
    async def exercise():
        socket = Socket(ready(), dispatch("MESSAGE_CREATE", 2, incoming()), interval=100)
        async with httpx.AsyncClient() as http:
            connection = discord.DiscordConnection(http, "account", BOT_ID, {"bot_token": TOKEN})
            connection.gateway = GATEWAY

            @asynccontextmanager
            async def connect(_url, **_kwargs):
                yield socket

            async def ingest(_message):
                raise sqlite3.OperationalError("busy")

            monkeypatch.setattr(discord, "connect", connect)
            monkeypatch.setattr(discord.random, "random", lambda: 0)
            with pytest.raises(sqlite3.OperationalError):
                await connection._socket(ingest, lambda _status: None)
            assert connection.session.sequence == 1

            socket = Socket(interval=100)
            await asyncio.wait_for(connection._socket(ingest, lambda _status: None), 1)
            assert socket.sent[0] == {"op": 6, "d": {"token": TOKEN, "session_id": "test-session", "seq": 1}}
            assert socket.sent[1] == {"op": 1, "d": 1}
            assert socket.close_codes == [4000]

    asyncio.run(exercise())


def test_binding_and_rest_failures_keep_credentials_private_and_only_retry_refusals():
    async def exercise():
        calls = []
        response_codes = iter((401, 429, 200, 503))

        def handler(request):
            calls.append(request.method)
            status = next(response_codes)
            if status == 429:
                return httpx.Response(status, json={"retry_after": 0})
            return httpx.Response(status, json={"id": "201", "message": TOKEN})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            with pytest.raises(ChannelError) as rejected:
                await discord.bind(http, {"bot_token": TOKEN})
            assert TOKEN not in str(rejected.value)
            assert not rejected.value.retryable
            connection = discord.DiscordConnection(http, "account", BOT_ID, {"bot_token": TOKEN})
            assert connection.intents == discord.GUILD_MESSAGES | discord.DIRECT_MESSAGES
            message = discord._message("account", BOT_ID, incoming())
            assert await connection.reply(message, "reply", {}) == "201"
            with pytest.raises(ChannelError) as uncertain:
                await connection.reply(message, "reply", {})
            assert uncertain.value.retryable
            assert TOKEN not in str(uncertain.value)
        assert calls == ["GET", "POST", "POST", "POST"]

    asyncio.run(exercise())


def test_message_content_intent_and_gateway_origin_validation():
    assert discord._message("account", BOT_ID, {**incoming(), "content": "", "mentions": []}) is None
    assert discord._message("account", BOT_ID, {**incoming(), "mentions": []}).bot_mentioned is False
    with pytest.raises(ChannelError, match="true or false"):
        discord._intents({"message_content": "maybe"})
    with pytest.raises(ValueError, match="endpoint"):
        discord._gateway_url("wss://gateway.discord.gg@example.com")
