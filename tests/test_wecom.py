import asyncio
import json
import time
from contextlib import asynccontextmanager

import httpx
import pytest

from a13n_channels.channels import wecom
from a13n_channels.channels.adapter import ChannelError


def callback(**body):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "callback-id"},
        "body": {
            "msgid": "message-id",
            "aibotid": "bot-id",
            "chattype": "single",
            "from": {"userid": "alice"},
            "create_time": 1_700_000_000,
            "msgtype": "text",
            "text": {"content": "hello"},
            **body,
        },
    }


def test_private_and_group_conversation_ids_are_platform_ids():
    private = wecom._message(callback(), "channel", "bot-id", 0)
    assert private.id == "message-id"
    assert (private.kind, private.conversation_id, private.sender_id) == ("private", "alice", "alice")
    assert private.timestamp == "2023-11-14T22:13:20+00:00"

    group = wecom._message(
        callback(
            chattype="group",
            chatid="group-id",
            msgtype="mixed",
            mixed={
                "msg_item": [
                    {"msgtype": "text", "text": {"content": "first"}},
                    {"msgtype": "image", "image": {"url": "https://example.com/secret", "aeskey": "private"}},
                    {"msgtype": "text", "text": {"content": "second"}},
                ]
            },
        ),
        "channel",
        "bot-id",
        0,
    )
    assert (group.kind, group.conversation_id, group.bot_mentioned) == ("group", "group-id", True)
    assert group.text == "first\nsecond"
    assert "secret" not in group.model_dump_json()


@pytest.mark.parametrize(
    "body",
    [
        {"aibotid": "other-bot"},
        {"chattype": "group"},
        {"from": {"userid": "bot-id"}},
        {"msgtype": "image", "image": {"url": "https://example.com/image.png"}},
    ],
)
def test_ignores_unsupported_wrong_bot_and_self_messages(body):
    assert wecom._message(callback(**body), "channel", "bot-id", 0) is None


class Socket:
    def __init__(self, *, auth_code=0):
        self.inbound = asyncio.Queue()
        self.commands = asyncio.Queue()
        self.auth_code = auth_code
        self.closed = False

    async def send(self, raw):
        frame = json.loads(raw)
        if frame["cmd"] == "aibot_subscribe":
            await self.ack(frame, self.auth_code)
        await self.commands.put(frame)

    async def recv(self):
        return json.dumps(await self.inbound.get())

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv()

    async def ack(self, request, code=0, *, body=None):
        await self.inbound.put({"headers": request["headers"], "errcode": code, "body": body})


def test_auth_dispatch_and_reply_wait_for_matching_receipts(monkeypatch):
    async def exercise():
        fake = Socket()

        @asynccontextmanager
        async def connect(url, **options):
            assert url == wecom.GATEWAY
            assert options["ping_interval"] is None
            try:
                yield fake
            finally:
                fake.closed = True

        monkeypatch.setattr(wecom, "connect", connect)
        async with httpx.AsyncClient() as http:
            config = {"bot_id": "bot-id", "secret": "bot-secret"}
            assert await wecom.ADAPTER.bind(http, config) == "bot-id"
            connection = wecom.ADAPTER.connect(http, "channel", "bot-id", config)
            status = asyncio.Queue()
            received = asyncio.Queue()

            async def ingest(message, *, context):
                await received.put((message, context))

            task = asyncio.create_task(connection.listen("", ingest, status.put_nowait))
            try:
                async with asyncio.timeout(1):
                    auth = await fake.commands.get()
                    assert auth["cmd"] == "aibot_subscribe"
                    assert auth["body"] == config
                    assert await status.get() == "connecting"
                    assert await status.get() == "connected"
                    await fake.inbound.put(callback(response_url="https://example.com/private"))
                    message, context = await received.get()
                    assert context["req_id"] == "callback-id"
                    assert "callback-id" not in message.model_dump_json()
                    assert "example.com" not in message.model_dump_json()

                    quick = asyncio.create_task(connection.reply(message, "hello", context))
                    answer = await fake.commands.get()
                    assert answer["cmd"] == "aibot_respond_msg"
                    assert answer["headers"] == {"req_id": "callback-id"}
                    assert answer["body"]["stream"]["finish"] is True
                    await fake.inbound.put({"headers": {"req_id": "unrelated"}, "errcode": 0})
                    assert not quick.done()
                    await fake.ack(answer, body={"msgid": "reply-1"})
                    assert await quick == "reply-1"

                    delayed = asyncio.create_task(
                        connection.reply(message, "done", {**context, "received_at": str(time.time() - 10)})
                    )
                    answer = await fake.commands.get()
                    assert answer["cmd"] == "aibot_send_msg"
                    assert answer["body"] == {
                        "chatid": "alice",
                        "chat_type": 1,
                        "msgtype": "markdown",
                        "markdown": {"content": "done"},
                    }
                    await fake.ack(answer, 40001)
                    with pytest.raises(ChannelError, match="rejected") as rejected:
                        await delayed
                    assert not rejected.value.retryable
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            assert fake.closed
            assert connection.active is None

    asyncio.run(exercise())


def test_heartbeat_and_missing_send_receipt(monkeypatch):
    async def exercise():
        fake = Socket()
        monkeypatch.setattr(wecom, "HEARTBEAT_INTERVAL", 0.005)
        monkeypatch.setattr(wecom, "RECEIPT_TIMEOUT", 0.1)
        socket = wecom._Socket(fake)
        heartbeat = asyncio.create_task(socket.heartbeat())

        async def ingest(*args, **kwargs):
            raise AssertionError("No message expected.")

        reader = asyncio.create_task(socket.read("channel", "bot-id", ingest))
        try:
            async with asyncio.timeout(1):
                ping = await fake.commands.get()
                assert ping["cmd"] == "ping"
                await fake.ack(ping)
                assert (await fake.commands.get())["cmd"] == "ping"
        finally:
            for task in (heartbeat, reader):
                task.cancel()
            await asyncio.gather(heartbeat, reader, return_exceptions=True)

        with pytest.raises(ChannelError, match="did not confirm") as uncertain:
            await socket.command("aibot_send_msg", "send")
        assert uncertain.value.retryable
        assert not socket.pending

    asyncio.run(exercise())


def test_auth_rejection_surfaces_connection_error_without_another_socket(monkeypatch):
    async def exercise():
        connections = []
        fake = Socket(auth_code=40001)

        @asynccontextmanager
        async def connect(*args, **kwargs):
            connections.append(fake)
            yield fake

        monkeypatch.setattr(wecom, "connect", connect)
        states = asyncio.Queue()

        async def ingest(*args, **kwargs):
            raise AssertionError("No message expected.")

        async with httpx.AsyncClient() as http:
            connection = wecom.WeComConnection(http, "channel", "bot-id", {"secret": "bot-secret"})
            task = asyncio.create_task(connection.listen("", ingest, states.put_nowait))
            try:
                async with asyncio.timeout(1):
                    assert await states.get() == "connecting"
                    assert await states.get() == "error"
                    assert connection.active is None
                    assert len(connections) == 1
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())
