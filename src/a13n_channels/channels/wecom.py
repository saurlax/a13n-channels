"""Receive and reply to Enterprise WeChat intelligent bot messages over WebSocket."""

import asyncio
import json
import logging
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from secrets import token_hex

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

GATEWAY = "wss://openws.work.weixin.qq.com"
HEARTBEAT_INTERVAL = 30
RECEIPT_TIMEOUT = 5
CALLBACK_REPLY_WINDOW = 4
MAX_TEXT_BYTES = 20_480
# Raw WebSocket debug frames contain bot secrets and user messages.
_WIRE_LOG = logging.getLogger("a13n_channels.wecom.wire")
_WIRE_LOG.disabled = True


class _CredentialsRejected(Exception):
    pass


class _ConnectionReplaced(Exception):
    pass


def _object(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _id(value: object) -> str:
    return value if isinstance(value, str) and 0 < len(value) <= 512 else ""


def _request_id(command: str) -> str:
    return f"{command}_{token_hex(8)}"


def _frame(command: str, request_id: str, body: dict | None = None) -> str:
    envelope = {"cmd": command, "headers": {"req_id": request_id}}
    if body is not None:
        envelope["body"] = body
    return json.dumps(envelope, ensure_ascii=False)


def _read_frame(raw: str | bytes) -> dict:
    try:
        return _object(json.loads(raw))
    except (ValueError, UnicodeDecodeError):
        return {}


def _receipt_ok(frame: dict) -> bool:
    return type(frame.get("errcode")) is int and frame["errcode"] == 0


def _content(body: dict) -> str:
    msgtype = body.get("msgtype")
    if msgtype in ("text", "voice"):
        content = _object(body.get(msgtype)).get("content")
        return content.strip() if isinstance(content, str) else ""
    if msgtype == "mixed":
        items = _object(body.get("mixed")).get("msg_item")
        if isinstance(items, list):
            return "\n".join(
                text for item in items if _object(item).get("msgtype") == "text" and (text := _content(item))
            )
    return ""


def _message(frame: dict, channel_id: str, bot_id: str, received_at: float) -> ChannelMessage | None:
    body = _object(frame.get("body"))
    if body.get("aibotid") != bot_id:
        return None
    message_id = _id(body.get("msgid"))
    user_id = _id(_object(body.get("from")).get("userid"))
    chat_type = body.get("chattype")
    if not message_id or not user_id or user_id == bot_id or chat_type not in ("single", "group"):
        return None
    conversation_id = user_id if chat_type == "single" else _id(body.get("chatid"))
    content = _content(body)
    if not conversation_id or not content:
        return None
    timestamp = body.get("create_time")
    if type(timestamp) is not int or not 0 < timestamp <= 253_402_300_799:
        timestamp = received_at
    return ChannelMessage(
        id=message_id,
        channel_id=channel_id,
        kind="private" if chat_type == "single" else "group",
        conversation_id=conversation_id,
        sender_id=user_id,
        text=content[:65_536],
        timestamp=datetime.fromtimestamp(timestamp, UTC).isoformat(),
        # Group message callbacks are sent when users address this intelligent bot.
        bot_mentioned=chat_type == "group",
    )


async def _bind(_http: httpx.AsyncClient, config: Configuration) -> str:
    bot_id = _id(config.get("bot_id"))
    secret = config.get("secret", "")
    if not bot_id or not secret.strip():
        raise ChannelError("Enter the WeCom intelligent bot's Bot ID and Secret.")
    # Opening another authenticated socket would displace a live listener during account edits.
    return bot_id


class _Socket:
    def __init__(self, socket: ClientConnection):
        self.socket = socket
        self.identity = token_hex(16)
        self.pending: dict[str, asyncio.Future[dict | None]] = {}

    async def authenticate(self, bot_id: str, secret: str) -> None:
        request_id = _request_id("aibot_subscribe")
        async with asyncio.timeout(RECEIPT_TIMEOUT):
            await self.socket.send(_frame("aibot_subscribe", request_id, {"bot_id": bot_id, "secret": secret}))
            while True:
                frame = _read_frame(await self.socket.recv())
                if _object(frame.get("headers")).get("req_id") != request_id or "errcode" not in frame:
                    continue
                if not _receipt_ok(frame):
                    raise _CredentialsRejected
                return

    async def command(self, command: str, request_id: str, body: dict | None = None) -> dict:
        if request_id in self.pending:
            raise ChannelError("A WeCom response to this message is awaiting confirmation.")
        pending: asyncio.Future[dict | None] = asyncio.get_running_loop().create_future()
        self.pending[request_id] = pending
        try:
            async with asyncio.timeout(RECEIPT_TIMEOUT):
                await self.socket.send(_frame(command, request_id, body))
                frame = await pending
        except (OSError, TimeoutError, WebSocketException):
            raise ChannelError("WeCom did not confirm the command.", retryable=True) from None
        finally:
            self.pending.pop(request_id, None)
            if not pending.done():
                pending.cancel()
        if frame is None or type(frame.get("errcode")) is not int:
            raise ChannelError("WeCom did not confirm the command.", retryable=True)
        if not _receipt_ok(frame):
            raise ChannelError("WeCom rejected the command.")
        return frame

    async def heartbeat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await self.command("ping", _request_id("ping"))

    async def read(self, channel_id: str, bot_id: str, ingest: Ingest) -> None:
        async for raw in self.socket:
            received_at = time.time()
            frame = _read_frame(raw)
            cmd = frame.get("cmd")
            request_id = _id(_object(frame.get("headers")).get("req_id"))
            if cmd == "aibot_msg_callback":
                message = _message(frame, channel_id, bot_id, received_at)
                if message:
                    await ingest(
                        message,
                        context={"req_id": request_id, "socket": self.identity, "received_at": str(received_at)},
                    )
            elif cmd == "aibot_event_callback":
                if _object(_object(frame.get("body")).get("event")).get("eventtype") == "disconnected_event":
                    raise _ConnectionReplaced
            elif "errcode" in frame and (future := self.pending.get(request_id)) is not None and not future.done():
                future.set_result(frame)

    def disconnect(self) -> None:
        for future in self.pending.values():
            if not future.done():
                future.set_result(None)


class WeComConnection:
    def __init__(self, _http: httpx.AsyncClient, channel_id: str, identity: str, config: Configuration):
        self.channel_id = channel_id
        self.bot_id = identity
        self.secret = config["secret"]
        self.active: _Socket | None = None

    async def listen(self, _cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        delay = 1
        while True:
            status("connecting" if delay == 1 else "reconnecting")
            try:
                async with connect(GATEWAY, ping_interval=None, open_timeout=10, logger=_WIRE_LOG) as websocket:
                    socket = _Socket(websocket)
                    await socket.authenticate(self.bot_id, self.secret)
                    self.active = socket
                    status("connected")
                    delay = 1
                    tasks = {
                        asyncio.create_task(socket.read(self.channel_id, self.bot_id, ingest)),
                        asyncio.create_task(socket.heartbeat()),
                    }
                    try:
                        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            task.result()
                    finally:
                        self.active = None
                        socket.disconnect()
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
            except (_CredentialsRejected, _ConnectionReplaced):
                status("error")
                # A rejected credential or a replacement connection requires an account change.
                await asyncio.Event().wait()
            except (OSError, TimeoutError, WebSocketException, ChannelError):
                pass
            status("reconnecting")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def reply(self, message: ChannelMessage, text: str, context: Mapping[str, str]) -> str:
        socket = self.active
        if socket is None:
            raise ChannelError("The WeCom bot is reconnecting.", retryable=True)
        if message.channel_id != self.channel_id or message.kind not in {"private", "group"}:
            raise ChannelError("Choose a message from this WeCom bot.")
        if len(text.encode()) > MAX_TEXT_BYTES:
            raise ChannelError("The WeCom reply exceeds the message size limit.")
        request_id = context.get("req_id")
        if (
            request_id
            and context.get("socket") == socket.identity
            and 0 <= time.time() - float(context["received_at"]) < CALLBACK_REPLY_WINDOW
        ):
            stream_id = _request_id("stream")
            frame = await socket.command(
                "aibot_respond_msg",
                request_id,
                {"msgtype": "stream", "stream": {"id": stream_id, "finish": True, "content": text}},
            )
            return _id(_object(frame.get("body")).get("msgid")) or stream_id

        # Agent runs often finish after WeCom's callback response window.
        request_id = _request_id("aibot_send_msg")
        frame = await socket.command(
            "aibot_send_msg",
            request_id,
            {
                "chatid": message.conversation_id,
                "chat_type": 1 if message.kind == "private" else 2,
                "msgtype": "markdown",
                "markdown": {"content": text},
            },
        )
        return _id(_object(frame.get("body")).get("msgid")) or request_id


ADAPTER = Adapter(
    key="wecom",
    label="WeCom",
    label_zh="企业微信",
    kinds=("private", "group"),
    fields=(
        CredentialField("bot_id", "Bot ID", "机器人 ID", secret=False),
        CredentialField("secret", "Bot Secret", "机器人密钥"),
    ),
    bind=_bind,
    connect=WeComConnection,
)
