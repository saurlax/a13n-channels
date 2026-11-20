"""Feishu custom-app bot using the outbound event WebSocket and IM reply API."""

import asyncio
import json
import logging
import re
import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlsplit

import httpx
from a13n_service.infra.errors import ServiceError
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import DecodeError, Message
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

ORIGIN = "https://open.feishu.cn"
FRAME_LIMIT = 1 << 20
CONTROL, DATA = 0, 1
MESSAGE_ID = re.compile(r"om_[A-Za-z0-9_-]{1,128}")
BOT_ID = re.compile(r"ou_[A-Za-z0-9_-]{1,128}")
log = logging.getLogger(__name__)
# The WebSocket URL contains a temporary ticket. Keep it out of handshake diagnostics.
wire_log = logging.getLogger(__name__ + ".wire")
wire_log.propagate = False
wire_log.addHandler(logging.NullHandler())


def _frame_type() -> type[Message]:
    """Build the pbbp2 proto2 frame declared by the Feishu SDK."""
    schema = descriptor_pb2.FileDescriptorProto(name="feishu-pbbp2.proto", package="pbbp2", syntax="proto2")
    header = schema.message_type.add(name="Header")
    frame = schema.message_type.add(name="Frame")
    field_type = descriptor_pb2.FieldDescriptorProto
    for number, name in enumerate(("key", "value"), 1):
        header.field.add(name=name, number=number, type=field_type.TYPE_STRING, label=field_type.LABEL_REQUIRED)
    for number, name, type_ in (
        (1, "SeqID", field_type.TYPE_UINT64),
        (2, "LogID", field_type.TYPE_UINT64),
        (3, "service", field_type.TYPE_INT32),
        (4, "method", field_type.TYPE_INT32),
    ):
        frame.field.add(name=name, number=number, type=type_, label=field_type.LABEL_REQUIRED)
    frame.field.add(
        name="headers",
        number=5,
        type=field_type.TYPE_MESSAGE,
        label=field_type.LABEL_REPEATED,
        type_name=".pbbp2.Header",
    )
    for number, name, type_ in (
        (6, "payload_encoding", field_type.TYPE_STRING),
        (7, "payload_type", field_type.TYPE_STRING),
        (8, "payload", field_type.TYPE_BYTES),
        (9, "LogIDNew", field_type.TYPE_STRING),
    ):
        frame.field.add(name=name, number=number, type=type_, label=field_type.LABEL_OPTIONAL)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(schema)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("pbbp2.Frame"))


Frame = _frame_type()


def _mapping(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _decode(value: str | bytes) -> dict:
    try:
        return _mapping(json.loads(value))
    except (json.JSONDecodeError, UnicodeError):
        return {}


def _post_text(content: dict) -> str:
    rows = content.get("content")
    if not isinstance(rows, list):
        return ""
    lines = [_text(content.get("title"))]
    for row in rows:
        if isinstance(row, list):
            lines.append("".join(_text(_mapping(part).get("text")) for part in row))
    return "\n".join(line for line in lines if line)


def normalize(payload: dict, channel_id: str, app_id: str, bot_id: str) -> ChannelMessage | None:
    header = _mapping(payload.get("header"))
    if header.get("event_type") != "im.message.receive_v1" or header.get("app_id") != app_id:
        return None
    event = _mapping(payload.get("event"))
    sender = _mapping(event.get("sender"))
    incoming = _mapping(event.get("message"))
    sender_id = _text(_mapping(sender.get("sender_id")).get("open_id"))
    message_id = _text(incoming.get("message_id"))
    conversation_id = _text(incoming.get("chat_id"))
    kind = {"p2p": "private", "group": "group"}.get(_text(incoming.get("chat_type")))
    if (
        sender.get("sender_type") != "user"
        or not sender_id
        or sender_id == bot_id
        or not MESSAGE_ID.fullmatch(message_id)
        or not conversation_id
        or not kind
    ):
        return None

    content = _decode(_text(incoming.get("content")))
    if incoming.get("message_type") == "text":
        text = _text(content.get("text"))
    elif incoming.get("message_type") == "post":
        text = _post_text(content)
    else:
        return None

    mentions = incoming.get("mentions")
    replacements = {}
    bot_mentioned = False
    for item in mentions if isinstance(mentions, list) else ():
        mention = _mapping(item)
        key = _text(mention.get("key"))
        addressed = _mapping(mention.get("id")).get("open_id") == bot_id
        bot_mentioned |= addressed
        if key:
            name = _text(mention.get("name"))
            replacements[key] = "" if addressed else f"@{name}" if name else key
    if replacements:
        pattern = "|".join(re.escape(key) for key in sorted(replacements, key=len, reverse=True))
        text = re.sub(pattern, lambda match: replacements[match[0]], text)
    if not text.strip():
        if not bot_mentioned:
            return None
        text = "[Bot mentioned]"

    created = incoming.get("create_time") or header.get("create_time")
    try:
        timestamp = datetime.fromtimestamp(int(created) / 1_000, UTC).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return ChannelMessage(
        id=message_id,
        channel_id=channel_id,
        kind=kind,
        conversation_id=conversation_id,
        sender_id=sender_id,
        text=text.strip()[:60_000],
        timestamp=timestamp,
        bot_mentioned=bot_mentioned,
        structured={key: value for key in ("thread_id", "root_id", "parent_id") if (value := _text(incoming.get(key)))},
    )


async def _request(http: httpx.AsyncClient, method: str, path: str, action: str, **kwargs) -> dict:
    try:
        response = await http.request(method, ORIGIN + path, timeout=10.0, follow_redirects=False, **kwargs)
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        raise ChannelError(
            f"Feishu rejected {action} (HTTP {status}).",
            retryable=status in {408, 429} or status >= 500,
        ) from None
    except httpx.RequestError:
        raise ChannelError(f"Feishu could not confirm {action}.", retryable=True) from None

    payload = _decode(response.content)
    code = payload.get("code")
    if type(code) is not int:
        raise ChannelError(f"Feishu returned an invalid response for {action}.", retryable=True)
    if code != 0:
        raise ChannelError(
            f"Feishu rejected {action} (code {code}). Check the app credentials, permissions and event settings.",
            retryable=code in {1, 1000040343, 99991400},
        )
    return payload


async def _tenant_token(http: httpx.AsyncClient, config: Configuration) -> tuple[str, int]:
    payload = await _request(
        http,
        "POST",
        "/open-apis/auth/v3/tenant_access_token/internal",
        "app authentication",
        json={"app_id": config["app_id"], "app_secret": config["app_secret"]},
    )
    token = _text(payload.get("tenant_access_token"))
    if not token:
        raise ChannelError("Feishu did not issue a tenant access token.", retryable=True)
    expire = payload.get("expire")
    return token, expire if type(expire) is int and expire > 0 else 0


async def bind(http: httpx.AsyncClient, config: Configuration) -> str:
    token, _ = await _tenant_token(http, config)
    payload = await _request(
        http,
        "GET",
        "/open-apis/bot/v3/info",
        "bot lookup",
        headers={"Authorization": f"Bearer {token}"},
    )
    bot_id = _text(_mapping(payload.get("bot")).get("open_id"))
    if not BOT_ID.fullmatch(bot_id):
        raise ChannelError("Feishu returned no bot identity. Enable the bot capability for this custom app.")
    return bot_id


@dataclass
class Fragments:
    count: int
    expires: float
    parts: dict[int, bytes] = field(default_factory=dict)


class EventFrames:
    def __init__(self, channel_id: str, app_id: str, bot_id: str):
        self.channel_id = channel_id
        self.app_id = app_id
        self.bot_id = bot_id
        self.pending: dict[str, Fragments] = {}

    def _assemble(self, payload: bytes, headers: dict[str, str]) -> bytes | None:
        try:
            count, index = int(headers.get("sum", "1")), int(headers.get("seq", "0"))
        except ValueError:
            return None
        if not 1 <= count <= 16 or not 0 <= index < count:
            return None
        if count == 1:
            return payload
        key = headers.get("message_id")
        if not key:
            return None
        now = time.monotonic()
        self.pending = {key: group for key, group in self.pending.items() if group.expires > now}
        group = self.pending.get(key)
        if group is None:
            if len(self.pending) >= 8:
                return None
            group = self.pending[key] = Fragments(count, now + 5)
        if group.count != count:
            self.pending.pop(key)
            return None
        group.parts[index] = payload
        if sum(map(len, group.parts.values())) > FRAME_LIMIT:
            self.pending.pop(key)
            return None
        if len(group.parts) == count:
            self.pending.pop(key)
            return b"".join(group.parts[index] for index in range(count))
        return None

    async def receive(self, raw: bytes, socket: ClientConnection, ingest: Ingest) -> dict | None:
        try:
            frame = Frame.FromString(raw)
        except DecodeError:
            return None
        if not frame.IsInitialized():
            return None
        headers = {header.key: header.value for header in frame.headers}
        if frame.method == CONTROL and headers.get("type") == "pong":
            return _decode(frame.payload)
        if frame.method != DATA or headers.get("type") != "event":
            return None
        payload = self._assemble(frame.payload, headers)
        if payload is None:
            return None
        start = time.monotonic()
        message = normalize(_decode(payload), self.channel_id, self.app_id, self.bot_id)
        code = 200
        if message is not None:
            try:
                # The platform expects an acknowledgement within three seconds.
                async with asyncio.timeout(2.5):
                    await ingest(message)
            except (ChannelError, ServiceError, sqlite3.Error, TimeoutError) as error:
                log.warning("Feishu durable ingestion failed (%s)", type(error).__name__)
                code = 500
        frame.headers.add(key="biz_rt", value=str(round((time.monotonic() - start) * 1000)))
        frame.payload = json.dumps({"code": code}).encode()
        await socket.send(frame.SerializeToString())
        return None


def _seconds(config: dict, field: str, default: int) -> int:
    value = config.get(field)
    return min(value, 300) if type(value) is int and value > 0 else default


class FeishuConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, identity: str, config: Configuration):
        self.http = http
        self.channel_id = channel_id
        self.bot_id = identity
        self.config = config
        self.token = ""
        self.token_expires = 0.0
        self.token_lock = asyncio.Lock()

    async def _endpoint(self) -> tuple[str, int, dict]:
        payload = await _request(
            self.http,
            "POST",
            "/callback/ws/endpoint",
            "long connection discovery",
            headers={"locale": "zh"},
            json={"AppID": self.config["app_id"], "AppSecret": self.config["app_secret"]},
        )
        data = _mapping(payload.get("data"))
        url = _text(data.get("URL"))
        parsed = urlsplit(url)
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "wss"
            or not hostname.endswith(".feishu.cn")
            or parsed.port not in {None, 443}
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ChannelError("Feishu returned an invalid long connection endpoint.", retryable=True)
        try:
            service_id = int(parse_qs(parsed.query)["service_id"][0])
            if not -(1 << 31) <= service_id < (1 << 31):
                raise ValueError
        except (ValueError, KeyError, IndexError):
            raise ChannelError("Feishu returned an invalid long connection service ID.", retryable=True) from None
        return url, service_id, _mapping(data.get("ClientConfig"))

    async def listen(self, _cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        delay = 2
        while True:
            try:
                url, service_id, config = await self._endpoint()
                async with connect(
                    url,
                    open_timeout=10,
                    close_timeout=2,
                    max_size=FRAME_LIMIT,
                    proxy=None,
                    logger=wire_log,
                ) as socket:
                    status("connected")
                    delay = _seconds(config, "ReconnectInterval", 2)
                    await self._consume(socket, service_id, config, ingest)
            except ChannelError as error:
                status("reconnecting" if error.retryable else "error")
                log.info("Feishu long connection unavailable for channel %s", self.channel_id)
            except (WebSocketException, TimeoutError, OSError, ValueError):
                status("reconnecting")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max(60, delay))

    async def _consume(self, socket: ClientConnection, service_id: int, config: dict, ingest: Ingest) -> None:
        frames = EventFrames(self.channel_id, self.config["app_id"], self.bot_id)
        interval = _seconds(config, "PingInterval", 120)
        next_ping = time.monotonic()
        pending_pong = False
        while True:
            remaining = next_ping - time.monotonic()
            if remaining <= 0:
                if pending_pong:
                    raise TimeoutError("Feishu missed a heartbeat")
                ping = Frame(SeqID=0, LogID=0, service=service_id, method=CONTROL)
                ping.headers.add(key="type", value="ping")
                await socket.send(ping.SerializeToString())
                pending_pong = True
                next_ping = time.monotonic() + interval
                remaining = interval
            try:
                async with asyncio.timeout(remaining):
                    raw = await socket.recv()
            except TimeoutError:
                continue
            pong = await frames.receive(raw, socket, ingest) if isinstance(raw, bytes) else None
            if pong is not None:
                pending_pong = False
                interval = _seconds(pong, "PingInterval", interval)
                next_ping = time.monotonic() + interval

    async def reply(self, message: ChannelMessage, text: str, _context: Mapping[str, str]) -> str:
        if not MESSAGE_ID.fullmatch(message.id):
            raise ChannelError("The source message is missing a Feishu message ID.")
        async with self.token_lock:
            if time.monotonic() >= self.token_expires:
                self.token, expire = await _tenant_token(self.http, self.config)
                self.token_expires = time.monotonic() + max(0, expire - 60)
            token = self.token

        fields = {"msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)}
        if message.kind == "group" and message.structured.get("thread_id"):
            fields["reply_in_thread"] = True
        payload = await _request(
            self.http,
            "POST",
            f"/open-apis/im/v1/messages/{message.id}/reply",
            "the reply",
            headers={"Authorization": f"Bearer {token}"},
            json=fields,
        )
        sent_id = _text(_mapping(payload.get("data")).get("message_id"))
        if not MESSAGE_ID.fullmatch(sent_id):
            raise ChannelError("Feishu did not confirm the reply with a message ID.", retryable=True)
        return sent_id


ADAPTER = Adapter(
    key="feishu",
    label="Feishu",
    label_zh="飞书",
    kinds=("private", "group"),
    fields=(
        CredentialField("app_id", "App ID", "应用 ID", secret=False),
        CredentialField("app_secret", "App Secret", "应用密钥"),
    ),
    bind=bind,
    connect=FeishuConnection,
)
