"""DingTalk application bot through Stream Mode and session webhook replies."""

import asyncio
import json
import logging
import sqlite3
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

_OPEN_URL = "https://api.dingtalk.com/v1.0/gateway/connections/open"
_TOPIC = "/v1.0/im/bot/messages/get"
_REPLY_URL = "https://oapi.dingtalk.com/robot/sendBySession"
_KINDS = {"1": "private", "2": "group"}
_log = logging.getLogger(__name__)
_wire_log = _log.getChild("wire")
_wire_log.propagate = False
_wire_log.addHandler(logging.NullHandler())


class SessionURL(httpx.URL):
    """Keep the reply credential in the wire query only, including in HTTPX logs."""

    def __str__(self) -> str:
        return _REPLY_URL

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)!r})"


def _object(raw: object) -> dict | None:
    if not isinstance(raw, (str, bytes)):
        return None
    try:
        value = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _valid_webhook(url: str) -> bool:
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    return (
        parts.scheme == "https"
        and parts.netloc in {"oapi.dingtalk.com", "oapi.dingtalk.com:443"}
        and parts.path == "/robot/sendBySession"
        and not parts.fragment
        and bool(parse_qs(parts.query).get("session"))
    )


def _positive_milliseconds(value: object) -> int:
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and len(value) <= 16 and value.isascii() and value.isdecimal():
        return int(value)
    return 0


def _normalize(channel_id: str, data: dict, frame_time: object) -> tuple[ChannelMessage, dict[str, str]] | None:
    if data.get("msgtype") != "text" or not isinstance(data.get("text"), dict):
        return None
    text = _text(data["text"].get("content")).strip()
    message_id = _text(data.get("msgId"))
    conversation_id = _text(data.get("conversationId"))
    kind = _KINDS.get(_text(data.get("conversationType")))
    sender = _text(data.get("senderId"))
    bot = _text(data.get("chatbotUserId"))
    if not all((text, message_id, conversation_id, kind, sender)) or sender == bot:
        return None
    created = _positive_milliseconds(data.get("createAt")) or _positive_milliseconds(frame_time)
    if not created:
        return None
    try:
        timestamp = datetime.fromtimestamp(created / 1000, UTC).isoformat()
    except (ValueError, OverflowError, OSError):
        return None

    webhook = _text(data.get("sessionWebhook"))
    expires = _positive_milliseconds(data.get("sessionWebhookExpiredTime"))
    context = {"session_webhook": webhook, "expires_at_ms": str(expires)} if expires and _valid_webhook(webhook) else {}
    return ChannelMessage(
        id=message_id,
        channel_id=channel_id,
        kind=kind,
        conversation_id=conversation_id,
        sender_id=sender,
        text=text,
        timestamp=timestamp,
        bot_mentioned=data.get("isInAtList") is True,
    ), context


async def _post(http: httpx.AsyncClient, url: str | SessionURL, body: dict, action: str) -> httpx.Response:
    request = http.build_request("POST", url, json=body, headers={"Accept": "application/json"})
    if isinstance(url, SessionURL):
        request.url = url
    try:
        response = await http.send(request, follow_redirects=False)
    except httpx.RequestError:
        raise ChannelError(f"DingTalk did not confirm {action}.", retryable=True) from None
    if response.status_code in {408, 429} or response.status_code >= 500:
        raise ChannelError(f"DingTalk did not confirm {action}. Try again later.", retryable=True)
    if not response.is_success:
        raise ChannelError(f"DingTalk rejected {action}. Check the bot configuration and permissions.")
    return response


async def _stream_url(http: httpx.AsyncClient, config: Configuration) -> str:
    response = await _post(
        http,
        _OPEN_URL,
        {
            "clientId": config["client_id"],
            "clientSecret": config["client_secret"],
            "subscriptions": [{"type": "CALLBACK", "topic": _TOPIC}],
        },
        "the Stream registration",
    )
    data = _object(response.content) or {}
    ticket = _text(data.get("ticket"))
    try:
        endpoint = urlsplit(_text(data.get("endpoint")))
        if (
            not ticket
            or endpoint.scheme != "wss"
            or not (endpoint.hostname or "").endswith(".dingtalk.com")
            or endpoint.port not in (None, 443)
            or endpoint.username is not None
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError
    except ValueError:
        raise ChannelError("DingTalk returned an invalid Stream endpoint.", retryable=True) from None
    return urlunsplit(endpoint._replace(query=urlencode({"ticket": ticket})))


async def bind(http: httpx.AsyncClient, config: Configuration) -> str:
    await _stream_url(http, config)
    return config["client_id"]


def _ack(message_id: str, *, code: int, message: str, data: dict) -> str:
    return json.dumps(
        {
            "code": code,
            "headers": {"messageId": message_id, "contentType": "application/json"},
            "message": message,
            "data": json.dumps(data),
        }
    )


class DingTalkConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, _identity: str, config: Configuration):
        self.http = http
        self.channel_id = channel_id
        self.config = config

    async def listen(self, _cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        delay = 1
        while True:
            try:
                url = await _stream_url(self.http, self.config)
                async with connect(url, proxy=None, logger=_wire_log) as socket:
                    status("connected")
                    if await self._consume(socket, ingest):
                        delay = 1
            except ChannelError as error:
                if not error.retryable:
                    status("login_required")
                    return
            except (OSError, TimeoutError, WebSocketException):
                pass
            status("reconnecting")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    async def _consume(self, socket: ClientConnection, ingest: Ingest) -> bool:
        received = False
        async for raw in socket:
            frame = _object(raw)
            if frame is None or not isinstance(frame.get("headers"), dict):
                continue
            received = True
            headers = frame["headers"]
            topic, frame_type = headers.get("topic"), frame.get("type")
            if frame_type == "SYSTEM" and topic == "disconnect":
                break
            message_id = _text(headers.get("messageId"))
            if not message_id:
                continue
            data = _object(frame.get("data"))
            if frame_type == "SYSTEM" and topic == "ping" and data is not None:
                await socket.send(_ack(message_id, code=200, message="OK", data=data))
            elif frame_type == "CALLBACK" and topic == _TOPIC:
                await self._receive_callback(socket, ingest, message_id, data, headers.get("time"))
        return received

    async def _receive_callback(
        self, socket: ClientConnection, ingest: Ingest, ack_id: str, data: dict | None, timestamp: object
    ) -> None:
        if data is None:
            await socket.send(_ack(ack_id, code=400, message="Invalid callback", data={"response": None}))
            return
        item = _normalize(self.channel_id, data, timestamp)
        if item is not None:
            message, context = item
            try:
                await ingest(message, context=context)
            except (sqlite3.Error, ChannelError):
                _log.warning("Could not persist a DingTalk message for channel %s", self.channel_id)
                await socket.send(_ack(ack_id, code=500, message="Internal error", data={"response": None}))
                return
        await socket.send(_ack(ack_id, code=200, message="OK", data={"response": None}))

    async def reply(self, _message: ChannelMessage, text: str, context: Mapping[str, str]) -> str:
        url = context.get("session_webhook", "")
        expires = _positive_milliseconds(context.get("expires_at_ms"))
        if not _valid_webhook(url) or not expires:
            raise ChannelError("The DingTalk message has no reply session.")
        if expires <= time.time() * 1000:
            raise ChannelError("The DingTalk reply session expired. Send a new message to the bot to continue.")
        response = await _post(self.http, SessionURL(url), {"msgtype": "text", "text": {"content": text}}, "the reply")
        if response.content:
            data = _object(response.content)
            if data is None or type(data.get("errcode")) is not int:
                raise ChannelError("DingTalk returned an invalid reply confirmation.", retryable=True)
            if data["errcode"] != 0:
                raise ChannelError("DingTalk rejected the reply. Check the reply session and bot permissions.")
        # The session webhook acknowledges delivery without a platform message ID.
        return str(uuid4())


ADAPTER = Adapter(
    key="dingtalk",
    label="DingTalk",
    label_zh="钉钉",
    kinds=("private", "group"),
    fields=(
        CredentialField("client_id", "Client ID", "应用 ID", secret=False),
        CredentialField("client_secret", "Client Secret", "应用密钥"),
    ),
    bind=bind,
    connect=DingTalkConnection,
)
