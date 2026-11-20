"""Discord bot messaging over Gateway v10 and the HTTP API."""

import asyncio
import json
import random
import re
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

API = "https://discord.com/api/v10"
GUILD_MESSAGES = 1 << 9
DIRECT_MESSAGES = 1 << 12
MESSAGE_CONTENT = 1 << 15
FATAL_CLOSE_CODES = {4004, 4010, 4011, 4012, 4013, 4014}
RESET_CLOSE_CODES = {4007, 4009}
SNOWFLAKE = re.compile(r"[0-9]{1,20}")


class RateLimited(ChannelError):
    def __init__(self, delay: float | None):
        super().__init__("Discord rate limited the request. Try again later.")
        self.delay = delay


def _id(value: object) -> str:
    if not isinstance(value, str) or SNOWFLAKE.fullmatch(value) is None:
        raise ValueError("Invalid Discord ID")
    return value


def _token(config: Configuration) -> str:
    token = config.get("bot_token", "").strip()
    if not token or any(char.isspace() or not char.isascii() or not char.isprintable() for char in token):
        raise ChannelError("Enter a Discord bot token.")
    return token


def _intents(config: Configuration) -> int:
    message_content = config.get("message_content", "").strip().lower()
    if message_content not in {"", "false", "true"}:
        raise ChannelError("Set the Discord message content intent to true or false.")
    return GUILD_MESSAGES | DIRECT_MESSAGES | (MESSAGE_CONTENT if message_content == "true" else 0)


def _gateway_url(raw: str) -> str:
    url = urlsplit(raw)
    host = url.hostname or ""
    if (
        url.scheme != "wss"
        or not host.endswith(".discord.gg")
        or url.username is not None
        or url.password is not None
        or url.port not in {None, 443}
        or url.fragment
    ):
        raise ValueError("Unexpected Discord Gateway endpoint")
    return urlunsplit(("wss", url.netloc, url.path or "/", "v=10&encoding=json", ""))


def _object(raw: str | bytes) -> dict:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("Expected a Discord JSON object")
    return value


def _rate_limit(response: httpx.Response) -> float | None:
    try:
        value = response.headers.get("Retry-After") or _object(response.content).get("retry_after")
        delay = float(value)
    except (TypeError, ValueError):
        return None
    return delay if 0 <= delay < float("inf") else None


async def _request(http: httpx.AsyncClient, token: str, method: str, path: str, **kwargs) -> dict:
    try:
        response = await http.request(
            method, API + path, headers={"Authorization": f"Bot {token}"}, timeout=10, follow_redirects=False, **kwargs
        )
        if response.status_code == 429 and (delay := _rate_limit(response)) is not None and delay <= 5:
            # A rate-limit response explicitly rejected the request, so retrying it cannot duplicate a reply.
            await asyncio.sleep(delay)
            response = await http.request(
                method,
                API + path,
                headers={"Authorization": f"Bot {token}"},
                timeout=10,
                follow_redirects=False,
                **kwargs,
            )
        if response.status_code == 429:
            raise RateLimited(_rate_limit(response))
        if response.status_code == 408 or response.status_code >= 500:
            raise ChannelError("Discord did not confirm the request.", retryable=True)
        if not response.is_success:
            raise ChannelError("Discord rejected the request. Check the bot token and channel permissions.")
        return _object(response.content)
    except (httpx.RequestError, ValueError, TypeError):
        raise ChannelError("Discord did not confirm the request.", retryable=True) from None


async def bind(http: httpx.AsyncClient, config: Configuration) -> str:
    _intents(config)
    profile = await _request(http, _token(config), "GET", "/users/@me")
    try:
        if profile.get("bot") is True:
            return _id(profile.get("id"))
    except ValueError:
        pass
    raise ChannelError("Discord did not return a bot identity.")


def _message(channel_id: str, bot_id: str, data: dict) -> ChannelMessage | None:
    author = data.get("author")
    text = data.get("content")
    if (
        data.get("type") not in {0, 19}
        or not isinstance(author, dict)
        or author.get("id") == bot_id
        or not isinstance(text, str)
        or not text.strip()
        or not isinstance(data.get("timestamp"), str)
    ):
        return None
    try:
        sender_id = _id(author.get("id"))
        conversation_id = _id(data.get("channel_id"))
        message_id = _id(data.get("id"))
    except ValueError:
        return None
    guild_id = data.get("guild_id")
    mentions = data.get("mentions", [])
    mentioned = isinstance(mentions, list) and any(
        isinstance(user, dict) and user.get("id") == bot_id for user in mentions
    )
    return ChannelMessage(
        id=message_id,
        channel_id=channel_id,
        kind="channel" if guild_id else "private",
        conversation_id=conversation_id,
        sender_id=sender_id,
        text=text,
        timestamp=data["timestamp"],
        bot_mentioned=mentioned if guild_id else None,
    )


@dataclass
class GatewaySession:
    id: str
    url: str
    sequence: int


class DiscordConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, identity: str, config: Configuration):
        self.http = http
        self.channel_id = channel_id
        self.identity = _id(identity)
        self.token = _token(config)
        self.intents = _intents(config)
        self.gateway: str | None = None
        self.session: GatewaySession | None = None

    async def listen(self, cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        delay = 5
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            retry_after = 0
            try:
                await self._socket(ingest, status)
            except ConnectionClosed as error:
                code = error.rcvd.code if error.rcvd is not None else None
                if code in FATAL_CLOSE_CODES:
                    status("login_required")
                    return
                if code in RESET_CLOSE_CODES:
                    self.session = None
            except RateLimited as error:
                retry_after = error.delay or 0
            except ChannelError as error:
                if not error.retryable:
                    status("login_required")
                    return
            except (WebSocketException, OSError, ValueError, KeyError, TypeError, sqlite3.Error):
                # Platform exceptions can contain raw frames or connection data.
                pass
            status("reconnecting")
            if loop.time() - start > 60:
                delay = 5
            await asyncio.sleep(max(delay, retry_after))
            delay = min(delay * 2, 60)

    async def _socket(self, ingest: Ingest, status: Callable[[str], None]) -> None:
        if self.gateway is None:
            value = await _request(self.http, self.token, "GET", "/gateway/bot")
            self.gateway = _gateway_url(value["url"])
        url = self.session.url if self.session else self.gateway
        async with connect(url, proxy=None, open_timeout=10, close_timeout=3, max_size=8 << 20) as socket:
            hello = _object(await asyncio.wait_for(socket.recv(), timeout=10))
            if hello.get("op") != 10:
                raise ValueError("Expected Discord Hello")
            interval = float(hello["d"]["heartbeat_interval"]) / 1000
            if not 0.1 <= interval <= 300:
                raise ValueError("Invalid Discord heartbeat interval")
            await self._start_session(socket)
            await self._events(socket, interval, ingest, status)

    async def _start_session(self, socket: ClientConnection) -> None:
        if self.session:
            await socket.send(
                json.dumps(
                    {"op": 6, "d": {"token": self.token, "session_id": self.session.id, "seq": self.session.sequence}}
                )
            )
        else:
            await socket.send(
                json.dumps(
                    {
                        "op": 2,
                        "d": {
                            "token": self.token,
                            "intents": self.intents,
                            "properties": {"os": "python", "browser": "a13n-channels", "device": "a13n-channels"},
                        },
                    }
                )
            )

    async def _events(
        self, socket: ClientConnection, interval: float, ingest: Ingest, status: Callable[[str], None]
    ) -> None:
        loop = asyncio.get_running_loop()
        next_beat = loop.time() + interval * random.random()
        awaiting_ack = False
        while True:
            wait = next_beat - loop.time()
            if wait <= 0:
                if awaiting_ack:
                    await socket.close(code=4000)
                    return
                await self._heartbeat(socket)
                awaiting_ack = True
                next_beat = loop.time() + interval
                continue
            try:
                frame = _object(await asyncio.wait_for(socket.recv(), timeout=wait))
            except TimeoutError:
                continue
            opcode = frame.get("op")
            if opcode == 11:
                awaiting_ack = False
            elif opcode == 1:
                await self._heartbeat(socket)
                awaiting_ack = True
                next_beat = loop.time() + interval
            elif opcode in {7, 9}:
                if opcode == 9 and frame.get("d") is not True:
                    self.session = None
                await socket.close(code=4000)
                return
            elif opcode == 0:
                await self._dispatch(frame, ingest, status)

    async def _heartbeat(self, socket: ClientConnection) -> None:
        await socket.send(json.dumps({"op": 1, "d": self.session.sequence if self.session else None}))

    async def _dispatch(self, frame: dict, ingest: Ingest, status: Callable[[str], None]) -> None:
        event = frame.get("t")
        payload = frame.get("d")
        sequence = frame.get("s")
        if type(sequence) is not int:
            raise ValueError("Discord dispatch is missing a sequence number")
        if event == "READY":
            if payload["user"]["id"] != self.identity:
                raise ChannelError("Discord resumed with an unexpected bot identity.")
            self.session = GatewaySession(payload["session_id"], _gateway_url(payload["resume_gateway_url"]), sequence)
            status("connected")
        elif self.session:
            if event == "MESSAGE_CREATE" and isinstance(payload, dict):
                message = _message(self.channel_id, self.identity, payload)
                if message is not None:
                    await ingest(message)
            # Commit the in-memory resume position only after durable ingest.
            self.session.sequence = sequence
            if event == "RESUMED":
                status("connected")

    async def reply(self, message: ChannelMessage, text: str, context: Mapping[str, str]) -> str:
        try:
            channel_id = _id(message.conversation_id)
            message_id = _id(message.id)
        except ValueError:
            raise ChannelError("The Discord reply is missing a valid source message.") from None
        result = await _request(
            self.http,
            self.token,
            "POST",
            f"/channels/{channel_id}/messages",
            json={
                "content": text,
                "message_reference": {"type": 0, "message_id": message_id, "fail_if_not_exists": True},
                "allowed_mentions": {"parse": [], "replied_user": False},
            },
        )
        try:
            return _id(result.get("id"))
        except ValueError:
            raise ChannelError("Discord did not confirm the reply.", retryable=True) from None


ADAPTER = Adapter(
    key="discord",
    label="Discord",
    label_zh="Discord",
    kinds=("private", "channel"),
    fields=(
        CredentialField("bot_token", "Bot token", "机器人令牌"),
        CredentialField(
            "message_content",
            "Message Content Intent (true or false)",
            "消息内容意图（true 或 false）",
            secret=False,
            required=False,
        ),
    ),
    bind=bind,
    connect=DiscordConnection,
)
