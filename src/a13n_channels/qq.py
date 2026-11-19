"""Outbound QQ official Bot transport."""

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Annotated
from urllib.parse import quote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from a13n_channels.messages import ChannelMessage

QQ_TOKEN = "https://bots.qq.com/app/getAppAccessToken"
QQ_API = "https://api.sgroup.qq.com"
MESSAGE_PATHS = {"private": "/v2/users", "group": "/v2/groups", "guild": "/channels", "guild_dm": "/dms"}
QQ_INTENTS = sum(1 << bit for bit in (0, 10, 12, 25, 26, 27, 30))
MESSAGE_TYPES = {
    "C2C_MESSAGE_CREATE": ("private", "user_openid", None),
    "GROUP_AT_MESSAGE_CREATE": ("group", "member_openid", "group_openid"),
    "GROUP_MESSAGE_CREATE": ("group", "member_openid", "group_openid"),
    "AT_MESSAGE_CREATE": ("guild", "id", "channel_id"),
    "MESSAGE_CREATE": ("guild", "id", "channel_id"),
    "DIRECT_MESSAGE_CREATE": ("guild_dm", "id", "guild_id"),
}
QQ_CONVERSATION_KINDS = tuple(dict.fromkeys(kind for kind, _, _ in MESSAGE_TYPES.values()))

type ChannelId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")]
type AppId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.:@-]{1,128}$")]


class QQSecret(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_secret: SecretStr = Field(min_length=1, max_length=4096)

    @field_validator("app_secret")
    @classmethod
    def ascii_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().isascii():
            raise ValueError("QQ AppSecret must be ASCII")
        return value


class QQCreate(QQSecret):
    name: str = Field(min_length=1, max_length=80)
    app_id: AppId
    qq_intents: int = Field(default=QQ_INTENTS, ge=1, le=2147483647)


class QQAccount(BaseModel):
    id: ChannelId
    name: str
    app_id: AppId
    state: str


@dataclass(slots=True)
class Resume:
    session_id: str | None = None
    sequence: int | None = None


def message(channel_id: str, packet: dict) -> ChannelMessage | None:
    event = packet.get("t")
    if event not in MESSAGE_TYPES:
        return None
    kind, author_field, conversation_field = MESSAGE_TYPES[event]
    data = packet["d"]
    if data["author"].get("bot") is True:
        return None
    sender = data["author"][author_field]
    bot_mentioned = None
    if kind in {"group", "guild"}:
        mentions = data.get("mentions")
        bot_mentioned = event in {"GROUP_AT_MESSAGE_CREATE", "AT_MESSAGE_CREATE"} or (
            isinstance(mentions, list)
            and any(isinstance(mention, dict) and mention.get("is_you") is True for mention in mentions)
        )
    return ChannelMessage(
        id=data["id"],
        channel_id=channel_id,
        kind=kind,
        conversation_id=data[conversation_field] if conversation_field else sender,
        sender_id=sender,
        text=(data.get("content") or "").strip(),
        timestamp=data["timestamp"],
        bot_mentioned=bot_mentioned,
    )


class QQConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, credentials: QQCreate):
        self.http = http
        self.channel_id = channel_id
        self.credentials = credentials
        self.token = ""
        self.expires = 0.0
        self.resume = Resume()

    async def _access_token(self) -> str:
        if time.monotonic() >= self.expires:
            response = await self.http.post(
                QQ_TOKEN,
                json={"appId": self.credentials.app_id, "clientSecret": self.credentials.app_secret.get_secret_value()},
            )
            response.raise_for_status()
            data = response.json()
            token, lifetime = data["access_token"], float(data["expires_in"])
            if not isinstance(token, str) or not token or not 0 < lifetime <= 86400:
                raise ValueError("Invalid QQ access token")
            self.token = token
            self.expires = time.monotonic() + lifetime - min(60, lifetime / 3)
        return self.token

    async def reply(self, incoming: ChannelMessage, text: str, sequence: int) -> str:
        token = await self._access_token()
        target = quote(incoming.conversation_id, safe="")
        body = {"content": text, "msg_id": incoming.id, "msg_seq": sequence}
        if incoming.kind in {"private", "group"}:
            body["msg_type"] = 0
        response = await self.http.post(
            f"{QQ_API}{MESSAGE_PATHS[incoming.kind]}/{target}/messages",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
        )
        response.raise_for_status()
        message_id = response.json()["id"]
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("QQ omitted the sent message ID")
        return message_id

    async def verify_credentials(self) -> None:
        await self._access_token()

    async def test_connection(self) -> None:
        async def discard(_message: ChannelMessage) -> None:
            pass

        async with asyncio.timeout(20):
            ready = await self._session(discard, lambda _state: None, stop_after_ready=True)
        if not ready:
            raise ValueError("QQ closed the connection before sending READY")

    async def listen(self, receive: Callable[[ChannelMessage], Awaitable[None]], status: Callable[[str], None]) -> None:
        delay = 1
        while True:
            try:
                await self._session(receive, status)
                delay = 1
            except (httpx.HTTPError, WebSocketException, OSError, ValueError, KeyError, TypeError):
                # Errors from the platform can contain credentials and raw payloads.
                pass
            status("reconnecting")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _session(
        self,
        receive: Callable[[ChannelMessage], Awaitable[None]],
        status: Callable[[str], None],
        *,
        stop_after_ready: bool = False,
    ) -> bool:
        token = "QQBot " + await self._access_token()
        response = await self.http.get(QQ_API + "/gateway", headers={"Authorization": token})
        if response.status_code == 401:
            self.expires = 0
        response.raise_for_status()
        url = response.json()["url"]
        parsed = urlsplit(url)
        if (
            parsed.scheme != "wss"
            or not (parsed.hostname or "").endswith(".qq.com")
            or parsed.username
            or parsed.password
        ):
            raise ValueError("Invalid QQ WebSocket URL")

        async with connect(url, proxy=None, open_timeout=15, max_size=1 << 20) as socket:
            hello = json.loads(await asyncio.wait_for(socket.recv(), 15))
            if hello.get("op") != 10:
                raise ValueError("Expected QQ Hello")
            interval = float(hello["d"]["heartbeat_interval"]) / 1000
            if not 0.1 <= interval <= 300:
                raise ValueError("Invalid QQ heartbeat interval")
            if self.resume.session_id is not None and self.resume.sequence is not None:
                await socket.send(
                    json.dumps(
                        {
                            "op": 6,
                            "d": {
                                "token": token,
                                "session_id": self.resume.session_id,
                                "seq": self.resume.sequence,
                            },
                        }
                    )
                )
            else:
                await socket.send(
                    json.dumps(
                        {
                            "op": 2,
                            "d": {
                                "token": token,
                                "intents": self.credentials.qq_intents,
                                "shard": [0, 1],
                            },
                        }
                    )
                )

            ack = asyncio.Event()
            ack.set()

            async def heartbeat() -> None:
                while True:
                    await asyncio.sleep(interval)
                    if not ack.is_set():
                        await socket.close()
                        return
                    ack.clear()
                    await socket.send(json.dumps({"op": 1, "d": self.resume.sequence}))

            beat = asyncio.create_task(heartbeat())
            try:
                async for raw in socket:
                    packet = json.loads(raw)
                    match packet.get("op"):
                        case 0:
                            if packet.get("t") == "READY":
                                self.resume.session_id = packet["d"]["session_id"]
                                status("connected")
                                if stop_after_ready:
                                    return True
                            elif packet.get("t") == "RESUMED":
                                status("connected")
                            else:
                                try:
                                    incoming = message(self.channel_id, packet)
                                except (ValueError, KeyError, TypeError, AttributeError):
                                    incoming = None
                                if incoming is not None and not stop_after_ready:
                                    await receive(incoming)
                            if isinstance(packet.get("s"), int):
                                self.resume.sequence = packet["s"]
                        case 1:
                            await socket.send(json.dumps({"op": 1, "d": self.resume.sequence}))
                        case 7:
                            return False
                        case 9:
                            if not packet.get("d"):
                                self.resume = Resume()
                                self.expires = 0
                            return False
                        case 11:
                            ack.set()
            finally:
                beat.cancel()
                with suppress(asyncio.CancelledError, WebSocketException):
                    await beat
                if socket.close_code in {4006, 4007, 4009}:
                    self.resume = Resume()
                if socket.close_code == 4004:
                    self.expires = 0
        return False
