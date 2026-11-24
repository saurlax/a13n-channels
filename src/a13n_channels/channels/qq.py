"""Outbound QQ official Bot transport."""

import asyncio
import base64
import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Annotated, Literal
from urllib.parse import quote, unquote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from a13n_channels.channels.qq_content import (
    MEDIA_LIMITS,
    MEDIA_TYPES,
    QQArk,
    QQEmbed,
    QQInbound,
    QQMarkdown,
    QQMedia,
    QQRichContent,
    image_type,
    media_url,
    project_content,
    validate_rich,
)
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

type QQConversationKind = Literal["private", "group", "guild", "guild_dm"]


type ChannelId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")]
type AppId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.:@-]{1,128}$")]


class QQRejected(Exception):
    """QQ returned a definitive application-level error."""


class QQPartPending(Exception):
    """QQ has not yet confirmed an uploaded part."""


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


def message(channel_id: str, packet: dict) -> QQInbound | None:
    event = packet.get("t")
    if event == "INTERACTION_CREATE":
        return interaction(channel_id, packet)
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
    text, attachments, urls, structured = project_content(data)
    return QQInbound(
        id=data["id"],
        channel_id=channel_id,
        kind=kind,
        conversation_id=data[conversation_field] if conversation_field else sender,
        sender_id=sender,
        text=text,
        timestamp=data["timestamp"],
        bot_mentioned=bot_mentioned,
        attachments=attachments,
        attachment_urls=urls,
        structured=structured,
    )


def interaction(channel_id: str, packet: dict) -> QQInbound | None:
    data = packet["d"]
    if data.get("type") not in {11, 12}:
        return None
    resolved = (data.get("data") or {}).get("resolved") or {}
    kind = {"c2c": "private", "group": "group", "guild": "guild"}.get(data.get("scene"))
    if kind is None:
        kind = {2: "private", 1: "group", 0: "guild"}.get(data.get("chat_type"))
    if kind == "private":
        sender = conversation = data.get("user_openid") or resolved.get("user_id")
    elif kind == "group":
        sender, conversation = data.get("group_member_openid"), data.get("group_openid")
    elif kind == "guild":
        sender, conversation = resolved.get("user_id"), data.get("channel_id")
    else:
        return None
    interaction_id = data.get("id")
    event_id = packet.get("id") or interaction_id
    if not all(
        isinstance(value, str) and 0 < len(value) <= 1024
        for value in (
            sender,
            conversation,
            interaction_id,
            event_id,
        )
    ):
        return None
    detail = {
        key: resolved[key][:4096]
        for key in ("button_data", "button_id", "feature_id", "message_id")
        if isinstance(resolved.get(key), str)
    }
    return QQInbound(
        id="interaction:" + hashlib.sha256(interaction_id.encode()).hexdigest(),
        channel_id=channel_id,
        kind=kind,
        conversation_id=conversation,
        sender_id=sender,
        text="[QQ interaction] " + json.dumps(detail, ensure_ascii=False),
        timestamp=data.get("timestamp") or datetime.now(UTC).isoformat(),
        bot_mentioned=True if kind != "private" else None,
        structured={"interaction_type": data["type"], "interaction": detail},
        event_reply_id=event_id,
        interaction_id=interaction_id,
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

    async def reply(
        self,
        incoming: ChannelMessage,
        text: str | None,
        sequence: int,
        *,
        rich: QQRichContent | None = None,
        data: bytes | None = None,
        reference_id: str | None = None,
        event_reply_id: str | None = None,
    ) -> str:
        context = {"event_id": event_reply_id} if event_reply_id is not None else {"msg_id": incoming.id}
        context["msg_seq"] = sequence
        if reference_id is not None:
            context["message_reference"] = {"message_id": reference_id}
        return await self._send_message(incoming.kind, incoming.conversation_id, text, context, rich=rich, data=data)

    async def send_proactive(
        self,
        kind: QQConversationKind,
        conversation_id: str,
        text: str | None,
        *,
        rich: QQRichContent | None = None,
        data: bytes | None = None,
    ) -> str:
        return await self._send_message(kind, conversation_id, text, {}, rich=rich, data=data)

    async def _post(self, path: str, body: dict, *, files: dict | None = None) -> dict:
        token = await self._access_token()
        headers = {"Authorization": f"QQBot {token}"}
        if files is None:
            response = await self.http.post(QQ_API + path, headers=headers, json=body)
        else:
            fields = {key: value if isinstance(value, str) else json.dumps(value) for key, value in body.items()}
            response = await self.http.post(QQ_API + path, headers=headers, data=fields, files=files)
        try:
            value = response.json() if response.content else {}
        except ValueError:
            response.raise_for_status()
            raise
        if not isinstance(value, dict):
            response.raise_for_status()
            raise TypeError("QQ returned an invalid response")
        code = value.get("code", value.get("err_code"))
        if path.endswith("/upload_part_finish") and str(code) == "40093001":
            raise QQPartPending
        response.raise_for_status()
        if code is not None and str(code) != "0":
            code = str(code)
            raise QQRejected(
                f"QQ rejected the operation (code {code if code.isascii() and code.isdigit() and len(code) < 20 else 'unknown'})."
            )
        return value

    async def _media_upload(self, base: str, media: QQMedia, filename: str, data: bytes | None) -> str:
        body = {"file_type": MEDIA_TYPES[media.kind], "file_name": filename, "srv_send_msg": False}
        if data is not None and len(data) > 4 << 20:
            upload_id = await self._upload_parts(base, body, data)
            body = {"upload_id": upload_id, "srv_send_msg": False}
        elif data is not None:
            body["file_data"] = base64.b64encode(data).decode("ascii")
        else:
            body["url"] = media.url
        response = await self._post(base + "/files", body)
        receipt = response.get("file_info")
        if not isinstance(receipt, str) or not 0 < len(receipt) <= 16384:
            raise ValueError("QQ omitted the media upload receipt")
        return receipt

    async def _upload_parts(self, base: str, body: dict, data: bytes) -> str:
        plan = await self._post(
            base + "/upload_prepare",
            {
                "file_type": body["file_type"],
                "file_name": body["file_name"],
                "file_size": len(data),
                "md5": hashlib.md5(data).hexdigest(),
                "sha1": hashlib.sha1(data).hexdigest(),
                "md5_10m": hashlib.md5(data[:10_002_432]).hexdigest(),
            },
        )
        upload_id, block, parts = plan["upload_id"], int(plan["block_size"]), plan["parts"]
        if (
            not isinstance(upload_id, str)
            or not 0 < len(upload_id) <= 1024
            or not 0 < block <= MEDIA_LIMITS["file"]
            or not isinstance(parts, list)
            or not 0 < len(parts) <= 256
            or len(parts) != (len(data) + block - 1) // block
        ):
            raise ValueError("QQ returned an invalid upload plan")
        urls = {}
        for part in parts:
            index = int(part["index"])
            url = media_url(part["presigned_url"])
            host = urlsplit(url).hostname or ""
            if index in urls or not 1 <= index <= len(parts) or not host.endswith((".myqcloud.com", ".tencentcos.cn")):
                raise ValueError("QQ returned an invalid upload part")
            urls[index] = url
        async with httpx.AsyncHTTPTransport() as transport:
            for index in range(1, len(parts) + 1):
                chunk = data[(index - 1) * block : index * block]
                request = httpx.Request("PUT", urls[index], content=chunk)
                request.extensions["timeout"] = {"connect": 10, "write": 30, "read": 15, "pool": 10}
                response = await transport.handle_async_request(request)
                try:
                    if not response.is_success:
                        raise QQRejected("QQ storage did not confirm the uploaded part.")
                finally:
                    await response.aclose()
                confirmation = {
                    "upload_id": upload_id,
                    "part_index": index,
                    "block_size": len(chunk),
                    "md5": hashlib.md5(chunk).hexdigest(),
                }
                for delay in (0, 0.4, 1):
                    if delay:
                        await asyncio.sleep(delay)
                    try:
                        await self._post(base + "/upload_part_finish", confirmation)
                    except QQPartPending:
                        continue
                    break
                else:
                    raise QQRejected("QQ did not confirm the uploaded part before finalization.")
        return upload_id

    async def _send_message(
        self,
        kind: str,
        conversation_id: str,
        text: str | None,
        context: dict,
        *,
        rich: QQRichContent | None,
        data: bytes | None,
    ) -> str:
        validate_rich(kind, text, rich)
        base = f"{MESSAGE_PATHS[kind]}/{quote(conversation_id, safe='')}"
        body = dict(context)
        files = None
        message_type = 0
        if text is not None:
            body["content"] = text
        if isinstance(rich, QQMarkdown):
            body["markdown"] = rich.model_dump(exclude={"type", "keyboard"}, exclude_none=True)
            if rich.keyboard is not None:
                body["keyboard"] = rich.keyboard
            message_type = 2
        elif isinstance(rich, (QQArk, QQEmbed)):
            body[rich.type] = rich.card
            message_type = 3 if rich.type == "ark" else 4
        elif isinstance(rich, QQMedia):
            source = rich.path or unquote(urlsplit(rich.url or "").path)
            filename = rich.filename or PurePosixPath(source).name[:255] or rich.kind
            mime = ""
            if data is not None:
                if not 0 < len(data) <= MEDIA_LIMITS[rich.kind]:
                    raise ValueError("The file is empty or exceeds the QQ media size limit.")
                if rich.kind == "image":
                    mime = image_type(data)
                    if not PurePosixPath(filename).suffix:
                        filename += "." + mime.removeprefix("image/")
            elif rich.url is None:
                raise ValueError("The Agent Environment did not supply the media file.")
            if kind in {"private", "group"}:
                body["media"] = {"file_info": await self._media_upload(base, rich, filename, data)}
                message_type = 7
            elif data is None:
                body["image"] = rich.url
            else:
                files = {"file_image": (filename, data, mime)}
        if kind in {"private", "group"}:
            body["msg_type"] = message_type
        message_id = (await self._post(base + "/messages", body, files=files))["id"]
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("QQ omitted the sent message ID")
        return message_id

    async def verify_credentials(self) -> None:
        await self._access_token()

    async def acknowledge_interaction(self, interaction_id: str) -> None:
        try:
            async with asyncio.timeout(2.5):
                token = await self._access_token()
                response = await self.http.put(
                    f"{QQ_API}/interactions/{quote(interaction_id, safe='')}",
                    headers={"Authorization": f"QQBot {token}"},
                    json={"code": 0},
                )
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError):
            # An uncertain acknowledgement must not replay a persisted event.
            return

    async def test_connection(self) -> None:
        async def discard(_message: ChannelMessage) -> None:
            pass

        async with asyncio.timeout(20):
            ready = await self._session(discard, lambda _state: None, stop_after_ready=True)
        if not ready:
            raise ValueError("QQ closed the connection before sending READY")

    async def listen(
        self, receive: Callable[[QQInbound], Awaitable[bool | None]], status: Callable[[str], None]
    ) -> None:
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
        receive: Callable[[QQInbound], Awaitable[bool | None]],
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
                                    inserted = await receive(incoming)
                                    if inserted and incoming.interaction_id is not None:
                                        await self.acknowledge_interaction(incoming.interaction_id)
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
