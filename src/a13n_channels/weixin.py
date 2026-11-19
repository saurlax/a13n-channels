"""Weixin ClawBot QR pairing and iLink text transport."""

import asyncio
import base64
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Literal
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from a13n_channels.messages import ChannelMessage
from a13n_channels.qr import qr_image

WEIXIN_CONVERSATION_KIND = "private"
WEIXIN_BASE = "https://ilinkai.weixin.qq.com"
CLIENT_VERSION = "2.4.8"
type PairingState = Literal[
    "wait",
    "scaned",
    "expired",
    "need_verifycode",
    "verify_code_blocked",
    "scaned_but_redirect",
    "binded_redirect",
    "confirmed",
]


class WeixinCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=80)


class WeixinCredentials(BaseModel):
    bot_id: str = Field(min_length=1, max_length=128)
    base_url: str
    token: SecretStr = Field(min_length=1, max_length=4096)


class PairingPoll(BaseModel):
    login_id: str
    verify_code: Annotated[str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")] | None = None


class PairingResult(BaseModel):
    status: PairingState
    qr_image: str | None = None
    login_id: str | None = None


@dataclass
class Pairing:
    login_id: str
    code: str
    host: str
    expires: float
    expected: str | None
    credentials: WeixinCredentials | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class WeixinError(ValueError):
    pass


class LoginRequired(WeixinError):
    pass


def weixin_origin(value: str) -> str:
    url = urlsplit(value)
    if (
        url.scheme != "https"
        or not (url.hostname or "").endswith(".weixin.qq.com")
        or url.port not in {None, 443}
        or url.username
        or url.password
        or url.path not in {"", "/"}
        or url.query
        or url.fragment
    ):
        raise WeixinError("Expected an official Weixin iLink origin")
    return value.rstrip("/")


def text_message(channel_id: str, bot_id: str, value: dict) -> tuple[ChannelMessage, str] | None:
    if value.get("message_type") != 1 or value.get("group_id") or value.get("to_user_id") != bot_id:
        return None
    text = "".join(item["text_item"]["text"] for item in value.get("item_list", []) if item.get("type") == 1)
    if not text.strip():
        return None
    sender = value["from_user_id"]
    context = value.get("context_token", "")
    if not isinstance(context, str) or len(context) > 16384 or not sender:
        raise WeixinError("Invalid Weixin message context")
    return ChannelMessage(
        id=str(value["message_id"]),
        channel_id=channel_id,
        kind=WEIXIN_CONVERSATION_KIND,
        conversation_id=sender,
        sender_id=sender,
        text=text,
        timestamp=datetime.fromtimestamp(value["create_time_ms"] / 1000, UTC).isoformat(),
    ), context


def headers(token: str | None = None, *, polling_qr: bool = False) -> dict[str, str]:
    result = {"iLink-App-Id": "bot", "iLink-App-ClientVersion": str((2 << 16) | (4 << 8) | 8)}
    if not polling_qr:
        result.update(
            AuthorizationType="ilink_bot_token",
            **{"X-WECHAT-UIN": base64.b64encode(str(secrets.randbits(32)).encode()).decode()},
        )
    if token:
        result["Authorization"] = "Bearer " + token
    return result


def response_object(response: httpx.Response) -> dict:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise WeixinError("Invalid Weixin response")
    return value


async def start_pairing(http: httpx.AsyncClient, expected: str | None) -> tuple[Pairing, PairingResult]:
    value = response_object(
        await http.post(
            WEIXIN_BASE + "/ilink/bot/get_bot_qrcode",
            params={"bot_type": "3"},
            headers=headers(),
            json={"local_token_list": []},
        )
    )
    code = value.get("qrcode")
    if not isinstance(code, str) or not 0 < len(code) <= 4096:
        raise WeixinError("Invalid Weixin pairing code")
    login = Pairing(secrets.token_hex(16), code, WEIXIN_BASE, asyncio.get_running_loop().time() + 300, expected)
    return login, PairingResult(
        status="wait", login_id=login.login_id, qr_image=qr_image(value.get("qrcode_img_content") or code)
    )


async def poll_pairing(
    http: httpx.AsyncClient,
    login: Pairing,
    verify_code: str | None,
) -> tuple[PairingResult, WeixinCredentials | None]:
    params = {"qrcode": login.code}
    if verify_code:
        params["verify_code"] = verify_code
    value = response_object(
        await http.get(
            login.host + "/ilink/bot/get_qrcode_status",
            params=params,
            headers=headers(polling_qr=True),
            timeout=40,
        )
    )
    result = PairingResult(status=value.get("status"))
    if result.status == "scaned_but_redirect" and (host := value.get("redirect_host")):
        login.host = weixin_origin(host if host.startswith("https://") else "https://" + host)
    if result.status != "confirmed":
        return result, None
    return result, WeixinCredentials(
        bot_id=value["ilink_bot_id"],
        base_url=weixin_origin(value["baseurl"]),
        token=value["bot_token"],
    )


class WeixinConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, credentials: WeixinCredentials):
        self.http = http
        self.channel_id = channel_id
        self.credentials = credentials

    async def request(self, action: str, body: dict, *, timeout: float = 15) -> dict:
        value = response_object(
            await self.http.post(
                self.credentials.base_url + "/ilink/bot/" + action,
                headers=headers(self.credentials.token.get_secret_value()),
                json={**body, "base_info": {"channel_version": CLIENT_VERSION, "bot_agent": "a13n-channels"}},
                timeout=timeout,
            )
        )
        codes = (value.get("ret", 0), value.get("errcode", 0))
        if -14 in codes:
            raise LoginRequired("Weixin requires re-pairing")
        if any(codes):
            raise WeixinError("Weixin rejected the operation")
        return value

    async def listen(
        self,
        cursor: str,
        ingest: Callable[[str, list[tuple[ChannelMessage, str]]], Awaitable[None]],
        status: Callable[[str], None],
    ) -> None:
        delay = 1
        while True:
            try:
                await self.request("msg/notifystart", {})
                status("connected")
                while True:
                    try:
                        batch = await self.request("getupdates", {"get_updates_buf": cursor}, timeout=45)
                    except httpx.ReadTimeout:
                        continue
                    if not isinstance(events := batch.get("msgs", []), list):
                        raise WeixinError("Invalid Weixin update batch")
                    messages = []
                    for event in events:
                        try:
                            item = text_message(self.channel_id, self.credentials.bot_id, event)
                        except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
                            continue
                        if item is not None:
                            messages.append(item)
                    next_cursor = batch.get("get_updates_buf") or cursor
                    if not isinstance(next_cursor, str):
                        raise WeixinError("Invalid Weixin update cursor")
                    await ingest(next_cursor, messages)
                    cursor = next_cursor
                    delay = 1
                    if not events:
                        await asyncio.sleep(0.25)
            except LoginRequired:
                status("login_required")
                return
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                status("reconnecting")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def reply(self, incoming: ChannelMessage, text: str, context: str, reply_id: str) -> str:
        await self.request(
            "sendmessage",
            {
                "msg": {
                    "from_user_id": "",
                    "to_user_id": incoming.conversation_id,
                    "client_id": reply_id,
                    "message_type": 2,
                    "message_state": 2,
                    "context_token": context,
                    "item_list": [{"type": 1, "text_item": {"text": text}}],
                }
            },
        )
        return reply_id
