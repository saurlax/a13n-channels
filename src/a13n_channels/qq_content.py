"""QQ media, forwarded records, and structured message content."""

import asyncio
import base64
import ipaddress
import json
import re
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Literal, NotRequired, TypedDict
from urllib.parse import parse_qs, urljoin, urlsplit

import httpx
from pydantic import BaseModel, Field, JsonValue, field_validator, model_validator

from a13n_channels.messages import ChannelAttachment, ChannelMessage, ReplyText

MEDIA_TYPES = {"image": 1, "video": 2, "audio": 3, "file": 4}
MEDIA_LIMITS = {"image": 30 << 20, "video": 100 << 20, "audio": 20 << 20, "file": 100 << 20}
MAX_ATTACHMENTS = 16
FACE = re.compile(r'<faceType=(\d+),faceId="(\d+)",ext="([^"]*)">')
RECORD_IMAGE = re.compile(
    r"^([ \t]*)\[附件[1-9][0-9]*\] 类型:图片 文件名:[^\r\n]+?"
    r" 尺寸:[0-9]+x[0-9]+ 大小:[0-9]+(?:\.[0-9]+)?(?:B|KB|MB|GB) URL:(https://\S+)(?=\r?$)",
    re.MULTILINE,
)
PRIVATE_FIELDS = {
    "access_token",
    "app_secret",
    "auth_token",
    "authorization",
    "credential",
    "context_token",
    "file_info",
    "presigned_url",
    "raw_url",
    "upload_id",
    "attachments",
    "message_scene",
    "secret",
    "token",
}


def media_url(value: str) -> str:
    if len(value) > 4096 or "\\" in value or any(character.isspace() or ord(character) < 32 for character in value):
        raise ValueError("Use a public HTTPS media URL without credentials.")
    url = urlsplit(value)
    host = (url.hostname or "").rstrip(".")
    if (
        url.scheme != "https"
        or not host
        or url.username is not None
        or url.password is not None
        or url.fragment
        or url.port not in {None, 443}
    ):
        raise ValueError("Use a public HTTPS media URL without credentials.")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host or host.endswith((".local", ".internal", ".localhost", ".localdomain")):
            raise ValueError("Use a public media hostname.") from None
    else:
        if not address.is_global:
            raise ValueError("Use a public media hostname.")
    return value


def received_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if value.startswith("//"):
        value = "https:" + value
    elif value.startswith("http://"):
        value = "https://" + value[7:]
    try:
        return media_url(value)
    except ValueError:
        return None


@asynccontextmanager
async def open_attachment(
    url: str, authorize_url: Callable[[str], None] | None = None
) -> AsyncIterator[httpx.Response]:
    """Pin a public address at every hop while preserving the origin's TLS and Host identity."""
    for _ in range(4):
        url = media_url(url)
        if authorize_url is not None:
            authorize_url(url)
        target = httpx.URL(url)
        addresses = await asyncio.get_running_loop().getaddrinfo(
            target.raw_host.decode("ascii"), 443, type=socket.SOCK_STREAM
        )
        address = addresses[0][4][0]
        if any(
            not (ip := ipaddress.ip_address(result[4][0])).is_global or ip.is_multicast or ip.is_reserved
            for result in addresses
        ):
            raise ValueError("QQ attachment host must resolve to public addresses.")
        request = httpx.Request(
            "GET",
            target.copy_with(host=address),
            headers={"Host": target.netloc.decode("ascii"), "Accept-Encoding": "identity"},
            extensions={"sni_hostname": target.raw_host.decode("ascii")},
        )
        # A separate pool per hop keeps TLS identity tied to the declared origin.
        async with httpx.AsyncHTTPTransport() as transport:
            response = await transport.handle_async_request(request)
            try:
                location = response.headers.get("Location")
                if response.status_code in {301, 302, 303, 307, 308} and location:
                    url = urljoin(url, location)
                    continue
                yield response
                return
            finally:
                await response.aclose()
    raise ValueError("QQ attachment returned too many redirects.")


class QQInbound(ChannelMessage):
    attachment_urls: tuple[str, ...] = Field(default=(), exclude=True, repr=False)
    event_reply_id: str | None = Field(default=None, exclude=True, repr=False)
    interaction_id: str | None = Field(default=None, exclude=True, repr=False)


def render_faces(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        if match[1] == "6":
            return "[QQ sticker]"
        if len(match[3]) <= 4096:
            try:
                data = json.loads(base64.b64decode(match[3], validate=True))
                if isinstance(data, dict) and isinstance(data.get("text"), str) and data["text"].strip():
                    return f"[QQ emoji: {data['text'][:256]}]"
            except (ValueError, UnicodeError):
                pass
        return f"[QQ emoji: {match[2]}]"

    return FACE.sub(replace, text)


def safe_structure(value: JsonValue, depth: int = 0) -> JsonValue:
    if depth >= 6:
        return "[Truncated]"
    if isinstance(value, dict):
        return {
            key[:128]: safe_structure(child, depth + 1)
            for key, child in list(value.items())[:24]
            if key.lower() not in PRIVATE_FIELDS
        }
    if isinstance(value, list):
        return [safe_structure(child, depth + 1) for child in value[:16]]
    if isinstance(value, str):
        if value.startswith(("auth_token=", "context_token=", "access_token=")):
            return "[Private]"
        return value[:4096]
    return value


def project_content(data: dict) -> tuple[str, tuple[ChannelAttachment, ...], tuple[str, ...], dict[str, JsonValue]]:
    """Project authenticated QQ content in source order, keeping attachment locators private."""
    attachments: dict[str, ChannelAttachment] = {}
    record_images: dict[str, None] = {}
    structured: dict[str, JsonValue] = {}
    lines: list[str] = []
    remaining = 60_000

    def record_image(match: re.Match[str]) -> str:
        url = received_url(match[2])
        if url is None:
            return match[1] + "[Image unavailable]"
        parsed = urlsplit(url)
        if parsed.hostname != "multimedia.nt.qq.com.cn" or parsed.path != "/download":
            return match[0]
        if parsed.query.count("&") >= 4:
            return match[1] + "[Image unavailable]"
        query = parse_qs(parsed.query, keep_blank_values=True)
        if not {"appid", "fileid", "rkey"} <= query.keys() <= {"appid", "fileid", "rkey", "spec"} or any(
            len(values) != 1 or not values[0] for values in query.values()
        ):
            return match[1] + "[Image unavailable]"
        if len(record_images) < MAX_ATTACHMENTS:
            record_images.setdefault(url, None)
        return match[1] + "[Image]"

    def append(text: str) -> None:
        nonlocal remaining
        lines.append(text[:remaining])
        remaining -= min(len(text), remaining) + 1

    def add_attachments(items: object, sticker: bool) -> None:
        if not isinstance(items, list):
            return
        for item in items[:MAX_ATTACHMENTS]:
            if len(attachments) >= MAX_ATTACHMENTS:
                break
            if not isinstance(item, dict):
                continue
            url = received_url(item.get("voice_wav_url")) or received_url(item.get("url"))
            if not url or url in attachments:
                continue
            mime = str(item.get("content_type") or "application/octet-stream").partition(";")[0].lower()[:128]
            kind = mime.partition("/")[0]
            if kind == "image" and sticker:
                kind = "sticker"
            elif kind not in {"image", "audio", "video"}:
                kind = "audio" if mime == "voice" else "file"
            if item.get("voice_wav_url") and kind == "audio":
                mime = "audio/wav"
            metadata = {
                key: item[key]
                for key in ("size", "width", "height")
                if type(item.get(key)) is int and 0 <= item[key] < 2**53
            }
            attachments[url] = ChannelAttachment(
                kind=kind, media_type=mime, filename=str(item.get("filename") or "attachment")[:255], **metadata
            )

    cards: list[dict[str, JsonValue]] = []
    card_budget = 8192
    pending = [(data, 0)]
    visited = 0
    while pending and remaining > 0 and visited < 256:
        item, depth = pending.pop()
        visited += 1
        indent = "  " * max(0, depth - 1)
        content = item.get("content")
        content = content[:60_000] if isinstance(content, str) else ""
        if item.get("message_type") == 102:
            append(indent + "[Chat history]")
            content = RECORD_IMAGE.sub(record_image, content)
        elif depth == 0 and item.get("msg_elements"):
            # The label goes after the outer user's text.
            structured["quoted"] = True
        if content.strip() and remaining > 0:
            append(indent + render_faces(content).strip().replace("\n", "\n" + indent))
        if depth == 0 and structured.get("quoted") and remaining > 0:
            append("[Quoted content]")
        add_attachments(item.get("attachments"), "<faceType=6," in content)
        if card_budget > 0 and len(cards) < 8:
            card = {
                key: safe_structure(item[key])
                for key in ("ark", "ark_data", "embed", "markdown", "keyboard")
                if isinstance(item.get(key), (dict, list))
            }
            if card:
                size = len(json.dumps(card, ensure_ascii=False))
                if size <= card_budget:
                    cards.append(card)
                    card_budget -= size
                else:
                    structured["cards_truncated"] = True
        children = item.get("msg_elements")
        if isinstance(children, list) and children and remaining > 0:
            if depth < 32:
                pending.extend((child, depth + 1) for child in reversed(children[:256]) if isinstance(child, dict))
                if len(children) > 256:
                    structured["truncated"] = True
            else:
                structured["truncated"] = True

    if pending or remaining <= 0 or structured.get("truncated"):
        lines.append("[Message truncated]")
        structured["truncated"] = True
    for url in record_images:
        if len(attachments) >= MAX_ATTACHMENTS:
            break
        attachments.setdefault(url, ChannelAttachment(kind="image", media_type="image/*", filename="image"))
    if cards:
        structured["cards"] = cards
    if data.get("message_type") == 102:
        structured["record"] = True
    reference = data.get("message_reference")
    if isinstance(reference, dict) and isinstance(reference.get("message_id"), str):
        structured["reference_id"] = reference["message_id"][:1024]
    scene = data.get("message_scene")
    if isinstance(scene, dict) and isinstance(scene.get("ext"), list):
        for value in scene["ext"][:32]:
            if isinstance(value, str) and value.startswith("msg_idx="):
                structured["native_reference_id"] = value[8:1032]
                break
    if not lines and (attachments or cards):
        lines.append("[QQ attachment]" if attachments else "[QQ card]")
    return "\n".join(lines), tuple(attachments.values()), tuple(attachments), structured


class QQMedia(BaseModel, extra="forbid"):
    type: Literal["media"]
    kind: Literal["image", "video", "audio", "file"] = "image"
    url: str | None = Field(default=None, max_length=4096, description="Public or signed HTTPS media URL.")
    path: str | None = Field(
        default=None, min_length=1, max_length=1024, description="Absolute Agent Environment path."
    )
    filename: str | None = Field(default=None, min_length=1, max_length=255, pattern=r"^[^/\\\x00-\x1f]+$")

    @field_validator("url")
    @classmethod
    def valid_url(cls, value: str | None) -> str | None:
        return media_url(value) if value is not None else None

    @model_validator(mode="after")
    def one_source(self):
        if (self.url is None) == (self.path is None):
            raise ValueError("Specify one media source, url or path.")
        if self.path is not None and (not self.path.startswith("/") or "\\" in self.path):
            raise ValueError("Use an absolute Agent Environment path.")
        return self


class QQMarkdownParameter(BaseModel, extra="forbid"):
    key: str = Field(min_length=1, max_length=128)
    values: list[Annotated[str, Field(max_length=4096)]] = Field(min_length=1, max_length=32)


class QQButtonPermission(TypedDict):
    type: int
    specify_user_ids: NotRequired[list[str]]
    specify_role_ids: NotRequired[list[str]]


class QQButtonAction(TypedDict):
    type: int
    permission: QQButtonPermission
    data: str
    unsupport_tips: NotRequired[str]
    reply: NotRequired[bool]
    enter: NotRequired[bool]
    anchor: NotRequired[int]
    click_limit: NotRequired[int]
    at_bot_show_channel_list: NotRequired[bool]


class QQButtonLabel(TypedDict):
    label: str
    visited_label: str
    style: int


class QQButton(TypedDict):
    id: str
    render_data: QQButtonLabel
    action: QQButtonAction
    group_id: NotRequired[str]


class QQKeyboardRow(TypedDict):
    buttons: Annotated[list[QQButton], Field(min_length=1, max_length=5)]


class QQKeyboardContent(TypedDict):
    rows: Annotated[list[QQKeyboardRow], Field(min_length=1, max_length=5)]


class QQKeyboard(TypedDict, total=False):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    content: QQKeyboardContent


class QQMarkdown(BaseModel, extra="forbid"):
    type: Literal["markdown"]
    content: str | None = Field(default=None, min_length=1, max_length=4096)
    custom_template_id: str | None = Field(default=None, min_length=1, max_length=128)
    params: list[QQMarkdownParameter] | None = Field(default=None, max_length=32)
    keyboard: QQKeyboard | None = Field(default=None, description="QQ keyboard template ID or inline rows.")

    @model_validator(mode="after")
    def one_format(self):
        if (self.content is None) == (self.custom_template_id is None):
            raise ValueError("Supply Markdown content or a QQ template ID.")
        if self.params is not None and self.custom_template_id is None:
            raise ValueError("Markdown params require a template ID.")
        if self.keyboard is not None and ("id" in self.keyboard) == ("content" in self.keyboard):
            raise ValueError("Specify a QQ keyboard template ID or inline rows.")
        return self


class QQArkObjectField(TypedDict):
    key: str
    value: str


class QQArkObject(TypedDict):
    obj_kv: list[QQArkObjectField]


class QQArkField(TypedDict):
    key: str
    value: NotRequired[str]
    obj: NotRequired[list[QQArkObject]]


class QQArkCard(TypedDict):
    template_id: int
    kv: list[QQArkField]


class QQArk(BaseModel, extra="forbid"):
    type: Literal["ark"]
    card: QQArkCard


class QQEmbedThumbnail(TypedDict):
    url: str


class QQEmbedField(TypedDict):
    name: str


class QQEmbedCard(TypedDict, total=False):
    title: str
    prompt: str
    thumbnail: QQEmbedThumbnail
    fields: list[QQEmbedField]


class QQEmbed(BaseModel, extra="forbid"):
    type: Literal["embed"]
    card: QQEmbedCard


type QQRichContent = QQMedia | QQMarkdown | QQArk | QQEmbed


class QQReply(BaseModel, extra="forbid"):
    text: ReplyText | None = None
    rich: QQRichContent | None = None

    @model_validator(mode="after")
    def supported_content(self):
        validate_rich("private", self.text, self.rich)
        if isinstance(self.rich, QQMedia) and self.rich.path is not None:
            raise ValueError("Reply API media requires an HTTPS URL. Agent tools can also use Environment paths.")
        return self


def validate_rich(kind: str, text: str | None, rich: QQRichContent | None) -> None:
    if rich is None:
        if text is None:
            raise ValueError("Supply text or rich content.")
        return
    if isinstance(rich, QQMedia):
        if rich.kind != "image" and kind in {"guild", "guild_dm"}:
            raise ValueError("QQ guild media supports images.")
        if text and rich.kind != "image":
            raise ValueError("Send non-image media and text as separate messages.")
    elif text:
        raise ValueError("Supply the Markdown or card as rich content without a separate text field.")
    elif isinstance(rich, QQEmbed) and not rich.card:
        raise ValueError("Supply at least one QQ Embed field.")
    elif len(rich.model_dump_json().encode()) > 16_384:
        raise ValueError("QQ structured messages must be at most 16 KiB.")


def image_type(data: bytes) -> str:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    raise ValueError("Use a PNG, JPEG, GIF, or WebP image.")
