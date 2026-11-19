"""The channel message and text reply shapes exposed to Console."""

from typing import Annotated

from pydantic import BaseModel, Field, JsonValue

type ReplyText = Annotated[str, Field(min_length=1, max_length=2000, pattern=r"\S")]


class TextReply(BaseModel):
    text: ReplyText


class ChannelAttachment(BaseModel):
    kind: str
    media_type: str
    filename: str
    size: int | None = None
    width: int | None = None
    height: int | None = None


class ChannelMessage(BaseModel):
    id: str
    channel_id: str
    kind: str
    conversation_id: str
    sender_id: str
    text: str
    timestamp: str
    bot_mentioned: bool | None = None
    attachments: tuple[ChannelAttachment, ...] = ()
    structured: dict[str, JsonValue] = Field(default_factory=dict)


def message_table(platform: str) -> str:
    return {
        "qq": "qq_messages",
        "weixin": "weixin_messages",
        "github": "github_messages",
        "github_app": "github_messages",
    }[platform]
