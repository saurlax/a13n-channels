"""Telegram Bot API long polling and native message replies."""

import asyncio
import logging
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import httpx

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

_API = "https://api.telegram.org"
_TOKEN = re.compile(r"[0-9]{1,16}:[A-Za-z0-9_-]{1,128}")
_IDLE_CURSOR_AGE = 6 * 24 * 60 * 60
_POLL_TIMEOUT = 25
log = logging.getLogger(__name__)


class TelegramError(ChannelError):
    def __init__(self, message: str, *, retryable: bool = False, retry_after: int = 0, login_required: bool = False):
        super().__init__(message, retryable=retryable)
        self.retry_after = retry_after
        self.login_required = login_required


class BotURL(httpx.URL):
    """Keep credentials in the wire path only, including in HTTPX request logs."""

    def __str__(self) -> str:
        return f"{_API}/bot[redacted]/{self.path.rsplit('/', 1)[-1]}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)!r})"


def _token(config: Configuration) -> str:
    token = config.get("bot_token", "")
    if not _TOKEN.fullmatch(token):
        raise ChannelError("Enter a Telegram bot token from BotFather.")
    return token


def _positive_integer(value: object) -> bool:
    return type(value) is int and value > 0


async def _api(
    http: httpx.AsyncClient, token: str, method: str, body: dict | None = None, *, timeout: int = 10
) -> object:
    request = http.build_request("POST", f"{_API}/bot{token}/{method}", json=body, timeout=timeout)
    request.url = BotURL(request.url)
    try:
        response = await http.send(request, follow_redirects=False)
    except httpx.RequestError:
        raise TelegramError("Telegram did not confirm the request.", retryable=True) from None

    try:
        data = response.json()
    except ValueError:
        data = None
    status = response.status_code
    if isinstance(data, dict) and data.get("ok") is False and type(data.get("error_code")) is int:
        status = data["error_code"]

    if status in {401, 404}:
        raise TelegramError("Telegram rejected the bot token. Check the BotFather credentials.", login_required=True)
    if status == 409 and method == "getUpdates":
        raise TelegramError("Telegram polling conflicts with an active webhook or another polling client.")
    if status == 429:
        parameters = data.get("parameters") if isinstance(data, dict) else None
        retry_after = parameters.get("retry_after") if isinstance(parameters, dict) else None
        if not _positive_integer(retry_after):
            header = response.headers.get("Retry-After", "")
            retry_after = max(1, int(header)) if re.fullmatch(r"[0-9]{1,8}", header) else 1
        raise TelegramError(
            f"Telegram rate-limited the request. Try again after {retry_after} seconds.", retry_after=retry_after
        )
    if status == 408 or status >= 500:
        raise TelegramError("Telegram did not confirm the request.", retryable=True)
    if not 200 <= status < 300 or not response.is_success or (isinstance(data, dict) and data.get("ok") is False):
        raise TelegramError("Telegram rejected the request. Check bot permissions and conversation availability.")
    if not isinstance(data, dict) or data.get("ok") is not True or "result" not in data:
        raise TelegramError("Telegram returned an invalid response.", retryable=True)
    return data["result"]


async def _bot(http: httpx.AsyncClient, token: str) -> tuple[int, str]:
    result = await _api(http, token, "getMe")
    if (
        not isinstance(result, dict)
        or result.get("is_bot") is not True
        or not _positive_integer(result.get("id"))
        or not isinstance(result.get("username"), str)
        or not result["username"]
    ):
        raise TelegramError("Telegram did not return a bot identity.", retryable=True)
    return result["id"], result["username"]


async def bind(http: httpx.AsyncClient, config: Configuration) -> str:
    token = _token(config)
    bot_id, _ = await _bot(http, token)
    webhook = await _api(http, token, "getWebhookInfo")
    if not isinstance(webhook, dict) or not isinstance(webhook.get("url"), str):
        raise TelegramError("Telegram did not return webhook status.", retryable=True)
    if webhook["url"]:
        raise TelegramError("Remove the active Telegram webhook before connecting this bot for long polling.")
    return str(bot_id)


def _addresses_bot(message: dict, text: str, entities: object, bot_id: int, username: str) -> bool:
    reply = message.get("reply_to_message")
    author = reply.get("from") if isinstance(reply, dict) else None
    if isinstance(author, dict) and author.get("id") == bot_id:
        return True
    if not isinstance(entities, list):
        return False
    raw = text.encode("utf-16-le")
    mention = f"@{username}".casefold()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        kind = entity.get("type")
        if kind == "text_mention":
            user = entity.get("user")
            if isinstance(user, dict) and user.get("id") == bot_id:
                return True
        if kind not in ("mention", "bot_command"):
            continue
        offset, length = entity.get("offset"), entity.get("length")
        if type(offset) is not int or offset < 0 or not _positive_integer(length):
            continue
        if 2 * (offset + length) > len(raw):
            continue
        value = raw[2 * offset : 2 * (offset + length)].decode("utf-16-le", errors="replace").casefold()
        if value == mention or (kind == "bot_command" and value.startswith("/") and value.endswith(mention)):
            return True
    return False


def _offset(cursor: str, now: int) -> int | None:
    if not cursor:
        return None
    next_id, consumed_at = map(int, cursor.split(":", 1))
    # Telegram chooses a fresh sequence after a week of inactivity. Its queue holds at most 24 hours.
    return next_id if now - consumed_at < _IDLE_CURSOR_AGE else None


class TelegramConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, identity: str, config: Configuration):
        self.http = http
        self.channel_id = channel_id
        self.identity = identity
        self.token = _token(config)

    def message(self, value: object, bot_id: int, username: str) -> tuple[ChannelMessage, dict[str, str]] | None:
        if not isinstance(value, dict) or value.get("direct_messages_topic") is not None:
            return None
        chat, sender = value.get("chat"), value.get("from")
        if not isinstance(chat, dict) or not isinstance(sender, dict):
            return None
        if chat.get("type") not in ("private", "group", "supergroup") or sender.get("is_bot") is True:
            return None
        chat_id, sender_id, message_id, date = (
            chat.get("id"),
            sender.get("id"),
            value.get("message_id"),
            value.get("date"),
        )
        if type(chat_id) is not int or chat_id == 0 or not _positive_integer(sender_id) or sender_id == bot_id:
            return None
        if not _positive_integer(message_id) or type(date) is not int or not 0 <= date < 10**10:
            return None
        text, entities = (
            (value.get("text"), value.get("entities"))
            if "text" in value
            else (
                value.get("caption"),
                value.get("caption_entities"),
            )
        )
        if not isinstance(text, str) or not text.strip():
            return None

        kind = "private" if chat["type"] == "private" else "group"
        conversation = str(chat_id)
        context = {"chat_id": conversation, "message_id": str(message_id)}
        if value.get("is_topic_message") is True:
            thread_id = value.get("message_thread_id")
            if not _positive_integer(thread_id):
                return None
            conversation = f"{chat_id}:{thread_id}"
            if kind == "group":
                kind = "thread"
            context["message_thread_id"] = str(thread_id)

        return ChannelMessage(
            id=f"{chat_id}:{message_id}",
            channel_id=self.channel_id,
            kind=kind,
            conversation_id=conversation,
            sender_id=str(sender_id),
            text=text,
            timestamp=datetime.fromtimestamp(date, UTC).isoformat(),
            bot_mentioned=_addresses_bot(value, text, entities, bot_id, username),
        ), context

    async def listen(self, cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        bot = None
        delay = 1
        while True:
            try:
                if bot is None:
                    bot = await _bot(self.http, self.token)
                    if str(bot[0]) != self.identity:
                        raise TelegramError("The Telegram token belongs to a different bot.", login_required=True)
                offset = _offset(cursor, int(time.time()))
                body = {"timeout": _POLL_TIMEOUT, "allowed_updates": ["message"], "limit": 50}
                if offset is not None:
                    body["offset"] = offset
                updates = await _api(self.http, self.token, "getUpdates", body, timeout=_POLL_TIMEOUT + 10)
                if not isinstance(updates, list) or any(
                    not isinstance(update, dict) or not _positive_integer(update.get("update_id")) for update in updates
                ):
                    raise TelegramError("Telegram returned an invalid update batch.", retryable=True)
                status("connected")
                for update in sorted(updates, key=lambda item: item["update_id"]):
                    update_id = update["update_id"]
                    if offset is not None and update_id < offset:
                        continue
                    incoming = self.message(update.get("message"), *bot)
                    next_cursor = f"{update_id + 1}:{int(time.time())}"
                    if incoming is None:
                        await ingest(None, cursor=next_cursor)
                    else:
                        message, context = incoming
                        await ingest(message, cursor=next_cursor, context=context)
                    offset, cursor = update_id + 1, next_cursor
                delay = 1
            except ChannelError as error:
                # Error strings from this adapter contain operator hints and no upstream bodies or tokens.
                if isinstance(error, TelegramError) and error.login_required:
                    status("login_required")
                    log.info("%s", error)
                    return
                retry_after = error.retry_after if isinstance(error, TelegramError) else 0
                status("reconnecting" if error.retryable or retry_after else "error")
                if isinstance(error, TelegramError):
                    log.info("%s", error)
                await asyncio.sleep(max(delay, retry_after))
                delay = min(2 * delay, 30)

    async def reply(self, message: ChannelMessage, text: str, context: Mapping[str, str]) -> str:
        try:
            chat_id = int(context["chat_id"])
            source_id = int(context["message_id"])
            thread_id = int(context["message_thread_id"]) if "message_thread_id" in context else None
        except (KeyError, ValueError):
            raise ChannelError("The Telegram message has no valid reply target.") from None
        conversation = str(chat_id) if thread_id is None else f"{chat_id}:{thread_id}"
        if message.conversation_id != conversation or message.id != f"{chat_id}:{source_id}":
            raise ChannelError("The Telegram reply target does not match the source message.")
        body = {"chat_id": chat_id, "text": text, "reply_parameters": {"message_id": source_id}}
        if thread_id is not None:
            body["message_thread_id"] = thread_id
        result = await _api(self.http, self.token, "sendMessage", body)
        if not isinstance(result, dict) or not _positive_integer(result.get("message_id")):
            raise TelegramError("Telegram did not confirm the reply.", retryable=True)
        return str(result["message_id"])


ADAPTER = Adapter(
    key="telegram",
    label="Telegram",
    label_zh="Telegram",
    kinds=("private", "group", "thread"),
    fields=(CredentialField("bot_token", "Bot token", "机器人令牌"),),
    bind=bind,
    connect=TelegramConnection,
)
