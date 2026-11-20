"""Slack Events API over Socket Mode and text replies through the Web API."""

import asyncio
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

_ID = re.compile(r"[A-Z0-9]{1,32}")
_TS = re.compile(r"([0-9]{1,10})\.([0-9]{6})")
_AUTH_ERRORS = {"account_inactive", "invalid_auth", "not_authed", "token_revoked"}
_UNCERTAIN_ERRORS = {"fatal_error", "internal_error", "service_unavailable"}
_API = "https://slack.com/api"


class SlackError(ChannelError):
    def __init__(self, message: str, *, retryable: bool = False, retry_after: int = 0):
        super().__init__(message, retryable=retryable)
        self.retry_after = retry_after


async def _api(http: httpx.AsyncClient, method: str, token: str, body: dict | None = None) -> dict:
    try:
        response = await http.post(
            f"{_API}/{method}",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
            follow_redirects=False,
            timeout=10,
        )
    except httpx.RequestError:
        raise SlackError("Slack did not confirm the request.", retryable=True) from None

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "")
        seconds = int(retry_after) if re.fullmatch(r"[0-9]{1,8}", retry_after) else 1
        raise SlackError("Slack rate-limited the request. Try again later.", retry_after=seconds)
    if response.status_code == 408 or response.status_code >= 500:
        raise SlackError("Slack did not confirm the request.", retryable=True)
    if not response.is_success:
        raise SlackError("Slack rejected the request. Check the app credentials and permissions.")
    try:
        result = response.json()
    except ValueError:
        raise SlackError("Slack returned an invalid response.", retryable=True) from None
    if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
        raise SlackError("Slack returned an invalid response.", retryable=True)
    if result["ok"] is True:
        return result

    error = result.get("error")
    if not isinstance(error, str):
        raise SlackError("Slack returned an invalid response.", retryable=True)
    if error in _AUTH_ERRORS:
        raise SlackError("Slack rejected the token. Check the app and bot credentials.")
    if error == "missing_scope":
        raise SlackError("The Slack app is missing an OAuth scope for this operation.")
    if error in {"channel_not_found", "not_in_channel", "is_archived"}:
        raise SlackError("Slack cannot post in this conversation. Check the app's channel membership.")
    if error in _UNCERTAIN_ERRORS:
        raise SlackError("Slack did not confirm the request.", retryable=True)
    raise SlackError("Slack rejected the request. Check the app configuration and permissions.")


async def _socket_url(http: httpx.AsyncClient, token: str) -> str:
    result = await _api(http, "apps.connections.open", token)
    url = result.get("url")
    if not isinstance(url, str):
        raise SlackError("Slack did not return a Socket Mode URL.", retryable=True)
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme == "wss"
            and parsed.hostname is not None
            and parsed.hostname.endswith(".slack.com")
            and parsed.port in {None, 443}
            and parsed.username is None
            and parsed.password is None
        ):
            return url
    except ValueError:
        pass
    raise SlackError("Slack returned an invalid Socket Mode URL.", retryable=True)


async def bind(http: httpx.AsyncClient, config: Configuration) -> str:
    if not re.fullmatch(r"xapp-[A-Za-z0-9-]+", config.get("app_token", "")):
        raise ChannelError("Enter a Slack app-level token beginning with xapp-.")
    if not re.fullmatch(r"xoxb-[A-Za-z0-9-]+", config.get("bot_token", "")):
        raise ChannelError("Enter a Slack bot token beginning with xoxb-.")
    identity = await _api(http, "auth.test", config["bot_token"])
    team_id, user_id, bot_id = (identity.get(field) for field in ("team_id", "user_id", "bot_id"))
    if not all(isinstance(item, str) and _ID.fullmatch(item) for item in (team_id, user_id, bot_id)):
        raise ChannelError("Install the Slack bot in the workspace and supply its workspace bot token.")
    await _socket_url(http, config["app_token"])
    return f"{team_id}:{user_id}"


class SlackConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, identity: str, config: Configuration):
        self.http = http
        self.channel_id = channel_id
        self.team_id, self.user_id = identity.split(":", 1)
        self.config = config

    def message(self, payload: dict) -> tuple[ChannelMessage, dict[str, str]] | None:
        if payload.get("type") != "event_callback":
            return None
        if (payload.get("context_team_id") or payload.get("team_id")) != self.team_id:
            return None
        authorizations = payload.get("authorizations")
        if not isinstance(authorizations, list) or not any(
            isinstance(authorization, dict)
            and authorization.get("is_bot") is True
            and authorization.get("user_id") == self.user_id
            and authorization.get("team_id") == self.team_id
            for authorization in authorizations
        ):
            return None
        event = payload.get("event")
        if not isinstance(event, dict) or event.get("type") not in ("message", "app_mention"):
            return None
        if event.get("bot_id") or event.get("user") == self.user_id:
            return None
        if event.get("subtype") not in (None, "file_share", "thread_broadcast"):
            return None
        channel, sender, ts, text = (event.get(field) for field in ("channel", "user", "ts", "text"))
        if not all(isinstance(item, str) and _ID.fullmatch(item) for item in (channel, sender)):
            return None
        timestamp = _TS.fullmatch(ts) if isinstance(ts, str) else None
        if timestamp is None or not isinstance(text, str) or not text.strip():
            return None

        # app_mention omits channel_type. The same message must have a stable kind across both subscriptions.
        kind = "private" if channel.startswith("D") or event.get("channel_type") == "im" else "channel"
        thread = event.get("thread_ts")
        if isinstance(thread, str) and _TS.fullmatch(thread):
            context = {"thread_ts": thread}
        elif kind == "channel":
            context = {"thread_ts": ts}
        else:
            context = {}
        seconds, micros = timestamp.groups()
        return ChannelMessage(
            id=f"{channel}:{ts}",
            channel_id=self.channel_id,
            kind=kind,
            conversation_id=channel,
            sender_id=sender,
            text=text,
            timestamp=datetime.fromtimestamp(int(seconds), UTC).replace(microsecond=int(micros)).isoformat(),
            bot_mentioned=event["type"] == "app_mention" or f"<@{self.user_id}>" in text,
        ), context

    async def _serve(self, ingest: Ingest, status: Callable[[str], None]) -> None:
        url = await _socket_url(self.http, self.config["app_token"])
        async with connect(url, open_timeout=10, close_timeout=2, max_size=1 << 20) as socket:
            async with asyncio.timeout(10):
                hello = json.loads(await socket.recv())
            if not isinstance(hello, dict) or hello.get("type") != "hello":
                raise SlackError("Slack did not initialize the socket.", retryable=True)
            status("connected")
            async for raw in socket:
                envelope = json.loads(raw)
                if not isinstance(envelope, dict):
                    continue
                if envelope.get("type") == "disconnect":
                    return
                envelope_id = envelope.get("envelope_id")
                if not isinstance(envelope_id, str) or not envelope_id:
                    continue
                payload = envelope.get("payload")
                if envelope.get("type") == "events_api" and isinstance(payload, dict):
                    incoming = self.message(payload)
                    if incoming is not None:
                        message, context = incoming
                        await ingest(message, context=context)
                await socket.send(json.dumps({"envelope_id": envelope_id}))

    async def listen(self, cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        delay = 1
        while True:
            retry_after = 0
            try:
                await self._serve(ingest, status)
            except SlackError as error:
                if not error.retryable and not error.retry_after:
                    status("login_required")
                    return
                retry_after = error.retry_after
            except (OSError, TimeoutError, WebSocketException, ValueError):
                pass
            else:
                delay = 1
            status("reconnecting")
            await asyncio.sleep(max(delay, retry_after))
            delay = min(60, delay * 2)

    async def reply(self, message: ChannelMessage, text: str, context: Mapping[str, str]) -> str:
        if message.channel_id != self.channel_id or not _ID.fullmatch(message.conversation_id):
            raise ChannelError("The Slack message is outside this account.")
        body = {
            "channel": message.conversation_id,
            "text": text,
            "mrkdwn": False,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        thread = context.get("thread_ts")
        if thread:
            if not _TS.fullmatch(thread):
                raise ChannelError("The Slack message has an invalid thread reference.")
            body["thread_ts"] = thread
        elif message.kind != "private":
            raise ChannelError("The Slack message has no thread reference.")
        result = await _api(self.http, "chat.postMessage", self.config["bot_token"], body)
        ts = result.get("ts")
        if not isinstance(ts, str) or not _TS.fullmatch(ts) or result.get("channel") != message.conversation_id:
            raise ChannelError("Slack did not confirm the destination and timestamp of the reply.", retryable=True)
        return f"{message.conversation_id}:{ts}"


ADAPTER = Adapter(
    key="slack",
    label="Slack",
    label_zh="Slack",
    kinds=("private", "channel"),
    fields=(
        CredentialField("app_token", "App-level token (xapp-)", "应用级令牌（xapp-）"),
        CredentialField("bot_token", "Bot token (xoxb-)", "机器人令牌（xoxb-）"),
    ),
    bind=bind,
    connect=SlackConnection,
)
