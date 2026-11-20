"""Telegram Bot API wire and cursor behavior."""

import asyncio
import json
import logging

import httpx
import pytest

from a13n_channels.channels import telegram
from a13n_channels.channels.adapter import ChannelError

TOKEN = "1234:test-token"
BOT = {"id": 1234, "is_bot": True, "username": "ChannelsBot"}


def ok(result):
    return httpx.Response(200, json={"ok": True, "result": result})


def endpoint(request: httpx.Request) -> str:
    assert request.method == "POST"
    assert request.url.host == "api.telegram.org"
    prefix = f"/bot{TOKEN}/".encode()
    assert request.url.raw_path.startswith(prefix)
    return request.url.raw_path.removeprefix(prefix).decode()


def incoming(**fields):
    return {
        "message_id": 44,
        "chat": {"id": -100000042, "type": "supergroup"},
        "from": {"id": 73, "is_bot": False},
        "text": "hello",
        "date": 1_700_000_000,
        **fields,
    }


def connection(http):
    return telegram.TelegramConnection(http, "channel", str(BOT["id"]), {"bot_token": TOKEN})


def test_binding_uses_stable_identity_and_redacts_http_log(caplog):
    requests = []

    def respond(request):
        requests.append(endpoint(request))
        assert TOKEN not in repr(request)
        return ok(BOT if requests[-1] == "getMe" else {"url": ""})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            assert await telegram.bind(http, {"bot_token": TOKEN}) == "1234"

    with caplog.at_level(logging.INFO, logger="httpx"):
        asyncio.run(run())
    assert requests == ["getMe", "getWebhookInfo"]
    assert "/bot[redacted]/getMe" in caplog.text
    assert TOKEN not in caplog.text


def test_binding_rejects_an_active_webhook_without_changing_it():
    requests = []

    def respond(request):
        requests.append(endpoint(request))
        return ok(BOT if requests[-1] == "getMe" else {"url": "https://example.com/private-route"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            with pytest.raises(ChannelError, match="Remove the active Telegram webhook") as failure:
                await telegram.bind(http, {"bot_token": TOKEN})
            assert "private-route" not in str(failure.value)

    asyncio.run(run())
    assert requests == ["getMe", "getWebhookInfo"]


def test_message_routing_and_bot_mentions_use_telegram_entities():
    adapter = connection(None)
    bot = (BOT["id"], BOT["username"])
    mention = "@CHANNELSBOT"
    value = incoming(text=f"🙂 {mention}", entities=[{"type": "mention", "offset": 3, "length": len(mention)}])
    value["is_topic_message"] = True
    value["message_thread_id"] = 13
    message, context = adapter.message(value, *bot)
    assert message.id == "-100000042:44"
    assert (message.kind, message.conversation_id, message.bot_mentioned) == ("thread", "-100000042:13", True)
    assert context == {"chat_id": "-100000042", "message_id": "44", "message_thread_id": "13"}

    group_reply, _ = adapter.message(incoming(reply_to_message={"from": BOT}), *bot)
    assert (group_reply.kind, group_reply.conversation_id, group_reply.bot_mentioned) == ("group", "-100000042", True)
    other_bot, _ = adapter.message(
        incoming(
            text="/work@OtherBot",
            entities=[
                {"type": "bot_command", "offset": 0, "length": 14},
            ],
        ),
        *bot,
    )
    assert other_bot.bot_mentioned is False
    command = "/work@ChannelsBot"
    addressed, _ = adapter.message(
        incoming(
            text=command,
            entities=[
                {"type": "bot_command", "offset": 0, "length": len(command)},
            ],
        ),
        *bot,
    )
    assert addressed.bot_mentioned is True

    assert adapter.message(incoming(**{"from": BOT}), *bot) is None
    assert adapter.message(incoming(is_topic_message=True), *bot) is None
    direct, _ = adapter.message(incoming(chat={"id": 73, "type": "private"}), *bot)
    assert (direct.kind, direct.conversation_id) == ("private", "73")
    private_topic, _ = adapter.message(
        incoming(
            chat={"id": 73, "type": "private"},
            is_topic_message=True,
            message_thread_id=12,
        ),
        *bot,
    )
    assert (private_topic.kind, private_topic.conversation_id) == ("private", "73:12")


def test_polling_commits_durable_offsets_for_accepted_and_skipped_updates(monkeypatch):
    monkeypatch.setattr(telegram.time, "time", lambda: 1_700_000_001)
    sleeps = []
    offsets = []
    received = []
    states = []

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(telegram.asyncio, "sleep", sleep)

    def respond(request):
        if endpoint(request) == "getMe":
            return ok(BOT)
        assert endpoint(request) == "getUpdates"
        body = json.loads(request.content)
        assert body["allowed_updates"] == ["message"]
        assert body["timeout"] == 25
        offsets.append(body.get("offset"))
        if len(offsets) == 1:
            return ok(
                [
                    {"update_id": 24, "edited_message": incoming()},
                    {"update_id": 23, "message": incoming()},
                ]
            )
        if len(offsets) == 2:
            return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 5}})
        raise asyncio.CancelledError

    async def ingest(message, *, cursor=None, context=None):
        received.append((message.id if message else None, cursor, context))

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            with pytest.raises(asyncio.CancelledError):
                await connection(http).listen("23:1700000000", ingest, states.append)

    asyncio.run(run())
    assert offsets == [23, 25, 25]
    assert received == [
        ("-100000042:44", "24:1700000001", {"chat_id": "-100000042", "message_id": "44"}),
        (None, "25:1700000001", None),
    ]
    assert sleeps == [5]
    assert states == ["connected", "reconnecting"]


def test_polling_does_not_acknowledge_an_update_when_ingest_fails(monkeypatch):
    monkeypatch.setattr(telegram.time, "time", lambda: 1_700_000_000)
    offsets = []
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(telegram.asyncio, "sleep", sleep)

    def respond(request):
        if endpoint(request) == "getMe":
            return ok(BOT)
        offsets.append(json.loads(request.content).get("offset"))
        if len(offsets) == 1:
            return ok([{"update_id": 23, "message": incoming()}])
        raise asyncio.CancelledError

    async def ingest(message, *, cursor=None, context=None):
        raise ChannelError("The local store is temporarily unavailable.", retryable=True)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            with pytest.raises(asyncio.CancelledError):
                await connection(http).listen("23:1699999999", ingest, lambda state: None)

    asyncio.run(run())
    assert offsets == [23, 23]
    assert sleeps == [1]


def test_polling_drops_stale_offset_before_a_new_telegram_sequence():
    assert telegram._offset("999999:10", telegram._IDLE_CURSOR_AGE + 10) is None
    assert telegram._offset("999999:10", telegram._IDLE_CURSOR_AGE + 9) == 999999


def test_reply_preserves_original_chat_message_and_topic():
    def respond(request):
        assert endpoint(request) == "sendMessage"
        assert json.loads(request.content) == {
            "chat_id": -100000042,
            "message_thread_id": 13,
            "text": "answer",
            "reply_parameters": {"message_id": 44},
        }
        return ok({"message_id": 47})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            adapter = connection(http)
            message, context = adapter.message(
                incoming(is_topic_message=True, message_thread_id=13), 1234, "ChannelsBot"
            )
            assert await adapter.reply(message, "answer", context) == "47"
            with pytest.raises(ChannelError, match="does not match the source"):
                await adapter.reply(message, "answer", {**context, "message_thread_id": "14"})

    asyncio.run(run())


def test_uncertain_reply_is_not_retried_and_the_bearer_token_is_not_exposed():
    requests = []

    def respond(request):
        requests.append(endpoint(request))
        raise httpx.ReadTimeout(f"Unexpected timeout using {TOKEN}", request=request)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            adapter = connection(http)
            message, context = adapter.message(incoming(), 1234, "ChannelsBot")
            with pytest.raises(ChannelError, match="did not confirm") as failure:
                await adapter.reply(message, "answer", context)
            assert failure.value.retryable
            assert TOKEN not in str(failure.value)
            assert failure.value.__suppress_context__

    asyncio.run(run())
    assert requests == ["sendMessage"]
