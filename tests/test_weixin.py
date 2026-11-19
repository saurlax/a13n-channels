import asyncio
import base64
import json
import sqlite3
from contextlib import AsyncExitStack, asynccontextmanager, closing
from types import SimpleNamespace

import httpx
import pytest
from a13n_harness.plugin_factories import build_harness_plugin_factory_catalog
from a13n_service.infra.crypto import KeyRing
from a13n_service.infra.errors import ServiceError
from a13n_service.infra.tasks import Tasks
from a13n_service.tenancy.authorize import WorkspaceScope
from pydantic import SecretStr

from a13n_channels import app
from a13n_channels.messages import TextReply
from a13n_channels.weixin import PairingPoll, WeixinCreate


def test_weixin_pairing_and_text_exchange(monkeypatch, tmp_path):
    polled = asyncio.Event()
    wx_token = "fixture-token"
    context = "fixture-context"
    upstream_starts = 0

    async def upstream(request):
        nonlocal upstream_starts
        match request.url.path.removeprefix("/ilink/bot/"):
            case "get_bot_qrcode":
                assert request.url.params["bot_type"] == "3"
                return httpx.Response(200, json={"qrcode": "opaque", "qrcode_img_content": "https://weixin.qq.com/qr"})
            case "get_qrcode_status":
                assert request.url.params["qrcode"] == "opaque"
                if "verify_code" not in request.url.params:
                    return httpx.Response(200, json={"status": "need_verifycode"})
                assert request.url.params["verify_code"] == "1234"
                return httpx.Response(
                    200,
                    json={
                        "status": "confirmed",
                        "ilink_bot_id": "bot1",
                        "bot_token": wx_token,
                        "baseurl": "https://ilinkai.weixin.qq.com",
                    },
                )
            case "msg/notifystart":
                upstream_starts += 1
                return httpx.Response(200, json={})
            case "getupdates":
                assert request.headers["Authorization"] == f"Bearer {wx_token}"
                if json.loads(request.content)["get_updates_buf"] == "cursor-1":
                    polled.set()
                    return httpx.Response(200, json={"ret": -14})
                return httpx.Response(
                    200,
                    json={
                        "get_updates_buf": "cursor-1",
                        "msgs": [
                            {
                                "message_type": 1,
                                "message_id": 42,
                                "from_user_id": "user1",
                                "to_user_id": "bot1",
                                "create_time_ms": 1763424000000,
                                "context_token": context,
                                "item_list": [{"type": 1, "text_item": {"text": "hello"}}],
                            }
                        ],
                    },
                )
            case "sendmessage":
                body = json.loads(request.content)["msg"]
                assert body["context_token"] == context
                assert body["to_user_id"] == "user1"
                assert body["item_list"] == [{"type": 1, "text_item": {"text": "reply"}}]
                return httpx.Response(200, json={})
            case _:
                raise AssertionError("Unexpected upstream request")

    @asynccontextmanager
    async def short_session(_storage):
        async def execute(_statement):
            return SimpleNamespace(all=lambda: [("ws_wx", "org", {"app_plugins": {"enabled": [app.APP_ID]}})])

        yield SimpleNamespace(execute=execute)

    async def authorize(_session, actor, workspace, verb):
        assert actor == "writer" and verb == "write"
        return WorkspaceScope("org", workspace)

    def client(**kwargs):
        return httpx.AsyncClient(**kwargs, transport=httpx.MockTransport(upstream))

    monkeypatch.setattr(app, "httpx", SimpleNamespace(AsyncClient=client, HTTPError=httpx.HTTPError))
    monkeypatch.setattr(app, "short_session", short_session)
    monkeypatch.setattr(app, "workspace_scope", authorize)
    definition = app.definition()
    state = tmp_path / "channels.sqlite3"
    settings = SimpleNamespace(extensions={"channels": app.ChannelsSettings(state_path=state)})
    keys = KeyRing(active_key_id="test", keys={"test": SecretStr(base64.b64encode(b"k" * 32).decode())})
    runtime = SimpleNamespace(
        storage=object(),
        settings=settings,
        keys=keys,
        tasks=Tasks(),
        plugins=build_harness_plugin_factory_catalog(plugin_keys=(app.APP_ID, "saurlax.channels.delivery")),
    )
    endpoints = {route.name: route.endpoint for route in definition.routers[0].routes}

    async def exercise():
        async with AsyncExitStack() as stack:
            await definition.startup(runtime, stack)
            workspace = (runtime, "writer", "ws_wx")
            account = await endpoints["add_weixin_channel"](WeixinCreate(name="Weixin"), *workspace)
            assert account.platform == "weixin" and account.state == "login_required"
            with pytest.raises(ServiceError, match="weixin_channel"):
                await endpoints["start_weixin_pairing"](account.id, runtime, "writer", "ws_other")
            start = await endpoints["start_weixin_pairing"](account.id, *workspace)
            assert start.qr_image.startswith("data:image/svg+xml;base64,")
            poll = endpoints["poll_weixin_pairing"]
            assert (
                await poll(account.id, PairingPoll(login_id=start.login_id), *workspace)
            ).status == "need_verifycode"
            proof = PairingPoll(login_id=start.login_id, verify_code="1234")
            assert (await poll(account.id, proof, *workspace)).status == "confirmed"
            assert (await endpoints["list_channel_accounts"]("ws_wx")).accounts[0].account_id is None
            save = endpoints["save_weixin_pairing"]
            assert (await save(account.id, proof, *workspace)).status == "confirmed"
            assert (await poll(account.id, proof, *workspace)).status == "expired"
            await asyncio.wait_for(polled.wait(), timeout=2)
            listed = (await endpoints["list_channel_accounts"]("ws_wx")).accounts
            assert listed[0].account_id == "bot1" and listed[0].state == "login_required"
            messages = await endpoints["list_weixin_messages"](account.id, "ws_wx")
            assert [message.text for message in messages] == ["hello"]
            assert context not in messages[0].model_dump_json()
            wire = state.read_bytes()
            assert wx_token.encode() not in wire and context.encode() not in wire
            receipt = await endpoints["reply_to_weixin_message"](
                account.id, TextReply(text="reply"), *workspace, messages[0].id
            )
            assert receipt["message_id"].startswith("wxmsg_")
            assert upstream_starts == 1
            await endpoints["delete_weixin_channel"](account.id, *workspace)
            assert (await endpoints["list_channel_accounts"]("ws_wx")).accounts == []
            with closing(sqlite3.connect(state)) as db:
                assert db.execute("SELECT message_id FROM weixin_messages").fetchall() == []

    asyncio.run(exercise())
