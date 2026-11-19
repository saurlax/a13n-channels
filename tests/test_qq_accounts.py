import asyncio
import base64
import sqlite3
from contextlib import AsyncExitStack, asynccontextmanager, closing
from types import SimpleNamespace

import pytest
from a13n_harness.plugin_factories import build_harness_plugin_factory_catalog
from a13n_service.infra.crypto import KeyRing
from a13n_service.infra.errors import ServiceError
from a13n_service.infra.tasks import Tasks
from a13n_service.settings import load_settings
from a13n_service.tenancy.authorize import WorkspaceScope
from pydantic import SecretStr

from a13n_channels import app
from a13n_channels.messages import ChannelMessage, TextReply
from a13n_channels.qq import QQCreate


def test_qq_accounts_and_messages_are_owned_by_the_workspace(tmp_path, monkeypatch):
    allowed = True
    stopped = asyncio.Event()
    stored = asyncio.Event()
    received = ChannelMessage(
        id="msg-1",
        channel_id="placeholder",
        kind="private",
        conversation_id="user-1",
        sender_id="user-1",
        text="hello",
        timestamp="2026-11-18T00:00:00Z",
    )

    @asynccontextmanager
    async def short_session(_storage):
        async def execute(_statement):
            return SimpleNamespace(all=lambda: [("ws_qq", "org_qq", {"app_plugins": {"enabled": [app.APP_ID]}})])

        yield SimpleNamespace(execute=execute)

    async def authorize(_session, actor, workspace_id, verb):
        assert actor == "writer"
        assert verb == "write"
        if not allowed:
            raise ServiceError("forbidden", "Workspace write required")
        return WorkspaceScope("org_qq", workspace_id)

    class QQFixture:
        def __init__(self, _http, channel_id, credentials):
            self.channel_id = channel_id
            self.credentials = credentials

        async def listen(self, receive, status):
            assert self.credentials.app_secret.get_secret_value() in {"qq-secret", "updated"}
            try:
                status("connected")
                if self.credentials.app_id == "10001":
                    incoming = received.model_copy(update={"channel_id": self.channel_id})
                    await receive(incoming)
                    await receive(incoming)
                    stored.set()
                await asyncio.Future()
            finally:
                stopped.set()

        async def reply(self, incoming, text, sequence):
            assert incoming.id == "msg-1"
            assert incoming.channel_id == self.channel_id
            assert text == "world"
            return f"sent-{sequence}"

    monkeypatch.setattr(app, "QQConnection", QQFixture)
    monkeypatch.setattr(app, "short_session", short_session)
    monkeypatch.setattr(app, "workspace_scope", authorize)
    definition = app.definition()
    state = tmp_path / "qq.sqlite3"
    config = tmp_path / "service.toml"
    config.write_text(f'[channels]\nstate_path = "{state}"\n')
    settings = load_settings(config, extensions=definition.settings)
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
        nonlocal allowed
        async with AsyncExitStack() as stack:
            await definition.startup(runtime, stack)
            list_accounts = endpoints["list_qq_accounts"]
            create = endpoints["add_qq_account"]
            update = endpoints["update_qq_account"]
            delete = endpoints["delete_qq_account"]
            messages = endpoints["list_qq_messages"]
            reply = endpoints["reply_to_qq_message"]
            body = QQCreate(name="Test QQ", app_id="10001", app_secret="qq-secret")
            allowed = False
            with pytest.raises(ServiceError, match="Workspace write required"):
                await create(body, runtime, "writer", "ws_qq")
            allowed = True
            account = await create(body, runtime, "writer", "ws_qq")
            assert "app_secret" not in account.model_dump()
            assert b"qq-secret" not in state.read_bytes()
            with pytest.raises(ServiceError, match="already registered"):
                await create(body, runtime, "writer", "ws_qq")
            await asyncio.wait_for(stored.wait(), timeout=2)
            assert (await list_accounts("ws_qq")).accounts[0].state == "connected"
            assert (await list_accounts("ws_other")).accounts == []
            with pytest.raises(ServiceError, match="qq_channel"):
                await messages(account.id, "ws_other")
            with pytest.raises(ServiceError, match="qq_channel"):
                await delete(account.id, runtime, "writer", "ws_other")
            assert [item.id for item in await messages(account.id, "ws_qq")] == ["msg-1"]
            for sequence in (1, 2):
                receipt = await reply(account.id, TextReply(text="world"), runtime, "writer", "ws_qq", "msg-1")
                assert receipt == {"message_id": f"sent-{sequence}"}
            with pytest.raises(ServiceError, match="AppSecret is required"):
                await update(account.id, app.QQEdit(app_id="10002"), runtime, "writer", "ws_qq")
            edited = await update(
                account.id, app.QQEdit(app_id="10002", app_secret="updated"), runtime, "writer", "ws_qq"
            )
            assert (edited.id, edited.name, edited.app_id) == (account.id, account.name, "10002")
            await asyncio.wait_for(stopped.wait(), timeout=2)
            assert await messages(account.id, "ws_qq") == []
            with pytest.raises(ServiceError, match="qq_message"):
                await reply(account.id, TextReply(text="world"), runtime, "writer", "ws_qq", "msg-1")
            with closing(sqlite3.connect(state)) as db:
                assert db.execute("SELECT message_id FROM qq_messages").fetchall() == [("msg-1",)]
                await delete(account.id, runtime, "writer", "ws_qq")
                assert db.execute("SELECT message_id FROM qq_messages").fetchall() == []
            assert (await list_accounts("ws_qq")).accounts == []

    asyncio.run(exercise())
