import asyncio
from types import SimpleNamespace

import httpx
import pytest
from a13n_service.infra.errors import ServiceError
from a13n_service.tenancy.authorize import WorkspaceScope
from test_delivery_routes import incoming, rule, store_for

from a13n_channels.app import ChannelsRuntime
from a13n_channels.channels.adapter import Adapter, ChannelError, CredentialField
from a13n_channels.channels.registry import ADAPTERS
from a13n_channels.connector_accounts import ConnectorInput, save_account


def test_connector_uses_shared_accounts_ingestion_and_reply(tmp_path, monkeypatch):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        host = SimpleNamespace(
            settings=SimpleNamespace(extensions={}), tasks=SimpleNamespace(start=asyncio.create_task)
        )
        replies = []

        class Connector:
            def __init__(self, _http, channel_id, identity, config):
                assert identity == "bot"
                assert config == {"token": "credential"}
                self.channel_id = channel_id

            async def listen(self, cursor, ingest, status):
                assert cursor == ""
                status("connected")
                message = incoming("test").model_copy(update={"channel_id": self.channel_id})
                await ingest(message, cursor="1", context={"reply_token": "private-token"})
                await ingest(message, cursor="2", context={"reply_token": "private-token"})

            async def reply(self, message, text, context):
                replies.append((message.id, text, context))
                return "reply-id"

        async def bind(_http, config):
            assert config["token"] in {"credential", "rotated"}
            return "bot"

        adapter = Adapter(
            "test",
            "Test connector",
            "测试",
            ("private",),
            (CredentialField("token", "Token", "令牌"),),
            bind,
            Connector,
        )
        monkeypatch.setitem(ADAPTERS, adapter.key, adapter)
        scope = WorkspaceScope("org", "workspace")
        async with httpx.AsyncClient() as http:
            runtime = ChannelsRuntime(host, store, http)
            body = ConnectorInput(name="Bot", config={"token": "credential"})
            view = runtime.channel_view(await save_account(runtime, scope, adapter, body, None))
            assert view.account_id == "bot" and view.conversation_kinds == ("private",)
            assert "credential" not in view.model_dump_json()

            with pytest.raises(ServiceError, match="already has a channel"):
                await save_account(runtime, WorkspaceScope("org", "different"), adapter, body, None)

            await store.save_route(
                scope.workspace_id,
                "route",
                rule("Route", "agent", (view.id, "private", "*")),
                principal_id="owner",
                create=True,
            )
            (account,) = await store.query("SELECT * FROM channel_accounts WHERE id = ?", view.id)
            assert "credential" not in account["credentials"]
            await runtime._listen(account)
            (message,) = await store.query("SELECT * FROM connector_messages")
            assert "private-token" not in message["payload"] and "private-token" not in message["context"]
            (delivery,) = await store.query("SELECT message_id FROM channel_deliveries")
            assert delivery["message_id"] == "test"
            (cursor,) = await store.query("SELECT cursor FROM connector_accounts")
            assert cursor["cursor"] == "2"
            assert await store.rename(scope.workspace_id, view.id, "Renamed connector")
            (renamed,) = await store.query("SELECT * FROM channel_accounts WHERE id = ?", view.id)
            assert (renamed["name"], renamed["credentials"], renamed["cursor"]) == (
                "Renamed connector",
                account["credentials"],
                "2",
            )
            assert await runtime.send_reply(account, "test", "Hello") == "reply-id"
            assert replies == [("test", "Hello", {"reply_token": "private-token"})]

            await save_account(runtime, scope, adapter, ConnectorInput(config={"token": "rotated"}), view.id)
            with pytest.raises(ChannelError, match="account changed"):
                await store.receive_connector(account, None, "3", None)
            with pytest.raises(ServiceError):
                await runtime.send_reply(account, "test", "Stale reply")
            assert len(replies) == 1
            await store.query("DELETE FROM connector_accounts WHERE id = ?", view.id)
            assert not await store.query("SELECT 1 FROM connector_messages")
            assert not await store.query("SELECT 1 FROM channel_deliveries")

    asyncio.run(exercise())
