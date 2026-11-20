import asyncio
import base64
from contextlib import AsyncExitStack
from types import SimpleNamespace

from a13n_harness.plugin_factories import build_harness_plugin_factory_catalog
from a13n_service.infra.crypto import KeyRing
from a13n_service.infra.tasks import Tasks
from a13n_service.tenancy.authorize import WorkspaceScope
from pydantic import SecretStr

from a13n_channels import app
from a13n_channels.channels.qq_pairing import QQScanSave


def test_qq_scan_saves_credentials_only_for_the_initiating_workspace_and_actor(tmp_path, monkeypatch):
    connector = tmp_path / "connector"
    connector.mkdir()
    (connector / "package.json").write_text('{"type":"module"}')
    sdk = connector / "dist/esm/index.js"
    sdk.parent.mkdir(parents=True)
    sdk.write_text("""
        export function startQrConnect(events, options) {
            if (options.displayQrCodeToConsole !== false || options.source) throw new Error("expected default identity");
            events.onQrDisplayed("https://q.qq.com/qqbot/openclaw/connect.html?task_id=test");
            setTimeout(() => events.onSuccess([{appId: "123456", appSecret: "sdk-secret"}]), 30);
        }
    """)

    async def no_connections(_active):
        return {}

    tested = []

    class Probe:
        def __init__(self, _http, _channel, credentials):
            self.credentials = credentials

        async def test_connection(self):
            assert self.credentials.app_secret.get_secret_value() == "sdk-secret"
            tested.append(self.credentials.app_id)

    monkeypatch.setattr(app, "QQConnection", Probe)
    monkeypatch.setattr(app.ChannelsRuntime, "_enabled_accounts", no_connections)

    class ScopeSession:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return None

    async def authorize(_session, _actor, workspace, verb):
        assert verb == "write"
        return WorkspaceScope("org", workspace)

    monkeypatch.setattr(app, "short_session", lambda _storage: ScopeSession())
    monkeypatch.setattr(app, "workspace_scope", authorize)
    state = tmp_path / "channels.sqlite3"
    settings = app.ChannelsSettings(state_path=state, qq_connector_path=connector)
    keys = KeyRing(active_key_id="test", keys={"test": SecretStr(base64.b64encode(b"k" * 32).decode())})
    runtime = SimpleNamespace(
        storage=object(),
        settings=SimpleNamespace(extensions={"channels": settings}),
        keys=keys,
        tasks=Tasks(),
        plugins=build_harness_plugin_factory_catalog(plugin_keys=(app.APP_ID, "saurlax.channels.delivery")),
    )
    definition = app.definition()
    endpoints = {route.name: route.endpoint for route in definition.routers[0].routes}
    actor = SimpleNamespace(id="author")

    async def exercise():
        async with AsyncExitStack() as stack:
            await definition.startup(runtime, stack)
            begin = endpoints["start_qq_pairing"]
            read = endpoints["read_qq_pairing"]
            save = endpoints["save_qq_pairing"]
            cancel = endpoints["cancel_qq_pairing"]
            accounts = endpoints["list_channel_accounts"]
            result = await begin(runtime, actor, "ws_qq")
            assert result.qr_image.startswith("data:image/svg+xml;base64,")
            await cancel(result.login_id, runtime, actor, "ws_qq")
            assert (await read(result.login_id, runtime, actor, "ws_qq")).status == "expired"
            result = await begin(runtime, actor, "ws_qq")
            assert (await read(result.login_id, runtime, actor, "ws_other")).status == "expired"
            other = SimpleNamespace(id="other")
            assert (await read(result.login_id, runtime, other, "ws_qq")).status == "expired"
            async with asyncio.timeout(2):
                while (response := await read(result.login_id, runtime, actor, "ws_qq")).status == "wait":
                    await asyncio.sleep(0.01)
            assert response.status == "confirmed"
            assert "123456" not in response.model_dump_json()
            probe = endpoints["test_scanned_qq_connection"]
            assert await probe(result.login_id, runtime, actor, "ws_qq") == {"connected": True}
            assert tested == ["123456"]
            assert (await accounts("ws_qq")).accounts == []
            saved = await save(result.login_id, QQScanSave(name="Scanned"), runtime, actor, "ws_qq")
            assert saved.app_id == "123456"
            assert "sdk-secret" not in response.model_dump_json()
            assert b"sdk-secret" not in state.read_bytes()
            listing = await accounts("ws_qq")
            assert listing.qq_scan_available
            assert [(account.name, account.account_id) for account in listing.accounts] == [("Scanned", "123456")]
            assert (await read(result.login_id, runtime, actor, "ws_qq")).status == "expired"
            second = await begin(runtime, actor, "ws_qq")
            async with asyncio.timeout(2):
                while (await read(second.login_id, runtime, actor, "ws_qq")).status == "wait":
                    await asyncio.sleep(0.01)
            rebound = await save(second.login_id, QQScanSave(channel_id=saved.id), runtime, actor, "ws_qq")
            assert (rebound.id, rebound.name) == (saved.id, saved.name)
            assert len((await accounts("ws_qq")).accounts) == 1

    asyncio.run(exercise())
