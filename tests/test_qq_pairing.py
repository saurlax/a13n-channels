import asyncio
import base64
import json
from contextlib import AsyncExitStack
from types import SimpleNamespace

import httpx
from a13n_harness.plugin_factories import build_harness_plugin_factory_catalog
from a13n_service.infra.crypto import KeyRing
from a13n_service.infra.tasks import Tasks
from a13n_service.tenancy.authorize import WorkspaceScope
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import SecretStr

from a13n_channels import app
from a13n_channels.channels import qq_pairing
from a13n_channels.channels.qq_pairing import QQScanSave


def test_qq_scan_saves_credentials_only_for_the_initiating_workspace_and_actor(tmp_path, monkeypatch):
    tasks = {}
    polls = {}

    def upstream(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.scheme == "https" and request.url.host == "q.qq.com"
        body = json.loads(request.content)
        if request.url.path == "/lite/create_bind_task":
            key = base64.b64decode(body["key"], validate=True)
            assert len(key) == 32 and key not in tasks.values()
            task_id = f"task-{len(tasks) + 1}"
            tasks[task_id] = key
            return httpx.Response(200, json={"retcode": 0, "data": {"task_id": task_id}})
        assert request.url.path == "/lite/poll_bind_result"
        task_id = body["task_id"]
        polls[task_id] = polls.get(task_id, 0) + 1
        if task_id == "task-2":
            data = {"status": 3}
        elif task_id == "task-1" or polls[task_id] == 2:
            data = {"status": 1}
        elif polls[task_id] == 1:
            raise httpx.ReadTimeout("Transient poll timeout")
        else:
            nonce = b"n" * 12
            encrypted = nonce + AESGCM(tasks[task_id]).encrypt(nonce, b"qq-scan-secret", None)
            data = {"status": 2, "bot_appid": 123456, "bot_encrypt_secret": base64.b64encode(encrypted).decode()}
        return httpx.Response(200, json={"retcode": 0, "data": data})

    def client(**kwargs):
        return httpx.AsyncClient(**kwargs, transport=httpx.MockTransport(upstream))

    monkeypatch.setattr(app, "httpx", SimpleNamespace(AsyncClient=client, HTTPError=httpx.HTTPError))
    monkeypatch.setattr(qq_pairing, "QQ_BIND_POLL_SECONDS", 0.01)

    async def no_connections(_active):
        return {}

    tested = []

    class Probe:
        def __init__(self, _http, _channel, credentials):
            self.credentials = credentials

        async def test_connection(self):
            assert self.credentials.app_secret.get_secret_value() == "qq-scan-secret"
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
    settings = app.ChannelsSettings(state_path=state)
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
            assert (await accounts("ws_qq")).qq_scan_available
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
            assert "qq-scan-secret" not in response.model_dump_json()
            assert b"qq-scan-secret" not in state.read_bytes()
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
            assert len(tasks) == 4 and polls["task-2"] == 1

    asyncio.run(exercise())
