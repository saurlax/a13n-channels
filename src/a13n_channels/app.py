"""Workspace channel management and outbound listeners."""

import asyncio
import fcntl
import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

import httpx
import jwt
from a13n_service.app_definitions import AppDefinition
from a13n_service.app_plugins import Frontend
from a13n_service.infra.db import short_session
from a13n_service.infra.errors import ServiceError, not_found
from a13n_service.infra.ids import new_object_id
from a13n_service.resources.agents import service as agents
from a13n_service.runs import threads
from a13n_service.runs.runtime import Runtime
from a13n_service.settings import Section
from a13n_service.tenancy.access import WorkspaceScope, workspace_scope
from a13n_service.tenancy.app_plugins import app_router, policy
from a13n_service.tenancy.authorize import authorize
from a13n_service.tenancy.requests import Actor, CurrentRuntime, WorkspaceId
from a13n_service.tenancy.tables import OrganizationRow, WorkspaceRow
from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from websockets.exceptions import WebSocketException

from a13n_channels.accounts import Accounts
from a13n_channels.delivery import Deliveries
from a13n_channels.github import GITHUB_KINDS, GitHubBind, GitHubConnection, GitHubPlatform, stamp
from a13n_channels.messages import ChannelMessage, TextReply
from a13n_channels.qq import QQ_CONVERSATION_KINDS, AppId, ChannelId, QQAccount, QQConnection, QQCreate, QQSecret
from a13n_channels.qq_pairing import LoginId, QQScan, QQScanCredentials, QQScans, QQScanSave, QQScanStatus
from a13n_channels.routing import RouteInput
from a13n_channels.tools import DELIVERY_FACTORY_KEY, FACTORY_KEY, ChannelToolFactory
from a13n_channels.weixin import (
    WEIXIN_CONVERSATION_KIND,
    LoginRequired,
    Pairing,
    PairingPoll,
    PairingResult,
    WeixinConnection,
    WeixinCreate,
    poll_pairing,
    start_pairing,
)

APP_ID = "saurlax.channels"
SETTINGS = "channels"
CONVERSATION_KINDS = {
    "qq": QQ_CONVERSATION_KINDS,
    "weixin": (WEIXIN_CONVERSATION_KIND,),
    "github": GITHUB_KINDS,
    "github_app": GITHUB_KINDS,
}


class ChannelsSettings(Section):
    state_path: Path | None = None
    qq_connector_path: Path | None = None


class QQEdit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    app_id: AppId | None = None
    app_secret: SecretStr | None = Field(default=None, min_length=1, max_length=4096)


class QQAccounts(BaseModel):
    configured: bool
    accounts: list[QQAccount]


class ChannelAccount(BaseModel):
    id: str
    platform: Literal["qq", "weixin", "github", "github_app"]
    name: str
    account_id: str | None
    state: str
    conversation_kinds: tuple[str, ...]


class ChannelList(BaseModel):
    configured: bool
    accounts: list[ChannelAccount]
    qq_scan_available: bool = False


class ChannelsRuntime:
    def __init__(self, runtime: Runtime, store: Accounts, http: httpx.AsyncClient):
        self.host = runtime
        self.store = store
        self.http = http
        self.members: dict[str, tuple[str, asyncio.Task]] = {}
        self.status: dict[str, str] = {}
        self.changed = asyncio.Event()
        self.pairings: dict[str, Pairing] = {}
        self.qq_scans = QQScans()
        self.qq_probes: set[str] = set()
        settings = runtime.settings.extensions.get(SETTINGS, ChannelsSettings())
        self.qq_connector = settings.qq_connector_path
        self.deliveries = Deliveries(store, runtime)

    def view(self, row: sqlite3.Row) -> QQAccount:
        return QQAccount(
            id=row["id"], name=row["name"], app_id=row["app_id"], state=self.status.get(row["id"], "inactive")
        )

    async def _enabled_accounts(self) -> dict[str, sqlite3.Row]:
        rows = await self.store.query("""
            SELECT id, organization_id, workspace_id, name, app_id, app_secret AS generation,
                   app_secret, intents, '' AS cursor, NULL AS credentials, 'qq' AS platform FROM qq_accounts
            UNION ALL
            SELECT id, organization_id, workspace_id, name, bot_id, credentials,
                   NULL, NULL, cursor, credentials, 'weixin' FROM weixin_accounts WHERE credentials IS NOT NULL
            UNION ALL
            SELECT id, organization_id, workspace_id, name, app_id, credentials,
                   NULL, NULL, cursor, credentials, platform FROM github_accounts
        """)
        if not rows:
            return {}
        statement = (
            select(WorkspaceRow.id, WorkspaceRow.organization_id, OrganizationRow.settings)
            .join(OrganizationRow, OrganizationRow.id == WorkspaceRow.organization_id)
            .where(WorkspaceRow.id.in_({row["workspace_id"] for row in rows}), WorkspaceRow.archived_at.is_(None))
        )
        async with short_session(self.host.storage) as session:
            scopes = {
                (ws, org)
                for ws, org, settings in (await session.execute(statement)).all()
                if APP_ID in policy(settings).enabled
            }
        return {row["id"]: row for row in rows if (row["workspace_id"], row["organization_id"]) in scopes}

    async def serve(self) -> None:
        try:
            while True:
                self.changed.clear()
                try:
                    desired = await self._enabled_accounts()
                except SQLAlchemyError:
                    desired = {}
                for channel_id, (generation, task) in list(self.members.items()):
                    row = desired.get(channel_id)
                    if (
                        row is None
                        or generation != row["generation"]
                        or (task.done() and self.status.get(channel_id) != "login_required")
                    ):
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        self.members.pop(channel_id)
                        self.status.pop(channel_id, None)
                for channel_id, row in desired.items():
                    if channel_id not in self.members:
                        try:
                            task = self._listen(row)
                        except ServiceError:
                            self.status[channel_id] = "error"
                            continue
                        self.members[channel_id] = (row["generation"], task)
                await self.qq_scans.expire()
                now = asyncio.get_running_loop().time()
                self.pairings = {key: item for key, item in self.pairings.items() if item.expires > now}
                try:
                    await asyncio.wait_for(self.changed.wait(), timeout=3)
                except TimeoutError:
                    pass
        finally:
            tasks = [task for _, task in self.members.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _listen(self, row: sqlite3.Row) -> asyncio.Task:
        channel_id = row["id"]

        def status(state: str) -> None:
            self.status[channel_id] = state

        if row["platform"] == "qq":
            qq = QQConnection(self.http, channel_id, self.store.credentials(row))

            async def receive(message: ChannelMessage) -> None:
                await self.store.receive(message, row["generation"])

            work = qq.listen(receive, status)
        elif row["platform"] == "weixin":
            weixin = WeixinConnection(self.http, channel_id, self.store.weixin_credentials(row))

            async def ingest(cursor: str, messages: list[tuple[ChannelMessage, str]]) -> None:
                await self.store.receive_weixin(row, cursor, messages)

            work = weixin.listen(row["cursor"], ingest, status)
        else:
            binding = self.store.github_credentials(row)

            async def ingest_github(message: ChannelMessage | None, cursor: str | None) -> None:
                await self.store.receive_github(row, message, cursor)

            async def repositories() -> set[str]:
                return await self.store.github_repositories(row)

            work = GitHubConnection(self.http, channel_id, binding).listen(
                binding, row["cursor"], repositories, ingest_github, status
            )
        status("connecting")
        return self.host.tasks.start(work, name=f"channels-{channel_id}")

    async def send_reply(self, account: sqlite3.Row, message_id: str, text: str) -> str:
        channel_id = account["id"]
        if account["platform"] == "qq":
            credentials = self.store.credentials(account)
            source = await self.store.query(
                """UPDATE qq_messages SET reply_sequence = reply_sequence + 1
                   WHERE channel_id = ? AND account_id = ? AND message_id = ? RETURNING payload, reply_sequence""",
                channel_id,
                credentials.app_id,
                message_id,
            )
            if not source:
                raise not_found("qq_message", message_id)
            incoming = ChannelMessage.model_validate_json(source[0]["payload"])
            try:
                return await QQConnection(self.http, channel_id, credentials).reply(
                    incoming, text, source[0]["reply_sequence"]
                )
            except httpx.HTTPStatusError as error:
                if error.response.status_code < 500:
                    raise ServiceError(
                        "invalid_argument", "QQ rejected the reply. Check message expiry and bot permissions."
                    ) from None
                raise ServiceError("unavailable", "QQ did not confirm the reply.") from None
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                raise ServiceError("unavailable", "QQ did not confirm the reply.") from None

        if account["platform"] in {"github", "github_app"}:
            rows = await self.store.query(
                "SELECT payload FROM github_messages WHERE channel_id = ? AND account_id = ? AND message_id = ?",
                channel_id,
                account["app_id"],
                message_id,
            )
            if not rows:
                raise not_found("github_message", message_id)
            binding = self.store.github_credentials(account)
            incoming = ChannelMessage.model_validate_json(rows[0]["payload"])
            try:
                return await GitHubConnection(self.http, channel_id, binding).reply(incoming, text)
            except httpx.HTTPStatusError as error:
                if error.response.status_code < 500:
                    raise ServiceError(
                        "invalid_argument", "GitHub rejected the comment. Check repository permissions."
                    ) from None
                raise ServiceError("unavailable", "GitHub did not confirm the comment.") from None
            except (httpx.HTTPError, jwt.PyJWTError, ValueError, KeyError, TypeError):
                raise ServiceError("unavailable", "GitHub did not confirm the comment.") from None

        source = await self.store.query(
            "SELECT payload, context FROM weixin_messages WHERE channel_id = ? AND account_id = ? AND message_id = ?",
            channel_id,
            account["app_id"],
            message_id,
        )
        if not source:
            raise not_found("weixin_message", message_id)
        if not account["credentials"] or not source[0]["context"]:
            raise ServiceError("invalid_argument", "Pair the bot and choose a message with a reply context.")
        incoming = ChannelMessage.model_validate_json(source[0]["payload"])
        context = self.store.unseal(
            account["organization_id"],
            "channels.weixin_messages",
            "context",
            f"{channel_id}:{incoming.id}",
            source[0]["context"],
        ).decode()
        connection = WeixinConnection(self.http, channel_id, self.store.weixin_credentials(account))
        try:
            return await connection.reply(incoming, text, context, new_object_id("wxmsg"))
        except LoginRequired:
            raise ServiceError("invalid_argument", "Weixin session expired. Pair the bot again.") from None
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            raise ServiceError("unavailable", "Weixin did not confirm the reply.") from None

    def channel_view(self, row: sqlite3.Row) -> ChannelAccount:
        default = "login_required" if row["platform"] == "weixin" and not row["account_id"] else "inactive"
        return ChannelAccount(
            id=row["id"],
            platform=row["platform"],
            name=row["name"],
            account_id=row["account_id"],
            state=self.status.get(row["id"], default),
            conversation_kinds=CONVERSATION_KINDS[row["platform"]],
        )


def definition() -> AppDefinition:
    router = app_router(APP_ID)
    channels: ChannelsRuntime | None = None

    @asynccontextmanager
    async def open_channels(runtime: Runtime) -> AsyncIterator[None]:
        nonlocal channels
        path = runtime.settings.extensions.get(SETTINGS, ChannelsSettings()).state_path
        if path is None:
            yield
            return
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.with_suffix(path.suffix + ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            store = Accounts(path, runtime.keys)
            await store.initialize()
            async with httpx.AsyncClient(timeout=15, follow_redirects=False, trust_env=False) as http:
                active = ChannelsRuntime(runtime, store, http)
                factories = [runtime.plugins.require(key) for key in (FACTORY_KEY, DELIVERY_FACTORY_KEY)]
                for factory in factories:
                    assert isinstance(factory, ChannelToolFactory)
                    factory.channels = active
                channels = active
                supervisor = runtime.tasks.start(active.serve(), name="channels-listeners", service=True)
                delivery = runtime.tasks.start(
                    active.deliveries.run(active._enabled_accounts),
                    name="channels-delivery",
                    service=True,
                )
                tasks = (supervisor, delivery)
                try:
                    yield
                finally:
                    for factory in factories:
                        factory.channels = None
                    channels = None
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await active.qq_scans.close()

    async def startup(runtime: Runtime, stack: AsyncExitStack) -> None:
        await stack.enter_async_context(open_channels(runtime))

    def require_channels() -> ChannelsRuntime:
        if channels is None:
            raise ServiceError("unavailable", "Channels storage is not configured.")
        return channels

    async def writable_scope(runtime: Runtime, actor: Actor, workspace_id: str) -> WorkspaceScope:
        async with short_session(runtime.storage) as session:
            return await workspace_scope(session, actor, workspace_id, "write")

    @router.get("/accounts", name="list_channel_accounts")
    async def list_channels(workspace_id: WorkspaceId) -> ChannelList:
        if channels is None:
            return ChannelList(configured=False, accounts=[])
        rows = await channels.store.query(
            """SELECT id, name, app_id AS account_id, 'qq' AS platform FROM qq_accounts WHERE workspace_id = ?
               UNION ALL
               SELECT id, name, bot_id, 'weixin' FROM weixin_accounts WHERE workspace_id = ?
               UNION ALL
               SELECT id, name, app_id, platform FROM github_accounts WHERE workspace_id = ? ORDER BY name, id""",
            workspace_id,
            workspace_id,
            workspace_id,
        )
        return ChannelList(
            configured=True,
            accounts=[channels.channel_view(row) for row in rows],
            qq_scan_available=channels.qq_connector is not None,
        )

    @router.get("/routes", name="list_channel_routes")
    async def list_routes(workspace_id: WorkspaceId) -> dict:
        if channels is None:
            return {"configured": False, "routes": []}
        routes = await channels.store.routes(workspace_id)
        attention = {
            row["route_id"]: row["count"]
            for row in await channels.store.query(
                """SELECT route_id, count(*) AS count FROM channel_deliveries
                   WHERE workspace_id = ? AND (status IN ('failed', 'unknown')
                       OR (status IN ('wake_queued', 'wake_submitted', 'queued', 'submitted') AND error IS NOT NULL))
                   GROUP BY route_id""",
                workspace_id,
            )
        }
        return {
            "configured": True,
            "routes": [
                {
                    **route.model_dump(exclude={"binding_id", "principal_id"}),
                    "attention": attention.get(route.id, 0),
                    "needs_authorization": route.principal_id is None,
                }
                for route in routes
            ],
        }

    @router.get("/route-agents", name="list_route_agents")
    async def route_agents(
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
        cursor: str | None = None,
    ) -> dict:
        page = await agents.list_agents(
            runtime.storage,
            actor,
            workspace_id,
            labels=[],
            archived=False,
            limit=100,
            cursor=cursor,
        )
        return {
            "items": [{"id": agent.id, "name": agent.name} for agent in page.items if agent.default_revision_id],
            "next_cursor": page.next_cursor,
        }

    @router.get("/route-sources", name="list_route_sources")
    async def route_sources(workspace_id: WorkspaceId, channel_id: ChannelId, kind: str) -> list[dict[str, str]]:
        active = require_channels()
        rows = await active.store.query(
            """WITH messages AS (
                   SELECT payload, qq_messages.rowid AS position FROM qq_messages
                   JOIN qq_accounts ON qq_accounts.id = qq_messages.channel_id AND app_id = account_id
                   WHERE qq_accounts.workspace_id = ? AND channel_id = ?
                   UNION ALL
                   SELECT payload, weixin_messages.rowid FROM weixin_messages
                   JOIN weixin_accounts ON weixin_accounts.id = weixin_messages.channel_id AND bot_id = account_id
                   WHERE weixin_accounts.workspace_id = ? AND channel_id = ?
                   UNION ALL
                   SELECT payload, github_messages.rowid FROM github_messages
                   JOIN github_accounts ON github_accounts.id = github_messages.channel_id AND app_id = account_id
                   WHERE github_accounts.workspace_id = ? AND channel_id = ?
               )
               SELECT json_extract(payload, '$.conversation_id') AS id FROM messages
               WHERE json_extract(payload, '$.kind') = ? GROUP BY id ORDER BY max(position) DESC LIMIT 100""",
            workspace_id,
            channel_id,
            workspace_id,
            channel_id,
            workspace_id,
            channel_id,
            kind,
        )
        return [{"id": row["id"]} for row in rows]

    async def save_route(
        body: RouteInput,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: str,
        route_id: str,
        *,
        create: bool,
    ) -> dict:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        authorize(actor, scope, "run")
        agent = await agents.get_agent(runtime.storage, actor, scope.workspace_id, body.agent_id)
        if agent.archived_at or not agent.default_revision_id:
            raise ServiceError("invalid_argument", "Choose a published Agent in this workspace.")
        if body.wake_agent_id:
            try:
                wake_agent = await agents.get_agent(runtime.storage, actor, scope.workspace_id, body.wake_agent_id)
            except ServiceError as error:
                if error.code != "not_found":
                    raise
                raise ServiceError(
                    "invalid_argument", "Choose a wake Agent in this workspace.", {"field": "wake_agent_id"}
                ) from error
            if wake_agent.archived_at or not wake_agent.default_revision_id:
                raise ServiceError("invalid_argument", "Choose a published wake Agent.", {"field": "wake_agent_id"})
        if body.fixed_thread_id:
            try:
                thread = await threads.get(runtime.storage, actor, scope.workspace_id, body.fixed_thread_id)
            except ServiceError as error:
                if error.code != "not_found":
                    raise
                raise ServiceError(
                    "invalid_argument", "Choose a Thread in this workspace.", {"field": "fixed_thread_id"}
                ) from error
            if thread.archived_at or thread.purpose != "conversation":
                raise ServiceError(
                    "invalid_argument", "Choose an active conversation Thread.", {"field": "fixed_thread_id"}
                )
        route = await active.store.save_route(scope.workspace_id, route_id, body, principal_id=actor.id, create=create)
        return route.model_dump(exclude={"binding_id", "principal_id"})

    @router.post("/routes", name="add_channel_route", status_code=201)
    async def add_route(body: RouteInput, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId) -> dict:
        return await save_route(body, runtime, actor, workspace_id, new_object_id("chroute"), create=True)

    @router.put("/routes/{route_id}", name="update_channel_route")
    async def edit_route(
        route_id: ChannelId,
        body: RouteInput,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> dict:
        return await save_route(body, runtime, actor, workspace_id, route_id, create=False)

    @router.delete("/routes/{route_id}", name="delete_channel_route", status_code=204)
    async def delete_route(
        route_id: ChannelId, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> None:
        scope = await writable_scope(runtime, actor, workspace_id)
        await require_channels().store.delete_route(scope.workspace_id, route_id)

    async def save_github(platform, body, runtime, actor, workspace_id, channel_id=None):
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        if bool(body.token) != (platform == "github"):
            raise ServiceError("invalid_argument", "Credentials must match the GitHub channel type.")
        previous = None
        if channel_id:
            rows = await active.store.query(
                "SELECT * FROM github_accounts WHERE id = ? AND workspace_id = ? AND platform = ?",
                channel_id,
                workspace_id,
                platform,
            )
            if not rows:
                raise not_found("github_channel", channel_id)
            previous = rows[0]
        else:
            channel_id = new_object_id("gh")
            if not body.name or not body.name.strip():
                raise ServiceError("invalid_argument", "Enter a channel name.")
        try:
            binding = await GitHubConnection(active.http, channel_id, body).bind(body)
        except (httpx.HTTPError, jwt.PyJWTError, ValueError, KeyError, TypeError):
            raise ServiceError(
                "invalid_argument", "GitHub could not verify the credentials, repositories or allowed users."
            ) from None
        if previous and previous["app_id"] != binding.identity:
            raise ServiceError("invalid_argument", "Use a new channel for a different GitHub identity.")
        payload = binding.model_dump(mode="json", exclude={"token", "private_key"}, exclude_none=True)
        if binding.token:
            payload["token"] = binding.token.get_secret_value()
        if binding.private_key:
            payload["private_key"] = binding.private_key.get_secret_value()
        credentials = active.store.seal(
            scope.organization_id,
            "channels.github_accounts",
            "credentials",
            channel_id,
            json.dumps(payload).encode(),
        )
        try:
            if previous:
                rows = await active.store.query(
                    """UPDATE github_accounts SET credentials = ?, cursor = ?
                       WHERE id = ? AND workspace_id = ? AND credentials = ?
                       RETURNING id, name, app_id AS account_id, platform""",
                    credentials,
                    stamp(),
                    channel_id,
                    workspace_id,
                    previous["credentials"],
                )
                if not rows:
                    raise ServiceError("conflict", "The GitHub account changed. Reopen the editor and retry.")
            else:
                rows = await active.store.query(
                    """INSERT INTO github_accounts VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       RETURNING id, name, app_id AS account_id, platform""",
                    channel_id,
                    scope.organization_id,
                    workspace_id,
                    body.name.strip(),
                    platform,
                    binding.identity,
                    credentials,
                    stamp(),
                )
        except sqlite3.IntegrityError:
            raise ServiceError("conflict", "This GitHub identity is already connected in the workspace.") from None
        active.changed.set()
        return active.channel_view(rows[0])

    @router.get("/qq/accounts", name="list_qq_accounts")
    async def accounts(workspace_id: WorkspaceId) -> QQAccounts:
        if channels is None:
            return QQAccounts(configured=False, accounts=[])
        rows = await channels.store.query(
            "SELECT * FROM qq_accounts WHERE workspace_id = ? ORDER BY name, id", workspace_id
        )
        return QQAccounts(configured=True, accounts=[channels.view(row) for row in rows])

    async def create_qq(active: ChannelsRuntime, scope: WorkspaceScope, body: QQCreate) -> QQAccount:
        if body.app_id in active.qq_probes:
            raise ServiceError("conflict", "Wait for the QQ connection test to finish before saving.")
        channel_id = new_object_id("qq")
        secret = active.store.encrypt(scope.organization_id, channel_id, body)
        try:
            rows = await active.store.query(
                "INSERT INTO qq_accounts VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING *",
                channel_id,
                scope.organization_id,
                scope.workspace_id,
                body.name,
                body.app_id,
                secret,
                body.qq_intents,
            )
        except sqlite3.IntegrityError:
            raise ServiceError("conflict", "This QQ AppID is already registered.") from None
        active.changed.set()
        return active.view(rows[0])

    @router.post("/qq/accounts", name="add_qq_account", status_code=201)
    async def add(body: QQCreate, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId) -> QQAccount:
        scope = await writable_scope(runtime, actor, workspace_id)
        return await create_qq(require_channels(), scope, body)

    async def test_qq(
        active: ChannelsRuntime, credentials: QQScanCredentials, scope: WorkspaceScope, channel_id: str | None
    ) -> dict[str, bool]:
        app_id = credentials.app_id
        if app_id in active.qq_probes:
            raise ServiceError("conflict", "A connection test for this QQ bot is already running.")
        active.qq_probes.add(app_id)
        try:
            if channel_id is not None and not await active.store.query(
                "SELECT id FROM qq_accounts WHERE id = ? AND workspace_id = ?", channel_id, scope.workspace_id
            ):
                raise not_found("qq_channel", channel_id)
            registered = await active.store.query("SELECT id FROM qq_accounts WHERE app_id = ?", app_id)
            if registered and registered[0]["id"] != channel_id:
                raise ServiceError("conflict", "This QQ AppID is already registered.")
            probe = QQConnection(
                active.http,
                "qq_test",
                QQCreate(name="Connection test", app_id=app_id, app_secret=credentials.app_secret),
            )
            if registered:
                await probe.verify_credentials()
                if active.status.get(channel_id) != "connected":
                    raise ServiceError("unavailable", "The current QQ listener is disconnected.")
            else:
                await probe.test_connection()
        except httpx.HTTPStatusError as error:
            raise ServiceError(
                "unavailable", f"QQ rejected the connection test (HTTP {error.response.status_code})."
            ) from None
        except (httpx.HTTPError, WebSocketException, OSError, ValueError, KeyError, TypeError):
            raise ServiceError("unavailable", "QQ did not confirm the WebSocket connection.") from None
        finally:
            active.qq_probes.remove(app_id)
        return {"connected": True}

    @router.post("/qq/test", name="test_manual_qq_connection")
    async def test_manual_qq(
        body: QQScanCredentials,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
        channel_id: ChannelId | None = None,
    ) -> dict[str, bool]:
        scope = await writable_scope(runtime, actor, workspace_id)
        return await test_qq(require_channels(), body, scope, channel_id)

    @router.post("/qq/pair/{login_id}/test", name="test_scanned_qq_connection")
    async def test_scanned_qq(
        login_id: LoginId,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
        channel_id: ChannelId | None = None,
    ) -> dict[str, bool]:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        login = active.qq_scans.find((workspace_id, actor.id), login_id)
        if login is None:
            raise not_found("qq_pairing", login_id)
        credentials = login.credentials
        if credentials is None:
            raise ServiceError("conflict", "Scan the QQ code and authorize the bot before testing.")
        return await test_qq(active, credentials, scope, channel_id)

    @router.post("/qq/pair", name="start_qq_pairing")
    async def begin_qq_scan(
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> QQScanStatus:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        if active.qq_connector is None:
            raise ServiceError("unavailable", "Configure the Tencent QQ Connector to enable QR binding.")
        key = (workspace_id, actor.id)
        scans = active.qq_scans
        previous = scans.sessions.get(key)
        login = QQScan(active.qq_connector)
        scans.sessions[key] = login
        try:
            if previous is not None:
                await previous.close()
            await asyncio.wait_for(login.ready.wait(), timeout=15)
        except TimeoutError:
            await scans.cancel(key, login)
            raise ServiceError("unavailable", "Tencent did not provide a QQ pairing QR code.") from None
        if scans.find(key, login.login_id) is None:
            return QQScanStatus(status="expired", login_id=login.login_id)
        return login.status()

    @router.get("/qq/pair/{login_id}", name="read_qq_pairing")
    async def read_qq_scan(
        login_id: LoginId,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> QQScanStatus:
        await writable_scope(runtime, actor, workspace_id)
        scans = require_channels().qq_scans
        login = scans.find((workspace_id, actor.id), login_id)
        if login is None:
            return QQScanStatus(status="expired", login_id=login_id)
        return login.status()

    @router.post("/qq/pair/{login_id}", name="save_qq_pairing", status_code=201)
    async def save_qq_scan(
        login_id: LoginId,
        body: QQScanSave,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> QQAccount:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        key = (workspace_id, actor.id)
        scans = active.qq_scans
        login = scans.find(key, login_id)
        if login is None:
            raise not_found("qq_pairing", login_id)
        async with login.lock:
            if scans.find(key, login_id) is not login:
                raise not_found("qq_pairing", login_id)
            credentials = login.credentials
            if credentials is None:
                raise ServiceError("conflict", "Scan the QQ code and authorize the bot before saving.")
            if body.channel_id is None:
                if body.name is None:
                    raise ServiceError("invalid_argument", "A name is required to add a QQ channel.")
                account = await create_qq(
                    active,
                    scope,
                    QQCreate(name=body.name, app_id=credentials.app_id, app_secret=credentials.app_secret),
                )
            else:
                if body.name is not None:
                    raise ServiceError("invalid_argument", "A channel name cannot be changed.")
                account = await update_qq(
                    active,
                    scope,
                    body.channel_id,
                    QQEdit(app_id=credentials.app_id, app_secret=credentials.app_secret),
                )
            await scans.cancel(key, login)
            return account

    @router.delete("/qq/pair/{login_id}", name="cancel_qq_pairing", status_code=204)
    async def cancel_qq_scan(
        login_id: LoginId,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> None:
        await writable_scope(runtime, actor, workspace_id)
        scans = require_channels().qq_scans
        key = (workspace_id, actor.id)
        if (login := scans.find(key, login_id)) is not None:
            await scans.cancel(key, login)

    async def update_qq(active: ChannelsRuntime, scope: WorkspaceScope, channel_id: str, body: QQEdit) -> QQAccount:
        rows = await active.store.query(
            "SELECT * FROM qq_accounts WHERE id = ? AND workspace_id = ?", channel_id, scope.workspace_id
        )
        if not rows:
            raise not_found("qq_channel", channel_id)
        current = rows[0]
        app_id = body.app_id or current["app_id"]
        if app_id in active.qq_probes:
            raise ServiceError("conflict", "Wait for the QQ connection test to finish before saving.")
        if body.app_secret is None:
            if app_id != current["app_id"]:
                raise ServiceError("invalid_argument", "An AppSecret is required when changing AppID.")
            return active.view(current)
        if not body.app_secret.get_secret_value().isascii():
            raise ServiceError("invalid_argument", "QQ AppSecret must be ASCII.")
        secret = active.store.encrypt(scope.organization_id, channel_id, QQSecret(app_secret=body.app_secret))
        try:
            updated = await active.store.query(
                """UPDATE qq_accounts SET app_id = ?, app_secret = ?
                   WHERE id = ? AND workspace_id = ? AND app_secret = ? RETURNING *""",
                app_id,
                secret,
                channel_id,
                scope.workspace_id,
                current["app_secret"],
            )
        except sqlite3.IntegrityError:
            raise ServiceError("conflict", "This QQ AppID is already registered.") from None
        if not updated:
            raise ServiceError("conflict", "The channel changed. Reopen it before editing.")
        active.changed.set()
        return active.view(updated[0])

    @router.patch("/qq/accounts/{channel_id}", name="update_qq_account")
    async def update(
        channel_id: ChannelId, body: QQEdit, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> QQAccount:
        scope = await writable_scope(runtime, actor, workspace_id)
        return await update_qq(require_channels(), scope, channel_id, body)

    @router.delete("/qq/accounts/{channel_id}", name="delete_qq_account", status_code=204)
    async def delete(channel_id: ChannelId, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId) -> None:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        rows = await active.store.query(
            "DELETE FROM qq_accounts WHERE id = ? AND workspace_id = ? RETURNING id",
            channel_id,
            workspace_id,
        )
        if not rows:
            raise not_found("qq_channel", channel_id)
        active.changed.set()

    @router.get("/qq/accounts/{channel_id}/messages", name="list_qq_messages")
    async def messages(channel_id: ChannelId, workspace_id: WorkspaceId) -> list[ChannelMessage]:
        active = require_channels()
        rows = await active.store.query(
            """SELECT payload FROM qq_accounts LEFT JOIN qq_messages
                 ON qq_accounts.id = qq_messages.channel_id AND qq_accounts.app_id = qq_messages.account_id
               WHERE qq_accounts.id = ? AND workspace_id = ? ORDER BY qq_messages.rowid DESC LIMIT 50""",
            channel_id,
            workspace_id,
        )
        if not rows:
            raise not_found("qq_channel", channel_id)
        return [ChannelMessage.model_validate_json(row["payload"]) for row in rows if row["payload"] is not None]

    @router.post("/qq/accounts/{channel_id}/reply", name="reply_to_qq_message")
    async def reply(
        channel_id: ChannelId,
        body: TextReply,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
        message_id: Annotated[str, Query(min_length=1, max_length=1024)],
    ) -> dict[str, str]:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        rows = await active.store.query(
            """SELECT *, 'qq' AS platform FROM qq_accounts WHERE id = ? AND workspace_id = ? AND organization_id = ?""",
            channel_id,
            workspace_id,
            scope.organization_id,
        )
        if not rows:
            raise not_found("qq_channel", channel_id)
        return {"message_id": await active.send_reply(rows[0], message_id, body.text)}

    async def weixin_account(active: ChannelsRuntime, channel_id: str, workspace_id: str) -> sqlite3.Row:
        rows = await active.store.query(
            "SELECT *, bot_id AS app_id, 'weixin' AS platform FROM weixin_accounts WHERE id = ? AND workspace_id = ?",
            channel_id,
            workspace_id,
        )
        if not rows:
            raise not_found("weixin_channel", channel_id)
        return rows[0]

    @router.post("/weixin/accounts", name="add_weixin_channel", status_code=201)
    async def add_weixin(
        body: WeixinCreate, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> ChannelAccount:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        channel_id = new_object_id("wx")
        rows = await active.store.query(
            """INSERT INTO weixin_accounts (id, organization_id, workspace_id, name) VALUES (?, ?, ?, ?)
               RETURNING id, name, bot_id AS account_id, 'weixin' AS platform""",
            channel_id,
            scope.organization_id,
            workspace_id,
            body.name,
        )
        return active.channel_view(rows[0])

    @router.post("/weixin/accounts/{channel_id}/pair", name="start_weixin_pairing")
    async def begin_weixin(
        channel_id: ChannelId, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> PairingResult:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        row = await weixin_account(active, channel_id, workspace_id)
        try:
            login, result = await start_pairing(active.http, row["credentials"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            raise ServiceError("unavailable", "Weixin could not start QR pairing.") from None
        active.pairings[channel_id] = login
        return result

    @router.post("/weixin/accounts/{channel_id}/pair/status", name="poll_weixin_pairing")
    async def check_weixin(
        channel_id: ChannelId,
        body: PairingPoll,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> PairingResult:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        await weixin_account(active, channel_id, workspace_id)
        login = active.pairings.get(channel_id)
        if login is None or login.login_id != body.login_id:
            return PairingResult(status="expired")
        async with login.lock:
            if active.pairings.get(channel_id) is not login or asyncio.get_running_loop().time() >= login.expires:
                return PairingResult(status="expired")
            if login.credentials is not None:
                return PairingResult(status="confirmed")
            try:
                result, credentials = await poll_pairing(active.http, login, body.verify_code)
            except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
                raise ServiceError("unavailable", "Weixin could not check QR pairing.") from None
            if active.pairings.get(channel_id) is not login or asyncio.get_running_loop().time() >= login.expires:
                return PairingResult(status="expired")
            if credentials is not None:
                login.credentials = credentials
            elif result.status in {"expired", "verify_code_blocked", "binded_redirect"}:
                active.pairings.pop(channel_id, None)
            return result

    @router.post("/weixin/accounts/{channel_id}/pair/save", name="save_weixin_pairing")
    async def save_weixin(
        channel_id: ChannelId,
        body: PairingPoll,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> PairingResult:
        scope = await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        await weixin_account(active, channel_id, workspace_id)
        login = active.pairings.get(channel_id)
        if login is None or login.login_id != body.login_id:
            return PairingResult(status="expired")
        async with login.lock:
            if active.pairings.get(channel_id) is not login or asyncio.get_running_loop().time() >= login.expires:
                return PairingResult(status="expired")
            credentials = login.credentials
            if credentials is None:
                raise ServiceError("conflict", "Scan and authorize the Weixin bot before saving.")
            payload = credentials.model_dump(exclude={"token"})
            payload["token"] = credentials.token.get_secret_value()
            sealed = active.store.seal(
                scope.organization_id,
                "channels.weixin_accounts",
                "credentials",
                channel_id,
                json.dumps(payload).encode(),
            )
            try:
                updated = await active.store.query(
                    """UPDATE weixin_accounts SET cursor = CASE WHEN bot_id = ? THEN cursor ELSE '' END,
                           bot_id = ?, credentials = ?
                       WHERE id = ? AND workspace_id = ? AND credentials IS ? RETURNING id""",
                    credentials.bot_id,
                    credentials.bot_id,
                    sealed,
                    channel_id,
                    workspace_id,
                    login.expected,
                )
            except sqlite3.IntegrityError:
                active.pairings.pop(channel_id, None)
                raise ServiceError("conflict", "This Weixin bot is already registered.") from None
            if active.pairings.get(channel_id) is login:
                active.pairings.pop(channel_id)
            if not updated:
                raise ServiceError("conflict", "The channel changed. Start pairing again.")
            active.changed.set()
            return PairingResult(status="confirmed")

    @router.delete("/weixin/accounts/{channel_id}/pair/{login_id}", name="cancel_weixin_pairing", status_code=204)
    async def cancel_weixin(
        channel_id: ChannelId,
        login_id: str,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> None:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        await weixin_account(active, channel_id, workspace_id)
        login = active.pairings.get(channel_id)
        if login is not None and login.login_id == login_id:
            active.pairings.pop(channel_id)

    @router.delete("/weixin/accounts/{channel_id}", name="delete_weixin_channel", status_code=204)
    async def delete_weixin(
        channel_id: ChannelId, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> None:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        rows = await active.store.query(
            "DELETE FROM weixin_accounts WHERE id = ? AND workspace_id = ? RETURNING id", channel_id, workspace_id
        )
        if not rows:
            raise not_found("weixin_channel", channel_id)
        active.pairings.pop(channel_id, None)
        active.changed.set()

    @router.get("/weixin/accounts/{channel_id}/messages", name="list_weixin_messages")
    async def weixin_messages(channel_id: ChannelId, workspace_id: WorkspaceId) -> list[ChannelMessage]:
        active = require_channels()
        account = await weixin_account(active, channel_id, workspace_id)
        rows = await active.store.query(
            "SELECT payload FROM weixin_messages WHERE channel_id = ? AND account_id = ? ORDER BY rowid DESC LIMIT 50",
            channel_id,
            account["bot_id"],
        )
        return [ChannelMessage.model_validate_json(row["payload"]) for row in rows]

    @router.post("/weixin/accounts/{channel_id}/reply", name="reply_to_weixin_message")
    async def reply_weixin(
        channel_id: ChannelId,
        body: TextReply,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
        message_id: Annotated[str, Query(min_length=1, max_length=1024)],
    ) -> dict[str, str]:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        account = await weixin_account(active, channel_id, workspace_id)
        return {"message_id": await active.send_reply(account, message_id, body.text)}

    @router.post("/{platform}/accounts", status_code=201)
    async def add_github(
        platform: GitHubPlatform, body: GitHubBind, runtime: CurrentRuntime, actor: Actor, workspace_id: WorkspaceId
    ) -> ChannelAccount:
        return await save_github(platform, body, runtime, actor, workspace_id)

    @router.patch("/{platform}/accounts/{channel_id}")
    async def edit_github(
        platform: GitHubPlatform,
        channel_id: ChannelId,
        body: GitHubBind,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> ChannelAccount:
        return await save_github(platform, body, runtime, actor, workspace_id, channel_id)

    @router.delete("/{platform}/accounts/{channel_id}", status_code=204)
    async def delete_github(
        platform: GitHubPlatform,
        channel_id: ChannelId,
        runtime: CurrentRuntime,
        actor: Actor,
        workspace_id: WorkspaceId,
    ) -> None:
        await writable_scope(runtime, actor, workspace_id)
        active = require_channels()
        rows = await active.store.query(
            "DELETE FROM github_accounts WHERE id = ? AND workspace_id = ? AND platform = ? RETURNING id",
            channel_id,
            workspace_id,
            platform,
        )
        if not rows:
            raise not_found("github_channel", channel_id)
        active.changed.set()

    return AppDefinition(
        app_id=APP_ID,
        name="Channels",
        factory_keys=frozenset({FACTORY_KEY, DELIVERY_FACTORY_KEY}),
        routers=(router,),
        settings={SETTINGS: ChannelsSettings},
        startup=startup,
        frontend=Frontend("a13n_channels", "static", entry="index.js", styles=("index.css",)),
    )
