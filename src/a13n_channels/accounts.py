"""App-owned accounts, encrypted credentials, and received channel messages."""

import asyncio
import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

from a13n_service.infra.crypto import Envelope, KeyRing, SecretLocation
from a13n_service.infra.errors import ServiceError, not_found
from a13n_service.infra.ids import new_object_id

from a13n_channels.channels.adapter import ChannelError
from a13n_channels.channels.github import GITHUB_KINDS, LOGIN_PATTERN, GitHubBinding
from a13n_channels.channels.qq import QQCreate, QQSecret
from a13n_channels.channels.qq_content import QQInbound
from a13n_channels.channels.weixin import WeixinCredentials
from a13n_channels.messages import ChannelMessage
from a13n_channels.routing import SCHEMA, Route, RouteInput, enqueue, github_channel_ids, read_routes, validate_route


class Accounts:
    def __init__(self, path: Path, keys: KeyRing):
        self.path = path
        self.keys = keys

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize)

    def _initialize(self) -> None:
        self.path.touch(mode=0o600, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS qq_accounts (
                    id TEXT PRIMARY KEY,
                    organization_id TEXT NOT NULL,
                    workspace_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    app_id TEXT NOT NULL UNIQUE,
                    app_secret TEXT NOT NULL,
                    intents INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS weixin_accounts (
                    id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
                    name TEXT NOT NULL, bot_id TEXT UNIQUE, credentials TEXT, cursor TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS github_accounts (
                    id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
                    name TEXT NOT NULL, platform TEXT NOT NULL, app_id TEXT NOT NULL,
                    credentials TEXT NOT NULL, cursor TEXT NOT NULL,
                    UNIQUE (workspace_id, platform, app_id)
                );
                CREATE TABLE IF NOT EXISTS github_messages (
                    channel_id TEXT NOT NULL REFERENCES github_accounts(id) ON DELETE CASCADE,
                    account_id TEXT NOT NULL, message_id TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY (channel_id, account_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS connector_accounts (
                    id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
                    name TEXT NOT NULL, platform TEXT NOT NULL, app_id TEXT NOT NULL,
                    credentials TEXT NOT NULL, cursor TEXT NOT NULL DEFAULT '', UNIQUE (platform, app_id)
                );
                CREATE TABLE IF NOT EXISTS connector_messages (
                    channel_id TEXT NOT NULL REFERENCES connector_accounts(id) ON DELETE CASCADE,
                    account_id TEXT NOT NULL, message_id TEXT NOT NULL, payload TEXT NOT NULL, context TEXT,
                    PRIMARY KEY (channel_id, account_id, message_id)
                );
            """)
            db.executescript(SCHEMA)
            with db:
                db.execute("BEGIN IMMEDIATE")
                if not any(row[1] == "principal_id" for row in db.execute("PRAGMA table_info(channel_routes)")):
                    db.execute("ALTER TABLE channel_routes ADD COLUMN principal_id TEXT")
                if not any(row[1] == "destination" for row in db.execute("PRAGMA table_info(channel_deliveries)")):
                    db.execute("ALTER TABLE channel_deliveries ADD COLUMN destination TEXT NOT NULL DEFAULT ''")
                    db.execute("""UPDATE channel_deliveries SET destination =
                        json_array('conversation', channel_id, account_id, kind, conversation_id)""")
                if not any(row[1] == "tool_key" for row in db.execute("PRAGMA table_info(channel_deliveries)")):
                    db.execute("ALTER TABLE channel_deliveries ADD COLUMN tool_key TEXT")
                    db.execute("ALTER TABLE channel_deliveries ADD COLUMN tool_request TEXT")
                db.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS channel_delivery_tool_key ON channel_deliveries(tool_key)"
                )
                db.execute("""CREATE INDEX IF NOT EXISTS channel_delivery_destination
                    ON channel_deliveries (destination, status)""")
                self._message_store(db, "qq", "app_id", "reply_sequence INTEGER NOT NULL DEFAULT 0", "reply_sequence")
                self._message_store(db, "weixin", "bot_id", "context TEXT", "context")
                db.execute("""CREATE VIEW IF NOT EXISTS channel_accounts AS
                    SELECT id, organization_id, workspace_id, name, app_id, 'qq' AS platform,
                           app_secret AS generation, app_secret, intents, '' AS cursor, NULL AS credentials
                    FROM qq_accounts
                    UNION ALL
                    SELECT id, organization_id, workspace_id, name, bot_id, 'weixin',
                           credentials, NULL, NULL, cursor, credentials FROM weixin_accounts
                    UNION ALL
                    SELECT id, organization_id, workspace_id, name, app_id, platform,
                           credentials, NULL, NULL, cursor, credentials FROM github_accounts
                    UNION ALL
                    SELECT id, organization_id, workspace_id, name, app_id, platform,
                           credentials, NULL, NULL, cursor, credentials FROM connector_accounts""")

    @staticmethod
    def _message_store(db: sqlite3.Connection, platform: str, identity: str, column: str, value: str) -> None:
        table = f"{platform}_messages"
        accounts = f"{platform}_accounts"
        fields = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        if "account_id" in fields:
            return
        if fields:
            db.execute(f"ALTER TABLE {table} RENAME TO {table}_previous")
        db.execute(f"""
            CREATE TABLE {table} (
                channel_id TEXT NOT NULL REFERENCES {accounts}(id) ON DELETE CASCADE,
                account_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                {column},
                PRIMARY KEY (channel_id, account_id, message_id)
            )
        """)
        if fields:
            db.execute(f"""
                INSERT INTO {table}
                SELECT previous.channel_id, account.{identity}, previous.message_id, previous.payload, previous.{value}
                FROM {table}_previous AS previous JOIN {accounts} AS account ON account.id = previous.channel_id
                ORDER BY previous.rowid
            """)
            db.execute(f"DROP TABLE {table}_previous")

    async def query(self, sql: str, *args: object) -> list[sqlite3.Row]:
        return await asyncio.to_thread(self._query, sql, args)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self.path)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys = ON")
            yield db

    def _query(self, sql: str, args: tuple[object, ...]) -> list[sqlite3.Row]:
        with self.transaction() as db:
            return db.execute(sql, args).fetchall()

    async def routes(self, workspace_id: str) -> list[Route]:
        def load() -> list[Route]:
            with self.transaction() as db:
                return read_routes(db, workspace_id)

        return await asyncio.to_thread(load)

    async def save_route(
        self, workspace_id: str, route_id: str, body: RouteInput, *, principal_id: str, create: bool
    ) -> Route:
        return await asyncio.to_thread(self._save_route, workspace_id, route_id, body, principal_id, create)

    def _save_route(self, workspace_id: str, route_id: str, body: RouteInput, principal_id: str, create: bool) -> Route:
        with self.transaction() as db:
            db.execute("BEGIN IMMEDIATE")
            routes = read_routes(db, workspace_id)
            previous = next((route for route in routes if route.id == route_id), None)
            if not create and previous is None:
                raise not_found("channel_route", route_id)
            github_channels = github_channel_ids(db, workspace_id)
            if body.enabled and any(
                (source.channel_id in github_channels or source.kind in GITHUB_KINDS)
                and (source.repository is None or not body.allowed_users.get(source.channel_id))
                for source in body.sources
            ):
                raise ServiceError(
                    "invalid_argument",
                    "Set the GitHub repositories in Sources and allowed users in Filters.",
                    {"field": "repository"},
                )
            if any(
                not re.fullmatch(LOGIN_PATTERN, user)
                for channel_id in github_channels
                for user in body.allowed_users.get(channel_id, [])
            ):
                raise ServiceError("invalid_argument", "Enter valid GitHub usernames.", {"field": "allowed_users"})
            validate_route(body, routes, route_id, github_channels)
            for channel_id in {source.channel_id for source in body.sources} | body.allowed_users.keys():
                if channel_id == "*":
                    if channel_id in body.allowed_users:
                        raise ServiceError("invalid_argument", "Choose an account for each allowed user.")
                    continue
                if not db.execute(
                    "SELECT id FROM channel_accounts WHERE id = ? AND workspace_id = ?",
                    (channel_id, workspace_id),
                ).fetchone():
                    raise ServiceError("invalid_argument", "Choose a channel in this workspace.")
            if any(source.repository and source.channel_id not in github_channels for source in body.sources):
                raise ServiceError("invalid_argument", "Choose a GitHub channel for a repository source.")
            destination_changed = previous is not None and (
                previous.thread_policy != body.thread_policy or previous.fixed_thread_id != body.fixed_thread_id
            )
            if (
                destination_changed
                and db.execute(
                    """SELECT 1 FROM channel_deliveries WHERE route_id = ?
                   AND status IN ('wake_queued', 'wake_submitted', 'queued', 'submitted', 'sending') LIMIT 1""",
                    (route_id,),
                ).fetchone()
            ):
                raise ServiceError(
                    "conflict",
                    "Wait for route deliveries to finish before changing the Thread destination.",
                    {"reason": "thread_destination_busy"},
                )
            binding_id = (
                previous.binding_id
                if previous
                and not destination_changed
                and previous.agent_id == body.agent_id
                and previous.principal_id == principal_id
                else new_object_id("chbind")
            )
            route = Route(**body.model_dump(), id=route_id, binding_id=binding_id, principal_id=principal_id)
            db.execute(
                """INSERT INTO channel_routes VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET name=excluded.name, agent_id=excluded.agent_id,
                   enabled=excluded.enabled, sources=excluded.sources, binding_id=excluded.binding_id,
                   principal_id=excluded.principal_id""",
                (
                    route_id,
                    workspace_id,
                    route.name,
                    route.agent_id,
                    route.enabled,
                    route.model_dump_json(
                        include={
                            "sources",
                            "thread_policy",
                            "context_idle_seconds",
                            "fixed_thread_id",
                            "multiparty_messages",
                            "wake_agent_id",
                            "allowed_users",
                        }
                    ),
                    binding_id,
                    principal_id,
                ),
            )
            if previous and (not route.enabled or binding_id != previous.binding_id):
                self._stop_deliveries(db, route_id)
                if binding_id != previous.binding_id:
                    db.execute("DELETE FROM channel_threads WHERE binding_id = ?", (previous.binding_id,))
            elif previous and previous.wake_agent_id != route.wake_agent_id:
                db.execute(
                    """UPDATE channel_deliveries SET status = 'stopped'
                       WHERE route_id = ? AND status IN ('wake_queued', 'wake_submitted')""",
                    (route_id,),
                )
            return route

    async def delete_route(self, workspace_id: str, route_id: str) -> None:
        def delete() -> None:
            with self.transaction() as db:
                row = db.execute(
                    "DELETE FROM channel_routes WHERE workspace_id = ? AND id = ? RETURNING binding_id",
                    (workspace_id, route_id),
                ).fetchone()
                if row is None:
                    raise not_found("channel_route", route_id)
                self._stop_deliveries(db, route_id)
                db.execute("DELETE FROM channel_threads WHERE binding_id = ?", (row["binding_id"],))

        await asyncio.to_thread(delete)

    @staticmethod
    def _stop_deliveries(db: sqlite3.Connection, route_id: str) -> None:
        db.execute(
            """UPDATE channel_deliveries SET status = 'stopped'
               WHERE route_id = ? AND status IN ('wake_queued', 'wake_submitted', 'queued', 'submitted')""",
            (route_id,),
        )

    @staticmethod
    def location(organization_id: str, channel_id: str) -> SecretLocation:
        return SecretLocation(organization_id, "channels.qq_accounts", "app_secret", channel_id)

    def encrypt(self, organization_id: str, channel_id: str, secret: QQSecret) -> str:
        return self.keys.protect(
            secret.app_secret.get_secret_value().encode(), self.location(organization_id, channel_id)
        ).model_dump_json()

    def credentials(self, account: sqlite3.Row) -> QQCreate:
        secret = self.keys.reveal(
            Envelope.model_validate_json(account["app_secret"]),
            self.location(account["organization_id"], account["id"]),
        )
        return QQCreate(
            name=account["name"], app_id=account["app_id"], app_secret=secret.decode(), qq_intents=account["intents"]
        )

    async def receive(self, incoming: ChannelMessage, generation: str) -> bool:
        return await asyncio.to_thread(self._receive_qq, incoming, generation)

    def _receive_qq(self, incoming: ChannelMessage, generation: str) -> bool:
        with self.transaction() as db:
            account = db.execute(
                "SELECT app_id, workspace_id, organization_id FROM qq_accounts WHERE id = ? AND app_secret = ?",
                (incoming.channel_id, generation),
            ).fetchone()
            if account is None:
                return False
            payload = incoming.model_dump(mode="json", exclude_defaults=True)
            if isinstance(incoming, QQInbound) and (incoming.attachment_urls or incoming.event_reply_id):
                payload["qq_context"] = self.seal(
                    account["organization_id"],
                    "channels.qq_messages",
                    "context",
                    self.qq_message_key(incoming.channel_id, account["app_id"], incoming.id),
                    json.dumps({"urls": incoming.attachment_urls, "event_reply_id": incoming.event_reply_id}).encode(),
                )
            saved = db.execute(
                """INSERT OR IGNORE INTO qq_messages (channel_id, account_id, message_id, payload)
                   VALUES (?, ?, ?, ?)""",
                (incoming.channel_id, account["app_id"], incoming.id, json.dumps(payload, ensure_ascii=False)),
            )
            if saved.rowcount:
                enqueue(db, account["workspace_id"], account["app_id"], incoming)
            return bool(saved.rowcount)

    @staticmethod
    def qq_message_key(channel_id: str, account_id: str, message_id: str) -> str:
        return json.dumps([channel_id, account_id, message_id], separators=(",", ":"))

    def qq_message_context(self, account: sqlite3.Row, message_id: str, context: str) -> dict:
        return json.loads(
            self.unseal(
                account["organization_id"],
                "channels.qq_messages",
                "context",
                self.qq_message_key(account["id"], account["app_id"], message_id),
                context,
            )
        )

    def seal(self, organization_id: str, table: str, column: str, row_id: str, value: bytes) -> str:
        return self.keys.protect(value, SecretLocation(organization_id, table, column, row_id)).model_dump_json()

    def unseal(self, organization_id: str, table: str, column: str, row_id: str, value: str) -> bytes:
        return self.keys.reveal(
            Envelope.model_validate_json(value), SecretLocation(organization_id, table, column, row_id)
        )

    def weixin_credentials(self, row: sqlite3.Row) -> WeixinCredentials:
        value = self.unseal(
            row["organization_id"], "channels.weixin_accounts", "credentials", row["id"], row["credentials"]
        )
        return WeixinCredentials.model_validate_json(value)

    async def receive_weixin(
        self, account: sqlite3.Row, cursor: str, messages: list[tuple[ChannelMessage, str]]
    ) -> None:
        await asyncio.to_thread(self._receive_weixin, account, cursor, messages)

    def _receive_weixin(self, account: sqlite3.Row, cursor: str, messages: list[tuple[ChannelMessage, str]]) -> None:
        channel_id = account["id"]
        with self.transaction() as db:
            changed = db.execute(
                "UPDATE weixin_accounts SET cursor = ? WHERE id = ? AND credentials = ?",
                (cursor, channel_id, account["credentials"]),
            ).rowcount
            if not changed:
                return
            for message, context in messages:
                stored = (
                    self.seal(
                        account["organization_id"],
                        "channels.weixin_messages",
                        "context",
                        f"{channel_id}:{message.id}",
                        context.encode(),
                    )
                    if context
                    else None
                )
                inserted = db.execute(
                    "INSERT OR IGNORE INTO weixin_messages VALUES (?, ?, ?, ?, ?)",
                    (channel_id, account["app_id"], message.id, message.model_dump_json(), stored),
                ).rowcount
                if inserted:
                    enqueue(db, account["workspace_id"], account["app_id"], message)

    def github_credentials(self, row: sqlite3.Row) -> GitHubBinding:
        data = json.loads(
            self.unseal(
                row["organization_id"], "channels.github_accounts", "credentials", row["id"], row["credentials"]
            )
        )
        data.pop("repositories", None)
        data.pop("allowed_users", None)
        return GitHubBinding.model_validate(data)

    async def github_repositories(self, account: sqlite3.Row) -> set[str]:
        return {
            source.repository.lower()
            for route in await self.routes(account["workspace_id"])
            if route.enabled and route.principal_id and route.allowed_users.get(account["id"])
            for source in route.sources
            if source.channel_id == account["id"] and source.repository
        }

    async def receive_github(self, account: sqlite3.Row, message: ChannelMessage | None, cursor: str | None) -> None:
        await asyncio.to_thread(self._receive_github, account, message, cursor)

    def _receive_github(self, account: sqlite3.Row, message: ChannelMessage | None, cursor: str | None) -> None:
        with self.transaction() as db:
            if message is None:
                db.execute(
                    "UPDATE github_accounts SET cursor = ? WHERE id = ? AND credentials = ?",
                    (cursor, account["id"], account["credentials"]),
                )
                return
            inserted = db.execute(
                """INSERT OR IGNORE INTO github_messages
                   SELECT id, app_id, ?, ? FROM github_accounts WHERE id = ? AND credentials = ?""",
                (message.id, message.model_dump_json(), account["id"], account["credentials"]),
            ).rowcount
            if inserted:
                enqueue(db, account["workspace_id"], account["app_id"], message)

    def connector_credentials(self, account: sqlite3.Row) -> dict[str, str]:
        return json.loads(
            self.unseal(
                account["organization_id"],
                "channels.connector_accounts",
                "credentials",
                account["id"],
                account["credentials"],
            )
        )

    async def receive_connector(
        self, account: sqlite3.Row, message: ChannelMessage | None, cursor: str | None, context: dict[str, str] | None
    ) -> None:
        await asyncio.to_thread(self._receive_connector, account, message, cursor, context)

    def _receive_connector(
        self, account: sqlite3.Row, message: ChannelMessage | None, cursor: str | None, context: dict[str, str] | None
    ) -> None:
        channel_id, account_id = account["id"], account["app_id"]
        with self.transaction() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute(
                "SELECT 1 FROM connector_accounts WHERE id = ? AND credentials = ?",
                (channel_id, account["credentials"]),
            ).fetchone():
                raise ChannelError("The channel account changed.", retryable=True)
            if message is not None:
                if message.channel_id != channel_id:
                    raise ValueError("A connector must emit messages for its own account.")
                private = None
                if context:
                    private = self.seal(
                        account["organization_id"],
                        "channels.connector_messages",
                        "context",
                        f"{channel_id}:{message.id}",
                        json.dumps(context).encode(),
                    )
                inserted = db.execute(
                    "INSERT OR IGNORE INTO connector_messages VALUES (?, ?, ?, ?, ?)",
                    (channel_id, account_id, message.id, message.model_dump_json(), private),
                ).rowcount
                if inserted:
                    enqueue(db, account["workspace_id"], account_id, message)
            if cursor is not None:
                db.execute("UPDATE connector_accounts SET cursor = ? WHERE id = ?", (cursor, channel_id))
