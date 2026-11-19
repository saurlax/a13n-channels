"""Workspace source rules and the durable queue for matched incoming messages."""

import json
import sqlite3
from hashlib import sha256
from typing import Annotated, Literal

from a13n_service.infra.errors import ServiceError
from pydantic import BaseModel, Field, StringConstraints, model_validator

from a13n_channels.messages import ChannelMessage

type Choice = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=512)]
type ThreadPolicy = Literal["conversation", "sender", "fixed", "message"]
type Kind = Literal["private", "group", "guild", "guild_dm", "channel", "thread", "*"]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS channel_routes (
        id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL,
        agent_id TEXT NOT NULL, enabled INTEGER NOT NULL, sources TEXT NOT NULL,
        binding_id TEXT NOT NULL, principal_id TEXT
    );
    CREATE INDEX IF NOT EXISTS channel_routes_workspace ON channel_routes (workspace_id);
    CREATE TABLE IF NOT EXISTS channel_threads (
        binding_id TEXT NOT NULL, channel_id TEXT NOT NULL, account_id TEXT NOT NULL,
        kind TEXT NOT NULL, conversation_id TEXT NOT NULL, thread_id TEXT NOT NULL,
        PRIMARY KEY (binding_id, channel_id, account_id, kind, conversation_id)
    );
    CREATE TABLE IF NOT EXISTS channel_deliveries (
        id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, route_id TEXT NOT NULL,
        binding_id TEXT NOT NULL, agent_id TEXT NOT NULL, channel_id TEXT NOT NULL,
        account_id TEXT NOT NULL, kind TEXT NOT NULL, conversation_id TEXT NOT NULL,
        message_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
        thread_id TEXT, entry_id TEXT, run_id TEXT, next_at REAL NOT NULL DEFAULT 0,
        error TEXT, destination TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS channel_sends (
        delivery_id TEXT NOT NULL REFERENCES channel_deliveries(id) ON DELETE CASCADE,
        call_id TEXT NOT NULL, text TEXT NOT NULL, status TEXT NOT NULL, message_id TEXT,
        PRIMARY KEY (delivery_id, call_id)
    );
    CREATE INDEX IF NOT EXISTS channel_delivery_due ON channel_deliveries (status, next_at);
    CREATE INDEX IF NOT EXISTS channel_delivery_source
        ON channel_deliveries (channel_id, account_id, kind, conversation_id, status);
    CREATE INDEX IF NOT EXISTS channel_delivery_route ON channel_deliveries (route_id);
    CREATE TRIGGER IF NOT EXISTS qq_channel_delivery_delete AFTER DELETE ON qq_accounts BEGIN
        DELETE FROM channel_deliveries WHERE channel_id = OLD.id;
        DELETE FROM channel_threads WHERE channel_id = OLD.id;
    END;
    CREATE TRIGGER IF NOT EXISTS weixin_channel_delivery_delete AFTER DELETE ON weixin_accounts BEGIN
        DELETE FROM channel_deliveries WHERE channel_id = OLD.id;
        DELETE FROM channel_threads WHERE channel_id = OLD.id;
    END;
"""


MULTIPARTY_KINDS = {"group", "guild", "channel", "thread"}


class Source(BaseModel, frozen=True):
    channel_id: Choice
    kind: Kind
    conversation_id: Choice
    mentions_only: bool = False

    @model_validator(mode="after")
    def validate_wildcard(self) -> "Source":
        if self.kind == "*" and self.conversation_id != "*":
            raise ValueError("An exact conversation requires a conversation type.")
        if self.mentions_only and self.kind not in MULTIPARTY_KINDS:
            raise ValueError("Mention filtering requires a group or channel conversation type.")
        return self

    @property
    def address(self) -> tuple[str, str, str]:
        return self.channel_id, self.kind, self.conversation_id

    @property
    def specificity(self) -> tuple[bool, bool, bool]:
        return (self.conversation_id != "*", self.channel_id != "*", self.kind != "*")

    def matches(self, channel_id: str, kind: str, conversation_id: str) -> bool:
        return (
            self.channel_id in {"*", channel_id}
            and self.kind in {"*", kind}
            and self.conversation_id in {"*", conversation_id}
        )


class RouteInput(BaseModel):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]
    agent_id: Choice
    enabled: bool = True
    sources: list[Source] = Field(min_length=1, max_length=100)
    thread_policy: ThreadPolicy = "conversation"
    fixed_thread_id: Choice | None = None
    multiparty_messages: Literal["all", "mentions"] | None = None
    wake_agent_id: Choice | None = None

    @model_validator(mode="after")
    def unique_sources(self) -> "RouteInput":
        if len({source.address for source in self.sources}) != len(self.sources):
            raise ValueError("Remove duplicate sources.")
        if self.fixed_thread_id is not None and self.thread_policy != "fixed":
            raise ValueError("A Thread ID requires the fixed Thread policy.")
        if self.wake_agent_id == self.agent_id:
            raise ValueError("Choose a different wake Agent from the target Agent.")
        return self


class Route(RouteInput):
    id: str
    binding_id: str
    principal_id: str | None


def read_routes(db: sqlite3.Connection, workspace_id: str) -> list[Route]:
    return [
        Route(
            id=row["id"],
            binding_id=row["binding_id"],
            principal_id=row["principal_id"],
            name=row["name"],
            agent_id=row["agent_id"],
            enabled=bool(row["enabled"]),
            **json.loads(row["sources"]),
        )
        for row in db.execute("SELECT * FROM channel_routes WHERE workspace_id = ? ORDER BY name, id", (workspace_id,))
    ]


def select_route(
    routes: list[Route], channel_id: str, kind: str, conversation_id: str, *, bot_mentioned: bool | None = None
) -> Route | None:
    candidates = (
        (source, route)
        for route in routes
        for source in route.sources
        if source.matches(channel_id, kind, conversation_id)
    )
    match = max(candidates, key=lambda candidate: candidate[0].specificity, default=None)
    if match is None:
        return None
    source, route = match
    mentions_only = (
        source.mentions_only if route.multiparty_messages is None else route.multiparty_messages == "mentions"
    )
    return route if kind not in MULTIPARTY_KINDS or not mentions_only or bot_mentioned is True else None


def validate_route(route: RouteInput, routes: list[Route], route_id: str) -> None:
    # For equal priority, the wildcard positions already match. Any overlap requires equal exact fields.
    sources = {source.address for source in route.sources}
    conflict = next(
        (
            other
            for other in routes
            if other.id != route_id and not sources.isdisjoint(source.address for source in other.sources)
        ),
        None,
    )
    if conflict:
        raise ServiceError("conflict", f'A source already belongs to the route "{conflict.name}".')


def enqueue(db: sqlite3.Connection, workspace_id: str, account_id: str, message: ChannelMessage) -> None:
    if not message.text.strip():
        return
    route = select_route(
        read_routes(db, workspace_id),
        message.channel_id,
        message.kind,
        message.conversation_id,
        bot_mentioned=message.bot_mentioned,
    )
    if route is None or not route.enabled or not route.principal_id:
        return
    key = sha256(json.dumps([workspace_id, message.channel_id, account_id, message.id]).encode()).hexdigest()
    match route.thread_policy:
        case "conversation":
            scope = ("conversation", message.channel_id, account_id, message.kind, message.conversation_id)
        case "sender":
            scope = ("sender", message.channel_id, account_id, message.kind, message.conversation_id, message.sender_id)
        case "fixed":
            scope = ("thread", route.fixed_thread_id) if route.fixed_thread_id else ("route", route.binding_id)
        case "message":
            scope = ("message", key)
    db.execute(
        """INSERT INTO channel_deliveries
           (id, workspace_id, route_id, binding_id, agent_id, channel_id, account_id, kind, conversation_id,
            message_id, destination, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            key,
            workspace_id,
            route.id,
            route.binding_id,
            route.agent_id,
            message.channel_id,
            account_id,
            message.kind,
            message.conversation_id,
            message.id,
            json.dumps(scope, ensure_ascii=False, separators=(",", ":")),
            "wake_queued" if route.wake_agent_id else "queued",
        ),
    )
