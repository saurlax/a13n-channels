"""Workspace source rules and the durable queue for matched incoming messages."""

import json
import sqlite3
from hashlib import sha256
from itertools import combinations
from typing import Annotated, Literal

from a13n_service.infra.errors import ServiceError
from pydantic import BaseModel, Field, StringConstraints, model_validator

from a13n_channels.channels.github import GITHUB_KINDS, RepositoryName
from a13n_channels.messages import ChannelMessage

type Choice = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=512)]
type ThreadPolicy = Literal["conversation", "sender", "fixed", "message"]
type Kind = Literal["private", "group", "guild", "guild_dm", "channel", "thread", "issue", "pull_request", "topic", "*"]

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
        last_submitted_at REAL NOT NULL DEFAULT 0,
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
    CREATE TRIGGER IF NOT EXISTS github_channel_delivery_delete AFTER DELETE ON github_accounts BEGIN
        DELETE FROM channel_deliveries WHERE channel_id = OLD.id;
        DELETE FROM channel_threads WHERE channel_id = OLD.id;
    END;
    CREATE TRIGGER IF NOT EXISTS connector_channel_delivery_delete AFTER DELETE ON connector_accounts BEGIN
        DELETE FROM channel_deliveries WHERE channel_id = OLD.id;
        DELETE FROM channel_threads WHERE channel_id = OLD.id;
    END;
"""


MULTIPARTY_KINDS = {"group", "guild", "channel", "thread", "issue", "pull_request"}


class Source(BaseModel, frozen=True):
    channel_id: Choice
    kind: Kind
    conversation_id: Choice
    mentions_only: bool = False
    repository: RepositoryName | None = None

    @model_validator(mode="after")
    def validate_wildcard(self) -> "Source":
        if self.repository and (
            self.channel_id == "*" or self.kind not in {"*", *GITHUB_KINDS} or self.conversation_id != "*"
        ):
            raise ValueError("A repository requires a GitHub account and repository-level conversation matching.")
        if self.kind == "*" and self.conversation_id != "*":
            raise ValueError("An exact conversation requires a conversation type.")
        if self.mentions_only and self.kind not in MULTIPARTY_KINDS:
            raise ValueError("Mention filtering requires a group or channel conversation type.")
        return self

    @property
    def address(self) -> tuple[str, str, str, str | None]:
        return self.channel_id, self.kind, self.conversation_id, self.repository.lower() if self.repository else None

    @property
    def specificity(self) -> tuple[bool, bool, bool, bool]:
        return (self.conversation_id != "*", self.repository is not None, self.channel_id != "*", self.kind != "*")

    def matches(self, channel_id: str, kind: str, conversation_id: str) -> bool:
        return (
            self.channel_id in {"*", channel_id}
            and self.kind in {"*", kind}
            and self.conversation_id in {"*", conversation_id}
            and (
                self.repository is None
                or (kind in GITHUB_KINDS and conversation_id.rpartition("#")[0].lower() == self.repository.lower())
            )
        )


class RouteInput(BaseModel):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]
    agent_id: Choice
    enabled: bool = True
    sources: list[Source] = Field(min_length=1, max_length=100)
    thread_policy: ThreadPolicy = "conversation"
    context_idle_seconds: int = Field(default=3600, strict=True, ge=1, le=2**53 - 1)
    fixed_thread_id: Choice | None = None
    multiparty_messages: Literal["all", "mentions"] | None = None
    wake_agent_id: Choice | None = None
    allowed_users: dict[Choice, Annotated[list[Choice], Field(min_length=1, max_length=100)]] = Field(
        default_factory=dict, max_length=100
    )

    def matches_sender(self, source: Source, channel_id: str, kind: str, sender_id: str | None) -> bool:
        users = self.allowed_users.get(channel_id)
        if kind in GITHUB_KINDS:
            return bool(source.repository and users and (sender_id or "").lower() in {user.lower() for user in users})
        return users is None or sender_id in users

    @model_validator(mode="after")
    def unique_sources(self) -> "RouteInput":
        if self.fixed_thread_id is not None and self.thread_policy != "fixed":
            raise ValueError("A Thread ID requires the fixed Thread policy.")
        if self.wake_agent_id == self.agent_id:
            raise ValueError("Choose a different wake Agent from the target Agent.")
        return self


class Route(RouteInput):
    id: str
    binding_id: str
    principal_id: str | None


def github_channel_ids(db: sqlite3.Connection, workspace_id: str) -> set[str]:
    return {row[0] for row in db.execute("SELECT id FROM github_accounts WHERE workspace_id = ?", (workspace_id,))}


def read_routes(db: sqlite3.Connection, workspace_id: str) -> list[Route]:
    routes = []
    for row in db.execute("SELECT * FROM channel_routes WHERE workspace_id = ? ORDER BY name, id", (workspace_id,)):
        config = json.loads(row["sources"])
        routes.append(
            Route(
                id=row["id"],
                binding_id=row["binding_id"],
                principal_id=row["principal_id"],
                name=row["name"],
                agent_id=row["agent_id"],
                enabled=bool(row["enabled"]),
                **config,
            )
        )
    return routes


def select_route(
    routes: list[Route],
    channel_id: str,
    kind: str,
    conversation_id: str,
    *,
    bot_mentioned: bool | None = None,
    sender_id: str | None = None,
) -> Route | None:
    candidates = (
        (source, route)
        for route in routes
        for source in route.sources
        if source.matches(channel_id, kind, conversation_id)
        and route.matches_sender(source, channel_id, kind, sender_id)
    )
    match = max(candidates, key=lambda candidate: candidate[0].specificity, default=None)
    if match is None:
        return None
    source, route = match
    mentions_only = (
        source.mentions_only if route.multiparty_messages is None else route.multiparty_messages == "mentions"
    )
    return route if kind not in MULTIPARTY_KINDS or not mentions_only or bot_mentioned is True else None


def validate_route(route: RouteInput, routes: list[Route], route_id: str, github_channels: set[str]) -> None:
    def overlaps(left: Source, right: Source, other: RouteInput) -> bool:
        if left.address != right.address:
            return False
        channel = left.channel_id
        left_users, right_users = route.allowed_users.get(channel), other.allowed_users.get(channel)
        if channel in github_channels or left.kind in GITHUB_KINDS:
            return left.repository is None or bool(
                {user.lower() for user in left_users or []} & {user.lower() for user in right_users or []}
            )
        return left_users is None or right_users is None or bool(set(left_users).intersection(right_users))

    if any(overlaps(left, right, route) for left, right in combinations(route.sources, 2)):
        raise ServiceError("conflict", "Remove overlapping sources.")
    for other in routes:
        if other.id != route_id and any(
            overlaps(left, right, other) for left in route.sources for right in other.sources
        ):
            raise ServiceError("conflict", f'A source already belongs to the route "{other.name}".')


def enqueue(db: sqlite3.Connection, workspace_id: str, account_id: str, message: ChannelMessage) -> None:
    if not message.text.strip():
        return
    route = select_route(
        read_routes(db, workspace_id),
        message.channel_id,
        message.kind,
        message.conversation_id,
        bot_mentioned=message.bot_mentioned,
        sender_id=message.sender_id,
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
