"""Validate and persist accounts used by outbound connectors."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import httpx
from a13n_service.infra.errors import ServiceError, not_found
from a13n_service.infra.ids import new_object_id
from a13n_service.tenancy.authorize import WorkspaceScope
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from a13n_channels.channels.adapter import Adapter, ChannelError, CredentialField

if TYPE_CHECKING:
    from a13n_channels.app import ChannelsRuntime


class ConnectorInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=80)
    config: dict[str, SecretStr] = Field(default_factory=dict, max_length=16)


class ConnectorDescription(BaseModel):
    key: str
    label: str
    label_zh: str
    fields: tuple[CredentialField, ...]


async def save_account(
    runtime: ChannelsRuntime, scope: WorkspaceScope, adapter: Adapter, body: ConnectorInput, channel_id: str | None
) -> sqlite3.Row:
    previous = None
    config: dict[str, str] = {}
    if channel_id is not None:
        rows = await runtime.store.query(
            "SELECT * FROM connector_accounts WHERE id = ? AND workspace_id = ? AND platform = ?",
            channel_id,
            scope.workspace_id,
            adapter.key,
        )
        if not rows:
            raise not_found("channel", channel_id)
        previous = rows[0]
        config = runtime.store.connector_credentials(previous)
    else:
        channel_id = new_object_id("ch")
        if not body.name or not body.name.strip():
            raise ServiceError("invalid_argument", "Enter a channel name.")

    fields = {field.key: field for field in adapter.fields}
    if body.config.keys() - fields.keys():
        raise ServiceError("invalid_argument", "The connector does not accept this configuration field.")
    for key, value in body.config.items():
        text = value.get_secret_value().strip()
        if len(text) > 4096:
            raise ServiceError("invalid_argument", f"{fields[key].label} is too long.")
        if text:
            config[key] = text
        else:
            config.pop(key, None)
    for field in adapter.fields:
        if field.required and not config.get(field.key):
            raise ServiceError("invalid_argument", f"Enter {field.label}.")

    try:
        identity = await adapter.bind(runtime.http, config)
    except ChannelError as error:
        raise ServiceError("unavailable" if error.retryable else "invalid_argument", str(error)) from None
    except (httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError):
        raise ServiceError("unavailable", "The connector could not verify the account.") from None
    if previous and identity != previous["app_id"]:
        raise ServiceError("invalid_argument", "Use a new channel for a different upstream identity.")

    encrypted = runtime.store.seal(
        scope.organization_id, "channels.connector_accounts", "credentials", channel_id, json.dumps(config).encode()
    )
    try:
        if previous:
            rows = await runtime.store.query(
                """UPDATE connector_accounts SET credentials = ?
                   WHERE id = ? AND workspace_id = ? AND credentials = ?
                   RETURNING id, name, app_id AS account_id, platform""",
                encrypted,
                channel_id,
                scope.workspace_id,
                previous["credentials"],
            )
            if not rows:
                raise ServiceError("conflict", "The account changed. Reopen the editor and retry.")
        else:
            rows = await runtime.store.query(
                """INSERT INTO connector_accounts(id, organization_id, workspace_id, name, platform, app_id, credentials)
                   VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id, name, app_id AS account_id, platform""",
                channel_id,
                scope.organization_id,
                scope.workspace_id,
                body.name.strip(),
                adapter.key,
                identity,
                encrypted,
            )
    except sqlite3.IntegrityError:
        raise ServiceError("conflict", "This upstream identity already has a channel in this deployment.") from None
    runtime.changed.set()
    return rows[0]
