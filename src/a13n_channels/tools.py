"""Workspace channel tools and conversation-scoped delivery tools."""

import asyncio
import json
from contextlib import aclosing
from hashlib import sha256
from typing import TYPE_CHECKING, Annotated

import httpx
from a13n_harness import AbstractHarnessPlugin
from a13n_harness.context import AgentContext
from a13n_harness.environment.models import EnvironmentError
from a13n_harness.plugin_factories import HarnessPluginFactory, HarnessPluginFactoryContext
from a13n_service.infra.db import short_session
from a13n_service.infra.errors import ServiceError
from a13n_service.runs.tables import InboxEntryRow, RunRow
from a13n_service.tenancy.access import principal_for
from a13n_service.tenancy.app_plugins import policy
from a13n_service.tenancy.authorize import ExecutionAuthority, WorkspaceScope, authorize
from a13n_service.tenancy.tables import OrganizationRow, WorkspaceRow
from pydantic import BaseModel, Field, TypeAdapter, field_validator
from pydantic_ai import ModelRetry, RunContext, Tool
from pydantic_ai.capabilities import Capability
from pydantic_ai.messages import BinaryContent, ToolReturn
from sqlalchemy import select

from a13n_channels.channels.qq import ChannelId, QQConnection, QQConversationKind, QQRejected
from a13n_channels.channels.qq_content import (
    MEDIA_LIMITS,
    QQMedia,
    QQRichContent,
    image_type,
    open_attachment,
    validate_rich,
)
from a13n_channels.messages import ChannelMessage, ReplyText, message_table
from a13n_channels.routing import select_route

if TYPE_CHECKING:
    from a13n_channels.app import ChannelsRuntime

FACTORY_KEY = "saurlax.channels"
DELIVERY_FACTORY_KEY = "saurlax.channels.delivery"
type MessageId = Annotated[str, Field(min_length=1, max_length=1024)]
type AttachmentIndex = Annotated[int, Field(ge=1, le=16)]
type EnvironmentPath = Annotated[str, Field(min_length=1, max_length=1024)]


class ChannelToolSettings(BaseModel, extra="forbid"):
    allowed_channels: str = Field(
        default="",
        title="Allowed channels",
        max_length=8192,
        description="Comma-separated channel account IDs. Leave blank to allow all channels in this workspace.",
    )

    @field_validator("allowed_channels")
    @classmethod
    def channel_ids(cls, value: str) -> str:
        ids = dict.fromkeys(item.strip() for item in value.split(",") if item.strip())
        adapter = TypeAdapter(ChannelId)
        return ", ".join(adapter.validate_python(item) for item in ids)


class ChannelToolFactory(HarnessPluginFactory):
    configuration_model = ChannelToolSettings
    display_name = "Channels"
    description = "Read channel history and attachments, and send proactive QQ messages."

    def __init__(self) -> None:
        self.channels: ChannelsRuntime | None = None

    @classmethod
    def plugin_key(cls) -> str:
        return FACTORY_KEY

    def create_plugin(self, context: HarnessPluginFactoryContext) -> AbstractHarnessPlugin:
        settings = ChannelToolSettings.model_validate(context.configuration)
        return WorkspaceChannelTools(context.plugin_id, self, frozenset(settings.allowed_channels.split(", ")) - {""})


class DeliveryToolSettings(BaseModel, extra="forbid"):
    pass


class DeliveryToolFactory(ChannelToolFactory):
    configuration_model = DeliveryToolSettings
    display_name = "Channels Delivery"
    description = "Read and reply in the conversation that triggered a channel run."

    @classmethod
    def plugin_key(cls) -> str:
        return DELIVERY_FACTORY_KEY

    def create_plugin(self, context: HarnessPluginFactoryContext) -> AbstractHarnessPlugin:
        return ChannelTools(context.plugin_id, self)


async def active_run(session, ctx: RunContext[AgentContext]):
    instance = ctx.deps.instance
    refs = instance.host_refs
    run_id = refs.get("run_id")
    if not run_id or instance.identity.issuer != "a13n-service" or instance.agent_instance_id != run_id:
        raise ModelRetry("Channel tools require an active Service run.")
    run = await session.get(RunRow, run_id)
    if (
        run is None
        or run.status != "running"
        or run.current_attempt_id != refs.get("attempt_id")
        or run.principal_id != instance.identity.subject
    ):
        raise ModelRetry("This channel run is no longer active.")
    return run


async def read_messages(store, account, kind: str, conversation_id: str, limit: int, before_id: str | None = None):
    table = message_table(account["platform"])
    rows = await store.query(
        f"""SELECT payload FROM {table} WHERE channel_id = ? AND account_id = ?
            AND json_extract(payload, '$.kind') = ? AND json_extract(payload, '$.conversation_id') = ?
            AND (? IS NULL OR rowid < (
                SELECT rowid FROM {table} WHERE channel_id = ? AND account_id = ? AND message_id = ?
                AND json_extract(payload, '$.kind') = ? AND json_extract(payload, '$.conversation_id') = ?
            ))
            ORDER BY rowid DESC LIMIT ?""",
        account["id"],
        account["app_id"],
        kind,
        conversation_id,
        before_id,
        account["id"],
        account["app_id"],
        before_id,
        kind,
        conversation_id,
        limit,
    )
    return [
        message.model_copy(update={"text": message.text[:4000]}).model_dump()
        for message in (ChannelMessage.model_validate_json(row["payload"]) for row in rows)
    ]


async def history_message(store, account, kind: str, conversation_id: str, message_id: str) -> dict:
    rows = await store.query(
        f"""SELECT payload FROM {message_table(account["platform"])}
           WHERE channel_id = ? AND account_id = ? AND message_id = ?
             AND json_extract(payload, '$.kind') = ? AND json_extract(payload, '$.conversation_id') = ?""",
        account["id"],
        account["app_id"],
        message_id,
        kind,
        conversation_id,
    )
    if not rows:
        raise ModelRetry("Choose a message from the selected conversation's history.")
    return json.loads(rows[0]["payload"])


async def read_qq_attachment(
    ctx: RunContext[AgentContext],
    store,
    account,
    kind: str,
    conversation_id: str,
    message_id: str,
    index: int,
    save_path: str | None,
) -> ToolReturn | dict:
    if account["platform"] != "qq":
        raise ModelRetry("Attachment reads are available for QQ.")
    payload = await history_message(store, account, kind, conversation_id, message_id)
    attachments = payload.get("attachments", [])
    if not 1 <= index <= len(attachments) or not payload.get("qq_context"):
        raise ModelRetry("Choose an attachment index from channel history.")
    metadata = attachments[index - 1]
    inline_image = save_path is None and metadata["kind"] in {"image", "sticker"}
    if not inline_image and save_path is None:
        raise ModelRetry("Supply an absolute Agent Environment save_path for this attachment.")
    if save_path is not None and (not save_path.startswith("/") or "\\" in save_path):
        raise ModelRetry("Use an absolute Agent Environment save_path.")
    limit = 10 << 20 if inline_image else MEDIA_LIMITS.get(metadata["kind"], MEDIA_LIMITS["image"])
    if metadata.get("size", 0) > limit:
        raise ModelRetry("This attachment exceeds the size limit for this operation. Use save_path for large images.")
    try:
        context = store.qq_message_context(account, message_id, payload["qq_context"])
        url = context["urls"][index - 1]
        async with asyncio.timeout(45), open_attachment(url, ctx.deps.configuration.authorize_url) as response:
            if response.status_code != 200:
                raise ModelRetry("QQ could not serve this attachment. Ask the sender to resend it.")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise ModelRetry("QQ returned an unsupported attachment encoding.")
            length = response.headers.get("Content-Length")
            if length is not None and int(length) > limit:
                raise ModelRetry("This attachment exceeds the size limit for this operation.")

            async def chunks():
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > limit:
                        raise ModelRetry("This attachment exceeds the size limit for this operation.")
                    yield chunk

            if inline_image:
                content = b"".join([chunk async for chunk in chunks()])
                return ToolReturn(
                    return_value=metadata, content=[BinaryContent(data=content, media_type=image_type(content))]
                )
            await ctx.deps.environment.files.write_bytes_stream(save_path, chunks(), mode="create")
            return {**metadata, "path": save_path}
    except (EnvironmentError, httpx.HTTPError, ValueError, OSError, ServiceError, IndexError):
        raise ModelRetry(
            "The QQ attachment could not be read or saved. Check its availability and the Environment path."
        ) from None


async def prepare_qq_send(
    ctx: RunContext[AgentContext], kind: str, text: str | None, rich: QQRichContent | None
) -> bytes | None:
    try:
        validate_rich(kind, text, rich)
        if isinstance(rich, QQMedia) and rich.path is not None:
            limit = MEDIA_LIMITS[rich.kind]
            buffer = bytearray()
            async with aclosing(ctx.deps.environment.files.read_bytes_stream(rich.path)) as stream:
                async for chunk in stream:
                    if len(buffer) + len(chunk) > limit:
                        raise ValueError("The file exceeds the QQ media size limit.")
                    buffer.extend(chunk)
            if not buffer:
                raise ValueError("The media file is empty.")
            data = bytes(buffer)
            if rich.kind == "image":
                image_type(data)
            return data
    except ValueError as error:
        raise ModelRetry(str(error)) from None
    except (EnvironmentError, OSError):
        raise ModelRetry("The Agent Environment could not read the media file.") from None
    return None


class WorkspaceChannelTools(AbstractHarnessPlugin):
    def __init__(self, plugin_id: str, factory: ChannelToolFactory, allowed: frozenset[str]) -> None:
        self._plugin_id = plugin_id
        self.factory = factory
        self.allowed = allowed

    @property
    def plugin_id(self) -> str:
        return self._plugin_id

    def get_capabilities(self):
        return [
            Capability(
                id=self.plugin_id,
                tools=[
                    Tool(self.channel_list_accounts),
                    Tool(self.channel_list_conversations),
                    Tool(self.channel_read_messages),
                    Tool(self.channel_read_message),
                    Tool(self.channel_read_attachment),
                    Tool(self.channel_send_proactive),
                ],
            )
        ]

    async def accounts(self, ctx: RunContext[AgentContext], *, sending: bool = False):
        active = self.factory.channels
        if active is None:
            raise ModelRetry("Channels is not configured on this Service.")
        async with short_session(active.host.storage) as session:
            run = await active_run(session, ctx)
            scope = WorkspaceScope(run.organization_id, run.workspace_id)
            workspace = await session.get(WorkspaceRow, scope.workspace_id)
            organization = await session.get(OrganizationRow, scope.organization_id)
            if (
                workspace is None
                or workspace.archived_at is not None
                or workspace.organization_id != scope.organization_id
                or organization is None
                or FACTORY_KEY not in policy(organization.settings).enabled
            ):
                raise ModelRetry("Channels is unavailable in this workspace.")
            try:
                actor = await principal_for(session, active.host.access, run.principal_id, confinement=scope)
                authority = ExecutionAuthority.model_validate(run.authority)
                authorize(actor, scope, "read", authority=authority)
                if sending:
                    authorize(actor, scope, "run", authority=authority)
            except ServiceError as error:
                raise ModelRetry(error.message) from None
        rows = await active.store.query(
            """SELECT id, name, app_id, organization_id, app_secret, intents, platform
               FROM channel_accounts WHERE workspace_id = ? AND organization_id = ? AND app_id IS NOT NULL
               ORDER BY name, id""",
            scope.workspace_id,
            scope.organization_id,
        )
        return active, {row["id"]: row for row in rows if not self.allowed or row["id"] in self.allowed}

    async def account(self, ctx: RunContext[AgentContext], channel_id: str, *, sending: bool = False):
        active, accounts = await self.accounts(ctx, sending=sending)
        if channel_id not in accounts:
            raise ModelRetry("Choose an available channel ID from channel_list_accounts.")
        return active, accounts[channel_id]

    async def channel_list_accounts(self, ctx: RunContext[AgentContext]) -> list[dict]:
        """List channel account IDs and names available for reading in this workspace."""
        _, accounts = await self.accounts(ctx)
        return [{key: row[key] for key in ("id", "name", "platform")} for row in accounts.values()]

    async def channel_list_conversations(
        self, ctx: RunContext[AgentContext], channel_id: ChannelId, limit: Annotated[int, Field(ge=1, le=100)] = 30
    ) -> list[dict]:
        """List recent conversations for an allowed channel. Use the returned kind and conversation ID to read messages."""
        active, account = await self.account(ctx, channel_id)
        rows = await active.store.query(
            f"""SELECT json_extract(payload, '$.kind') AS kind,
                       json_extract(payload, '$.conversation_id') AS conversation_id,
                       max(json_extract(payload, '$.timestamp')) AS latest_at
                FROM {message_table(account["platform"])} WHERE channel_id = ? AND account_id = ?
                GROUP BY kind, conversation_id ORDER BY max(rowid) DESC LIMIT ?""",
            account["id"],
            account["app_id"],
            limit,
        )
        return [dict(row) for row in rows]

    async def channel_read_messages(
        self,
        ctx: RunContext[AgentContext],
        channel_id: ChannelId,
        kind: Annotated[str, Field(max_length=32)],
        conversation_id: Annotated[str, Field(min_length=1, max_length=512)],
        limit: Annotated[int, Field(ge=1, le=20)] = 10,
        before_id: Annotated[str, Field(max_length=1024)] | None = None,
    ) -> list[dict]:
        """Read incoming messages newest first. Pass the last message ID as before_id for older messages."""
        active, account = await self.account(ctx, channel_id)
        return await read_messages(active.store, account, kind, conversation_id, limit, before_id)

    async def channel_read_message(
        self,
        ctx: RunContext[AgentContext],
        channel_id: ChannelId,
        kind: Annotated[str, Field(max_length=32)],
        conversation_id: Annotated[str, Field(min_length=1, max_length=512)],
        message_id: MessageId,
    ) -> dict:
        """Read the full stored text, structure, and attachment metadata for one message."""
        active, account = await self.account(ctx, channel_id)
        payload = await history_message(active.store, account, kind, conversation_id, message_id)
        return ChannelMessage.model_validate(payload).model_dump()

    async def channel_read_attachment(
        self,
        ctx: RunContext[AgentContext],
        channel_id: ChannelId,
        kind: QQConversationKind,
        conversation_id: Annotated[str, Field(min_length=1, max_length=512)],
        message_id: MessageId,
        index: AttachmentIndex,
        save_path: EnvironmentPath | None = None,
    ) -> ToolReturn | dict:
        """View a QQ image or sticker, or save an attachment to an absolute Agent Environment path."""
        active, account = await self.account(ctx, channel_id)
        return await read_qq_attachment(ctx, active.store, account, kind, conversation_id, message_id, index, save_path)

    async def channel_send_proactive(
        self,
        ctx: RunContext[AgentContext],
        channel_id: ChannelId,
        kind: QQConversationKind,
        conversation_id: Annotated[str, Field(min_length=1, max_length=512)],
        text: ReplyText | None = None,
        rich: QQRichContent | None = None,
    ) -> dict:
        """Send text, media, Markdown, or a card to a QQ conversation without a source message."""
        active, account = await self.account(ctx, channel_id, sending=True)
        if account["platform"] != "qq":
            return {"status": "unsupported", "reason": "Proactive sending is supported for QQ."}
        data = await prepare_qq_send(ctx, kind, text, rich)
        if data is not None:
            active, account = await self.account(ctx, channel_id, sending=True)
        qq = QQConnection(active.http, channel_id, active.store.credentials(account))
        try:
            message_id = await qq.send_proactive(kind, conversation_id, text, rich=rich, data=data)
        except httpx.HTTPStatusError as error:
            code = error.response.status_code
            return {"status": "rejected" if 400 <= code < 500 and code != 408 else "unknown", "http_status": code}
        except QQRejected as error:
            return {"status": "rejected", "message": str(error)}
        except (httpx.RequestError, ValueError, KeyError, TypeError):
            return {"status": "unknown"}
        return {"status": "sent", "message_id": message_id}


class ChannelTools(AbstractHarnessPlugin):
    def __init__(self, plugin_id: str, factory: ChannelToolFactory) -> None:
        self._plugin_id = plugin_id
        self.factory = factory

    @property
    def plugin_id(self) -> str:
        return self._plugin_id

    def get_capabilities(self):
        return [
            Capability(
                id=self.plugin_id,
                instructions="Use channel_send_message to reply in the originating chat. Normal assistant text is "
                "shown only in Console. Use channel_list_messages to inspect recent chat context. "
                "Replies use the message that started the run. "
                "When reading, use the message_id from Channel source to select the input. "
                "Use channel_get_attachment to inspect QQ images and stickers or save other files. "
                "To send a generated image or sticker, pass rich={type:'media',path:'/absolute/environment/path'} "
                "to channel_send_message.",
                tools=[
                    Tool(self.channel_list_messages),
                    Tool(self.channel_get_message),
                    Tool(self.channel_get_attachment),
                    Tool(self.channel_send_message),
                ],
            )
        ]

    async def source(self, ctx: RunContext[AgentContext], message_id: str | None = None, *, from_trigger: bool = False):
        active = self.factory.channels
        if active is None:
            raise ModelRetry("Channels is not configured on this Service.")
        async with short_session(active.host.storage) as session:
            run = await active_run(session, ctx)
            origins = [run.id]
            origin = run
            while origin.trigger == "resume" and origin.parent_run_id:
                origin = await session.get_one(RunRow, origin.parent_run_id)
                origins.append(origin.id)
            entries = select(InboxEntryRow.request_key).where(
                InboxEntryRow.assigned_run_id.in_(origins),
                InboxEntryRow.kind == "message",
                InboxEntryRow.status.in_(("assigned", "consumed")),
                InboxEntryRow.request_key.startswith("channels-"),
            )
            if from_trigger:
                entries = entries.where(InboxEntryRow.id == origin.source_entry_id)
            keys = list(await session.scalars(entries))
            workspace_id, principal_id, agent_id = run.workspace_id, run.principal_id, run.agent_id
            authority = ExecutionAuthority.model_validate(run.authority)

        if not keys:
            if from_trigger:
                raise ModelRetry("Replies require a run started by a channel message.")
            raise ModelRetry("Channel tools require a run with a channel delivery.")
        placeholders = ", ".join("?" for _ in keys)
        rows = await active.store.query(
            f"""SELECT * FROM channel_deliveries WHERE tool_key IN ({placeholders})
                AND workspace_id = ? AND agent_id = ? AND status IN ('queued', 'submitted')
                AND (? IS NULL OR message_id = ?) LIMIT 2""",
            *keys,
            workspace_id,
            agent_id,
            message_id,
            message_id,
        )
        if len(rows) > 1:
            raise ModelRetry(
                "Multiple channel messages are in this run. Pass the exact message_id from Channel source."
            )
        if not rows:
            raise ModelRetry("The channel delivery is no longer active.")
        job = rows[0]
        account = (await active._enabled_accounts()).get(job["channel_id"])
        if account is None or account["workspace_id"] != workspace_id or account["app_id"] != job["account_id"]:
            raise ModelRetry("The channel account is unavailable.")
        platform = account["platform"]
        rows = await active.store.query(
            f"SELECT payload FROM {message_table(platform)} WHERE channel_id = ? AND account_id = ? AND message_id = ?",
            job["channel_id"],
            job["account_id"],
            job["message_id"],
        )
        if not rows:
            raise ModelRetry("The source message is unavailable.")
        message = ChannelMessage.model_validate_json(rows[0]["payload"])
        route = select_route(
            await active.store.routes(workspace_id),
            message.channel_id,
            message.kind,
            message.conversation_id,
            bot_mentioned=message.bot_mentioned,
            sender_id=message.sender_id,
        )
        if (
            route is None
            or not route.enabled
            or route.binding_id != job["binding_id"]
            or route.principal_id != principal_id
        ):
            raise ModelRetry("The delivery route no longer authorizes this conversation.")
        try:
            actor = await active.deliveries.actor(principal_id, account)
            scope = WorkspaceScope(account["organization_id"], workspace_id)
            authorize(actor, scope, "read", authority=authority)
            authorize(actor, scope, "run", authority=authority)
        except ServiceError as error:
            raise ModelRetry(error.message) from None
        return active, account, job

    async def channel_list_messages(
        self,
        ctx: RunContext[AgentContext],
        limit: Annotated[int, Field(ge=1, le=20)] = 10,
        message_id: Annotated[str, Field(min_length=1, max_length=1024)] | None = None,
    ) -> list[dict]:
        """Read recent incoming messages and attachment metadata in the source conversation, newest first."""
        active, account, job = await self.source(ctx, message_id)
        return await read_messages(active.store, account, job["kind"], job["conversation_id"], limit)

    async def channel_get_attachment(
        self,
        ctx: RunContext[AgentContext],
        index: AttachmentIndex,
        message_id: MessageId,
        source_message_id: MessageId | None = None,
        save_path: EnvironmentPath | None = None,
    ) -> ToolReturn | dict:
        """View a QQ image or sticker, or save an attachment. Pass source_message_id to open an older message."""
        active, account, job = await self.source(ctx, source_message_id or message_id)
        return await read_qq_attachment(
            ctx,
            active.store,
            account,
            job["kind"],
            job["conversation_id"],
            message_id,
            index,
            save_path,
        )

    async def channel_get_message(
        self,
        ctx: RunContext[AgentContext],
        message_id: MessageId,
        source_message_id: MessageId | None = None,
    ) -> dict:
        """Read a full stored message. To read older history, identify this run's source with source_message_id."""
        active, account, job = await self.source(ctx, source_message_id or message_id)
        payload = await history_message(active.store, account, job["kind"], job["conversation_id"], message_id)
        return ChannelMessage.model_validate(payload).model_dump()

    async def channel_send_message(
        self,
        ctx: RunContext[AgentContext],
        text: ReplyText | None = None,
        rich: QQRichContent | None = None,
        reference_message_id: MessageId | None = None,
    ) -> dict:
        """Reply to the message that started this run. QQ supports rich content and reference_message_id quotes."""
        active, account, job = await self.source(ctx, from_trigger=True)
        if ctx.tool_call_id is None:
            raise ModelRetry("A recorded tool call is required to send a message.")
        if account["platform"] != "qq" and (rich is not None or reference_message_id is not None):
            raise ModelRetry("This channel supports text replies.")
        if text is None and rich is None:
            raise ModelRetry("Supply text or rich content.")
        recorded = (
            text
            if rich is None and reference_message_id is None
            else sha256(
                json.dumps(
                    [text, rich.model_dump(mode="json") if rich else None, reference_message_id],
                    sort_keys=True,
                ).encode()
            ).hexdigest()
        )
        call_id = sha256(ctx.tool_call_id.encode()).hexdigest()
        claimed = await active.store.query(
            """INSERT OR IGNORE INTO channel_sends (delivery_id, call_id, text, status)
               SELECT id, ?, ?, 'sending' FROM channel_deliveries
               WHERE id = ? AND status IN ('queued', 'submitted') RETURNING status""",
            call_id,
            recorded,
            job["id"],
        )
        if not claimed:
            existing = await active.store.query(
                "SELECT text, status, message_id FROM channel_sends WHERE delivery_id = ? AND call_id = ?",
                job["id"],
                call_id,
            )
            if not existing:
                raise ModelRetry("The channel delivery is no longer active.")
            previous = existing[0]
            if previous["text"] != recorded:
                raise ModelRetry("The tool call ID was already used for a different reply.")
            if previous["status"] == "sent":
                return {"status": "sent", "message_id": previous["message_id"]}
            return {"status": previous["status"], "message": "This send was already attempted. Do not repeat it."}
        try:
            data = await prepare_qq_send(ctx, job["kind"], text, rich)
            if data is not None:
                _, account, _ = await self.source(ctx, from_trigger=True)
            reference_id = None
            if reference_message_id is not None:
                quoted = await history_message(
                    active.store, account, job["kind"], job["conversation_id"], reference_message_id
                )
                reference_id = quoted.get("structured", {}).get("native_reference_id")
                if not reference_id:
                    if job["kind"] in {"private", "group"}:
                        raise ModelRetry(
                            "QQ did not supply a native quote identifier for that message. Use a normal reply."
                        )
                    reference_id = reference_message_id
        except ModelRetry:
            await active.store.query(
                "DELETE FROM channel_sends WHERE delivery_id = ? AND call_id = ? AND status = 'sending'",
                job["id"],
                call_id,
            )
            raise
        try:
            if rich is None and reference_id is None:
                sent = await active.send_reply(account, job["message_id"], text)
            else:
                sent = await active.send_reply(
                    account, job["message_id"], text, rich=rich, data=data, reference_id=reference_id
                )
        except ServiceError as error:
            status = "unknown" if error.code == "unavailable" else "failed"
            await active.store.query(
                "UPDATE channel_sends SET status = ? WHERE delivery_id = ? AND call_id = ?",
                status,
                job["id"],
                call_id,
            )
            return {"status": status, "message": error.message}
        await active.store.query(
            "UPDATE channel_sends SET status = 'sent', message_id = ? WHERE delivery_id = ? AND call_id = ?",
            sent,
            job["id"],
            call_id,
        )
        return {"status": "sent", "message_id": sent}
