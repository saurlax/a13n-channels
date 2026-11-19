"""Harness tools whose authority comes from the channel delivery that started the run."""

from hashlib import sha256
from typing import TYPE_CHECKING, Annotated

from a13n_harness import AbstractHarnessPlugin
from a13n_harness.context import AgentContext
from a13n_harness.plugin_factories import HarnessPluginFactory, HarnessPluginFactoryContext
from a13n_service.infra.db import short_session
from a13n_service.infra.errors import ServiceError
from a13n_service.runs.tables import InboxEntryRow, RunRow
from a13n_service.tenancy.authorize import ExecutionAuthority, WorkspaceScope, authorize
from pydantic import BaseModel, Field
from pydantic_ai import ModelRetry, RunContext, Tool
from pydantic_ai.capabilities import Capability

from a13n_channels.messages import ChannelMessage, ReplyText
from a13n_channels.routing import select_route

if TYPE_CHECKING:
    from a13n_channels.app import ChannelsRuntime

FACTORY_KEY = "saurlax.channels"


class ChannelToolSettings(BaseModel, extra="forbid"):
    pass


class ChannelToolFactory(HarnessPluginFactory):
    configuration_model = ChannelToolSettings
    display_name = "Channels"
    description = "Read recent messages and send a reply in the active channel conversation."

    def __init__(self) -> None:
        self.channels: ChannelsRuntime | None = None

    @classmethod
    def plugin_key(cls) -> str:
        return FACTORY_KEY

    def create_plugin(self, context: HarnessPluginFactoryContext) -> AbstractHarnessPlugin:
        return ChannelTools(context.plugin_id, self)


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
                "shown only in Console. Use channel_list_messages to inspect recent chat context.",
                tools=[Tool(self.channel_list_messages), Tool(self.channel_send_message)],
            )
        ]

    async def source(self, ctx: RunContext[AgentContext]):
        active = self.factory.channels
        instance = ctx.deps.instance
        refs = instance.host_refs
        run_id = refs.get("run_id")
        if active is None or instance.identity.issuer != "a13n-service" or instance.agent_instance_id != run_id:
            raise ModelRetry("Channel tools require a Service run started by a channel delivery.")
        async with short_session(active.host.storage) as session:
            run = await session.get(RunRow, run_id)
            if (
                run is None
                or run.status != "running"
                or run.current_attempt_id != refs.get("attempt_id")
                or run.principal_id != instance.identity.subject
            ):
                raise ModelRetry("This channel run is no longer active.")
            origin = run
            while origin.trigger == "resume" and origin.parent_run_id:
                origin = await session.get_one(RunRow, origin.parent_run_id)
            entry = await session.get(InboxEntryRow, origin.source_entry_id) if origin.source_entry_id else None
            key = entry.request_key if entry and entry.assigned_run_id == origin.id else None
            if not key or not key.startswith("channels-"):
                raise ModelRetry("Channel tools require a run started by a channel delivery.")
            workspace_id, principal_id, agent_id = run.workspace_id, run.principal_id, run.agent_id
            authority = ExecutionAuthority.model_validate(run.authority)

        rows = await active.store.query(
            """SELECT * FROM channel_deliveries WHERE tool_key = ? AND workspace_id = ? AND agent_id = ?
               AND status IN ('queued', 'submitted')""",
            key,
            workspace_id,
            agent_id,
        )
        if not rows:
            raise ModelRetry("The channel delivery is no longer active.")
        job = rows[0]
        account = (await active._enabled_accounts()).get(job["channel_id"])
        if account is None or account["workspace_id"] != workspace_id or account["app_id"] != job["account_id"]:
            raise ModelRetry("The channel account is unavailable.")
        platform = account["platform"]
        rows = await active.store.query(
            f"SELECT payload FROM {platform}_messages WHERE channel_id = ? AND account_id = ? AND message_id = ?",
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
        self, ctx: RunContext[AgentContext], limit: Annotated[int, Field(ge=1, le=20)] = 10
    ) -> list[dict]:
        """Read recent incoming text messages in the conversation that triggered this run, newest first."""
        active, account, job = await self.source(ctx)
        platform = account["platform"]
        rows = await active.store.query(
            f"""SELECT payload FROM {platform}_messages WHERE channel_id = ? AND account_id = ?
                AND json_extract(payload, '$.kind') = ? AND json_extract(payload, '$.conversation_id') = ?
                ORDER BY rowid DESC LIMIT ?""",
            job["channel_id"],
            job["account_id"],
            job["kind"],
            job["conversation_id"],
            limit,
        )
        return [
            message.model_copy(update={"text": message.text[:4000]}).model_dump()
            for message in (ChannelMessage.model_validate_json(row["payload"]) for row in rows)
        ]

    async def channel_send_message(self, ctx: RunContext[AgentContext], text: ReplyText) -> dict:
        """Send a text reply to the chat that triggered this run. Only call this when you want the chat to see it."""
        active, account, job = await self.source(ctx)
        if ctx.tool_call_id is None:
            raise ModelRetry("A recorded tool call is required to send a message.")
        call_id = sha256(ctx.tool_call_id.encode()).hexdigest()
        claimed = await active.store.query(
            """INSERT OR IGNORE INTO channel_sends (delivery_id, call_id, text, status)
               SELECT id, ?, ?, 'sending' FROM channel_deliveries
               WHERE id = ? AND status IN ('queued', 'submitted') RETURNING status""",
            call_id,
            text,
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
            if previous["text"] != text:
                raise ModelRetry("The tool call ID was already used for a different reply.")
            if previous["status"] == "sent":
                return {"status": "sent", "message_id": previous["message_id"]}
            return {"status": previous["status"], "message": "This send was already attempted. Do not repeat it."}
        try:
            sent = await active.send_reply(account, job["message_id"], text)
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
