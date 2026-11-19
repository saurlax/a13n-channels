import asyncio
import base64
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from a13n_service.infra.crypto import KeyRing
from a13n_service.infra.errors import ServiceError
from a13n_service.runs.schemas import NewThread
from a13n_service.tenancy.authorize import Grant, Principal, allowed_verbs
from pydantic import SecretStr

from a13n_channels import delivery
from a13n_channels.accounts import Accounts
from a13n_channels.delivery import Deliveries
from a13n_channels.messages import ChannelMessage
from a13n_channels.routing import RouteInput, Source, select_route


def store_for(tmp_path):
    key = SecretStr(base64.b64encode(b"k" * 32).decode())
    return Accounts(tmp_path / "channels.sqlite3", KeyRing(active_key_id="test", keys={"test": key}))


def rule(name, agent, *sources, enabled=True):
    return RouteInput(
        name=name,
        agent_id=agent,
        sources=[
            Source(channel_id=channel, kind=kind, conversation_id=conversation)
            for channel, kind, conversation in sources
        ],
        enabled=enabled,
    )


def test_workspace_rules_use_most_specific_match_even_when_disabled(tmp_path):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        default = await store.save_route(
            "workspace",
            "default",
            rule("Default", "agent", ("*", "*", "*")),
            principal_id="owner",
            create=True,
        )
        await store.save_route(
            "workspace",
            "private",
            rule("Private", "private-agent", ("channel", "private", "*")),
            principal_id="owner",
            create=True,
        )
        blocked = await store.save_route(
            "workspace",
            "block",
            rule("Block", "agent", ("channel", "private", "user"), enabled=False),
            principal_id="owner",
            create=True,
        )
        routes = await store.routes("workspace")
        assert select_route(routes, "channel", "private", "user") == blocked
        assert select_route(routes, "another", "private", "user") == default
        assert select_route(routes, "channel", "private", "another").id == "private"
        assert await store.routes("other") == []
        with pytest.raises(ServiceError, match="already belongs"):
            await store.save_route(
                "workspace",
                "overlap",
                rule("Overlap", "agent", ("channel", "private", "user")),
                principal_id="owner",
                create=True,
            )
        with pytest.raises(ServiceError, match="channel in this workspace"):
            await store.save_route(
                "other",
                "wrong",
                rule("Wrong", "agent", ("channel", "private", "user")),
                principal_id="owner",
                create=True,
            )
        await store.receive(incoming("blocked"), "credentials")
        assert await store.query("SELECT * FROM channel_deliveries") == []
        assert len(await store.query("SELECT * FROM qq_messages")) == 1

    asyncio.run(exercise())


@pytest.fixture
def published_agent(monkeypatch):
    async def agent(_storage, _actor, _workspace, agent_id):
        return SimpleNamespace(
            id=agent_id, default_revision_id="apr_012345678901234567890123456789ab", archived_at=None
        )

    async def revision(_storage, _actor, _workspace, _agent_id, revision_id):
        return SimpleNamespace(id=revision_id, config=SimpleNamespace(plugins=[]))

    monkeypatch.setattr(delivery.agents, "get_agent", agent)
    monkeypatch.setattr(delivery.agents, "get_revision", revision)


def incoming(message_id, conversation="user"):
    return ChannelMessage(
        id=message_id,
        channel_id="channel",
        kind="private",
        conversation_id=conversation,
        sender_id=conversation,
        text=f"prompt-{message_id}",
        timestamp="2026-11-18T01:00:00Z",
    )


@pytest.mark.parametrize("platform", ["qq", "weixin"])
def test_matched_text_retries_same_submission_and_reuses_source_thread(
    platform, tmp_path, monkeypatch, published_agent
):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        if platform == "qq":
            await store.query(
                "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
            )
            account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        else:
            await store.query(
                "INSERT INTO weixin_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', '')"
            )
            account = (await store.query("SELECT *, 'weixin' AS platform, bot_id AS app_id FROM weixin_accounts"))[0]

        async def receive(message):
            if platform == "qq":
                await store.receive(message, "credentials")
            else:
                await store.receive_weixin(account, "next-cursor", [(message, "reply-context")])

        await receive(incoming("before-route"))
        await store.save_route(
            "workspace",
            "route",
            rule("Assistant", "ag_012345678901234567890123456789ab", ("channel", "private", "*")),
            principal_id="owner",
            create=True,
        )
        assert await store.query("SELECT * FROM channel_deliveries") == []
        first = incoming("first")
        await receive(first)
        await receive(first)
        await receive(incoming("second"))
        await receive(incoming("other-source", "other"))
        assert len(await store.query("SELECT * FROM channel_deliveries")) == 3

        posts = {}
        attempts = []
        access = object()
        runtime = SimpleNamespace(access=access, storage=object())
        owner_active = False

        @asynccontextmanager
        async def session(storage):
            assert storage is runtime.storage
            yield object()

        async def owner(_session, current_access, principal_id, *, confinement):
            assert current_access is access and principal_id == "owner"
            assert confinement.workspace_id == "workspace" and confinement.organization_id == "org"
            verbs = frozenset({"admin", "write", "read", "run"} if owner_active else {"read"})
            return Principal(principal_id, "user", (Grant("org", None, verbs),), confinement)

        def record(host, actor, workspace_id, message, path, key):
            assert host is runtime and actor.id == "owner" and workspace_id == "workspace"
            assert allowed_verbs(actor, actor.confinement) == {"read", "run"}
            assert message.agent_id == "ag_012345678901234567890123456789ab"
            assert message.delivery == "next_run"
            assert message.options.overrides.plugins[0].plugin_key == "saurlax.channels.delivery"
            metadata = message.payload.content[1].value
            identifier = metadata["message_id"]
            invocation = (identifier, path, key, message)
            attempts.append(invocation)
            if key in posts:
                assert posts[key][0] == invocation
            else:
                posts[key] = (
                    invocation,
                    SimpleNamespace(
                        thread=SimpleNamespace(id="thread-" + metadata["conversation_id"]),
                        entry=SimpleNamespace(id=identifier),
                    ),
                )
                if identifier == "first":
                    raise ServiceError("unavailable", "lost receipt")
            return posts[key][1], True

        async def create(host, actor, workspace_id, message, *, request_key):
            assert isinstance(message, NewThread)
            return record(host, actor, workspace_id, message, "/threads", request_key)

        async def append(host, actor, workspace_id, thread_id, message, *, request_key):
            return record(host, actor, workspace_id, message, f"/threads/{thread_id}/inbox", request_key)

        async def read_entry(_storage, _actor, _workspace, _thread, entry_id):
            return SimpleNamespace(assigned_run_id=entry_id, status="consumed")

        async def read_run(_storage, _actor, _workspace, _run):
            return SimpleNamespace(status="completed", resumed_by_id=None)

        monkeypatch.setattr(delivery, "short_session", session)
        monkeypatch.setattr(delivery, "principal_for", owner)
        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.submit, "submit_message", append)
        monkeypatch.setattr(delivery.entries, "get_entry", read_entry)
        monkeypatch.setattr(delivery.runs, "get", read_run)

        deliveries = Deliveries(store, runtime)
        await deliveries.process({"channel": account})
        assert attempts == []
        assert any(
            job["error"] == "service:forbidden" for job in await store.query("SELECT error FROM channel_deliveries")
        )
        owner_active = True
        await store.query("UPDATE channel_deliveries SET next_at = 0")
        await deliveries.process({"channel": account})
        assert sorted(item[0] for item in attempts) == ["first", "other-source"]
        await store.query("UPDATE channel_deliveries SET next_at = 0")
        for _ in range(4):
            await deliveries.process({"channel": account})

        first_posts = [item for item in attempts if item[0] == "first"]
        assert len(first_posts) == 2 and first_posts[0] == first_posts[1]
        assert next(item[1] for item in attempts if item[0] == "second") == "/threads/thread-user/inbox"
        assert next(item[1] for item in attempts if item[0] == "other-source") == "/threads"
        assert {job["status"] for job in await store.query("SELECT status FROM channel_deliveries")} == {"completed"}

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("policy", "fixed_id", "first_batch", "expected"),
    [
        ("conversation", None, 2, {"one": "new-one", "two": "new-one", "three": "new-one", "four": "new-four"}),
        ("sender", None, 3, {"one": "new-one", "two": "new-two", "three": "new-one", "four": "new-four"}),
        ("fixed", None, 1, dict.fromkeys(("one", "two", "three", "four"), "new-one")),
        ("fixed", "existing", 1, dict.fromkeys(("one", "two", "three", "four"), "existing")),
        ("message", None, 4, {name: f"new-{name}" for name in ("one", "two", "three", "four")}),
    ],
)
def test_thread_destination_controls_reuse_and_queue_order(
    policy, fixed_id, first_batch, expected, tmp_path, monkeypatch, published_agent
):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        agent_id = "ag_012345678901234567890123456789ab"
        route = RouteInput(
            name="Route",
            agent_id=agent_id,
            sources=[Source(channel_id="channel", kind="*", conversation_id="*")],
            thread_policy=policy,
            fixed_thread_id=fixed_id,
        )
        saved = await store.save_route("workspace", "route", route, principal_id="owner", create=True)
        assert (await store.routes("workspace"))[0] == saved
        for name, conversation, sender in (
            ("one", "g1", "alice"),
            ("two", "g1", "bob"),
            ("three", "g1", "alice"),
            ("four", "g2", "alice"),
        ):
            message = incoming(name, conversation).model_copy(update={"kind": "group", "sender_id": sender})
            await store.receive(message, "credentials")
        other_policy = "message" if policy != "message" else "conversation"
        with pytest.raises(ServiceError, match="Wait for route deliveries"):
            await store.save_route(
                "workspace",
                "route",
                route.model_copy(update={"thread_policy": other_policy, "fixed_thread_id": None}),
                principal_id="owner",
                create=False,
            )

        submissions = {}

        def accept(message, thread_id=None):
            name = message.payload.content[1].value["message_id"]
            target = thread_id or f"new-{name}"
            submissions[name] = target
            return SimpleNamespace(thread=SimpleNamespace(id=target), entry=SimpleNamespace(id=name)), True

        async def create(_runtime, _actor, _workspace, message, *, request_key):
            return accept(message)

        async def append(_runtime, _actor, _workspace, thread_id, message, *, request_key):
            return accept(message, thread_id)

        async def actor(_self, _principal, _account):
            return object()

        async def collect(_self, _job, _actor):
            pass

        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.submit, "submit_message", append)
        monkeypatch.setattr(Deliveries, "actor", actor)
        monkeypatch.setattr(Deliveries, "collect", collect)
        deliveries = Deliveries(store, SimpleNamespace(storage=None))
        await deliveries.process({"channel": account})
        assert len(submissions) == first_batch
        for _ in range(3):
            await deliveries.process({"channel": account})
        assert submissions == expected

    asyncio.run(exercise())


def test_mention_filter_preserves_source_priority_and_rechecks_queued_messages(tmp_path):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        default = rule("Default", "agent", ("*", "*", "*"))
        await store.save_route("workspace", "default", default, principal_id="owner", create=True)
        all_messages = rule("Group", "agent", ("channel", "group", "user"))
        mentions = RouteInput(
            name="Group mentions",
            agent_id="agent",
            sources=[Source(channel_id="channel", kind="group", conversation_id="user", mentions_only=True)],
        )
        await store.save_route("workspace", "group", all_messages, principal_id="owner", create=True)
        with pytest.raises(ServiceError, match="already belongs"):
            await store.save_route("workspace", "duplicate", mentions, principal_id="owner", create=True)
        plain = incoming("before-change").model_copy(update={"kind": "group", "bot_mentioned": False})
        await store.receive(plain, "credentials")
        await store.save_route("workspace", "group", mentions, principal_id="owner", create=False)
        await store.receive(plain.model_copy(update={"id": "ordinary"}), "credentials")
        await store.receive(plain.model_copy(update={"id": "mention", "bot_mentioned": True}), "credentials")
        jobs = await store.query("SELECT * FROM channel_deliveries ORDER BY rowid")
        assert [(job["message_id"], job["route_id"]) for job in jobs] == [
            ("before-change", "group"),
            ("mention", "group"),
        ]
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        routes = await store.routes("workspace")
        await Deliveries(store, None).advance(jobs[0], account, routes)
        assert (await store.query("SELECT status FROM channel_deliveries WHERE message_id = 'before-change'"))[0][
            "status"
        ] == "stopped"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("output", "expected"),
    [('{"wake": false}', "filtered"), ('{"wake": "true"}', "failed"), ('{"wake": true}', "queued")],
)
def test_wake_filter_runs_before_the_main_agent(output, expected, tmp_path, monkeypatch, published_agent):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        route = RouteInput(
            name="Wake route",
            agent_id="ag_012345678901234567890123456789ab",
            wake_agent_id="ag_abcdefabcdefabcdefabcdefabcdefab",
            multiparty_messages="mentions",
            sources=[Source(channel_id="channel", kind="*", conversation_id="*")],
        )
        await store.save_route("workspace", "wake", route, principal_id="owner", create=True)
        group = incoming("plain").model_copy(update={"kind": "group", "bot_mentioned": False})
        await store.receive(group, "credentials")
        await store.receive(group.model_copy(update={"id": "mention", "bot_mentioned": True}), "credentials")
        await store.receive(incoming("private"), "credentials")
        assert [job["message_id"] for job in await store.query("SELECT * FROM channel_deliveries ORDER BY rowid")] == [
            "mention",
            "private",
        ]

        # The private message uses another destination. This test advances the mentioned message through each phase.
        async def jobs():
            return await store.query("SELECT * FROM channel_deliveries WHERE message_id = 'mention'")

        async def actor(*_args):
            return object()

        submitted = []

        async def create(_runtime, _actor, _workspace, body, *, request_key):
            submitted.append((body, request_key))
            return SimpleNamespace(thread=SimpleNamespace(id="thr_new"), entry=SimpleNamespace(id="inb_new")), True

        async def entry(*_args):
            return SimpleNamespace(assigned_run_id="run_wake")

        async def run(*_args):
            return SimpleNamespace(status="completed", resumed_by_id=None, output=output)

        monkeypatch.setattr(Deliveries, "actor", actor)
        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.entries, "get_entry", entry)
        monkeypatch.setattr(delivery.runs, "get", run)
        deliveries = Deliveries(store, SimpleNamespace(storage=None))
        routes = await store.routes("workspace")
        await deliveries.advance((await jobs())[0], account, routes)
        assert submitted[0][0].agent_id == route.wake_agent_id
        assert submitted[0][0].options.overrides.output_spec.schema_ is None
        assert (await jobs())[0]["status"] == "wake_submitted"
        await deliveries.advance((await jobs())[0], account, routes)
        assert (await jobs())[0]["status"] == expected
        assert len(submitted) == 1
        if expected == "queued":
            await deliveries.advance((await jobs())[0], account, routes)
            assert submitted[1][0].agent_id == route.agent_id
            assert submitted[1][0].payload.text() == group.text
            assert submitted[1][1] != submitted[0][1]
            assert (await jobs())[0]["status"] == "submitted"

    asyncio.run(exercise())


def test_channel_tools_use_trusted_delivery_and_do_not_repeat_a_send(tmp_path, monkeypatch):
    from pydantic_ai import ModelRetry

    from a13n_channels import tools

    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        await store.save_route(
            "workspace",
            "route",
            rule("Assistant", "agent", ("channel", "private", "*")),
            principal_id="owner",
            create=True,
        )
        await store.receive(incoming("source"), "credentials")
        await store.receive(incoming("other-chat", "other"), "credentials")
        await store.query("UPDATE channel_deliveries SET tool_key = 'channels-trusted' WHERE message_id = 'source'")
        authority = {
            "principal_id": "owner",
            "organization_id": "org",
            "workspace_id": "workspace",
            "verbs": ["read", "run"],
        }
        run = SimpleNamespace(
            id="run",
            status="running",
            current_attempt_id="attempt",
            principal_id="owner",
            workspace_id="workspace",
            agent_id="agent",
            trigger="input",
            source_entry_id="entry",
            authority=authority,
        )
        entry = SimpleNamespace(assigned_run_id="run", request_key="channels-trusted")

        class Session:
            async def get(self, model, _id):
                return run if model is tools.RunRow else entry

        @asynccontextmanager
        async def session(_storage):
            yield Session()

        async def enabled_accounts():
            return {"channel": account}

        async def actor(principal_id, _account):
            return Principal(principal_id, "user", (Grant("org", "workspace", frozenset({"read", "run"})),))

        sent = []

        async def send_reply(source_account, message_id, text):
            assert source_account == account
            sent.append((message_id, text))
            return "platform-message"

        monkeypatch.setattr(tools, "short_session", session)
        factory = tools.ChannelToolFactory()
        factory.channels = SimpleNamespace(
            host=SimpleNamespace(storage=None),
            store=store,
            _enabled_accounts=enabled_accounts,
            deliveries=SimpleNamespace(actor=actor),
            send_reply=send_reply,
        )
        plugin = tools.ChannelTools("channels", factory)
        ctx = SimpleNamespace(
            tool_call_id="call",
            deps=SimpleNamespace(
                instance=SimpleNamespace(
                    identity=SimpleNamespace(issuer="a13n-service", subject="owner"),
                    agent_instance_id="run",
                    host_refs={"run_id": "run", "attempt_id": "attempt"},
                )
            ),
        )
        assert [message["id"] for message in await plugin.channel_list_messages(ctx)] == ["source"]
        receipt = {"status": "sent", "message_id": "platform-message"}
        assert await plugin.channel_send_message(ctx, "reply") == receipt
        assert await plugin.channel_send_message(ctx, "reply") == receipt
        assert sent == [("source", "reply")]
        entry.request_key = "channels-fabricated"
        with pytest.raises(ModelRetry, match="no longer active"):
            await plugin.channel_send_message(ctx, "reply")

    asyncio.run(exercise())


def test_history_tools_filter_accounts_and_read_only_the_selected_conversation(tmp_path, monkeypatch):
    from pydantic_ai import ModelRetry

    from a13n_channels import tools

    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        for channel, workspace in (("channel", "workspace"), ("another", "workspace"), ("foreign", "other")):
            await store.query(
                "INSERT INTO qq_accounts VALUES (?, 'org', ?, ?, ?, 'credentials', 1)",
                channel,
                workspace,
                channel,
                channel,
            )
        for message_id, conversation in (("first", "one"), ("other-chat", "two"), ("latest", "one")):
            await store.receive(incoming(message_id, conversation), "credentials")

        run = SimpleNamespace(
            id="run",
            status="running",
            current_attempt_id="attempt",
            principal_id="owner",
            organization_id="org",
            workspace_id="workspace",
            trigger="input",
            source_entry_id="entry",
            authority={
                "principal_id": "owner",
                "organization_id": "org",
                "workspace_id": "workspace",
                "verbs": ["read"],
            },
        )
        organization = SimpleNamespace(settings={"app_plugins": {"enabled": [tools.FACTORY_KEY]}})
        entry = SimpleNamespace(assigned_run_id="run", request_key=None)

        class Session:
            async def get(self, model, _id):
                return {
                    tools.RunRow: run,
                    tools.InboxEntryRow: entry,
                    tools.OrganizationRow: organization,
                    tools.WorkspaceRow: SimpleNamespace(organization_id="org", archived_at=None),
                }[model]

        @asynccontextmanager
        async def session(_storage):
            yield Session()

        async def principal(_session, _access, principal_id, *, confinement):
            return Principal(principal_id, "user", (Grant("org", "workspace", frozenset({"read"})),), confinement)

        monkeypatch.setattr(tools, "short_session", session)
        monkeypatch.setattr(tools, "principal_for", principal)
        factory = tools.ChannelToolFactory()
        factory.channels = SimpleNamespace(host=SimpleNamespace(storage=None, access=None), store=store)
        plugin = tools.HistoryTools("channels", factory, frozenset({"channel", "foreign"}))
        ctx = SimpleNamespace(
            deps=SimpleNamespace(
                instance=SimpleNamespace(
                    identity=SimpleNamespace(issuer="a13n-service", subject="owner"),
                    agent_instance_id="run",
                    host_refs={"run_id": "run", "attempt_id": "attempt"},
                )
            )
        )
        assert await plugin.channel_list_accounts(ctx) == [{"id": "channel", "name": "channel", "platform": "qq"}]
        assert [row["conversation_id"] for row in await plugin.channel_list_conversations(ctx, "channel")] == [
            "one",
            "two",
        ]
        assert [row["id"] for row in await plugin.channel_read_messages(ctx, "channel", "private", "one", limit=1)] == [
            "latest"
        ]
        assert [
            row["id"]
            for row in await plugin.channel_read_messages(ctx, "channel", "private", "one", before_id="latest")
        ] == ["first"]
        for channel in ("another", "foreign"):
            with pytest.raises(ModelRetry, match="available channel"):
                await plugin.channel_read_messages(ctx, channel, "private", "one")
        all_channels = tools.HistoryTools("channels", factory, frozenset())
        assert {row["id"] for row in await all_channels.channel_list_accounts(ctx)} == {"channel", "another"}
        organization.settings = {}
        with pytest.raises(ModelRetry, match="unavailable"):
            await plugin.channel_list_accounts(ctx)

    asyncio.run(exercise())
