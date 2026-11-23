import asyncio
import base64
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from a13n_harness import ModelCapability
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


def channel_input(message):
    part = message.payload.content[0]
    header, text = part.text.split("\n\nMessage:\n", 1)
    return json.loads(header.splitlines()[0].removeprefix("Channel source: ")), text


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
        return SimpleNamespace(id=revision_id, config=SimpleNamespace(model="model", plugins=[]))

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


@pytest.mark.parametrize(
    ("platform", "image_case"),
    [
        ("qq", "inline"),
        ("qq", "too_many"),
        ("qq", "too_large"),
        ("qq", "unavailable"),
        ("qq", "text_only"),
        ("qq", "read_run_only"),
        ("weixin", None),
    ],
)
def test_matched_text_retries_same_submission_and_reuses_source_thread(
    platform, image_case, tmp_path, monkeypatch, published_agent
):
    from a13n_channels.channels.qq_content import QQInbound

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
        if platform == "qq":
            count = 5 if image_case == "too_many" else 4
            attachments = [
                {"kind": "image", "media_type": "image/png", "filename": f"photo-{index}.png"} for index in range(count)
            ]
            urls = [f"https://qq.example.com/photo-{index}.png?rkey=private" for index in range(count)]
            if image_case == "too_large":
                attachments[0]["size"] = delivery.INLINE_IMAGE_BYTES + 1
            attachments.insert(1, {"kind": "file", "media_type": "application/pdf", "filename": "document.pdf"})
            urls.insert(1, "https://qq.example.com/document.pdf?rkey=private")
            first = QQInbound.model_validate(
                {
                    **first.model_dump(),
                    "attachments": attachments,
                    "attachment_urls": urls,
                    "structured": {"record": True},
                }
            )
        await receive(first)
        await receive(first)
        await receive(incoming("second"))
        await receive(incoming("other-source", "other"))
        assert len(await store.query("SELECT * FROM channel_deliveries")) == 3

        posts = {}
        attempts = []
        access = object()
        runtime = SimpleNamespace(access=access, storage=object(), objects=object())
        owner_active = False
        downloads = []
        uploads = []
        created_assets = []

        @asynccontextmanager
        async def session(storage):
            assert storage is runtime.storage
            yield object()

        async def owner(_session, current_access, principal_id, *, confinement):
            assert current_access is access and principal_id == "owner"
            assert confinement.workspace_id == "workspace" and confinement.organization_id == "org"
            verbs = frozenset({"admin", "write", "read", "run"} if owner_active else {"read"})
            if owner_active and image_case == "read_run_only":
                verbs = frozenset({"read", "run"})
            return Principal(principal_id, "user", (Grant("org", None, verbs),), confinement)

        async def model(_session, _actor, _scope, key):
            assert key == "model"
            capabilities = [] if image_case == "text_only" else [ModelCapability.IMAGE_UNDERSTANDING]
            return SimpleNamespace(config=SimpleNamespace(characteristics=SimpleNamespace(capabilities=capabilities)))

        @asynccontextmanager
        async def download(url):
            downloads.append(url)
            status = 404 if image_case == "unavailable" and len(downloads) == 2 else 200
            yield httpx.Response(status, content=b"\x89PNG\r\n\x1a\npayload")

        async def stage(storage, objects, actor, workspace_id, *, request_key, filename, content_type, content):
            assert storage is runtime.storage and objects is runtime.objects
            assert workspace_id == "workspace" and actor.id == "owner"
            assert "write" in allowed_verbs(actor, actor.confinement)
            assert content_type == "image/png" and content.startswith(b"\x89PNG")
            uploads.append((request_key, filename))
            return SimpleNamespace(upload_id="upl_" + f"{len(uploads):032x}")

        async def create_asset(_storage, _actor, _workspace, body):
            asset = SimpleNamespace(id="ast_" + body.upload_id.removeprefix("upl_"))
            created_assets.append(asset.id)
            return asset, True

        def record(host, actor, workspace_id, message, path, key):
            assert host is runtime and actor.id == "owner" and workspace_id == "workspace"
            assert allowed_verbs(actor, actor.confinement) == {"read", "run"}
            assert message.agent_id == "ag_012345678901234567890123456789ab"
            assert message.delivery == "steer"
            assert message.options.overrides.plugins[0].plugin_key == "saurlax.channels.delivery"
            metadata, text = channel_input(message)
            assert text == f"prompt-{metadata['message_id']}"
            identifier = metadata["message_id"]
            if platform == "qq" and identifier == "first":
                assert 'Channel attachments: [{"index": 1' in message.payload.content[0].text
                assert 'Channel structure: {"record": true}' in message.payload.content[0].text
                assert "rkey=private" not in message.payload.content[0].text
            parts = message.payload.content[1:]
            if image_case == "inline" and identifier == "first":
                assert [part.asset_id for part in parts] == created_assets
                assert len(parts) == 4
            else:
                assert not parts
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
        monkeypatch.setattr(Deliveries, "has_context_activity", AsyncMock(return_value=True))
        monkeypatch.setattr(delivery, "principal_for", owner)
        monkeypatch.setattr(delivery, "resolve_model", model)
        monkeypatch.setattr(delivery, "open_attachment", download)
        monkeypatch.setattr(delivery.uploads, "stage", stage)
        monkeypatch.setattr(delivery.assets, "create_asset", create_asset)
        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.submit, "submit_message", append)
        monkeypatch.setattr(delivery.entries, "get_entry", read_entry)
        monkeypatch.setattr(delivery.runs, "get", read_run)

        deliveries = Deliveries(store, runtime, AsyncMock())
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
        assert len(downloads) == {"inline": 4, "unavailable": 2}.get(image_case, 0)
        assert all(".png?" in url for url in downloads)
        assert len(uploads) == (4 if image_case == "inline" else 0)
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
            name = channel_input(message)[0]["message_id"]
            target = thread_id or f"new-{name}"
            submissions[name] = target
            return SimpleNamespace(thread=SimpleNamespace(id=target), entry=SimpleNamespace(id=name)), True

        async def create(_runtime, _actor, _workspace, message, *, request_key):
            return accept(message)

        async def append(_runtime, _actor, _workspace, thread_id, message, *, request_key):
            return accept(message, thread_id)

        async def actor(_self, _principal, _account):
            return object()

        async def collect(_self, _job, _actor, _account):
            pass

        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.submit, "submit_message", append)
        monkeypatch.setattr(Deliveries, "actor", actor)
        monkeypatch.setattr(Deliveries, "collect", collect)
        monkeypatch.setattr(Deliveries, "has_context_activity", AsyncMock(return_value=True))
        deliveries = Deliveries(store, SimpleNamespace(storage=None), AsyncMock())
        await deliveries.process({"channel": account})
        assert len(submissions) == first_batch
        for _ in range(3):
            await deliveries.process({"channel": account})
        assert submissions == expected

    asyncio.run(exercise())


def test_managed_thread_rolls_over_after_idle_and_freezes_retry(tmp_path, monkeypatch, published_agent):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        route = rule("Route", "ag_012345678901234567890123456789ab", ("channel", "*", "*"))
        assert route.context_idle_seconds == 3600
        route = route.model_copy(update={"context_idle_seconds": 10})
        await store.save_route("workspace", "route", route, principal_id="owner", create=True)
        assert (await store.routes("workspace"))[0].context_idle_seconds == 10

        clock = SimpleNamespace(now=100)
        monkeypatch.setattr(delivery, "time", SimpleNamespace(time=lambda: clock.now))
        submissions = []

        async def create(_runtime, _actor, _workspace, message, *, request_key):
            name = channel_input(message)[0]["message_id"]
            submissions.append((name, None, request_key))
            if name == "expired" and len(submissions) == 4:
                raise ServiceError("unavailable", "Retry this submission.")
            return SimpleNamespace(thread=SimpleNamespace(id=f"new-{name}"), entry=SimpleNamespace(id=name)), True

        async def append(_runtime, _actor, _workspace, thread_id, message, *, request_key):
            name = channel_input(message)[0]["message_id"]
            submissions.append((name, thread_id, request_key))
            return SimpleNamespace(thread=SimpleNamespace(id=thread_id), entry=SimpleNamespace(id=name)), True

        monkeypatch.setattr(delivery.submit, "create_thread", create)
        monkeypatch.setattr(delivery.submit, "submit_message", append)
        monkeypatch.setattr(Deliveries, "actor", AsyncMock(return_value=object()))
        monkeypatch.setattr(Deliveries, "collect", AsyncMock())
        activity = AsyncMock(side_effect=[True, True, False])
        monkeypatch.setattr(Deliveries, "has_context_activity", activity)
        deliveries = Deliveries(store, SimpleNamespace(storage=None), AsyncMock())

        for name, now in [("first", 100), ("recent", 109), ("active", 119), ("expired", 129)]:
            clock.now = now
            await store.receive(incoming(name), "credentials")
            await deliveries.process({"channel": account})

        assert [(name, target) for name, target, _key in submissions] == [
            ("first", None),
            ("recent", "new-first"),
            ("active", "new-first"),
            ("expired", None),
        ]
        assert [call.args[2].timestamp() for call in activity.await_args_list] == [99, 109, 119]
        assert (await store.query("SELECT thread_id FROM channel_threads"))[0]["thread_id"] == "new-first"

        route = route.model_copy(update={"context_idle_seconds": 3600})
        await store.save_route("workspace", "route", route, principal_id="owner", create=False)
        clock.now = 159
        await deliveries.process({"channel": account})
        assert submissions[-1] == submissions[-2]
        assert (await store.query("SELECT thread_id FROM channel_threads"))[0]["thread_id"] == "new-expired"

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
        await Deliveries(store, None, AsyncMock()).advance(jobs[0], account, routes)
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
        deliveries = Deliveries(store, SimpleNamespace(storage=None), AsyncMock())
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
            assert channel_input(submitted[1][0])[1] == group.text
            assert submitted[1][1] != submitted[0][1]
            assert (await jobs())[0]["status"] == "submitted"

    asyncio.run(exercise())


def test_channel_tools_use_trusted_delivery_and_do_not_repeat_a_send(tmp_path, monkeypatch):
    import httpx
    from a13n_harness.configuration import RunConfiguration
    from pydantic_ai import ModelRetry

    from a13n_channels import tools
    from a13n_channels.channels.qq_content import QQInbound, QQMedia

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
        url = "https://qq.example.com/photo.png?rkey=private"
        source_message = QQInbound.model_validate(
            {
                **incoming("source").model_dump(),
                "attachments": [{"kind": "image", "media_type": "image/png", "filename": "photo.png"}],
                "attachment_urls": [url],
                "structured": {"record": True, "native_reference_id": "native-source"},
            }
        )
        assert await store.receive(source_message, "credentials")
        assert not await store.receive(source_message, "credentials")
        assert "rkey=private" not in (await store.query("SELECT payload FROM qq_messages"))[0]["payload"]
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
        keys = {"entry": "channels-trusted"}

        class Session:
            async def get(self, model, _id):
                assert model is tools.RunRow
                return run

            async def scalars(self, statement):
                if "entry" in statement.compile().params.values():
                    return [keys["entry"]] if "entry" in keys else []
                return list(keys.values())

        @asynccontextmanager
        async def session(_storage):
            yield Session()

        async def enabled_accounts():
            return {"channel": account}

        async def actor(principal_id, _account):
            return Principal(principal_id, "user", (Grant("org", "workspace", frozenset({"read", "run"})),))

        sent = []

        async def send_reply(source_account, message_id, text, **kwargs):
            assert source_account == account
            sent.append((message_id, text, kwargs) if kwargs else (message_id, text))
            return "platform-message"

        png = b"\x89PNG\r\n\x1a\npayload"
        downloads = []
        reads = []

        @asynccontextmanager
        async def download(address, authorize_url):
            authorize_url(address)
            downloads.append(address)
            yield httpx.Response(200, content=png)

        async def read_bytes_stream(path):
            reads.append(path)
            yield png

        monkeypatch.setattr(tools, "short_session", session)
        monkeypatch.setattr(tools, "open_attachment", download)
        factory = tools.ChannelToolFactory()
        factory.channels = SimpleNamespace(
            host=SimpleNamespace(storage=None),
            store=store,
            _enabled_accounts=enabled_accounts,
            deliveries=SimpleNamespace(actor=actor),
            send_reply=send_reply,
        )
        plugin = tools.ChannelTools("channels", factory)
        assert plugin.get_capabilities()
        ctx = SimpleNamespace(
            tool_call_id="call",
            deps=SimpleNamespace(
                configuration=RunConfiguration(),
                environment=SimpleNamespace(files=SimpleNamespace(read_bytes_stream=read_bytes_stream)),
                instance=SimpleNamespace(
                    identity=SimpleNamespace(issuer="a13n-service", subject="owner"),
                    agent_instance_id="run",
                    host_refs={"run_id": "run", "attempt_id": "attempt"},
                ),
            ),
        )
        assert [message["id"] for message in await plugin.channel_list_messages(ctx)] == ["source"]
        receipt = {"status": "sent", "message_id": "platform-message"}
        assert await plugin.channel_send_message(ctx, "reply") == receipt
        assert await plugin.channel_send_message(ctx, "reply") == receipt
        assert sent == [("source", "reply")]
        full_message = await plugin.channel_get_message(ctx, "source")
        assert full_message["structured"]["record"] and "qq_context" not in full_message
        assert full_message["attachments"][0]["filename"] == "photo.png"
        image = await plugin.channel_get_attachment(ctx, 1, "source")
        assert image.content[0].data == png and downloads == [url]
        with pytest.raises(ModelRetry, match="selected conversation"):
            await plugin.channel_get_attachment(ctx, 1, "other-chat", source_message_id="source")
        assert downloads == [url]
        ctx.tool_call_id = "rich"
        photo = QQMedia(type="media", path="/agent/photo.png")
        assert await plugin.channel_send_message(ctx, rich=photo, reference_message_id="source") == receipt
        assert await plugin.channel_send_message(ctx, rich=photo, reference_message_id="source") == receipt
        assert sent[-1] == ("source", None, {"rich": photo, "data": png, "reference_id": "native-source"})
        assert reads == ["/agent/photo.png"]
        assert not await store.query("SELECT text FROM channel_sends WHERE text LIKE '%photo.png%'")
        ctx.tool_call_id = "call"
        await store.query("UPDATE channel_deliveries SET tool_key = 'channels-second' WHERE message_id = 'other-chat'")
        keys["steered-entry"] = "channels-second"
        assert await plugin.channel_send_message(ctx, "reply") == receipt
        assert len(sent) == 2
        assert [message["id"] for message in await plugin.channel_list_messages(ctx, message_id="other-chat")] == [
            "other-chat"
        ]
        ctx.tool_call_id = "second-call"
        assert await plugin.channel_send_message(ctx, "second reply") == receipt
        assert sent[-1] == ("source", "second reply")
        keys["entry"] = "channels-fabricated"
        with pytest.raises(ModelRetry, match="no longer active"):
            await plugin.channel_send_message(ctx, "reply")

    asyncio.run(exercise())


def test_workspace_channel_tools_share_the_account_allowlist_for_reads_and_scheduled_sends(tmp_path, monkeypatch):
    import json

    import httpx
    from pydantic_ai import ModelRetry

    from a13n_channels import tools
    from a13n_channels.channels.qq import QQCreate

    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        for channel, workspace in (("channel", "workspace"), ("another", "workspace"), ("foreign", "other")):
            credentials = QQCreate(name=channel, app_id=channel, app_secret="test-secret")
            await store.query(
                "INSERT INTO qq_accounts VALUES (?, 'org', ?, ?, ?, ?, 1)",
                channel,
                workspace,
                channel,
                channel,
                store.encrypt("org", channel, credentials),
            )
        generation = (await store.query("SELECT app_secret FROM qq_accounts WHERE id = 'channel'"))[0]["app_secret"]
        await store.query(
            "INSERT INTO github_accounts VALUES ('gh', 'org', 'workspace', 'GitHub', 'github_app', 'app', 'secret', '')"
        )
        for message_id, conversation in (("first", "one"), ("other-chat", "two"), ("latest", "one")):
            await store.receive(incoming(message_id, conversation), generation)

        run = SimpleNamespace(
            id="run",
            status="running",
            current_attempt_id="attempt",
            principal_id="owner",
            organization_id="org",
            workspace_id="workspace",
            trigger="schedule",
            authority={
                "principal_id": "owner",
                "organization_id": "org",
                "workspace_id": "workspace",
                "verbs": ["read", "run"],
            },
        )
        organization = SimpleNamespace(settings={"app_plugins": {"enabled": [tools.FACTORY_KEY]}})

        class Session:
            async def get(self, model, _id):
                return {
                    tools.RunRow: run,
                    tools.OrganizationRow: organization,
                    tools.WorkspaceRow: SimpleNamespace(organization_id="org", archived_at=None),
                }[model]

        @asynccontextmanager
        async def session(_storage):
            yield Session()

        async def principal(_session, _access, principal_id, *, confinement):
            return Principal(
                principal_id, "user", (Grant("org", "workspace", frozenset({"read", "run"})),), confinement
            )

        messages = []

        def qq(request):
            if request.url.path == "/app/getAppAccessToken":
                return httpx.Response(200, json={"access_token": "test-token", "expires_in": 60})
            messages.append((request.url.path, json.loads(request.content)))
            return httpx.Response(200, json={"id": "sent-qq"})

        monkeypatch.setattr(tools, "short_session", session)
        monkeypatch.setattr(tools, "principal_for", principal)
        factory = tools.ChannelToolFactory()
        http = httpx.AsyncClient(transport=httpx.MockTransport(qq))
        factory.channels = SimpleNamespace(host=SimpleNamespace(storage=None, access=None), store=store, http=http)
        plugin = tools.WorkspaceChannelTools("channels", factory, frozenset({"channel", "foreign"}))
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
        all_channels = tools.WorkspaceChannelTools("channels", factory, frozenset())
        assert {row["id"] for row in await all_channels.channel_list_accounts(ctx)} == {"channel", "another", "gh"}
        assert await all_channels.channel_list_conversations(ctx, "gh") == []
        assert (await all_channels.channel_send_proactive(ctx, "gh", "private", "one", "alert"))["status"] == (
            "unsupported"
        )
        run.authority["verbs"] = ["read"]
        with pytest.raises(ModelRetry, match="delegation"):
            await all_channels.channel_send_proactive(ctx, "channel", "private", "one", "alert")
        run.authority["verbs"] = ["read", "run"]
        for channel in ("another", "foreign"):
            with pytest.raises(ModelRetry, match="available channel"):
                await plugin.channel_send_proactive(ctx, channel, "private", "unseen-recipient", "alert")
        assert await plugin.channel_send_proactive(ctx, "channel", "private", "unseen-recipient", "alert") == {
            "status": "sent",
            "message_id": "sent-qq",
        }
        assert await all_channels.channel_send_proactive(ctx, "another", "group", "group-id", "alert") == {
            "status": "sent",
            "message_id": "sent-qq",
        }
        assert messages == [
            ("/v2/users/unseen-recipient/messages", {"content": "alert", "msg_type": 0}),
            ("/v2/groups/group-id/messages", {"content": "alert", "msg_type": 0}),
        ]
        organization.settings = {}
        with pytest.raises(ModelRetry, match="unavailable"):
            await all_channels.channel_list_accounts(ctx)
        with pytest.raises(ModelRetry, match="unavailable"):
            await all_channels.channel_send_proactive(ctx, "channel", "private", "one", "alert")
        await http.aclose()

    asyncio.run(exercise())


def test_repository_sources_and_account_allowlists_round_trip(tmp_path):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        with store.transaction() as db:
            db.execute(
                "INSERT INTO github_accounts VALUES ('gh', 'org', 'workspace', 'GitHub', 'github_app', 'bot', '{}', '')"
            )
            db.execute("INSERT INTO qq_accounts VALUES ('qq', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)")
        body = RouteInput(
            name="Repository and group",
            agent_id="agent",
            sources=[
                Source(channel_id="gh", kind="*", conversation_id="*", repository="Owner/Repo"),
                Source(channel_id="qq", kind="group", conversation_id="*"),
            ],
            allowed_users={"gh": ["Alice"], "qq": ["qq-sender"]},
        )
        saved = await store.save_route("workspace", "repo", body, principal_id="owner", create=True)
        routes = await store.routes("workspace")
        assert routes == [saved]
        github_account = (await store.query("SELECT * FROM github_accounts"))[0]
        assert await store.github_repositories(github_account) == {"owner/repo"}
        assert select_route(routes, "gh", "issue", "owner/repo#7", sender_id="alice") == saved
        assert select_route(routes, "gh", "pull_request", "OWNER/REPO#8", sender_id="ALICE") == saved
        assert select_route(routes, "gh", "issue", "owner/other#7", sender_id="alice") is None
        assert select_route(routes, "gh", "issue", "owner/repo#7", sender_id="bob") is None
        assert select_route(routes, "qq", "group", "room", sender_id="qq-sender") == saved
        assert select_route(routes, "qq", "group", "room", sender_id="alice") is None
        with pytest.raises(ServiceError, match="already belongs"):
            duplicate = body.model_copy(
                update={"sources": [Source(channel_id="gh", kind="*", conversation_id="*", repository="owner/repo")]}
            )
            await store.save_route("workspace", "duplicate", duplicate, principal_id="owner", create=True)
        with pytest.raises(ServiceError, match="allowed users"):
            await store.save_route(
                "workspace", "repo", body.model_copy(update={"allowed_users": {}}), principal_id="owner", create=False
            )

    asyncio.run(exercise())


def test_obsolete_github_route_cannot_deliver_or_be_enabled(tmp_path):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        config = {
            "sources": [{"channel_id": "*", "kind": "issue", "conversation_id": "*"}],
            "github_filter": {"repositories": ["owner/repo"], "users": ["Alice"]},
        }
        with store.transaction() as db:
            db.execute(
                "INSERT INTO channel_routes VALUES ('legacy', 'workspace', 'Legacy', 'agent', 1, ?, 'binding', 'owner')",
                (json.dumps(config),),
            )
        routes = await store.routes("workspace")
        assert select_route(routes, "gh", "issue", "owner/repo#7", sender_id="alice") is None
        (before,) = routes
        with pytest.raises(ServiceError, match="allowed users"):
            body = RouteInput.model_validate(before.model_dump())
            await store.save_route("workspace", "legacy", body, principal_id="owner", create=False)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("status", "resumed", "send_error", "outcome"),
    [
        ("failed", False, None, "sent"),
        ("failed", True, "unavailable", "unknown"),
        ("cancelled", True, "invalid_argument", "failed"),
    ],
)
def test_terminal_run_notifies_only_its_trigger_once(tmp_path, monkeypatch, status, resumed, send_error, outcome):
    async def exercise():
        store = store_for(tmp_path)
        await store.initialize()
        await store.query(
            "INSERT INTO qq_accounts VALUES ('channel', 'org', 'workspace', 'Bot', 'bot', 'credentials', 1)"
        )
        account = (await store.query("SELECT *, 'qq' AS platform FROM qq_accounts"))[0]
        await store.save_route(
            "workspace", "route", rule("Default", "agent", ("*", "*", "*")), principal_id="owner", create=True
        )
        for name in ("trigger", "steering", "wake"):
            await store.receive(incoming(name, conversation=name), "credentials")
        await store.query(
            """UPDATE channel_deliveries SET entry_id = message_id, run_id = ?,
               status = CASE WHEN message_id = 'wake' THEN 'wake_submitted' ELSE 'submitted' END""",
            "successor" if resumed else "original",
        )
        origin = SimpleNamespace(
            trigger="message",
            source_entry_id="trigger",
            agent_id="agent",
            principal_id="owner",
            failure=SimpleNamespace(code="model_recovery_exhausted", message="private-token HTTP 429"),
            status=status,
            output="private partial answer",
        )

        async def get_run(_storage, _actor, _workspace, run_id):
            if run_id == "successor":
                return SimpleNamespace(
                    trigger="resume", parent_run_id="original", status=status, failure=origin.failure
                )
            assert run_id == "original"
            return origin

        send = AsyncMock(
            return_value="receipt", side_effect=ServiceError(send_error, "private") if send_error else None
        )
        monkeypatch.setattr(Deliveries, "actor", AsyncMock(return_value=SimpleNamespace(id="owner")))
        monkeypatch.setattr(delivery.runs, "get", get_run)
        runtime = SimpleNamespace(storage=None)
        worker = Deliveries(store, runtime, send)
        accounts = {"channel": account}
        await asyncio.gather(worker.process(accounts), worker.process(accounts))
        await Deliveries(store_for(tmp_path), runtime, send).process(accounts)
        text = (
            "Agent request was cancelled."
            if status == "cancelled"
            else "Agent request failed: service rate limit or quota exhausted (HTTP 429). Please try again later."
        )
        send.assert_awaited_once_with(account, "trigger", text)
        assert [tuple(row) for row in await store.query("SELECT status, text, message_id FROM channel_sends")] == [
            (outcome, text, None if send_error else "receipt")
        ]
        assert [
            tuple(row) for row in await store.query("SELECT status, error FROM channel_deliveries ORDER BY message_id")
        ] == [("failed", f"run:{status}"), ("failed", f"run:{status}"), ("failed", f"wake:{status}")]

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("code", "detail", "expected"),
    [
        ("unknown", "HTTP 403", "authorization failed"),
        ("unknown", "HTTP 503", "upstream service is unavailable"),
        ("configuration_unavailable", "", "configuration is unavailable"),
        ("model_recovery_exhausted", "", "model execution could not recover"),
        ("private-code", "", "Agent request failed."),
    ],
)
def test_failure_notice_only_publishes_fixed_diagnostics(code, detail, expected):
    failure = delivery.Failure(code=code, message=f"private prompt and token {detail}")
    notice = delivery.failure_notice("failed", failure)
    assert expected in notice
    assert "private" not in notice
