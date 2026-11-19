import asyncio
import json

import httpx
import pytest
from a13n_service.infra.errors import ServiceError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from a13n_channels.github import GitHubBind, GitHubConnection
from a13n_channels.routing import Route, Source, select_route, validate_route

CURSOR = "2026-11-19T01:00:00Z"
AUTHOR = {"id": 7, "login": "caller", "type": "User"}


@pytest.mark.parametrize("app", [False, True])
def test_github_bind_poll_and_reply(app):
    async def exercise():
        credentials = {"token": "ghp_fake"}
        if app:
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            credentials = {
                "app_id": "12",
                "installation_id": "34",
                "private_key": key.private_bytes(
                    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
                ).decode(),
            }
        body = GitHubBind(name="GitHub", **credentials)
        repo = {"id": 1, "full_name": "owner/repo"}
        issue = {"id": 2, "number": 42, "pull_request": {}, "created_at": CURSOR, "user": AUTHOR, "body": ""}
        posted = []

        def request(req):
            assert req.url.host == "api.github.com"
            path = req.url.path
            if path in {"/app", "/app/installations/34/access_tokens"}:
                assert app and req.headers["Authorization"].startswith("Bearer ey")
                value = (
                    {"id": 12, "slug": "channel"}
                    if path == "/app"
                    else {"token": "installation-token", "expires_at": "2099-01-01T00:00:00Z"}
                )
                return httpx.Response(200, json=value)
            assert req.headers["Authorization"] == ("Bearer installation-token" if app else "Bearer ghp_fake")
            if req.method == "POST":
                assert path == "/repos/owner/repo/issues/42/comments"
                posted.append(json.loads(req.content))
                return httpx.Response(201, json={"id": 123})
            if path == "/notifications":
                assert not app
                return httpx.Response(
                    200,
                    headers={"X-Poll-Interval": "90"},
                    json=[
                        {
                            "repository": repo,
                            "subject": {
                                "type": "PullRequest",
                                "url": "https://api.github.com/repos/owner/repo/pulls/42",
                            },
                        }
                    ],
                )
            values = {
                "/user": {"id": 9, "login": "channel"},
                "/installation/repositories": {"repositories": [repo]},
                "/users/caller": AUTHOR,
                "/repos/owner/repo": repo,
                "/repos/owner/repo/issues": [issue],
                "/repos/owner/repo/issues/42": issue,
                "/repos/owner/repo/issues/42/comments": [
                    {"id": 3, "created_at": CURSOR, "user": AUTHOR, "body": "@channel hello"},
                    {
                        "id": 4,
                        "created_at": CURSOR,
                        "user": {"id": 8, "login": "channel[bot]" if app else "channel"},
                        "body": "self",
                    },
                ],
                "/repos/owner/repo/pulls/42": {},
                "/repos/owner/repo/pulls/42/comments": [],
                "/repos/owner/repo/pulls/42/reviews": [],
            }
            return httpx.Response(200, json=values[path])

        async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as http:
            connection = GitHubConnection(http, "gh_channel", body)
            binding = await connection.bind(body)
            messages = [item async for item in connection.poll(binding, CURSOR, {"owner/repo"})]
            assert len(messages) == 1
            message = messages[0]
            assert (message.kind, message.conversation_id, message.bot_mentioned) == (
                "pull_request",
                "owner/repo#42",
                True,
            )
            assert await connection.reply(message, "reply") == "123"
            assert posted == [{"body": "reply"}]

    asyncio.run(exercise())


def test_github_route_filter_requires_both_repository_and_user():
    source = Source(
        channel_id="github-test",
        kind="issue",
        conversation_id="*",
        github_filter={"repositories": ["Owner/Repo"], "users": ["Alice"]},
    )
    route = Route(
        id="test",
        binding_id="binding",
        principal_id="owner",
        name="Test",
        agent_id="agent",
        sources=[source],
    )
    other = route.model_copy(
        update={
            "id": "other",
            "sources": [
                source.model_copy(update={"github_filter": source.github_filter.model_copy(update={"users": ["bob"]})})
            ],
        }
    )
    validate_route(other, [route], other.id, {"github-test"})
    combined = route.model_copy(update={"sources": [source, *other.sources]})
    validate_route(combined, [], combined.id, {"github-test"})
    assert select_route([combined], "github-test", "issue", "owner/repo#7", sender_id="bob") == combined
    with pytest.raises(ServiceError, match="already belongs"):
        validate_route(route, [route], "duplicate", {"github-test"})
    assert select_route([route, other], "github-test", "issue", "owner/repo#7", sender_id="ALICE") == route
    assert select_route([route, other], "github-test", "issue", "owner/repo#7", sender_id="bob") == other
    assert select_route([route], "github-test", "issue", "owner/other#7", sender_id="alice") is None
    assert select_route([route], "github-test", "issue", "owner/repo#7", sender_id="unknown") is None
    assert (
        select_route(
            [route.model_copy(update={"sources": [source.model_copy(update={"github_filter": None})]})],
            "github-test",
            "issue",
            "owner/repo#7",
            sender_id="alice",
        )
        is None
    )
