"""Poll GitHub issue and pull request activity under a user or installation identity."""

import asyncio
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

import httpx
import jwt
from pydantic import BaseModel, Field, SecretStr, StringConstraints, model_validator

from a13n_channels.messages import ChannelMessage

GITHUB_API = "https://api.github.com"
GITHUB_KINDS = ("issue", "pull_request")
type GitHubPlatform = Literal["github", "github_app"]
type RepositoryName = Annotated[str, StringConstraints(max_length=150, pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$")]
LOGIN_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9-]{0,38}$"
type Login = Annotated[str, StringConstraints(pattern=LOGIN_PATTERN)]


def stamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class GitHubCredentials(BaseModel, extra="forbid"):
    token: Annotated[SecretStr, Field(max_length=1024)] | None = None
    app_id: Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]{0,19}$")] | None = None
    installation_id: Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]{0,19}$")] | None = None
    private_key: Annotated[SecretStr, Field(max_length=16384)] | None = None

    @model_validator(mode="after")
    def credentials(self):
        app_fields = (self.app_id, self.installation_id, self.private_key)
        if self.token and not any(app_fields):
            if not self.token.get_secret_value().startswith("ghp_"):
                raise ValueError("Use a classic GitHub personal access token.")
            return self
        if not self.token and all(app_fields):
            return self
        raise ValueError("Supply a classic token or App ID, installation ID and private key.")


class GitHubBind(GitHubCredentials):
    name: str | None = Field(default=None, min_length=1, max_length=80)


class Repository(BaseModel):
    id: int
    full_name: RepositoryName


class GitHubBinding(GitHubCredentials):
    identity: str
    login: str


class GitHubConnection:
    def __init__(self, http: httpx.AsyncClient, channel_id: str, credentials: GitHubCredentials):
        self.http = http
        self.channel_id = channel_id
        self.credentials = credentials
        self.installation_token = ""
        self.expires = 0.0
        self.interval = 60

    def app_jwt(self) -> str:
        now = int(time.time())
        return jwt.encode(
            {"iss": self.credentials.app_id, "iat": now - 30, "exp": now + 540},
            self.credentials.private_key.get_secret_value(),
            algorithm="RS256",
        )

    async def token(self) -> str:
        if self.credentials.token:
            return self.credentials.token.get_secret_value()
        if time.time() >= self.expires - 60:
            result = await self.request(
                "POST", f"/app/installations/{self.credentials.installation_id}/access_tokens", token=self.app_jwt()
            )
            self.expires = datetime.fromisoformat(result["expires_at"]).timestamp()
            self.installation_token = result["token"]
        return self.installation_token

    async def request(self, method, path, *, token=None, params=None, body=None, response=False):
        result = await self.http.request(
            method,
            GITHUB_API + path,
            params=params,
            json=body,
            headers={
                "Authorization": f"Bearer {token or await self.token()}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        result.raise_for_status()
        if value := result.headers.get("X-Poll-Interval"):
            self.interval = max(self.interval, int(value))
        return result if response else result.json()

    async def pages(self, path, *, params=None, collection=None):
        for page in range(1, 21):
            result = await self.request(
                "GET", path, params={**(params or {}), "per_page": 100, "page": page}, response=True
            )
            data = result.json()
            for item in data[collection] if collection else data:
                yield item
            if "next" not in result.links:
                return
        raise ValueError("GitHub activity exceeded the pagination limit.")

    async def bind(self, body: GitHubBind) -> GitHubBinding:
        if body.token:
            identity = await self.request("GET", "/user")
            await self.request("GET", "/notifications", params={"per_page": 1})
            login = identity["login"]
            identity_id = str(identity["id"])
        else:
            app = await self.request("GET", "/app", token=self.app_jwt())
            await self.token()
            login = app["slug"] + "[bot]"
            identity_id = f"{app['id']}:{body.installation_id}"
        return GitHubBinding(
            **body.model_dump(exclude={"name"}),
            login=login,
            identity=identity_id,
        )

    def project(self, binding, repo, issue, obj, event, cursor) -> ChannelMessage | None:
        author = obj.get("user") or {}
        text = obj.get("body") or ""
        created = obj.get("submitted_at") or obj.get("created_at")
        if not created or created < cursor or not text.strip() or not author.get("id") or not author.get("login"):
            return None
        if author.get("type") == "Bot" or author.get("login", "").casefold() == binding.login.casefold():
            return None
        login = re.escape(binding.login.removesuffix("[bot]"))
        addressed = bool(re.search(rf"(?<![\w@-])@{login}(?:\[bot\])?(?![\w-])", text, re.IGNORECASE))
        return ChannelMessage(
            id=f"{repo.id}:{event}:{obj['id']}",
            channel_id=self.channel_id,
            kind="pull_request" if "pull_request" in issue else "issue",
            conversation_id=f"{repo.full_name}#{issue['number']}",
            sender_id=author["login"],
            text=text,
            timestamp=created,
            bot_mentioned=addressed,
        )

    async def scan_thread(self, binding, repo, issue, cursor):
        if message := self.project(binding, repo, issue, issue, "opened", cursor):
            yield message
        number = issue["number"]
        endpoints = [(f"/issues/{number}/comments", "comment", {"since": cursor})]
        if "pull_request" in issue:
            endpoints += [
                (f"/pulls/{number}/comments", "inline", {"since": cursor}),
                (f"/pulls/{number}/reviews", "review", {}),
            ]
        for path, event, params in endpoints:
            async for item in self.pages(f"/repos/{repo.full_name}{path}", params=params):
                if message := self.project(binding, repo, issue, item, event, cursor):
                    yield message

        if "pull_request" in issue:
            pull = await self.request("GET", f"/repos/{repo.full_name}/pulls/{number}")
            if any(
                user["login"].casefold() == binding.login.casefold() for user in pull.get("requested_reviewers", [])
            ):
                async for item in self.pages(f"/repos/{repo.full_name}/issues/{number}/timeline"):
                    reviewer = item.get("requested_reviewer") or {}
                    if (
                        item.get("event") != "review_requested"
                        or reviewer.get("login", "").casefold() != binding.login.casefold()
                    ):
                        continue
                    if message := self.project(
                        binding,
                        repo,
                        issue,
                        {**item, "user": item.get("actor"), "body": "Review requested for this pull request."},
                        "review_requested",
                        cursor,
                    ):
                        yield message.model_copy(update={"bot_mentioned": True})

    async def verify_repository(self, repo: Repository) -> None:
        current = await self.request("GET", f"/repos/{repo.full_name}")
        if current["id"] != repo.id:
            raise ValueError("The configured GitHub repository changed identity.")

    async def poll(self, binding: GitHubBinding, cursor: str, repositories: set[str]) -> AsyncIterator[ChannelMessage]:

        if binding.token:
            async for notice in self.pages("/notifications", params={"all": "true", "since": cursor}):
                repo = Repository.model_validate(notice["repository"])
                subject = notice["subject"]
                if repo.full_name.lower() not in repositories or subject["type"] not in {"Issue", "PullRequest"}:
                    continue
                match = re.fullmatch(
                    rf"{re.escape(GITHUB_API)}/repos/{re.escape(repo.full_name)}/(?:issues|pulls)/([1-9][0-9]*)",
                    subject["url"] or "",
                    re.IGNORECASE,
                )
                if match:
                    await self.verify_repository(repo)
                    issue = await self.request("GET", f"/repos/{repo.full_name}/issues/{match[1]}")
                    async for message in self.scan_thread(binding, repo, issue, cursor):
                        yield message
        else:
            async for item in self.pages("/installation/repositories", collection="repositories"):
                repo = Repository.model_validate(item)
                if repo.full_name.lower() not in repositories:
                    continue
                await self.verify_repository(repo)
                async for issue in self.pages(
                    f"/repos/{repo.full_name}/issues", params={"state": "all", "since": cursor}
                ):
                    async for message in self.scan_thread(binding, repo, issue, cursor):
                        yield message

    async def listen(
        self,
        binding: GitHubBinding,
        cursor: str,
        repositories: Callable[[], Awaitable[set[str]]],
        store: Callable[..., Awaitable],
        status: Callable,
    ):
        while True:
            try:
                cutoff = max(cursor, (datetime.now(UTC) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                if selected := await repositories():
                    async for message in self.poll(binding, cursor, selected):
                        await store(message, None)
                await store(None, cutoff)
                cursor = cutoff
                status("connected")
                await asyncio.sleep(self.interval)
            except (httpx.HTTPError, jwt.PyJWTError, ValueError, KeyError, TypeError) as error:
                status(
                    "login_required"
                    if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 401
                    else "error"
                )
                delay = self.interval
                if isinstance(error, httpx.HTTPStatusError):
                    headers = error.response.headers
                    retry = headers.get("Retry-After", "")
                    reset = headers.get("X-RateLimit-Reset", "")
                    if retry.isdigit():
                        delay = max(delay, int(retry))
                    if headers.get("X-RateLimit-Remaining") == "0" and reset.isdigit():
                        delay = max(delay, int(reset) - time.time())
                await asyncio.sleep(delay)

    async def reply(self, incoming: ChannelMessage, text: str) -> str:
        repo, _, number = incoming.conversation_id.rpartition("#")
        identity = incoming.id.split(":", 1)[0]
        if not number.isdecimal() or not identity.isdecimal():
            raise ValueError("The source repository is unavailable.")
        await self.verify_repository(Repository(id=int(identity), full_name=repo))
        receipt = await self.request("POST", f"/repos/{repo}/issues/{number}/comments", body={"body": text})
        if type(receipt.get("id")) is not int:
            raise ValueError("GitHub omitted the comment ID.")
        return str(receipt["id"])
