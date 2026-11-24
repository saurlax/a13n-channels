"""Contracts for independently configured outbound channel connections."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

import httpx

from a13n_channels.messages import ChannelMessage

type Configuration = Mapping[str, str]


class Ingest(Protocol):
    """Commit an optional message and cursor before acknowledging the upstream event."""

    async def __call__(
        self,
        message: ChannelMessage | None,
        *,
        cursor: str | None = None,
        context: dict[str, str] | None = None,
    ) -> None: ...


class Connection(Protocol):
    """An account-scoped listener and its source-reply transport."""

    async def listen(self, cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None: ...

    async def reply(self, message: ChannelMessage, text: str, context: Mapping[str, str]) -> str: ...


class ChannelError(Exception):
    """A safe operator-facing message about an upstream failure."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True)
class CredentialField:
    key: str
    label: str
    label_zh: str
    secret: bool = True
    required: bool = True


@dataclass(frozen=True)
class Adapter:
    key: str
    label: str
    label_zh: str
    kinds: tuple[str, ...]
    fields: tuple[CredentialField, ...]
    bind: Callable[[httpx.AsyncClient, Configuration], Awaitable[str]]
    connect: Callable[[httpx.AsyncClient, str, str, Configuration], Connection]
    migrate_config: Callable[[Configuration], dict[str, str]] | None = None
