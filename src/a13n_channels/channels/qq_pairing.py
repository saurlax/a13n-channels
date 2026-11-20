"""Run Tencent's QQ QR Connector and retain credentials only until the initiating actor confirms."""

import asyncio
import json
import secrets
from contextlib import suppress
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from a13n_channels.channels.qq import AppId, ChannelId, QQSecret
from a13n_channels.qr import qr_image

type LoginId = Annotated[str, Field(pattern=r"^[a-f0-9]{32}$")]
type PairingScope = tuple[str, str]


class QQScanSave(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=80)
    channel_id: ChannelId | None = None


class QQScanCredentials(QQSecret):
    app_id: AppId


class QQScanStatus(BaseModel):
    status: Literal["wait", "confirmed", "expired", "failed"]
    login_id: str
    qr_image: str | None = None


class QQScan:
    def __init__(self, connector: Path):
        self.login_id = secrets.token_hex(16)
        self.expires = asyncio.get_running_loop().time() + 300
        self.image: str | None = None
        self.credentials: QQScanCredentials | None = None
        self.ready = asyncio.Event()
        self.lock = asyncio.Lock()
        self.task = asyncio.create_task(self._run(connector), name="qq-channel-pairing")

    def status(self) -> QQScanStatus:
        if self.credentials is not None:
            return QQScanStatus(status="confirmed", login_id=self.login_id)
        if self.task.done():
            return QQScanStatus(status="failed", login_id=self.login_id)
        return QQScanStatus(status="wait", login_id=self.login_id, qr_image=self.image)

    async def _run(self, connector: Path) -> None:
        try:
            async with asyncio.timeout_at(self.expires):
                process = await asyncio.create_subprocess_exec(
                    "node",
                    str(Path(__file__).with_name("qq_scan.mjs")),
                    str(connector),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                try:
                    assert process.stdout is not None
                    while line := await process.stdout.readline():
                        event = json.loads(line)
                        match event["kind"]:
                            case "qr":
                                url = urlsplit(event["url"])
                                if url.scheme != "https" or url.netloc != "q.qq.com":
                                    return
                                self.image = qr_image(event["url"])
                                self.ready.set()
                            case "confirmed":
                                self.credentials = QQScanCredentials(
                                    app_id=event["appId"], app_secret=event["appSecret"]
                                )
                                return
                            case "failed":
                                return
                finally:
                    if process.returncode is None:
                        with suppress(ProcessLookupError):
                            process.kill()
                    await process.wait()
        except (OSError, ValueError, KeyError, TypeError):
            # The SDK stream can contain credentials. Keep errors generic.
            return
        finally:
            self.ready.set()

    async def close(self) -> None:
        self.credentials = None
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


class QQScans:
    def __init__(self) -> None:
        self.sessions: dict[PairingScope, QQScan] = {}

    def find(self, scope: PairingScope, login_id: str) -> QQScan | None:
        login = self.sessions.get(scope)
        if login and login.login_id == login_id and login.expires > asyncio.get_running_loop().time():
            return login
        return None

    async def expire(self) -> None:
        now = asyncio.get_running_loop().time()
        for scope, login in tuple(self.sessions.items()):
            if login.expires <= now:
                await self.cancel(scope, login)

    async def cancel(self, scope: PairingScope, login: QQScan) -> None:
        if self.sessions.get(scope) is login:
            self.sessions.pop(scope)
            await login.close()

    async def close(self) -> None:
        sessions = tuple(self.sessions.values())
        self.sessions.clear()
        await asyncio.gather(*(login.close() for login in sessions))
