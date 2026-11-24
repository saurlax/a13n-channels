"""Use QQ's QR binding flow and retain credentials only until the initiating actor confirms."""

import asyncio
import base64
import secrets
from typing import Annotated, Literal
from urllib.parse import urlencode

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import BaseModel, ConfigDict, Field

from a13n_channels.channels.qq import AppId, ChannelId, QQSecret
from a13n_channels.qr import qr_image

QQ_BIND_ORIGIN = "https://q.qq.com"
QQ_BIND_POLL_SECONDS = 2

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


async def binding_data(http: httpx.AsyncClient, path: str, body: dict[str, str]) -> dict[str, object]:
    response = await http.post(f"{QQ_BIND_ORIGIN}/lite/{path}", json=body, timeout=10)
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or result.get("retcode") != 0 or not isinstance(result.get("data"), dict):
        raise ValueError("QQ could not complete the binding request.")
    return result["data"]


class QQScan:
    def __init__(self, http: httpx.AsyncClient):
        self.login_id = secrets.token_hex(16)
        self.expires = asyncio.get_running_loop().time() + 300
        self.image: str | None = None
        self.credentials: QQScanCredentials | None = None
        self.ready = asyncio.Event()
        self.lock = asyncio.Lock()
        self.task = asyncio.create_task(self._run(http), name="qq-channel-pairing")

    def status(self) -> QQScanStatus:
        if self.credentials is not None:
            return QQScanStatus(status="confirmed", login_id=self.login_id)
        if self.task.done():
            return QQScanStatus(status="failed", login_id=self.login_id)
        return QQScanStatus(status="wait", login_id=self.login_id, qr_image=self.image)

    async def _run(self, http: httpx.AsyncClient) -> None:
        try:
            async with asyncio.timeout_at(self.expires):
                while True:
                    key = AESGCM.generate_key(bit_length=256)
                    task = await binding_data(http, "create_bind_task", {"key": base64.b64encode(key).decode("ascii")})
                    task_id = task.get("task_id")
                    if not isinstance(task_id, str) or not task_id:
                        raise ValueError("QQ did not provide a binding task ID.")
                    query = urlencode({"task_id": task_id, "source": "", "_wv": 2})
                    self.image = qr_image(f"{QQ_BIND_ORIGIN}/qqbot/openclaw/connect.html?{query}")
                    self.ready.set()

                    while True:
                        await asyncio.sleep(QQ_BIND_POLL_SECONDS)
                        try:
                            result = await binding_data(http, "poll_bind_result", {"task_id": task_id})
                        except httpx.TransportError:
                            continue
                        match result.get("status", 0):
                            case 0 | 1:
                                continue
                            case 3:
                                break
                            case 2:
                                app_id = result["bot_appid"]
                                if type(app_id) not in (str, int):
                                    raise ValueError("QQ did not provide a bot AppID.")
                                encrypted = base64.b64decode(result["bot_encrypt_secret"], validate=True)
                                # QQ sends a 12-byte nonce followed by the ciphertext and GCM tag.
                                secret = AESGCM(key).decrypt(encrypted[:12], encrypted[12:], None)
                                self.credentials = QQScanCredentials(app_id=str(app_id), app_secret=secret.decode())
                                return
                            case _:
                                raise ValueError("QQ returned an unknown binding status.")
        except (httpx.HTTPError, TimeoutError, InvalidTag, ValueError, KeyError, TypeError):
            # Responses may contain binding credentials. Keep errors generic.
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
