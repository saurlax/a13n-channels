"""MQTT 3.1.1 text delivery over verified TLS."""

import asyncio
import hashlib
import ipaddress
import json
import re
import socket
import ssl
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, unquote, urlsplit
from uuid import uuid4

import httpx

from a13n_channels.channels.adapter import Adapter, ChannelError, Configuration, CredentialField, Ingest
from a13n_channels.messages import ChannelMessage

_IO_TIMEOUT = 10
_KEEPALIVE = 30
_MAX_PACKET = 128 * 1024
_MAX_PAYLOAD = 64 * 1024
_PACKET_ID = b"\x00\x01"
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_MESSAGE_ID = re.compile(r"[a-zA-Z0-9_.:-]{1,128}")
_REPLY_ID = re.compile(r"a13n-reply-[0-9a-f]{32}")
_NAT64 = ipaddress.IPv6Network("64:ff9b::/96")


def _valid_topic(topic: str) -> bool:
    return (
        1 <= len(topic) <= 512 and topic == topic.strip() and topic != "*" and all(char.isprintable() for char in topic)
    )


def valid_publish_topic(topic: str) -> bool:
    return _valid_topic(topic) and not any(wildcard in topic for wildcard in "+#")


def valid_subscription_filter(topic_filter: str) -> bool:
    levels = topic_filter.split("/")
    return _valid_topic(topic_filter) and all(
        ("+" not in level or level == "+") and ("#" not in level or (level == "#" and i == len(levels) - 1))
        for i, level in enumerate(levels)
    )


def matches_topic(topic_filter: str, topic: str) -> bool:
    if topic.startswith("$") and topic_filter.startswith(("#", "+")):
        return False
    levels = topic.split("/")
    for index, pattern in enumerate(topic_filter.split("/")):
        if pattern == "#":
            return True
        if index >= len(levels) or pattern not in {"+", levels[index]}:
            return False
    return len(levels) == topic_filter.count("/") + 1


def migrate_config(config: Configuration) -> dict[str, str]:
    if "host" not in config:
        return dict(config)
    host = config["host"]
    port = config.get("port", "").strip()
    username = quote(config.get("username", ""), safe="")
    password = quote(config.get("password", ""), safe="")
    migrated = {key: value for key, value in config.items() if key not in {"host", "port", "username", "password"}}
    address = f"{host}:{port}" if port else host
    migrated.setdefault("broker_url", f"mqtts://{username}:{password}@{address}")
    return migrated


@dataclass(frozen=True, repr=False)
class Broker:
    host: str
    port: int
    username: str
    password: str
    topic_filter: str
    reply_topic: str

    @classmethod
    def from_config(cls, config: Configuration) -> "Broker":
        address = migrate_config(config).get("broker_url", "")
        try:
            parsed = urlsplit(address)
            if (
                any(char.isspace() or ord(char) < 32 or char == "\\" for char in address)
                or parsed.scheme != "mqtts"
                or not parsed.hostname
                or parsed.netloc.count("@") != 1
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
                or re.search(r"%(?![a-fA-F0-9]{2})", parsed.netloc)
            ):
                raise ValueError
            port = 8883 if parsed.port is None else parsed.port
            if port < 1:
                raise ValueError
            username = unquote(parsed.username or "", errors="strict")
            password = unquote(parsed.password or "", errors="strict")
        except ValueError:
            raise ChannelError("Enter a valid mqtts://username:password@host:port broker URL.") from None

        hostname = parsed.hostname.rstrip(".")
        try:
            host = hostname.encode("idna").decode("ascii").lower()
        except UnicodeError:
            raise ChannelError("Enter a public MQTT broker hostname.") from None
        labels = host.split(".")
        if len(host) > 253 or len(labels) < 2 or labels[-1].isdigit() or not all(_LABEL.fullmatch(x) for x in labels):
            raise ChannelError("Enter a public MQTT broker hostname.")

        if not username or not password or "\0" in username or len(username) > 512 or len(password) > 2048:
            raise ChannelError("Include the MQTT username and password in the broker URL.")

        topic_filter = config.get("topic_filter", "").strip()
        reply_topic = config.get("reply_topic", "").strip()
        if topic_filter and not valid_subscription_filter(topic_filter):
            raise ChannelError("Enter a valid MQTT subscription filter.")
        if reply_topic and not valid_publish_topic(reply_topic):
            raise ChannelError("Enter a valid MQTT reply topic.")
        return cls(host, port, username, password, topic_filter, reply_topic)


def _public_ip(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    if isinstance(ip, ipaddress.IPv6Address):
        if ip in _NAT64 or ip.sixtofour or ip.teredo or ip.is_site_local or ip.scope_id:
            return False
        ip = ip.ipv4_mapped or ip
    return ip.is_global and not (ip.is_multicast or ip.is_reserved)


async def _dial(broker: Broker) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo(broker.host, broker.port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
    addresses = list(dict.fromkeys((family, addr[0]) for family, _, _, _, addr in results))
    if not addresses or any(not _public_ip(addr) for _, addr in addresses):
        raise ChannelError("The MQTT broker must resolve only to public IP addresses.")

    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    for family, address in addresses:
        try:
            async with asyncio.timeout(4):
                return await asyncio.open_connection(
                    address,
                    broker.port,
                    family=family,
                    flags=socket.AI_NUMERICHOST,
                    ssl=context,
                    server_hostname=broker.host,
                )
        except (OSError, TimeoutError):
            continue
    raise ChannelError("Could not establish verified TLS with the MQTT broker.", retryable=True)


def _field(value: str) -> bytes:
    data = value.encode("utf-8")
    return len(data).to_bytes(2, "big") + data


def _packet(header: int, body: bytes = b"") -> bytes:
    length = len(body)
    frame = bytearray([header])
    while length >= 128:
        frame.append((length & 127) | 128)
        length >>= 7
    frame.append(length)
    return bytes(frame) + body


async def _read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    header = (await reader.readexactly(1))[0]
    length = 0
    for shift in range(0, 28, 7):
        digit = (await reader.readexactly(1))[0]
        length |= (digit & 127) << shift
        if length > _MAX_PACKET or (shift and digit == 0):
            break
        if not digit & 128:
            return header, await reader.readexactly(length)
    raise ChannelError("The MQTT broker sent an invalid or oversized packet.", retryable=True)


async def _send(writer: asyncio.StreamWriter, header: int, body: bytes = b"") -> None:
    async with asyncio.timeout(_IO_TIMEOUT):
        writer.write(_packet(header, body))
        await writer.drain()


@asynccontextmanager
async def _connection(broker: Broker) -> AsyncIterator[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
    writer = None
    try:
        async with asyncio.timeout(_IO_TIMEOUT):
            reader, writer = await _dial(broker)
            client_id = "a13n" + uuid4().hex[:18]
            # MQTT level 4, username and password enabled, clean session, no will.
            body = _field("MQTT") + bytes((4, 0xC2)) + _KEEPALIVE.to_bytes(2, "big")
            await _send(writer, 0x10, body + _field(client_id) + _field(broker.username) + _field(broker.password))
            header, response = await _read_packet(reader)
            if header != 0x20 or len(response) != 2 or response[0] != 0 or response[1] > 5:
                raise ChannelError("The MQTT broker sent an invalid CONNACK.", retryable=True)
            if response[1] in {4, 5}:
                raise ChannelError("The MQTT broker rejected the credentials.")
            if response[1]:
                raise ChannelError("The MQTT broker refused the connection.", retryable=response[1] == 3)
        yield reader, writer
    except (OSError, EOFError, TimeoutError):
        raise ChannelError(
            "The MQTT connection ended before the broker confirmed the operation.", retryable=True
        ) from None
    finally:
        if writer is not None:
            writer.close()
            with suppress(OSError, TimeoutError):
                async with asyncio.timeout(2):
                    await writer.wait_closed()


async def bind(_http: httpx.AsyncClient, config: Configuration) -> str:
    broker = Broker.from_config(config)
    async with _connection(broker):
        pass
    # Keep the account key stable across password changes and avoid returning credentials in that identifier.
    identity = [broker.host, broker.port, broker.username]
    if broker.topic_filter:
        # Preserve identities created when the subscription belonged to the account.
        identity.append(broker.topic_filter)
    address = json.dumps(identity)
    return hashlib.sha256(address.encode()).hexdigest()


def _decode_publish(header: int, body: bytes) -> tuple[str, bytes, bytes | None]:
    qos = (header >> 1) & 3
    topic_size = int.from_bytes(body[:2], "big")
    end = 2 + topic_size
    if qos > 1 or (not qos and header & 8) or not topic_size or len(body) < end + 2 * qos:
        raise ChannelError("The MQTT broker sent an invalid PUBLISH packet.", retryable=True)
    try:
        topic = body[2:end].decode("utf-8")
    except UnicodeError:
        raise ChannelError("The MQTT broker sent an invalid publication topic.", retryable=True) from None
    if not topic or "\0" in topic or any(wildcard in topic for wildcard in "+#"):
        raise ChannelError("The MQTT broker sent an invalid publication topic.", retryable=True)
    packet_id = body[end : end + 2] if qos else None
    if packet_id == b"\x00\x00":
        raise ChannelError("The MQTT broker sent an invalid PUBLISH identifier.", retryable=True)
    return topic, body[end + 2 * qos :], packet_id


def _channel_message(channel_id: str, topic: str, payload: bytes) -> tuple[ChannelMessage, dict[str, str]] | None:
    if len(payload) > _MAX_PAYLOAD or not _valid_topic(topic):
        return None
    try:
        text = payload.decode("utf-8")
    except UnicodeError:
        return None
    if not text.strip() or "\0" in text:
        return None

    context = {}
    message_id = uuid4().hex
    if text.lstrip().startswith("{"):
        try:
            envelope = json.loads(text)
        except (ValueError, RecursionError):
            envelope = {}
        source_id = envelope.get("message_id")
        if isinstance(source_id, str) and _MESSAGE_ID.fullmatch(source_id):
            # MQTT 3.1.1 can deliver our publications to any matching local subscription.
            if _REPLY_ID.fullmatch(source_id) and isinstance(envelope.get("in_reply_to"), str):
                return None
            message_id = hashlib.sha256(f"{topic}\0{source_id}".encode()).hexdigest()
            context["message_id"] = source_id
            content = envelope.get("text")
            if (
                isinstance(content, str)
                and content.strip()
                and "\0" not in content
                # JSON escape sequences can otherwise introduce unpaired UTF-16 surrogates.
                and not any("\ud800" <= char <= "\udfff" for char in content)
            ):
                text = content
    return (
        ChannelMessage(
            id=f"mqtt_{message_id}",
            channel_id=channel_id,
            kind="topic",
            conversation_id=topic,
            sender_id="mqtt",
            text=text,
            timestamp=datetime.now(UTC).isoformat(),
        ),
        context,
    )


async def _ping(writer: asyncio.StreamWriter, pong: asyncio.Event) -> None:
    while True:
        await asyncio.sleep(_KEEPALIVE / 2)
        pong.clear()
        await _send(writer, 0xC0)
        async with asyncio.timeout(_IO_TIMEOUT):
            await pong.wait()


class MQTTConnection:
    def __init__(self, _http: httpx.AsyncClient, channel_id: str, _identity: str, config: Configuration):
        self.channel_id = channel_id
        self.broker = Broker.from_config(config)
        self.subscriptions = self.subscription_filters(set())

    def subscription_filters(self, route_filters: set[str]) -> tuple[str, ...]:
        return tuple(sorted(route_filters | ({self.broker.topic_filter} if self.broker.topic_filter else set())))

    async def _publish_received(self, writer: asyncio.StreamWriter, ingest: Ingest, header: int, body: bytes) -> None:
        topic, payload, packet_id = _decode_publish(header, body)
        if not header & 1 and any(matches_topic(pattern, topic) for pattern in self.subscriptions):
            result = _channel_message(self.channel_id, topic, payload)
            if result is not None:
                message, context = result
                await ingest(message, context=context)
        if packet_id is not None:
            await _send(writer, 0x40, packet_id)

    async def _listen_packets(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        ingest: Ingest,
        status: Callable[[str], None],
        pong: asyncio.Event,
    ) -> None:
        filters = b"".join(_field(pattern) + b"\x01" for pattern in self.subscriptions)
        await _send(writer, 0x82, _PACKET_ID + filters)
        async with asyncio.timeout(_IO_TIMEOUT):
            while True:
                header, body = await _read_packet(reader)
                if header == 0x90 and len(body) == 2 + len(self.subscriptions) and body[:2] == _PACKET_ID:
                    if any(code not in {0, 1} for code in body[2:]):
                        raise ChannelError("The MQTT broker rejected the subscription.")
                    break
                if header >> 4 != 3:
                    raise ChannelError("The MQTT broker sent an invalid SUBACK.", retryable=True)
                await self._publish_received(writer, ingest, header, body)
        status("connected")
        while True:
            header, body = await _read_packet(reader)
            if header >> 4 == 3:
                await self._publish_received(writer, ingest, header, body)
            elif header == 0xD0 and not body:
                pong.set()
            else:
                raise ChannelError("The MQTT broker sent an unexpected packet.", retryable=True)

    async def listen(self, _cursor: str, ingest: Ingest, status: Callable[[str], None]) -> None:
        if not self.subscriptions:
            status("inactive")
            return
        delay = 1
        while True:
            started = asyncio.get_running_loop().time()
            try:
                async with _connection(self.broker) as (reader, writer):
                    pong = asyncio.Event()
                    tasks = (
                        asyncio.create_task(self._listen_packets(reader, writer, ingest, status, pong)),
                        asyncio.create_task(_ping(writer, pong)),
                    )
                    try:
                        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in tasks:
                            if task in done:
                                task.result()
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
            except ChannelError as error:
                if not error.retryable:
                    status("login_required")
                    return
            status("reconnecting")
            if asyncio.get_running_loop().time() - started > _KEEPALIVE:
                delay = 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    async def reply(
        self, message: ChannelMessage, text: str, context: Mapping[str, str], *, reply_topic: str | None = None
    ) -> str:
        if (
            message.channel_id != self.channel_id
            or message.kind != "topic"
            or not valid_publish_topic(message.conversation_id)
        ):
            raise ChannelError("The MQTT reply is missing a valid source topic.")
        topic = reply_topic or self.broker.reply_topic or message.conversation_id
        if not valid_publish_topic(topic):
            raise ChannelError("Enter a valid MQTT reply topic.")
        message_id = f"a13n-reply-{uuid4().hex}"
        payload = json.dumps(
            {
                "message_id": message_id,
                "in_reply_to": context.get("message_id", message.id),
                "conversation_id": message.conversation_id,
                "text": text,
            },
            ensure_ascii=False,
        ).encode()
        async with _connection(self.broker) as (reader, writer):
            async with asyncio.timeout(_IO_TIMEOUT):
                await _send(writer, 0x32, _field(topic) + _PACKET_ID + payload)
                if await _read_packet(reader) != (0x40, _PACKET_ID):
                    raise ChannelError("The MQTT broker did not confirm the publication.", retryable=True)
        return message_id


ADAPTER = Adapter(
    key="mqtt",
    label="MQTT",
    label_zh="MQTT",
    kinds=("topic",),
    fields=(CredentialField("broker_url", "Broker URL (mqtts://)", "Broker 连接 URL（mqtts://）"),),
    bind=bind,
    connect=MQTTConnection,
    migrate_config=migrate_config,
)
