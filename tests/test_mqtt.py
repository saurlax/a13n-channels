import asyncio
import json
import socket
import ssl

import pytest

from a13n_channels.channels import mqtt
from a13n_channels.channels.adapter import ChannelError

CONFIG = {
    "host": "broker.example.com",
    "username": "bot",
    "password": "password",
    "topic_filter": "events/#",
    "reply_topic": "results",
}


class Writer:
    def __init__(self):
        self.frames = []
        self.closed = False

    def write(self, frame):
        self.frames.append(frame)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


def test_rejects_a_reply_topic_inside_the_subscription():
    for topic_filter, reply_topic in (("events/#", "events"), ("events/+", "events/reply"), ("#", "results")):
        with pytest.raises(ChannelError, match="outside"):
            mqtt.Broker.from_config({**CONFIG, "topic_filter": topic_filter, "reply_topic": reply_topic})
    assert not mqtt._matches("#", "$SYS/broker")
    assert mqtt._matches("events/+", "events/")


@pytest.mark.parametrize("untrusted_ip", ["127.0.0.1", "169.254.169.254", "10.0.0.1", "64:ff9b::7f00:1"])
def test_tls_dial_pins_validated_address_and_uses_hostname_for_certificates(monkeypatch, untrusted_ip):
    async def run():
        calls = []
        addresses = ["8.8.8.8"]

        async def getaddrinfo(host, port, **kwargs):
            assert (host, port) == ("broker.example.com", 8883)
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (addr, port)) for addr in addresses]

        async def open_connection(host, port, **kwargs):
            calls.append((host, port, kwargs))
            return None, None

        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)
        monkeypatch.setattr(mqtt.asyncio, "open_connection", open_connection)
        broker = mqtt.Broker.from_config(CONFIG)
        await mqtt._dial(broker)
        ((host, port, kwargs),) = calls
        assert (host, port) == ("8.8.8.8", 8883)
        assert kwargs["flags"] & socket.AI_NUMERICHOST
        assert kwargs["server_hostname"] == "broker.example.com"
        assert kwargs["ssl"].check_hostname and kwargs["ssl"].verify_mode == ssl.CERT_REQUIRED
        addresses.append(untrusted_ip)
        with pytest.raises(ChannelError, match="public IP"):
            await mqtt._dial(broker)
        assert len(calls) == 1

    asyncio.run(run())


def test_normalizes_text_and_deduplicates_only_stable_application_ids():
    first, context = mqtt._channel_message("channel", "events/topic", b'{"message_id":"request-42","text":"hello"}')
    repeated, _ = mqtt._channel_message("channel", "events/topic", b'{"message_id":"request-42","text":"hello"}')
    other_topic, _ = mqtt._channel_message("channel", "events/other", b'{"message_id":"request-42","text":"hello"}')
    assert (first.kind, first.conversation_id, first.sender_id, first.text) == (
        "topic",
        "events/topic",
        "mqtt",
        "hello",
    )
    assert first.id == repeated.id != other_topic.id
    assert context == {"message_id": "request-42"}
    plain, _ = mqtt._channel_message("channel", "events/topic", b"hello")
    later, _ = mqtt._channel_message("channel", "events/topic", b"hello")
    assert plain.id != later.id
    assert mqtt._channel_message("channel", "events/topic", b"\xff") is None


def test_subscribes_and_acknowledges_only_after_ingest(monkeypatch):
    async def run():
        reader = asyncio.StreamReader()
        writer = Writer()

        async def dial(_broker):
            return reader, writer

        monkeypatch.setattr(mqtt, "_dial", dial)
        incoming_id = b"\x00\x11"
        retained_id = b"\x00\x12"
        publish = mqtt._field("events/topic")
        reader.feed_data(
            b"\x20\x02\x00\x00"
            + mqtt._packet(0x32, publish + incoming_id + b"hello")
            + b"\x90\x03\x00\x01\x01"
            + mqtt._packet(0x33, publish + retained_id + b"retained")
        )
        received = []
        connected = asyncio.Event()

        async def ingest(message, *, context):
            assert mqtt._packet(0x40, incoming_id) not in writer.frames
            received.append(message)

        def status(state):
            if state == "connected":
                connected.set()

        connector = mqtt.ADAPTER.connect(None, "channel", "account", CONFIG)
        task = asyncio.create_task(connector.listen("", ingest, status))
        try:
            await asyncio.wait_for(connected.wait(), 1)
            assert writer.frames[0][0] == 0x10
            assert writer.frames[1] == mqtt._packet(0x82, b"\x00\x01" + mqtt._field("events/#") + b"\x01")
            assert writer.frames[2:] == [b"\x40\x02\x00\x11", b"\x40\x02\x00\x12"]
            assert [message.text for message in received] == ["hello"]
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert writer.closed

    asyncio.run(run())


@pytest.mark.parametrize("ack", [b"\x40\x02\x00\x01", b"\x40\x02\x00\x02", b""])
def test_sends_to_fixed_reply_topic_and_requires_matching_puback(monkeypatch, ack):
    async def run():
        reader = asyncio.StreamReader()
        writer = Writer()
        reader.feed_data(b"\x20\x02\x00\x00" + ack)
        reader.feed_eof()

        async def dial(_broker):
            return reader, writer

        monkeypatch.setattr(mqtt, "_dial", dial)
        connector = mqtt.ADAPTER.connect(None, "channel", "account", CONFIG)
        message, context = mqtt._channel_message("channel", "events/topic", b'{"message_id":"req-1"}')
        if ack == b"\x40\x02\x00\x01":
            sent_id = await connector.reply(message, "world", context)
        else:
            with pytest.raises(ChannelError, match="confirm"):
                await connector.reply(message, "world", context)
            sent_id = None

        frame = asyncio.StreamReader()
        frame.feed_data(writer.frames[1])
        header, body = await mqtt._read_packet(frame)
        assert header == 0x32
        prefix = mqtt._field("results") + b"\x00\x01"
        assert body.startswith(prefix)
        payload = json.loads(body[len(prefix) :])
        assert payload == {
            "message_id": sent_id or payload["message_id"],
            "conversation_id": "events/topic",
            "in_reply_to": "req-1",
            "text": "world",
        }
        assert writer.closed

    asyncio.run(run())
