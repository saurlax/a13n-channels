import asyncio
import base64
import hashlib
import json

import httpx
import pytest
from pydantic_ai import Tool
from pydantic_ai.profiles.google import GoogleJsonSchemaTransformer, GoogleOpenAPISchemaTransformer

from a13n_channels.channels import qq
from a13n_channels.channels.qq_content import QQArk, QQMarkdown, QQMedia, QQReply, open_attachment
from a13n_channels.tools import ChannelToolFactory, ChannelTools

PNG = b"\x89PNG\r\n\x1a\npayload"
STICKER = "https://grouptalk.c2c.qq.com/sticker?rkey=private"
RECORD = "https://multimedia.nt.qq.com.cn/download?appid=one&fileid=two&rkey=three"
TIME = "2026-11-19T00:00:00Z"


def incoming(content="", **fields):
    return qq.message(
        "channel",
        {
            "t": "GROUP_AT_MESSAGE_CREATE",
            "d": {
                "author": {"member_openid": "sender"},
                "id": "source",
                "timestamp": TIME,
                "group_openid": "room",
                "content": content,
                **fields,
            },
        },
    )


def connection(client):
    return qq.QQConnection(client, "channel", qq.QQCreate(name="Bot", app_id="bot", app_secret="test-secret"))


def token_response():
    return httpx.Response(200, json={"access_token": "test-token", "expires_in": 60})


@pytest.mark.parametrize("transformer", [GoogleJsonSchemaTransformer, GoogleOpenAPISchemaTransformer])
def test_gemini_tool_schema_keeps_native_qq_card_and_button_fields(transformer):
    plugin = ChannelTools("channels", ChannelToolFactory())
    schema = transformer(Tool(plugin.channel_send_message).function_schema.json_schema).walk()
    assert "message_id" not in schema["properties"]
    rendered = json.dumps(schema)
    for field in ("specify_user_ids", "render_data", "obj_kv", "template_id", "thumbnail"):
        assert f'"{field}"' in rendered


def test_qq_projects_attachments_stickers_forwarded_records_quotes_and_cards():
    emoji = base64.b64encode('{"text":"笑"}'.encode()).decode()
    source = incoming(
        f'<faceType=1,faceId="2",ext="{emoji}"> caption',
        msg_elements=[
            {
                "content": '<faceType=6,faceId="1",ext="">',
                "attachments": [{"url": STICKER, "content_type": "image/gif", "filename": "sticker.gif"}],
            },
            {
                "message_type": 102,
                "content": f"record\n[附件1] 类型:图片 文件名:photo.jpg 尺寸:1x2 大小:3KB URL:{RECORD}",
                "msg_elements": [
                    {
                        "content": "nested text",
                        "ark_data": {"template_id": 23, "auth_token": "private-token"},
                        "attachments": [{"url": "//tran.ftn.qq.com/download", "content_type": "application/pdf"}],
                    }
                ],
            },
        ],
        message_scene={"ext": ["msg_idx=native-quote"]},
        message_reference={"message_id": "quoted"},
    )
    assert source.text.splitlines() == [
        "[QQ emoji: 笑] caption",
        "[Quoted content]",
        "[QQ sticker]",
        "[Chat history]",
        "record",
        "[Image]",
        "  nested text",
    ]
    assert source.sender_id == "sender" and source.bot_mentioned
    assert [item.kind for item in source.attachments] == ["sticker", "file", "image"]
    assert source.attachment_urls == (STICKER, "https://tran.ftn.qq.com/download", RECORD)
    assert source.structured == {
        "quoted": True,
        "cards": [{"ark_data": {"template_id": 23}}],
        "reference_id": "quoted",
        "native_reference_id": "native-quote",
    }
    assert all(value not in source.model_dump_json() for value in ("rkey=", "private-token", "attachment_urls"))
    assert incoming(attachments=[{"url": STICKER, "content_type": "image/gif"}]).attachments[0].kind == "image"


def test_qq_attachment_fetch_pins_tls_origin_and_revalidates_redirects(monkeypatch):
    authorized = []
    requested = []

    def transfer(req):
        requested.append(req)
        assert req.url.host == "8.8.8.8"
        assert req.headers["Host"] == "grouptalk.c2c.qq.com"
        assert req.extensions["sni_hostname"] == "grouptalk.c2c.qq.com"
        assert "authorization" not in req.headers and "cookie" not in req.headers
        if req.url.path == "/redirect":
            return httpx.Response(302, headers={"Location": "https://alias.example.com/internal"})
        return httpx.Response(200, content=PNG)

    async def getaddrinfo(host, port, *, type):
        address = "8.8.8.8" if host == "grouptalk.c2c.qq.com" else "127.0.0.1"
        return [(None, type, None, None, (address, port))]

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda: httpx.MockTransport(transfer))

    async def exercise():
        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)
        async with open_attachment(STICKER, authorized.append) as response:
            assert await response.aread() == PNG
        with pytest.raises(ValueError, match="resolve to public"):
            async with open_attachment("https://grouptalk.c2c.qq.com/redirect", authorized.append):
                pytest.fail("A redirect to a private address must be rejected")

    asyncio.run(exercise())
    assert len(requested) == 2
    assert authorized[-1] == "https://alias.example.com/internal"


@pytest.mark.parametrize("kind,path", [("private", "/v2/users/room"), ("group", "/v2/groups/room")])
def test_qq_sends_url_and_environment_images_as_media(kind, path):
    requests = []

    def request(req):
        if req.url.path == "/app/getAppAccessToken":
            return token_response()
        assert req.headers["Authorization"] == "QQBot test-token"
        body = json.loads(req.content)
        requests.append((req.url.path, body))
        return httpx.Response(200, json={"file_info": "receipt"} if req.url.path.endswith("/files") else {"id": "sent"})

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
            bot = connection(client)
            source = incoming().model_copy(update={"kind": kind})
            assert await bot.reply(source, None, 1, rich=QQMedia(type="media", path="/photo.png"), data=PNG) == "sent"
            assert (
                await bot.send_proactive(
                    kind, "room", "caption", rich=QQMedia(type="media", url="https://assets.example.com/photo.png")
                )
                == "sent"
            )

    asyncio.run(exercise())
    assert requests == [
        (
            path + "/files",
            {
                "file_type": 1,
                "file_name": "photo.png",
                "srv_send_msg": False,
                "file_data": base64.b64encode(PNG).decode(),
            },
        ),
        (
            path + "/messages",
            {"msg_id": "source", "msg_seq": 1, "content": " ", "msg_type": 7, "media": {"file_info": "receipt"}},
        ),
        (
            path + "/files",
            {
                "file_type": 1,
                "file_name": "photo.png",
                "srv_send_msg": False,
                "url": "https://assets.example.com/photo.png",
            },
        ),
        (path + "/messages", {"content": "caption", "msg_type": 7, "media": {"file_info": "receipt"}}),
    ]


def test_qq_multipart_confirms_pending_parts_before_sending(monkeypatch):
    data = PNG + b"x" * (4 << 20)
    block = 3 << 20
    operations = []
    confirmations = []

    def request(req):
        if req.url.path == "/app/getAppAccessToken":
            return token_response()
        name = req.url.path.rsplit("/", 1)[-1]
        body = json.loads(req.content)
        operations.append(name)
        match name:
            case "upload_prepare":
                assert body["file_size"] == len(data)
                assert body["md5"] == hashlib.md5(data).hexdigest()
                return httpx.Response(
                    200,
                    json={
                        "upload_id": "private-receipt",
                        "block_size": block,
                        "parts": [
                            {
                                "index": index,
                                "presigned_url": f"https://test.cos.ap-shanghai.myqcloud.com/{index}?signed=1",
                            }
                            for index in (2, 1)
                        ],
                    },
                )
            case "upload_part_finish":
                confirmations.append(body)
                if len(confirmations) == 1:
                    return httpx.Response(400, json={"code": 40093001})
                return httpx.Response(204)
            case "files":
                assert body == {"upload_id": "private-receipt", "srv_send_msg": False}
                return httpx.Response(200, json={"file_info": "confirmed"})
            case "messages":
                assert body["media"] == {"file_info": "confirmed"}
                return httpx.Response(200, json={"id": "sent"})
        pytest.fail(f"Unexpected operation {name}")

    def upload(req):
        assert req.method == "PUT"
        index = int(req.url.path[1:])
        operations.append(f"put:{index}")
        assert "authorization" not in req.headers and "cookie" not in req.headers
        assert req.content == data[(index - 1) * block : index * block]
        return httpx.Response(200)

    monkeypatch.setattr(qq.httpx, "AsyncHTTPTransport", lambda: httpx.MockTransport(upload))

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
            assert (
                await connection(client).reply(
                    incoming(), None, 1, rich=QQMedia(type="media", path="/agent/image.png"), data=data
                )
                == "sent"
            )

    asyncio.run(exercise())
    assert operations == [
        "upload_prepare",
        "put:1",
        "upload_part_finish",
        "upload_part_finish",
        "put:2",
        "upload_part_finish",
        "files",
        "messages",
    ]
    assert confirmations[0] == confirmations[1]
    assert confirmations[2]["md5"] == hashlib.md5(data[block:]).hexdigest()


def test_qq_sends_cards_markdown_buttons_guild_images_and_interaction_replies():
    requests = []

    def request(req):
        if req.url.path == "/app/getAppAccessToken":
            return token_response()
        if req.method == "PUT":
            assert req.url.path == "/interactions/ack-id"
            assert json.loads(req.content) == {"code": 0}
            return httpx.Response(204)
        if req.url.path == "/channels/room/messages":
            assert req.headers["Content-Type"].startswith("multipart/form-data")
            assert b'filename="photo.png"' in req.content
            assert PNG in req.content
        else:
            requests.append(json.loads(req.content))
        return httpx.Response(200, json={"id": "sent"})

    event = qq.message(
        "channel",
        {
            "t": "INTERACTION_CREATE",
            "id": "reply-id",
            "d": {
                "id": "ack-id",
                "chat_type": 2,
                "type": 11,
                "timestamp": TIME,
                "data": {"resolved": {"button_id": "button", "button_data": "go", "user_id": "room"}},
            },
        },
    )
    assert event.kind == "private" and event.conversation_id == "room"
    assert event.structured["interaction"] == {"button_id": "button", "button_data": "go"}
    assert event.id.startswith("interaction:") and "reply-id" not in event.model_dump_json()

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
            bot = connection(client)
            await bot.acknowledge_interaction(event.interaction_id)
            await bot.reply(
                event,
                None,
                1,
                rich=QQMarkdown(
                    type="markdown",
                    custom_template_id="template",
                    params=[{"key": "name", "values": ["QQ"]}],
                    keyboard={"id": "keyboard-template"},
                ),
                event_reply_id=event.event_reply_id,
            )
            await bot.reply(
                incoming(), None, 2, rich=QQArk(type="ark", card={"template_id": 23, "kv": []}), reference_id="quote"
            )
            await bot.reply(
                incoming().model_copy(update={"kind": "guild"}),
                None,
                1,
                rich=QQMedia(type="media", path="/photo.png"),
                data=PNG,
            )

    asyncio.run(exercise())
    assert requests == [
        {
            "event_id": "reply-id",
            "msg_seq": 1,
            "msg_type": 2,
            "keyboard": {"id": "keyboard-template"},
            "markdown": {"custom_template_id": "template", "params": [{"key": "name", "values": ["QQ"]}]},
        },
        {
            "msg_id": "source",
            "msg_seq": 2,
            "msg_type": 3,
            "ark": {"template_id": 23, "kv": []},
            "message_reference": {"message_id": "quote"},
        },
    ]
    with pytest.raises(ValueError, match="public media"):
        QQMedia(type="media", url="https://127.0.0.1/private.png")
    with pytest.raises(ValueError, match="requires an HTTPS URL"):
        QQReply(rich=QQMedia(type="media", path="/photo.png"))
