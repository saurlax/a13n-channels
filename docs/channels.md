# Configure and use Channels

## Configure Service

Run one Service process in the `all` role for this App's current storage and listener model. Set its state path in the Service settings file and use a persistent volume. Configure the host Service encryption key to protect bot credentials and Weixin reply contexts.

```toml
[channels]
state_path = "/var/lib/a13n-channels/channels.sqlite3"
```

`A13N_CHANNELS__STATE_PATH` can also set the path. The App stores workspace-owned accounts and received messages in SQLite. Keep the database directory and host encryption key across restarts. The lock beside the database ensures one Service process owns its listeners.

Restart Service after changing its settings. The source checkout's `make dev` launcher writes a fresh `var/dev/service.toml`, so keep the App settings in the configuration you pass to Service.

## Connect a channel

Enable **Channels** under **Settings → Organization → App plugins**. In **Apps → Channels**, select **Add channel**, choose a platform, and enter its credentials or complete pairing. Check the account's connection status before creating a delivery route.

### QQ bot credentials

In **Apps → Channels → Add channel**, choose **QQ Bot** and enter its name, AppID, and AppSecret. Channels connects to QQ using its outbound Gateway protocol. Open the channel row to manage its credentials. Replacing an AppID requires its matching AppSecret. Ordinary group messages require QQ’s receive-all-messages setting. Guild channels require the corresponding QQ permissions.

### QQ QR binding

Install Tencent's connector in the Service runtime alongside Node.js 18 or later. The App uses the connector's default third-party identity on the authorization screen.

```bash
npm install --prefix /opt/a13n/qq-connector --ignore-scripts @tencent-connect/qqbot-connector@1.2.0
```

Add `qq_connector_path = "/opt/a13n/qq-connector/node_modules/@tencent-connect/qqbot-connector"` to the existing `[channels]` settings section and restart Service. If the connector must use the deployment's HTTPS proxy, set `NODE_USE_ENV_PROXY=1` in the Service process environment on a Node version that supports it.

With the connector configured, select **Scan QR code** under **QQ Bot** and approve the binding in the QQ mobile app. The current dialog displays the QR code and confirms authorization after approval. After authorization, Channels automatically opens a temporary QQ WebSocket and waits for the platform to report readiness. Use **Test connection** to retry or to check manually entered credentials. Click **Save** to add the channel. Service saves the returned credentials in the workspace using the same encrypted storage as manual setup. Select **Enter credentials** to add a bot manually. Deployments without the connector can continue using manual setup.

### Weixin pairing

For Weixin, choose **Weixin ClawBot** in **Add channel**. Enter a name, get the QR code, then scan and confirm it in Weixin. Enter the verification code if prompted. Open the channel row to pair again. Click **Save** after approving the QR code in Weixin. Weixin supports private text messages and replies to messages with an active reply context.

The Console page manages accounts and their connection status. To remove a QQ or Weixin channel, open its row and choose **Delete** in the editor footer. Confirming stops the connection and permanently deletes the channel and its locally stored messages, including messages from previous bot bindings. When a channel is paired with a different bot, incoming messages and replies use the new bot's identity. Earlier records stay associated with the previous bot. The App API scopes replies to their source conversation. Disabling the App for an organization stops its connections and retains its stored accounts and messages.

### GitHub and GitHub App

Choose **GitHub** to connect a user with a classic personal access token. Grant `notifications` and the repository permissions needed for reading and commenting. The connector polls that user's notifications without marking them as read.

Choose **GitHub App** to connect an installation with its App ID, installation ID, and PEM private key. Install the App on the selected repositories and grant **Issues** and **Pull requests** permissions for the conversations it should handle. Commenting requires write permission. The connector obtains short-lived installation tokens and polls repository activity.

For either mode, select the channel account and choose **All**, **Issue**, or **Pull Request** in the route’s **Sources** table. Enter repositories as `owner/repo` in **Conversation**. In **Filters → Allowed users**, add the account and the GitHub usernames that may trigger the route. Separate usernames with commas. GitHub delivery requires both a matching repository and an allowed sender. Polling covers the repositories configured on active routes. The allowlist applies per account. QQ and Weixin sources accept any sender unless an allowlist is set for their account. Issue and PR descriptions, ordinary comments, and PR review text and inline comments use the same conversation as the parent Issue or PR. The mentions filter recognizes the bound GitHub login or App slug and review requests explicitly assigned to that identity when present in polled activity. The send tool posts an ordinary comment to the originating Issue or PR. Polling coverage depends on GitHub notifications for user accounts and the repository activity returned for App installations.

### Outbound connectors

Choose the platform in **Add channel**, supply the connection credentials, and create a delivery route to a published Agent. Channels keeps one connection per upstream identity in a deployment. Keep the upstream bot or subscription dedicated to this deployment when the platform shares or load-balances events across consumers.

| Platform | Platform setup | Conversation and reply behavior |
| --- | --- | --- |
| [Feishu](https://open.feishu.cn/document/ukTMukTMukTM/uYDNxYjL2QTM24iN0EjN/event-subscription-configure-) | Publish a mainland Feishu custom app with the bot capability. Configure long-connection event delivery for `im.message.receive_v1`, and grant the receive and bot-send permissions for the intended chats. Enter the App ID and App Secret. | Private and group chats, including bot mentions. Delivers text and text extracted from posts. Replies to the original message. |
| [Slack](https://docs.slack.dev/apis/events-api/using-socket-mode/) | Enable Socket Mode. Enter an app-level token with `connections:write` and a bot token with `chat:write`. Subscribe to `app_mention` or the required `message.*` bot events and grant their corresponding scopes. Invite the bot to the channels it needs. | Direct messages and joined channels. Replies in the source Slack thread. App-level sockets share event delivery, so dedicate the Slack app to the intended workspace. |
| [Discord](https://docs.discord.com/developers/events/gateway) | Add the bot to the server and grant View Channel, Send Messages, and Read Message History. Grant Send Messages in Threads for thread replies. Enter the Bot token. To read ordinary unmentioned guild messages, enable Message Content Intent in the Developer Portal and set the connector field to `true`. | Private and guild messages. Guild threads use their native channel IDs. Replies reference the original message and suppress mention notifications. Gateway resume is in-process. |
| [DingTalk](https://open-dingtalk.github.io/developerpedia/docs/learn/stream/protocol/) | Enable a bot for an enterprise application and select Stream Mode. Enter its Client ID and Client Secret. | Delivers private and group bot callbacks. Text replies use the original session webhook. If it expires, send a new message to the bot. |
| [WeCom](https://developer.work.weixin.qq.com/document/path/101463) | Create an intelligent bot with long-connection access and enter its Bot ID and Secret. | Private chats and group events addressed to the bot. Replies use the original callback while its reply window is open and send to the same conversation after it closes. |
| [Telegram](https://core.telegram.org/bots/api#getupdates) | Enter a BotFather token. Stop any other poller and remove any configured webhook before connecting. Telegram's group privacy mode and bot permissions determine which messages are delivered. | Private chats, groups, and forum topics. Topic routes use `chat_id:message_thread_id`. Replies target the original message and topic. Delivers text and media captions. |
| [MQTT](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) | Enter a public TLS broker hostname, credentials, an inbound topic filter, and a separate reply topic outside the filter. The default TLS port is 8883. Give the broker account subscribe access to the filter and publish access to the reply topic. | Routes use the actual incoming topic, with sender `mqtt`. Payloads are UTF-8. Producers can include a stable JSON `message_id` for deduplication. Retained messages are skipped. Replies are JSON on the configured topic. QoS 1 ACK confirms the broker response. Subscribers confirm receipt separately. Clean sessions have no offline backlog. |

The outbound connectors deliver text and use explicit `channel_send_message` calls for replies. Route filters and allowed senders apply before an Agent runs.

## Route messages to Agents

1. Connect a channel account in **Apps → Channels**.
2. Open **Delivery routes → Add route** and choose a published Agent and its message sources.
3. Configure filters, then save with **Enable route** selected.

New matching messages trigger the target Agent. Channels loads the conversation tools automatically for each routed run. This workflow requires no changes in the Agent editor.

Select a channel account to see its supported conversation types. For chat channels, select an observed conversation ID or enter it directly. Leave the conversation field empty to match every conversation of the selected type. Choosing **All channel accounts** locks both conversation fields to their all-values settings. [GitHub routes](#github-and-github-app) require a matching repository and an allowed sender.

Under **Filters**, choose whether group and guild messages must mention the bot or whether all received messages qualify. This applies across the route's sources. Private messages always pass this filter. New routes in Console default to mentions only. Older routes retain their saved behavior until a route-wide filter is selected. The mention filter applies to the highest-priority matching route.

Choose a different published **Wake Agent** to decide whether a qualifying message reaches the target Agent. Configure the decision criteria in the wake Agent's instructions. Each decision runs in a separate Thread and returns a boolean. A positive decision delivers the original message to the target Agent under its configured Thread policy. A negative or failed decision skips delivery. Open the Wake Agent's decision Thread in Console to inspect the run.

Matching sources are ranked by exact conversation ID, repository, specific account, then specific conversation type. Saving equally specific overlaps is rejected. A paused matching rule prevents fallback to a more general rule. Messages with no enabled matching rule stay in channel storage. Creating or enabling a route does not replay them.

Saving authorizes the route to submit inputs under the saving user's identity. The plugin loads that person's current workspace grants for each delivery and uses only the read and run verbs. Losing either grant pauses delivery. Saving as a different user starts fresh Threads. Routes created by the previous HTTP-based implementation require one save to authorize in-process delivery. Remove its delivery URL and Runner key settings when upgrading.

Each matched message is submitted as one user input containing its source metadata, text, and any available QQ attachment and structure metadata. New messages for the same delivery binding steer a compatible active Run. The host delivers immediately when supported and otherwise incorporates the message at a safe boundary. An idle Thread starts a Run. The per-message Thread policy always starts a fresh Run. The route selects one of four Thread policies:

| Policy | Thread scope |
| --- | --- |
| Reuse by conversation (default) | One Thread per channel account and conversation. |
| Reuse by sender | One Thread per sender within each channel account and conversation. |
| Reuse one Thread | All sources share an existing Thread in the workspace, or a Thread created on first delivery when the ID is empty. |
| New Thread for each message | A separate Session and Thread for every message. |

**Context retention (seconds)** defaults to 3600 for new and existing routes. For Threads created by Channels, the next qualifying message starts a fresh Session once the Thread and its child work have been idle for the configured time. Earlier Sessions remain available in Console. Per-message routes always start fresh, and fixed routes with an explicit Thread ID continue targeting that Thread.

In a route's editor, select **Reset conversations** and confirm to clear that route's Thread bindings and explicit fixed Thread ID immediately. Subsequent deliveries start fresh Threads according to the route's Thread policy. Active Runs and submissions already prepared for retry finish against their original Threads. Earlier Sessions remain available in Console.

A new target Agent or saving user resets the route's managed Thread bindings. An explicit fixed Thread ID continues to target that Thread. Changing the Thread destination requires pending deliveries to settle.

The plugin submits directly through the host Service and uses the same idempotency key when retrying an uncertain submission.

Channel delivery adds these Harness tools to the target Agent for that run:

- `channel_list_messages` reads recent incoming messages and attachment metadata from the originating conversation.
- `channel_get_message` reads one full stored message, including expanded forwards and structured content.
- `channel_get_attachment` views a QQ image or sticker, or saves a QQ attachment into the Agent Environment.
- `channel_send_message` replies to the message that started the Run. It accepts text or QQ rich content. QQ and Weixin use that message's reply context.

When a Run receives multiple channel messages, use the intended message ID to select the conversation for reads. The reply tool selects the triggering message even when later inputs steer the same Run. Assistant text stays in Console. The tools resolve the conversation from the Run's delivery record and require a currently enabled route and account. Failed deliveries and unconfirmed sends appear in route status. A replay of the same send call never resends an unconfirmed reply.

If the target Run fails or is cancelled, Channels sends a short notice in the conversation of the message that started it. This also applies after a Run resumes. The notice uses fixed text for known failure categories and a generic message for other errors. The delivery stays failed. Channels records the send before contacting the platform and skips that attempt during recovery if confirmation is missing.

### Read across channels

In the Agent editor, open **Advanced → Plugins → Add plugin**, choose **Channels**, and publish the Agent configuration. Set **Allowed channels** to a comma-separated list of channel account IDs, or leave it blank to allow all channel accounts in the current workspace for the active tools. Use the Channels App's account IDs. The **Channels Delivery** plugin is loaded automatically by routes.

- `channel_list_accounts` lists readable channel accounts.
- `channel_list_conversations` lists recent conversations for one account.
- `channel_read_messages` reads incoming messages from a selected conversation. Pass the last returned message ID as `before_id` to page backward.
- `channel_read_message` reads one full stored message.
- `channel_read_attachment` views or saves a QQ attachment using a message ID and its one-based attachment index.
- `channel_send_proactive` sends text or rich content to a QQ conversation without an incoming message. Pass the channel account ID, conversation kind, and conversation ID. QQ applies its platform permissions and delivery limits.

The configured Channels tools are available in regular, scheduled, and routed runs with workspace read access. Proactive sends additionally require workspace run access. Routed runs also receive the Channels Delivery tools for replying to the original conversation.

## QQ images, stickers, records, and cards

`channel_send_message` and `channel_send_proactive` accept `text` alone for a plain-text message and `rich` alone for a standalone image. Supply both `text` and an image `rich` object to send the image and its caption in one message. Send voice, video, and files in a separate call from any accompanying text.

| Content | Example `rich` value |
| --- | --- |
| Generated image or sticker | `{"type":"media","path":"/workspace/sticker.gif"}` |
| Image by URL | `{"type":"media","url":"https://example.com/image.png"}` |
| Voice, video, or file | `{"type":"media","kind":"audio","path":"/workspace/reply.wav"}` |
| Markdown with buttons | `{"type":"markdown","content":"**Choose an option**","keyboard":{"id":"TEMPLATE_ID"}}` |
| Markdown template | `{"type":"markdown","custom_template_id":"TEMPLATE_ID","params":[{"key":"title","values":["Status"]}]}` |
| Ark or Embed card | `{"type":"ark","card":{"template_id":23,"kv":[{"key":"#DESC#","value":"Status"}]}}` |

QQ C2C and group sends upload bytes or a URL to obtain a media receipt. Large files use QQ's multipart upload flow. Guild and guild DM media use the image field or an image file. The QQ reply API accepts the same text and rich message objects with HTTPS URLs for media. QQ controls availability of file types, custom cards, keyboards, quotes, and proactive messages for each bot and conversation.

QQ history preserves images, custom sticker markers, emoji labels, audio, video, files, quotes, cards, and forwarded chat records supplied in Gateway events. Attachment URLs stay private. Pass `message_id` and the reported one-based `index` to `channel_get_attachment` to inspect an image or sticker. Supply an absolute Environment `save_path` for other attachments or to save a larger image. For older messages in the same conversation, identify the current delivery with `source_message_id`. Temporary QQ links can expire. A fresh message supplies a new link.

When an incoming QQ message has one to four images or stickers totaling at most 10 MiB, Channels includes them with the text as image inputs for a vision-capable target model. The route owner needs current workspace write access to create the image Assets. These Assets follow Service's workspace visibility and retention rules. Messages with more images, unavailable images, or a text-only model still deliver text and attachment metadata. Files, videos, and audio download on demand through `channel_get_attachment`.

Use `reference_message_id` on a reply to quote a stored message from that conversation when QQ supplies a reference identifier. To share a forwarded record's contents, send its text or a Markdown recap. QQ callback buttons and quick menus deliver an interaction to the matching route and use the event's native reply context. The bot needs the interaction intent enabled for those callbacks.

[QQ send API](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/send-receive/send.md) · [QQ buttons](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/trans/msg-btn.md) · [QQ SDK media uploads](https://github.com/tencent-connect/qqbot-nodejs/blob/main/USAGE.md#10-媒体上传与发送)
