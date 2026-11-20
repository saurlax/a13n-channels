# Channels

An a13n Service App for managing QQ bots, Weixin ClawBot, and GitHub delivery in Console. It connects to QQ by outbound WebSocket and Weixin by iLink long polling. Routes deliver incoming messages to Agents and provide tools for reading conversation history and replying through the source channel.

## Prerequisites and compatibility

Use Python 3.13, uv, Git, Make, Node.js 24 and pnpm 10.30.3 on Linux or macOS. The App Plugins API is currently a prerelease source contract. `.service-ref` pins the compatible Agent Foundation commit; `make setup` checks out that source under `.host/` and installs Service and its workspace dependencies into **this project's `.venv`**. A bare install of the current PyPI `a13n-service` is not a substitute for that pinned contract. Review and update the pin when adopting a newer Service release.

## Build and test

```bash
make setup
make verify
```

`make setup` installs the plugin editable after building its frontend. To reuse a compatible source checkout, run `make setup HOST_SOURCE=/absolute/path/to/agent-foundation`. The checkout must match `.service-ref`; its own virtual environment is left unchanged. `make verify` builds the frontend, wheel and sdist, checks Python code, installs the wheel and tests real entry-point discovery and packaged assets. Run `make setup` again to return to editable development after `make verify`.

| Command                                                   | Purpose                                                                 |
| --------------------------------------------------------- | ----------------------------------------------------------------------- |
| `make frontend`                                           | Build frontend assets into `src/a13n_channels/static/` |
| `make dev`                                                | Watch frontend source and rebuild the host-mounted bundle               |
| `make build`                                              | Create wheel and sdist in `dist/`                                       |
| `make check`                                              | Check Python formatting and lint                                        |
| `make test`                                               | Test the installed plugin                                               |
| `make install HOST_PYTHON=/path/to/host/.venv/bin/python` | Build and install into a compatible Service environment                 |

The GitHub Actions workflow runs these build/test steps on pushes and pull requests and uploads wheel/sdist artifacts. It also rebuilds a wheel from the sdist using its prebuilt frontend. The separate tag-triggered release workflow follows the same build-artifact → publish-job pattern as Service. Configure your GitHub `pypi` environment with a `PYPI_TOKEN` secret and any required approvals, choose a unique distribution name, then update `pyproject.toml` and push a matching `vX.Y.Z` tag to publish. Release tags must match the current project version. The publish job only downloads verified artifacts and runs `uv publish`; it does not rebuild. Ordinary pushes and pull requests never publish. Deployment remains owned by your project.

## Open the App in Console

1. Prepare and start a Service checkout at `.service-ref` using its [local development guide](https://github.com/converge-ai-labs/agent-foundation/blob/2746cecd6c920ee683bc545f6c63df46285dd6c8/dev/service/README.md). Use `make dev-status` in that checkout to discover its URL.
2. Install this plugin into that checkout's Service environment with `make install HOST_PYTHON=/absolute/path/to/host/.venv/bin/python`.
3. Add the Channels settings described below and restart Service with the same environment and settings file. The upstream checkout launcher writes a fresh `var/dev/service.toml` on `make dev`; keep the App section in the configuration you pass to Service.
4. In Console, open **Settings → Organization → App plugins**, enable this App, and then open it under **Apps** in a workspace. Its route is `/workspace/{workspaceId}/apps/saurlax.channels/overview`.

For live frontend development, install this project editable into the prepared host environment with `uv pip install --python /path/to/host/.venv/bin/python -e .`, run `make dev` here, and reload the App page after each build. `make dev` emits the real host-mounted bundle; it does not run a separate React application. Restart Service after Python changes, installation or upgrades. Increment the version in `pyproject.toml` when changing shipped frontend bytes: Service versions its frontend URLs by the installed distribution version. The Makefile selects the wheel using the current manifest version even when older artifacts remain in `dist/`.

## Configure channels

Run one Service process in the `all` role for this App's current storage and listener model. Set its state path in the Service settings file and use a persistent volume. Configure the host Service encryption key to protect bot credentials and Weixin reply contexts.

```toml
[channels]
state_path = "/var/lib/a13n-channels/channels.sqlite3"
```

`A13N_CHANNELS__STATE_PATH` can also set the path. The App stores workspace-owned accounts and received messages in SQLite. Keep the database directory and host encryption key across restarts. The lock beside the database ensures one Service process owns its listeners.

In Console, enable **Channels** for the organization and open **Apps → Channels** in a workspace. Choose **Add channel** and enter the QQ bot's name, AppID, and AppSecret. The App connects to QQ using its outbound Gateway protocol. The account list refreshes connection state. Open the channel row to edit its credentials or delete it. The type and name are fixed. QQ credentials can be replaced by scan or entered manually. Replacing an AppID requires its matching AppSecret.

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

## Route messages to Agents

1. Enable **Channels** under **Settings → Organization → App plugins**.
2. Open **Apps → Channels** in the workspace and bind a QQ Bot or Weixin ClawBot.
3. Open **Delivery routes → Add route**, choose a published Agent and the message sources, then save the route with **Enable route** selected.

New matching messages trigger the target Agent. Channels loads the conversation tools automatically for each routed run. This workflow requires no changes in the Agent editor.

Select a channel account to see its supported conversation types. For chat channels, select an observed conversation ID or enter it directly. Leave the conversation field empty to match every conversation of the selected type. Choosing **All channel accounts** locks both conversation fields to their all-values settings. GitHub delivery uses the repository and sender settings described above.

Under **Filters**, choose whether group and guild messages must mention the bot or whether all received messages qualify. This applies across the route's sources. Private messages always pass this filter. New routes in Console default to mentions only. Older routes retain their saved behavior until a route-wide filter is selected. Ordinary group messages require QQ's receive-all-messages setting, and guild channels require the corresponding QQ permissions. The mention filter applies to the highest-priority matching route.

Choose a different published **Wake Agent** to decide whether a qualifying message reaches the target Agent. Configure the decision criteria in the wake Agent's instructions. Each decision runs in a separate Thread and returns a boolean. A positive decision delivers the original message to the target Agent under its configured Thread policy. A negative decision skips delivery. A failed decision appears in route status.

Matching sources are ranked by exact conversation ID, repository, specific account, then specific conversation type. Saving equally specific overlaps is rejected. A paused matching rule prevents fallback to a more general rule. Messages with no enabled matching rule stay in channel storage. Creating or enabling a route does not replay them.

Saving authorizes the route to submit inputs under the saving user's identity. The plugin loads that person's current workspace grants for each delivery and uses only the read and run verbs. Losing either grant pauses delivery. Saving as a different user starts fresh Threads. Routes created by the previous HTTP-based implementation require one save to authorize in-process delivery. Remove its delivery URL and Runner key settings when upgrading.

Each matched message is submitted as one user input containing its source metadata, text, and any available QQ attachment and structure metadata. New messages for the same delivery binding steer a compatible active Run. The host delivers immediately when supported and otherwise incorporates the message at a safe boundary. An idle Thread starts a Run. The per-message Thread policy always starts a fresh Run. The route selects one of four Thread policies:

| Policy | Thread scope |
| --- | --- |
| Reuse by conversation (default) | One Thread per channel account and conversation. |
| Reuse by sender | One Thread per sender within each channel account and conversation. |
| Reuse one Thread | All sources share an existing Thread in the workspace, or a Thread created on first delivery when the ID is empty. |
| New Thread for each message | A separate Session and Thread for every message. |

A new target Agent or saving user resets the route's managed Thread bindings. An explicit fixed Thread ID continues to target that Thread. Changing the Thread destination requires pending deliveries to settle.

The plugin submits directly through the host Service and uses the same idempotency key when retrying an uncertain submission.

Channel delivery adds these Harness tools to the target Agent for that run:

- `channel_list_messages` reads recent incoming messages and attachment metadata from the originating conversation.
- `channel_get_message` reads one full stored message, including expanded forwards and structured content.
- `channel_get_attachment` views a QQ image or sticker, or saves a QQ attachment into the Agent Environment.
- `channel_send_message` replies to the message that started the Run. It accepts text or QQ rich content. QQ and Weixin use that message's reply context.

When a Run receives multiple channel messages, use the intended message ID to select the conversation for reads. The reply tool selects the triggering message even when later inputs steer the same Run. Assistant text stays in Console. The tools resolve the conversation from the Run's delivery record and require a currently enabled route and account. Failed deliveries and unconfirmed sends appear in route status. A replay of the same send call never resends an unconfirmed reply.

### Read history and send proactive QQ messages

In the Agent editor, open **Advanced → Plugins → Add plugin**, choose **Channels**, and publish the Agent configuration. Set **Allowed channels** to a comma-separated list of channel account IDs, or leave it blank to allow all channel accounts in the current workspace for the active tools. Use the Channels App's account IDs. The **Channels Delivery** plugin is loaded automatically by routes.

- `channel_list_accounts` lists readable channel accounts.
- `channel_list_conversations` lists recent conversations for one account.
- `channel_read_messages` reads incoming messages from a selected conversation. Pass the last returned message ID as `before_id` to page backward.
- `channel_read_message` reads one full stored message.
- `channel_read_attachment` views or saves a QQ attachment using a message ID and its one-based attachment index.
- `channel_send_proactive` sends text or rich content to a QQ conversation without an incoming message. Pass the channel account ID, conversation kind, and conversation ID. QQ applies its platform permissions and delivery limits.

The configured Channels tools are available in regular, scheduled, and routed runs with workspace read access. Proactive sends additionally require workspace run access. Routed runs also receive the Channels Delivery tools for replying to the original conversation.

### QQ images, stickers, records, and cards

Use `channel_send_message` with `rich` set to one of the following objects. `channel_send_proactive` accepts the same content.

| Content | Example `rich` value |
| --- | --- |
| Generated image or sticker | `{"type":"media","path":"/workspace/sticker.gif"}` |
| Image by URL | `{"type":"media","url":"https://example.com/image.png"}` |
| Voice, video, or file | `{"type":"media","kind":"audio","path":"/workspace/reply.wav"}` |
| Markdown with buttons | `{"type":"markdown","content":"**Choose an option**","keyboard":{"id":"TEMPLATE_ID"}}` |
| Markdown template | `{"type":"markdown","custom_template_id":"TEMPLATE_ID","params":[{"key":"title","values":["Status"]}]}` |
| Ark or Embed card | `{"type":"ark","card":{"template_id":23,"kv":[{"key":"#DESC#","value":"Status"}]}}` |

An image can include `text` as a caption. Send captions for other media separately. QQ C2C and group sends upload bytes or a URL to obtain a media receipt. Large files use QQ's multipart upload flow. Guild and guild DM media use the image field or an image file. The QQ reply API accepts the same text and rich message objects with HTTPS URLs for media. QQ controls availability of file types, custom cards, keyboards, quotes, and proactive messages for each bot and conversation.

QQ history preserves images, custom sticker markers, emoji labels, audio, video, files, quotes, cards, and forwarded chat records supplied in Gateway events. Attachment URLs stay private. Pass `message_id` and the reported one-based `index` to `channel_get_attachment` to inspect an image or sticker. Supply an absolute Environment `save_path` for other attachments or to save a larger image. For older messages in the same conversation, identify the current delivery with `source_message_id`. Temporary QQ links can expire. A fresh message supplies a new link.

When an incoming QQ message has one to four images or stickers totaling at most 10 MiB, Channels includes them with the text as image inputs for a vision-capable target model. The route owner needs current workspace write access to create the image Assets. These Assets follow Service's workspace visibility and retention rules. Messages with more images, unavailable images, or a text-only model still deliver text and attachment metadata. Files, videos, and audio download on demand through `channel_get_attachment`.

Use `reference_message_id` on a reply to quote a stored message from that conversation when QQ supplies a reference identifier. To share a forwarded record's contents, send its text or a Markdown recap. QQ callback buttons and quick menus deliver an interaction to the matching route and use the event's native reply context. The bot needs the interaction intent enabled for those callbacks.

[QQ send API](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/send-receive/send.md) · [QQ buttons](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/trans/msg-btn.md) · [QQ SDK media uploads](https://github.com/tencent-connect/qqbot-nodejs/blob/main/USAGE.md#10-媒体上传与发送)

## Where to make changes

- `src/a13n_channels/app.py` registers the workspace account and routing APIs. `routing.py` matches sources, `delivery.py` submits inputs, and `tools.py` implements the conversation tools.
- `src/a13n_channels/qq.py` implements the QQ Gateway and send APIs. `src/a13n_channels/qq_content.py` defines rich messages and projects incoming QQ content. `src/a13n_channels/accounts.py` owns account and message storage.
- `frontend/src/index.tsx` mounts the Channels page in Console. App requests and cache keys are scoped by the host bridge.
- `pyproject.toml` owns entry-point discovery and wheel/sdist asset packaging.

The WeChat icon is adapted from [Simple Icons](https://simpleicons.org/) (CC0).

See [AGENTS.md](AGENTS.md) for project boundaries and Conventional Commit guidance.
