# a13n-channels

An a13n Service App for connecting messaging platforms and event sources to Agents. Manage channel accounts and delivery routes in Console. Agents can look up stored messages across allowed channel accounts and reply through the source channel.

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 18
    rankSpacing: 28
    padding: 10
    wrappingWidth: 135
    diagramPadding: 10
---
flowchart LR
    accTitle: a13n-channels architecture
    accDescr: Channel messages are filtered and routed to a13n Agents. Agents with Channels tools can query stored messages across allowed channel accounts.

    subgraph channels["Channels"]
        direction TB
        qq@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/qq.svg", label: "QQ Bot", h: 40, constraint: "on" }
        feishu@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/feishu.svg", label: "Feishu", h: 40, constraint: "on" }
        discord@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/discord.svg", label: "Discord", h: 40, constraint: "on" }
        weixin@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/weixin.svg", label: "WeChat", h: 40, constraint: "on" }
        dingtalk@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/dingtalk.svg", label: "DingTalk", h: 40, constraint: "on" }
        telegram@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/telegram.svg", label: "Telegram", h: 40, constraint: "on" }
        wecom@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/wecom.svg", label: "WeCom", h: 40, constraint: "on" }
        slack@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/slack.svg", label: "Slack", h: 40, constraint: "on" }
        mqtt@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/mqtt.svg", label: "MQTT", h: 40, constraint: "on" }
        github@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/github.svg", label: "GitHub", h: 40, constraint: "on" }
        githubApp@{ img: "https://raw.githubusercontent.com/saurlax/a13n-channels/main/frontend/src/icons/github.svg", label: "GitHub App", h: 40, constraint: "on" }
        qq ~~~ weixin ~~~ wecom ~~~ github
        feishu ~~~ dingtalk ~~~ slack ~~~ githubApp
        discord ~~~ telegram ~~~ mqtt ~~~ spacer[" "]:::spacer
    end

    subgraph a13n["a13n"]
        direction TB

        subgraph plugin["a13n-channels plugin"]
            adapters[Channel adapters]
            routers["Delivery routes<br/>Filters and routing"]
            history[("Workspace<br/>message history")]

            adapters --> routers
            adapters --> history
        end

        agentA([Agent A])
        agentB([Agent B])
        agentC([Agent C])

        routers --> agentA & agentB & agentC
        history <-.->|Cross-channel lookup| agentC
    end

    channels -->|Incoming messages| a13n
    %% Keep the top-to-bottom layout and show only the lookup arrowhead toward history.
    classDef channelLogo fill:none,stroke:none,stroke-width:0px
    class qq,feishu,discord,weixin,dingtalk,telegram,wecom,slack,mqtt,github,githubApp channelLogo
    classDef spacer fill:none,stroke:none
    linkStyle 14 marker-end:none
```

## Build and install

Use Python 3.13, uv, Git, Make, Node.js 24, and pnpm 10.30.3 on Linux or macOS. Prepare a [Service environment](https://github.com/converge-ai-labs/agent-foundation/blob/2746cecd6c920ee683bc545f6c63df46285dd6c8/dev/service/README.md) at the commit pinned in [`.service-ref`](.service-ref).

```bash
make setup
make verify
make install HOST_PYTHON=/absolute/path/to/service/.venv/bin/python
```

`make setup` prepares the pinned Service dependencies in this project's virtual environment. `make verify` tests the installed wheel. See the [development guide](docs/development.md) for the frontend workflow and release process.

## Try the integration

1. [Configure Channels and restart Service](docs/channels.md#configure-service).
2. In Console, enable **Channels** under **Settings → Organization → App plugins**, then [connect an account](docs/channels.md#connect-a-channel) in **Apps → Channels**.
3. Under **Delivery routes**, select a published Agent and a message source. Enable the route and send a test message.

Routed runs can read and reply in the source conversation. Add the **Channels** plugin to an Agent to [read history across allowed channel accounts](docs/channels.md#read-across-channels).

## Contributing

Bug reports, documentation fixes, and focused pull requests are welcome. Use [GitHub Issues](https://github.com/saurlax/a13n-channels/issues) to report bugs or discuss design proposals. See the [development guide](docs/development.md#build-and-test) for the checks to run before opening a pull request.
