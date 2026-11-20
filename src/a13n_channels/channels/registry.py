"""Account descriptors for Channels-managed outbound connectors."""

from a13n_channels.channels import dingtalk, discord, feishu, mqtt, slack, telegram, wecom
from a13n_channels.channels.adapter import Adapter

ADAPTERS: dict[str, Adapter] = {
    item.key: item
    for item in (
        feishu.ADAPTER,
        slack.ADAPTER,
        discord.ADAPTER,
        dingtalk.ADAPTER,
        wecom.ADAPTER,
        telegram.ADAPTER,
        mqtt.ADAPTER,
    )
}
