import { React } from "@a13n/console";
import dingtalk from "./icons/dingtalk.svg?inline";
import discord from "./icons/discord.svg?inline";
import feishu from "./icons/feishu.svg?inline";
import github from "./icons/github.svg?inline";
import mqtt from "./icons/mqtt.svg?inline";
import qq from "./icons/qq.svg?inline";
import slack from "./icons/slack.svg?inline";
import telegram from "./icons/telegram.svg?inline";
import wecom from "./icons/wecom.svg?inline";
import weixin from "./icons/weixin.svg?inline";

const icons = { qq, weixin, github, github_app: github, feishu, slack, discord, dingtalk, wecom, telegram, mqtt };
export const builtInPlatforms = ["qq", "weixin", "github", "github_app"] as const;
export type Platform = string;

export function ChannelIcon({ platform }: { platform: Platform }) {
  const source = icons[platform as keyof typeof icons];
  if (!source) return null;
  return <img className="a13n-channel-icon" data-platform={platform}
    src={source} alt="" width={16} height={16} />;
}
