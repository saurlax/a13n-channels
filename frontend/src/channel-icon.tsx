import { React } from "@a13n/console";
import qq from "./icons/qq.svg?inline";
import weixin from "./icons/weixin.svg?inline";

const icons = { qq, weixin };
export type Platform = keyof typeof icons;

export function ChannelIcon({ platform }: { platform: Platform }) {
  return <img className="a13n-channel-icon" data-platform={platform}
    src={icons[platform]} alt="" width={16} height={16} />;
}
