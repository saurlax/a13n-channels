import {
  React, ApiError, Badge, Button, ChoiceField, Empty, ErrorPage, Input, Link, Loading, Page, Switch,
  useAccess, useApp, useNavigate, useQuery, useQueryClient, useTranslation,
} from "@a13n/console";
import { ChannelIcon, type Platform } from "./channel-icon";
import { ChannelSelect } from "./select";

const english = {
  title: "Channels", name: "Name", sources: "Sources", channel: "Channel account",
  kind: "Conversation type", conversation: "Conversation", conversationId: "Conversation ID", agent: "Target Agent",
  status: "Status", enable: "Enable route", enabled: "Enabled", paused: "Paused", attention: "Needs attention",
  add: "Add route", routes: "Delivery routes", empty: "No routes yet", missingRoute: "Route not found",
  addSource: "Add source", removeSource: "Remove source", selectAgent: "Select a published Agent",
  anyChannel: "All channel accounts", anyConversation: "All conversations", conversationPlaceholder: "All conversations when empty", removeConversation: "Remove conversation",
  sourceLimit: "Use at most 100 source rules, counting each conversation ID.",
  missing: "Unavailable", selectChannel: "Select a channel account",
  private: "Private chat", group: "Group chat", guild: "Guild channel", guild_dm: "Guild direct message",
  all: "All", repository: "Repository", allowedUsers: "Allowed users", user: "User",
  addUser: "Add users", removeUser: "Remove allowed users", usersPlaceholder: "Separate user IDs with commas",
  invalidUsers: "Choose a channel account and enter valid users for each row.",
  githubError: "Enter GitHub repositories and allowed users.",
  filters: "Filters", multiparty: "Group and channel messages",
  mentionsOnly: "Only @bot", allMessages: "All messages", originalFilters: "Keep current source filters",
  wakeAgent: "Wake Agent", noWakeAgent: "None",
  wakeHelp: "Decides whether matching messages reach the target Agent. Configure the criteria in the wake Agent’s instructions.",
  wakeError: "Choose a different published Agent in this workspace.",
  issue: "Issue", pullRequest: "Pull Request", channelKind: "Channel", thread: "Thread", topic: "MQTT topic", allKinds: "All conversation types",
  cancel: "Cancel", save: "Save", remove: "Delete", deleteTitle: "Delete route",
  deleteError: "The route could not be deleted. Try again.",
  deleteBody: "New messages will stop using this route. Existing Console conversations are retained.",
  unconfigured: "Channels storage is not configured.",
  threadPolicy: "Thread behavior", fixedThreadId: "Thread ID (optional)",
  contextIdle: "Context retention (seconds)",
  contextIdleHelp: "After this much idle time, the next message starts a new Session.",
  policies: { conversation: "Reuse by conversation", sender: "Reuse by sender", fixed: "Reuse one Thread", message: "New Thread for each message" },
  policyHelp: {
    conversation: "Each channel account and conversation has its own history.",
    sender: "Each sender in a conversation has their own history.",
    fixed: "All matched sources share history. Leave the ID empty to create the Thread on the first delivery.",
    message: "Each message starts a separate Thread and Session.",
  },
  invalidThread: "Enter the ID of an active conversation Thread in this workspace.",
  busyThread: "Wait for deliveries to finish before changing the Thread destination.",
  routeAuthority: "Saving lets this route use your permission to run Agents in this workspace.",
  needsAuthorization: "Save to authorize",
  conflict: "This source overlaps a route with the same priority.",
  error: "The route could not be saved. Check its sources and the destination Agent.",
};

const chinese: typeof english = {
  title: "渠道", name: "名称", sources: "来源", channel: "渠道账号",
  kind: "会话类型", conversation: "会话", conversationId: "会话 ID", agent: "目标 Agent",
  status: "状态", enable: "启用路由", enabled: "已启用", paused: "已停用", attention: "投递异常",
  add: "添加路由", routes: "投递路由", empty: "暂无投递路由", missingRoute: "找不到路由",
  addSource: "添加来源", removeSource: "移除来源", selectAgent: "选择已发布的 Agent",
  anyChannel: "全部渠道账号", anyConversation: "全部会话", conversationPlaceholder: "留空匹配全部会话", removeConversation: "移除会话",
  sourceLimit: "来源规则最多 100 条，每个会话 ID 各算一条。",
  missing: "不可用", selectChannel: "选择渠道账号",
  private: "私聊", group: "群聊", guild: "频道子频道", guild_dm: "频道私信",
  all: "全部", repository: "仓库", allowedUsers: "允许的用户", user: "用户",
  addUser: "添加用户", removeUser: "移除允许的用户", usersPlaceholder: "多个用户 ID 用逗号分隔",
  invalidUsers: "请为每行选择渠道账号并填写有效的用户。",
  githubError: "请填写 GitHub 仓库和允许触发的用户。",
  filters: "筛选器", multiparty: "多人聊天消息",
  mentionsOnly: "仅 @ 机器人", allMessages: "全量消息", originalFilters: "保留已有来源筛选",
  wakeAgent: "唤醒 Agent", noWakeAgent: "不使用",
  wakeHelp: "决定匹配的消息是否交给目标 Agent。判断条件写在唤醒 Agent 的指令里。",
  wakeError: "请选择当前工作区内另一个已发布的 Agent。",
  issue: "Issue", pullRequest: "Pull Request", channelKind: "频道", thread: "话题", topic: "MQTT 主题", allKinds: "全部会话类型",
  cancel: "取消", save: "保存", remove: "删除", deleteTitle: "删除路由",
  deleteError: "未能删除路由，请重试。",
  deleteBody: "新消息将不再使用这条路由。已有的 Console 会话会保留。",
  unconfigured: "尚未配置渠道存储。",
  threadPolicy: "Thread 投递模式", fixedThreadId: "Thread ID（选填）",
  contextIdle: "上下文保留时间（秒）",
  contextIdleHelp: "空闲达到此时长后，下一条消息会开启新会话。",
  policies: { conversation: "按会话复用", sender: "按会话内发送者复用", fixed: "固定 Thread", message: "每条消息新建 Thread" },
  policyHelp: {
    conversation: "每个渠道账号下的每个会话都有独立的上下文。",
    sender: "同一会话内，每个发送者都有独立的上下文。",
    fixed: "所有匹配的来源共享上下文。留空则在首次投递时创建 Thread。",
    message: "每条消息都创建独立的 Thread 和 Session。",
  },
  invalidThread: "请输入当前工作区内未归档的会话 Thread ID。",
  busyThread: "请等当前投递完成后再更改 Thread 投递目标。",
  routeAuthority: "保存后，这条路由会使用你在当前工作区的运行权限。",
  needsAuthorization: "请重新保存以授权投递",
  conflict: "这个来源与另一条同优先级的路由重叠。",
  error: "操作未完成。请检查路由来源和目标 Agent。",
};

type Account = { id: string; name: string; platform: Platform; conversation_kinds: string[] };
type Agent = { id: string; name: string };
type Source = { channel_id: string; kind: string; conversation_id: string; mentions_only?: boolean; repository?: string | null };
type MultipartyFilter = "all" | "mentions";
type ThreadPolicy = keyof typeof english.policies;
const threadPolicies: ThreadPolicy[] = ["conversation", "sender", "fixed", "message"];
type RouteInput = {
  name: string; agent_id: string; enabled: boolean; sources: Source[];
  thread_policy: ThreadPolicy; fixed_thread_id: string | null;
  context_idle_seconds: number;
  multiparty_messages: MultipartyFilter | null; wake_agent_id: string | null;
  allowed_users: Record<string, string[]>;
};
type Route = RouteInput & { id: string; attention: number; needs_authorization: boolean };
type RouteList = { configured: boolean; routes: Route[] };
type Text = typeof english;

type SourceDraft = Pick<Source, "channel_id" | "kind" | "mentions_only"> & {
  conversations: string[]; input: string;
};
type UserDraft = { id: number; channelId: string; users: string };

const isGitHub = (account?: Account) => account?.platform === "github" || account?.platform === "github_app";
const repoPattern = "[A-Za-z0-9_\\-]+/[A-Za-z0-9_.\\-]+";
const validRepo = (value: string) => value.length <= 150 && new RegExp(`^${repoPattern}$`).test(value);
const splitList = (value: string) => value.split(/[,，]/).map(item => item.trim()).filter(Boolean);
const accountOption = (account: Account) => ({
  value: account.id, label: account.name, icon: <ChannelIcon platform={account.platform} />,
});

const sourceDefaults = (): SourceDraft => ({ channel_id: "", kind: "*", conversations: [], input: "" });

function draftSources(sources: Source[]): SourceDraft[] {
  const rows: SourceDraft[] = [];
  const groups = new Map<string, SourceDraft>();
  for (const source of sources) {
    const { channel_id, kind, mentions_only, conversation_id, repository } = source;
    const draft = { ...sourceDefaults(), channel_id, kind, mentions_only };
    const conversation = repository ?? conversation_id;
    const key = JSON.stringify([channel_id, kind, Boolean(mentions_only), Boolean(repository)]);
    const existing = conversation === "*" ? undefined : groups.get(key);
    if (existing) {
      existing.conversations.push(conversation);
      continue;
    }
    const row = { ...draft, conversations: conversation === "*" ? [] : [conversation] };
    rows.push(row);
    if (row.conversations.length) groups.set(key, row);
  }
  return rows;
}

function conversationIds({ conversations, input }: SourceDraft, github: boolean): string[] {
  const ids = [...conversations, input.trim()].filter(Boolean);
  return [...new Set(github ? ids.map(id => id.toLowerCase()) : ids)];
}

function kindName(kind: string, text: Text) {
  switch (kind) {
    case "*": return text.allKinds;
    case "private": return text.private;
    case "group": return text.group;
    case "guild": return text.guild;
    case "guild_dm": return text.guild_dm;
    case "channel": return text.channelKind;
    case "thread": return text.thread;
    case "topic": return text.topic;
    case "issue": return text.issue;
    case "pull_request": return text.pullRequest;
    default: return kind;
  }
}

async function responseJSON<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(String(response.status));
  return response.json();
}

function TrashIcon() {
  return (
    <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7" />
    </svg>
  );
}

function SourceEditor({ source, accounts, update, remove, text, disabled }: {
  source: SourceDraft; accounts: Account[]; update: (value: SourceDraft) => void;
  remove?: () => void; text: Text; disabled: boolean;
}) {
  const app = useApp();
  const optionsId = React.useId();
  const account = accounts.find(item => item.id === source.channel_id);
  const github = isGitHub(account);
  const supported = ["*", ...(account?.conversation_kinds ?? [])];
  const kind = account ? source.kind : "*";
  const conversationLabel = github ? text.repository : text.conversationId;
  function addConversation() {
    const value = source.input.trim();
    if (value === "*" || (github && value && !validRepo(value))) return;
    update({ ...source, conversations: conversationIds(source, github), input: "" });
  }
  const observed = useQuery({
    queryKey: app.queryKey("route-sources", source.channel_id, source.kind),
    enabled: Boolean(account && !github && !disabled && kind !== "*"),
    queryFn: async ({ signal }) => responseJSON<{ id: string }[]>(await app.request(
      "/route-sources?" + new URLSearchParams({ channel_id: source.channel_id, kind: source.kind }),
      { signal },
    )),
  });
  return (
    <tr>
      <td>
        <ChannelSelect label={text.channel} required disabled={disabled} value={source.channel_id || null}
          placeholder={text.selectChannel}
          options={[
            { value: "*", label: text.anyChannel },
            ...accounts.map(accountOption),
            ...(source.channel_id && source.channel_id !== "*" && !account
              ? [{ value: source.channel_id, label: `${text.missing} (${source.channel_id})` }] : []),
          ]}
          onChange={id => update({ ...sourceDefaults(), channel_id: id })} />
      </td>
      <td>
        <ChannelSelect label={text.kind} disabled={disabled || !account} value={kind}
          options={(supported.includes(kind) ? supported : [kind, ...supported])
            .map(value => ({ value, label: value === "*" && account ? text.all : kindName(value, text) }))}
          onChange={kind => update({ ...source, kind, mentions_only: false,
            conversations: github ? source.conversations : [], input: github ? source.input : "" })} />
      </td>
      <td>
        {!account || (!github && kind === "*") ? (
          <span className="a13n-route-all-conversations">{text.anyConversation}</span>
        ) : (
          <div className="a13n-route-conversations">
            {source.conversations.map(id => (
              <span className="a13n-route-conversation-tag" key={id}>
                <span title={id}>{id}</span>
                {!disabled && <button type="button" aria-label={`${text.removeConversation} ${id}`}
                  onClick={() => update({ ...source, conversations: source.conversations.filter(value => value !== id) })}>
                  ×
                </button>}
              </span>
            ))}
            <Input unstyled aria-label={conversationLabel} disabled={disabled} list={github ? undefined : optionsId}
              placeholder={github ? "owner/repo" : source.conversations.length ? conversationLabel : text.conversationPlaceholder}
              value={source.input} maxLength={github ? 150 : 512}
              pattern={github ? repoPattern : String.raw`(?!\s*\*\s*$).*`}
              onChange={event => update({ ...source, input: event.currentTarget.value })}
              onBlur={() => { if (source.input) addConversation(); }}
              onKeyDown={event => {
                if (event.key === "Enter" && !event.nativeEvent.isComposing) {
                  event.preventDefault();
                  if (event.currentTarget.reportValidity()) addConversation();
                }
              }} />
            <datalist id={optionsId}>
              {observed.data?.filter(item => !source.conversations.includes(item.id))
                .map(item => <option key={item.id} value={item.id} />)}
            </datalist>
          </div>
        )}
      </td>
      {remove !== undefined && <td className="a13n-route-source-action">
        <Button type="button" size="icon-sm" variant="ghost" disabled={disabled} aria-label={text.removeSource}
          title={text.removeSource} onClick={remove}><TrashIcon /></Button>
      </td>}
    </tr>
  );
}

function RouteDelete({ route, text, dismiss, onDeleted }: {
  route: Route; text: Text; dismiss: () => void; onDeleted: () => void;
}) {
  const app = useApp();
  const dialog = React.useRef<HTMLDialogElement>(null);
  const titleId = React.useId();
  const [pending, setPending] = React.useState(false);
  const [failed, setFailed] = React.useState(false);
  React.useEffect(() => { dialog.current?.showModal(); }, []);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending) return;
    setPending(true);
    setFailed(false);
    try {
      await app.request(`/routes/${encodeURIComponent(route.id)}`, { method: "DELETE" });
      onDeleted();
    } catch {
      setFailed(true);
      setPending(false);
    }
  }

  return (
    <dialog className="a13n-qq-dialog" ref={dialog} aria-labelledby={titleId}
      onCancel={event => { event.preventDefault(); if (!pending) dismiss(); }}>
      <form onSubmit={submit}>
        <header className="a13n-channel-edit-header">
          <h2 id={titleId}>{text.deleteTitle}</h2>
          <p>{route.name}</p>
        </header>
        <p>{text.deleteBody}</p>
        {failed && <p role="alert">{text.deleteError}</p>}
        <div className="a13n-qq-actions">
          <Button type="button" variant="outline" disabled={pending} onClick={dismiss}>{text.cancel}</Button>
          <Button type="submit" variant="destructive" disabled={pending}>{text.deleteTitle}</Button>
        </div>
      </form>
    </dialog>
  );
}

function RouteEditor({ route, accounts, agents, text, back, canEdit }: {
  route?: Route; accounts: Account[]; agents: Agent[]; text: Text; back: string; canEdit: boolean;
}) {
  const app = useApp();
  const client = useQueryClient();
  const navigate = useNavigate();
  const [name, setName] = React.useState(route?.name ?? "");
  const [agent, setAgent] = React.useState(route?.agent_id ?? "");
  const [wakeAgent, setWakeAgent] = React.useState(route?.wake_agent_id ?? "");
  const legacyFilters = route?.multiparty_messages == null && Boolean(route?.sources.some(source => source.mentions_only));
  const [multiparty, setMultiparty] = React.useState<MultipartyFilter | "legacy">(
    route?.multiparty_messages ?? (legacyFilters ? "legacy" : route ? "all" : "mentions"),
  );
  const [policy, setPolicy] = React.useState<ThreadPolicy>(route?.thread_policy ?? "conversation");
  const [threadId, setThreadId] = React.useState(route?.fixed_thread_id ?? "");
  const [contextIdle, setContextIdle] = React.useState(String(route?.context_idle_seconds ?? 3600));
  const threadHelpId = React.useId();
  const contextIdleHelpId = React.useId();
  const enabledId = React.useId();
  const [enabled, setEnabled] = React.useState(route?.enabled ?? true);
  const [sources, setSources] = React.useState(
    () => (route ? draftSources(route.sources) : [sourceDefaults()]).map((value, id) => ({ id, value })),
  );
  const [allowedUsers, setAllowedUsers] = React.useState<UserDraft[]>(() =>
    Object.entries(route?.allowed_users ?? {}).map(([channelId, users], id) => ({ id, channelId, users: users.join(", ") })),
  );
  const nextUser = React.useRef(allowedUsers.length);
  const nextSource = React.useRef(sources.length);
  const [deleting, setDeleting] = React.useState(false);
  const [pending, setPending] = React.useState(false);
  const [error, setError] = React.useState("");
  const disabled = pending || !canEdit;
  const canRemoveSource = canEdit && sources.length > 1;
  const managesContext = policy !== "message" && !(policy === "fixed" && threadId.trim());

  function returnToList() {
    void client.invalidateQueries({ queryKey: app.queryKey("channel-routes") });
    navigate(back);
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (disabled) return;
    const usersByAccount = new Map<string, string[]>();
    for (const row of allowedUsers) {
      const users = splitList(row.users);
      const account = accounts.find(item => item.id === row.channelId);
      if (!account || !users.length || (isGitHub(account) && users.some(user => !/^[A-Za-z0-9][A-Za-z0-9-]{0,38}$/.test(user)))) {
        setError(text.invalidUsers);
        return;
      }
      usersByAccount.set(account.id, [...new Set([...(usersByAccount.get(account.id) ?? []), ...users])]);
    }
    const savedSources: Source[] = [];
    for (const { value } of sources) {
      const github = isGitHub(accounts.find(item => item.id === value.channel_id));
      const ids = conversationIds(value, github);
      if (github && (ids.some(id => !validRepo(id)) || (enabled && (!ids.length || !usersByAccount.has(value.channel_id))))) {
        setError(text.githubError);
        return;
      }
      for (const id of ids.length ? ids : ["*"]) {
        savedSources.push({
          channel_id: value.channel_id, kind: value.kind, conversation_id: github ? "*" : id,
          ...(github && id !== "*" ? { repository: id } : {}),
          ...(multiparty === "legacy" ? { mentions_only: value.mentions_only } : {}),
        });
      }
    }
    if (savedSources.length > 100) {
      setError(text.sourceLimit);
      return;
    }
    setPending(true);
    setError("");
    try {
      await app.request(route ? `/routes/${encodeURIComponent(route.id)}` : "/routes", {
        method: route ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name, agent_id: agent, enabled,
          sources: savedSources, allowed_users: Object.fromEntries(usersByAccount),
          multiparty_messages: multiparty === "legacy" ? null : multiparty, wake_agent_id: wakeAgent || null,
          thread_policy: policy, fixed_thread_id: policy === "fixed" ? threadId.trim() || null : null,
          context_idle_seconds: managesContext ? Number(contextIdle) : route?.context_idle_seconds ?? 3600,
        } satisfies RouteInput),
      });
      returnToList();
    } catch (error) {
      if (error instanceof ApiError && error.details.reason === "thread_destination_busy") {
        setError(text.busyThread);
      } else if (error instanceof ApiError && error.details.field === "repository") {
        setError(text.githubError);
      } else if (error instanceof ApiError && error.details.field === "allowed_users") {
        setError(text.invalidUsers);
      } else if (error instanceof ApiError && error.details.field === "wake_agent_id") {
        setError(text.wakeError);
      } else if (error instanceof ApiError && error.details.field === "fixed_thread_id") {
        setError(text.invalidThread);
      } else {
        setError(error instanceof ApiError && error.code === "conflict" ? text.conflict : text.error);
      }
      setPending(false);
    }
  }

  return (
    <Page title={route?.name ?? text.add} back={back} backLabel={text.routes}>
      <form className="a13n-route-form" onSubmit={submit}>
        <div className="a13n-route-settings">
          <label>{text.name}<Input required maxLength={80} disabled={disabled}
            value={name} onChange={event => setName(event.currentTarget.value)} /></label>
          <ChoiceField label={text.agent} required disabled={disabled} value={agent || undefined}
            placeholder={text.selectAgent} onValueChange={value => { setAgent(value); if (value === wakeAgent) setWakeAgent(""); }}
            options={[
              ...agents.map(item => ({ value: item.id, label: item.name })),
              ...(agent && !agents.some(item => item.id === agent)
                ? [{ value: agent, label: `${text.missing} (${agent})`, disabled: true }] : []),
            ]} />
          <div className="a13n-route-enabled">
            <label htmlFor={enabledId}>{text.enable}</label>
            <Switch id={enabledId} checked={enabled} disabled={disabled} onCheckedChange={setEnabled} />
          </div>
        </div>
        <section className="a13n-route-sources" aria-label={text.threadPolicy}>
          <div className="a13n-route-settings">
            <ChoiceField label={text.threadPolicy} disabled={disabled} value={policy}
              aria-describedby={threadHelpId} onValueChange={value => setPolicy(value as ThreadPolicy)}
              options={threadPolicies.map(value => ({ value, label: text.policies[value] }))} />
            {policy === "fixed" && <label>{text.fixedThreadId}<Input disabled={disabled} maxLength={512}
              value={threadId} onChange={event => setThreadId(event.currentTarget.value)}
              aria-describedby={threadHelpId} /></label>}
            {managesContext && <label>{text.contextIdle}<Input type="number" required disabled={disabled}
              min={1} max={Number.MAX_SAFE_INTEGER} step={1} value={contextIdle}
              onChange={event => setContextIdle(event.currentTarget.value)} aria-describedby={contextIdleHelpId} /></label>}
          </div>
          <p id={threadHelpId} className="a13n-route-thread-help">{text.policyHelp[policy]}</p>
          {managesContext && <p id={contextIdleHelpId} className="a13n-route-thread-help">{text.contextIdleHelp}</p>}
        </section>
        <section className="a13n-route-sources" aria-label={text.sources}>
          <div className="a13n-route-section-header">
            <h2>{text.sources}</h2>
            {canEdit && <Button type="button" size="sm" variant="outline" disabled={pending || sources.length >= 100}
              onClick={() => {
                const id = nextSource.current++;
                setSources(current => [...current, { id, value: sourceDefaults() }]);
              }}>{text.addSource}</Button>}
          </div>
          <div className="a13n-route-source-table">
            <table aria-label={text.sources}>
              <colgroup><col /><col /><col />{canRemoveSource && <col className="a13n-route-source-action" />}</colgroup>
              <thead><tr>
                <th scope="col">{text.channel}</th>
                <th scope="col">{text.kind}</th>
                <th scope="col">{text.conversation}</th>
                {canRemoveSource && <th scope="col" aria-label={text.removeSource} />}
              </tr></thead>
              <tbody>{sources.map(item => <SourceEditor key={item.id} source={item.value} accounts={accounts}
                text={text} disabled={disabled}
                update={value => setSources(current => current.map(source => source.id === item.id ? { ...source, value } : source))}
                remove={canRemoveSource ? () => setSources(current => current.filter(source => source.id !== item.id)) : undefined} />)}
              </tbody>
            </table>
          </div>
        </section>
        <section className="a13n-route-sources" aria-label={text.filters}>
          <div className="a13n-route-section-header"><h2>{text.filters}</h2></div>
          <div className="a13n-route-settings">
            <ChoiceField label={text.multiparty} disabled={disabled} value={multiparty}
              onValueChange={value => setMultiparty(value as MultipartyFilter | "legacy")}
              options={[
                { value: "mentions", label: text.mentionsOnly },
                { value: "all", label: text.allMessages },
                ...(legacyFilters ? [{ value: "legacy", label: text.originalFilters }] : []),
              ]} />
            <ChoiceField label={text.wakeAgent} disabled={disabled} value={wakeAgent || "none"}
              onValueChange={value => setWakeAgent(value === "none" ? "" : value)}
              options={[
                { value: "none", label: text.noWakeAgent },
                ...agents.filter(item => item.id !== agent).map(item => ({ value: item.id, label: item.name })),
                ...(wakeAgent && !agents.some(item => item.id === wakeAgent)
                  ? [{ value: wakeAgent, label: `${text.missing} (${wakeAgent})`, disabled: true }] : []),
              ]} />
          </div>
          <p className="a13n-route-thread-help">{text.wakeHelp}</p>
          <div className="a13n-route-section-header">
            <h3>{text.allowedUsers}</h3>
            {canEdit && <Button type="button" size="sm" variant="outline" disabled={disabled || !accounts.length || allowedUsers.length >= 100}
              onClick={() => {
                const id = nextUser.current++;
                setAllowedUsers(current => [...current, { id, channelId: "", users: "" }]);
              }}>{text.addUser}</Button>}
          </div>
          {allowedUsers.length > 0 && <div className="a13n-route-source-table a13n-route-user-table">
            <table aria-label={text.allowedUsers}>
              <colgroup><col /><col />{canEdit && <col className="a13n-route-source-action" />}</colgroup>
              <thead><tr>
                <th scope="col">{text.channel}</th><th scope="col">{text.user}</th>
                {canEdit && <th scope="col" aria-label={text.removeUser} />}
              </tr></thead>
              <tbody>{allowedUsers.map(row => (
                <tr key={row.id}>
                  <td><ChannelSelect label={text.channel} required disabled={disabled} value={row.channelId || null}
                    placeholder={text.selectChannel}
                    options={[
                      ...accounts.map(accountOption),
                      ...(row.channelId && !accounts.some(account => account.id === row.channelId)
                        ? [{ value: row.channelId, label: `${text.missing} (${row.channelId})` }] : []),
                    ]}
                    onChange={channelId => setAllowedUsers(current => current.map(item =>
                      item.id === row.id ? { ...item, channelId, users: "" } : item))} /></td>
                  <td><Input aria-label={text.user} required disabled={disabled} value={row.users}
                    placeholder={isGitHub(accounts.find(account => account.id === row.channelId)) ? "octocat" : text.usersPlaceholder}
                    onChange={event => {
                      const users = event.currentTarget.value;
                      setAllowedUsers(current => current.map(item => item.id === row.id ? { ...item, users } : item));
                    }} /></td>
                  {canEdit && <td className="a13n-route-source-action">
                    <Button type="button" size="icon-sm" variant="ghost" disabled={disabled} aria-label={text.removeUser}
                      title={text.removeUser}
                      onClick={() => setAllowedUsers(current => current.filter(item => item.id !== row.id))}>
                      <TrashIcon />
                    </Button>
                  </td>}
                </tr>
              ))}</tbody>
            </table>
          </div>}
        </section>
        {error && <p role="alert">{error}</p>}
        {canEdit && <>
          <p className="a13n-route-notice">{text.routeAuthority}</p>
          <div className="a13n-qq-actions a13n-route-form-actions">
            {route && <Button type="button" className="a13n-channel-delete" variant="destructive-outline"
              disabled={pending} onClick={() => setDeleting(true)}>{text.remove}</Button>}
            <Button type="button" variant="outline" disabled={pending} onClick={() => navigate(back)}>{text.cancel}</Button>
            <Button type="submit" disabled={pending || !agents.some(item => item.id === agent) || !name.trim()}>
              {text.save}
            </Button>
          </div>
        </>}
      </form>
      {route && deleting && <RouteDelete route={route} text={text} dismiss={() => setDeleting(false)} onDeleted={returnToList} />}
    </Page>
  );
}

export function RoutePage({ nav, base, routeKey }: { nav: React.ReactNode; base: string; routeKey?: string }) {
  const app = useApp();
  const { can, workspace } = useAccess();
  const { i18n } = useTranslation();
  const text = i18n.language.startsWith("zh") ? chinese : english;
  const navigate = useNavigate();
  const routes = useQuery({
    queryKey: app.queryKey("channel-routes"),
    queryFn: async ({ signal }): Promise<RouteList> => responseJSON(await app.request("/routes", { signal })),
    refetchInterval: 5000,
  });
  const accounts = useQuery({
    queryKey: app.queryKey("channel-accounts"),
    queryFn: async ({ signal }) => responseJSON<{ accounts: Account[] }>(await app.request("/accounts", { signal })),
  });
  const agents = useQuery({
    queryKey: app.queryKey("route-agents"),
    queryFn: async ({ signal }): Promise<Agent[]> => {
      const items: Agent[] = [];
      let cursor: string | null = null;
      do {
        const params = cursor ? "?" + new URLSearchParams({ cursor }) : "";
        const page: { items: Agent[]; next_cursor: string | null } = await responseJSON(
          await app.request("/route-agents" + params, { signal }),
        );
        items.push(...page.items);
        cursor = page.next_cursor;
      } while (cursor);
      return items;
    },
  });
  const data = routes.data;
  const canEdit = Boolean(can("write") && !workspace?.archived_at);
  const routeList = data?.routes ?? [];
  const listPath = `${base}?tab=routes`;
  const add = canEdit
    ? <Button render={<Link to={`${base}/routes/new`} />}>{text.add}</Button> : undefined;
  const loading = routes.isPending || accounts.isPending || agents.isPending;
  const error = routes.error ?? accounts.error ?? agents.error;
  if (routeKey) {
    const route = routeList.find(item => item.id === routeKey);
    const create = routeKey === "new" && canEdit;
    if (!loading && !error && data?.configured && (create || route)) {
      return <RouteEditor key={routeKey} route={route} canEdit={canEdit} back={listPath}
        text={text} accounts={accounts.data?.accounts ?? []} agents={agents.data ?? []} />;
    }
    return (
      <Page title={text.routes} back={listPath} backLabel={text.routes}>
        {loading ? <Loading /> : error ? <ErrorPage error={error} /> :
          <Empty title={data?.configured ? text.missingRoute : text.unconfigured} />}
      </Page>
    );
  }
  const accountsById = new Map(accounts.data?.accounts.map(account => [account.id, account]));
  return (
    <Page title={text.title} actions={routeList.length > 0 ? add : undefined}>
      {nav}
      {routes.isPending || accounts.isPending || agents.isPending ? <Loading /> :
        routes.isError || accounts.isError || agents.isError
          ? <ErrorPage error={routes.error ?? accounts.error ?? agents.error} /> :
        !data?.configured ? <Empty title={text.unconfigured} /> : <>
          {!routeList.length ? <Empty title={text.empty} action={add} /> :
            <div className="a13n-qq-list">
              <table role="table" aria-label={text.sources}>
                <thead><tr>{[text.name, text.sources, text.agent, text.status].map(label =>
                  <th key={label} scope="col">{label}</th>)}</tr></thead>
                <tbody>
                  {routeList.map(route => {
                    const href = `${base}/routes/${encodeURIComponent(route.id)}`;
                    const channelIds = [...new Set(route.sources.map(source => source.channel_id))];
                    const allChannels = channelIds.includes("*");
                    const channels = channelIds.map(id => ({
                      name: accountsById.get(id)?.name ?? id,
                      platform: accountsById.get(id)?.platform,
                    }));
                    const first = channels[0];
                    const channelNames = allChannels ? text.anyChannel : channels.map(channel => channel.name).join(", ");
                    return (
                      <tr key={route.id} data-editable onClick={() => navigate(href)}>
                        <th scope="row" className="a13n-channel-identity">
                          <Link to={href} onClick={event => event.stopPropagation()}><strong>{route.name}</strong></Link>
                        </th>
                        <td data-label={text.sources} title={channelNames}>
                          <div className="a13n-route-source-summary">
                            <Badge variant="secondary" className="a13n-route-source-badge">
                              {!allChannels && first.platform && <ChannelIcon platform={first.platform} />}
                              <span>{allChannels ? text.all : first.name}</span>
                            </Badge>
                            {!allChannels && channels.length > 1 &&
                              <span className="a13n-route-source-count">+{channels.length - 1}</span>}
                          </div>
                        </td>
                        <td data-label={text.agent}>{agents.data?.find(item => item.id === route.agent_id)?.name ?? route.agent_id}</td>
                        <td data-label={text.status}>
                          <span className="a13n-channel-status" data-tone={route.enabled ? "success" : "neutral"}>
                            <span className="a13n-channel-status-dot" aria-hidden="true" />
                            {route.enabled ? text.enabled : text.paused}
                          </span>
                          {route.needs_authorization && <span className="a13n-channel-status" data-tone="warning">
                            {text.needsAuthorization}
                          </span>}
                          {route.attention > 0 && <span className="a13n-channel-status" data-tone="warning">
                            {text.attention} ({route.attention})
                          </span>}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>}
        </>}
    </Page>
  );
}
