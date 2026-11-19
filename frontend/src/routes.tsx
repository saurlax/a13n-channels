import {
  React, ApiError, Button, ChoiceField, Empty, ErrorPage, Input, Link, Loading, Page, Switch,
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
  filters: "Filters", multiparty: "Group and channel messages",
  mentionsOnly: "Only @bot", allMessages: "All messages", originalFilters: "Keep current source filters",
  wakeAgent: "Wake Agent", noWakeAgent: "None",
  wakeHelp: "Decides whether matching messages reach the target Agent. Configure the criteria in the wake Agent’s instructions.",
  wakeError: "Choose a different published Agent in this workspace.",
  channelKind: "Channel", thread: "Thread", allKinds: "All conversation types",
  cancel: "Cancel", save: "Save", remove: "Delete", deleteTitle: "Delete route",
  deleteError: "The route could not be deleted. Try again.",
  deleteBody: "New messages will stop using this route. Existing Console conversations are retained.",
  unconfigured: "Channels storage is not configured.",
  threadPolicy: "Thread behavior", fixedThreadId: "Thread ID (optional)",
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
  filters: "筛选器", multiparty: "多人聊天消息",
  mentionsOnly: "仅 @ 机器人", allMessages: "全量消息", originalFilters: "保留已有来源筛选",
  wakeAgent: "唤醒 Agent", noWakeAgent: "不使用",
  wakeHelp: "决定匹配的消息是否交给目标 Agent。判断条件写在唤醒 Agent 的指令里。",
  wakeError: "请选择当前工作区内另一个已发布的 Agent。",
  channelKind: "频道", thread: "话题", allKinds: "全部会话类型",
  cancel: "取消", save: "保存", remove: "删除", deleteTitle: "删除路由",
  deleteError: "未能删除路由，请重试。",
  deleteBody: "新消息将不再使用这条路由。已有的 Console 会话会保留。",
  unconfigured: "尚未配置渠道存储。",
  threadPolicy: "Thread 投递模式", fixedThreadId: "Thread ID（选填）",
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

type Account = { id: string; name: string; platform: Platform };
type Agent = { id: string; name: string };
type Source = { channel_id: string; kind: string; conversation_id: string; mentions_only?: boolean };
type MultipartyFilter = "all" | "mentions";
type ThreadPolicy = keyof typeof english.policies;
const threadPolicies: ThreadPolicy[] = ["conversation", "sender", "fixed", "message"];
type RouteInput = {
  name: string; agent_id: string; enabled: boolean; sources: Source[];
  thread_policy: ThreadPolicy; fixed_thread_id: string | null;
  multiparty_messages: MultipartyFilter | null; wake_agent_id: string | null;
};
type Route = RouteInput & { id: string; attention: number; needs_authorization: boolean };
type RouteList = { configured: boolean; routes: Route[] };
type Text = typeof english;

type SourceDraft = Omit<Source, "conversation_id"> & { conversations: string[]; input: string };

const sourceDefaults = (): SourceDraft => ({ channel_id: "", kind: "private", conversations: [], input: "" });

function draftSources(sources: Source[]): SourceDraft[] {
  const rows: SourceDraft[] = [];
  const groups = new Map<string, SourceDraft>();
  for (const { conversation_id, ...source } of sources) {
    const key = JSON.stringify([source.channel_id, source.kind, Boolean(source.mentions_only)]);
    const existing = conversation_id === "*" ? undefined : groups.get(key);
    if (existing) {
      existing.conversations.push(conversation_id);
      continue;
    }
    const conversations = conversation_id === "*" ? [] : [conversation_id];
    const row = { ...source, conversations, input: "" };
    rows.push(row);
    if (conversations.length) groups.set(key, row);
  }
  return rows;
}

function conversationIds({ conversations, input }: SourceDraft): string[] {
  const id = input.trim();
  return id ? [...new Set([...conversations, id])] : conversations;
}
const kinds = ["*", "private", "group", "guild", "guild_dm"];

function kindName(kind: string, text: Text) {
  switch (kind) {
    case "*": return text.allKinds;
    case "private": return text.private;
    case "group": return text.group;
    case "guild": return text.guild;
    case "guild_dm": return text.guild_dm;
    case "channel": return text.channelKind;
    case "thread": return text.thread;
    default: return kind;
  }
}

async function responseJSON<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(String(response.status));
  return response.json();
}

function SourceEditor({ source, accounts, update, remove, text, disabled }: {
  source: SourceDraft; accounts: Account[]; update: (value: SourceDraft) => void;
  remove?: () => void; text: Text; disabled: boolean;
}) {
  const app = useApp();
  const optionsId = React.useId();
  const account = accounts.find(item => item.id === source.channel_id);
  const supported = account?.platform === "weixin" ? kinds.slice(0, 2) : kinds;
  function addConversation() {
    if (source.input.trim() === "*") return;
    update({ ...source, conversations: conversationIds(source), input: "" });
  }
  const observed = useQuery({
    queryKey: app.queryKey("route-sources", source.channel_id, source.kind),
    enabled: Boolean(account && !disabled && source.kind !== "*"),
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
            ...accounts.map(item => ({
              value: item.id, label: item.name, icon: <ChannelIcon platform={item.platform} />,
            })),
            ...(source.channel_id && source.channel_id !== "*" && !account
              ? [{ value: source.channel_id, label: `${text.missing} (${source.channel_id})` }] : []),
          ]}
          onChange={id => {
            const weixin = accounts.some(item => item.id === id && item.platform === "weixin");
            update({ ...source, channel_id: id, kind: weixin ? "private" : source.kind, conversations: [], input: "",
              mentions_only: !weixin && source.mentions_only });
          }} />
      </td>
      <td>
        <ChannelSelect label={text.kind} disabled={disabled} value={source.kind}
          options={(supported.includes(source.kind) ? supported : [source.kind, ...supported])
            .map(kind => ({ value: kind, label: kindName(kind, text) }))}
          onChange={kind => update({ ...source, kind, mentions_only: false, conversations: [], input: "" })} />
      </td>
      <td>
        {source.kind === "*" ? <span>{text.anyConversation}</span> : (
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
            <Input unstyled aria-label={text.conversationId} disabled={disabled} list={optionsId}
              placeholder={source.conversations.length ? text.conversationId : text.conversationPlaceholder}
              value={source.input} maxLength={512} pattern={String.raw`(?!\s*\*\s*$).*`}
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
          title={text.removeSource} onClick={remove}>×</Button>
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
  const threadHelpId = React.useId();
  const enabledId = React.useId();
  const [enabled, setEnabled] = React.useState(route?.enabled ?? true);
  const [sources, setSources] = React.useState(
    () => (route ? draftSources(route.sources) : [sourceDefaults()]).map((value, id) => ({ id, value })),
  );
  const nextSource = React.useRef(sources.length);
  const [deleting, setDeleting] = React.useState(false);
  const [pending, setPending] = React.useState(false);
  const [error, setError] = React.useState("");
  const disabled = pending || !canEdit;
  const canRemoveSource = canEdit && sources.length > 1;

  function returnToList() {
    void client.invalidateQueries({ queryKey: app.queryKey("channel-routes") });
    navigate(back);
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (disabled) return;
    const savedSources = sources.flatMap(({ value }) => {
      const ids = conversationIds(value);
      return (ids.length ? ids : ["*"]).map(conversation_id => ({
        channel_id: value.channel_id,
        kind: value.kind,
        conversation_id,
        ...(multiparty === "legacy" ? { mentions_only: value.mentions_only } : {}),
      }));
    });
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
          sources: savedSources,
          multiparty_messages: multiparty === "legacy" ? null : multiparty, wake_agent_id: wakeAgent || null,
          thread_policy: policy, fixed_thread_id: policy === "fixed" ? threadId.trim() || null : null,
        } satisfies RouteInput),
      });
      returnToList();
    } catch (error) {
      if (error instanceof ApiError && error.details.reason === "thread_destination_busy") {
        setError(text.busyThread);
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
          </div>
          <p id={threadHelpId} className="a13n-route-thread-help">{text.policyHelp[policy]}</p>
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
  const sourceName = (source: Source) => {
    const channel = source.channel_id === "*" ? text.anyChannel :
      accounts.data?.accounts.find(item => item.id === source.channel_id)?.name ?? source.channel_id;
    return [channel, kindName(source.kind, text), source.conversation_id === "*" ? null : source.conversation_id]
      .filter(Boolean).join(" · ");
  };
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
                    const sources = route.sources.map(sourceName);
                    return (
                      <tr key={route.id} data-editable onClick={() => navigate(href)}>
                        <th scope="row" className="a13n-channel-identity">
                          <Link to={href} onClick={event => event.stopPropagation()}><strong>{route.name}</strong></Link>
                        </th>
                        <td data-label={text.sources} className="a13n-route-cell" title={sources.join("\n")}>
                          {sources[0]}
                          {sources.length > 1 && <span className="a13n-route-source-count">+{sources.length - 1}</span>}
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
