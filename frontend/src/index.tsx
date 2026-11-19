import {
  React, Button, Empty, ErrorPage, Input, Link, Loading, Page,
  useAccess, useApp, useQuery, useQueryClient, useParams, useSearchParams, useTranslation,
} from "@a13n/console";
import { ChannelIcon, type Platform } from "./channel-icon";
import { RoutePage } from "./routes";
import { ChannelSelect } from "./select";
import "./app.css";

const english = {
  title: "Channels", add: "Add channel", name: "Name", secret: "AppSecret",
  channelType: "Channel type", qq: "QQ Bot", weixin: "Weixin ClawBot", status: "Status",
  secretPlaceholder: "Leave blank to keep the current AppSecret", remove: "Delete", cancel: "Cancel", save: "Save",
  empty: "No channels yet", unconfigured: "Channels storage is not configured.",
  deleteTitle: "Delete channel",
  deleteBody: "This disconnects the channel and permanently deletes the messages saved locally for it.",
  error: "The operation could not be completed. Refresh the list and check the bot before trying again.",
  scan: "Scan with Weixin and confirm on your phone.", scanned: "Scanned. Confirm on your phone.",
  getCode: "Get QR code", verify: "Verify", verifyCode: "Verification code", pairingExpired: "Pairing ended. Get a new QR code.",
  loginRequired: "Pairing required", qr: "Weixin pairing QR code",
  bindingMethod: "Binding method", scanQQ: "Scan QR code", manual: "Enter credentials",
  qqQR: "QQ pairing QR code", qqScanHelp: "Scan with the QQ mobile app and authorize the bot.",
  qqScanReady: "Authorized", test: "Test connection", testing: "Testing connection…",
  testOK: "Connection successful", testFailed: "Connection test failed. Try again.", qrLoading: "Getting QR code…",
  connected: "Connected", connecting: "Connecting", inactive: "Inactive",
  reconnecting: "Reconnecting", errorState: "Connection error", unknown: "Unknown",
};

const chinese: typeof english = {
  title: "渠道", add: "添加渠道", name: "名称", secret: "AppSecret",
  channelType: "渠道类型", qq: "QQ 机器人", weixin: "微信 ClawBot", status: "状态",
  secretPlaceholder: "留空则保留现有 AppSecret", remove: "删除", cancel: "取消", save: "保存",
  empty: "暂无渠道", unconfigured: "尚未配置渠道存储。",
  deleteTitle: "删除渠道",
  deleteBody: "删除后将断开该渠道的连接，并永久删除该渠道在本地保存的消息。",
  error: "操作未完成。请刷新列表并检查机器人状态后再试。",
  scan: "请用微信扫码并在手机上确认。", scanned: "已扫码，请在手机上确认。",
  getCode: "获取二维码", verify: "验证", verifyCode: "验证码", pairingExpired: "配对已结束，请重新获取二维码。",
  loginRequired: "待配对", qr: "微信配对二维码",
  bindingMethod: "绑定方式", scanQQ: "扫码绑定", manual: "手动填写",
  qqQR: "QQ绑定二维码", qqScanHelp: "请用手机QQ扫码，并授权绑定机器人。",
  qqScanReady: "已授权", test: "测试连接", testing: "正在测试连接…",
  testOK: "连接成功", testFailed: "连接测试失败，可重试。", qrLoading: "正在获取二维码…",
  connected: "已连接", connecting: "连接中", inactive: "未运行",
  reconnecting: "重连中", errorState: "连接异常", unknown: "未知",
};

type Account = { id: string; name: string; platform: Platform; account_id: string | null; state: string };
type Pairing = { login_id: string; qr_image: string; status: string };
type QQScan = { login_id: string; qr_image: string | null; status: "wait" | "confirmed" | "expired" | "failed" };
type Accounts = { configured: boolean; accounts: Account[]; qq_scan_available: boolean };
type Action = { kind: "add" } | { kind: "edit"; account: Account };

const endpoint = (platform: Platform) => `/${platform}/accounts`;
const accountPath = (account: Account) => `${endpoint(account.platform)}/${encodeURIComponent(account.id)}`;

function ChannelStatus({ state, text }: { state: string; text: typeof english }) {
  const statuses = {
    connected: { label: text.connected, tone: "success" },
    connecting: { label: text.connecting, tone: "info" },
    reconnecting: { label: text.reconnecting, tone: "info" },
    login_required: { label: text.loginRequired, tone: "warning" },
    inactive: { label: text.inactive, tone: "neutral" },
    error: { label: text.errorState, tone: "danger" },
  };
  const { label, tone } = statuses[state as keyof typeof statuses] ?? { label: text.unknown, tone: "neutral" };
  return (
    <span className="a13n-channel-status" data-tone={tone}>
      <span className="a13n-channel-status-dot" aria-hidden="true" />
      {label}
    </span>
  );
}

async function requireSuccess(response: Response) {
  if (!response.ok) throw new Error(`Channel request failed (${response.status})`);
  return response;
}

function ChannelEditorHeader({ account, text, titleId }: {
  account: Account;
  text: typeof english;
  titleId: string;
}) {
  return (
    <header className="a13n-channel-edit-header">
      <h2 id={titleId}>{account.name}</h2>
      <p id={`${titleId}-description`} className="a13n-channel-icon-label">
        <ChannelIcon platform={account.platform} />{text[account.platform]}
      </p>
    </header>
  );
}

function DeleteChannelDialog({ account, text, dismiss, onDeleted }: {
  account: Account;
  text: typeof english;
  dismiss: () => void;
  onDeleted: () => void;
}) {
  const app = useApp();
  const client = useQueryClient();
  const dialog = React.useRef<HTMLDialogElement>(null);
  const titleId = React.useId();
  const [pending, setPending] = React.useState(false);
  const [failed, setFailed] = React.useState(false);

  React.useEffect(() => {
    dialog.current?.showModal();
  }, []);

  function cancel() {
    dialog.current?.close();
    dismiss();
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending) return;
    setPending(true);
    setFailed(false);
    try {
      await requireSuccess(await app.request(accountPath(account), { method: "DELETE" }));
    } catch {
      setFailed(true);
      setPending(false);
      return;
    }
    void client.invalidateQueries({ queryKey: app.queryKey("channel-accounts") });
    onDeleted();
  }

  return (
    <dialog ref={dialog} className="a13n-qq-dialog" aria-labelledby={titleId}
      aria-describedby={`${titleId}-description`}
      onCancel={(event) => { event.preventDefault(); if (!pending) cancel(); }}>
      <form onSubmit={submit}>
        <header className="a13n-channel-edit-header">
          <h2 id={titleId}>{text.deleteTitle}</h2>
          <p>{account.name}</p>
        </header>
        <p id={`${titleId}-description`}>{text.deleteBody}</p>
        {failed && <p role="alert">{text.error}</p>}
        <div className="a13n-qq-actions">
          <Button type="button" variant="outline" disabled={pending} onClick={cancel}>{text.cancel}</Button>
          <Button type="submit" variant="destructive" disabled={pending}>{text.deleteTitle}</Button>
        </div>
      </form>
    </dialog>
  );
}

function AccountDialog({ action, text, dismiss, pair, requestDelete, qqScanAvailable }: {
  action: Action;
  text: typeof english;
  dismiss: () => void;
  pair: (account: Account) => void;
  requestDelete: (account: Account) => void;
  qqScanAvailable: boolean;
}) {
  const app = useApp();
  const client = useQueryClient();
  const dialog = React.useRef<HTMLDialogElement>(null);
  const form = React.useRef<HTMLFormElement>(null);
  const titleId = React.useId();
  const add = action.kind === "add";
  const account = action.kind === "add" ? null : action.account;
  const editing = action.kind === "edit";
  const [platform, setPlatform] = React.useState<Platform>(account?.platform ?? "qq");
  const [qqMethod, setQQMethod] = React.useState(add && qqScanAvailable ? "scan" : "manual");
  const [scan, setScan] = React.useState<QQScan | null>(null);
  const [scanFailed, setScanFailed] = React.useState(false);
  const [scanAttempt, retryScan] = React.useReducer((attempt: number) => attempt + 1, 0);
  const [draftAppId, setDraftAppId] = React.useState(account?.account_id ?? "");
  const [hasSecret, setHasSecret] = React.useState(false);
  const qqFields = platform === "qq";
  const scanning = qqFields && qqMethod === "scan";
  const scanReady = scan?.status === "confirmed";
  const [pending, setPending] = React.useState(false);
  const [failed, setFailed] = React.useState(false);

  const [connection, setConnection] = React.useState<"idle" | "testing" | "success" | "failed">("idle");
  const activeTest = React.useRef<AbortController | null>(null);
  const testing = connection === "testing";

  async function testConnection() {
    if (activeTest.current || pending) return;
    const scanPath = scanning && scanReady && scan ? `/qq/pair/${encodeURIComponent(scan.login_id)}/test` : null;
    let credentials: Record<string, string> | undefined;
    if (!scanPath) {
      if (scanning || !form.current) return;
      const appID = form.current.elements.namedItem("app_id") as HTMLInputElement;
      const appSecret = form.current.elements.namedItem("app_secret") as HTMLInputElement;
      if (!appID.reportValidity() || !appSecret.reportValidity()) return;
      credentials = { app_id: appID.value, app_secret: appSecret.value };
    }
    const controller = new AbortController();
    activeTest.current = controller;
    setConnection("testing");
    try {
      const path = scanPath ?? "/qq/test";
      const target = editing && account ? `${path}?channel_id=${encodeURIComponent(account.id)}` : path;
      await requireSuccess(await app.request(target, {
        method: "POST", signal: controller.signal,
        ...(credentials && { headers: { "Content-Type": "application/json" }, body: JSON.stringify(credentials) }),
      }));
      if (!controller.signal.aborted) setConnection("success");
    } catch {
      if (!controller.signal.aborted) setConnection("failed");
    } finally {
      if (activeTest.current === controller) activeTest.current = null;
    }
  }

  React.useEffect(() => {
    if (scanning && scanReady) void testConnection();
    return () => {
      activeTest.current?.abort();
      activeTest.current = null;
    };
  }, [scanning, scanReady, scan?.login_id, platform]);

  React.useEffect(() => {
    if (!scanning) return;
    const controller = new AbortController();
    let path: string | undefined;
    function cancel() {
      if (path) void app.request(path, { method: "DELETE" }).catch(() => {});
    }
    async function watch() {
      setScan(null);
      setScanFailed(false);
      // Read the returned session ID even if the form closes before the SDK supplies its QR code.
      try {
        const start = await requireSuccess(await app.request("/qq/pair", { method: "POST" }));
        let result = await start.json() as QQScan;
        path = `/qq/pair/${encodeURIComponent(result.login_id)}`;
        if (controller.signal.aborted) {
          cancel();
          return;
        }
        setScan(result);
        while (result.status === "wait") {
          await new Promise((resolve) => setTimeout(resolve, 1000));
          if (controller.signal.aborted) return;
          result = await (await requireSuccess(await app.request(path, { signal: controller.signal }))).json();
          setScan(result);
        }
        if (result.status !== "confirmed") setScanFailed(true);
      } catch {
        if (!controller.signal.aborted) setScanFailed(true);
      }
    }
    void watch();
    return () => {
      controller.abort();
      cancel();
    };
  }, [scanning, scanAttempt]);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending || testing || (scanning && !scanReady)) return;
    setPending(true);
    setFailed(false);
    try {
      const scanPath = scanning && scan ? `/qq/pair/${encodeURIComponent(scan.login_id)}` : undefined;
      const path = scanPath ?? (add ? endpoint(platform) : accountPath(action.account));
      const method = add || scanPath ? "POST" : "PATCH";
      const body = Object.fromEntries(new FormData(event.currentTarget));
      if (editing) {
        if (scanPath) body.channel_id = action.account.id;
        if (body.app_secret === "") delete body.app_secret;
      }
      const response = await app.request(path, {
        method,
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
      });
      if (scanning && response.status === 404) {
        setScan(null);
        setScanFailed(true);
        return;
      }
      await requireSuccess(response);
      if (add && platform === "weixin") pair(await response.json());
      dismiss();
    } catch {
      setFailed(true);
    } finally {
      setPending(false);
      void client.invalidateQueries({ queryKey: app.queryKey("channel-accounts") });
    }
  }
  React.useEffect(() => {
    dialog.current?.showModal();
  }, []);
  return (
    <dialog ref={dialog} className="a13n-qq-dialog" aria-labelledby={titleId}
      aria-describedby={editing ? `${titleId}-description` : undefined}
      onCancel={(event) => { event.preventDefault(); if (!pending) dismiss(); }}>
      <form ref={form} onSubmit={submit}>
        {editing
          ? <ChannelEditorHeader account={action.account} text={text} titleId={titleId} />
          : <h2 id={titleId}>{text.add}</h2>}
        {add && <>
          <label htmlFor={`${titleId}-platform`}>{text.channelType}
            <ChannelSelect id={`${titleId}-platform`} label={text.channelType} container={dialog}
              value={platform} disabled={pending || testing}
              options={[
                { value: "qq", label: text.qq, icon: <ChannelIcon platform="qq" /> },
                { value: "weixin", label: text.weixin, icon: <ChannelIcon platform="weixin" /> },
              ]}
              onChange={value => { setConnection("idle"); setPlatform(value); }} />
          </label>
          <label>{text.name}<Input name="name" required maxLength={80} disabled={pending} /></label>
        </>}
        {qqFields && qqScanAvailable && <label htmlFor={`${titleId}-binding`}>{text.bindingMethod}
          <ChannelSelect id={`${titleId}-binding`} label={text.bindingMethod} container={dialog}
            value={qqMethod} disabled={pending || testing}
            options={[{ value: "scan", label: text.scanQQ }, { value: "manual", label: text.manual }]}
            onChange={value => { setConnection("idle"); setHasSecret(false); setQQMethod(value); }} />
        </label>}
        {qqFields && !scanning && <>
          <label>AppID<Input name="app_id" value={draftAppId}
            onChange={(event) => { setConnection("idle"); setDraftAppId(event.currentTarget.value); }}
            disabled={testing} required maxLength={128} pattern="[A-Za-z0-9_.:@\\-]+" /></label>
          <label>{text.secret}<Input name="app_secret" type="password"
            onChange={(event) => { setConnection("idle"); setHasSecret(Boolean(event.currentTarget.value)); }}
            disabled={testing} required={add || draftAppId !== account?.account_id}
            placeholder={editing ? text.secretPlaceholder : undefined}
            maxLength={4096} autoComplete="new-password" /></label>
        </>}
        {scanning && (scanReady ? (
          <div className="a13n-qq-authorized" role="status">
            <span aria-hidden="true">✓</span>
            <strong>{text.qqScanReady}</strong>
          </div>
        ) : <>
          <p role={scanFailed ? "alert" : "status"}>{scanFailed ? text.error
            : scan?.qr_image ? text.qqScanHelp : text.qrLoading}</p>
          {!scanFailed && scan?.qr_image && <img className="a13n-weixin-qr" src={scan.qr_image} alt={text.qqQR} />}
          {scanFailed && <Button type="button" variant="outline" onClick={retryScan}>{text.getCode}</Button>}
        </>)}
        {qqFields && (!scanning || scanReady) && (
          <div className={scanning ? "a13n-qq-test a13n-qq-test-center" : "a13n-qq-test"}>
            {connection !== "idle" && <p role={connection === "failed" ? "alert" : "status"}>{
              testing ? text.testing : connection === "success" ? text.testOK : text.testFailed
            }</p>}
            {(!scanning || connection === "failed") &&
              <Button type="button" variant="outline" size="sm" disabled={pending || testing || (!scanning && !hasSecret)} onClick={() => void testConnection()}>
                {text.test}
              </Button>}
          </div>
        )}
        {failed && <p role="alert">{text.error}</p>}
        <div className="a13n-qq-actions">
          {editing && <Button type="button" className="a13n-channel-delete" variant="destructive-outline"
            disabled={pending || testing} onClick={() => requestDelete(action.account)}>{text.remove}</Button>}
          <Button type="button" variant="outline" disabled={pending} onClick={dismiss}>{text.cancel}</Button>
          <Button type="submit" disabled={pending || testing || (scanning && !scanReady)}>
            {text.save}
          </Button>
        </div>
      </form>
    </dialog>
  );
}

function PairingDialog({ account, text, dismiss, requestDelete }: {
  account: Account;
  text: typeof english;
  dismiss: () => void;
  requestDelete: (account: Account) => void;
}) {
  const app = useApp();
  const client = useQueryClient();
  const dialog = React.useRef<HTMLDialogElement>(null);
  const active = React.useRef<AbortController | null>(null);
  const loginId = React.useRef<string | null>(null);
  const titleId = React.useId();
  const [pairing, setPairing] = React.useState<Pairing | null>(null);
  const [pending, setPending] = React.useState(false);
  const [failed, setFailed] = React.useState(false);
  const path = `${accountPath(account)}/pair`;
  const terminal = ["expired", "verify_code_blocked", "binded_redirect"];

  React.useEffect(() => {
    dialog.current?.showModal();
    return () => {
      active.current?.abort();
      if (loginId.current) void app.request(`${path}/${encodeURIComponent(loginId.current)}`, {
        method: "DELETE",
      }).catch(() => {});
    };
  }, []);

  async function run(verifyCode?: string) {
    if (pending) return;
    const abort = new AbortController();
    active.current = abort;
    setPending(true);
    setFailed(false);
    try {
      let current = pairing;
      if (!verifyCode || !current) {
        setPairing(null);
        current = await (await requireSuccess(await app.request(path, {
          method: "POST", signal: abort.signal,
        }))).json() as Pairing;
        loginId.current = current.login_id;
        setPairing(current);
      }
      while (true) {
        const response = await requireSuccess(await app.request(`${path}/status`, {
          method: "POST", headers: { "Content-Type": "application/json" }, signal: abort.signal,
          body: JSON.stringify({ login_id: current.login_id, verify_code: verifyCode }),
        }));
        const { status } = await response.json();
        current = { ...current, status };
        setPairing(current);
        if (status === "confirmed" || terminal.includes(status) || status === "need_verifycode") break;
        verifyCode = undefined;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    } catch {
      if (!abort.signal.aborted) setFailed(true);
    } finally {
      if (!abort.signal.aborted) setPending(false);
    }
  }

  async function savePairing() {
    if (!pairing || pending) return;
    setPending(true);
    setFailed(false);
    try {
      const response = await requireSuccess(await app.request(`${path}/save`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ login_id: pairing.login_id }),
      }));
      if ((await response.json()).status !== "confirmed") {
        setPairing({ ...pairing, status: "expired" });
        return;
      }
      loginId.current = null;
      void client.invalidateQueries({ queryKey: app.queryKey("channel-accounts") });
      dismiss();
    } catch {
      setFailed(true);
    } finally {
      setPending(false);
    }
  }

  const status = pairing?.status;
  return (
    <dialog ref={dialog} className="a13n-qq-dialog a13n-weixin-pairing" aria-labelledby={titleId}
      aria-describedby={`${titleId}-description`}
      onCancel={(event) => { event.preventDefault(); if (!pending || status !== "confirmed") dismiss(); }}>
      <ChannelEditorHeader account={account} text={text} titleId={titleId} />
      {!pairing && <p>{text.scan}</p>}
      {pairing && status !== "confirmed" && !terminal.includes(pairing.status) &&
        <img className="a13n-weixin-qr" src={pairing.qr_image} alt={text.qr} />}
      {status === "confirmed" && <div className="a13n-qq-authorized" role="status">
        <span aria-hidden="true">✓</span><strong>{text.qqScanReady}</strong>
      </div>}
      {status && status !== "confirmed" && <p role="status">{terminal.includes(status) ? text.pairingExpired :
        status.startsWith("scaned") || status === "need_verifycode" ? text.scanned : text.scan}</p>}
      {status === "need_verifycode" && <form onSubmit={(event) => {
        event.preventDefault();
        void run(String(new FormData(event.currentTarget).get("verify_code")));
      }}>
        <label>{text.verifyCode}<Input name="verify_code" required inputMode="numeric" pattern="[0-9]+" maxLength={16} /></label>
        <Button type="submit" disabled={pending}>{text.verify}</Button>
      </form>}
      {failed && <p role="alert">{text.error}</p>}
      <div className="a13n-qq-actions">
        <Button className="a13n-channel-delete" variant="destructive-outline" disabled={pending}
          onClick={() => requestDelete(account)}>{text.remove}</Button>
        {status !== "confirmed" && <Button variant="outline" disabled={pending} onClick={() => void run()}>{text.getCode}</Button>}
        <Button variant="outline" disabled={pending && status === "confirmed"} onClick={dismiss}>{text.cancel}</Button>
        <Button disabled={pending || status !== "confirmed"} onClick={() => void savePairing()}>{text.save}</Button>
      </div>
    </dialog>
  );
}

function Channels({ nav }: { nav: React.ReactNode }) {
  const app = useApp();
  const { can, workspace } = useAccess();
  const { i18n } = useTranslation();
  const text = i18n.language.startsWith("zh") ? chinese : english;
  const [action, setAction] = React.useState<Action | null>(null);
  const [pairing, setPairing] = React.useState<Account | null>(null);
  const [deleting, setDeleting] = React.useState<Account | null>(null);
  const query = useQuery({
    queryKey: app.queryKey("channel-accounts"),
    queryFn: async ({ signal }): Promise<Accounts> =>
      (await requireSuccess(await app.request("/accounts", { signal }))).json(),
    refetchInterval: (result) => result.state.data?.accounts.length ? 5000 : false,
  });
  if (query.isPending) return <Loading page />;
  if (!query.data) return <ErrorPage error={query.error} />;
  const { accounts, configured, qq_scan_available } = query.data;
  const editable = can("write") && !workspace?.archived_at;
  const editChannel = (account: Account) => {
    if (account.platform === "qq") setAction({ kind: "edit", account });
    else setPairing(account);
  };
  const finishDelete = () => {
    setDeleting(null);
    setPairing(null);
    setAction(null);
  };
  const addChannel = editable ? <Button onClick={() => setAction({ kind: "add" })}>{text.add}</Button> : undefined;
  return (
    <Page title={text.title} actions={configured && accounts.length > 0 ? addChannel : undefined}>
      {nav}
      {query.isError && <p role="alert">{text.error}</p>}
      {!configured ? <Empty title={text.unconfigured} /> : accounts.length === 0 ? (
        <Empty title={text.empty} action={addChannel} />
      ) : (
        <div className="a13n-qq-list">
          <table role="table" aria-label={text.title}>
            <thead>
              <tr>
                <th scope="col">{text.name}</th>
                <th scope="col">{text.channelType}</th>
                <th scope="col">{text.status}</th>
              </tr>
            </thead>
            <tbody role="rowgroup">
              {accounts.map((account) => (
                <tr key={account.id} role="row" data-editable={editable || undefined} tabIndex={editable ? 0 : undefined}
                  onClick={editable ? () => editChannel(account) : undefined}
                  onKeyDown={editable ? (event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      editChannel(account);
                    }
                  } : undefined}>
                  <th scope="row" role="rowheader" className="a13n-channel-identity">
                    <strong title={account.name}>{account.name}</strong>
                    {account.account_id && <small title={account.account_id}>{account.account_id}</small>}
                  </th>
                  <td role="cell" data-label={text.channelType}>
                    <span className="a13n-channel-icon-label">
                      <ChannelIcon platform={account.platform} />{text[account.platform]}
                    </span>
                  </td>
                  <td role="cell" data-label={text.status}>
                    <ChannelStatus state={account.state} text={text} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {editable && pairing && <PairingDialog account={pairing} text={text}
        dismiss={() => setPairing(null)} requestDelete={setDeleting} />}
      {editable && action && <AccountDialog key={action.kind} action={action} text={text}
        dismiss={() => setAction(null)} pair={setPairing} requestDelete={setDeleting} qqScanAvailable={qq_scan_available} />}
      {editable && deleting && <DeleteChannelDialog account={deleting} text={text}
        dismiss={() => setDeleting(null)} onDeleted={finishDelete} />}
    </Page>
  );
}

function ScopedChannels() {
  const app = useApp();
  const { i18n } = useTranslation();
  const zh = i18n.language.startsWith("zh");
  const [params] = useSearchParams();
  const { "*": childPath } = useParams();
  const routeKey = childPath?.match(/^routes\/([^/]+)\/?$/)?.[1];
  const routes = Boolean(routeKey) || params.get("tab") === "routes";
  const base = `/workspace/${app.workspaceId}/apps/${app.appId}/overview`;
  const nav = (
    <nav className="a13n-channel-tabs" aria-label={zh ? "渠道管理" : "Channels views"}>
      <Link to={base} aria-current={routes ? undefined : "page"}>{zh ? "渠道账号" : "Channel accounts"}</Link>
      <Link to={`${base}?tab=routes`} aria-current={routes ? "page" : undefined}>{zh ? "投递路由" : "Delivery routes"}</Link>
    </nav>
  );
  const key = `${app.organizationId}/${app.workspaceId}`;
  return routes ? <RoutePage key={key} nav={nav} base={base} routeKey={routeKey} /> : <Channels key={key} nav={nav} />;
}

export default {
  bridge: 1,
  pages: [{ id: "overview", label: { en: "Channels", "zh-CN": "渠道" }, component: ScopedChannels }],
};
