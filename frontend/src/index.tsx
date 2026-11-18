import { React, Page, Loading, ErrorPage, useApp, useQuery } from "@a13n/console";
import "./app.css";

function Overview() {
  const app = useApp();
  const query = useQuery({
    queryKey: app.queryKey("summary"),
    queryFn: async ({ signal }) => {
      const response = await app.request("/summary", { signal });
      if (!response.ok) throw new Error(`Summary request failed (${response.status})`);
      return response.json();
    },
  });
  if (query.isPending) return <Loading page />;
  if (query.error) return <ErrorPage error={query.error} />;
  return (
    <Page title={ "Channels" } description="Your workspace App is ready to build on.">
      <section className="a13n-channels-overview" aria-label="App connection">
        <h2>Backend connected</h2>
        <p>{query.data.message}</p>
        <dl>
          <dt>Workspace</dt>
          <dd>{query.data.workspace_id}</dd>
        </dl>
      </section>
    </Page>
  );
}

export default {
  bridge: 1,
  pages: [{ id: "overview", label: { en: "Channels", "zh-CN": "渠道" }, component: Overview }],
};
