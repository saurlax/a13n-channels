# a13n-channels repository guide

a13n-channels adds a Channels tab to a13n Console. The App owns channel accounts, outbound platform connections, and messages. Channel Gateway is a protocol reference. The initial App uses the existing Service extension surface. Reverse WebSocket ingress stays outside the initial scope.

## Sources and workflow

- Read [README.md](README.md), then trace the affected code and tests before proposing a change. Use the [Service App guide](https://github.com/converge-ai-labs/agent-foundation/blob/main/docs/a13n-service/app-plugins.md), [App plugin contract](https://github.com/converge-ai-labs/agent-foundation/blob/main/spec/a13n-service/15-app-plugins.md), and [Channel Gateway contracts](https://github.com/saurlax/channel-gateway/tree/main/spec) for the boundaries each project owns. Check the host implementation at `.service-ref` before relying on an API described on upstream main.
- Write canonical repository content in English. Use the target language for localized UI strings. Lead with the result or decision, state scope directly, and qualify claims only where the evidence or correct use requires it. Keep edits focused. [Writing guidance](https://gist.github.com/Wh1isper/8132cd97ddd999d953d2b34a29deaf2e#file-anti-defensive-writing-md)
- Use GitHub Issues for unresolved design proposals and PRs for changes. Keep docs, implementation, and tests aligned with the behavior they describe. Preserve unrelated work and secrets.

## Engineering boundaries

- Keep the App ID `saurlax.channels` consistent across package discovery, the App definition, and its router.
- Use `app_router()` for Service authentication, organization enablement, and initial workspace admission. Enforce the operation's workspace and resource permissions in the handler. Keep channel credentials encrypted on the Service host and scope accounts and messages to their workspace. Preserve platform authentication in each adapter.
- Mount pages through `@a13n/console` and reuse its React tree, theme, translation, and request facilities. Use workspace-scoped query keys and scope CSS to the App. Follow the [Console design guidelines](https://github.com/converge-ai-labs/agent-foundation/blob/main/spec/frontend/console-design-guidelines.md) for interaction changes.
- Keep code direct, cohesive, and grounded in current requirements. Give each rule one owner. Add abstractions, guards, fallbacks, and retries when supported inputs, established boundaries, or concrete failure paths justify them. Preserve necessary correctness and security protections. Fix failures at their owner and surface actionable errors.
- Keep external I/O asynchronous. If persistence is needed, follow the host App database contract at the selected Service revision and keep transactions short. Make reversible decisions directly. Use a proof of concept when resolving an uncertainty would materially change the design or avoid costly rework.
- Check the behavior at its normal entry point. Scale review and tests to affected invariants and risks. Verify findings against the code and contracts. Report material gaps and actual check outcomes.

## Validation and commits

- Use `make setup` to prepare the environment pinned by `.service-ref`. Use `make check` for Python lint and formatting, `make test` for focused behavior, and `make verify` for wheel, sdist, and installed-wheel checks. When adopting a new Service API, update the pin deliberately and use a matching host checkout.
- Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) in English. Use `type(scope): description` or `type: description`, with a short imperative description. Examples include `feat(console): add channel account list`, `fix(api): enforce route workspace scope`, and `docs: describe local setup`.

## Channel adapters

- Put platform protocol modules in `src/a13n_channels/channels/`. Export an `ADAPTER` using `channels/adapter.py` for new outbound connections. Keep account storage, authorization, routing, and Run delivery in the shared application. Register each adapter once for account forms, routing, ingestion, and replies.
- Bind one stable upstream bot or subscription identity per account. Enqueue normalized `ChannelMessage` values using stable upstream event IDs. Persist cursor changes with ingestion when a platform supplies a durable cursor. Pass private reply material through the encrypted context argument and omit it from normalized message data and logs.
- Use stable conversation IDs and platform-native reply destinations. Ignore self-generated events, honor platform rate limits, and make listening cancellable. Report a positive send result only after the platform acknowledges it. Test wire envelopes and routing through the shared host, and identify the platforms that still need live account validation.
