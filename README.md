# alwith-runtime

Private. ALwith Runtime: ownership of ACP agent processes, session run states, the
per-session event journal, and the `alwith-runtime-v1` frame protocol over stdio or WebSocket.
Ships as the `alwith-runtime` binary; source stays here. Every ALwith application (U, Desktop,
Board, connectors) spawns or connects to this executable. Optional native services are built in;
there is no separate Web-only host executable or dependency on Desktop source.

| Crate | What |
| --- | --- |
| `alwith-api` (sibling repository) | Protocol types and Rust client; TypeScript client is `@alwith/api` |
| `crates/runtime-core` | `Runtime` (spawn / generations / lanes / batching), `RuntimeRegistry` (run states, `done` + `mark_read`, attach with pending-permission replay), `Journal` (sequenced per-session jsonl) |
| `crates/runtime-ws` | The frame protocol and its WebSocket entry: token handshake, request envelopes, global event fan-out, `Extension` hook for host-specific methods |
| `crates/runtime-stdio` | The stdio entry: one connection over stdin/stdout, same frames, bounded event queue with a `lagged` notice instead of a disconnect |
| `crates/runtime-modules` | Module host: scans `ALWITH_MODULES_DIR` (the app's `node_modules`) for packages carrying `alwith-module.json`, spawns each entry as a subprocess speaking the same frames, routes `modules/<name>/<method>` and broadcasts module events as `{type:"module"}` |
| `crates/runtime-services` | Built-in fs, store, shell, git and QA RPC adapters; shared capability libraries live in private `alwith-modules`. Services are opt-in and use the same implementation over both transports |
| `apps/runtime` | The `alwith-runtime` binary. Engines come from `ALWITH_RUNTIME_ENGINES`; `--listen stdio://` (default) or `--listen ws://IP:PORT` |

## alwith-runtime

Desktop path (default): the host spawns the binary and talks over its pipes. The first stdout
line is `{"type":"ready","transport":"stdio"}`; stdin EOF shuts everything down.

```sh
ALWITH_RUNTIME_ENGINES='{"codex":{"command":"/path/codex-acp-v2","env":{"CODEX_PATH":"/path/codex"}}}' \
alwith-runtime
# {"type":"ready","transport":"stdio"}
```

Browser and remote path:

```sh
ALWITH_RUNTIME_TOKEN=secret ALWITH_RUNTIME_ENGINES='{...}' alwith-runtime --listen ws://127.0.0.1:0
# {"type":"ready","transport":"ws","url":"ws://127.0.0.1:52341","port":52341}
```

`--listen ws://` alone reads `ALWITH_RUNTIME_ADDR` (default `127.0.0.1:0`). Optional for both:
`ALWITH_RUNTIME_JOURNAL` (default the platform cache dir under `alwith/runtime/sessions`),
`ALWITH_RUNTIME_PARENT_PID` (exit when the parent goes away), `ALWITH_MODULES_DIR` (load modules from
there; see `crates/runtime-modules`). Logs go to stderr only; stdout carries nothing but protocol frames.

## Optional native services

Resource services are configured independently with `ALWITH_RUNTIME_SERVICES`:

```json
{"schemaVersion":1,"fs":{"roots":["/srv/work"]},"store":{"dataDir":"/srv/app/data","cacheDir":"/srv/app/cache"}}
```

Omitted capabilities are disabled. `fs`, `git`, `shell` and `qa` each accept their own nonempty
absolute `roots`; `store` requires distinct absolute `dataDir` and `cacheDir`. Unknown fields
and unsupported schema versions are rejected. No resource service requires another service.
`services/capabilities` returns `{schemaVersion:1,enabled:[...] }` (sorted capability names).
`services/ping` and provider management remain available with no resource services enabled.
`ALWITH_FS_ROOTS` separately constrains provider-launch cwd; it does not enable resource services.
`ALWITH_SETTINGS_PATH` selects provider settings, not the store data directory.

The same binary exposes `fs/*`, `store/*`, `git/*`, `shell/exec`, `qa/*` and `engines/status`
over stdio or authenticated WebSocket. These are built-in services, not module subprocesses.
Application modules still use `modules/<name>/<method>`.

Service events use `{"type":"service","event":{...}}`. File watchers belong to a connection,
not a global watcher-id map: equal IDs on different connections do not collide, and disconnect
releases watchers even if a long-running operation remains. Long service calls do not hold a
connection-wide lock, so cancellation can run concurrently. Slow event consumers are disconnected;
after reconnect, re-register watchers and reload store/QA snapshots. `storeInvalidated` means the
specified scope needs reloading after a store broadcast gap; it is not a normal key mutation.

Provider management is native and independent of these optional resource services:
set `ALWITH_SETTINGS_PATH` to use `launch:{providerConfig:{engine,providerId,modelId,...}}`.
Runtime rereads settings for each launch, resolves credentials and delegates CLI translation to
`alwith-cli-adapters`. Desktop and browser use this same path. `engines/probeCapabilities`
also works without fs/store/QA services. Raw `launch:{engine,args,env,cwd}` remains available
for arbitrary third-party CLIs. Engine names are arbitrary: declare `providerAdapter` in each
engine specification (`raw`, `alwith`, `dsh`, or declarative `environment`); see the protocol.
`engines/capabilities` lists configured engines and adapter kinds without exposing credentials.

Optional `overrides:{args,env,envRemove,cwd}` appends execution policy or application environment
to a provider-based launch; other override keys are rejected. Environment removals survive the
protocol and are applied before explicit environment values. Existing configured filesystem roots
also constrain launch cwd, but are not an OS sandbox.

ALwith.dev itself reads its executor defaults and adds its tool/plugin policy. Runtime does not
special-case its business methods or return credential-bearing launch templates via an RPC.
`ALWITH_CLI_PATH`, `ALWITH_BUN_PATH`, `ALWITH_DSH_ENTRY` select installed engine paths;
`ALWITH_AI_GATEWAY_BASE_URL` and `ALWITH_PROJECTS_DIR` configure gateway/project locations.
With provider translation, keep the engine table's args free of adapter-owned args such as `--acp`
or the dsh entry; the adapter supplies these once.

QA execution requiring the Playwright reporter must set `ALWITH_QA_REPORTER_PATH`. Missing QA assets
do not prevent an unrelated chat/fs client from starting. QA platform credentials intentionally remain provided by Desktop through its native adapter.
Standalone Runtime explicitly rejects QA cases requiring a credential provider it does not have;
a server that needs those cases must supply an appropriate provider. This is an accepted business
boundary, not a requirement to move Desktop authentication into Runtime. This refactor does not claim unified login has shipped.

Security: a Runtime token grants access as a trusted operator. Filesystem roots constrain file APIs,
not arbitrary shell commands or Agent tools. This is not an OS sandbox or a multi-tenant server.
Use protected transport/access control when deploying beyond localhost.

Build source requires sibling `alwith-api` and `alwith-modules`; applications consuming the binary
do not need their source. CI/release checkout requires `BUILD_TOKEN` with read access to both private
repositories. No source is added to release archives.

Verification: `crates/runtime-services/src/__tests__/services.rs` exercises real stdio and WebSocket
service requests, denied paths, store events, watcher isolation and cleanup; runtime-ws tests prove
same-connection cancellation does not wait for the long call. No real model sessions are needed.

## Protocol

The full reference an outsider builds against is [alwith-api/PROTOCOL.md](https://github.com/nyssance/alwith-api/blob/main/PROTOCOL.md). Summary:

WebSocket handshake: `Sec-WebSocket-Protocol: alwith-runtime-v1, alwith-auth.<token>`. stdio has no handshake; the pipe is the authorization and the single connection's owner id is `stdio`.

Requests `{id, method, params}` → `{id, result | error}`. Methods: `start`, `send`, `stop`,
`attachSession`, `releaseSession`, `stopSession`, `markRead`, `setSessionTitle`, `sessionLifecycle`,
`sessionEvents {sessionId, afterSequence?, limit?}`, `runStates`, `version`, `agents`, `ping`,
`stopGeneration {agentId, generation}`, `detachOwner {owner}`, `releaseExecution {executionId}`,
`agentForSession {sessionId}`, `sessionReplay {sessionId}`, `setSlowMode {delayMs}`. Write requests and
`attachSession` accept an optional `owner` (a sub-owner label such as a window); the owner is recorded as
`<connection>:<owner>`, the connection itself may write to every sub-owner's agent.

Events on the same socket, discriminated by `type`: `wire`, `inbound`, `outbound`, `stderr`,
`exit`, `runStates`. Events go to every authenticated connection; `send`, `stop` and
`releaseSession` only succeed for the owning connection (`attachSession` takes ownership). On stdio the
event queue is bounded; when it overflows the dropped count arrives as `{"type":"lagged","dropped":n}`
and the client replays with `sessionEvents`.

## Release

`alwith-runtime` is distributed as binaries only. Tag the workspace version (`v0.1.0` for
`version = "0.1.0"` in `Cargo.toml`) and `.github/workflows/release.yml` builds six
targets (macOS arm64/x64, Linux x64/arm64, Windows x64/arm64) and publishes
`alwith-runtime-<rust-triple>.tar.gz` plus `SHA256SUMS` to a GitHub Release.

Assets go to this repository by default. To publish from this private repo to a public
one, set the repository variable `RELEASE_REPO` (`owner/name`) and the secret
`RELEASE_TOKEN` (a token with `contents: write` on that repo). Consumers pin the version:
alwith-u keeps it in `runtime.json` and `scripts/stage.ts` downloads and verifies
the archive for the target it is building.

```sh
git tag v0.1.0 && git push origin v0.1.0
```

## Validate

```sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
