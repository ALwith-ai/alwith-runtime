# alwith-runtime

Private. ALwith Runtime: ownership of ACP agent processes, session run states, the
per-session event journal, and the `alwith-runtime-v1` frame protocol over stdio or WebSocket.
Ships as the `alwith-runtime` binary; source stays here. Every ALwith application (U, Desktop,
Board, connectors) spawns or connects to this executable. Concrete capabilities are optional module
processes. Building Runtime requires no other ALwith repository.

| Crate | What |
| --- | --- |
| `crates/runtime-protocol` | Runtime-owned wire types; exported bindings are consumed by client SDKs |
| `crates/runtime-core` | `Runtime` (spawn / generations / lanes / batching), `RuntimeRegistry` (run states, `done` + `mark_read`, attach with pending-permission replay), `Journal` (sequenced per-session jsonl) |
| `crates/runtime-ws` | The frame protocol and its WebSocket entry: token handshake, request envelopes, global event fan-out, `Extension` hook for host-specific methods |
| `crates/runtime-stdio` | The stdio entry: one connection over stdin/stdout, same frames, bounded event queue with a `lagged` notice instead of a disconnect |
| `crates/runtime-modules` | Module host: scans `ALWITH_MODULES_DIR` (the app's `node_modules`) for packages carrying `alwith-module.json`, spawns each entry as a subprocess speaking the same frames, routes `modules/<name>/<method>` and broadcasts module events as `{type:"module"}` |
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

## Independent build

Only this repository and registry dependencies are required:

```sh
cargo build --release --locked -p alwith-runtime
cargo test --workspace --locked
```

There are no sibling source dependencies, Git submodules or private dependency checkouts.
CI and release jobs check out only this repository. The dependency-boundary test rejects
path dependencies outside this repository, including dev dependencies.

Protocol ownership is `crates/runtime-protocol`. A client can obtain its Rust bindings from
`alwith-runtime --export-protocol rust`; the API repository imports this generated snapshot.
The Runtime never imports a client implementation. Client integration tests live in the API
repository and run against a separately built Runtime executable.

## Optional capabilities

Filesystem, Git, Shell, Store, QA and ALwith-specific provider translation live in the
optional `@alwith/services` module bundle, built in `alwith-modules`, not in Runtime.
Desktop selects that bundle; U's raw Codex engine launch does not need it.
The same Runtime executable supports both applications and a standalone server.

A module may declare `routes:["namespace"]` in its manifest. Runtime forwards that namespace
through the module's private pipe, retaining connection identity and disconnect cleanup.
It does not know the module's domain types, settings or implementation. Duplicate namespaces
are rejected. Existing module calls still use `modules/<name>/<method>`.

For optional provider translation, set `ALWITH_RUNTIME_LAUNCH_RESOLVER` to the installed
module name. Runtime sends provider-based launches over the private pipe, then owns the
actual engine spawn. Credentials resolved there are not exposed by a public preparation RPC.
Raw `launch:{engine,args,env,envRemove,cwd,provider,model}` needs no module or ALwith account.
The application remains responsible for its platform identity policy.

Service configuration (including `ALWITH_RUNTIME_SERVICES`, provider settings and QA assets)
is interpreted by the optional module. See its README. Modules inherit the configured process
environment; this is not an OS sandbox or multi-tenant security boundary.

Module-private operations (unavailable through public `modules/...` calls):

- `_runtime/call`: `{connectionId,owner,method,params}`. Modules declaring `owner-requests` also receive a private `callId`. They may call `_runtime/requestForCall` with `{callId,method:"acpRequest",params}` while that application call and connection remain alive. Runtime supplies the original owner, strips any owner supplied by the module, and runs the normal agent ownership check. A routed application's optional `owner` label is scoped beneath its connection, matching core commands. Scopes expire on completion, cancellation or disconnect; another module cannot use them. This lets a credential-owning service call an engine without returning credentials to its caller.
- `_runtime/disconnect`: `{connectionId}`; must release connection-owned resources.
- `_runtime/ready`: resolves after initialization.
- `_runtime/resolveLaunch`: optional private launch resolver.

Module events `{type:"_runtime/service",connectionId,event}` become
`{type:"service",event}` only on the originating connection. A
`_runtime/disconnect` event requests that connection's shutdown. Runtime rejects private
operation access from ordinary clients and disconnects affected clients if their module exits.

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

`stopSession` stops the entire agent process that owns the session. For agents that host multiple
sessions and support ACP `session/close`, send that ACP request to close only one session. Runtime
removes its run-state and pending permissions only after a successful close response; other sessions
and process ownership remain unchanged. A later successful `session/resume` makes it active again.

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

## Distribution

The Runtime ships only through npm. Pushing a `v<version>` tag makes `release.yml` build five platforms (macOS arm64, Linux x64/arm64, Windows x64/arm64) and publish `@alwith/runtime` plus five `@alwith/runtime-<os>-<cpu>` platform packages. An app's `bun install` installs the one for its platform, and at build time `stageRuntime()` (`@alwith/runtime/stage`) verifies the sha before copying it into the sidecar; consumers that do not use npm take the tgz from the registry directly. `scripts/package-release.ts` puts the binary, LICENSE, third-party licence texts and `artifact.json` into each platform package. There is one version line: Cargo workspace = tag = `packages/runtime`. Bump those three before a release; the first version of a new package name is published by hand, after that trusted publishing is fully automatic.

### Windows CRT

Windows x64 / ARM64 link the CRT statically through `.cargo/config.toml`, so the Runtime does not require users to install the
VC++ Redistributable. Windows packaging needs LLVM's `llvm-readobj` on PATH (preinstalled on the release
runner); the packaging script checks the actual EXE's regular and delay-load import tables and fails without producing a platform package if it finds a dynamic CRT dependency.
The `RUSTFLAGS` / `CARGO_ENCODED_RUSTFLAGS` environment variables can override the Cargo configuration, so checking build flags alone is not enough.

```sh
bun test ./scripts/__tests__/check-windows-runtime.test.ts
bun scripts/check-windows-runtime.ts target/x86_64-pc-windows-msvc/release/alwith-runtime.exe
```

Before a release, also verify on a clean Windows machine without the VC++ Redistributable that the new build prints `ready`.
A security update to the static CRT requires rebuilding and republishing the Runtime; existing npm packages do not change when the source is fixed,
so consumers must update to the new release and repackage their app.

## Formatting

Run `bun install --frozen-lockfile` to install the repository's formatting tools.
`bun run format` formats JavaScript, TypeScript, CSS and supported configuration files;
`bun run format:rust` formats the Rust workspace; `bun run format:all` runs both.
Prettier and its Tailwind plugin match ALwith Desktop's versions; Prettier and Rustfmt settings match Desktop's rules.
Generated artifacts, dependency directories, licenses and Markdown are excluded from Prettier.
