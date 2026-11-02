//! ALwith Runtime: the standalone Runtime process. The host spawns it; it speaks stdio by default
//! (`--listen stdio://`), and browsers and remote clients use `--listen ws://IP:PORT`. The engine table
//! comes in through env.
//!
//! - `stdio://`: requests come in on stdin, responses and events go out on stdout (first a line
//!   `{"type":"ready","transport":"stdio"}`), logs go to stderr. It exits on stdin EOF. No token is
//!   needed; the pipe is the authorization.
//! - `ws://IP:PORT` (a bare `ws://` reads `ALWITH_RUNTIME_ADDR`, default `127.0.0.1:0`): requires
//!   `ALWITH_RUNTIME_TOKEN`, and once up prints a line `{"type":"ready","url":...}` on stdout.
//!
//! Modules: `ALWITH_MODULES_DIR` points at the app's `node_modules`; every package in it that carries an
//! `alwith-module.json` is spawned as a child process. The app calls it through
//! `modules/<name>/<method>`, and its events are broadcast as `{"type":"module"}`.
//!
//! Engine table `ALWITH_RUNTIME_ENGINES` (JSON):
//! ```json
//! { "codex": { "command": "/path/codex-acp-v2", "args": [], "env": { "CODEX_PATH": "/path/codex" } } }
//! ```
//! Shape of the `launch` field of a `start` request:
//! `{ "engine": "codex", "cwd": "...", "args": [...], "env": {...}, "provider": "...", "model": "..." }`.
//! `cwd`, `args` and `env` are appended on top of the engine table; `provider` / `model` are only labels
//! for the run state, and Runtime does not interpret them.

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use alwith_runtime_core::{AgentMetadata, AttentionStore, Journal, ResponseError, Runtime};
use alwith_runtime_modules::{ModuleExtension, ModuleHost};
use alwith_runtime_ws::{AgentLauncher, RuntimeCommand, RuntimeServer, execute};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::process::Command;

#[derive(Clone, Debug, Deserialize)]
struct EngineSpec {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartParams {
    engine: String,
    cwd: Option<String>,
    /// Process arguments appended after the engine table's `args` (e.g. alwith_dev's execution policy).
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    env_remove: Vec<String>,
    #[serde(default)]
    provider: String,
    provider_revision: Option<u64>,
    #[serde(default)]
    model: String,
    /// Connection label shown in the run state (e.g. "DeepSeek API"); supplied by the host, not interpreted
    /// by Runtime.
    #[serde(default)]
    connection: String,
}

struct TableLauncher {
    engines: HashMap<String, EngineSpec>,
    version: String,
    modules: Arc<ModuleHost>,
    launch_resolver: Option<String>,
}

impl TableLauncher {
    fn engine_names(&self) -> String {
        let mut names: Vec<&str> = self.engines.keys().map(String::as_str).collect();
        names.sort_unstable();
        names.join(", ")
    }
}

#[async_trait]
impl AgentLauncher for TableLauncher {
    async fn prepare(
        &self, _agent_id: &str, owner_id: &str, params: Value,
    ) -> Result<(Command, AgentMetadata), String> {
        let params = if params.get("providerConfig").is_some() {
            let resolver = self.launch_resolver.as_ref().ok_or(
                "providerConfig requires a configured ALWITH_RUNTIME_LAUNCH_RESOLVER module; raw engine launches need no module"
            )?;
            self.modules
                .call_module(resolver, "_runtime/resolveLaunch", params)
                .await
                .map_err(|error| error.to_string())?
        } else {
            params
        };
        let params: StartParams = serde_json::from_value(params).map_err(|error| format!("start params: {error}"))?;
        let spec = self
            .engines
            .get(&params.engine)
            .ok_or_else(|| format!("unknown engine {}; known: {}", params.engine, self.engine_names()))?;
        if !std::path::Path::new(&spec.command).is_file() {
            return Err(format!("engine {} executable missing: {}", params.engine, spec.command));
        }
        let mut command = Command::new(&spec.command);
        command.args(&spec.args).args(&params.args).envs(&spec.env);
        for key in &params.env_remove {
            command.env_remove(key);
        }
        command.envs(&params.env);
        if let Some(cwd) = &params.cwd {
            command.current_dir(cwd);
        }
        Ok((
            command,
            AgentMetadata {
                owner_id: owner_id.to_string(),
                engine: params.engine,
                provider: params.provider,
                provider_revision: params.provider_revision,
                model: params.model,
                connection: params.connection,
            },
        ))
    }

    fn version(&self) -> String {
        self.version.clone()
    }
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

/// Completes when the parent process given by `ALWITH_RUNTIME_PARENT_PID` is gone. It takes the same
/// shutdown path as a signal (stop modules, `kill_all` every agent); it must not `process::exit`
/// past them and leave orphans.
#[cfg(unix)]
async fn parent_gone() {
    let Some(parent) = env::var("ALWITH_RUNTIME_PARENT_PID").ok().and_then(|value| value.parse::<u32>().ok()) else {
        return std::future::pending().await;
    };
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if std::os::unix::process::parent_id() != parent {
            log::info!("parent {parent} exited; alwith-runtime shutting down");
            return;
        }
    }
}

#[cfg(not(unix))]
async fn parent_gone() {
    std::future::pending().await
}

fn raise_file_descriptor_limit() {
    #[cfg(unix)]
    unsafe {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur = limit.rlim_max.min(65536);
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
        }
    }
}

/// The three exit triggers share one shutdown path: Ctrl-C, SIGTERM, parent process gone.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.expect("install Ctrl-C handler"),
            signal = terminate.recv() => signal.expect("SIGTERM stream ended"),
            () = parent_gone() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.expect("install Ctrl-C handler"),
        () = parent_gone() => {}
    }
}

/// The two forms of `--listen`.
enum Listen {
    Stdio,
    Ws(SocketAddr),
}

const USAGE: &str = "usage: alwith-runtime [--listen stdio://|ws://IP:PORT] [--version] [--export-protocol rust]";

fn parse_listen(args: &[String]) -> Result<Listen, String> {
    let mut listen = None;
    let mut iterator = args.iter();
    while let Some(argument) = iterator.next() {
        match argument.as_str() {
            "--listen" => listen = Some(iterator.next().ok_or("--listen needs a value")?.clone()),
            other if other.starts_with("--listen=") => listen = Some(other["--listen=".len()..].to_string()),
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    match listen.as_deref().unwrap_or("stdio://") {
        "stdio://" => Ok(Listen::Stdio),
        "ws://" => {
            let address = env::var("ALWITH_RUNTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_string());
            address.parse().map(Listen::Ws).map_err(|error| format!("ALWITH_RUNTIME_ADDR {address}: {error}"))
        }
        other => match other.strip_prefix("ws://") {
            Some(address) => address.parse().map(Listen::Ws).map_err(|error| format!("--listen {other}: {error}")),
            None => Err(format!("unsupported --listen {other}; use stdio:// or ws://IP:PORT")),
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args: Vec<String> = env::args().skip(1).collect();
    if args == ["--export-protocol", "rust"] {
        print!("{}", include_str!("../../../crates/runtime-protocol/src/lib.rs"));
        return Ok(());
    }
    if args.iter().any(|argument| argument == "--version") {
        println!("alwith-runtime {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let listen = parse_listen(&args)?;
    raise_file_descriptor_limit();
    let engines: HashMap<String, EngineSpec> = serde_json::from_str(&required("ALWITH_RUNTIME_ENGINES"))?;
    if engines.is_empty() {
        return Err("ALWITH_RUNTIME_ENGINES must list at least one engine".into());
    }
    let journal = match env::var_os("ALWITH_RUNTIME_JOURNAL") {
        Some(root) => Journal::open(root)?,
        None => Journal::open(alwith_runtime_core::default_journal_root())?,
    };
    let attention = match env::var_os("ALWITH_RUNTIME_ATTENTION") {
        Some(root) => AttentionStore::open(root)?,
        None => AttentionStore::open(alwith_runtime_core::default_attention_root())?,
    };
    let runtime = Arc::new(Runtime::with_storage(journal, attention));
    let token = match listen {
        Listen::Stdio => String::new(),
        Listen::Ws(_) => required("ALWITH_RUNTIME_TOKEN"),
    };
    // Modules: one child process per package carrying an alwith-module.json under `ALWITH_MODULES_DIR`
    // (the app's node_modules).
    let modules_dir = env::var_os("ALWITH_MODULES_DIR").map(std::path::PathBuf::from);
    let module_host = ModuleHost::new(Arc::clone(&runtime), env!("CARGO_PKG_VERSION"));
    let extension = Arc::new(ModuleExtension(Arc::clone(&module_host)));
    let launch_resolver = env::var("ALWITH_RUNTIME_LAUNCH_RESOLVER").ok();
    let launcher = Arc::new(TableLauncher {
        engines,
        version: env!("CARGO_PKG_VERSION").to_string(),
        modules: Arc::clone(&module_host),
        launch_resolver: launch_resolver.clone(),
    });
    let server = RuntimeServer::new(runtime, launcher, extension, token);
    if let Some(dir) = &modules_dir {
        let host = &module_host;
        let dispatch_server = server.clone();
        host.set_dispatch(Arc::new(move |owner, method, params| {
            let server = dispatch_server.clone();
            Box::pin(async move {
                let envelope = serde_json::json!({ "method": method, "params": params });
                let command: RuntimeCommand =
                    serde_json::from_value(envelope).map_err(ResponseError::invalid_params)?;
                execute(&server, &owner, command).await
            })
        }));
        for status in host.load_dir(dir).await {
            log::info!(
                "module {} {} alive={} persistent={}",
                status.name,
                status.version,
                status.alive,
                status.persistent
            );
        }
    }
    if let Some(resolver) = launch_resolver {
        if !module_host.list().iter().any(|module| module.name == resolver && module.alive) {
            return Err(format!("configured launch resolver module {resolver} is not running").into());
        }
        // A round trip proves the optional resolver has initialized before clients can start agents.
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            module_host.call_module(&resolver, "_runtime/ready", Value::Null),
        )
        .await
        .map_err(|elapsed| format!("launch resolver module {resolver} did not become ready: {elapsed}"))??;
    }
    match listen {
        Listen::Stdio => alwith_runtime_stdio::serve_stdio(server, shutdown_signal()).await?,
        Listen::Ws(address) => {
            let listener = TcpListener::bind(address).await?;
            let local = listener.local_addr()?;
            println!(
                "{}",
                serde_json::json!({ "type": "ready", "transport": "ws", "url": format!("ws://{local}"), "port": local.port() })
            );
            server.serve_with_shutdown(listener, axum::Router::new(), shutdown_signal()).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn stdio_is_the_default() {
        assert!(matches!(parse_listen(&[]).unwrap(), Listen::Stdio));
        assert!(matches!(parse_listen(&args(&["--listen", "stdio://"])).unwrap(), Listen::Stdio));
    }

    #[test]
    fn ws_takes_an_explicit_address() {
        match parse_listen(&args(&["--listen=ws://127.0.0.1:4321"])).unwrap() {
            Listen::Ws(address) => assert_eq!(address.port(), 4321),
            Listen::Stdio => panic!("expected ws"),
        }
    }

    #[test]
    fn other_schemes_and_arguments_are_rejected() {
        assert!(parse_listen(&args(&["--listen", "unix://x"])).is_err());
        assert!(parse_listen(&args(&["--bogus"])).is_err());
        assert!(parse_listen(&args(&["--listen"])).is_err());
    }

    #[tokio::test]
    async fn arbitrary_engines_and_providers_work_without_any_module_or_sdk() {
        let root = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_storage(
            Journal::open(root.path().join("sessions")).unwrap(),
            AttentionStore::open(root.path()).unwrap(),
        ));
        let launcher = TableLauncher {
            engines: [(
                "company-agent".into(),
                EngineSpec {
                    command: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
                    args: vec!["--acp".into()],
                    env: [("REMOVE_THIS".into(), "old".into())].into(),
                },
            )]
            .into(),
            version: "test".into(),
            modules: ModuleHost::new(runtime, "test"),
            launch_resolver: None,
        };
        let (command, metadata) = launcher
            .prepare(
                "agent",
                "app",
                serde_json::json!({
                    "engine":"company-agent", "provider":"custom-provider", "model":"custom-model",
                    "args":["--extra"], "env":{"CUSTOM_KEY":"test-key"}, "envRemove":["REMOVE_THIS"]
                }),
            )
            .await
            .unwrap();
        assert_eq!(metadata.owner_id, "app");
        assert_eq!(metadata.provider, "custom-provider");
        assert_eq!(metadata.model, "custom-model");
        assert_eq!(metadata.provider_revision, None);
        let (_, confirmed) = launcher
            .prepare(
                "versioned",
                "app",
                serde_json::json!({
                    "engine":"company-agent", "provider":"custom-provider", "providerRevision":0
                }),
            )
            .await
            .unwrap();
        assert_eq!(confirmed.provider_revision, Some(0));
        let command = command.as_std();
        assert_eq!(command.get_args().collect::<Vec<_>>(), ["--acp", "--extra"]);
        let environment: HashMap<_, _> = command.get_envs().collect();
        assert_eq!(environment[std::ffi::OsStr::new("REMOVE_THIS")], None);
        assert_eq!(environment[std::ffi::OsStr::new("CUSTOM_KEY")], Some(std::ffi::OsStr::new("test-key")));
        assert!(
            launcher
                .prepare(
                    "agent",
                    "app",
                    serde_json::json!({
                        "providerConfig":{"engine":"company-agent"}
                    })
                )
                .await
                .unwrap_err()
                .contains("ALWITH_RUNTIME_LAUNCH_RESOLVER")
        );
    }
}
