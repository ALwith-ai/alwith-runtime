//! ALwith Runtime:独立的 Runtime 进程。宿主 spawn 它;默认走 stdio(`--listen stdio://`),
//! 浏览器与远程用 `--listen ws://IP:PORT`。引擎表经 env 给。
//!
//! - `stdio://`:stdin 进请求,stdout 出响应与事件(先打一行 `{"type":"ready","transport":"stdio"}`),
//!   日志走 stderr。stdin EOF 即退出。不需要 token,管道就是授权。
//! - `ws://IP:PORT`(`ws://` 单独出现取 `ALWITH_RUNTIME_ADDR`,缺省 `127.0.0.1:0`):需要 `ALWITH_RUNTIME_TOKEN`,
//!   起来后在 stdout 打一行 `{"type":"ready","url":...}`。
//!
//! 模块:`ALWITH_MODULES_DIR` 指向应用的 `node_modules`,里面每个带 `alwith-module.json` 的包被 spawn 为
//! 子进程,应用经 `modules/<name>/<method>` 调它,它的事件以 `{"type":"module"}` 广播。
//!
//! 引擎表 `ALWITH_RUNTIME_ENGINES`(JSON):
//! ```json
//! { "codex": { "command": "/path/codex-acp-v2", "args": [], "env": { "CODEX_PATH": "/path/codex" } } }
//! ```
//! `start` 请求的 `launch` 形状:`{ "engine": "codex", "cwd": "...", "args": [...], "env": {...}, "provider": "...", "model": "..." }`。
//! `cwd`、`args` 与 `env` 追加在引擎表之上;`provider` / `model` 只是给运行态看的标签,Runtime 不解释。

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use alwith_runtime_core::{AgentMetadata, Journal, ResponseError, Runtime};
use alwith_runtime_modules::{ModuleExtension, ModuleHost};
use alwith_runtime_ws::{AgentLauncher, Extension, NoExtension, RuntimeCommand, RuntimeServer, execute};
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
    /// 追加在引擎表 `args` 之后的进程参数(如 alwith_dev 的执行策略)。
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    env_remove: Vec<String>,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    model: String,
    /// 运行态里显示的连接标签(如 "DeepSeek API"),宿主给,Runtime 不解释。
    #[serde(default)]
    connection: String,
}

struct TableLauncher {
    engines: HashMap<String, EngineSpec>,
    version: String,
    services: Arc<alwith_runtime_services::ServiceExtension>,
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
        let params: StartParams = serde_json::from_value(self.services.resolve_launch(params)?)
            .map_err(|error| format!("start params: {error}"))?;
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

/// `ALWITH_RUNTIME_PARENT_PID` 指定的父进程没了就完成 —— 和收到信号走同一条收尾路径
/// (停模块、kill_all 所有 agent),不能 `process::exit` 跳过它们留下孤儿。
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

/// 三种退出信号走同一条收尾:Ctrl-C、SIGTERM、父进程消失。
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

/// `--listen` 的两种形态。
enum Listen {
    Stdio,
    Ws(SocketAddr),
}

const USAGE: &str = "usage: alwith-runtime [--listen stdio://|ws://IP:PORT] [--version]";

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
    let runtime = Arc::new(Runtime::with_journal(journal));
    let token = match listen {
        Listen::Stdio => String::new(),
        Listen::Ws(_) => required("ALWITH_RUNTIME_TOKEN"),
    };
    // 模块:`ALWITH_MODULES_DIR`(应用的 node_modules)里每个带 alwith-module.json 的包一个子进程。
    let modules_dir = env::var_os("ALWITH_MODULES_DIR").map(std::path::PathBuf::from);
    let module_host = modules_dir.as_ref().map(|_| ModuleHost::new(Arc::clone(&runtime), env!("CARGO_PKG_VERSION")));
    let extension: Arc<dyn Extension> = match &module_host {
        Some(host) => Arc::new(ModuleExtension(Arc::clone(host))),
        None => Arc::new(NoExtension),
    };
    let extension = Arc::new(alwith_runtime_services::ServiceExtension::from_env(extension)?);
    let launcher = Arc::new(TableLauncher {
        engines,
        version: env!("CARGO_PKG_VERSION").to_string(),
        services: Arc::clone(&extension),
    });
    let server = RuntimeServer::new(runtime, launcher, extension, token);
    if let (Some(host), Some(dir)) = (&module_host, &modules_dir) {
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
        list.iter().map(|item| item.to_string()).collect()
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
}
