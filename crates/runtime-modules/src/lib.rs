//! Module host. A module is a package: a directory with an `alwith-module.json` manifest plus an
//! executable entry. Runtime spawns the entry as a child process according to the manifest and speaks
//! to it on stdio with the same one-frame-per-line protocol the app uses.
//!
//! Three kinds of frame, told apart by their fields:
//!
//! - Module -> Runtime request `{id, method, params}`: the module is a client and may call any Runtime
//!   method; the owner is recorded as `module:<name>`. Runtime answers `{id, result | error}`.
//! - Runtime -> module request: the app calls `modules/<name>/<method>`, and Runtime forwards it to the
//!   module unchanged as `{id, method, params}`; the module answers `{id, result | error}`. Each
//!   direction numbers its own ids.
//! - Events `{type, ...}`: Runtime events are pushed to the module as they are; events sent by the
//!   module are wrapped as `{"type":"module","module":<name>,"event":{...}}` and broadcast to every
//!   app connection.
//!
//! Manifest fields: `name`, `version`, `entry` (a relative path per target triple, or `default`),
//! `runtime` (minimum Runtime version, recorded only), `persistent` (requires Runtime to stay
//! resident), `capabilities` (declares what it will touch, recorded but not enforced). Discovery
//! rules: `<dir>/*/alwith-module.json` and `<dir>/@*/*/alwith-module.json`, i.e. the app's
//! `node_modules`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use alwith_runtime_core::{ResponseError, Runtime, RuntimeEvent, RuntimeEventSink};
use alwith_runtime_ws::{ConnectionState, Extension};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc, oneshot};

pub const MANIFEST_FILE: &str = "alwith-module.json";
pub const METHOD_PREFIX: &str = "modules/";
/// Method name the app uses to query loaded modules.
pub const LIST_METHOD: &str = "modules";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleManifest {
    pub name: String,
    pub version: String,
    /// target triple -> entry relative path; falls back to `default` when nothing matches.
    pub entry: BTreeMap<String, String>,
    #[serde(default)]
    pub runtime: Option<String>,
    #[serde(default)]
    pub persistent: bool,
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Optional RPC namespaces served by this module, not compiled into Runtime.
    #[serde(default)]
    pub routes: Vec<String>,
}

impl ModuleManifest {
    /// Looks for the entry by the host's target triple first, then falls back to `default` if there is none
    /// (or the file is missing, e.g. an unpackaged dev build).
    pub fn entry_path(&self, module_dir: &Path) -> Result<PathBuf, String> {
        let triple = current_triple();
        let candidates = [self.entry.get(&triple), self.entry.get("default")];
        let mut tried = Vec::new();
        for relative in candidates.into_iter().flatten() {
            let path = module_dir.join(relative);
            if path.is_file() {
                return Ok(path);
            }
            tried.push(path.display().to_string());
        }
        if tried.is_empty() {
            return Err(format!("module {} has no entry for {triple} and no default", self.name));
        }
        Err(format!("module {} entry missing: {}", self.name, tried.join(", ")))
    }
}

pub fn current_triple() -> String {
    let arch = std::env::consts::ARCH;
    let os = match std::env::consts::OS {
        "macos" => "apple-darwin",
        "linux" => "unknown-linux-gnu",
        "windows" => "pc-windows-msvc",
        other => other,
    };
    format!("{arch}-{os}")
}

/// Scans a directory (usually the app's `node_modules`) for all packages carrying a manifest.
pub fn discover(modules_dir: &Path) -> Vec<(PathBuf, ModuleManifest)> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(modules_dir) else { return found };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.starts_with('@')) {
            if let Ok(scoped) = std::fs::read_dir(&path) {
                for scoped_entry in scoped.flatten() {
                    push_if_module(&mut found, &scoped_entry.path());
                }
            }
            continue;
        }
        push_if_module(&mut found, &path);
    }
    found.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    found
}

fn push_if_module(found: &mut Vec<(PathBuf, ModuleManifest)>, dir: &Path) {
    let manifest_path = dir.join(MANIFEST_FILE);
    if !manifest_path.is_file() {
        return;
    }
    match std::fs::read_to_string(&manifest_path)
        .map_err(|error| error.to_string())
        .and_then(|text| serde_json::from_str::<ModuleManifest>(&text).map_err(|error| error.to_string()))
    {
        // node_modules often holds symlinks (workspace / bun link): resolve the entry against the real
        // directory, otherwise the relative path misses.
        Ok(manifest) => found.push((dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf()), manifest)),
        Err(error) => log::warn!("[modules] {} unreadable: {error}", manifest_path.display()),
    }
}

/// Manifest `runtime` field: `>=X.Y.Z` (or a bare `X.Y.Z`, equivalent). Only this one form is accepted;
/// any other form is an error rather than pretending to be satisfied.
pub fn runtime_satisfies(version: &str, requirement: &str) -> Result<bool, String> {
    fn parse(text: &str) -> Result<(u64, u64, u64), String> {
        let mut parts = text.trim().split('.').map(|part| part.parse::<u64>());
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(Ok(major)), Some(Ok(minor)), Some(Ok(patch)), None) => Ok((major, minor, patch)),
            _ => Err(format!("not a X.Y.Z version: {text}")),
        }
    }
    let required = requirement.trim().strip_prefix(">=").unwrap_or(requirement.trim());
    Ok(parse(version)? >= parse(required)?)
}

/// Executor of module -> Runtime requests, wired in by the Runtime main program after it builds the
/// RuntimeServer.
pub type Dispatch = Arc<
    dyn Fn(String, String, Value) -> Pin<Box<dyn Future<Output = Result<Value, ResponseError>> + Send>> + Send + Sync,
>;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, ResponseError>>>>>;
type ModuleInput = Arc<tokio::sync::Mutex<Option<ChildStdin>>>;

struct PendingCall {
    id: u64,
    pending: Pending,
    stdin: ModuleInput,
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        if self.pending.lock().expect("pending lock poisoned").remove(&self.id).is_none() {
            return;
        }
        let input = Arc::clone(&self.stdin);
        let line = format!("{}\n", serde_json::json!({"method":"_runtime/cancel", "params":{"id":self.id}}));
        tokio::spawn(async move {
            let mut input = input.lock().await;
            if let Some(input) = input.as_mut() {
                let _ = input.write_all(line.as_bytes()).await;
                let _ = input.flush().await;
            }
        });
    }
}

struct ModuleProcess {
    manifest: ModuleManifest,
    stdin: ModuleInput,
    pending: Pending,
    next_id: AtomicU64,
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// Runtime holds only a weak reference to the sink; the strong reference hangs on the module process, so
    /// events keep flowing as long as the process lives.
    _sink: Arc<dyn RuntimeEventSink>,
    _child: Child,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleStatus {
    pub name: String,
    pub version: String,
    pub persistent: bool,
    pub capabilities: Vec<String>,
    pub alive: bool,
}

/// An app connection's subscription to module events: when the queue is full the protocol layer
/// disconnects the slow connection (the same policy as Runtime events),
/// the client catches up after reconnecting; a subscription is never silently dropped while the
/// connection believes it is still receiving.
struct Subscriber {
    id: u64,
    events: mpsc::Sender<String>,
    disconnect: Arc<Notify>,
    routed_modules: HashSet<String>,
}

struct Delegation {
    module: String,
    owner: String,
    connection_id: u64,
}

struct DelegationGuard<'a> {
    id: u64,
    calls: &'a Mutex<HashMap<u64, Delegation>>,
}
impl Drop for DelegationGuard<'_> {
    fn drop(&mut self) {
        self.calls.lock().expect("module call scopes").remove(&self.id);
    }
}

pub struct ModuleHost {
    runtime: Arc<Runtime>,
    /// This Runtime's version; modules whose manifest `runtime` field requires a higher one are not loaded.
    runtime_version: String,
    modules: Mutex<HashMap<String, Arc<ModuleProcess>>>,
    subscribers: Mutex<Vec<Subscriber>>,
    next_subscriber: AtomicU64,
    next_call: AtomicU64,
    calls: Mutex<HashMap<u64, Delegation>>,
    dispatch: OnceLock<Dispatch>,
}

impl ModuleHost {
    pub fn new(runtime: Arc<Runtime>, runtime_version: impl Into<String>) -> Arc<Self> {
        let host = Arc::new(Self {
            runtime,
            runtime_version: runtime_version.into(),
            modules: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(Vec::new()),
            next_subscriber: AtomicU64::new(1),
            next_call: AtomicU64::new(1),
            calls: Mutex::new(HashMap::new()),
            dispatch: OnceLock::new(),
        });
        let weak = Arc::downgrade(&host);
        host.runtime.set_tool_dispatch(Arc::new(move |module, agent_id, params| {
            let weak = weak.clone();
            Box::pin(async move {
                let host = weak.upgrade().ok_or_else(|| ResponseError::connection_lost("module host stopped"))?;
                host.call_module(&module, "_runtime/toolCall", serde_json::json!({"agentId":agent_id,"params":params}))
                    .await
            })
        }));
        host
    }

    /// Wires in the executor of module -> Runtime requests. Called once after the RuntimeServer is built.
    pub fn set_dispatch(&self, dispatch: Dispatch) {
        let _ = self.dispatch.set(dispatch);
    }

    /// Loads every module in the directory; a single module failing to start is only logged and does not
    /// affect the others.
    pub async fn load_dir(self: &Arc<Self>, modules_dir: &Path) -> Vec<ModuleStatus> {
        for (dir, manifest) in discover(modules_dir) {
            if let Err(error) = self.spawn(&dir, manifest.clone()).await {
                log::error!("[modules] {} failed to start: {error}", manifest.name);
            }
        }
        self.list()
    }

    pub fn list(&self) -> Vec<ModuleStatus> {
        let mut statuses: Vec<ModuleStatus> = self
            .modules
            .lock()
            .expect("modules lock poisoned")
            .values()
            .map(|module| ModuleStatus {
                name: module.manifest.name.clone(),
                version: module.manifest.version.clone(),
                persistent: module.manifest.persistent,
                capabilities: module.manifest.capabilities.clone(),
                alive: module.alive.load(Ordering::Relaxed),
            })
            .collect();
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        statuses
    }

    /// Whether any resident (persistent) module is alive (Runtime's exit policy uses this to decide whether
    /// to stay).
    pub fn has_persistent(&self) -> bool {
        self.modules
            .lock()
            .expect("modules lock poisoned")
            .values()
            .any(|module| module.manifest.persistent && module.alive.load(Ordering::Relaxed))
    }

    pub async fn spawn(self: &Arc<Self>, module_dir: &Path, manifest: ModuleManifest) -> Result<(), String> {
        {
            let modules = self.modules.lock().expect("modules lock poisoned");
            if modules.contains_key(&manifest.name) {
                return Err(format!("duplicate module {}", manifest.name));
            }
            let mut routes = HashSet::new();
            for route in &manifest.routes {
                if route.is_empty()
                    || route.contains('/')
                    || route.starts_with('_')
                    || route == "modules"
                    || !routes.insert(route)
                    || modules.values().any(|module| module.manifest.routes.contains(route))
                {
                    return Err(format!("invalid or duplicate module route {route}"));
                }
            }
        }
        if let Some(requirement) = &manifest.runtime
            && !runtime_satisfies(&self.runtime_version, requirement)?
        {
            return Err(format!(
                "module {} needs runtime {requirement}, this runtime is {}",
                manifest.name, self.runtime_version
            ));
        }
        let entry = manifest.entry_path(module_dir)?;
        let mut command = Command::new(&entry);
        command
            .current_dir(module_dir)
            .env("ALWITH_MODULE_NAME", &manifest.name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| format!("spawn {}: {error}", entry.display()))?;
        let stdin = Arc::new(tokio::sync::Mutex::new(Some(child.stdin.take().expect("piped stdin"))));
        let stdout = child.stdout.take().expect("piped stdout");
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let name = manifest.name.clone();
        log::info!("[modules] {name} {} started (pid {:?})", manifest.version, child.id());

        // Runtime events -> module stdin (a module is a client).
        let sink: Arc<dyn RuntimeEventSink> =
            Arc::new(ModuleEventSink { stdin: Arc::clone(&stdin), alive: Arc::clone(&alive) });
        let sink_id = self.runtime.add_sink(&sink);

        let process = Arc::new(ModuleProcess {
            manifest,
            stdin: Arc::clone(&stdin),
            pending: Arc::clone(&pending),
            next_id: AtomicU64::new(1),
            alive: Arc::clone(&alive),
            _sink: sink,
            _child: child,
        });
        self.modules.lock().expect("modules lock poisoned").insert(name.clone(), Arc::clone(&process));

        let host = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            // Requests may call back into the same module. Keep consuming responses
            // while dispatch is pending, and own every dispatch until pipe EOF.
            let mut requests = tokio::task::JoinSet::new();
            loop {
                let line = tokio::select! {
                    result = requests.join_next(), if !requests.is_empty() => {
                        if let Some(Err(error)) = result {
                            log::error!("[modules] {name}: request task failed: {error}");
                        }
                        continue;
                    }
                    result = lines.next_line() => match result {
                        Ok(Some(line)) => line,
                        Ok(None) => break,
                        Err(error) => {
                            log::warn!("[modules] {name}: stdout failed: {error}");
                            break;
                        }
                    },
                };
                let text = line.trim();
                if text.is_empty() {
                    continue;
                }
                let value: Value = match serde_json::from_str(text) {
                    Ok(value) => value,
                    Err(error) => {
                        log::warn!("[modules] {name}: unparseable frame: {error}");
                        continue;
                    }
                };
                if value.get("type").is_some() {
                    host.broadcast(&name, value);
                } else if value.get("method").is_some() {
                    let host = Arc::clone(&host);
                    let name = name.clone();
                    let stdin = Arc::clone(&stdin);
                    requests.spawn(async move {
                        host.handle_module_request(&name, &stdin, value).await;
                    });
                } else if let Some(id) = value.get("id").and_then(Value::as_u64) {
                    let response = match value.get("error") {
                        Some(error) if !error.is_null() => Err(ResponseError::parse(error)),
                        _ => Ok(value.get("result").cloned().unwrap_or(Value::Null)),
                    };
                    if let Some(sender) = pending.lock().expect("pending lock poisoned").remove(&id) {
                        let _ = sender.send(response);
                    }
                }
            }
            alive.store(false, Ordering::Relaxed);
            requests.abort_all();
            while requests.join_next().await.is_some() {}
            for subscriber in host.subscribers.lock().expect("subscribers lock poisoned").iter() {
                if subscriber.routed_modules.contains(&name) {
                    subscriber.disconnect.notify_one();
                }
            }
            host.runtime.remove_sink(sink_id);
            host.runtime.detach_owners_under(&format!("module:{name}")).await;
            let pending: Vec<_> = pending.lock().expect("pending lock poisoned").drain().collect();
            for (_, sender) in pending {
                let _ = sender.send(Err(ResponseError::connection_lost(format!("module {name} exited"))));
            }
            log::warn!("[modules] {name} exited");
        });
        Ok(())
    }

    fn broadcast(&self, module: &str, event: Value) {
        if matches!(event.get("type").and_then(Value::as_str), Some("_runtime/service" | "_runtime/disconnect")) {
            let Some(id) = event.get("connectionId").and_then(Value::as_u64) else { return };
            let subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
            if let Some(subscriber) =
                subscribers.iter().find(|subscriber| subscriber.id == id && subscriber.routed_modules.contains(module))
            {
                if event["type"] == "_runtime/disconnect" {
                    subscriber.disconnect.notify_one();
                } else if let Some(payload) = event.get("event") {
                    let frame = serde_json::json!({"type":"service", "event":payload}).to_string();
                    if subscriber.events.try_send(frame).is_err() {
                        subscriber.disconnect.notify_one();
                    }
                }
            }
            return;
        }
        let payload = serde_json::json!({ "type": "module", "module": module, "event": event }).to_string();
        let mut subscribers = self.subscribers.lock().expect("subscribers lock poisoned");
        subscribers.retain(|subscriber| match subscriber.events.try_send(payload.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                subscriber.disconnect.notify_one();
                true
            }
            Err(TrySendError::Closed(_)) => false,
        });
    }

    /// Private-pipe delegation lasts only for the active application call and connection.
    /// The module cannot nominate an owner or reuse a scope after its call returns.
    fn delegated_request(&self, module: &str, params: Value) -> Result<(String, String, Value), ResponseError> {
        let call_id = params["callId"].as_u64().ok_or_else(|| ResponseError::invalid_params("callId required"))?;
        let calls = self.calls.lock().expect("module call scopes");
        let call = calls
            .get(&call_id)
            .filter(|call| call.module == module)
            .ok_or_else(|| ResponseError::runtime("module call scope expired or belongs to another module"))?;
        if !self
            .subscribers
            .lock()
            .expect("subscribers lock poisoned")
            .iter()
            .any(|subscriber| subscriber.id == call.connection_id)
        {
            return Err(ResponseError::connection_lost("application connection closed"));
        }
        let method = params["method"].as_str().ok_or_else(|| ResponseError::invalid_params("method required"))?;
        if method != "acpRequest" {
            return Err(ResponseError::invalid_params("call delegation only permits ACP requests"));
        }
        let mut arguments = params["params"].clone();
        arguments
            .as_object_mut()
            .ok_or_else(|| ResponseError::invalid_params("params must be an object"))?
            .remove("owner");
        Ok((call.owner.clone(), method.to_string(), arguments))
    }

    async fn handle_module_request(&self, module: &str, stdin: &ModuleInput, value: Value) {
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        let method = value.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        let result = match self.dispatch.get() {
            Some(dispatch) if method == "_runtime/requestForCall" => match self.delegated_request(module, params) {
                Ok((owner, method, params)) => dispatch(owner, method, params).await,
                Err(error) => Err(error),
            },
            Some(dispatch) => dispatch(format!("module:{module}"), method, params).await,
            None => Err(ResponseError::runtime("module host has no dispatcher")),
        };
        let response = match result {
            Ok(result) => serde_json::json!({ "id": id, "result": result, "error": null }),
            Err(error) => serde_json::json!({ "id": id, "result": null, "error": error }),
        };
        let mut line = response.to_string();
        line.push('\n');
        let mut stdin = stdin.lock().await;
        let Some(stdin) = stdin.as_mut() else { return };
        let _ = stdin.write_all(line.as_bytes()).await;
        let _ = stdin.flush().await;
    }

    /// App -> module request.
    pub async fn call_module(&self, module: &str, method: &str, params: Value) -> Result<Value, ResponseError> {
        let process = self
            .modules
            .lock()
            .expect("modules lock poisoned")
            .get(module)
            .cloned()
            .ok_or_else(|| ResponseError::runtime(format!("module {module} is not loaded")))?;
        if !process.alive.load(Ordering::Relaxed) {
            return Err(ResponseError::runtime(format!("module {module} is not running")));
        }
        let id = process.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        process.pending.lock().expect("pending lock poisoned").insert(id, sender);
        let _pending = PendingCall { id, pending: Arc::clone(&process.pending), stdin: Arc::clone(&process.stdin) };
        let mut line = serde_json::json!({ "id": id, "method": method, "params": params }).to_string();
        line.push('\n');
        {
            let mut stdin = process.stdin.lock().await;
            let Some(stdin) = stdin.as_mut() else {
                process.pending.lock().expect("pending lock poisoned").remove(&id);
                return Err(ResponseError::connection_lost(format!("module {module}: stdin closed")));
            };
            if let Err(error) = stdin.write_all(line.as_bytes()).await.and(stdin.flush().await) {
                process.pending.lock().expect("pending lock poisoned").remove(&id);
                return Err(ResponseError::connection_lost(format!("module {module}: write failed: {error}")));
            }
        }
        receiver.await.unwrap_or_else(|_| Err(ResponseError::connection_lost(format!("module {module} exited"))))
    }
}

struct ModuleEventSink {
    stdin: ModuleInput,
    alive: Arc<std::sync::atomic::AtomicBool>,
}

impl RuntimeEventSink for ModuleEventSink {
    fn publish(&self, event: RuntimeEvent) {
        if !self.alive.load(Ordering::Relaxed) {
            return;
        }
        let mut line = serde_json::to_string(&event).expect("serialize runtime event");
        line.push('\n');
        let stdin = Arc::clone(&self.stdin);
        tokio::spawn(async move {
            let mut stdin = stdin.lock().await;
            let Some(stdin) = stdin.as_mut() else { return };
            let _ = stdin.write_all(line.as_bytes()).await;
            let _ = stdin.flush().await;
        });
    }
}

struct Subscription {
    id: u64,
    host: Arc<ModuleHost>,
    closed: std::sync::atomic::AtomicBool,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.host.subscribers.lock().expect("subscribers lock poisoned").retain(|subscriber| subscriber.id != self.id);
    }
}

/// Attaches the module host to the RuntimeServer's `Extension` slot.
pub struct ModuleExtension(pub Arc<ModuleHost>);

#[async_trait]
impl Extension for ModuleExtension {
    fn connect(&self, _owner_id: &str, events: mpsc::Sender<String>, disconnect: Arc<Notify>) -> ConnectionState {
        let host = &self.0;
        let id = host.next_subscriber.fetch_add(1, Ordering::Relaxed);
        host.subscribers.lock().expect("subscribers lock poisoned").push(Subscriber {
            id,
            events,
            disconnect,
            routed_modules: HashSet::new(),
        });
        Box::new(Subscription { id, host: Arc::clone(host), closed: std::sync::atomic::AtomicBool::new(false) })
    }

    async fn call(
        &self, owner_id: &str, connection: &ConnectionState, method: &str, params: Value,
    ) -> Result<Option<Value>, ResponseError> {
        if method == LIST_METHOD {
            return serde_json::to_value(self.0.list())
                .map(Some)
                .map_err(|error| ResponseError::runtime(error.to_string()));
        }
        let subscription = connection.downcast_ref::<Subscription>().expect("module subscription");
        if subscription.closed.load(Ordering::Acquire) {
            return Err(ResponseError::connection_lost("module connection closed"));
        }
        let Some(rest) = method.strip_prefix(METHOD_PREFIX) else {
            let namespace = method.split_once('/').map(|(namespace, _)| namespace);
            let module = self
                .0
                .modules
                .lock()
                .expect("modules lock poisoned")
                .values()
                .find(|module| {
                    namespace.is_some_and(|namespace| module.manifest.routes.iter().any(|route| route == namespace))
                })
                .map(|module| module.manifest.name.clone());
            let Some(module) = module else { return Ok(None) };
            {
                let mut subscribers = self.0.subscribers.lock().expect("subscribers lock poisoned");
                let subscriber = subscribers
                    .iter_mut()
                    .find(|subscriber| subscriber.id == subscription.id)
                    .ok_or_else(|| ResponseError::connection_lost("module connection closed"))?;
                subscriber.routed_modules.insert(module.clone());
            }
            let delegated = self.0.modules.lock().expect("modules lock poisoned").get(&module).is_some_and(|process| {
                process.manifest.capabilities.iter().any(|capability| capability == "owner-requests")
            });
            let mut params = params;
            let owner = if delegated {
                let label = params.as_object_mut().and_then(|params| params.remove("owner"));
                let label = match label {
                    Some(Value::String(label)) => Some(label),
                    Some(Value::Null) | None => None,
                    Some(_) => return Err(ResponseError::invalid_params("owner must be a string")),
                };
                alwith_runtime_ws::owner_of(owner_id, label)
            } else {
                owner_id.to_string()
            };
            let mut envelope =
                serde_json::json!({"connectionId":subscription.id,"owner":owner,"method":method,"params":params});
            let _scope = if delegated {
                let id = self.0.next_call.fetch_add(1, Ordering::Relaxed);
                self.0
                    .calls
                    .lock()
                    .expect("module call scopes")
                    .insert(id, Delegation { module: module.clone(), owner, connection_id: subscription.id });
                envelope["callId"] = id.into();
                Some(DelegationGuard { id, calls: &self.0.calls })
            } else {
                None
            };
            return self.0.call_module(&module, "_runtime/call", envelope).await.map(Some);
        };
        let Some((module, module_method)) = rest.split_once('/') else {
            return Err(ResponseError::invalid_params(format!(
                "module method must look like modules/<name>/<method>, got {method}"
            )));
        };
        if module_method.starts_with("_runtime/") {
            return Err(ResponseError::method_not_found(method));
        }
        self.0.call_module(module, module_method, params).await.map(Some)
    }

    async fn disconnect(&self, connection: &ConnectionState) {
        let subscription = connection.downcast_ref::<Subscription>().expect("module subscription");
        subscription.closed.store(true, Ordering::Release);
        let modules = {
            let mut subscribers = self.0.subscribers.lock().expect("subscribers lock poisoned");
            let modules = subscribers
                .iter()
                .find(|subscriber| subscriber.id == subscription.id)
                .map(|subscriber| subscriber.routed_modules.clone())
                .unwrap_or_default();
            subscribers.retain(|subscriber| subscriber.id != subscription.id);
            modules
        };
        for module in modules {
            // A dead module must not prevent the transport from releasing the remaining resources.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.0.call_module(&module, "_runtime/disconnect", serde_json::json!({"connectionId":subscription.id})),
            )
            .await;
        }
    }

    async fn shutdown(&self) {
        let modules: Vec<Arc<ModuleProcess>> =
            self.0.modules.lock().expect("modules lock poisoned").values().cloned().collect();
        for module in modules {
            let mut stdin = module.stdin.lock().await;
            // ChildStdin::shutdown does not close the owned pipe on every platform.
            // Drop the sole handle so modules observe EOF and dispatch tasks stop.
            stdin.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alwith_runtime_core::{AgentMetadata, Journal};
    use alwith_runtime_ws::{AgentLauncher, RuntimeCommand, RuntimeServer, execute};

    struct NoLauncher;

    #[async_trait]
    impl AgentLauncher for NoLauncher {
        async fn prepare(&self, _: &str, _: &str, _: Value) -> Result<(Command, AgentMetadata), String> {
            Err("no engines in this test".into())
        }
        fn version(&self) -> String {
            "0.0.0-test".into()
        }
    }

    /// A module written in python: answers echo / fail, sends one event at startup and one ping to Runtime.
    const MODULE_SCRIPT: &str = r#"#!/usr/bin/env python3
import json, sys
print(json.dumps({"type": "hello", "pid": 1}), flush=True)
print(json.dumps({"id": 1, "method": "ping"}), flush=True)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    value = json.loads(line)
    if "method" in value:
        if value["method"] == "echo":
            print(json.dumps({"id": value["id"], "result": value.get("params")}), flush=True)
        else:
            print(json.dumps({"id": value["id"], "error": {"code": -32601, "message": "unknown " + value["method"]}}), flush=True)
    elif "id" in value:
        print(json.dumps({"type": "runtime-answered", "result": value.get("result")}), flush=True)
    elif "type" in value:
        print(json.dumps({"type": "saw-runtime-event", "event": value["type"]}), flush=True)
"#;

    fn write_module(root: &Path, name: &str) {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let entry = dir.join("entry.py");
        std::fs::write(&entry, MODULE_SCRIPT).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::fs::write(
            dir.join(MANIFEST_FILE),
            serde_json::json!({ "name": name, "version": "0.0.1", "entry": { "default": "entry.py" }, "persistent": true, "capabilities": ["network"] }).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn delegated_requests_are_bound_to_module_call_and_live_connection() {
        let journal = tempfile::tempdir().unwrap();
        let host = ModuleHost::new(Arc::new(Runtime::with_journal(Journal::open(journal.path()).unwrap())), "0.3.0");
        let (events, _) = mpsc::channel(1);
        host.subscribers.lock().unwrap().push(Subscriber {
            id: 7,
            events,
            disconnect: Arc::new(Notify::new()),
            routed_modules: HashSet::new(),
        });
        host.calls
            .lock()
            .unwrap()
            .insert(11, Delegation { module: "services".into(), owner: "window:actual".into(), connection_id: 7 });
        let request = serde_json::json!({"callId":11,"method":"acpRequest","params":{"agentId":"agent","method":"providers/set","owner":"forged"}});
        let scope = DelegationGuard { id: 11, calls: &host.calls };
        let (owner, _, params) = host.delegated_request("services", request.clone()).unwrap();
        assert_eq!(owner, "window:actual");
        assert!(params.get("owner").is_none());
        assert!(host.delegated_request("different-module", request.clone()).is_err());
        let mut forbidden = request.clone();
        forbidden["method"] = "start".into();
        assert!(host.delegated_request("services", forbidden).is_err());
        host.subscribers.lock().unwrap().clear();
        assert!(host.delegated_request("services", request.clone()).is_err());
        drop(scope);
        assert!(host.delegated_request("services", request).is_err());
        assert!(host.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn discovers_plain_and_scoped_packages() {
        let root = tempfile::tempdir().unwrap();
        write_module(root.path(), "b-mod");
        write_module(&root.path().join("@alwith"), "a-mod");
        std::fs::create_dir_all(root.path().join("not-a-module")).unwrap();
        let found = discover(root.path());
        let names: Vec<&str> = found.iter().map(|(_, manifest)| manifest.name.as_str()).collect();
        assert_eq!(names, vec!["a-mod", "b-mod"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_module_request_can_wait_for_a_callback_to_the_same_module() {
        let root = tempfile::tempdir().unwrap();
        write_module(root.path(), "reverse");
        let journal = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(journal.path()).unwrap()));
        let host = ModuleHost::new(runtime, "0.3.0");
        let weak_host = Arc::downgrade(&host);
        let (completed, received) = oneshot::channel();
        let completed = Arc::new(Mutex::new(Some(completed)));
        host.set_dispatch(Arc::new(move |owner, method, _| {
            let host = weak_host.upgrade().unwrap();
            let completed = Arc::clone(&completed);
            Box::pin(async move {
                assert_eq!(owner, "module:reverse");
                assert_eq!(method, "ping");
                let result = host.call_module("reverse", "echo", serde_json::json!({"tool": "read"})).await;
                completed.lock().unwrap().take().unwrap().send(result.clone()).unwrap();
                result
            })
        }));
        host.load_dir(root.path()).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), received)
            .await
            .expect("reverse callback must not deadlock")
            .unwrap()
            .unwrap();
        assert_eq!(result, serde_json::json!({"tool": "read"}));
        ModuleExtension(Arc::clone(&host)).shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn module_eof_cancels_its_pending_dispatch_tasks() {
        struct Dropped(Arc<Notify>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let root = tempfile::tempdir().unwrap();
        write_module(root.path(), "closing");
        let journal = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(journal.path()).unwrap()));
        let host = ModuleHost::new(runtime, "0.3.0");
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let start_signal = Arc::clone(&started);
        let drop_signal = Arc::clone(&dropped);
        host.set_dispatch(Arc::new(move |_, _, _| {
            let started = Arc::clone(&start_signal);
            let dropped = Arc::clone(&drop_signal);
            Box::pin(async move {
                let _guard = Dropped(dropped);
                started.notify_one();
                std::future::pending().await
            })
        }));
        host.load_dir(root.path()).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), started.notified()).await.unwrap();
        ModuleExtension(Arc::clone(&host)).shutdown().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped.notified())
            .await
            .expect("pipe EOF must release outstanding dispatch work");
        assert!(!host.list()[0].alive);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn modules_answer_app_requests_and_can_call_the_runtime() {
        if !Path::new("/usr/bin/env").exists() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        write_module(root.path(), "echo");
        let journal = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(journal).unwrap()));
        let host = ModuleHost::new(Arc::clone(&runtime), "0.3.0");
        let server = RuntimeServer::new(
            runtime,
            Arc::new(NoLauncher),
            Arc::new(ModuleExtension(Arc::clone(&host))),
            String::new(),
        );
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
        let statuses = host.load_dir(root.path()).await;
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].alive && statuses[0].persistent);
        assert!(host.has_persistent());

        // An app connection through the stdio entry.
        let (client_reader, server_writer) = tokio::io::duplex(64 * 1024);
        let (server_reader, mut client_writer) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            alwith_runtime_stdio::serve(server, BufReader::new(server_reader), server_writer, std::future::pending())
                .await
                .unwrap();
        });
        let mut reader = BufReader::new(client_reader);
        let request =
            serde_json::json!({ "id": 7, "method": "modules/echo/echo", "params": { "x": 1 } }).to_string() + "\n";
        client_writer.write_all(request.as_bytes()).await.unwrap();
        let listing = serde_json::json!({ "id": 8, "method": "modules" }).to_string() + "\n";
        client_writer.write_all(listing.as_bytes()).await.unwrap();
        let unknown = serde_json::json!({ "id": 9, "method": "modules/echo/nope" }).to_string() + "\n";
        client_writer.write_all(unknown.as_bytes()).await.unwrap();

        // Runtime events must reach the module: the module reports each received event type back as its own
        // event.
        host.runtime.publish(RuntimeEvent::Stderr { agent_id: "probe".into(), line: "hello module".into() });

        let mut echoed = None;
        let mut listed = None;
        let mut failed = None;
        let mut hello = false;
        let mut runtime_answered = false;
        let mut saw_runtime_event = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while echoed.is_none()
            || listed.is_none()
            || failed.is_none()
            || !hello
            || !runtime_answered
            || !saw_runtime_event
        {
            let mut line = String::new();
            tokio::time::timeout_at(deadline, reader.read_line(&mut line)).await.expect("frames in time").unwrap();
            let value: Value = serde_json::from_str(line.trim()).unwrap();
            match (value.get("id").and_then(Value::as_u64), value.get("type").and_then(Value::as_str)) {
                (Some(7), _) => echoed = value.get("result").cloned(),
                (Some(8), _) => listed = value.get("result").cloned(),
                (Some(9), _) => failed = value.pointer("/error/message").and_then(Value::as_str).map(str::to_string),
                (_, Some("module")) => match value.pointer("/event/type").and_then(Value::as_str) {
                    Some("hello") => hello = true,
                    Some("runtime-answered") => {
                        assert_eq!(value.pointer("/event/result"), Some(&Value::String("pong".into())));
                        runtime_answered = true;
                    }
                    Some("saw-runtime-event") => {
                        assert_eq!(value.pointer("/event/event"), Some(&Value::String("stderr".into())));
                        saw_runtime_event = true;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        assert_eq!(echoed, Some(serde_json::json!({ "x": 1 })));
        assert_eq!(listed.unwrap()[0]["name"], "echo");
        assert_eq!(failed.unwrap(), "unknown nope");
    }

    #[tokio::test]
    async fn a_full_event_queue_disconnects_the_slow_connection_instead_of_dropping_its_subscription() {
        let journal = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(journal).unwrap()));
        let host = ModuleHost::new(runtime, "0.3.0");
        let extension = ModuleExtension(Arc::clone(&host));
        let (events, mut receiver) = mpsc::channel::<String>(1);
        let disconnect = Arc::new(Notify::new());
        let _connection = extension.connect("web-1", events, Arc::clone(&disconnect));

        host.broadcast("m", serde_json::json!({ "type": "one" }));
        host.broadcast("m", serde_json::json!({ "type": "two" }));
        tokio::time::timeout(std::time::Duration::from_secs(1), disconnect.notified())
            .await
            .expect("slow connection told to disconnect");
        assert_eq!(host.subscribers.lock().unwrap().len(), 1, "subscription stays until the connection drops");
        assert!(receiver.recv().await.unwrap().contains("\"one\""));
        drop(_connection);
        assert!(host.subscribers.lock().unwrap().is_empty());
    }

    #[test]
    fn manifest_runtime_requirement_is_a_minimum_version() {
        assert!(runtime_satisfies("0.3.0", ">=0.3.0").unwrap());
        assert!(runtime_satisfies("0.10.1", ">=0.3.0").unwrap());
        assert!(!runtime_satisfies("0.2.9", ">=0.3.0").unwrap());
        assert!(runtime_satisfies("1.0.0", "0.3.0").unwrap());
        assert!(runtime_satisfies("0.3.0", "^0.3").is_err());
    }

    #[tokio::test]
    async fn a_module_that_needs_a_newer_runtime_is_not_loaded() {
        let root = tempfile::tempdir().unwrap();
        write_module(root.path(), "future");
        let manifest_path = root.path().join("future").join(MANIFEST_FILE);
        let mut manifest: Value = serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest["runtime"] = Value::String(">=9.0.0".into());
        std::fs::write(&manifest_path, manifest.to_string()).unwrap();
        let journal = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(journal).unwrap()));
        let host = ModuleHost::new(runtime, "0.3.0");
        let statuses = host.load_dir(root.path()).await;
        assert!(statuses.is_empty(), "{statuses:?}");
    }

    #[tokio::test]
    async fn private_module_operations_are_not_public_rpc() {
        let root = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root.path()).unwrap()));
        let extension = ModuleExtension(ModuleHost::new(runtime, "0.4.0"));
        let (events, _receiver) = mpsc::channel(8);
        let connection = extension.connect("client", events, Arc::new(Notify::new()));
        for method in ["_runtime/resolveLaunch", "_runtime/call", "_runtime/disconnect", "_runtime/ready"] {
            let error = extension
                .call("client", &connection, &format!("modules/example/{method}"), Value::Null)
                .await
                .unwrap_err();
            assert_eq!(error.code, ResponseError::METHOD_NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn service_events_only_reach_the_connection_routed_to_that_module() {
        let root = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root.path()).unwrap()));
        let host = ModuleHost::new(runtime, "0.4.0");
        let extension = ModuleExtension(Arc::clone(&host));
        let (first, mut first_events) = mpsc::channel(8);
        let (second, mut second_events) = mpsc::channel(8);
        let disconnect = Arc::new(Notify::new());
        let first = extension.connect("first", first, Arc::clone(&disconnect));
        let _second = extension.connect("second", second, Arc::new(Notify::new()));
        let id = first.downcast_ref::<Subscription>().unwrap().id;
        host.subscribers
            .lock()
            .unwrap()
            .iter_mut()
            .find(|subscriber| subscriber.id == id)
            .unwrap()
            .routed_modules
            .insert("files".into());
        let event = serde_json::json!({"type":"_runtime/service","connectionId":id,"event":{"type":"fsChanged"}});
        host.broadcast("unrelated", event.clone());
        assert!(first_events.try_recv().is_err());
        host.broadcast("files", event);
        assert_eq!(
            serde_json::from_str::<Value>(&first_events.try_recv().unwrap()).unwrap(),
            serde_json::json!({"type":"service","event":{"type":"fsChanged"}})
        );
        assert!(second_events.try_recv().is_err());
        host.broadcast("files", serde_json::json!({"type":"_runtime/disconnect","connectionId":id}));
        tokio::time::timeout(std::time::Duration::from_secs(1), disconnect.notified()).await.unwrap();
    }
}
