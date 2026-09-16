//! Built-in Runtime services, shared by stdio and WebSocket.
//! No Tauri application or separate host process is required.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
use alwith_api::AgentMetadata;
pub mod providers;
mod configuration;
pub use configuration::ServicesConfig;
use alwith_fs::FileSystem;
use alwith_qa::{QaEnvironmentSnapshot, QaHost, QaRunRequest, QaStream};
use alwith_runtime_core::ResponseError;
use alwith_runtime_ws::{ConnectionState, Extension};
use alwith_store::{ScopedStores, StoreScope};
use async_trait::async_trait;
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use tokio::process::Command;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum ServiceCommand {
    #[serde(rename = "fs/readText")]
    FsReadText { path: String },
    #[serde(rename = "fs/readBase64")]
    FsReadBase64 { path: String },
    #[serde(rename = "fs/writeText")]
    FsWriteText { path: String, content: String },
    #[serde(rename = "fs/writeBase64")]
    FsWriteBase64 { path: String, content: String },
    #[serde(rename = "fs/appendText")]
    FsAppendText { path: String, content: String },
    #[serde(rename = "fs/exists")]
    FsExists { path: String },
    #[serde(rename = "fs/readDir")]
    FsReadDir { path: String },
    #[serde(rename = "fs/readDirFiltered")]
    FsReadDirFiltered { path: String, show_hidden: bool, show_gitignored: bool },
    #[serde(rename = "fs/stat")]
    FsStat { path: String },
    #[serde(rename = "fs/createDir")]
    FsCreateDir { path: String },
    #[serde(rename = "fs/remove")]
    FsRemove { path: String },
    #[serde(rename = "fs/rename")]
    FsRename { from: String, to: String },
    #[serde(rename = "fs/copy")]
    FsCopy { from: String, to: String },
    #[serde(rename = "fs/watch")]
    FsWatch { watcher_id: String, path: String, recursive: bool },
    #[serde(rename = "fs/unwatch")]
    FsUnwatch { watcher_id: String },
    #[serde(rename = "store/entries")]
    StoreEntries { scope: StoreScope, store: String },
    #[serde(rename = "store/set")]
    StoreSet { scope: StoreScope, store: String, key: String, value: Value },
    #[serde(rename = "store/delete")]
    StoreDelete { scope: StoreScope, store: String, key: String },
    #[serde(rename = "store/clear")]
    StoreClear { scope: StoreScope, store: String },
    #[serde(rename = "store/reload")]
    StoreReload { scope: StoreScope, store: String },
    #[serde(rename = "shell/exec")]
    ExecBash { command: String, cwd: String },
    // —— QA 双面(方法名与参数字段 1:1 对齐桌面 tauri 命令,核心逻辑在 alwith_qa)——
    #[serde(rename = "qa/projects")]
    QaProjects { root: String },
    #[serde(rename = "qa/listEnvironments")]
    QaListEnvironments { project: String },
    #[serde(rename = "qa/saveEnvironment")]
    QaSaveEnvironment { project: String, environment: QaEnvironmentSnapshot },
    #[serde(rename = "qa/deleteEnvironment")]
    QaDeleteEnvironment { project: String, environment_id: String },
    #[serde(rename = "qa/scaffoldProject")]
    QaScaffoldProject { root: String, name: String, spec: alwith_qa::QaScaffoldSpec },
    #[serde(rename = "qa/inspectDependencies")]
    QaInspectDependencies { project: String },
    #[serde(rename = "qa/installDependencies")]
    QaInstallDependencies { project: String, dependency_ids: Vec<String> },
    #[serde(rename = "qa/listDevices")]
    QaListDevices { platform: String },
    #[serde(rename = "qa/listCases")]
    QaListCases { project: String },
    #[serde(rename = "qa/runStream")]
    QaRunStream { root: String, project_name: String, run_id: String, request: QaRunRequest },
    #[serde(rename = "qa/cancel")]
    QaCancel { run_id: String },
    #[serde(rename = "qa/projectFiles")]
    QaProjectFiles { root: String, project: String, sub: Option<String> },
    #[serde(rename = "qa/projectFileRead")]
    QaProjectFileRead { root: String, project: String, name: String },
    #[serde(rename = "qa/projectFileWrite")]
    QaProjectFileWrite { root: String, project: String, name: String, text: String },
    #[serde(rename = "qa/projectRelativePath")]
    QaProjectRelativePath { project: String, path: String },
    #[serde(rename = "qa/testSource")]
    QaTestSource { root: String, project: String, file: String },
    #[serde(rename = "qa/testDelete")]
    QaTestDelete { root: String, project: String, file: String },
    #[serde(rename = "qa/testDataFiles")]
    QaTestDataFiles { root: String, project: String, file: String },
    #[serde(rename = "qa/testDataFile")]
    QaTestDataFile { root: String, project: String, file: String, name: String },
    #[serde(rename = "qa/testResults")]
    QaTestResults { root: String, project: String },
    #[serde(rename = "qa/caseStatuses")]
    QaCaseStatuses { root: String, project: String, environment_id: String },
    #[serde(rename = "qa/runHistory")]
    QaRunHistory { root: String, project: String },
    #[serde(rename = "qa/resultFileBase64")]
    QaResultFileBase64 { root: String, project: String, path: String },
    // —— 路径选择 / 文件树只读(capability roots 内)——
    #[serde(rename = "fs/capabilityRoots")]
    FsCapabilityRoots,
    #[serde(rename = "fs/listDirectories")]
    FsListDirectories { path: String },
    #[serde(rename = "fs/listEntries")]
    FsListEntries { path: String },
    // —— Git 只读(gix,禁 shell git;核心逻辑在 alwith_git)——
    #[serde(rename = "git/recentCommits")]
    GitRecentCommits { cwd: String, limit: u32 },
    #[serde(rename = "git/changedFiles")]
    GitChangedFiles { cwd: String, from_revision: String, to_revision: String },
    #[serde(rename = "git/currentBranch")]
    GitCurrentBranch { path: String },
    #[serde(rename = "git/fileStatuses")]
    GitFileStatuses { root: String },
    #[serde(rename = "git/syncStatus")]
    GitSyncStatus { root: String },

}

struct Services {
    file_systems: HashMap<&'static str, FileSystem>,
    stores: Option<Arc<ScopedStores>>,
    qa_host: Option<Arc<dyn QaHost>>,
    events: broadcast::Sender<Value>,
    store_events: Option<JoinHandle<()>>,
}

impl Drop for Services {
    fn drop(&mut self) {
        if let Some(task) = &self.store_events { task.abort(); }
    }
}

/// Services are opt-in. An Agent-only application needs no filesystem roots,
/// settings file, QA reporter or ALwith-specific CLI.
pub struct ServiceExtension {
    providers: providers::ProviderManager,
    services: Option<Arc<Services>>,
    next: Arc<dyn Extension>,
}

struct ServiceConnection {
    next: ConnectionState,
    watchers: tokio::sync::Mutex<FileWatchers>,
    closed: std::sync::atomic::AtomicBool,
    events: Option<JoinHandle<()>>,
}

impl Drop for ServiceConnection {
    fn drop(&mut self) {
        if let Some(task) = self.events.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
enum EventSink {
    Broadcast(broadcast::Sender<Value>),
    Connection { events: mpsc::Sender<String>, disconnect: Arc<Notify> },
}

impl EventSink {
    fn emit(&self, event: Value) -> Result<(), String> {
        match self {
            Self::Broadcast(sender) => {
                // No connected view is valid: services outlive their subscribers.
                match sender.send(event) {
                    Ok(_) | Err(broadcast::error::SendError(_)) => Ok(()),
                }
            }
            Self::Connection { events, disconnect } => {
                let frame = serde_json::json!({ "type": "service", "event": event }).to_string();
                events.try_send(frame).map_err(|error| {
                    // Never leave a consumer connected after losing an event.
                    disconnect.notify_one();
                    format!("service event queue: {error}")
                })
            }
        }
    }

    fn disconnect(&self) {
        match self {
            Self::Connection { disconnect, .. } => disconnect.notify_one(),
            Self::Broadcast(_) => panic!("broadcast services do not own a connection"),
        }
    }
}

impl ServiceExtension {
    pub fn resolve_launch(&self, launch: Value) -> Result<Value, String> {
        self.providers.resolve_launch(launch)
    }

    pub fn from_env(next: Arc<dyn Extension>) -> Result<Self, String> {
        let config = match std::env::var("ALWITH_RUNTIME_SERVICES") {
            Ok(value) => serde_json::from_str(&value).map_err(|error| format!("ALWITH_RUNTIME_SERVICES: {error}"))?,
            Err(std::env::VarError::NotPresent) => ServicesConfig::default(),
            Err(error) => return Err(format!("ALWITH_RUNTIME_SERVICES: {error}")),
        };
        Self::configured(next, config, providers::ProviderManager::from_env()?)
    }

    pub fn configured(next: Arc<dyn Extension>, config: ServicesConfig, providers: providers::ProviderManager) -> Result<Self, String> {
        config.validate()?;
        let mut file_systems = HashMap::new();
        for (name, paths) in [("fs", config.fs), ("git", config.git), ("shell", config.shell), ("qa", config.qa)] {
            if let Some(paths) = paths { file_systems.insert(name, FileSystem::new(paths.roots)?); }
        }
        let stores = config.store.map(|store| Arc::new(ScopedStores::new(store.data_dir, store.cache_dir)));
        let (events, _) = broadcast::channel(256);
        let store_events = stores.as_ref().map(|stores| {
            let event_sink = EventSink::Broadcast(events.clone());
            let (mut data_changes, mut cache_changes) = stores.subscribe();
            tokio::spawn(async move {
            loop {
                let (scope, change) = tokio::select! {
                    change = data_changes.recv() => ("data", change),
                    change = cache_changes.recv() => ("cache", change),
                };
                match change {
                    Ok(change) => {
                        event_sink
                            .emit(serde_json::json!({
                                "type": "storeChanged", "scope": scope,
                                "store": change.store, "key": change.key, "value": change.value
                            }))
                            .expect("broadcast service event");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        // Tell views to invalidate rather than silently losing mutations.
                        event_sink
                            .emit(serde_json::json!({
                                "type": "storeInvalidated", "scope": scope, "missed": count
                            }))
                            .expect("broadcast service invalidation");
                    }
                }
            }
        })
        });
        let qa_host = file_systems.contains_key("qa").then(|| Arc::new(EmittingQaHost { client: EventSink::Broadcast(events.clone()) }) as Arc<dyn QaHost>);
        let services = Arc::new(Services { file_systems, stores, qa_host, events, store_events });
        Ok(Self { providers, services: Some(services), next })
    }

}

#[async_trait]
impl Extension for ServiceExtension {
    fn connect(&self, owner_id: &str, events: mpsc::Sender<String>, disconnect: Arc<Notify>) -> ConnectionState {
        let sink = EventSink::Connection { events: events.clone(), disconnect: Arc::clone(&disconnect) };
        let task = self.services.as_ref().map(|services| {
            let mut subscription = services.events.subscribe();
            let sink = sink.clone();
            tokio::spawn(async move {
                loop {
                    match subscription.recv().await {
                        Ok(event) => {
                            if let Err(error) = sink.emit(event) {
                                log::error!("{error}");
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            log::error!("service subscriber missed {count} events; disconnecting");
                            sink.disconnect();
                            break;
                        }
                    }
                }
            })
        });
        Box::new(ServiceConnection {
            next: self.next.connect(owner_id, events, disconnect),
            watchers: tokio::sync::Mutex::new(FileWatchers::new(sink)),
            closed: std::sync::atomic::AtomicBool::new(false),
            events: task,
        })
    }

    async fn call(
        &self, owner_id: &str, connection: &ConnectionState, method: &str, params: Value,
    ) -> Result<Option<Value>, ResponseError> {
        let connection = connection.downcast_ref::<ServiceConnection>().expect("service connection state");
        if connection.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ResponseError::connection_lost("service connection closed"));
        }
        if method == "engines/capabilities" { return Ok(Some(self.providers.capabilities())); }
        if method == "engines/probeCapabilities" {
            return self.providers.probe_capabilities(params).await.map(Some).map_err(ResponseError::runtime);
        }
        if method == "services/ping" { return Ok(Some(Value::String("pong".into()))); }
        if method == "services/capabilities" {
            let mut capabilities: Vec<&str> = self.services.as_ref().map(|services| services.file_systems.keys().copied().collect()).unwrap_or_default();
            if self.services.as_ref().is_some_and(|services| services.stores.is_some()) { capabilities.push("store"); }
            capabilities.sort_unstable();
            return Ok(Some(serde_json::json!({"schemaVersion":1,"enabled":capabilities})));
        }
        if method == "engines/status" { return self.providers.status().await.map(Some).map_err(ResponseError::runtime); }
        let namespace = method.split_once('/').map(|(namespace, _)| namespace);
        if !matches!(namespace, Some("fs" | "store" | "qa" | "git" | "shell" | "engines" | "services")) {
            return self.next.call(owner_id, &connection.next, method, params).await;
        }
        let services = self.services.as_ref().ok_or_else(|| ResponseError::runtime(
            "Runtime resource services are disabled; configure ALWITH_RUNTIME_SERVICES"
        ))?;
        let namespace = namespace.expect("known service namespace");
        let enabled = match namespace { "store" => services.stores.is_some(), other => services.file_systems.contains_key(other) };
        if !enabled { return Err(ResponseError::runtime(format!("Runtime capability {namespace} is disabled"))); }
        let command = serde_json::from_value(serde_json::json!({ "method": method, "params": params }))
            .map_err(ResponseError::invalid_params)?;
        services.execute(command, connection, namespace).await.map(Some).map_err(ResponseError::runtime)
    }

    async fn disconnect(&self, connection: &ConnectionState) {
        let state = connection.downcast_ref::<ServiceConnection>().expect("service connection state");
        state.closed.store(true, std::sync::atomic::Ordering::Release);
        if let Some(task) = &state.events {
            task.abort();
        }
        let watchers = std::mem::take(&mut state.watchers.lock().await.watchers);
        tokio::task::spawn_blocking(move || drop(watchers)).await.expect("release filesystem watchers");
        self.next.disconnect(&state.next).await;
    }

    async fn shutdown(&self) {
        self.next.shutdown().await;
        if self.services.as_ref().is_some_and(|services| services.qa_host.is_some()) {
            alwith_qa::kill_all();
        }
    }
}

fn resolve_reporter_path() -> Result<PathBuf, String> {
    let path =
        std::env::var_os("ALWITH_QA_REPORTER_PATH").map(PathBuf::from).ok_or("QA requires ALWITH_QA_REPORTER_PATH")?;
    if !path.is_file() {
        return Err(format!("QA reporter does not exist: {}", path.display()));
    }
    Ok(path)
}

struct EmittingQaHost {
    client: EventSink,
}

#[async_trait]
impl QaHost for EmittingQaHost {
    fn emit(&self, event: QaStream) -> Result<(), String> {
        let mut payload = serde_json::to_value(&event).map_err(|error| error.to_string())?;
        payload.as_object_mut().expect("QaStream object").insert("type".into(), Value::String("qaStream".into()));
        self.client.emit(payload)
    }

    fn reporter_path(&self) -> Result<PathBuf, String> {
        resolve_reporter_path()
    }

    async fn access_token(&self, _endpoint: &str) -> Result<String, String> {
        Err("QA requires an ALwith credential provider; Runtime identity is not configured".into())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FileChangedEvent {
    #[serde(rename = "type")]
    event_type: &'static str,
    watcher_id: String,
    paths: Vec<String>,
}

struct FileWatchers {
    watchers: HashMap<String, RecommendedWatcher>,
    client: EventSink,
}

impl FileWatchers {
    fn new(client: EventSink) -> Self {
        Self { watchers: HashMap::new(), client }
    }

    async fn add(&mut self, watcher_id: String, path: PathBuf, recursive: bool) -> Result<(), String> {
        if self.watchers.contains_key(&watcher_id) {
            return Err(format!("filesystem watcher already exists: {watcher_id}"));
        }
        let client = self.client.clone();
        let event_watcher_id = watcher_id.clone();
        let watcher = tokio::task::spawn_blocking(move || {
            let mut watcher = RecommendedWatcher::new(
                move |result: notify::Result<notify::Event>| {
                    let event = match result {
                        Ok(event) => FileChangedEvent {
                            event_type: "fsChanged",
                            watcher_id: event_watcher_id.clone(),
                            paths: event.paths.into_iter().map(|path| path.to_string_lossy().into_owned()).collect(),
                        },
                        Err(error) => {
                            log::error!("filesystem watcher failed: {error}");
                            client.disconnect();
                            return;
                        }
                    };
                    if let Err(error) = client.emit(serde_json::to_value(&event).expect("serialize filesystem event")) {
                        log::error!("filesystem event delivery failed: {error}");
                    }
                },
                Config::default(),
            )
            .map_err(|error| error.to_string())?;
            let mode = if recursive { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };
            watcher.watch(&path, mode).map_err(|error| error.to_string())?;
            Ok::<_, String>(watcher)
        })
        .await
        .map_err(|error| error.to_string())??;
        self.watchers.insert(watcher_id, watcher);
        Ok(())
    }

    async fn remove(&mut self, watcher_id: &str) -> Result<(), String> {
        let watcher = self
            .watchers
            .remove(watcher_id)
            .ok_or_else(|| format!("filesystem watcher does not exist: {watcher_id}"))?;
        tokio::task::spawn_blocking(move || drop(watcher)).await.map_err(|error| error.to_string())
    }
}

impl Drop for FileWatchers {
    fn drop(&mut self) {
        let watchers = std::mem::take(&mut self.watchers);
        tokio::task::spawn_blocking(move || drop(watchers));
    }
}

#[cfg(test)]
#[path = "__tests__/services.rs"]
mod tests;

impl Services {
    fn file_system(&self, capability: &str) -> &FileSystem {
        self.file_systems.get(capability).expect("capability was validated before dispatch")
    }

    async fn execute(&self, command: ServiceCommand, connection: &ServiceConnection, capability: &str) -> Result<Value, String> {
        let state = self;
        match command {
            ServiceCommand::FsReadText { path } => Ok(Value::String(state.file_system(capability).read_text(path.into()).await?)),
            ServiceCommand::FsReadBase64 { path } => {
                Ok(Value::String(state.file_system(capability).read_base64(path.into()).await?))
            }
            ServiceCommand::FsWriteText { path, content } => {
                state.file_system(capability).write_text(path.into(), content).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsWriteBase64 { path, content } => {
                state.file_system(capability).write_base64(path.into(), content).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsAppendText { path, content } => {
                state.file_system(capability).append_text(path.into(), content).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsExists { path } => Ok(Value::Bool(state.file_system(capability).exists(path.into()).await?)),
            ServiceCommand::FsReadDir { path } => {
                serde_json::to_value(state.file_system(capability).read_dir(path.into()).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::FsReadDirFiltered { path, show_hidden, show_gitignored } => serde_json::to_value(
                state.file_system(capability).read_dir_filtered(path.into(), show_hidden, show_gitignored).await?,
            )
            .map_err(|error| error.to_string()),
            ServiceCommand::FsStat { path } => {
                serde_json::to_value(state.file_system(capability).stat(path.into()).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::FsCreateDir { path } => {
                state.file_system(capability).create_dir(path.into()).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsRemove { path } => {
                state.file_system(capability).remove(path.into()).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsRename { from, to } => {
                state.file_system(capability).rename(from.into(), to.into()).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsCopy { from, to } => {
                state.file_system(capability).copy(from.into(), to.into()).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsWatch { watcher_id, path, recursive } => {
                let path = state.file_system(capability).authorize_watch_path(PathBuf::from(path).as_path())?;
                let mut watchers = connection.watchers.lock().await;
                if connection.closed.load(std::sync::atomic::Ordering::Acquire) {
                    return Err("service connection closed".into());
                }
                watchers.add(watcher_id, path, recursive).await?;
                Ok(Value::Null)
            }
            ServiceCommand::FsUnwatch { watcher_id } => {
                connection.watchers.lock().await.remove(&watcher_id).await?;
                Ok(Value::Null)
            }
            ServiceCommand::StoreEntries { scope, store } => {
                Ok(Value::Object(state.stores.as_ref().expect("store capability validated").open(scope, &store)?.entries().into_iter().collect()))
            }
            ServiceCommand::StoreSet { scope, store, key, value } => {
                state.stores.as_ref().expect("store capability validated").open(scope, &store)?.set(key, value)?;
                Ok(Value::Null)
            }
            ServiceCommand::StoreDelete { scope, store, key } => {
                state.stores.as_ref().expect("store capability validated").open(scope, &store)?.remove(&key)?;
                Ok(Value::Null)
            }
            ServiceCommand::StoreClear { scope, store } => {
                state.stores.as_ref().expect("store capability validated").open(scope, &store)?.clear()?;
                Ok(Value::Null)
            }
            ServiceCommand::StoreReload { scope, store } => {
                state.stores.as_ref().expect("store capability validated").open(scope, &store)?.reload()?;
                Ok(Value::Null)
            }
            ServiceCommand::ExecBash { command, cwd } => {
                state.file_system(capability).authorize_watch_path(Path::new(&cwd))?;
                serde_json::to_value(alwith_shell::exec_bash(command, cwd).await?).map_err(|error| error.to_string())
            }
            // —— QA 双面:项目路径全部先过 capability roots 校验(同 AlwithDevStart 模式),
            // 校验通过后把调用方原样字符串交给 alwith_qa(与桌面 invoke 同一契约)——
            ServiceCommand::QaProjects { root } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_projects(root).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::QaListEnvironments { project } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                serde_json::to_value(alwith_qa::qa_list_environments(project).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::QaSaveEnvironment { project, environment } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                alwith_qa::qa_save_environment(project, environment).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaDeleteEnvironment { project, environment_id } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                alwith_qa::qa_delete_environment(project, environment_id).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaScaffoldProject { root, name, spec } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::qa_scaffold_project(root, name, spec).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaInspectDependencies { project } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                serde_json::to_value(alwith_qa::qa_inspect_dependencies(project).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaInstallDependencies { project, dependency_ids } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                alwith_qa::qa_install_dependencies(project, dependency_ids).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaListDevices { platform } => {
                serde_json::to_value(alwith_qa::devices::list(platform).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::QaListCases { project } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                serde_json::to_value(alwith_qa::qa_list_cases(project).await?).map_err(|error| error.to_string())
            }
            ServiceCommand::QaRunStream { root, project_name, run_id, request } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                alwith_qa::qa_run_stream(Arc::clone(state.qa_host.as_ref().expect("QA capability validated")), root, project_name, run_id, request).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaCancel { run_id } => Ok(Value::Bool(alwith_qa::qa_cancel(run_id).await?)),
            ServiceCommand::QaProjectFiles { root, project, sub } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_project_files(root, project, sub).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaProjectFileRead { root, project, name } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                Ok(Value::String(alwith_qa::native::qa_project_file_read(root, project, name).await?))
            }
            ServiceCommand::QaProjectFileWrite { root, project, name, text } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                alwith_qa::native::qa_project_file_write(root, project, name, text).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaProjectRelativePath { project, path } => {
                state.file_system(capability).authorize_watch_path(Path::new(&project))?;
                Ok(Value::String(alwith_qa::native::qa_project_relative_path(project, path).await?))
            }
            ServiceCommand::QaTestSource { root, project, file } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                Ok(Value::String(alwith_qa::native::qa_test_source(root, project, file).await?))
            }
            ServiceCommand::QaTestDelete { root, project, file } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                alwith_qa::native::qa_test_delete(root, project, file).await?;
                Ok(Value::Null)
            }
            ServiceCommand::QaTestDataFiles { root, project, file } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_test_data_files(root, project, file).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaTestDataFile { root, project, file, name } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                Ok(Value::String(alwith_qa::native::qa_test_data_file(root, project, file, name).await?))
            }
            ServiceCommand::QaTestResults { root, project } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_test_results(root, project).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaCaseStatuses { root, project, environment_id } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_case_statuses(root, project, environment_id).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaRunHistory { root, project } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                serde_json::to_value(alwith_qa::native::qa_run_history(root, project).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::QaResultFileBase64 { root, project, path } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                Ok(Value::String(alwith_qa::native::qa_result_file_base64(root, project, path).await?))
            }
            // —— 路径选择 / 文件树只读 ——
            ServiceCommand::FsCapabilityRoots => Ok(Value::Array(
                state
                    .file_system(capability)
                    .roots()
                    .into_iter()
                    .map(|root| Value::String(root.to_string_lossy().into_owned()))
                    .collect(),
            )),
            ServiceCommand::FsListDirectories { path } => {
                serde_json::to_value(state.file_system(capability).list_directories(path.into()).await?)
                    .map_err(|error| error.to_string())
            }
            ServiceCommand::FsListEntries { path } => {
                serde_json::to_value(state.file_system(capability).list_entries(path.into()).await?)
                    .map_err(|error| error.to_string())
            }
            // —— Git 只读(gix):路径同样必须在 capability roots 内 ——
            ServiceCommand::GitRecentCommits { cwd, limit } => {
                state.file_system(capability).authorize_watch_path(Path::new(&cwd))?;
                let commits = tokio::task::spawn_blocking(move || alwith_git::recent_commits(&cwd, limit as usize))
                    .await
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(commits).map_err(|error| error.to_string())
            }
            ServiceCommand::GitChangedFiles { cwd, from_revision, to_revision } => {
                state.file_system(capability).authorize_watch_path(Path::new(&cwd))?;
                let paths =
                    tokio::task::spawn_blocking(move || alwith_git::changed_files(&cwd, &from_revision, &to_revision))
                        .await
                        .map_err(|error| error.to_string())??;
                serde_json::to_value(paths).map_err(|error| error.to_string())
            }
            ServiceCommand::GitCurrentBranch { path } => {
                state.file_system(capability).authorize_watch_path(Path::new(&path))?;
                let branch = tokio::task::spawn_blocking(move || alwith_git::current_branch(&path))
                    .await
                    .map_err(|error| error.to_string())??;
                serde_json::to_value(branch).map_err(|error| error.to_string())
            }
            ServiceCommand::GitFileStatuses { root } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                let statuses = tokio::task::spawn_blocking(move || alwith_git::file_statuses(&root))
                    .await
                    .map_err(|error| error.to_string())??;
                serde_json::to_value(statuses).map_err(|error| error.to_string())
            }
            ServiceCommand::GitSyncStatus { root } => {
                state.file_system(capability).authorize_watch_path(Path::new(&root))?;
                let status = tokio::task::spawn_blocking(move || alwith_git::sync_status(&root))
                    .await
                    .map_err(|error| error.to_string())??;
                serde_json::to_value(status).map_err(|error| error.to_string())
            }

        }
    }
}
