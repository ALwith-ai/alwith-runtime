use alwith_runtime_protocol::{AgentMetadata, ResponseError, RuntimeEvent};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{oneshot, watch};

use crate::attention::{self, AttentionStore, ProcessLease};
use crate::claims::{AbortOutcome, ClaimError, ClaimKey, Claims};
use crate::process::AgentProcess;
use crate::{AttachInfo, Journal, RuntimeRegistry, SessionEventFrame, SessionRunState, is_lifecycle_line};
use crate::{
    ClaimOutcome, ClaimPhase, ClaimRetirement, ClaimStatus, ClaimTarget, DeletionOutcome, DeletionStage,
    SessionDeletion,
};
use crate::{MAX_MESSAGE_BYTES, protocol_lines};

/// Default journal directory: under the system cache directory, so "Settings → Cache → Clear" reclaims it in one go.
pub fn default_journal_root() -> std::path::PathBuf {
    dirs::cache_dir().expect("platform cache directory").join("alwith").join("Runtime").join("sessions")
}

/// Where the attention state and the process leases live: data, not cache -- an unread `done` or
/// an interrupted turn is a fact the user has not seen yet, and clearing a cache must not erase it.
pub fn default_attention_root() -> std::path::PathBuf {
    dirs::data_dir().expect("platform data directory").join("alwith").join("Runtime")
}

/// Maximum frames one `session_events` call returns; clients page by `sequence`.
pub const SESSION_EVENTS_PAGE: usize = 2_000;

const OUTPUT_BATCH_WINDOW: Duration = Duration::from_millis(16);
const PORT_SCAN_INTERVAL: Duration = Duration::from_secs(2);

/// Private module tool traffic never enters the public event bus or replay journal.
pub type PrivateToolDispatch = Arc<
    dyn Fn(String, String, Value) -> Pin<Box<dyn Future<Output = Result<Value, ResponseError>> + Send>> + Send + Sync,
>;
const TOOL_CALL_METHOD: &str = "_alwith/tool/call";

// Credentials go to the engine, never to observers or replay diagnostics.
fn public_request_line(value: &Value) -> String {
    if matches!(value.get("method").and_then(Value::as_str), Some("providers/set" | "auth/login")) {
        let mut public = value.clone();
        public["params"] = serde_json::json!({"redacted": true});
        public.to_string()
    } else {
        value.to_string()
    }
}

// Keep credential values only for the lifetime of their engine. Providers can rotate keys;
// old values remain sensitive while late errors from an earlier request may still arrive.
#[derive(Default)]
struct PrivateValues(HashSet<String>);
impl PrivateValues {
    fn command(command: &Command) -> Self {
        let mut values = Self::default();
        for (key, value) in command.as_std().get_envs() {
            let key = key.to_string_lossy().to_ascii_uppercase();
            if (key.ends_with("_API_KEY")
                || key.ends_with("_AUTH_TOKEN")
                || key.ends_with("_ACCESS_TOKEN")
                || key.ends_with("_SECRET"))
                && let Some(value) = value
            {
                values.remember(&Value::String(value.to_string_lossy().into_owned()), true);
            }
        }
        values
    }
    fn remember(&mut self, value: &Value, private: bool) {
        match value {
            Value::String(value) if private && !value.is_empty() => {
                self.0.insert(value.clone());
                if let Some(token) = value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))
                    && !token.is_empty()
                {
                    self.0.insert(token.into());
                }
            }
            Value::Object(fields) => {
                for (key, value) in fields {
                    let key = key.to_ascii_lowercase().replace(['_', '-'], "");
                    self.remember(
                        value,
                        private
                            || matches!(
                                key.as_str(),
                                "apikey"
                                    | "bearertoken"
                                    | "accesstoken"
                                    | "refreshtoken"
                                    | "password"
                                    | "authorization"
                                    | "headers"
                                    | "token"
                                    | "secret"
                            ),
                    );
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.remember(value, private);
                }
            }
            _ => {}
        }
    }
    fn text(&self, text: &str) -> String {
        let mut values: Vec<_> = self.0.iter().collect();
        values.sort_by_key(|value| std::cmp::Reverse(value.len()));
        values.into_iter().fold(text.to_string(), |text, secret| text.replace(secret.as_str(), "[REDACTED]"))
    }
    fn json(&self, value: &mut Value) {
        match value {
            Value::String(text) => *text = self.text(text),
            Value::Object(values) => {
                for value in values.values_mut() {
                    self.json(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.json(value);
                }
            }
            _ => {}
        }
    }
    fn line(&self, line: &str) -> String {
        if self.0.is_empty() {
            return line.to_string();
        }
        match serde_json::from_str::<Value>(line) {
            Ok(mut value) => {
                self.json(&mut value);
                value.to_string()
            }
            Err(_) => self.text(line),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSnapshot {
    pub agent_id: String,
    pub pid: Option<u32>,
    pub owner_id: String,
    pub provider: String,
    pub model: String,
    pub connection: String,
}

pub trait RuntimeEventSink: Send + Sync {
    fn publish(&self, event: RuntimeEvent);
}

struct ProviderUpdate {
    provider: String,
    revision: Option<u64>,
}

/// A request the Runtime sent to the agent that is still unanswered. `owner_id` is the holder it was sent for: ""
/// when the Runtime itself asked, and for a prompt once its holder let go, because the turn belongs to the session.
struct PendingAcp {
    sender: oneshot::Sender<Result<Value, ResponseError>>,
    owner_id: String,
    method: String,
    id: Value,
}

struct AgentHandle {
    tool_owner: Option<String>,
    private_tool_ids: HashSet<String>,
    private_values: PrivateValues,
    stdin: Arc<AsyncMutex<ChildStdin>>,
    child: AgentProcess,
    metadata: AgentMetadata,
    /// This process as distinct from any other under the same agent id (see `AttachInfo`).
    instance_id: String,
    failure: Option<String>,
    initialize_request_id: Option<Value>,
    initialize_response: Option<Value>,
    /// ACP requests the Runtime sent (its own carry ids like `rt-<n>`), waiting for the agent's response.
    pending_acp: HashMap<String, PendingAcp>,
    /// This process as the attention store knows it (`None` once it was gone before it could be read).
    lease: Option<ProcessLease>,
    /// Ids retracted with `$/cancel_request` after their holder let go: the agent still answers them, and the answer is dropped here.
    retracted_acp: HashSet<String>,
    // Route IDs (such as "main") are not provider identities. Commit the supplied
    // identity only when its matching providers/set request succeeds.
    pending_providers: HashMap<String, Option<ProviderUpdate>>,
    provider_changes: watch::Sender<()>,
    next_acp_id: u64,
}

/// Prefix of ACP request ids the Runtime sends; apps use numeric ids, so the two never collide.
const ACP_REQUEST_ID_PREFIX: &str = "rt-";
/// Protocol version the Runtime reports when it handshakes on an app's behalf.
pub const ACP_PROTOCOL_VERSION: u64 = 2;

/// One agent frame together with its position in the session journal (present only once journaled).
struct AcceptedLine {
    line: String,
    stamp: Option<(String, u64)>,
}

/// Where this frame landed in a batch of journal actions: a line is journaled at most once, so the first match suffices.
fn journal_stamp(stamps: Vec<(String, u64)>) -> Option<(String, u64)> {
    stamps.into_iter().next()
}

pub struct Runtime {
    tool_dispatch: OnceLock<PrivateToolDispatch>,
    registry: Mutex<RuntimeRegistry>,
    journal: Journal,
    attention: AttentionStore,
    claims: Mutex<Claims>,
    /// Signalled whenever an agent process has been reaped (`delete_begin` waits on it).
    agent_exited: tokio::sync::Notify,
    /// Names this Runtime process; claim tokens and agent instance ids carry it, so nothing minted
    /// by an earlier Runtime can be mistaken for ours.
    runtime_instance: String,
    next_agent_instance: AtomicU64,
    agents: Mutex<HashMap<String, AgentHandle>>,
    // Creation across generations, sends, receives and exit of one agent are serialized; different agents never wait on each other.
    lanes: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    sinks: Mutex<BTreeMap<u64, Weak<dyn RuntimeEventSink>>>,
    next_sink_id: AtomicU64,
    slow_mode_delay_ms: AtomicU64,
    port_scanner_started: AtomicBool,
}

impl Default for Runtime {
    fn default() -> Self {
        let root = default_journal_root();
        let attention_root = default_attention_root();
        Self::with_storage(
            Journal::open(&root).unwrap_or_else(|error| panic!("open Runtime journal at {}: {error}", root.display())),
            AttentionStore::open(&attention_root).unwrap_or_else(|error| {
                panic!("open Runtime attention store at {}: {error}", attention_root.display())
            }),
        )
    }
}

impl Runtime {
    /// Open on the given storage and settle what the previous Runtime left there (`attention`):
    /// its agent processes are reclaimed and its sessions come back, a running one as interrupted.
    pub fn with_storage(journal: Journal, attention: AttentionStore) -> Self {
        let runtime_instance = {
            use std::hash::{BuildHasher, RandomState};
            format!("{:x}", RandomState::new().hash_one((now_ms(), std::process::id())))
        };
        let runtime = Self {
            tool_dispatch: OnceLock::new(),
            registry: Mutex::default(),
            journal,
            attention,
            claims: Mutex::new(Claims::new(runtime_instance.clone())),
            agent_exited: tokio::sync::Notify::new(),
            runtime_instance,
            next_agent_instance: AtomicU64::default(),
            agents: Mutex::default(),
            lanes: Mutex::default(),
            sinks: Mutex::default(),
            next_sink_id: AtomicU64::default(),
            slow_mode_delay_ms: AtomicU64::default(),
            port_scanner_started: AtomicBool::default(),
        };
        runtime.recover();
        runtime
    }

    fn recover(&self) {
        let store = self.attention.root().display();
        let leases =
            self.attention.read_leases().unwrap_or_else(|error| panic!("Runtime attention store {store}: {error}"));
        for lease in attention::reclaim_orphans(&leases) {
            log::warn!(
                "reclaimed agent {} (pid {}) left running by the previous Runtime",
                lease.agent_id,
                lease.process_id
            );
        }
        self.attention.write_leases(&[]).unwrap_or_else(|error| panic!("Runtime attention store {store}: {error}"));
        let deletions =
            self.attention.read_deletions().unwrap_or_else(|error| panic!("Runtime attention store {store}: {error}"));
        if !deletions.is_empty() {
            self.claims.lock().expect("runtime claims lock poisoned").restore_deletions(deletions);
            self.persist_deletions();
        }
        let persisted =
            self.attention.read_run_states().unwrap_or_else(|error| panic!("Runtime attention store {store}: {error}"));
        if persisted.is_empty() {
            return;
        }
        self.registry.lock().expect("runtime registry lock poisoned").restore_run_states(persisted, now_ms());
        self.publish_run_states();
    }

    fn persist_deletions(&self) {
        let deletions = self.claims.lock().expect("runtime claims lock poisoned").deletions();
        if let Err(error) = self.attention.write_deletions(&deletions) {
            log::error!("persist session deletions: {error}");
        }
    }

    /// The leases of every agent process currently held, written whenever one starts or is reaped.
    fn persist_leases(&self) {
        let leases = self
            .agents
            .lock()
            .expect("runtime agents lock poisoned")
            .values()
            .filter_map(|handle| handle.lease.clone())
            .collect::<Vec<_>>();
        if let Err(error) = self.attention.write_leases(&leases) {
            log::error!("persist agent leases: {error}");
        }
    }

    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    pub fn set_tool_dispatch(&self, dispatch: PrivateToolDispatch) {
        let _ = self.tool_dispatch.set(dispatch);
    }

    /// Persist the journal actions the registry just decided and return each persisted frame's `(sessionId, sequence)`
    /// (in Append order), stamped on the frame when the live event goes out. A journal write failure must not take
    /// down the ACP channel, but it must be reported.
    fn commit_journal(&self) -> Vec<(String, u64)> {
        let ops = self.registry.lock().expect("runtime registry lock poisoned").take_journal_ops();
        match self.journal.apply(&ops, now_ms()) {
            Ok(frames) => {
                let appended = ops.iter().filter_map(|op| match op {
                    crate::JournalOp::Append { session_id, .. } => Some(session_id.clone()),
                    crate::JournalOp::Reset { .. } => None,
                });
                appended.zip(frames).map(|(session_id, frame)| (session_id, frame.sequence)).collect()
            }
            Err(error) => {
                log::error!("Runtime journal: {error}");
                self.publish(RuntimeEvent::Stderr {
                    agent_id: String::new(),
                    line: format!("Runtime journal: {error}"),
                });
                Vec::new()
            }
        }
    }

    fn lane(&self, agent_id: &str) -> Arc<AsyncMutex<()>> {
        let mut lanes = self.lanes.lock().expect("Runtime lanes lock poisoned");
        if let Some(lane) = lanes.get(agent_id).and_then(Weak::upgrade) {
            return lane;
        }
        lanes.retain(|_, lane| lane.strong_count() > 0);
        let lane = Arc::new(AsyncMutex::new(()));
        lanes.insert(agent_id.to_string(), Arc::downgrade(&lane));
        lane
    }

    fn fail_agent(&self, agent_id: &str, error: String) {
        let message = {
            let mut agents = self.agents.lock().expect("runtime agents lock poisoned");
            let Some(handle) = agents.get_mut(agent_id) else { return };
            if handle.failure.is_some() {
                return;
            }
            handle.failure = Some(error.clone());
            match handle.child.start_kill() {
                Ok(()) => error,
                Err(kill_error) => format!("{error}; stop agent failed: {kill_error}"),
            }
        };
        let message = self.public_agent_line(agent_id, &message);
        // Keep the stdout task alive to drain the pipe and deliver Exit normally; no new requests run.
        self.publish(RuntimeEvent::Stderr { agent_id: agent_id.to_string(), line: message.clone() });
        self.publish_agent_lines(
            agent_id,
            vec![AcceptedLine {
                line: serde_json::json!({
                    "jsonrpc": "2.0", "method": "_alwith/runtimeError", "params": { "message": message }
                })
                .to_string(),
                stamp: None,
            }],
        );
    }
    pub fn add_sink(&self, sink: &Arc<dyn RuntimeEventSink>) -> u64 {
        let sink_id = self.next_sink_id.fetch_add(1, Ordering::Relaxed);
        self.sinks.lock().expect("Runtime sink lock poisoned").insert(sink_id, Arc::downgrade(sink));
        sink_id
    }

    pub fn remove_sink(&self, sink_id: u64) {
        self.sinks.lock().expect("Runtime sink lock poisoned").remove(&sink_id);
    }

    pub fn set_slow_mode(&self, delay_ms: u64) {
        self.slow_mode_delay_ms.store(delay_ms, Ordering::Relaxed);
    }

    /// Start an agent process. A live process with the same name is a caller bug (switching engines = stop, wait for
    /// exit, then start), so this errors out.
    pub async fn start(
        self: &Arc<Self>, agent_id: String, metadata: AgentMetadata, command: &mut Command,
    ) -> Result<(), String> {
        let lane = self.lane(&agent_id);
        let _operation = lane.lock().await;
        // Refuse outright while a process with the same name is alive. Switching engines is "stop, wait until it has
        // really exited, then start"; there is no implicit replacement, so "two processes for one agentId" cannot exist
        // and late output has no ownership ambiguity. The handle stays in the table until teardown (pump reads EOF →
        // reap → broadcast Exit), so this also covers the window where the process is dead but its last lines are
        // still draining.
        if self.agents.lock().map_err(|error| error.to_string())?.contains_key(&agent_id) {
            return Err(format!(
                "agent {agent_id} is still running; stop it and wait for its exit before starting again"
            ));
        }
        let removed_sessions = self
            .registry
            .lock()
            .map_err(|error| error.to_string())?
            .begin_agent(agent_id.clone(), metadata.owner_id.clone());
        if !removed_sessions.is_empty() {
            self.publish_run_states();
        }

        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        let mut child = AgentProcess::spawn(command).map_err(|error| format!("spawn ACP agent: {error}"))?;
        let lease = child.id().and_then(|process_id| attention::lease_for(&agent_id, process_id));
        let instance_id = format!(
            "{agent_id}#{}.{}",
            self.runtime_instance,
            self.next_agent_instance.fetch_add(1, Ordering::AcqRel) + 1
        );
        let stdin = child.inner().stdin.take().ok_or("ACP agent has no stdin")?;
        let stdout = child.inner().stdout.take().ok_or("ACP agent has no stdout")?;
        let stderr = child.inner().stderr.take().ok_or("ACP agent has no stderr")?;
        self.agents.lock().map_err(|error| error.to_string())?.insert(
            agent_id.clone(),
            AgentHandle {
                private_tool_ids: HashSet::new(),
                private_values: PrivateValues::command(command),
                tool_owner: metadata
                    .owner_id
                    .strip_prefix("module:")
                    .map(|owner| owner.split(':').next().unwrap_or(owner).to_string()),
                stdin: Arc::new(AsyncMutex::new(stdin)),
                child,
                metadata,
                instance_id,
                failure: None,
                lease,
                initialize_request_id: None,
                initialize_response: None,
                pending_acp: HashMap::new(),
                retracted_acp: HashSet::new(),
                pending_providers: HashMap::new(),
                provider_changes: watch::channel(()).0,
                next_acp_id: 1,
            },
        );

        let runtime = Arc::clone(self);
        let stdout_agent_id = agent_id.clone();
        tokio::spawn(async move {
            runtime.pump_stdout(stdout_agent_id, stdout).await;
        });

        self.start_port_scanner();

        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            let lane = runtime.lane(&agent_id);
            while let Ok(Some(line)) = lines.next_line().await {
                let _operation = lane.lock().await;
                runtime.publish(RuntimeEvent::Stderr {
                    agent_id: agent_id.clone(),
                    line: runtime.public_agent_line(&agent_id, &line),
                });
            }
        });

        Ok(())
    }

    pub async fn send(self: &Arc<Self>, agent_id: &str, line: String) -> Result<(), String> {
        if line.len() > MAX_MESSAGE_BYTES {
            return Err(format!("ACP message exceeds {MAX_MESSAGE_BYTES} bytes"));
        }
        let value: Value = serde_json::from_str(&line).map_err(|error| error.to_string())?;
        let provider_request = if value.get("method").and_then(Value::as_str) == Some("providers/set") {
            Some(
                value
                    .get("id")
                    .filter(|id| id.is_string() || id.is_number())
                    .ok_or("provider selection requires a request id")?
                    .to_string(),
            )
        } else {
            None
        };
        {
            let mut agents = self.agents.lock().map_err(|error| error.to_string())?;
            let handle = agents.get_mut(agent_id).ok_or_else(|| format!("no agent process {agent_id}"))?;
            if let Some(id) = &provider_request {
                if handle.pending_providers.contains_key(id) {
                    return Err("provider request id is already pending".into());
                }
                // Both raw send and ACP requests reserve before their first await.
                handle.pending_providers.insert(id.clone(), None);
            }
        }
        let runtime = Arc::clone(self);
        let agent_id = agent_id.to_string();
        // Delivery and failure cleanup survive cancellation of the requesting connection.
        tokio::spawn(async move {
            let result = runtime.send_inner(&agent_id, value).await;
            if result.is_err()
                && let Some(id) = provider_request
            {
                let mut agents = runtime.agents.lock().map_err(|error| error.to_string())?;
                if let Some(handle) = agents.get_mut(&agent_id) {
                    handle.pending_providers.remove(&id);
                    handle.provider_changes.send_replace(());
                }
            }
            result
        })
        .await
        .map_err(|error| format!("Runtime send task failed: {error}"))?
    }

    async fn send_inner(&self, agent_id: &str, value: Value) -> Result<(), String> {
        self.slow_mode_wait().await;
        let lane = self.lane(agent_id);
        let _operation = lane.lock().await;
        let stdin = {
            let agents = self.agents.lock().map_err(|error| error.to_string())?;
            let handle = agents.get(agent_id).ok_or_else(|| format!("no agent process {agent_id}"))?;
            if let Some(error) = &handle.failure {
                return Err(error.clone());
            }
            Arc::clone(&handle.stdin)
        };
        if value.get("method").is_none()
            && let Some(id) = value.get("id")
        {
            let agents = self.agents.lock().map_err(|error| error.to_string())?;
            if agents.get(agent_id).expect("agent under lane").private_tool_ids.contains(&id.to_string()) {
                return Err("module-owned tool replies cannot be supplied by application connections".into());
            }
        }
        // ACP v2 handshakes once per connection; when a new owner (reload, attach) sends `initialize` again the Runtime
        // replays the original response from cache instead of bothering the engine.
        let method = value.get("method").and_then(Value::as_str);
        let cached_initialize = method == Some("initialize")
            && self
                .agents
                .lock()
                .map_err(|error| error.to_string())?
                .get(agent_id)
                .expect("agent under lane")
                .initialize_response
                .is_some();
        if cached_initialize {
            let request_id = value.get("id").ok_or("initialize requires request id")?;
            let response = self
                .agents
                .lock()
                .map_err(|error| error.to_string())?
                .get(agent_id)
                .expect("agent under lane")
                .initialize_response
                .clone()
                .ok_or("agent has no successful initialize response")?;
            self.publish_agent_lines(
                agent_id,
                vec![AcceptedLine {
                    line: serde_json::json!({
                        "jsonrpc": "2.0", "id": request_id, "result": response
                    })
                    .to_string(),
                    stamp: None,
                }],
            );
            return Ok(());
        }
        if value.get("method").and_then(Value::as_str) == Some("session/resume") {
            // Both engines use the same session/resume: the engine reads the ALwith record itself. The Runtime only reserves
            // the record identity before the request goes out, so another engine or process cannot restore the same record
            // at the same time; once the engine answers, the registry confirms or releases the reservation.
            let session_id =
                value.pointer("/params/sessionId").and_then(Value::as_str).ok_or("resume missing sessionId")?;
            value.get("id").ok_or("resume requires request id")?;
            if session_id.is_empty() || session_id.contains(['/', '\\']) {
                return Err("invalid recordId".into());
            }
            self.registry.lock().map_err(|error| error.to_string())?.reserve_session(agent_id, session_id)?;
        }
        value.as_object().ok_or("ACP envelope must be an object")?;
        if matches!(method, Some("providers/set" | "auth/login")) {
            self.agents
                .lock()
                .map_err(|error| error.to_string())?
                .get_mut(agent_id)
                .expect("agent under lane")
                .private_values
                .remember(&value["params"], false);
        }
        let provider_change = if method == Some("providers/set") {
            let revision = match value.pointer("/params/_meta/alwith/providerRevision") {
                None | Some(Value::Null) => None,
                Some(revision) => Some(revision.as_u64().ok_or("providerRevision must be a non-negative integer")?),
            };
            let id = value
                .get("id")
                .filter(|id| id.is_string() || id.is_number())
                .ok_or("provider selection requires a request id")?
                .to_string();
            let update = match value.pointer("/params/_meta/alwith/providerId") {
                Some(Value::String(provider)) if !provider.is_empty() => {
                    Some(ProviderUpdate { provider: provider.clone(), revision })
                }
                Some(_) => return Err("provider selection requires a non-empty providerId".into()),
                None => None,
            };
            Some((id, update))
        } else {
            None
        };
        let wire_line = value.to_string();
        let write_result = async {
            let mut writer = stdin.lock().await;
            writer.write_all(wire_line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        }
        .await;
        if let Err(error) = write_result {
            let message = format!("send to agent {agent_id} failed: {error}");
            self.fail_agent(agent_id, message.clone());
            return Err(message);
        }
        if let Some((id, provider)) = provider_change {
            self.agents
                .lock()
                .map_err(|error| error.to_string())?
                .get_mut(agent_id)
                .expect("agent under lane")
                .pending_providers
                .insert(id, provider);
        }
        if value.get("method").and_then(Value::as_str) == Some("initialize") {
            self.agents
                .lock()
                .map_err(|error| error.to_string())?
                .get_mut(agent_id)
                .expect("agent under lane")
                .initialize_request_id = value.get("id").cloned();
        }
        let public_wire = self.public_agent_line(agent_id, &public_request_line(&value));
        let line = public_wire.clone();
        self.publish(RuntimeEvent::Wire { agent_id: agent_id.to_string(), outbound: true, lines: vec![public_wire] });
        let changed =
            self.registry.lock().map_err(|error| error.to_string())?.record_outbound(agent_id, &line, now_ms());
        let stamp = journal_stamp(self.commit_journal());
        if changed {
            self.publish_run_states();
        }
        self.publish(RuntimeEvent::Outbound {
            agent_id: agent_id.to_string(),
            line,
            session_id: stamp.as_ref().map(|(session_id, _)| session_id.clone()),
            sequence: stamp.map(|(_, sequence)| sequence),
        });
        Ok(())
    }

    pub fn stop(&self, agent_id: &str) -> Result<(), String> {
        if let Some(handle) = self.agents.lock().map_err(|error| error.to_string())?.get_mut(agent_id) {
            handle.child.start_kill().map_err(|error| format!("stop agent {agent_id}: {error}"))?;
        }
        Ok(())
    }

    pub fn stop_session(&self, session_id: &str) -> Result<(), String> {
        if let Some(agent_id) = self.agent_id_for_session(session_id) {
            self.stop(&agent_id)?;
        }
        Ok(())
    }

    pub fn kill_all(&self) {
        self.agents.lock().expect("runtime agents lock poisoned").clear();
    }

    pub fn agent_snapshots(&self) -> Vec<AgentSnapshot> {
        self.agents
            .lock()
            .expect("runtime agents lock poisoned")
            .iter()
            .map(|(agent_id, handle)| AgentSnapshot {
                agent_id: agent_id.clone(),
                pid: handle.child.id(),
                owner_id: handle.metadata.owner_id.clone(),
                provider: handle.metadata.provider.clone(),
                model: handle.metadata.model.clone(),
                connection: handle.metadata.connection.clone(),
            })
            .collect()
    }

    /// When the owning connection drops, owner becomes empty = orphan: any connection may stop it (reclaim) or claim it through attach
    pub fn agent_orphaned(&self, agent_id: &str) -> bool {
        self.agents
            .lock()
            .expect("runtime agents lock poisoned")
            .get(agent_id)
            .is_some_and(|handle| handle.metadata.owner_id.is_empty())
    }

    pub fn owns_agent(&self, agent_id: &str, owner_id: &str) -> bool {
        self.agents
            .lock()
            .expect("runtime agents lock poisoned")
            .get(agent_id)
            .is_some_and(|handle| handle.metadata.owner_id == owner_id)
    }

    pub fn agent_id_for_session(&self, session_id: &str) -> Option<String> {
        self.registry.lock().expect("runtime registry lock poisoned").agent_id_for_session(session_id)
    }

    /// The agent's current owner (empty string = unowned); None when there is no such agent.
    pub fn agent_owner(&self, agent_id: &str) -> Option<String> {
        self.agents
            .lock()
            .expect("runtime agents lock poisoned")
            .get(agent_id)
            .map(|handle| handle.metadata.owner_id.clone())
    }

    /// A connection dropped: it (`root`) and every sub-owner under it (`root:<label>`) become unowned together.
    pub async fn detach_owners_under(self: &Arc<Self>, root: &str) -> bool {
        let prefix = format!("{root}:");
        let mut owners: std::collections::BTreeSet<String> = self
            .agents
            .lock()
            .expect("runtime agents lock poisoned")
            .values()
            .map(|handle| handle.metadata.owner_id.clone())
            .filter(|owner| owner.starts_with(&prefix))
            .collect();
        owners.extend(
            self.run_states().into_iter().map(|state| state.owner_id).filter(|owner| owner.starts_with(&prefix)),
        );
        let mut changed = self.detach_owner(root).await;
        for owner in owners {
            changed |= self.detach_owner(&owner).await;
        }
        changed
    }

    pub fn run_states(&self) -> Vec<SessionRunState> {
        self.registry.lock().expect("runtime registry lock poisoned").run_states()
    }

    /// Every raw frame of a session (for mobile attach replay), with answered permission requests removed.
    pub fn session_replay(&self, session_id: &str) -> Vec<String> {
        self.session_events(session_id, None, usize::MAX).into_iter().map(|frame| frame.line).collect()
    }

    /// Lifecycle frames (prompt / permission requests / tool state transitions), without AI prose; still queryable after the agent ends.
    pub fn session_lifecycle(&self, session_id: &str) -> Vec<crate::SessionLifecycleFrame> {
        self.session_events(session_id, None, usize::MAX)
            .into_iter()
            .filter(|frame| serde_json::from_str::<Value>(&frame.line).is_ok_and(|value| is_lifecycle_line(&value)))
            .map(|frame| crate::SessionLifecycleFrame {
                sequence: frame.sequence,
                recorded_at: frame.recorded_at,
                line: frame.line,
            })
            .collect()
    }

    /// Session frames after `after_sequence`, at most `limit`; late joiners catch up with it.
    /// Answered `session/request_permission` frames are left out, since replaying one would get it answered twice.
    pub fn session_events(
        &self, session_id: &str, after_sequence: Option<u64>, limit: usize,
    ) -> Vec<SessionEventFrame> {
        let pending = self.registry.lock().expect("runtime registry lock poisoned").pending_permission_ids(session_id);
        let frames = match self.journal.read_since(session_id, after_sequence, limit) {
            Ok(frames) => frames,
            Err(error) => {
                log::error!("Runtime journal: {error}");
                return Vec::new();
            }
        };
        filter_answered_permissions(frames, &pending)
    }

    pub fn mark_read(&self, session_id: &str, version: Option<u64>) -> bool {
        let changed = self.registry.lock().expect("runtime registry lock poisoned").mark_read(session_id, version);
        if changed {
            self.publish_run_states();
        }
        changed
    }

    /// Explicitly release an execution lease (see `RuntimeRegistry::release_execution`); broadcasts when anything changed.
    pub fn release_execution(&self, execution_id: &str) -> bool {
        let changed = self.registry.lock().expect("runtime registry lock poisoned").release_execution(execution_id);
        if changed {
            self.publish_run_states();
        }
        changed
    }

    pub fn set_title(&self, session_id: &str, title: String) -> bool {
        let changed = self.registry.lock().expect("runtime registry lock poisoned").set_title(session_id, title);
        if changed {
            self.publish_run_states();
        }
        changed
    }

    /// A holder let go (its connection closed, or it released explicitly). What it was still waiting on and nobody
    /// took over, nobody will collect: the agent is told with `$/cancel_request` (it MAY stop the work and MUST
    /// still answer; the answer is dropped here) and the waiting caller fails at once. A `session/prompt` is the
    /// session's turn, not the holder's request: it goes on for the next holder and is only ever ended by
    /// `session/cancel`.
    pub async fn detach_owner(self: &Arc<Self>, owner_id: &str) -> bool {
        let retracted = {
            let mut agents = self.agents.lock().expect("runtime agents lock poisoned");
            let mut retracted = Vec::new();
            for (agent_id, handle) in agents.iter_mut() {
                if handle.metadata.owner_id == owner_id {
                    handle.metadata.owner_id.clear();
                }
                let abandoned: Vec<String> = handle
                    .pending_acp
                    .iter()
                    .filter(|(_, pending)| pending.owner_id == owner_id && pending.method != "session/prompt")
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in abandoned {
                    let pending = handle.pending_acp.remove(&id).expect("abandoned request was just listed");
                    let _ = pending.sender.send(Err(ResponseError::runtime(format!(
                        "{} retracted: its holder {owner_id} let go",
                        pending.method
                    ))));
                    handle.retracted_acp.insert(id);
                    retracted.push((agent_id.clone(), pending.id));
                }
                for pending in handle.pending_acp.values_mut().filter(|pending| pending.owner_id == owner_id) {
                    pending.owner_id.clear();
                }
            }
            retracted
        };
        for (agent_id, id) in retracted {
            let frame =
                serde_json::json!({ "jsonrpc": "2.0", "method": "$/cancel_request", "params": { "requestId": id } });
            if let Err(error) = self.send(&agent_id, frame.to_string()).await {
                log::error!("Runtime: could not retract request {id} on agent {agent_id}: {error}");
            }
        }
        let changed = self.registry.lock().expect("runtime registry lock poisoned").detach_owner(owner_id);
        self.claims.lock().expect("runtime claims lock poisoned").owner_gone(owner_id);
        if changed {
            self.publish_run_states();
        }
        changed
    }

    /// The process is gone: the claims it carried failed, deletions waiting on it may unlink, and
    /// whoever waits for an exit is woken.
    fn settle_exited_agent(&self, agent_id: &str) {
        let mut persist = false;
        {
            let mut claims = self.claims.lock().expect("runtime claims lock poisoned");
            claims.agent_exited(agent_id);
            let stopping: Vec<String> = claims
                .deletions()
                .into_iter()
                .filter(|deletion| deletion.stage == DeletionStage::Stopping)
                .map(|deletion| deletion.session_id)
                .collect();
            for session_id in stopping {
                if self.agent_id_for_session(&session_id).is_none() {
                    claims.delete_stage(&session_id, DeletionStage::Unlinking);
                    persist = true;
                }
            }
        }
        if persist {
            self.persist_deletions();
        }
        self.agent_exited.notify_waiters();
    }

    // ---- Claims (docs/plans/2026-10-19-session-lifecycle.md 1.1-1.4) ----

    /// Take the session for `(caller_instance_id, attempt_id)`. A live process running it is
    /// attached on the spot (taking it over from any other holder); otherwise the claim is
    /// `reserved` and turns active when the claimant's agent resumes the session.
    pub async fn claim(
        &self, session_id: &str, attempt_id: &str, input_digest: &str, caller_instance_id: &str, owner_id: String,
    ) -> Result<ClaimOutcome, ResponseError> {
        let key = ClaimKey { caller_instance_id: caller_instance_id.to_string(), attempt_id: attempt_id.to_string() };
        {
            let mut claims = self.claims.lock().expect("runtime claims lock poisoned");
            if let Some(deletion) = claims.deletion(session_id)
                && claims.status(&key).is_none()
            {
                return Err(claim_error(ClaimError {
                    code: "claim/deleting",
                    message: format!("session {session_id} is being deleted ({:?})", deletion.stage),
                }));
            }
            if claims.status(&key).is_some() {
                // Idempotent: the same attempt gets its recorded answer; the state machine checks
                // the digest, no process work is involved.
                let claim = claims.claim(key, &owner_id, session_id, input_digest, None).map_err(claim_error)?;
                return Ok(ClaimOutcome { claim, attach: None });
            }
        }
        let attach = self.attach(session_id, owner_id.clone()).await;
        let live = attach.as_ref().map(|info| (info.agent_id.clone(), info.agent_instance_id.clone()));
        let claim = self
            .claims
            .lock()
            .expect("runtime claims lock poisoned")
            .claim(key, &owner_id, session_id, input_digest, live)
            .map_err(claim_error)?;
        Ok(ClaimOutcome { claim, attach })
    }

    /// Withdraw an attempt, before or after its claim arrived. A claim this attempt alone held is
    /// released (the process stays, headless); one taken over by a later claimant is reported as
    /// `claim/transferred` and left alone.
    pub fn claim_abort(
        &self, caller_instance_id: &str, attempt_id: &str, owner_id: &str,
    ) -> Result<ClaimStatus, ResponseError> {
        let key = ClaimKey { caller_instance_id: caller_instance_id.to_string(), attempt_id: attempt_id.to_string() };
        let outcome = self.claims.lock().expect("runtime claims lock poisoned").abort(key);
        match outcome {
            AbortOutcome::Aborted(status) | AbortOutcome::Terminal(status) => Ok(status),
            AbortOutcome::ReleaseAndAbort { session_id, status } => {
                self.release_session(&session_id, owner_id);
                Ok(status)
            }
            AbortOutcome::Transferred(status) => {
                let mut error = ResponseError::runtime("claim/transferred");
                error.data = Some(serde_json::to_value(status).expect("claim status serializes"));
                Err(error)
            }
        }
    }

    pub fn claim_status(&self, caller_instance_id: &str, attempt_id: &str) -> Option<ClaimStatus> {
        let key = ClaimKey { caller_instance_id: caller_instance_id.to_string(), attempt_id: attempt_id.to_string() };
        self.claims.lock().expect("runtime claims lock poisoned").status(&key)
    }

    /// The holder lets go of a live claim: `released` keeps the process (headless), `stopped`
    /// ends it. Checked against the token and the holder.
    pub fn release_claim(
        &self, claim_token: &str, owner_id: &str, target: ClaimTarget,
    ) -> Result<ClaimStatus, ResponseError> {
        let how = match target {
            ClaimTarget::Released => ClaimRetirement::Released,
            ClaimTarget::Stopped => ClaimRetirement::Stopped,
        };
        let status = self
            .claims
            .lock()
            .expect("runtime claims lock poisoned")
            .retire(claim_token, owner_id, how)
            .map_err(claim_error)?;
        match target {
            ClaimTarget::Released => {
                self.release_session(&status.session_id, owner_id);
            }
            ClaimTarget::Stopped => self.stop_session(&status.session_id).map_err(ResponseError::runtime)?,
        }
        Ok(status)
    }

    // ---- Deletion (1.4) ----

    pub fn deleting(&self, session_id: &str) -> Option<SessionDeletion> {
        self.claims.lock().expect("runtime claims lock poisoned").deletion(session_id).cloned()
    }

    pub fn deletions(&self) -> Vec<SessionDeletion> {
        self.claims.lock().expect("runtime claims lock poisoned").deletions()
    }

    /// Begin deleting a session: its claims retire, new ones are refused, the process running it
    /// is stopped and waited for. Returns at `unlinking`: no process runs the session, the record
    /// may be unlinked (`delete_end` reports how that went). Intent and stage are persisted first,
    /// so a crash in between resumes here.
    pub async fn delete_begin(&self, session_id: &str) -> Result<SessionDeletion, ResponseError> {
        let deletion = self.claims.lock().expect("runtime claims lock poisoned").delete_begin(session_id, now_ms());
        self.persist_deletions();
        if deletion.stage != DeletionStage::Stopping {
            return Ok(deletion);
        }
        loop {
            let exited = self.agent_exited.notified();
            tokio::pin!(exited);
            exited.as_mut().enable();
            let Some(agent_id) = self.agent_id_for_session(session_id) else { break };
            self.stop(&agent_id).map_err(ResponseError::runtime)?;
            exited.await;
        }
        let deletion = self
            .claims
            .lock()
            .expect("runtime claims lock poisoned")
            .delete_stage(session_id, DeletionStage::Unlinking)
            .expect("deletion was just begun");
        self.persist_deletions();
        Ok(deletion)
    }

    /// The deleter reports the unlink. `deleted` forgets the session (attention entry included);
    /// a failure keeps the deletion with the state the deleter found.
    pub fn delete_end(
        &self, session_id: &str, outcome: DeletionOutcome,
    ) -> Result<Option<SessionDeletion>, ResponseError> {
        let deletion = self
            .claims
            .lock()
            .expect("runtime claims lock poisoned")
            .delete_end(session_id, outcome)
            .map_err(claim_error)?;
        self.persist_deletions();
        if deletion.is_none() {
            let forgotten = self.registry.lock().expect("runtime registry lock poisoned").forget_session(session_id);
            if forgotten {
                self.publish_run_states();
            }
        }
        Ok(deletion)
    }

    /// The user gives the deletion up: the session may be claimed again.
    pub fn delete_abandon(&self, session_id: &str) -> bool {
        let abandoned = self.claims.lock().expect("runtime claims lock poisoned").delete_abandon(session_id);
        if abandoned {
            self.persist_deletions();
        }
        abandoned
    }

    pub async fn attach(&self, session_id: &str, owner_id: String) -> Option<AttachInfo> {
        loop {
            let agent_id =
                self.registry.lock().expect("runtime registry lock poisoned").agent_id_for_session(session_id)?;
            let lane = self.lane(&agent_id);
            let operation = lane.lock().await;
            let state = {
                let mut registry = self.registry.lock().expect("runtime registry lock poisoned");
                // The holder may have changed while waiting; locate the matching agent lane again.
                if registry.agent_id_for_session(session_id).as_deref() != Some(agent_id.as_str()) {
                    continue;
                }
                let mut agents = self.agents.lock().expect("runtime agents lock poisoned");
                let handle = agents.get_mut(&agent_id)?;
                if !handle.pending_providers.is_empty() {
                    Err(handle.provider_changes.subscribe())
                } else {
                    let mut info = registry.attach(session_id, owner_id.clone())?;
                    // A takeover: what the previous holder was still waiting on now belongs to the new one, so the
                    // previous holder letting go later retracts none of it.
                    let previous = std::mem::replace(&mut handle.metadata.owner_id, owner_id.clone());
                    if !previous.is_empty() && previous != owner_id {
                        for pending in handle.pending_acp.values_mut().filter(|pending| pending.owner_id == previous) {
                            pending.owner_id = owner_id.clone();
                        }
                    }
                    info.agent_instance_id = handle.instance_id.clone();
                    info.engine = handle.metadata.engine.clone();
                    info.provider = handle.metadata.provider.clone();
                    info.provider_revision = handle.metadata.provider_revision;
                    Ok(info)
                }
            };
            let info = match state {
                Ok(info) => info,
                Err(mut pending) => {
                    // Responses need the lane; subscription and the decision to wait were atomic with the snapshot.
                    drop(operation);
                    if pending.changed().await.is_err() {
                        return None;
                    }
                    continue;
                }
            };
            self.publish_run_states();
            return Some(info);
        }
    }

    pub fn release_session(&self, session_id: &str, owner_id: &str) -> bool {
        let mut registry = self.registry.lock().expect("runtime registry lock poisoned");
        let Some(agent_id) = registry.agent_id_for_session(session_id) else { return false };
        if !registry.release_session(session_id, owner_id) {
            return false;
        }
        if let Some(handle) = self.agents.lock().expect("runtime agents lock poisoned").get_mut(&agent_id) {
            handle.metadata.owner_id.clear();
        }
        drop(registry);
        self.publish_run_states();
        true
    }

    /// The agent lane must be held. Session identity is the sessionId the engine reports (both engines name ALwith
    /// records by it); the Runtime does not translate it. A frame returning `None` is the response to a request the
    /// Runtime itself sent: it is journaled but not put into `inbound` (the app never sent it). Journaled frames come
    /// back with `(sessionId, sequence)`, stamped on the live event so clients can detect missed frames.
    fn public_agent_line(&self, agent_id: &str, line: &str) -> String {
        self.agents
            .lock()
            .expect("runtime agents lock poisoned")
            .get(agent_id)
            .map_or_else(|| line.to_string(), |handle| handle.private_values.line(line))
    }

    fn accept_line(&self, agent_id: &str, line: String) -> Result<Option<AcceptedLine>, String> {
        let value = serde_json::from_str::<Value>(&line).ok();
        let mut answered_runtime_request = false;
        {
            let mut agents = self.agents.lock().map_err(|error| error.to_string())?;
            let handle = agents.get_mut(agent_id).ok_or("agent stopped while receiving")?;
            // Update before resolving the caller: an immediate attach must see the
            // acknowledged provider, even if the original requester has disconnected.
            if let Some(response) = value.as_ref().filter(|value| value.get("method").is_none())
                && let Some(id) = response.get("id")
                && let Some(update) = handle.pending_providers.remove(&id.to_string())
            {
                if response.get("error").is_none_or(Value::is_null) && response.get("result").is_some() {
                    handle.metadata.provider_revision = update.as_ref().and_then(|update| update.revision);
                    if let Some(update) = update {
                        handle.metadata.provider = update.provider;
                    }
                }
                handle.provider_changes.send_replace(());
            }
            if let Some(response) = value.as_ref().filter(|value| value.get("method").is_none())
                && let Some(id) = response.get("id").and_then(Value::as_str)
            {
                if let Some(pending) = handle.pending_acp.remove(id) {
                    let outcome = match response.get("error") {
                        Some(error) if !error.is_null() => Err(ResponseError::parse(error)),
                        _ => Ok(response.get("result").cloned().unwrap_or(Value::Null)),
                    };
                    let _ = pending.sender.send(outcome);
                    answered_runtime_request = true;
                } else if handle.retracted_acp.remove(id) {
                    // The answer the protocol owes a retracted request: nobody is waiting for it.
                    answered_runtime_request = true;
                }
            }
            // ACP v2 handshakes once per connection; later windows attaching read this original response.
            if let Some(response) = value.as_ref().filter(|value| value.get("method").is_none())
                && handle.initialize_request_id.is_some()
                && response.get("id") == handle.initialize_request_id.as_ref()
            {
                handle.initialize_request_id = None;
                if response.get("error").is_none() {
                    handle.initialize_response =
                        Some(response.get("result").ok_or("initialize response missing result")?.clone());
                }
            }
        }
        let line = self.public_agent_line(agent_id, &line);
        self.registry.lock().map_err(|error| error.to_string())?.record_inbound_batch(
            agent_id,
            std::slice::from_ref(&line),
            now_ms(),
        );
        let stamp = journal_stamp(self.commit_journal());
        self.activate_claims(agent_id);
        Ok(if answered_runtime_request { None } else { Some(AcceptedLine { line, stamp }) })
    }

    /// A session the holder reserved a claim for has appeared on the holder's agent: the claim is
    /// active from now on. Runs after every inbound frame; cheap when nothing is reserved.
    fn activate_claims(&self, agent_id: &str) {
        let Some((owner_id, sessions)) =
            self.registry.lock().expect("runtime registry lock poisoned").agent_sessions(agent_id)
        else {
            return;
        };
        if owner_id.is_empty() {
            return;
        }
        let Some(instance_id) = self
            .agents
            .lock()
            .expect("runtime agents lock poisoned")
            .get(agent_id)
            .map(|handle| handle.instance_id.clone())
        else {
            return;
        };
        let mut claims = self.claims.lock().expect("runtime claims lock poisoned");
        for session_id in sessions {
            claims.activate(&owner_id, &session_id, agent_id, &instance_id);
        }
    }

    async fn pump_stdout(self: Arc<Self>, agent_id: String, stdout: tokio::process::ChildStdout) {
        let mut reader = protocol_lines(stdout);
        let lane = self.lane(&agent_id);
        let mut tool_tasks = tokio::task::JoinSet::new();
        let mut tool_calls = HashMap::<String, tokio::task::AbortHandle>::new();
        loop {
            let next = tokio::select! {
                completed = tool_tasks.join_next(), if !tool_tasks.is_empty() => {
                    if let Some(Ok(id)) = completed { tool_calls.remove(&id); }
                    continue;
                }
                next = reader.next_line() => next,
            };
            let first = match next {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(error) => {
                    let _operation = lane.lock().await;
                    self.fail_agent(&agent_id, format!("read agent stdout: {error}"));
                    break;
                }
            };
            let mut batch_bytes = first.len();
            let mut batch = vec![first];
            let deadline = tokio::time::Instant::now() + OUTPUT_BATCH_WINDOW;
            let mut reached_end = false;
            loop {
                match tokio::time::timeout_at(deadline, reader.next_line()).await {
                    Ok(Ok(Some(line))) => {
                        batch_bytes += line.len();
                        batch.push(line);
                        if batch_bytes >= MAX_MESSAGE_BYTES {
                            break;
                        }
                    }
                    Ok(Ok(None)) => {
                        reached_end = true;
                        break;
                    }
                    Ok(Err(error)) => {
                        let _operation = lane.lock().await;
                        self.fail_agent(&agent_id, format!("read agent stdout: {error}"));
                        reached_end = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            self.slow_mode_wait().await;
            let _operation = lane.lock().await;
            if !self.agents.lock().expect("runtime agents lock poisoned").contains_key(&agent_id) {
                break;
            }
            let mut public_batch = Vec::new();
            for line in batch {
                let value = serde_json::from_str::<Value>(&line).ok();
                if let Some(frame) = value.as_ref().filter(|v| v["method"] == TOOL_CALL_METHOD) {
                    let Some(id) = frame.get("id").filter(|id| id.is_string() || id.is_number()).cloned() else {
                        continue;
                    };
                    let key = id.to_string();
                    // ACP request IDs cannot be reused while pending. Do not repeat a side effect.
                    if tool_calls.contains_key(&key) {
                        continue;
                    }
                    let owner = {
                        let agents = self.agents.lock().expect("runtime agents lock poisoned");
                        agents.get(&agent_id).expect("agent under lane").tool_owner.clone()
                    };
                    // Tools declared by an application connection (an agent started by a page, not by a
                    // module) run in that application: the call travels to the client as an ordinary agent
                    // request and the client answers it, exactly like a permission request.
                    let Some(owner) = owner else {
                        public_batch.push(line);
                        continue;
                    };
                    let stdin = {
                        let mut agents = self.agents.lock().expect("runtime agents lock poisoned");
                        let handle = agents.get_mut(&agent_id).expect("agent under lane");
                        handle.private_tool_ids.insert(key.clone());
                        Arc::clone(&handle.stdin)
                    };
                    let dispatch = self.tool_dispatch.get().cloned();
                    let params = frame.get("params").cloned().unwrap_or(Value::Null);
                    let caller = agent_id.clone();
                    let task_key = key.clone();
                    let abort = tool_tasks.spawn(async move {
                        let result = match dispatch {
                            Some(dispatch) => {
                                tokio::time::timeout(Duration::from_secs(1800), dispatch(owner, caller, params))
                                    .await
                                    .unwrap_or_else(|_| Err(ResponseError::runtime("client tool timed out")))
                            }
                            None => Err(ResponseError::method_not_found(TOOL_CALL_METHOD)),
                        };
                        let response = match result {
                            Ok(result) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}),
                            Err(error) => serde_json::json!({"jsonrpc":"2.0","id":id,"error":error}),
                        };
                        // Captured pipe belongs to this process, never a replacement with the same agent ID.
                        let mut input = stdin.lock().await;
                        let _ = input.write_all(format!("{response}\n").as_bytes()).await;
                        let _ = input.flush().await;
                        task_key
                    });
                    tool_calls.insert(key, abort);
                    continue;
                }
                if let Some(frame) = value.as_ref().filter(|v| v["method"] == "$/cancel_request")
                    && let Some(id) = frame.pointer("/params/requestId")
                    && let Some(task) = tool_calls.remove(&id.to_string())
                {
                    task.abort();
                    continue;
                }
                public_batch.push(line);
            }
            self.publish(RuntimeEvent::Wire {
                agent_id: agent_id.clone(),
                outbound: false,
                lines: public_batch.iter().map(|line| self.public_agent_line(&agent_id, line)).collect(),
            });
            let active = self
                .agents
                .lock()
                .expect("runtime agents lock poisoned")
                .get(&agent_id)
                .is_some_and(|handle| handle.failure.is_none());
            if active {
                let mut accepted = Vec::new();
                for line in public_batch {
                    match self.accept_line(&agent_id, line) {
                        Ok(Some(line)) => accepted.push(line),
                        Ok(None) => {}
                        Err(error) => {
                            self.fail_agent(&agent_id, error);
                            break;
                        }
                    }
                }
                if !accepted.is_empty() {
                    self.publish_agent_lines(&agent_id, accepted);
                }
                self.publish_run_states();
            }
            if reached_end {
                break;
            }
        }
        tool_tasks.abort_all();
        while tool_tasks.join_next().await.is_some() {}
        let _operation = lane.lock().await;
        // The old process was already torn down by the replacement; its Exit must not reach the new process's subscribers.
        let changed = self.registry.lock().expect("runtime registry lock poisoned").finish_agent(&agent_id);
        let mut handle = self.agents.lock().expect("runtime agents lock poisoned").remove(&agent_id);
        self.persist_leases();
        self.settle_exited_agent(&agent_id);
        if let Some(handle) = &mut handle {
            for (_, pending) in handle.pending_acp.drain() {
                let _ = pending.sender.send(Err(ResponseError::runtime(format!("agent {agent_id} exited"))));
            }
            // A closed stdout ends the ACP channel; a still-running process must not hold the agent lane forever.
            if let Err(error) = handle.child.stop_and_wait().await {
                self.publish(RuntimeEvent::Stderr {
                    agent_id: agent_id.clone(),
                    line: format!("stop and wait agent exit: {error}"),
                });
            }
        }
        if changed {
            self.publish_run_states();
        }
        self.publish(RuntimeEvent::Exit { agent_id });
    }

    fn publish_agent_lines(&self, agent_id: &str, accepted: Vec<AcceptedLine>) {
        for AcceptedLine { line, stamp } in &accepted {
            let Ok(value) = serde_json::from_str::<Value>(line) else { continue };
            let Some(method) = value.get("method").and_then(Value::as_str) else { continue };
            let params = value.get("params").cloned().unwrap_or(Value::Null);
            let session_id = stamp.as_ref().map(|(session_id, _)| session_id.clone());
            let sequence = stamp.as_ref().map(|(_, sequence)| *sequence);
            match value.get("id") {
                Some(id) if !id.is_null() => self.publish(RuntimeEvent::AcpAgentRequest {
                    agent_id: agent_id.to_string(),
                    id: id.clone(),
                    method: method.to_string(),
                    params,
                    session_id,
                    sequence,
                }),
                _ => self.publish(RuntimeEvent::AcpNotification {
                    agent_id: agent_id.to_string(),
                    method: method.to_string(),
                    params,
                    session_id,
                    sequence,
                }),
            }
        }
        let lines = accepted.into_iter().map(|accepted| accepted.line).collect();
        self.publish(RuntimeEvent::Inbound { agent_id: agent_id.to_string(), lines });
    }

    /// Send an ACP request to the agent on the app's behalf and wait for the response. It takes the same path as
    /// `send`: envelope, journal and run state as usual. Errors come in two kinds: the Runtime's own (no such agent,
    /// the agent exited midway, `ResponseError::RUNTIME`) and ACP errors from the agent (code / data as is).
    /// `request_id`: a JSON-RPC id (string or number) chosen by the app, needed only to withdraw it with
    /// `$/cancel_request`; when omitted the Runtime assigns one (`rt-<n>`). Ids still pending on the same agent
    /// cannot be reused. `owner_id` is the holder the request is sent for ("" for the Runtime itself): a takeover
    /// transfers what it is still waiting on to the new holder, letting go retracts it (see `detach_owner`).
    pub async fn acp_request(
        self: &Arc<Self>, agent_id: &str, method: &str, params: Value, request_id: Option<Value>, owner_id: &str,
    ) -> Result<Value, ResponseError> {
        let (id, receiver) = {
            let mut agents = self.agents.lock().map_err(|error| error.to_string())?;
            let handle = agents.get_mut(agent_id).ok_or_else(|| format!("no agent process {agent_id}"))?;
            if method == "initialize"
                && let Some(cached) = &handle.initialize_response
            {
                return Ok(cached.clone());
            }
            let (id, id_value) = match request_id {
                Some(Value::String(id)) => (id.clone(), Value::String(id)),
                Some(Value::Number(id)) => (id.to_string(), Value::Number(id)),
                Some(other) => {
                    return Err(ResponseError::invalid_params(format!(
                        "request id must be a string or number, got {other}"
                    )));
                }
                None => {
                    let id = format!("{ACP_REQUEST_ID_PREFIX}{}", handle.next_acp_id);
                    handle.next_acp_id += 1;
                    (id.clone(), Value::String(id))
                }
            };
            if handle.pending_acp.contains_key(&id) {
                return Err(ResponseError::invalid_params(format!(
                    "request id {id} is still pending on agent {agent_id}"
                )));
            }
            let (sender, receiver) = oneshot::channel();
            handle.pending_acp.insert(
                id.clone(),
                PendingAcp { sender, owner_id: owner_id.to_string(), method: method.to_string(), id: id_value },
            );
            (id, receiver)
        };
        let frame = serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if let Err(error) = self.send(agent_id, frame.to_string()).await {
            if let Ok(mut agents) = self.agents.lock()
                && let Some(handle) = agents.get_mut(agent_id)
            {
                handle.pending_acp.remove(&id);
            }
            return Err(error.into());
        }
        receiver.await.unwrap_or_else(|_| {
            Err(ResponseError::runtime(format!("agent {agent_id} exited before answering {method}")))
        })
    }

    /// Send an ACP notification to the agent (`session/cancel` and the like).
    pub async fn acp_notify(
        self: &Arc<Self>, agent_id: &str, method: &str, params: Value,
    ) -> Result<(), ResponseError> {
        let frame = serde_json::json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.send(agent_id, frame.to_string()).await.map_err(ResponseError::runtime)
    }

    /// Answer a request from the agent (the `id` in the `acpAgentRequest` event).
    pub async fn acp_respond(
        self: &Arc<Self>, agent_id: &str, id: Value, result: Option<Value>, error: Option<Value>,
    ) -> Result<(), ResponseError> {
        let frame = match error {
            Some(error) => serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": error }),
            None => serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result.unwrap_or(Value::Null) }),
        };
        self.send(agent_id, frame.to_string()).await.map_err(ResponseError::runtime)
    }

    /// Handshake once; if it already happened (the app sent `initialize` itself, or the Runtime did on its behalf)
    /// the cached result is used. An app that wants to declare its client capabilities (such as elicitation) calls
    /// `initialize` before its first session operation.
    pub async fn ensure_initialized(
        self: &Arc<Self>, agent_id: &str, client_version: &str, info: Option<Value>, capabilities: Option<Value>,
    ) -> Result<Value, ResponseError> {
        self.acp_request(
            agent_id,
            "initialize",
            serde_json::json!({
                "protocolVersion": ACP_PROTOCOL_VERSION,
                "info": info.unwrap_or_else(|| serde_json::json!({ "name": "alwith-runtime", "version": client_version })),
                "capabilities": capabilities.unwrap_or_else(|| serde_json::json!({}))
            }),
            None,
            "",
        )
        .await
    }

    /// Every change goes out and to disk from here: the registry stamps versions on commit, the
    /// snapshot is persisted, then broadcast. A persistence failure is logged, not fatal -- the
    /// session itself lives in the engine's record, this is the attention over it.
    fn publish_run_states(&self) {
        let sessions = self.registry.lock().expect("runtime registry lock poisoned").commit_run_states();
        if let Err(error) = self.attention.write_run_states(&sessions) {
            log::error!("persist run states: {error}");
        }
        self.publish(RuntimeEvent::RunStates { sessions });
    }

    fn start_port_scanner(self: &Arc<Self>) {
        if self.port_scanner_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            loop {
                runtime.scan_agent_ports().await;
                tokio::time::sleep(PORT_SCAN_INTERVAL).await;
            }
        });
    }

    async fn scan_agent_ports(&self) {
        let agents = self
            .agents
            .lock()
            .expect("runtime agents lock poisoned")
            .iter()
            .map(|(agent_id, handle)| (agent_id.clone(), handle.child.id().expect("running ACP agent has process id")))
            .collect::<Vec<_>>();
        if agents.is_empty() {
            return;
        }
        let root_process_ids = agents.iter().map(|(_, process_id)| *process_id).collect::<Vec<_>>();
        let ports_by_root =
            match tokio::task::spawn_blocking(move || crate::port_scanner::listening_ports_by_root(&root_process_ids))
                .await
                .expect("port scanner task")
            {
                Ok(ports_by_root) => ports_by_root,
                Err(error) => {
                    // Reading the system socket table can fail transiently on some platforms; keep the previous port snapshot and retry next round.
                    log::warn!("[Runtime] scan agent listening ports failed: {error}");
                    return;
                }
            };
        // The process may have changed during the scan (stopped and restarted under the same name): match by pid, and
        // do not record ports of a different process.
        let current_process_ids = self
            .agents
            .lock()
            .expect("runtime agents lock poisoned")
            .iter()
            .map(|(agent_id, handle)| (agent_id.clone(), handle.child.id()))
            .collect::<HashMap<_, _>>();
        let mut changed = false;
        let mut registry = self.registry.lock().expect("runtime registry lock poisoned");
        for (agent_id, process_id) in agents {
            if current_process_ids.get(&agent_id) == Some(&Some(process_id)) {
                changed |= registry.set_agent_listening_ports(
                    &agent_id,
                    ports_by_root.get(&process_id).expect("scanned root process").clone(),
                );
            }
        }
        drop(registry);
        if changed {
            self.publish_run_states();
        }
    }

    /// Broadcast an event to every registered sink. The Runtime uses it itself, and hosts may use it to inject events (such as stderr notices).
    pub fn publish(&self, event: RuntimeEvent) {
        let sinks: Vec<Arc<dyn RuntimeEventSink>> = {
            let mut registered = self.sinks.lock().expect("Runtime sink lock poisoned");
            let sinks = registered.values().filter_map(Weak::upgrade).collect();
            registered.retain(|_, sink| sink.strong_count() > 0);
            sinks
        };
        for sink in sinks {
            sink.publish(event.clone());
        }
    }

    async fn slow_mode_wait(&self) {
        let delay_ms = self.slow_mode_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

/// Keep only permission requests that are still unanswered; other frames stay as they are.
fn filter_answered_permissions(frames: Vec<SessionEventFrame>, pending_ids: &[String]) -> Vec<SessionEventFrame> {
    frames
        .into_iter()
        .filter(|frame| {
            let Ok(value) = serde_json::from_str::<Value>(&frame.line) else { return true };
            if value.get("method").and_then(Value::as_str) != Some("session/request_permission") {
                return true;
            }
            value.get("id").map(ToString::to_string).is_some_and(|id| pending_ids.contains(&id))
        })
        .collect()
}

fn claim_error(error: ClaimError) -> ResponseError {
    let mut response = ResponseError::runtime(error.code);
    response.data = Some(serde_json::json!({ "message": error.message }));
    response
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before UNIX epoch").as_millis() as i64
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn launch_credentials_are_redacted_even_before_first_provider_request() {
        let mut command = Command::new("engine");
        command.env("ANTHROPIC_AUTH_TOKEN", "launch-private-key").env("OPENAI_API_KEY", "another-private-key");
        let values = PrivateValues::command(&command);
        let message = values.line(r#"{"error":"launch-private-key and another-private-key"}"#);
        assert!(!message.contains("launch-private-key"));
        assert!(!message.contains("another-private-key"));
        assert!(serde_json::from_str::<Value>(&message).is_ok());
    }

    #[tokio::test]
    async fn tool_callbacks_are_private_and_bound_to_the_spawn_owner() {
        let runtime = Arc::new(test_runtime());
        let callback_started = Arc::new(tokio::sync::Notify::new());
        let release_callback = Arc::new(tokio::sync::Notify::new());
        let started = Arc::clone(&callback_started);
        let release = Arc::clone(&release_callback);
        runtime.set_tool_dispatch(Arc::new(move |owner, agent, params| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                assert_eq!(owner, "board");
                assert_eq!(agent, "tool-agent");
                assert_eq!(params["name"], "private-read");
                Ok(serde_json::json!({"success":true,"contentItems":[{"type":"text","text":"secret-tool-result"}]}))
            })
        }));
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let mut command = Command::new("python3");
        command.args([
            "-c",
            r#"
import json,sys
print(json.dumps({'jsonrpc':'2.0','id':9,'method':'_alwith/tool/call','params':{'name':'private-read'}}),flush=True)
reply=json.loads(sys.stdin.readline())
assert reply['id']==9 and reply['result']['contentItems'][0]['text']=='secret-tool-result'
print(json.dumps({'method':'test/finished','params':{}}),flush=True)
"#,
        ]);
        let mut meta = metadata("codex");
        meta.owner_id = "module:board:chat".into();
        runtime.start("tool-agent".into(), meta, &mut command).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), callback_started.notified()).await.unwrap();
        let spoof = runtime
            .send("tool-agent", r#"{"jsonrpc":"2.0","id":9,"result":{"success":true,"contentItems":[]}}"#.into())
            .await;
        assert!(spoof.unwrap_err().contains("module-owned tool replies"));
        release_callback.notify_one();
        let mut finished = false;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await.unwrap().unwrap();
            let encoded = serde_json::to_string(&event).unwrap();
            assert!(
                !encoded.contains("private-read") && !encoded.contains("secret-tool-result"),
                "private frame escaped: {encoded}"
            );
            if let RuntimeEvent::AcpNotification { method, .. } = &event {
                finished |= method == "test/finished";
            }
            if matches!(event, RuntimeEvent::Exit { .. }) {
                break;
            }
        }
        assert!(finished);
    }

    /// An agent started by an application connection has no module tool owner: its `_alwith/tool/call`
    /// reaches that application as an ordinary agent request, and the application's reply is accepted.
    #[tokio::test]
    async fn tool_calls_of_application_owned_agents_are_forwarded_to_the_client() {
        let runtime = Arc::new(test_runtime());
        runtime.set_tool_dispatch(Arc::new(|_, _, _| {
            Box::pin(async { panic!("module dispatch must not run for an application-owned agent") })
        }));
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let mut command = Command::new("python3");
        command.args([
            "-c",
            r#"
import json,sys
print(json.dumps({'jsonrpc':'2.0','id':9,'method':'_alwith/tool/call','params':{'name':'client-read','arguments':{}}}),flush=True)
reply=json.loads(sys.stdin.readline())
assert reply['id']==9 and reply['result']['contentItems'][0]['text']=='page-tool-result', reply
print(json.dumps({'method':'test/finished','params':{}}),flush=True)
"#,
        ]);
        runtime.start("page-agent".into(), metadata("codex"), &mut command).await.unwrap();
        let mut forwarded = false;
        let mut finished = false;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await.unwrap().unwrap();
            match &event {
                RuntimeEvent::AcpAgentRequest { agent_id, id, method, params, .. } if method == TOOL_CALL_METHOD => {
                    assert_eq!(agent_id, "page-agent");
                    assert_eq!(params["name"], "client-read");
                    forwarded = true;
                    let reply = serde_json::json!({
                        "jsonrpc": "2.0", "id": id,
                        "result": {"success": true, "contentItems": [{"type": "text", "text": "page-tool-result"}]}
                    });
                    runtime.send("page-agent", reply.to_string()).await.unwrap();
                }
                RuntimeEvent::AcpNotification { method, .. } if method == "test/finished" => finished = true,
                RuntimeEvent::Exit { .. } => break,
                _ => {}
            }
        }
        assert!(forwarded, "tool call never reached the client");
        assert!(finished, "agent did not accept the client's tool result");
    }

    #[tokio::test]
    async fn tool_cancellation_and_process_exit_drop_owned_dispatches() {
        for cancel in [true, false] {
            let runtime = Arc::new(test_runtime());
            let dropped = Arc::new(tokio::sync::Notify::new());
            let started = Arc::new(tokio::sync::Notify::new());
            let drop_signal = Arc::clone(&dropped);
            let start_signal = Arc::clone(&started);
            runtime.set_tool_dispatch(Arc::new(move |_, _, _| {
                let dropped = Arc::clone(&drop_signal);
                let started = Arc::clone(&start_signal);
                Box::pin(async move {
                    struct Guard(Arc<tokio::sync::Notify>);
                    impl Drop for Guard {
                        fn drop(&mut self) {
                            self.0.notify_one();
                        }
                    }
                    let _guard = Guard(dropped);
                    started.notify_one();
                    std::future::pending().await
                })
            }));
            let mut command = Command::new("python3");
            command.args([
                "-c",
                if cancel {
                    r#"
import json,sys,time
print(json.dumps({'id':9,'method':'_alwith/tool/call','params':{}}),flush=True)
time.sleep(.1)
print(json.dumps({'method':'$/cancel_request','params':{'requestId':9}}),flush=True)
sys.stdin.readline()
"#
                } else {
                    r#"
import json,time
print(json.dumps({'id':9,'method':'_alwith/tool/call','params':{}}),flush=True)
time.sleep(.1)
"#
                },
            ]);
            let mut meta = metadata("codex");
            meta.owner_id = "module:board".into();
            runtime.start("tool-agent".into(), meta, &mut command).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), started.notified()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), dropped.notified()).await.unwrap();
            if cancel {
                runtime.stop("tool-agent").unwrap();
            }
        }
    }

    #[tokio::test]
    async fn letting_go_retracts_the_holders_requests_but_not_the_sessions_turn() {
        let runtime = Arc::new(test_runtime());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let mut command = Command::new("python3");
        command.args([
            "-c",
            r#"
import json,sys
for line in sys.stdin:
    frame=json.loads(line)
    if frame.get('method')=='$/cancel_request':
        rid=frame['params']['requestId']
        print(json.dumps({'jsonrpc':'2.0','method':'test/cancelled','params':{'requestId':rid}}),flush=True)
        print(json.dumps({'jsonrpc':'2.0','id':rid,'error':{'code':-32800,'message':'Request cancelled'}}),flush=True)
    elif frame.get('method')=='test/finish':
        print(json.dumps({'jsonrpc':'2.0','method':'test/finished','params':{}}),flush=True)
"#,
        ]);
        let mut meta = metadata("codex");
        meta.owner_id = "window:1".into();
        runtime.start("agent".into(), meta, &mut command).await.unwrap();
        let request = |method: &'static str, id: &'static str| {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                runtime
                    .acp_request(
                        "agent",
                        method,
                        serde_json::json!({"sessionId":"s"}),
                        Some(serde_json::json!(id)),
                        "window:1",
                    )
                    .await
            })
        };
        let list = request("session/list", "list-1");
        let prompt = request("session/prompt", "prompt-1");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pending = runtime
                    .agents
                    .lock()
                    .expect("runtime agents lock poisoned")
                    .get("agent")
                    .map_or(0, |handle| handle.pending_acp.len());
                if pending == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both requests reach the agent");

        runtime.detach_owner("window:1").await;

        let error = list.await.unwrap().unwrap_err();
        assert_eq!(error.message, "session/list retracted: its holder window:1 let go");
        assert!(!prompt.is_finished(), "the turn is the session's, not the holder's");

        runtime.acp_notify("agent", "test/finish", serde_json::json!({})).await.unwrap();
        let mut cancelled = Vec::new();
        let mut late_answers = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = receiver.recv().await {
                if let RuntimeEvent::Inbound { lines, .. } = event {
                    let mut finished = false;
                    for line in lines {
                        let frame: Value = serde_json::from_str(&line).unwrap();
                        match frame["method"].as_str() {
                            Some("test/cancelled") => cancelled.push(frame["params"]["requestId"].clone()),
                            Some("test/finished") => finished = true,
                            _ => {
                                if frame.get("id").is_some() && frame.get("method").is_none() {
                                    late_answers.push(frame["id"].clone());
                                }
                            }
                        }
                    }
                    if finished {
                        break;
                    }
                }
            }
        })
        .await
        .expect("the agent reports what it was told");
        assert_eq!(cancelled, vec![serde_json::json!("list-1")], "only the holder's own request is retracted");
        assert!(late_answers.is_empty(), "the answer owed to a retracted request is dropped, not forwarded");
        assert!(!prompt.is_finished());

        runtime.stop("agent").unwrap();
        assert!(prompt.await.unwrap().is_err());
    }

    fn test_runtime() -> Runtime {
        let root = tempfile::tempdir().expect("temp journal dir").keep();
        Runtime::with_storage(
            Journal::open(root.join("sessions")).expect("open test journal"),
            AttentionStore::open(root).expect("open test attention store"),
        )
    }

    /// The attention over a session outlives the Runtime: a session caught running comes back
    /// needing the user with the reason on it, an unread `done` stays unread, and versions carry on.
    #[test]
    fn run_states_come_back_after_a_restart_with_the_interruption_named() {
        let root = tempfile::tempdir().expect("temp dir").keep();
        let session = |id: &str, state: crate::RunState, version: u64| SessionRunState {
            session_id: id.into(),
            state,
            reusable_draft: false,
            owner_id: "main".into(),
            cwd: "/work".into(),
            title: "Title".into(),
            since: 10,
            tasks: Vec::new(),
            listening_ports: vec![3000],
            execution_id: None,
            execution_role: None,
            last_activity_at: 12,
            version,
            interruption: None,
        };
        let previous = AttentionStore::open(&root).unwrap();
        previous
            .write_run_states(&[
                session("cut", crate::RunState::Running, 3),
                session("finished", crate::RunState::Done, 5),
            ])
            .unwrap();
        previous
            .write_leases(&[ProcessLease { agent_id: "ghost".into(), process_id: u32::MAX - 2, started_at: 1 }])
            .unwrap();

        let runtime =
            Runtime::with_storage(Journal::open(root.join("sessions")).unwrap(), AttentionStore::open(&root).unwrap());
        let mut states = runtime.run_states();
        states.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        assert_eq!(states[0].state, crate::RunState::RequiresAction);
        assert_eq!(states[0].interruption.as_deref(), Some(crate::RUNTIME_RESTARTED));
        assert_eq!(states[0].owner_id, "");
        assert!(states[0].listening_ports.is_empty());
        assert_eq!(states[0].version, 4, "the interruption is a change, so the version moved once");
        assert_eq!(states[1].state, crate::RunState::Done);
        assert_eq!(states[1].version, 5, "same state, same version: losing the owner is not an attention change");
        assert!(AttentionStore::open(&root).unwrap().read_leases().unwrap().is_empty());
        assert_eq!(
            states_sorted(AttentionStore::open(&root).unwrap().read_run_states().unwrap()),
            states_sorted(runtime.run_states())
        );

        assert!(!runtime.mark_read("finished", Some(4)), "an older confirmation does not clear a newer state");
        assert!(runtime.mark_read("finished", Some(5)));
        assert_eq!(runtime.run_states().iter().find(|state| state.session_id == "finished").unwrap().version, 6);
    }

    fn states_sorted(mut states: Vec<SessionRunState>) -> Vec<SessionRunState> {
        states.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        states
    }

    struct ChannelSink {
        sender: mpsc::UnboundedSender<RuntimeEvent>,
    }

    impl RuntimeEventSink for ChannelSink {
        fn publish(&self, event: RuntimeEvent) {
            self.sender.send(event).expect("runtime event receiver");
        }
    }

    fn metadata(engine: &str) -> AgentMetadata {
        AgentMetadata {
            owner_id: "main".into(),
            engine: engine.into(),
            provider: "test".into(),
            provider_revision: None,
            model: "test".into(),
            connection: "test".into(),
        }
    }

    /// A claim on a session a live agent runs takes it on the spot; one on a session nobody runs is
    /// reserved and turns active when the holder's agent resumes it; deletion retires the claims,
    /// stops the process, waits for it, and only `delete_end` forgets the session.
    #[tokio::test]
    async fn claims_follow_the_process_and_deletion_waits_for_it() {
        let runtime = Arc::new(test_runtime());
        runtime.start("agent".into(), metadata("dsh"), &mut Command::new("cat")).await.unwrap();
        let resume = |id: u64, session: &str| {
            serde_json::json!({"jsonrpc":"2.0","id":id,"method":"session/resume","params":{"sessionId":session,"cwd":"/work"}})
                .to_string()
        };
        runtime.send("agent", resume(1, "s")).await.unwrap();
        runtime.accept_line("agent", r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}"#.into()).unwrap();

        let mine = runtime.claim("s", "a1", "digest", "win-1", "conn:main".into()).await.unwrap();
        let attach = mine.attach.expect("a live agent runs s");
        assert_eq!(attach.agent_id, "agent");
        assert!(attach.agent_instance_id.starts_with("agent#"), "{}", attach.agent_instance_id);
        assert_eq!(
            mine.claim.phase,
            ClaimPhase::Active { agent_id: "agent".into(), agent_instance_id: attach.agent_instance_id.clone() }
        );
        let again = runtime.claim("s", "a1", "digest", "win-1", "conn:main".into()).await.unwrap();
        assert_eq!(again.claim, mine.claim, "idempotent");
        assert!(again.attach.is_none());
        assert_eq!(
            runtime.claim("s", "a1", "other", "win-1", "conn:main".into()).await.unwrap_err().message,
            "claim/digest_mismatch"
        );

        let reserved = runtime.claim("t", "a2", "digest", "win-1", "conn:main".into()).await.unwrap();
        assert_eq!(reserved.claim.phase, ClaimPhase::Reserved);
        assert!(reserved.attach.is_none());
        runtime.send("agent", resume(2, "t")).await.unwrap();
        runtime.accept_line("agent", r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"t"}}"#.into()).unwrap();
        assert!(
            matches!(runtime.claim_status("win-1", "a2").unwrap().phase, ClaimPhase::Active { agent_id, .. } if agent_id == "agent"),
            "the holder's agent resumed t, so the reserved claim is active"
        );

        let begun = runtime.delete_begin("s").await.unwrap();
        assert_eq!(begun.stage, DeletionStage::Unlinking, "the process was stopped and reaped");
        assert_eq!(runtime.agent_id_for_session("s"), None);
        assert_eq!(
            runtime.claim_status("win-1", "a1").unwrap().phase,
            ClaimPhase::Retired { how: ClaimRetirement::Deleted }
        );
        assert!(
            matches!(runtime.claim_status("win-1", "a2").unwrap().phase, ClaimPhase::Failed { .. }),
            "t's claim died with the process"
        );
        assert_eq!(
            runtime.claim("s", "a3", "digest", "win-1", "conn:main".into()).await.unwrap_err().message,
            "claim/deleting"
        );
        assert_eq!(runtime.deletions().len(), 1);
        assert!(runtime.run_states().iter().all(|state| state.session_id != "s"), "the dead agent's entries are gone");
        assert_eq!(runtime.delete_end("s", DeletionOutcome::Deleted).unwrap(), None);
        assert!(runtime.deletions().is_empty());
        assert!(AttentionStore::open(runtime.attention.root()).unwrap().read_deletions().unwrap().is_empty());
        assert_eq!(
            runtime.claim("s", "a3", "digest", "win-1", "conn:main".into()).await.unwrap().claim.phase,
            ClaimPhase::Reserved
        );
    }

    // cat blocks on the held piped stdin, so the process lives until the runtime drops (kill_on_drop)
    async fn dsh_runtime() -> Arc<Runtime> {
        let runtime = Arc::new(test_runtime());
        runtime.start("agent".into(), metadata("dsh"), &mut Command::new("cat")).await.unwrap();
        runtime
    }

    #[tokio::test]
    async fn credentials_reach_the_engine_without_entering_observer_events() {
        let runtime = Arc::new(test_runtime());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let mut command = Command::new("python3");
        command.args([
            "-c",
            r#"
import json,sys
for line in sys.stdin:
    frame=json.loads(line)
    assert frame['params']['_meta']['codex']['bearerToken']=='private-provider-token'
    print('upstream rejected private-provider-token',file=sys.stderr,flush=True)
    print(json.dumps({'jsonrpc':'2.0','id':frame['id'],'result':{'received':True,'echo':'private-provider-token'}}),flush=True)
"#,
        ]);
        runtime.start("agent".into(), metadata("codex"), &mut command).await.unwrap();
        let result = runtime
            .acp_request(
                "agent",
                "providers/set",
                serde_json::json!({
                    "providerId":"openai", "apiType":"openai", "baseUrl":"https://fixture.invalid",
                    "_meta":{"codex":{"bearerToken":"private-provider-token"}}
                }),
                None,
                "main",
            )
            .await
            .unwrap();
        assert_eq!(result["received"], true);
        assert_eq!(result["echo"], "private-provider-token");
        let mut saw_redacted_wire = false;
        let mut saw_redacted_outbound = false;
        let mut saw_redacted_stderr = false;
        // stderr and stdout are separate pipes; wait for the diagnostic as well as the reply.
        while !saw_redacted_stderr || !saw_redacted_wire || !saw_redacted_outbound {
            let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await.unwrap().unwrap();
            let encoded = serde_json::to_string(&event).unwrap();
            assert!(!encoded.contains("private-provider-token"));
            match event {
                RuntimeEvent::Wire { outbound: true, lines, .. } => {
                    saw_redacted_wire |= lines.iter().any(|line| line.contains("redacted"));
                }
                RuntimeEvent::Outbound { line, .. } => saw_redacted_outbound |= line.contains("redacted"),
                RuntimeEvent::Stderr { line, .. } => saw_redacted_stderr |= line.contains("[REDACTED]"),
                _ => {}
            }
        }
        assert!(saw_redacted_wire && saw_redacted_outbound);
        runtime.stop("agent").unwrap();
    }

    #[test]
    fn diagnostics_redact_escaped_and_rotated_credentials_without_corrupting_json() {
        let mut private = PrivateValues::default();
        let original = "key-with-\"quote\nand-newline";
        private.remember(&serde_json::json!({"headers":{"authorization":format!("Bearer {original}")}}), false);
        private.remember(&serde_json::json!({"_meta":{"codex":{"bearerToken":"rotated-secret"}}}), false);
        let encoded = serde_json::json!({"id":1,"error":{"message":format!("{original} rotated-secret")}}).to_string();
        let public: Value = serde_json::from_str(&private.line(&encoded)).unwrap();
        assert_eq!(public["id"], 1);
        assert_eq!(public["error"]["message"], "[REDACTED] [REDACTED]");
        assert_eq!(private.line(&format!("stderr: {original}")), "stderr: [REDACTED]");
    }

    #[test]
    fn auth_diagnostics_remove_all_private_input_but_keep_request_identity() {
        let public: Value = serde_json::from_str(&public_request_line(&serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"auth/login", "params":{"password":"private-password", "_meta":{"apiKey":"private-key"}}
        }))).unwrap();
        assert_eq!(public["id"], 1);
        assert_eq!(public["method"], "auth/login");
        assert_eq!(public["params"], serde_json::json!({"redacted":true}));
    }

    #[tokio::test]
    async fn resume_reserves_the_record_for_every_engine_until_the_engine_answers() {
        for engine in ["alwith", "dsh"] {
            let runtime = Arc::new(test_runtime());
            runtime.start("agent".into(), metadata(engine), &mut Command::new("cat")).await.unwrap();
            runtime.start("other".into(), metadata(engine), &mut Command::new("cat")).await.unwrap();
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
            runtime.add_sink(&sink);
            let request = r#"{"jsonrpc":"2.0","id":2,"method":"session/resume","params":{"sessionId":"record","cwd":"/p","replayFrom":{"type":"start"}}}"#;
            runtime.send("agent", request.to_string()).await.unwrap();
            // Before the engine answers, the record identity already belongs to this agent; another engine or process cannot restore the same record at the same time.
            assert_eq!(runtime.agent_id_for_session("record").as_deref(), Some("agent"));
            let error = runtime.send("other", request.into()).await.unwrap_err();
            assert!(error.contains("already active or opening"), "{error}");
            assert!(runtime.agents.lock().unwrap()["other"].failure.is_none());
            // The engine receives a standard session/resume: replayFrom passes through as is, only the Runtime envelope is stripped.
            loop {
                let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
                if let RuntimeEvent::Wire { outbound: true, lines, .. } = event {
                    let wire: Value = serde_json::from_str(&lines[0]).unwrap();
                    assert_eq!(wire["method"], "session/resume");
                    assert_eq!(wire["params"]["replayFrom"]["type"], "start");
                    assert!(wire.get("_alwithRuntime").is_none());
                    break;
                }
            }
            runtime.accept_line("agent", r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#.into()).unwrap();
            assert_eq!(runtime.agent_id_for_session("record").as_deref(), Some("agent"));
            // The same process reattaching its own session is not a takeover.
            runtime.send("agent", request.into()).await.unwrap();
            runtime.stop("agent").unwrap();
            runtime.stop("other").unwrap();
        }
    }

    #[tokio::test]
    async fn repeated_initialize_is_answered_from_cache_without_reaching_the_engine() {
        let runtime = dsh_runtime().await;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        // No successful handshake yet: initialize goes to the engine as usual (a real handshake).
        runtime.send("agent", r#"{"jsonrpc":"2.0","id":20,"method":"initialize","params":{}}"#.into()).await.unwrap();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if let RuntimeEvent::Wire { outbound: true, lines, .. } = event {
                assert!(lines[0].contains(r#""id":20"#), "the first initialize reaches the engine");
                break;
            }
        }
        // A server request with the same id cannot pose as the handshake response, and a failed handshake is not cached.
        runtime.accept_line("agent", r#"{"jsonrpc":"2.0","id":20,"method":"test","params":{}}"#.into()).unwrap();
        runtime
            .accept_line("agent", r#"{"jsonrpc":"2.0","id":20,"error":{"code":-32602,"message":"rejected"}}"#.into())
            .unwrap();
        runtime.send("agent", r#"{"jsonrpc":"2.0","id":29,"method":"initialize","params":{}}"#.into()).await.unwrap();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if let RuntimeEvent::Wire { outbound: true, lines, .. } = event {
                assert!(
                    lines[0].contains(r#""id":29"#),
                    "a failed handshake leaves no cache, so this one goes to the engine as usual"
                );
                break;
            }
        }
        runtime.send("agent", r#"{"jsonrpc":"2.0","id":21,"method":"initialize","params":{}}"#.into()).await.unwrap();
        let response = serde_json::json!({"protocolVersion":2,"info":{"name":"test","version":"1"},"capabilities":{"session":{"resume":{},"fork":{}},"providers":{}}});
        runtime
            .accept_line("agent", serde_json::json!({"jsonrpc":"2.0","id":21,"result":response}).to_string())
            .unwrap();
        // Drain the outbound frames of the real handshake with id 21; the assertions below only look at the cache replay.
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if let RuntimeEvent::Wire { outbound: true, lines, .. } = event
                && lines[0].contains(r#""id":21"#)
            {
                break;
            }
        }
        for request_id in [30, 31] {
            runtime
                .send(
                    "agent",
                    serde_json::json!({"jsonrpc":"2.0","id":request_id,"method":"initialize","params":{"protocolVersion":2,"info":{"name":"second","version":"1"}}})
                        .to_string(),
                )
                .await
                .unwrap();
            loop {
                let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
                assert!(
                    !matches!(event, RuntimeEvent::Wire { outbound: true, .. }),
                    "initialize state must not reach the engine"
                );
                if let RuntimeEvent::Inbound { lines, .. } = event {
                    let result: Value = serde_json::from_str(&lines[0]).unwrap();
                    if result["id"] == request_id && result.get("method").is_none() {
                        assert_eq!(result["result"], response);
                        break;
                    }
                }
            }
        }
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn the_handshake_cache_does_not_survive_a_restart() {
        let runtime = dsh_runtime().await;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        runtime.send("agent", r#"{"id":20,"method":"initialize","params":{}}"#.into()).await.unwrap();
        runtime.accept_line("agent", r#"{"id":20,"result":{"protocolVersion":2,"capabilities":{}}}"#.into()).unwrap();
        let metadata = runtime.agents.lock().unwrap().get("agent").unwrap().metadata.clone();
        // Restart = stop, wait until it has really exited, then start.
        runtime.stop("agent").unwrap();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if matches!(event, RuntimeEvent::Exit { .. }) {
                break;
            }
        }
        runtime.start("agent".into(), metadata, &mut Command::new("cat")).await.unwrap();
        // The new process has not handshaken yet: this goes to the engine instead of being answered from cache.
        runtime.send("agent", r#"{"id":30,"method":"initialize","params":{}}"#.into()).await.unwrap();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if let RuntimeEvent::Wire { outbound: true, lines, .. } = event
                && lines[0].contains(r#""id":30"#)
            {
                break;
            }
        }
        runtime.stop("agent").unwrap();
    }

    /// Journaled frames carry the same sequence on the live event: the app's `session/prompt` (outbound) and the same
    /// frame echoed by the agent (cat echo, which with an id is an acpAgentRequest) each take one number, matching the
    /// sequences in `session_events`; clients use this to detect missed frames and refill in order. Frames that are
    /// not journaled (here the initialize response echoed by cat) have no sequence.
    #[tokio::test]
    async fn journaled_frames_carry_their_sequence_on_live_events() {
        let runtime = dsh_runtime().await;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let prompt = r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#;
        runtime.send("agent", prompt.into()).await.unwrap();
        let mut outbound = None;
        let mut echoed = None;
        while outbound.is_none() || echoed.is_none() {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            match event {
                RuntimeEvent::Outbound { session_id, sequence, .. } => outbound = Some((session_id, sequence)),
                RuntimeEvent::AcpAgentRequest { method, session_id, sequence, .. } if method == "session/prompt" => {
                    echoed = Some((session_id, sequence));
                }
                _ => {}
            }
        }
        assert_eq!(outbound, Some((Some("s1".to_string()), Some(0))));
        assert_eq!(echoed, Some((Some("s1".to_string()), Some(1))));
        let journaled: Vec<u64> =
            runtime.session_events("s1", None, 10).into_iter().map(|frame| frame.sequence).collect();
        assert_eq!(journaled, vec![0, 1]);
        runtime.stop("agent").unwrap();
    }

    /// Switching engines = stop, wait for exit, then start. A second process with the same name may not start while
    /// the first is alive; this replaces the old generation mechanism: with no "two processes for one agentId", late
    /// output has no ownership ambiguity.
    #[tokio::test]
    async fn starting_a_live_agent_again_is_refused_until_it_exits() {
        let runtime = dsh_runtime().await;
        let metadata = runtime.agents.lock().unwrap()["agent"].metadata.clone();
        let error = runtime.start("agent".into(), metadata.clone(), &mut Command::new("cat")).await.unwrap_err();
        assert!(error.contains("still running"), "{error}");

        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        runtime.stop("agent").unwrap();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await.unwrap().unwrap();
            if matches!(event, RuntimeEvent::Exit { .. }) {
                break;
            }
        }
        // Receiving Exit means "it is really gone": from now on starting the same name always succeeds.
        runtime.start("agent".into(), metadata, &mut Command::new("cat")).await.unwrap();
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn batches_stdout_and_removes_exited_process() {
        let runtime = Arc::new(test_runtime());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink: Arc<dyn RuntimeEventSink> = Arc::new(ChannelSink { sender });
        runtime.add_sink(&sink);
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf '%s\\n' first second"]);

        runtime.start("agent-1".to_string(), metadata("alwith"), &mut command).await.expect("start test agent");

        let mut received_lines = None;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .expect("runtime event timeout")
                .expect("runtime event channel");
            match event {
                RuntimeEvent::Inbound { lines, .. } => received_lines = Some(lines),
                RuntimeEvent::Exit { agent_id } => {
                    assert_eq!(agent_id, "agent-1");
                    break;
                }
                RuntimeEvent::Wire { .. }
                | RuntimeEvent::Outbound { .. }
                | RuntimeEvent::Stderr { .. }
                | RuntimeEvent::RunStates { .. }
                | RuntimeEvent::AcpNotification { .. }
                | RuntimeEvent::AcpAgentRequest { .. } => {}
            }
        }

        assert_eq!(received_lines, Some(vec!["first".to_string(), "second".to_string()]));
        assert!(runtime.agent_snapshots().is_empty());
    }

    #[tokio::test]
    async fn provider_switch_updates_attach_only_after_success() {
        let runtime = Arc::new(test_runtime());
        let mut command = Command::new("python3");
        command.args([
            "-c",
            r#"
import json,sys
for line in sys.stdin:
    frame=json.loads(line)
    if frame['method']=='session/resume':
        reply={'result':{'sessionId':frame['params']['sessionId']}}
    elif frame['params']['_meta']['alwith']['providerId']=='rejected':
        reply={'error':{'code':-32000,'message':'provider rejected'}}
    else:
        assert frame['params']['providerId']=='main'
        reply={'result':{}}
    print(json.dumps({'jsonrpc':'2.0','id':frame['id'],**reply}),flush=True)
"#,
        ]);
        let mut initial = metadata("alwith");
        initial.provider = "claude-plan".into();
        runtime.start("agent".into(), initial, &mut command).await.unwrap();
        runtime
            .acp_request(
                "agent",
                "session/resume",
                serde_json::json!({"sessionId":"session", "cwd":"/project"}),
                None,
                "main",
            )
            .await
            .unwrap();
        assert_eq!(runtime.attach("session", "main".into()).await.unwrap().provider, "claude-plan");

        runtime
            .acp_request(
                "agent",
                "providers/set",
                serde_json::json!({
                    "providerId":"main", "apiType":"anthropic", "baseUrl":"https://fixture.invalid",
                    "_meta":{"alwith":{"providerId":"litellm", "model":"deepseek-V4.1-flash"}}
                }),
                None,
                "main",
            )
            .await
            .unwrap();
        assert_eq!(runtime.agent_snapshots()[0].provider, "litellm");
        assert_eq!(runtime.attach("session", "chat".into()).await.unwrap().provider, "litellm");

        let error = runtime
            .acp_request(
                "agent",
                "providers/set",
                serde_json::json!({
                    "providerId":"main", "apiType":"anthropic", "baseUrl":"https://fixture.invalid",
                    "_meta":{"alwith":{"providerId":"rejected", "model":"other"}}
                }),
                None,
                "main",
            )
            .await
            .unwrap_err();
        assert_eq!(error.message, "provider rejected");
        assert_eq!(runtime.agent_snapshots()[0].provider, "litellm");
        assert_eq!(runtime.attach("session", "main".into()).await.unwrap().provider, "litellm");
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn pending_provider_switch_does_not_change_attach_metadata() {
        let runtime = dsh_runtime().await;
        runtime
            .send(
                "agent",
                serde_json::json!({
                    "jsonrpc":"2.0", "id":7, "method":"providers/set", "params":{
                        "providerId":"main", "_meta":{"alwith":{"providerId":"litellm"}}
                    }
                })
                .to_string(),
            )
            .await
            .unwrap();
        assert_eq!(runtime.agent_snapshots()[0].provider, "test");
        runtime.accept_line("agent", serde_json::json!({"jsonrpc":"2.0","id":8,"result":{}}).to_string()).unwrap();
        assert_eq!(runtime.agent_snapshots()[0].provider, "test");
        runtime.accept_line("agent", serde_json::json!({"jsonrpc":"2.0","id":7,"result":{}}).to_string()).unwrap();
        assert_eq!(runtime.agent_snapshots()[0].provider, "litellm");
        runtime.stop("agent").unwrap();
    }

    fn register_provider_test_session(runtime: &Runtime) {
        let mut registry = runtime.registry.lock().unwrap();
        registry.record_outbound(
            "agent",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"sessionId":"session","cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session"}}"#.into()],
            2,
        );
    }

    #[tokio::test]
    async fn attach_waits_for_provider_confirmation_without_holding_the_agent_lane() {
        for succeeds in [true, false] {
            let runtime = dsh_runtime().await;
            register_provider_test_session(&runtime);
            runtime.agents.lock().unwrap().get_mut("agent").unwrap().metadata.provider_revision = Some(7);
            runtime.send("agent", serde_json::json!({
                "jsonrpc":"2.0", "id":7, "method":"providers/set",
                "params":{"providerId":"main", "_meta":{"alwith":{"providerId":"litellm", "providerRevision":8}}}
            }).to_string()).await.unwrap();
            let attaching = runtime.attach("session", "chat".into());
            tokio::pin!(attaching);
            assert!(futures_util::poll!(&mut attaching).is_pending());
            assert!(runtime.owns_agent("agent", "main"));
            let lane = runtime.lane("agent");
            let operation = lane.lock().await;
            let reply = if succeeds {
                serde_json::json!({"id":7,"result":{}})
            } else {
                serde_json::json!({"id":7,"error":{"code":-1,"message":"rejected"}})
            };
            runtime.accept_line("agent", reply.to_string()).unwrap();
            drop(operation);
            let attached = tokio::time::timeout(Duration::from_secs(2), attaching).await.unwrap().unwrap();
            assert_eq!(attached.provider, if succeeds { "litellm" } else { "test" });
            assert_eq!(attached.provider_revision, Some(if succeeds { 8 } else { 7 }));
            runtime.stop("agent").unwrap();
        }
    }

    #[tokio::test]
    async fn attach_waits_for_accepted_raw_provider_send_before_it_is_written() {
        let runtime = dsh_runtime().await;
        register_provider_test_session(&runtime);
        let sending = runtime.send(
            "agent",
            serde_json::json!({"id":7,"method":"providers/set","params":{
                "_meta":{"alwith":{"providerId":"litellm","providerRevision":0}}
            }})
            .to_string(),
        );
        tokio::pin!(sending);
        assert!(futures_util::poll!(&mut sending).is_pending());
        let attaching = runtime.attach("session", "chat".into());
        tokio::pin!(attaching);
        assert!(futures_util::poll!(&mut attaching).is_pending());
        sending.await.unwrap();
        runtime.accept_line("agent", r#"{"id":7,"result":{}}"#.into()).unwrap();
        let attached = tokio::time::timeout(Duration::from_secs(2), attaching).await.unwrap().unwrap();
        assert_eq!(attached.provider_revision, Some(0));
        assert_eq!(attached.provider, "litellm");
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn failed_provider_send_cleans_up_even_when_its_caller_is_dropped() {
        let runtime = dsh_runtime().await;
        register_provider_test_session(&runtime);
        {
            let sending = runtime.send(
                "agent",
                serde_json::json!({"id":7,"method":"providers/set","params":{
                    "_meta":{"alwith":{"providerId":"bad","providerRevision":-1}}
                }})
                .to_string(),
            );
            tokio::pin!(sending);
            assert!(futures_util::poll!(&mut sending).is_pending());
        }
        let attached = tokio::time::timeout(Duration::from_secs(2), runtime.attach("session", "chat".into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(attached.provider, "test");
        assert_eq!(attached.provider_revision, None);
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn attach_stops_waiting_when_an_unconfirmed_provider_process_exits() {
        let runtime = dsh_runtime().await;
        register_provider_test_session(&runtime);
        runtime
            .send(
                "agent",
                serde_json::json!({"id":7,"method":"providers/set","params":{
                    "_meta":{"alwith":{"providerId":"litellm","providerRevision":0}}
                }})
                .to_string(),
            )
            .await
            .unwrap();
        let attaching = runtime.attach("session", "chat".into());
        tokio::pin!(attaching);
        assert!(futures_util::poll!(&mut attaching).is_pending());
        runtime.stop("agent").unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), attaching).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn provider_revision_is_zero_or_unknown_only_when_confirmed() {
        let runtime = dsh_runtime().await;
        register_provider_test_session(&runtime);
        for revision in [serde_json::json!(0), Value::Null] {
            let mut meta = serde_json::json!({"providerId":"litellm"});
            if !revision.is_null() {
                meta["providerRevision"] = revision.clone();
            }
            runtime
                .send(
                    "agent",
                    serde_json::json!({"id":7,"method":"providers/set","params":{"_meta":{"alwith":meta}}}).to_string(),
                )
                .await
                .unwrap();
            runtime.accept_line("agent", r#"{"id":7,"result":{}}"#.into()).unwrap();
            let attached = runtime.attach("session", "main".into()).await.unwrap();
            assert_eq!(attached.provider, "litellm");
            assert_eq!(attached.provider_revision, revision.as_u64());
        }
        assert!(
            runtime
                .send(
                    "agent",
                    serde_json::json!({"id":8,"method":"providers/set","params":{
                        "_meta":{"alwith":{"providerId":"bad","providerRevision":-1}}
                    }})
                    .to_string()
                )
                .await
                .is_err()
        );
        assert_eq!(runtime.attach("session", "main".into()).await.unwrap().provider, "litellm");
        runtime.stop("agent").unwrap();
    }

    #[tokio::test]
    async fn attach_returns_initial_provider_before_any_switch() {
        let runtime = Arc::new(test_runtime());
        let mut command = Command::new("cat");
        runtime
            .start(
                "agent-1".to_string(),
                AgentMetadata {
                    owner_id: "main".to_string(),
                    engine: "alwith".to_string(),
                    provider: "kimi".to_string(),
                    provider_revision: None,
                    model: "test".to_string(),
                    connection: "test".to_string(),
                },
                &mut command,
            )
            .await
            .expect("start test agent");

        {
            let mut registry = runtime.registry.lock().expect("runtime registry lock poisoned");
            registry.record_outbound(
                "agent-1",
                r#"{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"sessionId":"session-1","cwd":"/project"}}"#,
                1,
            );
            registry.record_inbound_batch(
                "agent-1",
                &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
                2,
            );
        }

        let info = runtime.attach("session-1", "browser".to_string()).await.expect("attach live agent");
        // Without a hot switch the provider comes from the launch metadata; the registry layer only fills an empty
        // placeholder, so dropping this runtime-layer backfill would let an empty providerId flow all the way to the frontend
        assert_eq!(info.provider, "kimi");
    }
}
