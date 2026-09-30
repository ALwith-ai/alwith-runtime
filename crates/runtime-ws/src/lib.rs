//! Runtime's WebSocket wire protocol. The embedded desktop host and the standalone `alwith-runtime`
//! are both just port adapters on top of this layer:
//! process, generation, run state, attach / release, permission replay and event batching all live
//! in `alwith-runtime-core`; this crate only handles
//! "how to connect": handshake auth, the request envelope, event fan-out, and mounting the host's
//! own methods (alwith.dev, store, QA...)
//! onto the same connection via [`Extension`].
//!
//! Wire contract (`alwith-runtime-v1`):
//!
//! - Handshake: `Sec-WebSocket-Protocol` carries both `alwith-runtime-v1` and
//! `alwith-auth.<token>`; missing either yields 401.
//! - Request: `{ "id": u64, "method": "camelCase", "params": {...} }` -> `{ "id", "result" |
//! "error" }`.
//!   A failed envelope parse also replies with the original `id` (0 if unavailable); otherwise the
//! client's pending entry keyed by id would wait forever.
//! - Events: JSON frames on the same connection as requests, discriminated by `type`: `wire` /
//! `lines` / `outbound` / `stderr` /
//!   `exit` / `runStates` (the serde form of [`RuntimeEvent`]); host extension events also carry
//! `type`.
//! - Events are broadcast globally to all authenticated connections; write operations (send / stop
//! / release) accept only the owner connection.
//! - Sub-owners: write and attach requests may carry `owner` (the desktop window label); the owner
//! is recorded as `<connection>:<owner>`; the connection itself
//!   has write access to the agents of its sub-owners, and `detachOwner` reassigns a sub-owner's
//! agents to no owner.

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use alwith_runtime_core::{AgentMetadata, ResponseError, Runtime, RuntimeEvent, RuntimeEventSink, SESSION_EVENTS_PAGE};
use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use uuid::Uuid;

pub const PROTOCOL: &str = "alwith-runtime-v1";
pub const AUTH_PREFIX: &str = "alwith-auth.";
pub const EVENT_QUEUE_CAPACITY: usize = 1_024;
/// Shutdown window for in-flight connections after Ctrl-C / SIGTERM: a persistent WebSocket never
/// closes on its own, so an unbounded wait would make
/// graceful shutdown never finish; force exit at the deadline.
pub const SHUTDOWN_FORCE_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);

/// The host decides "which process to start": manifest, entry resolution and env assembly all live
/// in the host; the protocol layer only receives a Command.
#[async_trait]
pub trait AgentLauncher: Send + Sync {
    /// `params` is the raw `launch` field of the `start` request; the host defines its shape.
    async fn prepare(&self, agent_id: &str, owner_id: &str, params: Value) -> Result<(Command, AgentMetadata), String>;
    /// The version reported as `version`.
    fn version(&self) -> String;
}

/// Per-connection host-side state (file watchers, subscription forwarding tasks...), dropped when
/// the connection closes.
pub type ConnectionState = Box<dyn std::any::Any + Send + Sync>;
/// Requests share connection resources; lock only the specific mutable resource, never hold a
/// connection-level lock while waiting for business execution.
pub type SharedConnectionState = Arc<ConnectionState>;

/// Host extension: methods the protocol layer does not recognize are handed to it; when a
/// connection opens it may push its own events into the same event channel.
#[async_trait]
pub trait Extension: Send + Sync {
    /// Connection established. The returned state lives as long as the connection; the host
    /// subscribes to its own broadcast here and pushes to the client via `events`;
    /// when the channel is full it should call `disconnect.notify_one()` so the protocol layer
    /// drops this slow connection.
    fn connect(&self, owner_id: &str, events: mpsc::Sender<String>, disconnect: Arc<Notify>) -> ConnectionState;
    /// Methods the protocol layer lacks. `Ok(None)` means the host does not recognize the method
    /// either.
    async fn call(
        &self, owner_id: &str, connection: &ConnectionState, method: &str, params: Value,
    ) -> Result<Option<Value>, ResponseError>;
    /// Transport disconnects even when a long-running call still holds the state.
    async fn disconnect(&self, _connection: &ConnectionState) {}
    /// Cleanup when the service stops (stopping orchestration, etc.); the protocol layer then calls
    /// `kill_all`.
    async fn shutdown(&self) {}
}

/// No-op implementation for when there is no host extension.
pub struct NoExtension;

#[async_trait]
impl Extension for NoExtension {
    fn connect(&self, _owner_id: &str, _events: mpsc::Sender<String>, _disconnect: Arc<Notify>) -> ConnectionState {
        Box::new(())
    }

    async fn call(
        &self, _owner_id: &str, _connection: &ConnectionState, _method: &str, _params: Value,
    ) -> Result<Option<Value>, ResponseError> {
        Ok(None)
    }
}

/// Methods the protocol layer recognizes itself. Parameter fields correspond one-to-one with the TS
/// `RuntimePort` (camelCase).
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RuntimeCommand {
    /// `owner`: the sub-owner label under the connection (the desktop window label). The owner is
    /// recorded as `<connection>:<owner>`; if omitted it is the connection itself.
    /// The connection itself has write access to all its sub-owners' agents; sub-owners cannot
    /// write to each other.
    /// `launch`: startup parameters on top of the engine table; the shape is defined by
    /// `AgentLauncher`.
    Start {
        agent_id: String,
        launch: Value,
        #[serde(default)]
        owner: Option<String>,
    },
    Send {
        agent_id: String,
        line: String,
        #[serde(default)]
        owner: Option<String>,
    },
    Stop {
        agent_id: String,
        #[serde(default)]
        owner: Option<String>,
    },
    AttachSession {
        session_id: String,
        #[serde(default)]
        owner: Option<String>,
    },
    ReleaseSession {
        session_id: String,
        #[serde(default)]
        owner: Option<String>,
    },
    StopSession {
        session_id: String,
        #[serde(default)]
        owner: Option<String>,
    },
    /// A sub-owner (window) is gone: its agents become ownerless; processes are not killed.
    DetachOwner {
        owner: String,
    },
    /// Explicit execution lease release (alwith_dev execution finished).
    ReleaseExecution {
        execution_id: String,
    },
    AgentForSession {
        session_id: String,
    },
    /// All raw session frames (answered permissions removed), for clients that need everything at
    /// once.
    SessionReplay {
        session_id: String,
    },
    /// dev-only: deliberately slow down agent output.
    SetSlowMode {
        delay_ms: u64,
    },
    MarkRead {
        session_id: String,
    },
    SetSessionTitle {
        session_id: String,
        title: String,
    },
    SessionLifecycle {
        session_id: String,
    },
    /// Late-joining clients refill by `sequence`; `afterSequence` defaults to the start, `limit`
    /// defaults to [`SESSION_EVENTS_PAGE`].
    SessionEvents {
        session_id: String,
        after_sequence: Option<u64>,
        limit: Option<usize>,
    },
    RunStates,
    /// Runtime's own version.
    Version,
    Agents,
    Ping,
    // ---- ACP: Runtime speaks ACP v2 on behalf of the app, so the app no longer assembles frames
    // itself. Raw send / inbound remain as an escape hatch. ----
    /// Any ACP request; Runtime assigns the id and awaits the response. `id` is optional: the app
    /// picks its own id when it needs to withdraw (`$/cancel_request`).
    AcpRequest {
        agent_id: String,
        method: String,
        #[serde(default)]
        params: Value,
        #[serde(default)]
        id: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    /// Any ACP notification (no id).
    AcpNotify {
        agent_id: String,
        method: String,
        #[serde(default)]
        params: Value,
        #[serde(default)]
        owner: Option<String>,
    },
    /// Answer a request sent by the agent (the `id` in the `acpAgentRequest` event).
    AcpRespond {
        agent_id: String,
        id: Value,
        #[serde(default)]
        result: Option<Value>,
        #[serde(default)]
        error: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    /// ACP handshake, may carry the app's own client info and capabilities (e.g. elicitation); if
    /// already done, the cached reply is returned.
    /// Optional: on first use of a session method, Runtime handshakes with its own identity and
    /// empty capabilities.
    Initialize {
        agent_id: String,
        #[serde(default)]
        info: Option<Value>,
        #[serde(default)]
        capabilities: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    /// Create a session; the first time, Runtime handshakes first (`initialize`). `meta` goes into
    /// `_meta`.
    SessionNew {
        agent_id: String,
        cwd: String,
        #[serde(default)]
        mcp_servers: Vec<Value>,
        #[serde(default)]
        meta: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    SessionLoad {
        agent_id: String,
        session_id: String,
        cwd: String,
        #[serde(default)]
        mcp_servers: Vec<Value>,
        #[serde(default)]
        meta: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    SessionResume {
        agent_id: String,
        session_id: String,
        cwd: String,
        #[serde(default)]
        replay_from: Option<Value>,
        #[serde(default)]
        meta: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    SessionFork {
        agent_id: String,
        session_id: String,
        cwd: String,
        #[serde(default)]
        mcp_servers: Vec<Value>,
        #[serde(default)]
        meta: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    /// One conversation turn; returns only when the turn ends (`{stopReason, _meta}`). In-progress
    /// content flows through `acpNotification` events.
    Prompt {
        agent_id: String,
        session_id: String,
        prompt: Value,
        #[serde(default)]
        meta: Option<Value>,
        #[serde(default)]
        owner: Option<String>,
    },
    /// Cancel the current turn (`session/cancel` notification).
    Cancel {
        agent_id: String,
        session_id: String,
        #[serde(default)]
        owner: Option<String>,
    },
}

/// Put `meta` into ACP's `_meta`.
fn with_meta(mut params: Value, meta: Option<Value>) -> Value {
    if let Some(meta) = meta {
        params["_meta"] = meta;
    }
    params
}

#[derive(Debug, Deserialize)]
struct RequestEnvelope {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
pub struct ResponseEnvelope {
    pub id: u64,
    pub result: Option<Value>,
    pub error: Option<ResponseError>,
}

#[derive(Clone)]
pub struct RuntimeServer {
    pub runtime: Arc<Runtime>,
    pub launcher: Arc<dyn AgentLauncher>,
    pub extension: Arc<dyn Extension>,
    token: Arc<str>,
    system: Arc<Mutex<System>>,
}

impl RuntimeServer {
    pub fn new(
        runtime: Arc<Runtime>, launcher: Arc<dyn AgentLauncher>, extension: Arc<dyn Extension>, token: String,
    ) -> Self {
        Self { runtime, launcher, extension, token: token.into(), system: Arc::new(Mutex::new(System::new())) }
    }

    /// WebSocket is mounted at the root path, like `codex app-server`; `/readyz` and `/healthz` are
    /// for probes.
    /// The host's own HTTP routes (artifact preview, etc.) go into `extra`.
    pub fn router(self, extra: Router) -> Router {
        Router::new()
            .route("/", get(upgrade))
            .route("/readyz", get(|| async { "ok" }))
            .route("/healthz", get(|| async { "ok" }))
            .with_state(self)
            .merge(extra)
    }

    /// Start the service and wait for `shutdown`; after a signal, allow [`SHUTDOWN_FORCE_WINDOW`]
    /// to wind down, then `kill_all` all agents.
    pub async fn serve_with_shutdown(
        self, listener: TcpListener, extra: Router, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), std::io::Error> {
        let runtime = Arc::clone(&self.runtime);
        let extension = Arc::clone(&self.extension);
        let app = self.router(extra);
        let (shutdown_started_sender, shutdown_started_receiver) = tokio::sync::oneshot::channel::<()>();
        let graceful_shutdown = async move {
            shutdown.await;
            let _ = shutdown_started_sender.send(());
        };
        let serve_future = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown).into_future();
        tokio::pin!(serve_future);
        let result = tokio::select! {
            result = &mut serve_future => result,
            () = async {
                let _ = shutdown_started_receiver.await;
                tokio::time::sleep(SHUTDOWN_FORCE_WINDOW).await;
            } => Ok(()),
        };
        let _ = tokio::time::timeout(SHUTDOWN_FORCE_WINDOW, extension.shutdown()).await;
        runtime.kill_all();
        result
    }
}

async fn upgrade(
    websocket: WebSocketUpgrade, headers: HeaderMap, State(server): State<RuntimeServer>,
) -> Result<Response, StatusCode> {
    let protocols = headers
        .get("sec-websocket-protocol")
        .and_then(|header| header.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let authenticated = protocols
        .split(',')
        .map(str::trim)
        .any(|protocol| protocol.strip_prefix(AUTH_PREFIX) == Some(server.token.as_ref()));
    if !authenticated || !protocols.split(',').map(str::trim).any(|protocol| protocol == PROTOCOL) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(websocket
        .max_message_size(alwith_runtime_core::MAX_MESSAGE_BYTES)
        .max_frame_size(alwith_runtime_core::MAX_MESSAGE_BYTES)
        .protocols([PROTOCOL])
        .on_upgrade(move |socket| handle_socket(socket, server)))
}

struct SocketEventSink {
    sender: mpsc::Sender<String>,
    disconnect: Arc<Notify>,
}

impl RuntimeEventSink for SocketEventSink {
    fn publish(&self, event: RuntimeEvent) {
        let payload = serde_json::to_string(&event).expect("serialize runtime event");
        match self.sender.try_send(payload) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            // Slow connection: better to drop it than buffer without bound; the client refills by
            // sequence after reconnecting.
            Err(TrySendError::Full(_)) => self.disconnect.notify_one(),
        }
    }
}

/// The main loop only reads, never writes: writes are done by a separate task, so a slow connection
/// blocks only its own write side, not disconnect, cancellation or other requests.
/// When the queue is full `SocketEventSink` triggers `disconnect`; the write side is abandoned
/// immediately here, and the client refills by sequence after reconnecting.
async fn handle_socket(socket: WebSocket, server: RuntimeServer) {
    let owner_id = format!("web-{}", Uuid::new_v4());
    let (event_sender, event_receiver) = mpsc::channel::<String>(EVENT_QUEUE_CAPACITY);
    let (control_sender, control_receiver) = mpsc::channel::<Message>(16);
    let disconnect = Arc::new(Notify::new());
    let sink: Arc<dyn RuntimeEventSink> =
        Arc::new(SocketEventSink { sender: event_sender.clone(), disconnect: Arc::clone(&disconnect) });
    let sink_id = server.runtime.add_sink(&sink);
    let connection: SharedConnectionState =
        Arc::new(server.extension.connect(&owner_id, event_sender.clone(), Arc::clone(&disconnect)));
    let (sender, mut receiver) = socket.split();
    let write_failed = Arc::new(Notify::new());
    let writer = tokio::spawn(socket_write_loop(sender, event_receiver, control_receiver, Arc::clone(&write_failed)));

    loop {
        tokio::select! {
            () = disconnect.notified() => break,
            () = write_failed.notified() => break,
            message = receiver.next() => {
                let Some(Ok(message)) = message else { break };
                match message {
                    Message::Text(text) => {
                        // Requests are handled concurrently: a request that waits for a whole turn
                        // (like `prompt`) must not block the next one on the same connection (e.g.
                        // answering a permission).
                        // Responses return to this single writer via the event channel, so frames
                        // never interleave.
                        let server = server.clone();
                        let owner_id = owner_id.clone();
                        let connection = Arc::clone(&connection);
                        let out = event_sender.clone();
                        tokio::spawn(async move {
                            let response = process_request_shared(&server, &owner_id, &connection, &text).await;
                            let payload = serde_json::to_string(&response).expect("serialize runtime response");
                            let _ = out.send(payload).await;
                        });
                    }
                    Message::Close(_) => break,
                    // When the write side is blocked, drop the pong if it cannot be queued: the
                    // peer will ping again, not worth blocking the read side for.
                    Message::Ping(payload) => { let _ = control_sender.try_send(Message::Pong(payload)); }
                    Message::Pong(_) | Message::Binary(_) => {}
                }
            }
        }
    }

    server.extension.disconnect(&connection).await;
    drop(connection);
    server.runtime.remove_sink(sink_id);
    server.runtime.detach_owners_under(&owner_id);
    writer.abort();
}

/// Writer: events and responses leave through the same queue, pong goes via the control channel. A
/// write failure means the connection is gone; notify the main loop.
async fn socket_write_loop(
    mut sender: futures_util::stream::SplitSink<WebSocket, Message>, mut events: mpsc::Receiver<String>,
    mut control: mpsc::Receiver<Message>, failed: Arc<Notify>,
) {
    loop {
        let message = tokio::select! {
            event = events.recv() => match event {
                Some(event) => Message::Text(event.into()),
                None => return,
            },
            control = control.recv() => match control {
                Some(message) => message,
                None => return,
            },
        };
        if sender.send(message).await.is_err() {
            failed.notify_one();
            return;
        }
    }
}

/// Both core and extension requests run concurrently; long operations never block cancellation.
pub async fn process_request_shared(
    server: &RuntimeServer, owner_id: &str, connection: &SharedConnectionState, text: &str,
) -> ResponseEnvelope {
    let request: RequestEnvelope = match serde_json::from_str(text) {
        Ok(request) => request,
        Err(error) => {
            let id = serde_json::from_str::<Value>(text)
                .ok()
                .and_then(|value| value.get("id").and_then(Value::as_u64))
                .unwrap_or(0);
            return ResponseEnvelope { id, result: None, error: Some(ResponseError::invalid_params(error)) };
        }
    };
    let envelope = serde_json::json!({ "method": request.method, "params": request.params });
    let result = match serde_json::from_value::<RuntimeCommand>(envelope) {
        Ok(command) => execute(server, owner_id, command).await,
        Err(parse_error) => match server.extension.call(owner_id, connection, &request.method, request.params).await {
            Ok(Some(result)) => Ok(result),
            Ok(None) => Err(unknown_method(&request.method, &parse_error)),
            Err(error) => Err(error),
        },
    };
    match result {
        Ok(result) => ResponseEnvelope { id: request.id, result: Some(result), error: None },
        Err(error) => ResponseEnvelope { id: request.id, result: None, error: Some(error) },
    }
}

/// One request frame in, one response frame out. Shared by the ws and stdio entry points; a failed
/// envelope parse also replies with the original `id`.
pub async fn process_request(
    server: &RuntimeServer, owner_id: &str, connection: &ConnectionState, text: &str,
) -> ResponseEnvelope {
    let request: RequestEnvelope = match serde_json::from_str(text) {
        Ok(request) => request,
        Err(error) => {
            let id = serde_json::from_str::<Value>(text)
                .ok()
                .and_then(|value| value.get("id").and_then(Value::as_u64))
                .unwrap_or(0);
            return ResponseEnvelope { id, result: None, error: Some(ResponseError::invalid_params(error)) };
        }
    };
    let result = dispatch(server, owner_id, connection, request.method, request.params).await;
    match result {
        Ok(result) => ResponseEnvelope { id: request.id, result: Some(result), error: None },
        Err(error) => ResponseEnvelope { id: request.id, result: None, error: Some(error) },
    }
}

/// Method name does not match `RuntimeCommand`: a core method with bad params -> INVALID_PARAMS; no
/// such method at all -> METHOD_NOT_FOUND.
fn unknown_method(method: &str, parse_error: &serde_json::Error) -> ResponseError {
    let detail = parse_error.to_string();
    if detail.starts_with("unknown variant") {
        ResponseError::method_not_found(method)
    } else {
        ResponseError::invalid_params(format!("{method}: {detail}"))
    }
}

async fn dispatch(
    server: &RuntimeServer, owner_id: &str, connection: &ConnectionState, method: String, params: Value,
) -> Result<Value, ResponseError> {
    let envelope = serde_json::json!({ "method": method, "params": params });
    match serde_json::from_value::<RuntimeCommand>(envelope) {
        Ok(command) => execute(server, owner_id, command).await,
        Err(parse_error) => match server.extension.call(owner_id, connection, &method, params).await? {
            Some(result) => Ok(result),
            None => Err(unknown_method(&method, &parse_error)),
        },
    }
}

/// Connection + optional sub-owner -> the actual owner string.
pub fn owner_of(connection: &str, owner: Option<String>) -> String {
    match owner {
        Some(label) if !label.is_empty() => format!("{connection}:{label}"),
        _ => connection.to_string(),
    }
}

/// Implementation of the protocol-layer methods. `mark_read` and the two read-only replays do not
/// require ownership: read state is a global fact, observing is a first-class right;
/// write operations (send / stop / release) still go through the owner check.
pub async fn execute(
    server: &RuntimeServer, connection: &str, command: RuntimeCommand,
) -> Result<Value, ResponseError> {
    let runtime = &server.runtime;
    match command {
        RuntimeCommand::Start { agent_id, launch, owner } => {
            let owner_id = owner_of(connection, owner);
            let (mut command, metadata) = server.launcher.prepare(&agent_id, &owner_id, launch).await?;
            runtime.start(agent_id, metadata, &mut command).await?;
            Ok(Value::Null)
        }
        RuntimeCommand::Send { agent_id, line, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.send(&agent_id, line).await?;
            Ok(Value::Null)
        }
        RuntimeCommand::Stop { agent_id, owner } => {
            require_owned_or_orphaned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.stop(&agent_id)?;
            Ok(Value::Null)
        }
        RuntimeCommand::AttachSession { session_id, owner } => {
            let info = runtime.attach(&session_id, owner_of(connection, owner)).await;
            serde_json::to_value(info).map_err(|error| ResponseError::runtime(error.to_string()))
        }
        RuntimeCommand::ReleaseSession { session_id, owner } => {
            let owner_id = owner_of(connection, owner);
            require_owned_session(runtime, &owner_id, &session_id)?;
            runtime.release_session(&session_id, &owner_id);
            Ok(Value::Null)
        }
        RuntimeCommand::StopSession { session_id, owner } => {
            require_owned_session(runtime, &owner_of(connection, owner), &session_id)?;
            runtime.stop_session(&session_id)?;
            Ok(Value::Null)
        }
        RuntimeCommand::DetachOwner { owner } => {
            if owner.is_empty() {
                return Err("detachOwner needs a non-empty owner label".into());
            }
            runtime.detach_owner(&owner_of(connection, Some(owner)));
            Ok(Value::Null)
        }
        RuntimeCommand::ReleaseExecution { execution_id } => Ok(Value::Bool(runtime.release_execution(&execution_id))),
        RuntimeCommand::AgentForSession { session_id } => {
            serde_json::to_value(runtime.agent_id_for_session(&session_id))
                .map_err(|error| ResponseError::runtime(error.to_string()))
        }
        RuntimeCommand::SessionReplay { session_id } => serde_json::to_value(runtime.session_replay(&session_id))
            .map_err(|error| ResponseError::runtime(error.to_string())),
        RuntimeCommand::SetSlowMode { delay_ms } => {
            runtime.set_slow_mode(delay_ms);
            Ok(Value::Null)
        }
        RuntimeCommand::MarkRead { session_id } => {
            runtime.mark_read(&session_id);
            Ok(Value::Null)
        }
        RuntimeCommand::SetSessionTitle { session_id, title } => {
            runtime.set_title(&session_id, title);
            Ok(Value::Null)
        }
        RuntimeCommand::SessionLifecycle { session_id } => serde_json::to_value(runtime.session_lifecycle(&session_id))
            .map_err(|error| ResponseError::runtime(error.to_string())),
        RuntimeCommand::SessionEvents { session_id, after_sequence, limit } => {
            let limit = limit.unwrap_or(SESSION_EVENTS_PAGE).min(SESSION_EVENTS_PAGE);
            serde_json::to_value(runtime.session_events(&session_id, after_sequence, limit))
                .map_err(|error| ResponseError::runtime(error.to_string()))
        }
        RuntimeCommand::RunStates => {
            serde_json::to_value(runtime.run_states()).map_err(|error| ResponseError::runtime(error.to_string()))
        }
        RuntimeCommand::Version => Ok(Value::String(server.launcher.version())),
        RuntimeCommand::Agents => {
            let snapshots = runtime.agent_snapshots();
            let pids: Vec<Pid> =
                snapshots.iter().filter_map(|snapshot| snapshot.pid.map(|pid| Pid::from(pid as usize))).collect();
            let mut system = server.system.lock().map_err(|error| ResponseError::runtime(error.to_string()))?;
            system.refresh_processes(ProcessesToUpdate::Some(&pids), true);
            let agents = snapshots
                .into_iter()
                .map(|snapshot| {
                    let process = snapshot.pid.and_then(|pid| system.process(Pid::from(pid as usize)));
                    serde_json::json!({
                        "agentId": snapshot.agent_id,
                        "pid": snapshot.pid,
                        "provider": snapshot.provider,
                        "model": snapshot.model,
                        "connection": snapshot.connection,
                        "cpu": process.map(sysinfo::Process::cpu_usage).unwrap_or(0.0),
                        "memMb": process.map(|process| process.memory() as f64 / (1024.0 * 1024.0)).unwrap_or(0.0),
                        "uptimeSecs": process.map(sysinfo::Process::run_time).unwrap_or(0)
                    })
                })
                .collect::<Vec<_>>();
            Ok(Value::Array(agents))
        }
        RuntimeCommand::Ping => Ok(Value::String("pong".into())),
        RuntimeCommand::AcpRequest { agent_id, method, params, id, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.acp_request(&agent_id, &method, params, id).await
        }
        RuntimeCommand::AcpNotify { agent_id, method, params, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.acp_notify(&agent_id, &method, params).await?;
            Ok(Value::Null)
        }
        RuntimeCommand::AcpRespond { agent_id, id, result, error, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.acp_respond(&agent_id, id, result, error).await?;
            Ok(Value::Null)
        }
        RuntimeCommand::Initialize { agent_id, info, capabilities, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.ensure_initialized(&agent_id, &server.launcher.version(), info, capabilities).await
        }
        RuntimeCommand::SessionNew { agent_id, cwd, mcp_servers, meta, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.ensure_initialized(&agent_id, &server.launcher.version(), None, None).await?;
            let params = with_meta(serde_json::json!({ "cwd": cwd, "mcpServers": mcp_servers }), meta);
            runtime.acp_request(&agent_id, "session/new", params, None).await
        }
        RuntimeCommand::SessionLoad { agent_id, session_id, cwd, mcp_servers, meta, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.ensure_initialized(&agent_id, &server.launcher.version(), None, None).await?;
            let params =
                with_meta(serde_json::json!({ "sessionId": session_id, "cwd": cwd, "mcpServers": mcp_servers }), meta);
            runtime.acp_request(&agent_id, "session/load", params, None).await
        }
        RuntimeCommand::SessionResume { agent_id, session_id, cwd, replay_from, meta, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.ensure_initialized(&agent_id, &server.launcher.version(), None, None).await?;
            let mut params = serde_json::json!({ "sessionId": session_id, "cwd": cwd });
            if let Some(replay_from) = replay_from {
                params["replayFrom"] = replay_from;
            }
            runtime.acp_request(&agent_id, "session/resume", with_meta(params, meta), None).await
        }
        RuntimeCommand::SessionFork { agent_id, session_id, cwd, mcp_servers, meta, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.ensure_initialized(&agent_id, &server.launcher.version(), None, None).await?;
            let params =
                with_meta(serde_json::json!({ "sessionId": session_id, "cwd": cwd, "mcpServers": mcp_servers }), meta);
            runtime.acp_request(&agent_id, "session/fork", params, None).await
        }
        RuntimeCommand::Prompt { agent_id, session_id, prompt, meta, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            let params = with_meta(serde_json::json!({ "sessionId": session_id, "prompt": prompt }), meta);
            runtime.acp_request(&agent_id, "session/prompt", params, None).await
        }
        RuntimeCommand::Cancel { agent_id, session_id, owner } => {
            require_owned(runtime, &owner_of(connection, owner), &agent_id)?;
            runtime.acp_notify(&agent_id, "session/cancel", serde_json::json!({ "sessionId": session_id })).await?;
            Ok(Value::Null)
        }
    }
}

/// Owner check: the caller itself, or the caller is the connection root of the agent's owner
/// (`<root>` of `<root>:<label>`).
pub fn require_owned(runtime: &Runtime, owner_id: &str, agent_id: &str) -> Result<(), String> {
    if runtime.owns_agent(agent_id, owner_id) {
        return Ok(());
    }
    if let Some(actual) = runtime.agent_owner(agent_id)
        && actual.starts_with(&format!("{owner_id}:"))
    {
        return Ok(());
    }
    Err(format!("agent is not owned by this connection: {agent_id}"))
}

/// stop only: an orphan agent (owner connection gone) can be reclaimed by any connection, no need
/// to attach and claim it first.
pub fn require_owned_or_orphaned(runtime: &Runtime, owner_id: &str, agent_id: &str) -> Result<(), String> {
    if runtime.agent_orphaned(agent_id) {
        return Ok(());
    }
    require_owned(runtime, owner_id, agent_id)
}

pub fn require_owned_session(runtime: &Runtime, owner_id: &str, session_id: &str) -> Result<(), String> {
    let agent_id =
        runtime.agent_id_for_session(session_id).ok_or_else(|| format!("no agent for session: {session_id}"))?;
    require_owned(runtime, owner_id, &agent_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alwith_runtime_core::Journal;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

    struct EchoLauncher;

    #[async_trait]
    impl AgentLauncher for EchoLauncher {
        async fn prepare(
            &self, _agent_id: &str, owner_id: &str, params: Value,
        ) -> Result<(Command, AgentMetadata), String> {
            let engine = params.get("engine").and_then(Value::as_str).ok_or("engine is required")?.to_string();
            Ok((
                Command::new("cat"),
                AgentMetadata {
                    owner_id: owner_id.into(),
                    engine,
                    provider: "test".into(),
                    model: "test".into(),
                    connection: "test".into(),
                },
            ))
        }

        fn version(&self) -> String {
            "0.0.0-test".into()
        }
    }

    struct CountingExtension;

    struct BlockingExtension;

    #[async_trait]
    impl Extension for BlockingExtension {
        fn connect(&self, _: &str, _: mpsc::Sender<String>, _: Arc<Notify>) -> ConnectionState {
            Box::new(Notify::new())
        }
        async fn call(
            &self, _: &str, connection: &ConnectionState, method: &str, _: Value,
        ) -> Result<Option<Value>, ResponseError> {
            let signal = connection.downcast_ref::<Notify>().unwrap();
            match method {
                "wait" => signal.notified().await,
                "release" => signal.notify_one(),
                _ => return Ok(None),
            }
            Ok(Some(Value::Null))
        }
    }

    #[tokio::test]
    async fn long_extension_calls_do_not_block_same_connection_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root.path()).unwrap()));
        let server = RuntimeServer::new(runtime, Arc::new(EchoLauncher), Arc::new(BlockingExtension), "secret".into());
        let (events, _receiver) = mpsc::channel(8);
        let connection = Arc::new(server.extension.connect("test", events, Arc::new(Notify::new())));
        let (waiting, cancellation) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                process_request_shared(&server, "test", &connection, r#"{"id":1,"method":"wait"}"#),
                process_request_shared(&server, "test", &connection, r#"{"id":2,"method":"release"}"#),
            )
        })
        .await
        .expect("cancellation must run while the first request is pending");
        assert!(waiting.error.is_none());
        assert!(cancellation.error.is_none());
    }

    #[async_trait]
    impl Extension for CountingExtension {
        fn connect(&self, _owner_id: &str, _events: mpsc::Sender<String>, _disconnect: Arc<Notify>) -> ConnectionState {
            Box::new(std::sync::atomic::AtomicU32::new(0))
        }

        async fn call(
            &self, _owner_id: &str, connection: &ConnectionState, method: &str, _params: Value,
        ) -> Result<Option<Value>, ResponseError> {
            if method != "hostCount" {
                return Ok(None);
            }
            let count = connection.downcast_ref::<std::sync::atomic::AtomicU32>().expect("connection state");
            Ok(Some(Value::from(count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)))
        }
    }

    async fn start() -> (String, tokio::task::JoinHandle<()>) {
        let root = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root).unwrap()));
        let server = RuntimeServer::new(runtime, Arc::new(EchoLauncher), Arc::new(CountingExtension), "secret".into());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            server.serve_with_shutdown(listener, Router::new(), std::future::pending()).await.unwrap();
        });
        (format!("ws://{address}"), task)
    }

    async fn connect(url: &str, token: &str) -> Socket {
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&format!("{PROTOCOL}, {AUTH_PREFIX}{token}")).unwrap(),
        );
        tokio_tungstenite::connect_async(request).await.unwrap().0
    }

    #[derive(Deserialize)]
    struct ResponseView {
        id: u64,
        result: Option<Value>,
        error: Option<ResponseError>,
    }

    async fn call(socket: &mut Socket, id: u64, method: &str, params: Value) -> ResponseView {
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::json!({ "id": id, "method": method, "params": params }).to_string().into(),
            ))
            .await
            .unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let text = message.into_text().unwrap();
            let value: Value = serde_json::from_str(&text).unwrap();
            if value.get("type").is_some() {
                continue;
            }
            return serde_json::from_value(value).unwrap();
        }
    }

    #[tokio::test]
    async fn rejects_missing_or_wrong_token() {
        let (url, _task) = start().await;
        let mut request = url.as_str().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("alwith-runtime-v1, alwith-auth.wrong"));
        assert!(tokio_tungstenite::connect_async(request).await.is_err());
    }

    #[tokio::test]
    async fn core_methods_and_extension_methods_share_one_connection() {
        let (url, _task) = start().await;
        let mut socket = connect(&url, "secret").await;
        let pong = call(&mut socket, 1, "ping", Value::Null).await;
        assert_eq!(pong.id, 1);
        assert_eq!(pong.result, Some(Value::String("pong".into())));

        let states = call(&mut socket, 2, "runStates", Value::Null).await;
        assert_eq!(states.result, Some(Value::Array(Vec::new())));

        let events = call(&mut socket, 3, "sessionEvents", serde_json::json!({ "sessionId": "nope" })).await;
        assert_eq!(events.result, Some(Value::Array(Vec::new())));

        let first = call(&mut socket, 4, "hostCount", Value::Null).await;
        let second = call(&mut socket, 5, "hostCount", Value::Null).await;
        assert_eq!(first.result, Some(Value::from(1)));
        assert_eq!(second.result, Some(Value::from(2)));

        let unknown = call(&mut socket, 6, "nothing", Value::Null).await;
        let unknown = unknown.error.unwrap();
        assert_eq!(unknown.code, ResponseError::METHOD_NOT_FOUND);
        assert!(unknown.message.contains("unknown runtime method nothing"));

        let malformed = call(&mut socket, 7, "start", serde_json::json!({ "agentId": "a" })).await;
        assert!(malformed.error.is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sub_owners_are_isolated_but_the_connection_root_can_write() {
        let (url, _task) = start().await;
        let mut socket = connect(&url, "secret").await;
        let started = call(
            &mut socket,
            1,
            "start",
            serde_json::json!({ "agentId": "cat", "launch": { "engine": "echo" }, "owner": "main" }),
        )
        .await;
        assert!(started.error.is_none(), "{:?}", started.error);
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#;
        let other =
            call(&mut socket, 2, "send", serde_json::json!({ "agentId": "cat", "line": line, "owner": "side" })).await;
        assert!(other.error.unwrap().message.contains("not owned"));
        let same =
            call(&mut socket, 3, "send", serde_json::json!({ "agentId": "cat", "line": line, "owner": "main" })).await;
        assert!(same.error.is_none(), "{:?}", same.error);
        let root = call(&mut socket, 4, "send", serde_json::json!({ "agentId": "cat", "line": line })).await;
        assert!(root.error.is_none(), "{:?}", root.error);
        let agent = call(&mut socket, 5, "agentForSession", serde_json::json!({ "sessionId": "s1" })).await;
        assert_eq!(agent.result, Some(Value::String("cat".into())));
        let detached = call(&mut socket, 6, "detachOwner", serde_json::json!({ "owner": "main" })).await;
        assert!(detached.error.is_none());
        // Once ownerless, anyone can stop it.
        let stopped = call(&mut socket, 7, "stop", serde_json::json!({ "agentId": "cat", "owner": "side" })).await;
        assert!(stopped.error.is_none(), "{:?}", stopped.error);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn started_agents_journal_their_frames_for_late_joiners() {
        let (url, _task) = start().await;
        let mut socket = connect(&url, "secret").await;
        let started =
            call(&mut socket, 1, "start", serde_json::json!({ "agentId": "cat", "launch": { "engine": "echo" } }))
                .await;
        assert!(started.error.is_none(), "{:?}", started.error);
        // cat echoes the prompt back verbatim as an inbound frame; both go into the session
        // journal.
        let prompt = r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#;
        let sent = call(&mut socket, 2, "send", serde_json::json!({ "agentId": "cat", "line": prompt })).await;
        assert!(sent.error.is_none(), "{:?}", sent.error);
        let mut frames = Vec::new();
        for _ in 0..50 {
            let response = call(&mut socket, 3, "sessionEvents", serde_json::json!({ "sessionId": "s1" })).await;
            frames = response.result.unwrap().as_array().unwrap().clone();
            if frames.len() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["sequence"], 0);
        assert_eq!(frames[0]["direction"], "out");
        assert_eq!(frames[1]["sequence"], 1);
        assert_eq!(frames[1]["direction"], "in");
        let tail =
            call(&mut socket, 4, "sessionEvents", serde_json::json!({ "sessionId": "s1", "afterSequence": 0 })).await;
        assert_eq!(tail.result.unwrap().as_array().unwrap().len(), 1);

        // The second connection is not the owner: it can read the journal but cannot send on the
        // owner's behalf.
        let mut observer = connect(&url, "secret").await;
        let seen = call(&mut observer, 1, "sessionEvents", serde_json::json!({ "sessionId": "s1" })).await;
        assert_eq!(seen.result.unwrap().as_array().unwrap().len(), 2);
        let denied = call(&mut observer, 2, "send", serde_json::json!({ "agentId": "cat", "line": prompt })).await;
        assert!(denied.error.unwrap().message.contains("not owned"));
        let stopped = call(&mut socket, 5, "stop", serde_json::json!({ "agentId": "cat" })).await;
        assert!(stopped.error.is_none());
    }
}
