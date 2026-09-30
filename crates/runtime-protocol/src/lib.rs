//! Types in the `alwith-runtime-v1` frame protocol: session run state, lifecycle frames, journal
//! frames,
//! agent metadata and Runtime events.
//! Canonical wire types owned by Runtime. Client bindings are exported from this definition.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;

/// The `error` in a response frame, in JSON-RPC shape. Runtime's own errors use the codes below;
/// ACP errors returned by the agent (`acpRequest` etc.) pass through unchanged, `code`/`data` come
/// from the agent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
pub struct ResponseError {
    pub code: i64,
    pub message: String,
    /// Data attached to an agent error (e.g. `{details}`, `{authMethods}`); null if absent.
    #[serde(default)]
    #[specta(type = Option<specta_typescript::Unknown>)]
    pub data: Option<Value>,
}

impl ResponseError {
    /// Runtime failed to execute (no such agent, not the owner, agent exited...).
    pub const RUNTIME: i64 = -32000;
    /// Connection lost: Runtime exited or cannot be written to. Generated locally by the client,
    /// never sent by Runtime.
    pub const CONNECTION_LOST: i64 = -32001;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;

    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::new(Self::RUNTIME, message)
    }

    pub fn connection_lost(message: impl Into<String>) -> Self {
        Self::new(Self::CONNECTION_LOST, message)
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(Self::METHOD_NOT_FOUND, format!("unknown runtime method {method}"))
    }

    pub fn invalid_params(detail: impl std::fmt::Display) -> Self {
        Self::new(Self::INVALID_PARAMS, format!("invalid params: {detail}"))
    }

    /// The frame's `error` object -> as is; if it is not that shape, the whole value becomes the
    /// message.
    pub fn parse(error: &Value) -> Self {
        match serde_json::from_value::<Self>(error.clone()) {
            Ok(parsed) => parsed,
            Err(_) => Self::runtime(error.to_string()),
        }
    }
}

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ResponseError {}

impl From<String> for ResponseError {
    fn from(message: String) -> Self {
        Self::runtime(message)
    }
}

impl From<&str> for ResponseError {
    fn from(message: &str) -> Self {
        Self::runtime(message)
    }
}

impl From<ResponseError> for String {
    fn from(error: ResponseError) -> Self {
        error.message
    }
}

/// Agent requests still unanswered at attach time (`session/request_permission` etc.); the new
/// owner answers them with `acpRespond` using the same `id`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
pub struct PendingAgentRequest {
    #[specta(type = specta_typescript::Unknown)]
    pub id: Value,
    pub method: String,
    #[specta(type = specta_typescript::Unknown)]
    pub params: Value,
}

/// Session run state. The first three are values of ACP v2 `StateUpdate` directly (reported by the
/// agent, no longer derived by Runtime from traffic);
/// `Done` is a product state we synthesize ourselves: the protocol has no notion of "finished but
/// unseen". It = idle + unread,
/// synthesized by `upsert_run_state` on a running/done -> idle transition and consumed only by
/// `markRead`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    RequiresAction,
    Done,
    Idle,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct SessionTask {
    pub id: String,
    pub title: String,
    pub background: bool,
    pub since: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct SessionRunState {
    pub session_id: String,
    pub state: RunState,
    pub reusable_draft: bool,
    #[serde(rename = "owner")]
    pub owner_id: String,
    /// Session working directory (params.cwd of session/new|load|resume|fork). Empty string = the
    /// init request carried none
    /// (e.g. a session tracked only from a prompt after a process restart). Used for mobile list
    /// display.
    pub cwd: String,
    /// Session title (session_info_update.title in session/update, same source as the desktop chat
    /// title).
    /// Empty string = the agent has not produced a title yet. Used for mobile list display.
    pub title: String,
    /// Time the current state was entered (epoch ms). Kept when the same state continues.
    pub since: i64,
    pub tasks: Vec<SessionTask>,
    pub listening_ports: Vec<u16>,
    pub execution_id: Option<String>,
    pub execution_role: Option<String>,
    pub last_activity_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct SessionLifecycleFrame {
    pub sequence: u64,
    pub recorded_at: i64,
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachInfo {
    pub agent_id: String,
    /// The core this agent was spawned with (authoritative AgentMetadata value), filled by
    /// `Runtime::attach`.
    pub engine: String,
    /// The agent's most recently confirmed provider (startup config or a successful providers/set).
    /// attach does not spawn,
    /// so the frontend uses this to mark the current provider (the _meta.alwith.providerId sent
    /// with every prompt depends on it).
    /// The registry holds no process metadata; filled by `Runtime::attach` (the only public exit).
    pub provider: String,
    /// Provider configuration acknowledged by this process; absent for unmanaged launches.
    pub provider_revision: Option<u64>,
    /// Agent requests still unanswered, for the new owner to answer. Sourced from the inbound
    /// replay buffer, which may have been flushed in extreme cases.
    pub pending_agent_requests: Vec<PendingAgentRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
pub enum FrameDirection {
    /// agent -> Runtime (session/update, request_permission, etc.).
    In,
    /// Runtime -> agent (session/prompt, etc.).
    Out,
}

/// One frame in the journal. `line` is the raw ACP JSON-RPC text (before the Runtime envelope).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventFrame {
    pub sequence: u64,
    pub recorded_at: i64,
    pub direction: FrameDirection,
    pub line: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentMetadata {
    pub owner_id: String,
    /// Which core was started (desktop engine manifest key; Runtime only passes it through). attach
    /// does not spawn, so the frontend can only learn it here.
    pub engine: String,
    pub provider: String,
    /// Provider configuration acknowledged by this process; absent for unmanaged launches.
    pub provider_revision: Option<u64>,
    pub model: String,
    pub connection: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RuntimeEvent {
    /// stdio protocol diagnostics; params of outbound providers/set and auth/login are redacted.
    /// The engine receives the raw secret params; observers and replay records never see them.
    Wire {
        agent_id: String,
        outbound: bool,
        lines: Vec<String>,
    },
    /// ACP frames output by the agent (agent -> app), batched every 16ms, each already carrying the
    /// `_alwithRuntime` envelope.
    Inbound {
        agent_id: String,
        lines: Vec<String>,
    },
    /// A frame the app sent to the agent. Frames entering the session journal carry `session_id`
    /// and `sequence` (see `AcpNotification`).
    Outbound {
        agent_id: String,
        line: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
    },
    Stderr {
        agent_id: String,
        line: String,
    },
    /// The agent process has been reaped. There is never a second process running under the same
    /// agentId (`start` rejects while it is alive),
    /// so this event is unambiguous: receiving it means "my agent is gone".
    Exit {
        agent_id: String,
    },
    RunStates {
        sessions: Vec<SessionRunState>,
    },
    /// ACP notification from the agent (`session/update` etc.), already unframed by Runtime;
    /// coexists with the same frame in `inbound`.
    ///
    /// Frames that entered the session journal carry `session_id` and `sequence` (same number as
    /// the same frame in `sessionEvents`):
    /// the client uses it to detect missed frames (a sequence gap) and refill via `sessionEvents`.
    /// Frames not journaled have neither field.
    AcpNotification {
        agent_id: String,
        method: String,
        params: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
    },
    /// ACP request from the agent (`session/request_permission`, `elicitation/create`, `fs/*`...),
    /// the app answers with `acpRespond` using the same `id`. Sequence fields as in
    /// `AcpNotification`.
    AcpAgentRequest {
        agent_id: String,
        id: Value,
        method: String,
        params: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
    },
}
