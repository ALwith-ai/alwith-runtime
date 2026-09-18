//! `alwith-runtime-v1` 帧协议里的类型:会话运行态、生命周期帧、日志帧、agent 元数据与 Runtime 事件。
//! Canonical wire types owned by Runtime. Client bindings are exported from this definition.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;

/// 响应帧里的 `error`,JSON-RPC 的形状。Runtime 自己的错误用下面几个 code;
/// agent 回的 ACP 错误(`acpRequest` 等)原样透传,`code`/`data` 都是 agent 给的。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
pub struct ResponseError {
    pub code: i64,
    pub message: String,
    /// agent 错误附带的数据(如 `{details}`、`{authMethods}`);没有就是 null。
    #[serde(default)]
    #[specta(type = Option<specta_typescript::Unknown>)]
    pub data: Option<Value>,
}

impl ResponseError {
    /// Runtime 执行失败(没有该 agent、不是属主、agent 退出…)。
    pub const RUNTIME: i64 = -32000;
    /// 连接断了:Runtime 退出、写不进去。客户端本地生成,Runtime 不会发。
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

    /// 帧里的 `error` 对象 → 原样;不是这个形状就整个当 message。
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

/// attach 时仍没回答的 agent 请求(`session/request_permission` 等),新属主用 `acpRespond` 带同一个 `id` 回答。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type)]
pub struct PendingAgentRequest {
    #[specta(type = specta_typescript::Unknown)]
    pub id: Value,
    pub method: String,
    #[specta(type = specta_typescript::Unknown)]
    pub params: Value,
}

/// 会话运行态。前三个直接是 ACP v2 `StateUpdate` 的取值(agent 直报,不再由 Runtime 从流量推导);
/// `Done` 是我们自己合成的产品态 —— 协议没有「完成没人看」这个概念,它 = idle + 未读,
/// 由 `upsert_run_state` 在 running/done → idle 的迁移上合成,只由 `markRead` 消费。
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
    /// 会话工作目录(session/new|load|resume|fork 的 params.cwd)。空串 = init 请求没带
    /// (如进程重启后仅凭 prompt 追踪到的会话)。移动端列表显示用。
    pub cwd: String,
    /// 会话标题(session/update 的 session_info_update.title,与桌面 chat 标题同源)。
    /// 空串 = agent 还没产出标题。移动端列表显示用。
    pub title: String,
    /// 进入当前状态的时刻(epoch ms)。同态延续时保留。
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
    /// 该 agent spawn 时起的芯(AgentMetadata 权威值),由 `Runtime::attach` 填。
    pub engine: String,
    /// 该 agent spawn 时定死的 provider(AgentMetadata 权威值)。attach 不 spawn,前端注册表
    /// 只能从这里知道活进程跑的是哪家(prompt 逐轮下发的 _meta.alwith.providerId 依赖它)。
    /// registry 不持进程 metadata,由 `Runtime::attach`(唯一公开出口)填。
    pub provider: String,
    /// 仍没回答的 agent 请求,给新属主接着答。来源是入站回放缓冲,极端情况下可能已被冲掉。
    pub pending_agent_requests: Vec<PendingAgentRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
pub enum FrameDirection {
    /// agent → Runtime(session/update、request_permission 等)。
    In,
    /// Runtime → agent(session/prompt 等)。
    Out,
}

/// 日志里的一帧。`line` 是 ACP JSON-RPC 原文(Runtime 信封之前)。
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
    /// 起的是哪颗芯(desktop engine 清单键;Runtime 只透传,不解释)。attach 不 spawn,前端只能从这里知道。
    pub engine: String,
    pub provider: String,
    pub model: String,
    pub connection: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RuntimeEvent {
    /// stdio 协议诊断；出站 providers/set 与 auth/login 的 params 脱敏。
    /// 引擎收到原始私密参数，观察者和回放记录不会收到这些参数。
    Wire {
        agent_id: String,
        outbound: bool,
        lines: Vec<String>,
    },
    /// agent 输出的 ACP 帧(agent → 应用),按 16ms 合批,每帧已带 `_alwithRuntime` 信封。
    Inbound {
        agent_id: String,
        lines: Vec<String>,
    },
    /// 应用发给 agent 的一帧。进会话日志的帧带 `session_id` 与 `sequence`(见 `AcpNotification`)。
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
    /// agent 进程已收尸。同一个 agentId 不会有第二个进程在跑(`start` 在它活着时直接拒),
    /// 所以这条事件没有歧义:收到它就是「我这个 agent 没了」。
    Exit {
        agent_id: String,
    },
    RunStates {
        sessions: Vec<SessionRunState>,
    },
    /// agent 发来的 ACP 通知(`session/update` 等),Runtime 已拆帧;与 `inbound` 里的同一帧并存。
    ///
    /// 进了会话日志的帧带 `session_id` 与 `sequence`(与 `sessionEvents` 里同一帧的序号相同):
    /// 客户端据此发现漏帧(序号跳了)并用 `sessionEvents` 补齐。不进日志的帧没有这两个字段。
    AcpNotification {
        agent_id: String,
        method: String,
        params: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
    },
    /// agent 发来的 ACP 请求(`session/request_permission`、`elicitation/create`、`fs/*`…),
    /// 应用用 `acpRespond` 带同一个 `id` 回答。序号字段同 `AcpNotification`。
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
