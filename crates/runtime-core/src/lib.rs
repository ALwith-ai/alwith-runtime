use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

mod framing;
mod journal;
pub use framing::{MAX_BUFFERED_BYTES, MAX_MESSAGE_BYTES, ProtocolLines, protocol_lines};
mod port_scanner;
pub mod process;
mod runtime;

pub use alwith_runtime_protocol::{
    AgentMetadata, AttachInfo, FrameDirection, PendingAgentRequest, ResponseError, RunState, RuntimeEvent,
    SessionEventFrame, SessionLifecycleFrame, SessionRunState, SessionTask,
};
pub use journal::{Journal, JournalOp, SESSION_JOURNAL_MAX_BYTES};
pub use runtime::{
    AgentSnapshot, PrivateToolDispatch, Runtime, RuntimeEventSink, SESSION_EVENTS_PAGE, default_journal_root,
};

const AGENT_REPLAY_CAPACITY: usize = 1_000;

/// 生命周期帧:prompt、权限请求、工具状态迁移。不含 AI 正文,供观察面与跨窗接管重建事实流。
pub fn is_lifecycle_line(value: &Value) -> bool {
    let method = value.get("method").and_then(Value::as_str);
    method == Some("session/prompt")
        || method == Some("session/request_permission")
        || value.pointer("/params/update/sessionUpdate").and_then(Value::as_str) == Some("tool_call_update")
}

#[derive(Debug)]
enum OutboundRequest {
    Prompt { session_id: String },
    SessionInit { session_id: Option<String>, cwd: Option<String> },
}

#[derive(Debug)]
struct AgentTrack {
    owner_id: String,
    outbound: HashMap<String, OutboundRequest>,
    sessions: Vec<String>,
    /// 原生 resume 发给引擎前保留身份，响应后转为 sessions 或释放。
    reserved_sessions: HashSet<String>,
    prompt_inflight: HashMap<String, u32>,
    used_sessions: HashSet<String>,
    /// agent 用 `state_update` 帧报来的状态,按 sessionId 记最新一条。
    reported_states: HashMap<String, RunState>,
    pending_permissions: HashMap<String, String>,
    working_directories: HashMap<String, String>,
    current_session_id: Option<String>,
    listening_ports: Vec<u16>,
}

impl AgentTrack {
    fn new(owner_id: String) -> Self {
        Self {
            owner_id,
            outbound: HashMap::new(),
            sessions: Vec::new(),
            reserved_sessions: HashSet::new(),
            prompt_inflight: HashMap::new(),
            used_sessions: HashSet::new(),
            reported_states: HashMap::new(),
            pending_permissions: HashMap::new(),
            working_directories: HashMap::new(),
            current_session_id: None,
            listening_ports: Vec::new(),
        }
    }

    fn activate_session(&mut self, session_id: &str) {
        if !self.sessions.iter().any(|tracked| tracked == session_id) {
            self.sessions.push(session_id.to_string());
        }
        self.current_session_id = Some(session_id.to_string());
    }

    /// 会话当前状态。
    ///
    /// ACP v2 起 agent 用 `state_update` 帧直报(running / idle / requires_action),这里优先用它 ——
    /// 自己从流量推导会在「后台活动在 idle 期间继续发 update」这类情形上误判(协议明写这不改变
    /// 状态,而计数式推导会把它当成又在跑)。帧还没到过的会话(刚建、尚未发过 prompt)回退到计数。
    fn state(&self, session_id: &str) -> RunState {
        if let Some(reported) = self.reported_states.get(session_id) {
            return *reported;
        }
        if self.pending_permissions.values().any(|pending| pending == session_id) {
            RunState::RequiresAction
        } else if self.prompt_inflight.get(session_id).copied().unwrap_or(0) > 0 {
            RunState::Running
        } else {
            RunState::Idle
        }
    }
}

#[derive(Debug, Default)]
pub struct RuntimeRegistry {
    tracks: HashMap<String, AgentTrack>,
    run_states: HashMap<String, SessionRunState>,
    /// sessionId → 已知会话标题。两个来源:桌面前端推来的「正在显示的标题」(含手动改名与
    /// 启动恢复)和 ACP 标题帧。**必须与 run_states 分开存**:标题帧每会话只在 prompt 流程发
    /// 一次,resume 回来的会话在下次发消息前根本收不到帧;标题也可能先于 run_states 条目到达。
    /// 条目建立时从这里回填,否则移动端只能显示「新会话」。会话收尾不清此表 —— 同一个
    /// sessionId 之后还会 resume 回来,清了就又回到没标题。
    known_titles: HashMap<String, String>,
    inbound_replay: HashMap<String, VecDeque<String>>,
    /// 处理帧时判定出的日志动作,由 runtime 在同一批处理后 `take_journal_ops` 取走落盘。
    journal_ops: Vec<JournalOp>,
}

impl RuntimeRegistry {
    /// 开一条 agent 记录。`agent_id` 上不可能已有记录 —— `Runtime::start` 在进程还活着时直接拒绝,
    /// 所以同名的两个进程不存在。返回值是这条记录建立时顺手清掉的会话(理论上为空)。
    pub fn begin_agent(&mut self, agent_id: String, owner_id: String) -> Vec<String> {
        self.inbound_replay.remove(&agent_id);
        let previous = self.tracks.insert(agent_id, AgentTrack::new(owner_id));
        let removed_sessions = previous.map_or_else(Vec::new, |track| track.sessions);
        self.remove_sessions(&removed_sessions);
        removed_sessions
    }

    /// 返回运行态是否变化(要不要广播);这一批判定出的日志动作放进 `journal_ops`。
    pub fn record_outbound(&mut self, agent_id: &str, line: &str, now_ms: i64) -> bool {
        if !self.tracks.contains_key(agent_id) {
            return false;
        }
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => return false,
        };
        let content_changed = self.record_content(FrameDirection::Out, &value, line, now_ms);
        let Some(track) = self.tracks.get_mut(agent_id) else { return false };
        let Some(session_id) = apply_outbound(track, &value) else { return false };
        let state_changed = self.refresh_session(agent_id, &session_id, now_ms);
        let metadata_changed = apply_execution_metadata(&mut self.run_states, &session_id, &value);
        content_changed || state_changed || metadata_changed
    }

    pub fn record_inbound_batch(&mut self, agent_id: &str, lines: &[String], now_ms: i64) -> bool {
        let replay = self.inbound_replay.entry(agent_id.to_string()).or_default();
        for line in lines {
            push_bounded(replay, line.clone(), AGENT_REPLAY_CAPACITY);
            while replay.iter().map(String::len).sum::<usize>() > MAX_BUFFERED_BYTES {
                replay.pop_front();
            }
        }

        let mut changed = false;
        for line in lines {
            let value: Value = match serde_json::from_str(line) {
                Ok(value) => value,
                Err(_) => continue,
            };
            changed |= self.record_content(FrameDirection::In, &value, line, now_ms);
            if value.get("id").is_none() {
                if value.get("method").and_then(Value::as_str) == Some("$/cancel_request") {
                    let cancelled = self.tracks.get_mut(agent_id).and_then(|track| apply_cancel_request(track, &value));
                    if let Some(session_id) = cancelled {
                        changed |= self.refresh_session(agent_id, &session_id, now_ms);
                    }
                    continue;
                }
                changed |= self.apply_state_frame(agent_id, &value, now_ms);
                changed |= apply_task_frame(&mut self.run_states, &value, now_ms);
                changed |= apply_title_frame(&mut self.run_states, &mut self.known_titles, &value);
                continue;
            }
            let session_id = self.tracks.get_mut(agent_id).and_then(|track| apply_inbound(track, &value));
            if let Some(session_id) = session_id {
                changed |= self.refresh_session(agent_id, &session_id, now_ms);
            }
        }
        changed
    }

    /// 取走待落盘的日志动作。runtime 每处理一批帧就取一次,顺序即帧顺序。
    pub fn take_journal_ops(&mut self) -> Vec<JournalOp> {
        std::mem::take(&mut self.journal_ops)
    }

    /// ACP v2 的 `state_update` 帧:agent 直报会话状态(running / idle / requires_action)。
    ///
    /// 记进 track 后走同一条 refresh_session —— `Done` 的合成、去重与广播都不动,因为「完成没人看」
    /// 是我们的产品态,协议不表达它。认不出的取值(协议给 state 留了扩展口)按报错记一笔后忽略:
    /// 静默当成 idle 会让运行中的会话看起来已经结束。
    fn apply_state_frame(&mut self, agent_id: &str, value: &Value, now_ms: i64) -> bool {
        if value.get("method").and_then(Value::as_str) != Some("session/update") {
            return false;
        }
        let update = value.pointer("/params/update");
        if update.and_then(|u| u.get("sessionUpdate")).and_then(Value::as_str) != Some("state_update") {
            return false;
        }
        let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) else {
            return false;
        };
        let reported = match update.and_then(|u| u.get("state")).and_then(Value::as_str) {
            Some("running") => RunState::Running,
            Some("idle") if update.and_then(|u| u.get("stopReason")).and_then(Value::as_str) == Some("_error") => {
                RunState::RequiresAction
            }
            Some("idle") => RunState::Idle,
            Some("requires_action") => RunState::RequiresAction,
            other => {
                log::warn!("Runtime: unknown ACP state_update state {other:?}, ignoring frame");
                return false;
            }
        };
        let Some(track) = self.tracks.get_mut(agent_id) else { return false };
        track.activate_session(session_id);
        track.reported_states.insert(session_id.to_string(), reported);
        let session_id = session_id.to_string();
        self.refresh_session(agent_id, &session_id, now_ms)
    }

    pub fn finish_agent(&mut self, agent_id: &str) -> bool {
        let Some(track) = self.tracks.remove(agent_id) else {
            return false;
        };
        let sessions = track.sessions;
        self.inbound_replay.remove(agent_id);
        self.remove_sessions(&sessions)
    }

    pub fn attach(&mut self, session_id: &str, owner_id: String) -> Option<AttachInfo> {
        let (agent_id, sessions, pending_ids) = self.tracks.iter_mut().find_map(|(agent_id, track)| {
            if !track.sessions.iter().any(|tracked| tracked == session_id) {
                return None;
            }
            track.owner_id = owner_id.clone();
            Some((
                agent_id.clone(),
                track.sessions.clone(),
                track.pending_permissions.keys().cloned().collect::<Vec<_>>(),
            ))
        })?;
        for tracked_session in sessions {
            if let Some(state) = self.run_states.get_mut(&tracked_session) {
                state.owner_id = owner_id.clone();
            }
        }
        let pending_agent_requests = self.pending_agent_requests(&agent_id, &pending_ids);
        Some(AttachInfo { agent_id, engine: String::new(), provider: String::new(), pending_agent_requests })
    }

    pub fn release_session(&mut self, session_id: &str, owner_id: &str) -> bool {
        let Some(track) = self.tracks.values_mut().find(|track| track.sessions.iter().any(|id| id == session_id))
        else {
            return false;
        };
        if track.owner_id != owner_id {
            return false;
        }
        track.owner_id.clear();
        let sessions = track.sessions.clone();
        for tracked_session in sessions {
            if let Some(state) = self.run_states.get_mut(&tracked_session) {
                state.owner_id.clear();
            }
        }
        true
    }

    pub fn detach_owner(&mut self, owner_id: &str) -> bool {
        for track in self.tracks.values_mut().filter(|track| track.owner_id == owner_id) {
            track.owner_id.clear();
        }
        let mut changed = false;
        for state in self.run_states.values_mut().filter(|state| state.owner_id == owner_id) {
            state.owner_id.clear();
            changed = true;
        }
        changed
    }

    /// 执行租约显式释放:执行结束后把还挂着该 executionId 的会话运行态清掉。
    /// 不清的下场:只读观察横幅挂死在一个早已结束的执行上(2026-07-30 审计批次二 #2)。
    pub fn release_execution(&mut self, execution_id: &str) -> bool {
        let mut changed = false;
        for state in self.run_states.values_mut() {
            if state.execution_id.as_deref() == Some(execution_id) {
                state.execution_id = None;
                state.execution_role = None;
                changed = true;
            }
        }
        changed
    }

    pub fn mark_read(&mut self, session_id: &str) -> bool {
        let Some(state) = self.run_states.get_mut(session_id) else { return false };
        if state.state != RunState::Done {
            return false;
        }
        state.state = RunState::Idle;
        true
    }

    /// 记下这个会话的标题。**先进 known_titles 再谈条目**:没有条目不是异常,而是常态
    /// (resume 前、标题帧先到),记下来等条目建立时回填。返回值只回答「要不要广播」——
    /// 只有当下就摆在 run_states 里的条目被改动了才需要。
    pub fn set_title(&mut self, session_id: &str, title: String) -> bool {
        self.known_titles.insert(session_id.to_string(), title.clone());
        let Some(state) = self.run_states.get_mut(session_id) else { return false };
        if state.title == title {
            return false;
        }
        state.title = title;
        true
    }

    pub fn set_agent_listening_ports(&mut self, agent_id: &str, listening_ports: Vec<u16>) -> bool {
        let track = self.tracks.get_mut(agent_id).expect("tracked agent");
        if track.listening_ports == listening_ports {
            return false;
        }
        track.listening_ports = listening_ports.clone();
        let Some(session_id) = &track.current_session_id else { return false };
        let state = self.run_states.get_mut(session_id).expect("active session has run state");
        state.listening_ports = listening_ports;
        true
    }

    pub fn run_states(&self) -> Vec<SessionRunState> {
        self.run_states.values().cloned().collect()
    }

    pub fn agent_id_for_session(&self, session_id: &str) -> Option<String> {
        self.tracks
            .iter()
            .find(|(_, track)| {
                track.sessions.iter().any(|tracked| tracked == session_id)
                    || track.reserved_sessions.contains(session_id)
            })
            .map(|(agent_id, _)| agent_id.clone())
    }

    /// 记录占用:session/resume 发给引擎前占住记录身份,不同芯、不同进程不能同时恢复同一份记录;
    /// 同一进程再次接回自己已激活的会话返回 `Ok(false)`。
    pub(crate) fn reserve_session(&mut self, agent_id: &str, session_id: &str) -> Result<bool, String> {
        if let Some(owner) = self.agent_id_for_session(session_id) {
            if owner == agent_id && self.tracks[agent_id].sessions.iter().any(|id| id == session_id) {
                return Ok(false);
            }
            return Err(format!("record {session_id} is already active or opening in agent {owner}"));
        }
        self.tracks
            .get_mut(agent_id)
            .ok_or("resume agent is not tracked")?
            .reserved_sessions
            .insert(session_id.to_string());
        Ok(true)
    }

    pub fn release_session_reservation(&mut self, agent_id: &str, session_id: &str) {
        self.tracks.get_mut(agent_id).expect("reserved agent is tracked").reserved_sessions.remove(session_id);
    }

    /// 该会话仍未答复的 `session/request_permission` 请求 id(JSON 原文形式,如 `9` 或 `"abc"`)。
    /// 回放日志时只保留这些请求:已答过的一条也不能重放,否则会被答两次。
    pub fn pending_permission_ids(&self, session_id: &str) -> Vec<String> {
        self.tracks
            .values()
            .flat_map(|track| track.pending_permissions.iter())
            .filter(|(_, pending)| pending.as_str() == session_id)
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn refresh_session(&mut self, agent_id: &str, session_id: &str, now_ms: i64) -> bool {
        let track = self.tracks.get(agent_id).expect("tracked agent");
        let state = track.state(session_id);
        let reusable_draft = !track.used_sessions.contains(session_id);
        let owner_id = track.owner_id.clone();
        let cwd = track.working_directories.get(session_id).cloned().unwrap_or_default();
        let inactive_session_ids =
            track.sessions.iter().filter(|tracked| tracked.as_str() != session_id).cloned().collect::<Vec<_>>();
        let listening_ports = if track.current_session_id.as_deref() == Some(session_id) {
            track.listening_ports.clone()
        } else {
            Vec::new()
        };
        let mut changed = false;
        for inactive_session_id in inactive_session_ids {
            if let Some(inactive) = self.run_states.get_mut(&inactive_session_id)
                && !inactive.listening_ports.is_empty()
            {
                inactive.listening_ports.clear();
                changed = true;
            }
        }
        let known_title = self.known_titles.get(session_id).cloned();
        changed
            | upsert_run_state(
                &mut self.run_states,
                owner_id,
                session_id.to_string(),
                cwd,
                state,
                reusable_draft,
                now_ms,
                listening_ports,
                known_title,
            )
    }

    fn remove_sessions(&mut self, sessions: &[String]) -> bool {
        let before = self.run_states.len();
        for session_id in sessions {
            self.run_states.remove(session_id);
        }
        self.run_states.len() != before
    }

    fn pending_agent_requests(&self, agent_id: &str, pending_ids: &[String]) -> Vec<PendingAgentRequest> {
        self.inbound_replay
            .get(agent_id)
            .into_iter()
            .flatten()
            .filter_map(|line| {
                let value = serde_json::from_str::<Value>(line).ok()?;
                let method = value.get("method").and_then(Value::as_str)?;
                let id = value.get("id")?;
                let pending = method == "session/request_permission"
                    && pending_ids.iter().any(|pending| pending == &id.to_string());
                pending.then(|| PendingAgentRequest {
                    id: id.clone(),
                    method: method.to_string(),
                    params: value.get("params").cloned().unwrap_or(Value::Null),
                })
            })
            .collect()
    }

    /// 判定这一帧要不要进会话日志,并刷新 `lastActivityAt`。落盘由 runtime 做。
    fn record_content(&mut self, direction: FrameDirection, value: &Value, line: &str, now_ms: i64) -> bool {
        let method = value.get("method").and_then(Value::as_str);
        let resets_content = match method {
            Some("session/resume") => {
                value.pointer("/params/replayFrom").is_some_and(|replay_from| !replay_from.is_null())
            }
            _ => false,
        };
        if direction == FrameDirection::Out && resets_content {
            if let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) {
                self.journal_ops.push(JournalOp::Reset { session_id: session_id.to_string() });
            }
            return false;
        }
        if !matches!(method, Some("session/update" | "session/prompt" | "session/request_permission")) {
            return false;
        }
        // ACP notices are live-only and must never be replayed after reconnect.
        if method == Some("session/update")
            && value.pointer("/params/update/sessionUpdate").and_then(Value::as_str) == Some("notice")
        {
            return false;
        }
        let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) else { return false };
        self.journal_ops.push(JournalOp::Append {
            session_id: session_id.to_string(),
            direction,
            line: line.to_string(),
        });
        let Some(state) = self.run_states.get_mut(session_id) else { return false };
        if state.last_activity_at == now_ms {
            return false;
        }
        state.last_activity_at = now_ms;
        true
    }
}

fn push_bounded<T>(queue: &mut VecDeque<T>, value: T, capacity: usize) {
    if queue.len() >= capacity {
        queue.pop_front();
    }
    queue.push_back(value);
}

fn apply_outbound(track: &mut AgentTrack, value: &Value) -> Option<String> {
    let id = value.get("id").map(ToString::to_string)?;
    match value.get("method").and_then(Value::as_str) {
        Some("session/prompt") => {
            let session_id = value.pointer("/params/sessionId").and_then(Value::as_str)?;
            track.activate_session(session_id);
            track.used_sessions.insert(session_id.to_string());
            *track.prompt_inflight.entry(session_id.to_string()).or_insert(0) += 1;
            track.outbound.insert(id, OutboundRequest::Prompt { session_id: session_id.to_string() });
            Some(session_id.to_string())
        }
        Some("session/new" | "session/resume" | "session/fork") => {
            if let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str)
                && match value.get("method").and_then(Value::as_str) {
                    Some("session/fork") => true,
                    Some("session/resume") => !track.sessions.iter().any(|tracked| tracked == session_id),
                    _ => false,
                }
            {
                track.used_sessions.insert(session_id.to_string());
            }
            // fork 的 params.sessionId 是源会话；新身份只能来自响应。
            let session_id = if value.get("method").and_then(Value::as_str) == Some("session/resume") {
                value.pointer("/params/sessionId").and_then(Value::as_str).map(String::from)
            } else {
                None
            };
            let cwd = value.pointer("/params/cwd").and_then(Value::as_str).map(String::from);
            track.outbound.insert(id, OutboundRequest::SessionInit { session_id, cwd });
            None
        }
        Some(_) => None,
        None => track.pending_permissions.remove(&id),
    }
}

/// `$/cancel_request`(无 id 的通知):agent 取消自己发出的请求 —— `session/cancel` 时级联取消
/// 挂起的权限 / 问卷(规范 Cancellation 页)。对应的 request_permission 从此不再等答复:不摘掉的话
/// attach 会把它当未答权限重放成僵尸弹窗,用户一点又吐出一帧没有回合的 running。
/// requestId 与登记时的 id 用同一种字符串化(JSON 值原样 to_string),数字 / 字符串 id 都对得上。
/// 返回被撤下的权限所属会话;认不出的 requestId(已答过 / 不是权限)是空操作。
fn apply_cancel_request(track: &mut AgentTrack, value: &Value) -> Option<String> {
    let request_id = value.pointer("/params/requestId")?.to_string();
    track.pending_permissions.remove(&request_id)
}

fn apply_inbound(track: &mut AgentTrack, value: &Value) -> Option<String> {
    let id = value.get("id").map(ToString::to_string)?;
    match value.get("method").and_then(Value::as_str) {
        Some("session/request_permission") => {
            let session_id = value.pointer("/params/sessionId").and_then(Value::as_str)?;
            if !track.sessions.iter().any(|tracked| tracked == session_id) {
                track.sessions.push(session_id.to_string());
            }
            track.pending_permissions.insert(id, session_id.to_string());
            Some(session_id.to_string())
        }
        Some(_) => None,
        None => match track.outbound.remove(&id)? {
            OutboundRequest::Prompt { session_id } => {
                if let Some(inflight) = track.prompt_inflight.get_mut(&session_id) {
                    *inflight = inflight.saturating_sub(1);
                }
                Some(session_id)
            }
            OutboundRequest::SessionInit { session_id, cwd } => {
                if let Some(session_id) = &session_id {
                    track.reserved_sessions.remove(session_id);
                }
                value.get("result")?;
                let session_id = session_id
                    .or_else(|| value.pointer("/result/sessionId").and_then(Value::as_str).map(String::from))?;
                track.activate_session(&session_id);
                if let Some(cwd) = cwd {
                    track.working_directories.insert(session_id.clone(), cwd);
                }
                Some(session_id)
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn upsert_run_state(
    states: &mut HashMap<String, SessionRunState>, owner_id: String, session_id: String, cwd: String,
    next_state: RunState, reusable_draft: bool, now_ms: i64, listening_ports: Vec<u16>, known_title: Option<String>,
) -> bool {
    let current = states.get(&session_id);
    let state = match current {
        Some(current)
            if next_state == RunState::Idle && matches!(current.state, RunState::Running | RunState::Done) =>
        {
            RunState::Done
        }
        _ => next_state,
    };
    if current.is_some_and(|current| {
        current.state == state
            && current.owner_id == owner_id
            && current.cwd == cwd
            && current.reusable_draft == reusable_draft
            && current.listening_ports == listening_ports
    }) {
        return false;
    }
    let since = current.filter(|current| current.state == state).map_or(now_ms, |current| current.since);
    // 条目还没有标题就从已知标题表回填 —— resume 回来的会话不会再发标题帧,不回填
    // 移动端只能显示「新会话」。已有标题的条目不动:那是更新的那一份。
    let title = match current.map(|current| current.title.clone()) {
        Some(title) if !title.is_empty() => title,
        _ => known_title.unwrap_or_default(),
    };
    let tasks = current.map(|current| current.tasks.clone()).unwrap_or_default();
    let execution_id = current.and_then(|current| current.execution_id.clone());
    let execution_role = current.and_then(|current| current.execution_role.clone());
    let last_activity_at = current.map_or(now_ms, |current| current.last_activity_at);
    states.insert(
        session_id.clone(),
        SessionRunState {
            session_id,
            state,
            reusable_draft,
            owner_id,
            cwd,
            title,
            since,
            tasks,
            listening_ports,
            execution_id,
            execution_role,
            last_activity_at,
        },
    );
    true
}

fn apply_execution_metadata(states: &mut HashMap<String, SessionRunState>, session_id: &str, value: &Value) -> bool {
    let Some(metadata) = value.pointer("/params/_meta/alwith") else { return false };
    let execution_id = metadata.get("executionId").and_then(Value::as_str).map(String::from);
    let execution_role = metadata.get("executionRole").and_then(Value::as_str).map(String::from);
    let Some(state) = states.get_mut(session_id) else { return false };
    if state.execution_id == execution_id && state.execution_role == execution_role {
        return false;
    }
    state.execution_id = execution_id;
    state.execution_role = execution_role;
    true
}

fn apply_task_frame(states: &mut HashMap<String, SessionRunState>, value: &Value, now_ms: i64) -> bool {
    if value.get("method").and_then(Value::as_str) != Some("session/update") {
        return false;
    }
    let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) else { return false };
    let Some(update) = value.pointer("/params/update") else { return false };
    let Some(tool_call_id) = update.get("toolCallId").and_then(Value::as_str) else { return false };
    let Some(state) = states.get_mut(session_id) else { return false };
    // v2 只有 tool_call_update 一种工具帧(upsert:没见过的 toolCallId 即新建),工具名走标准
    // `name`(v1 时只能读 `_meta.claudeCode.toolName` 这条 vendor 扩展)。
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("tool_call_update") {
        return false;
    }
    let status = update.get("status").and_then(Value::as_str).unwrap_or("");
    if matches!(status, "completed" | "failed" | "cancelled") {
        let before = state.tasks.len();
        state.tasks.retain(|task| task.id != tool_call_id);
        return state.tasks.len() != before;
    }
    let tool_name = update.get("name").and_then(Value::as_str).unwrap_or("");
    if !matches!(tool_name, "Task" | "Agent") || state.tasks.iter().any(|task| task.id == tool_call_id) {
        return false;
    }
    let title = update.get("title").and_then(Value::as_str).unwrap_or(tool_name).to_string();
    let background = update.pointer("/_meta/alwith/runInBackground").and_then(Value::as_bool).unwrap_or(false);
    state.tasks.push(SessionTask { id: tool_call_id.to_string(), title, background, since: now_ms });
    true
}

/// ACP 标题帧 → run_states。**帧先记进 known_titles 再谈条目**:帧可能早于条目建立到达,
/// 那时丢掉这一帧就永远等不到第二帧(每会话只发一次)。
fn apply_title_frame(
    states: &mut HashMap<String, SessionRunState>, known_titles: &mut HashMap<String, String>, value: &Value,
) -> bool {
    if value.get("method").and_then(Value::as_str) != Some("session/update") {
        return false;
    }
    let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str) else { return false };
    let Some(update) = value.pointer("/params/update") else { return false };
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("session_info_update") {
        return false;
    }
    let Some(title) = update.get("title").and_then(Value::as_str) else { return false };
    known_titles.insert(session_id.to_string(), title.to_string());
    let Some(state) = states.get_mut(session_id) else { return false };
    if state.title == title {
        return false;
    }
    state.title = title.to_string();
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_resume_reservation_is_exclusive_and_released_on_protocol_error() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("a".into(), "main".into());
        registry.begin_agent("b".into(), "main".into());
        assert!(registry.reserve_session("a", "record").unwrap());
        assert!(registry.reserve_session("a", "record").is_err());
        assert!(registry.reserve_session("b", "record").is_err());
        registry.record_outbound(
            "a",
            r#"{"id":1,"method":"session/resume","params":{"sessionId":"record","cwd":"/p"}}"#,
            1,
        );
        registry.record_inbound_batch("a", &[r#"{"id":1,"error":{"code":-1,"message":"rejected"}}"#.into()], 2);
        assert_eq!(registry.agent_id_for_session("record"), None);
        assert!(registry.reserve_session("b", "record").unwrap());
        registry.record_outbound(
            "b",
            r#"{"id":1,"method":"session/resume","params":{"sessionId":"record","cwd":"/p"}}"#,
            3,
        );
        registry.record_inbound_batch("b", &[r#"{"id":1,"result":{}}"#.into()], 4);
        assert!(!registry.reserve_session("b", "record").unwrap());
        assert!(registry.reserve_session("a", "record").is_err());
    }

    #[test]
    fn release_execution_clears_stale_leases() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        );
        assert!(registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[],"_meta":{"alwith":{"executionId":"exec-1","executionRole":"engineering"}}}}"#,
            3,
        ));
        assert_eq!(registry.run_states()[0].execution_id.as_deref(), Some("exec-1"));

        assert!(registry.release_execution("exec-1"));
        let state = &registry.run_states()[0];
        assert_eq!(state.execution_id, None);
        assert_eq!(state.execution_role, None);
        // 已经没有这个租约:再释放一次不再报变化
        assert!(!registry.release_execution("exec-1"));
    }

    #[test]
    fn tracks_prompt_permission_and_completion() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        assert!(!registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        ));
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Idle);

        assert!(registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[]}}"#,
            3,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Running);
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-1"}}}"#.into()],
            4,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::RequiresAction);
        assert!(registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":9,"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}"#,
            5,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Running);
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":2,"result":{"stopReason":"end_turn"}}"#.into()],
            6,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Done);
    }

    /// 标题先于条目到达是常态(桌面启动就把已知标题推来、resume 的会话不再发标题帧),
    /// 记下来等条目建立时回填 —— 丢掉这一次就永远等不到第二次,手机只能显示「新会话」。
    #[test]
    fn title_pushed_before_the_session_exists_is_backfilled() {
        let mut registry = RuntimeRegistry::default();
        assert!(!registry.set_title("session-1", "改过的名字".into())); // 无条目可改 → 不广播
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        );
        assert_eq!(registry.run_states()[0].title, "改过的名字");
    }

    #[test]
    fn listening_ports_follow_only_the_current_session() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        );
        assert!(registry.set_agent_listening_ports("agent-1", vec![1420]));

        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/resume","params":{"sessionId":"session-2","cwd":"/project"}}"#,
            3,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session-2"}}"#.into()],
            4,
        );

        let states = registry
            .run_states()
            .into_iter()
            .map(|state| (state.session_id, state.listening_ports))
            .collect::<HashMap<_, _>>();
        assert_eq!(states["session-1"], Vec::<u16>::new());
        assert_eq!(states["session-2"], vec![1420]);
    }

    #[test]
    fn attach_replays_only_pending_permissions() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"sessionId":"session-1","cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[
                r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into(),
                r#"{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"sessionId":"session-1"}}"#.into(),
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk"}}}"#.into(),
            ],
            2,
        );

        let attach = registry.attach("session-1", "browser".into()).expect("active session");
        assert_eq!(attach.agent_id, "agent-1");
        assert_eq!(attach.pending_agent_requests.len(), 1);
        assert_eq!(registry.run_states()[0].owner_id, "browser");
    }

    /// agent 级联取消(`$/cancel_request`)后,那条权限请求不再是「未答」:状态回落、attach 不再重放。
    #[test]
    fn cancel_request_drops_the_pending_permission() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        );
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[]}}"#,
            3,
        );
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-1"}}}"#.into()],
            4,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::RequiresAction);

        // 数字 id 与 requestId 同一种字符串化才对得上
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":9}}"#.into()],
            5,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Running);
        let attach = registry.attach("session-1", "browser".into()).expect("active session");
        assert!(attach.pending_agent_requests.is_empty());

        // 取消一个不存在 / 已答的请求是空操作,不广播
        assert!(!registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":9}}"#.into()],
            6,
        ));
    }

    fn appended_sessions(ops: &[JournalOp]) -> Vec<&str> {
        ops.iter()
            .filter_map(|op| match op {
                JournalOp::Append { session_id, .. } => Some(session_id.as_str()),
                JournalOp::Reset { .. } => None,
            })
            .collect()
    }

    #[test]
    fn notice_is_live_only_and_does_not_change_run_state() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent".into(), "main".into());
        registry.record_outbound(
            "agent",
            r#"{"id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}"#,
            1,
        );
        registry.take_journal_ops();
        let before = serde_json::to_value(registry.run_states()).unwrap();
        let changed = registry.record_inbound_batch("agent", &[r#"{"method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"notice","severity":"warning","title":"Advisory"}}}"#.into()], 2);
        assert!(!changed);
        assert!(registry.take_journal_ops().is_empty());
        assert_eq!(serde_json::to_value(registry.run_states()).unwrap(), before);
    }

    #[test]
    fn journal_ops_are_per_session_and_reset_on_history_replay() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[]}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk"}}}"#.into(),
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-2","update":{"sessionUpdate":"agent_message_chunk"}}}"#.into(),
            ],
            2,
        );
        let ops = registry.take_journal_ops();
        assert_eq!(appended_sessions(&ops), vec!["session-1", "session-1", "session-2"]);
        assert!(matches!(ops[0], JournalOp::Append { direction: FrameDirection::Out, .. }));
        assert!(matches!(ops[1], JournalOp::Append { direction: FrameDirection::In, .. }));
        assert!(registry.take_journal_ops().is_empty(), "ops are handed over exactly once");

        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/resume","params":{"sessionId":"session-1","cwd":"/project","replayFrom":{"type":"start"}}}"#,
            3,
        );
        assert_eq!(registry.take_journal_ops(), vec![JournalOp::Reset { session_id: "session-1".into() }]);

        // 不带 replayFrom 的 resume 只恢复上下文,历史照留,也不进日志(它不是会话内容)。
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":3,"method":"session/resume","params":{"sessionId":"session-1","cwd":"/project","replayFrom":null}}"#,
            4,
        );
        assert!(registry.take_journal_ops().is_empty());
    }

    #[test]
    fn pending_permission_ids_track_answers_and_cancellations() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_inbound_batch(
            "agent-1",
            &[
                r#"{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-1"}}}"#.into(),
                r#"{"jsonrpc":"2.0","id":8,"method":"session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-2"}}}"#.into(),
            ],
            1,
        );
        let mut pending = registry.pending_permission_ids("session-1");
        pending.sort();
        assert_eq!(pending, vec!["7", "8"]);
        assert_eq!(appended_sessions(&registry.take_journal_ops()).len(), 2);

        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":7,"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}"#,
            2,
        );
        assert_eq!(registry.pending_permission_ids("session-1"), vec!["8"]);
        // 权限响应不是会话内容,不进日志。
        assert!(registry.take_journal_ops().is_empty());

        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":8,"result":{"outcome":{"outcome":"cancelled"}}}"#,
            3,
        );
        assert!(registry.pending_permission_ids("session-1").is_empty());
    }

    #[test]
    fn lifecycle_predicate_excludes_ai_text_and_keeps_tool_transitions() {
        let prompt: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}"#,
        )
        .unwrap();
        let text: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"流式文字"}}}}"#,
        )
        .unwrap();
        let tool: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call_update","toolCallId":"tool-1","status":"completed"}}}"#,
        )
        .unwrap();
        let permission: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"s"}}"#,
        )
        .unwrap();
        assert!(is_lifecycle_line(&prompt));
        assert!(!is_lifecycle_line(&text));
        assert!(is_lifecycle_line(&tool));
        assert!(is_lifecycle_line(&permission));
    }

    #[test]
    fn task_and_title_frames_survive_state_transitions() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
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
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[]}}"#,
            3,
        );
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"tool_call_update","toolCallId":"tool-1","name":"Task","title":"Audit facts","status":"pending","_meta":{"alwith":{"runInBackground":true}}}}}"#.into(),
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"session_info_update","title":"Runtime audit"}}}"#.into(),
            ],
            4,
        ));
        let state = &registry.run_states()[0];
        assert_eq!(state.title, "Runtime audit");
        assert_eq!(state.tasks.len(), 1);
        assert!(state.tasks[0].background);

        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":2,"result":{"stopReason":"end_turn"}}"#.into()],
            5,
        );
        let state = &registry.run_states()[0];
        assert_eq!(state.state, RunState::Done);
        assert_eq!(state.title, "Runtime audit");
        assert_eq!(state.tasks.len(), 1);

        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"tool_call_update","toolCallId":"tool-1","status":"completed"}}}"#.into()],
            6,
        );
        assert!(registry.run_states()[0].tasks.is_empty());
    }

    #[test]
    fn failed_turn_remains_actionable_until_a_new_turn_runs() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent".into(), "main".into());
        let frame = |state: &str, stop_reason: Option<&str>| {
            serde_json::json!({
                "jsonrpc":"2.0", "method":"session/update", "params": {
                    "sessionId":"session", "update": {
                        "sessionUpdate":"state_update", "state":state, "stopReason":stop_reason
                    }
                }
            })
            .to_string()
        };
        registry.record_inbound_batch("agent", &[frame("running", None)], 1);
        registry.record_inbound_batch("agent", &[frame("idle", Some("_error"))], 2);
        assert_eq!(registry.run_states()[0].state, RunState::RequiresAction);
        registry.mark_read("session");
        assert_eq!(registry.run_states()[0].state, RunState::RequiresAction);
        registry.record_inbound_batch("agent", &[frame("running", None)], 4);
        assert_eq!(registry.run_states()[0].state, RunState::Running);
        registry.record_inbound_batch("agent", &[frame("idle", Some("end_turn"))], 5);
        assert_eq!(registry.run_states()[0].state, RunState::Done);
    }

    /// 收尾只发生一次:agent 记录被摘掉之后再收一次是空操作,不会把别人的会话也清了。
    #[test]
    fn finishing_an_agent_twice_is_a_noop() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("agent-1".into(), "main".into());
        registry.record_outbound(
            "agent-1",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/project"}}"#,
            1,
        );
        registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"session-1"}}"#.into()],
            2,
        );
        assert_eq!(registry.run_states().len(), 1);
        assert!(registry.finish_agent("agent-1"), "收尾清掉了它的会话");
        assert!(registry.run_states().is_empty());
        assert!(!registry.finish_agent("agent-1"), "第二次没有东西可清");
    }

    #[test]
    fn concurrent_executor_and_supervisor_share_the_existing_run_state_pool() {
        let mut registry = RuntimeRegistry::default();
        registry.begin_agent("executor-agent".into(), "browser".into());
        registry.begin_agent("supervisor-agent".into(), "browser".into());

        assert!(registry.record_outbound(
            "executor-agent",
            r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"executor-session","prompt":[],"_meta":{"alwith":{"executionId":"execution-1","executionRole":"executor"}}}}"#,
            100
        ));
        assert!(registry.record_outbound(
            "supervisor-agent",
            r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"supervisor-session","prompt":[],"_meta":{"alwith":{"executionId":"execution-1","executionRole":"supervisor"}}}}"#,
            200
        ));

        let mut states = registry.run_states();
        states.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].execution_id.as_deref(), Some("execution-1"));
        assert_eq!(states[0].execution_role.as_deref(), Some("executor"));
        assert_eq!(states[0].last_activity_at, 100);
        assert_eq!(states[1].execution_id.as_deref(), Some("execution-1"));
        assert_eq!(states[1].execution_role.as_deref(), Some("supervisor"));
        assert_eq!(states[1].last_activity_at, 200);
    }
}
