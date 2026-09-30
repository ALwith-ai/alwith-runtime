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

/// Lifecycle frames: prompt, permission requests, tool state transitions. Carries no AI body text;
/// lets the observer and cross-window takeover rebuild the fact stream.
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
    /// Native resume keeps the identity before it is sent to the engine; after the response it
    /// becomes a session or is released.
    reserved_sessions: HashSet<String>,
    prompt_inflight: HashMap<String, u32>,
    used_sessions: HashSet<String>,
    /// State reported by the agent via `state_update` frames, latest one per sessionId.
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

    /// Current session state.
    ///
    /// Since ACP v2 the agent reports it directly through `state_update` frames (running / idle /
    /// requires_action), and this takes priority here --
    /// deriving it from traffic ourselves misjudges cases such as "background activity keeps
    /// sending updates while idle" (the protocol explicitly says this does not change the
    /// state, whereas count-based derivation would treat it as running again). Sessions no frame
    /// has arrived for yet (just created, no prompt sent) fall back to counting.
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
    /// sessionId -> known session title. Two sources: the "title currently shown" pushed by the
    /// desktop frontend (including manual renames and
    /// startup restore) and ACP title frames. **Must be stored separately from run_states**: a
    /// title frame is sent only once per session, in the prompt flow,
    /// a resumed session receives no frame before its next message, and a title may also arrive
    /// before its run_states entry.
    /// It is back-filled from here when the entry is created, otherwise mobile could only show "New
    /// session". Session teardown does not clear this table --
    /// the same sessionId will be resumed later, and clearing it would leave it untitled again.
    known_titles: HashMap<String, String>,
    inbound_replay: HashMap<String, VecDeque<String>>,
    /// Journal actions decided while processing frames; the runtime takes them with
    /// `take_journal_ops` after the same batch and persists them.
    journal_ops: Vec<JournalOp>,
}

impl RuntimeRegistry {
    /// Open an agent record. There can be no existing record for `agent_id` -- `Runtime::start`
    /// rejects outright while the process is alive,
    /// so two processes with the same name do not exist. The return value is the sessions cleared
    /// as a side effect when this record was created (in theory empty).
    pub fn begin_agent(&mut self, agent_id: String, owner_id: String) -> Vec<String> {
        self.inbound_replay.remove(&agent_id);
        let previous = self.tracks.insert(agent_id, AgentTrack::new(owner_id));
        let removed_sessions = previous.map_or_else(Vec::new, |track| track.sessions);
        self.remove_sessions(&removed_sessions);
        removed_sessions
    }

    /// Returns whether the run state changed (whether to broadcast); the journal actions decided
    /// for this batch go into `journal_ops`.
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

    /// Take the journal actions waiting to be persisted. The runtime takes them once per batch of
    /// frames, in frame order.
    pub fn take_journal_ops(&mut self) -> Vec<JournalOp> {
        std::mem::take(&mut self.journal_ops)
    }

    /// ACP v2 `state_update` frame: the agent reports session state directly (running / idle /
    /// requires_action).
    ///
    /// Recorded into the track, then goes through the same refresh_session -- `Done` synthesis,
    /// dedup and broadcast are untouched, because "finished but unseen"
    /// is our product state, which the protocol does not express. Unrecognized values (the protocol
    /// leaves state an extension point) are logged as an error and then ignored:
    /// silently treating them as idle would make a running session look finished.
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
        Some(AttachInfo {
            agent_id,
            engine: String::new(),
            provider: String::new(),
            provider_revision: None,
            pending_agent_requests,
        })
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

    /// Explicit release of an execution lease: once execution ends, clear the run state of any
    /// session still carrying that executionId.
    /// What happens if it is not cleared: the read-only observer banner hangs on an execution that
    /// ended long ago (2026-07-30 audit batch 2 #2).
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

    /// Record this session's title. **Write to known_titles first, then worry about the entry**:
    /// having no entry is not an anomaly but the norm
    /// (before resume, title frame arrived first); record it and back-fill when the entry is
    /// created. The return value only answers "whether to broadcast" --
    /// that is needed only when an entry currently sitting in run_states was changed.
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

    /// Record claim: before session/resume is sent to the engine, claim the record identity;
    /// different engines or different processes cannot resume the same record at once;
    /// the same process re-attaching to a session it already activated returns `Ok(false)`.
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

    /// Request ids (raw JSON form, e.g. `9` or `"abc"`) of this session's
    /// `session/request_permission` requests that are still unanswered.
    /// When replaying the journal keep only these requests: an answered one must not be replayed
    /// either, or it would be answered twice.
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

    /// Decide whether this frame enters the session journal, and refresh `lastActivityAt`.
    /// Persisting is done by the runtime.
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
            // The fork's params.sessionId is the source session; the new identity can only come
            // from the response.
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

/// `\$/cancel_request` (a notification with no id): the agent cancels a request it issued itself --
/// on `session/cancel` it cascades to cancel
/// pending permissions / questionnaires (spec Cancellation page). The corresponding
/// request_permission no longer waits for an answer: if not removed,
/// attach would replay it as an unanswered permission, a zombie dialog, and a click would emit a
/// running frame with no turn.
/// requestId is stringified the same way as the id at registration time (JSON value `to_string` as
/// is), so numeric / string ids both match.
/// Returns the session owning the withdrawn permission; an unrecognized requestId (already answered
/// / not a permission) is a no-op.
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
    // If the entry has no title yet, back-fill from the known-titles table -- a resumed session
    // sends no more title frames, and without back-fill
    // mobile could only show "New session". An entry that already has a title is left alone: that
    // is the newer one.
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
    // v2 has only one tool frame, tool_call_update (upsert: an unseen toolCallId means new); the
    // tool name comes from the standard
    // `name` (in v1 it could only be read from the `_meta.claudeCode.toolName` vendor extension).
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

/// ACP title frame -> run_states. **Record the frame in known_titles first, then worry about the
/// entry**: the frame may arrive before the entry is created,
/// and dropping it then means the second frame never comes (sent only once per session).
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
        // This lease is already gone: releasing again reports no change
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

    /// A title arriving before its entry is the norm (desktop pushes known titles at startup, a
    /// resumed session sends no title frame),
    /// record it and back-fill when the entry is created -- dropping this one means the second
    /// never comes, and the phone could only show "New session".
    #[test]
    fn title_pushed_before_the_session_exists_is_backfilled() {
        let mut registry = RuntimeRegistry::default();
        assert!(!registry.set_title("session-1", "改过的名字".into())); // no entry to change -> no broadcast
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

    /// After an agent cascade cancel (`\$/cancel_request`), that permission request is no longer
    /// "unanswered": state falls back and attach no longer replays it.
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

        // A numeric id matches requestId only when stringified the same way
        assert!(registry.record_inbound_batch(
            "agent-1",
            &[r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":9}}"#.into()],
            5,
        ));
        assert_eq!(registry.run_states()[0].state, RunState::Running);
        let attach = registry.attach("session-1", "browser".into()).expect("active session");
        assert!(attach.pending_agent_requests.is_empty());

        // Cancelling a nonexistent / already-answered request is a no-op, no broadcast
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

        // A resume without replayFrom only restores context; history stays, and it does not enter
        // the journal (it is not session content).
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
        // A permission response is not session content and does not enter the journal.
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

    /// Teardown happens once: after the agent record is removed, a second teardown is a no-op and
    /// does not clear anyone else's sessions.
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
        assert!(registry.finish_agent("agent-1"), "teardown cleared its sessions");
        assert!(registry.run_states().is_empty());
        assert!(!registry.finish_agent("agent-1"), "the second time there is nothing to clear");
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
