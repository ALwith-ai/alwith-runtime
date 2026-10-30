//! What the Runtime still answers for after it stops: the attention state of every session it
//! held (`run-states.json`) and the agent processes it started (`agents.json`). Both live in the
//! platform data directory, written atomically (temp file + rename).
//!
//! On start the previous Runtime's leftovers are settled first (`docs/plans/2026-10-19-session-lifecycle.md`
//! 1.5): its agent processes are reclaimed, because nothing can reach them any more (their stdio
//! died with it), and its sessions come back as they were, except that a session caught `running`
//! was cut short and now needs the user, with the reason on it. Nothing is lowered to `idle`: that
//! would hide an interruption behind a green light. `done` stays unread, `requires_action` stays.
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use alwith_runtime_protocol::{RunState, SessionRunState};
use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessesToUpdate, System};

/// The reason put on a session the Runtime found `running` when it came back.
pub const RUNTIME_RESTARTED: &str = "runtime restarted";

const FORMAT: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunStatesFile {
    format: u32,
    sessions: Vec<SessionRunState>,
}

/// An agent process this Runtime started and has not yet reaped: its pid and its start time
/// (seconds since the epoch, as the OS reports it), so a pid reused by a stranger is never mistaken
/// for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessLease {
    pub agent_id: String,
    pub process_id: u32,
    pub started_at: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeasesFile {
    format: u32,
    agents: Vec<ProcessLease>,
}

pub struct AttentionStore {
    root: PathBuf,
}

impl std::fmt::Debug for AttentionStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("AttentionStore").field("root", &self.root).finish()
    }
}

impl AttentionStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| format!("create attention dir {}: {error}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn run_states_path(&self) -> PathBuf {
        self.root.join("run-states.json")
    }

    fn leases_path(&self) -> PathBuf {
        self.root.join("agents.json")
    }

    pub fn write_run_states(&self, sessions: &[SessionRunState]) -> Result<(), String> {
        write_json(&self.run_states_path(), &RunStatesFile { format: FORMAT, sessions: sessions.to_vec() })
    }

    /// The sessions the previous Runtime published last; empty when it never wrote any.
    pub fn read_run_states(&self) -> Result<Vec<SessionRunState>, String> {
        Ok(read_json::<RunStatesFile>(&self.run_states_path())?.map_or_else(Vec::new, |file| file.sessions))
    }

    pub fn write_leases(&self, agents: &[ProcessLease]) -> Result<(), String> {
        write_json(&self.leases_path(), &LeasesFile { format: FORMAT, agents: agents.to_vec() })
    }

    pub fn read_leases(&self) -> Result<Vec<ProcessLease>, String> {
        Ok(read_json::<LeasesFile>(&self.leases_path())?.map_or_else(Vec::new, |file| file.agents))
    }
}

// Lock a stable sidecar: replacing the JSON changes its file identity. Never unlink the
// lock file, so separate handles and processes coordinate on the same OS lock. Readers
// participate too, keeping Windows destination handles closed during replacement.
fn lock_json(path: &Path) -> Result<fs::File, String> {
    let lock_path = path.with_extension("json.lock");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| format!("open attention lock {}: {error}", lock_path.display()))?;
    file.lock().map_err(|error| format!("lock {}: {error}", lock_path.display()))?;
    Ok(file)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let _lock = lock_json(path)?;
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let parent = path.parent().ok_or_else(|| format!("attention path has no parent: {}", path.display()))?;
    // Each writer owns its temporary file, including across distinct store instances.
    // Keep it beside the destination so persistence is an atomic same-filesystem rename.
    let mut file = tempfile::Builder::new()
        .prefix(".attention-")
        .tempfile_in(parent)
        .map_err(|error| format!("create temporary file for {}: {error}", path.display()))?;
    file.write_all(&bytes).map_err(|error| format!("write {}: {error}", path.display()))?;
    file.as_file().sync_all().map_err(|error| format!("sync {}: {error}", path.display()))?;
    file.persist(path).map_err(|error| format!("replace {}: {error}", path.display()))?;
    Ok(())
}

/// `None` when the file does not exist. Anything else that fails to read or parse is an error
/// naming the file: the Runtime wrote it itself, so that is corruption to look at, not to skip.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, String> {
    let _lock = lock_json(path)?;
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|error| format!("parse {}: {error}", path.display()))
}

/// The state a persisted session comes back in once the Runtime that held it is gone. The agent
/// went with that Runtime, so a running turn was cut short: the session needs the user and says
/// why. Owner, tasks, ports and the execution lease all belonged to the dead process and are
/// cleared; the state itself is otherwise kept as it was.
pub fn recovered(mut state: SessionRunState, now_ms: i64) -> SessionRunState {
    if state.state == RunState::Running {
        state.state = RunState::RequiresAction;
        state.since = now_ms;
        state.interruption = Some(RUNTIME_RESTARTED.to_string());
    }
    state.owner_id.clear();
    state.tasks.clear();
    state.listening_ports.clear();
    state.execution_id = None;
    state.execution_role = None;
    state
}

/// The lease for a process this Runtime just started; `None` once the process is already gone.
pub fn lease_for(agent_id: &str, process_id: u32) -> Option<ProcessLease> {
    let pid = Pid::from_u32(process_id);
    let mut system = System::new().expect("failed to initialize system process information");
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    let process = system.process(pid)?;
    Some(ProcessLease { agent_id: agent_id.to_string(), process_id, started_at: process.start_time() })
}

/// Kill the leased processes that are still alive, with their descendants (an agent's MCP servers
/// and shells), and return the leases that named a living process. A pid that now belongs to a
/// process with a different start time is someone else's and is left alone.
pub fn reclaim_orphans(leases: &[ProcessLease]) -> Vec<ProcessLease> {
    if leases.is_empty() {
        return Vec::new();
    }
    let mut system = System::new().expect("failed to initialize system process information");
    system.refresh_processes(ProcessesToUpdate::All, false);
    let alive = leases
        .iter()
        .filter(|lease| {
            system
                .process(Pid::from_u32(lease.process_id))
                .is_some_and(|process| process.start_time() == lease.started_at)
        })
        .cloned()
        .collect::<Vec<_>>();
    let roots = alive.iter().map(|lease| lease.process_id).collect::<Vec<_>>();
    let mut victims = HashSet::new();
    for descendants in crate::port_scanner::descendant_process_ids_by_root(&roots).into_values() {
        victims.extend(descendants);
    }
    for process_id in victims {
        if let Some(process) = system.process(Pid::from_u32(process_id)) {
            process.kill();
        }
    }
    alive
}

#[cfg(test)]
mod tests {
    use super::*;
    use alwith_runtime_protocol::SessionTask;

    fn session(state: RunState) -> SessionRunState {
        SessionRunState {
            session_id: "session".into(),
            state,
            reusable_draft: false,
            owner_id: "window".into(),
            cwd: "/work".into(),
            title: "Title".into(),
            since: 10,
            tasks: vec![SessionTask { id: "task".into(), title: "sub".into(), background: false, since: 11 }],
            listening_ports: vec![3000],
            execution_id: Some("execution".into()),
            execution_role: Some("executor".into()),
            last_activity_at: 12,
            version: 7,
            interruption: None,
        }
    }

    #[test]
    fn run_states_round_trip_through_the_file() {
        let directory = tempfile::tempdir().unwrap();
        let store = AttentionStore::open(directory.path()).unwrap();
        assert!(store.read_run_states().unwrap().is_empty());
        store.write_run_states(&[session(RunState::Done)]).unwrap();
        assert_eq!(store.read_run_states().unwrap(), vec![session(RunState::Done)]);
        store.write_run_states(&[]).unwrap();
        assert!(store.read_run_states().unwrap().is_empty());
        assert!(!directory.path().join("run-states.json.tmp").exists());
    }

    #[test]
    fn concurrent_writers_use_independent_temporary_files() {
        let directory = tempfile::tempdir().unwrap();
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let root = directory.path();
                let barrier = &barrier;
                scope.spawn(move || {
                    // Separate handles reproduce collisions beyond a single store object.
                    let store = AttentionStore::open(root).unwrap();
                    barrier.wait();
                    for iteration in 0..32 {
                        let mut state = session(RunState::Done);
                        state.title = format!("{worker}:{iteration}").repeat(256);
                        store.write_run_states(&[state]).unwrap();
                        assert_eq!(store.read_run_states().unwrap().len(), 1);
                        store
                            .write_leases(&[ProcessLease {
                                agent_id: format!("agent-{worker}"),
                                process_id: worker + 1,
                                started_at: 1,
                            }])
                            .unwrap();
                        assert_eq!(store.read_leases().unwrap().len(), 1);
                    }
                });
            }
        });
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 4, "only data and stable lock files remain");
    }

    #[test]
    fn failed_replace_removes_its_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let store = AttentionStore::open(directory.path()).unwrap();
        fs::create_dir(directory.path().join("run-states.json")).unwrap();
        assert!(store.write_run_states(&[]).unwrap_err().contains("replace"));
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    }

    #[test]
    fn a_corrupt_file_is_an_error_naming_it() {
        let directory = tempfile::tempdir().unwrap();
        let store = AttentionStore::open(directory.path()).unwrap();
        fs::write(directory.path().join("agents.json"), b"{").unwrap();
        let error = store.read_leases().unwrap_err();
        assert!(error.contains("agents.json"), "{error}");
    }

    #[test]
    fn recovery_interrupts_only_what_was_running() {
        let running = recovered(session(RunState::Running), 99);
        assert_eq!(running.state, RunState::RequiresAction);
        assert_eq!(running.interruption.as_deref(), Some(RUNTIME_RESTARTED));
        assert_eq!(running.since, 99);
        assert_eq!(running.owner_id, "");
        assert!(running.tasks.is_empty());
        assert!(running.listening_ports.is_empty());
        assert_eq!(running.execution_id, None);
        assert_eq!(running.version, 7, "the version moves when the registry publishes, not here");
        for state in [RunState::RequiresAction, RunState::Done, RunState::Idle] {
            let kept = recovered(session(state), 99);
            assert_eq!(kept.state, state);
            assert_eq!(kept.interruption, None);
            assert_eq!(kept.since, 10);
        }
    }

    #[test]
    fn a_lease_names_a_living_process_and_a_dead_pid_is_not_reclaimed() {
        let own = std::process::id();
        let lease = lease_for("self", own).expect("this process is alive");
        assert_eq!(lease.process_id, own);
        let stale = ProcessLease { started_at: lease.started_at.wrapping_add(1), ..lease.clone() };
        assert!(reclaim_orphans(&[stale]).is_empty(), "a different start time is a different process");
        let gone = ProcessLease { agent_id: "gone".into(), process_id: u32::MAX - 1, started_at: 0 };
        assert!(reclaim_orphans(&[gone]).is_empty());
    }
}
