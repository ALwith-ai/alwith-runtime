//! 会话事件日志:每个会话一个追加写的 jsonl,每帧带单调递增的 `sequence`。
//!
//! 它是 Runtime 里**唯一**的会话帧留存点:晚加入的端按 `sequence` 补齐、观察面读生命周期、
//! 移动端 attach 时回放,全部从这里读;注册表只保留状态机与未答权限的 id,不再各留一份
//! 环形缓冲。Runtime 进程退出、agent 收尾之后文件都还在,重启后可继续查。
//!
//! 它**不是会话记录**:记录归引擎自写(`docs/architecture/runtime.md`「Runtime 不写记录」),
//! 这里存的是 Runtime 经手的 ACP 帧原文,供观察与补齐,丢了只丢观察面,不丢会话。

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use alwith_api::{FrameDirection, SessionEventFrame};

/// 单个会话日志的字节上限。超过就只保留后半段(sequence 连续性不受影响,只是最早的帧没了)。
pub const SESSION_JOURNAL_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// 注册表判定出的日志动作;由 runtime 落盘,注册表自己不做 IO。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalOp {
    Append {
        session_id: String,
        direction: FrameDirection,
        line: String,
    },
    /// 引擎将重放历史(带 `replayFrom` 的 resume):旧帧作废,从头记。
    Reset {
        session_id: String,
    },
}

struct OpenSession {
    writer: BufWriter<File>,
    next_sequence: u64,
    bytes: u64,
}

pub struct Journal {
    root: PathBuf,
    sessions: Mutex<HashMap<String, OpenSession>>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Journal").field("root", &self.root).finish_non_exhaustive()
    }
}

fn validate_session_id(session_id: &str) -> Result<(), String> {
    if session_id.is_empty() || session_id.contains(['/', '\\']) || session_id == "." || session_id == ".." {
        return Err(format!("invalid session id for journal: {session_id:?}"));
    }
    Ok(())
}

impl Journal {
    /// 打开(必要时创建)日志根目录。文件按需打开,目录里可以已有上次留下的日志。
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| format!("create journal dir {}: {error}", root.display()))?;
        Ok(Self { root, sessions: Mutex::new(HashMap::new()) })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, session_id: &str) -> PathBuf {
        self.root.join(format!("{session_id}.jsonl"))
    }

    fn open_session(&self, session_id: &str) -> Result<OpenSession, String> {
        validate_session_id(session_id)?;
        let path = self.path(session_id);
        let next_sequence = match File::open(&path) {
            Ok(file) => last_sequence(BufReader::new(file))?.map_or(0, |sequence| sequence + 1),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(format!("open journal {}: {error}", path.display())),
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| format!("open journal {}: {error}", path.display()))?;
        let bytes = file.metadata().map_err(|error| error.to_string())?.len();
        Ok(OpenSession { writer: BufWriter::new(file), next_sequence, bytes })
    }

    /// 应用一批注册表给出的动作,返回追加成功的帧(带分配好的 sequence)。整批结束后 flush 一次。
    pub fn apply(&self, ops: &[JournalOp], recorded_at: i64) -> Result<Vec<SessionEventFrame>, String> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let mut sessions = self.sessions.lock().map_err(|error| error.to_string())?;
        let mut appended = Vec::new();
        let mut touched = Vec::new();
        for op in ops {
            match op {
                JournalOp::Reset { session_id } => {
                    validate_session_id(session_id)?;
                    sessions.remove(session_id);
                    let path = self.path(session_id);
                    match fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(format!("reset journal {}: {error}", path.display())),
                    }
                }
                JournalOp::Append { session_id, direction, line } => {
                    if !sessions.contains_key(session_id) {
                        sessions.insert(session_id.clone(), self.open_session(session_id)?);
                    }
                    let open = sessions.get_mut(session_id).expect("session opened above");
                    let frame = SessionEventFrame {
                        sequence: open.next_sequence,
                        recorded_at,
                        direction: *direction,
                        line: line.clone(),
                    };
                    let mut encoded = serde_json::to_string(&frame).map_err(|error| error.to_string())?;
                    encoded.push('\n');
                    open.writer.write_all(encoded.as_bytes()).map_err(|error| format!("append journal: {error}"))?;
                    open.next_sequence += 1;
                    open.bytes += encoded.len() as u64;
                    appended.push(frame);
                    if !touched.iter().any(|touched: &String| touched == session_id) {
                        touched.push(session_id.clone());
                    }
                }
            }
        }
        for session_id in touched {
            let open = sessions.get_mut(&session_id).expect("touched session is open");
            open.writer.flush().map_err(|error| format!("flush journal: {error}"))?;
            if open.bytes > SESSION_JOURNAL_MAX_BYTES {
                let compacted = self.compact(&session_id)?;
                sessions.insert(session_id, compacted);
            }
        }
        Ok(appended)
    }

    /// 超限时只保留后半段:重写文件,sequence 原样保留。
    fn compact(&self, session_id: &str) -> Result<OpenSession, String> {
        let path = self.path(session_id);
        let frames = read_frames(&path, None, usize::MAX)?;
        let keep = frames.len() / 2;
        let temporary = self.root.join(format!("{session_id}.jsonl.tmp"));
        {
            let mut writer = BufWriter::new(
                File::create(&temporary).map_err(|error| format!("compact journal {}: {error}", path.display()))?,
            );
            for frame in &frames[keep..] {
                serde_json::to_writer(&mut writer, frame).map_err(|error| error.to_string())?;
                writer.write_all(b"\n").map_err(|error| error.to_string())?;
            }
            writer.flush().map_err(|error| error.to_string())?;
        }
        fs::rename(&temporary, &path).map_err(|error| format!("compact journal {}: {error}", path.display()))?;
        self.open_session(session_id)
    }

    /// `after_sequence` 之后的帧(不含),最多 `limit` 条。`None` 从头读。
    pub fn read_since(
        &self, session_id: &str, after_sequence: Option<u64>, limit: usize,
    ) -> Result<Vec<SessionEventFrame>, String> {
        validate_session_id(session_id)?;
        // 持锁读:保证读到的是已经 flush 的全部帧,不会与正在写的批次交错。
        let _sessions = self.sessions.lock().map_err(|error| error.to_string())?;
        read_frames(&self.path(session_id), after_sequence, limit)
    }

    /// 已有日志的会话 id 列表(重启后观察面据此列出还能查的会话)。
    pub fn sessions(&self) -> Result<Vec<String>, String> {
        let mut ids = Vec::new();
        for entry in
            fs::read_dir(&self.root).map_err(|error| format!("list journal {}: {error}", self.root.display()))?
        {
            let entry = entry.map_err(|error| error.to_string())?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name.strip_suffix(".jsonl") {
                ids.push(id.to_string());
            }
        }
        ids.sort();
        Ok(ids)
    }
}

fn read_frames(path: &Path, after_sequence: Option<u64>, limit: usize) -> Result<Vec<SessionEventFrame>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("read journal {}: {error}", path.display())),
    };
    let mut frames = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| format!("read journal {}: {error}", path.display()))?;
        if line.is_empty() {
            continue;
        }
        let frame: SessionEventFrame = serde_json::from_str(&line)
            .map_err(|error| format!("corrupt journal frame in {}: {error}", path.display()))?;
        if after_sequence.is_some_and(|after| frame.sequence <= after) {
            continue;
        }
        frames.push(frame);
        if frames.len() >= limit {
            break;
        }
    }
    Ok(frames)
}

/// 文件末尾那一帧的 sequence。只读最后 64KB,大文件不用整个扫。
fn last_sequence(mut reader: BufReader<File>) -> Result<Option<u64>, String> {
    let length = reader.get_ref().metadata().map_err(|error| error.to_string())?.len();
    let start = length.saturating_sub(64 * 1024);
    reader.seek(SeekFrom::Start(start)).map_err(|error| error.to_string())?;
    let mut last: Option<u64> = None;
    let mut lines = reader.lines();
    if start > 0 {
        // 从中间起读,第一行多半是半截,丢掉。
        lines.next();
    }
    for line in lines {
        let line = line.map_err(|error| error.to_string())?;
        if line.is_empty() {
            continue;
        }
        let frame: SessionEventFrame =
            serde_json::from_str(&line).map_err(|error| format!("corrupt journal frame: {error}"))?;
        last = Some(frame.sequence);
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(session_id: &str, line: &str) -> JournalOp {
        JournalOp::Append { session_id: session_id.into(), direction: FrameDirection::In, line: line.into() }
    }

    #[test]
    fn sequences_are_monotonic_per_session_and_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::open(directory.path()).unwrap();
        let frames = journal.apply(&[append("a", "1"), append("b", "1"), append("a", "2")], 10).unwrap();
        assert_eq!(frames.iter().map(|frame| frame.sequence).collect::<Vec<_>>(), vec![0, 0, 1]);

        let reopened = Journal::open(directory.path()).unwrap();
        let frames = reopened.apply(&[append("a", "3")], 11).unwrap();
        assert_eq!(frames[0].sequence, 2);
        let all = reopened.read_since("a", None, usize::MAX).unwrap();
        assert_eq!(all.iter().map(|frame| frame.line.as_str()).collect::<Vec<_>>(), vec!["1", "2", "3"]);
        assert_eq!(reopened.read_since("a", Some(1), usize::MAX).unwrap().len(), 1);
        assert_eq!(reopened.read_since("a", None, 2).unwrap().len(), 2);
        assert_eq!(reopened.sessions().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn reset_drops_the_session_and_restarts_sequences() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::open(directory.path()).unwrap();
        journal.apply(&[append("a", "1"), append("a", "2")], 1).unwrap();
        journal.apply(&[JournalOp::Reset { session_id: "a".into() }], 2).unwrap();
        assert!(journal.read_since("a", None, usize::MAX).unwrap().is_empty());
        let frames = journal.apply(&[append("a", "3")], 3).unwrap();
        assert_eq!(frames[0].sequence, 0);
    }

    #[test]
    fn oversized_journals_keep_the_newer_half() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::open(directory.path()).unwrap();
        let line = "x".repeat(1024);
        let ops: Vec<JournalOp> =
            (0..(SESSION_JOURNAL_MAX_BYTES / 1024 + 64) as usize).map(|_| append("a", &line)).collect();
        journal.apply(&ops, 1).unwrap();
        let frames = journal.read_since("a", None, usize::MAX).unwrap();
        assert!(frames.len() < ops.len());
        assert!(frames.len() >= ops.len() / 2 - 1);
        assert_eq!(frames.last().unwrap().sequence, ops.len() as u64 - 1);
        let next = journal.apply(&[append("a", "tail")], 2).unwrap();
        assert_eq!(next[0].sequence, ops.len() as u64);
    }

    #[test]
    fn rejects_path_like_session_ids() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::open(directory.path()).unwrap();
        assert!(journal.apply(&[append("../escape", "1")], 1).is_err());
        assert!(journal.read_since("", None, 1).is_err());
    }
}
