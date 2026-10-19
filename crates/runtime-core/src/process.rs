// By default, spawning a child process on Windows allocates a console (conhost.exe), which shows
// up as a black window flashing.
// `CREATE_NO_WINDOW` (0x08000000) makes Windows neither create a new console nor inherit the
// parent's console.
// When that child spawns further processes, it has no console of its own, so the console
// children it spawns will not flash a window either.
//
// One trait covers both std and tokio `Command`; call sites need only a single `.hide_console()`.
// On non-windows platforms it is a no-op, so no spawn site needs `#[cfg(windows)]`.
// The single implementation lives in alwith-runtime (shared by desktop and alwith-runtime (ws));
// desktop's `platform/windows_hide.rs` only re-exports it.

use command_group::{AsyncCommandGroup, AsyncGroupChild};

/// The ACP process owns its own process group (a Job Object on Windows). On exit the whole group is
/// reclaimed, not just the adapter's main process.
pub(crate) struct AgentProcess {
    child: AsyncGroupChild,
    stop_requested: bool,
}

impl AgentProcess {
    pub(crate) fn spawn(command: &mut tokio::process::Command) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        let mut group = command.group();
        group.kill_on_drop(true);
        // command-group resets the Windows creation flags, so the no-console behavior must be kept
        // on its builder.
        #[cfg(windows)]
        group.creation_flags(CREATE_NO_WINDOW);
        let child = group.spawn()?;
        Ok(Self { child, stop_requested: false })
    }

    pub(crate) fn inner(&mut self) -> &mut tokio::process::Child {
        self.child.inner()
    }

    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// Whether a stop was asked for (`start_kill`), as opposed to the process ending on its own.
    pub(crate) fn stop_requested(&self) -> bool {
        self.stop_requested
    }

    pub(crate) fn start_kill(&mut self) -> std::io::Result<()> {
        if self.stop_requested {
            return Ok(());
        }
        if let Err(error) = self.child.start_kill() {
            // A normal exit may finish before the stop request; ESRCH clearly means the whole group
            // is gone, and other system errors are not swallowed.
            #[cfg(unix)]
            if error.raw_os_error() == Some(libc::ESRCH) {
                self.stop_requested = true;
                return Ok(());
            }
            return Err(error);
        }
        self.stop_requested = true;
        Ok(())
    }

    pub(crate) async fn stop_and_wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        // Terminate the whole group first, then reap the main process; after reaping, send no more
        // signals to a group ID that may be reused.
        self.start_kill()?;
        self.child.wait().await
    }
}

impl Drop for AgentProcess {
    fn drop(&mut self) {
        // command-group's Unix kill_on_drop only acts on the Tokio main process, so the process
        // group must be terminated explicitly.
        if let Err(error) = self.start_kill() {
            log::error!("stop ACP process group on drop: {error}");
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::AgentProcess;
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

    #[tokio::test]
    async fn stopping_an_already_reaped_group_is_a_noop() {
        let mut command = tokio::process::Command::new("sh");
        command.arg("-c").arg("exit 0");
        let mut child = AgentProcess::spawn(&mut command).unwrap();
        child.stop_and_wait().await.unwrap();
        child.start_kill().unwrap();
        child.start_kill().unwrap();
    }

    #[tokio::test]
    async fn stopping_or_dropping_agent_closes_descendant_pipes() {
        for drop_process in [false, true] {
            let mut command = tokio::process::Command::new("sh");
            command
                .arg("-c")
                .arg("(printf 'ready\\n'; exec sleep 60) & wait")
                .stdout(Stdio::piped())
                .kill_on_drop(true);
            let mut child = AgentProcess::spawn(&mut command).unwrap();
            let mut stdout = BufReader::new(child.inner().stdout.take().unwrap());
            let mut ready = String::new();
            tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut ready)).await.unwrap().unwrap();
            assert_eq!(ready, "ready\n");
            if drop_process {
                drop(child);
            } else {
                child.start_kill().unwrap();
                child.start_kill().unwrap();
                tokio::time::timeout(Duration::from_secs(5), child.stop_and_wait()).await.unwrap().unwrap();
            }
            let mut remainder = String::new();
            // This hangs if the main process terminates while descendants stay alive; main-process
            // exit alone must not count as passing evidence.
            tokio::time::timeout(Duration::from_secs(5), stdout.read_to_string(&mut remainder)).await.unwrap().unwrap();
            assert!(remainder.is_empty());
        }
    }
}

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

pub trait HideConsoleExt {
    fn hide_console(&mut self) -> &mut Self;
}

impl HideConsoleExt for std::process::Command {
    fn hide_console(&mut self) -> &mut Self {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            self.creation_flags(CREATE_NO_WINDOW);
        }
        self
    }
}

impl HideConsoleExt for tokio::process::Command {
    fn hide_console(&mut self) -> &mut Self {
        #[cfg(windows)]
        {
            self.creation_flags(CREATE_NO_WINDOW);
        }
        self
    }
}
