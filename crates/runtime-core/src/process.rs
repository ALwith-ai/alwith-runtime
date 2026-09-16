// Windows 下 spawn 子进程默认会分配一个 console（conhost.exe），表现为黑框一闪。
// `CREATE_NO_WINDOW` (0x08000000) 让 Windows 既不创建新 console 也不继承父级 console。
// 子进程内部再 spawn 时，由于自身没有 console，再 spawn console 子进程也不会闪窗。
//
// 同一个 trait 同时覆盖 std + tokio Command，调用点只需 `.hide_console()` 一行。
// non-windows 平台是 no-op，避免在每个 spawn 点写 `#[cfg(windows)]`。
// 唯一实现下沉在 alwith-runtime（desktop 与 alwith-runtime (ws) 双面共用），desktop 的
// `platform/windows_hide.rs` 只做 re-export。

use command_group::{AsyncCommandGroup, AsyncGroupChild};

/// ACP 进程拥有自己的进程组（Windows 为 Job Object）。退出时收回整组，而非仅适配器主进程。
pub(crate) struct AgentProcess {
    child: AsyncGroupChild,
    stop_requested: bool,
}

impl AgentProcess {
    pub(crate) fn spawn(command: &mut tokio::process::Command) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        let mut group = command.group();
        group.kill_on_drop(true);
        // command-group 会重设 Windows creation flags，必须在它的 builder 上保留不弹 console 的行为。
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

    pub(crate) fn start_kill(&mut self) -> std::io::Result<()> {
        if self.stop_requested {
            return Ok(());
        }
        if let Err(error) = self.child.start_kill() {
            // 正常退出可能先于停止请求完成；ESRCH 明确表示整组已不存在，不吞其它系统错误。
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
        // 先终止整组，再回收主进程；回收后不再向可能被复用的组 ID 发信号。
        self.start_kill()?;
        self.child.wait().await
    }
}

impl Drop for AgentProcess {
    fn drop(&mut self) {
        // command-group 的 Unix kill_on_drop 仅作用于 Tokio 主进程，必须显式终止进程组。
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
            // 主进程终止而后代仍存活时，这里会卡住，不能仅用主进程退出作为通过证据。
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
