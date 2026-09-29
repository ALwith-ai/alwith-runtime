//! Runtime 的 stdio 入口。宿主 spawn `alwith-runtime`,stdin 进一行一帧的请求,stdout 出响应与事件,
//! 帧的形状与 `alwith-runtime-ws` 完全相同(请求 `{id, method, params}`,响应 `{id, result | error}`,
//! 事件带 `type`)。stdout 上只有协议,日志一律走 stderr。
//!
//! 与 ws 的差别只有三处:
//! - 一个进程一条连接,属主固定为 `stdio`,不做 token 握手——管道本身就是授权。
//! - stdin 到 EOF 视为宿主离开:停扩展、`kill_all`、返回。
//! - 事件队列有界。ws 上慢连接会被断开、由客户端按 sequence 补齐;stdio 断不了,所以队列满时
//!   丢弃事件并计数,队列腾出后先补一条 `{"type":"lagged","dropped":n}`,客户端据此用
//!   `sessionEvents` 回放。审批与错误不会静默消失:它们都在会话日志里。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alwith_runtime_core::{RuntimeEvent, RuntimeEventSink, protocol_lines};
use alwith_runtime_ws::{RuntimeServer, SHUTDOWN_FORCE_WINDOW, SharedConnectionState, process_request_shared};
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

pub const OWNER_ID: &str = "stdio";
pub const EVENT_QUEUE_CAPACITY: usize = 8_192;
/// 收尾后给写端排空队列的窗口;宿主已经不读了就到点放弃。
pub const WRITER_DRAIN_WINDOW: std::time::Duration = std::time::Duration::from_millis(500);

struct QueueEventSink {
    sender: mpsc::Sender<String>,
    dropped: Arc<AtomicU64>,
}

impl RuntimeEventSink for QueueEventSink {
    fn publish(&self, event: RuntimeEvent) {
        let payload = serde_json::to_string(&event).expect("serialize runtime event");
        match self.sender.try_send(payload) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// 在任意一对读写流上跑一条 Runtime 连接;`serve_stdio` 把它接到进程的 stdin / stdout。
///
/// 主循环只读不写:写由独立任务做,stdout 没人读(宿主卡住)也堵不住 `shutdown`、断开与 stdin 的处理。
/// 宿主主动走有两条路,效果相同:stdin EOF,或发一条 `{"id":n,"method":"shutdown"}`(先收到回执,再收尾)
/// —— 起子进程的宿主不一定能关自己那头的 stdin(Tauri 的 sidecar 就不能),所以给一个方法。
/// 返回时所有 agent 已被 `kill_all`。
pub async fn serve<R, W>(
    server: RuntimeServer, input: R, output: W, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (event_sender, event_receiver) = mpsc::channel::<String>(EVENT_QUEUE_CAPACITY);
    let dropped = Arc::new(AtomicU64::new(0));
    let sink: Arc<dyn RuntimeEventSink> =
        Arc::new(QueueEventSink { sender: event_sender.clone(), dropped: Arc::clone(&dropped) });
    let sink_id = server.runtime.add_sink(&sink);
    let disconnect = Arc::new(tokio::sync::Notify::new());
    let connection: SharedConnectionState =
        Arc::new(server.extension.connect(OWNER_ID, event_sender.clone(), Arc::clone(&disconnect)));
    let write_failed = Arc::new(tokio::sync::Notify::new());
    let mut writer = tokio::spawn(write_loop(output, event_receiver, Arc::clone(&dropped), Arc::clone(&write_failed)));
    let mut lines = protocol_lines(input);
    tokio::pin!(shutdown);

    let result = loop {
        tokio::select! {
            () = &mut shutdown => break Ok(()),
            () = disconnect.notified() => break Ok(()),
            () = write_failed.notified() => break Err(std::io::Error::other("stdout closed")),
            line = lines.next_line() => {
                let line = match line {
                    Ok(Some(line)) => line,
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(error),
                };
                if line.trim().is_empty() { continue }
                if let Some(id) = shutdown_request_id(&line) {
                    // 回执排进队列(写端会送出),然后走与 stdin EOF 相同的收尾。
                    let receipt = serde_json::json!({ "id": id, "result": null, "error": null }).to_string();
                    let _ = event_sender.send(receipt).await;
                    break Ok(());
                }
                // 请求并发处理(同 ws):响应经同一条事件队列回到写端。用 send 而不是 try_send,响应不许丢。
                let server = server.clone();
                let connection = Arc::clone(&connection);
                let out = event_sender.clone();
                tokio::spawn(async move {
                    let response = process_request_shared(&server, OWNER_ID, &connection, &line).await;
                    let payload = serde_json::to_string(&response).expect("serialize runtime response");
                    let _ = out.send(payload).await;
                });
            }
        }
    };

    server.extension.disconnect(&connection).await;
    drop(connection);
    server.runtime.remove_sink(sink_id);
    server.runtime.detach_owners_under(OWNER_ID);
    let _ = tokio::time::timeout(SHUTDOWN_FORCE_WINDOW, server.extension.shutdown()).await;
    server.runtime.kill_all();
    // 给写端一个窗口把回执与最后的事件送出去;宿主不读就不等它。
    drop(sink);
    drop(event_sender);
    let _ = tokio::time::timeout(WRITER_DRAIN_WINDOW, &mut writer).await;
    writer.abort();
    result
}

/// 写端:事件与响应从同一条队列出去。队列曾满过就先补一条 `lagged`。写失败即宿主没了,通知主循环。
async fn write_loop<W: AsyncWrite + Unpin>(
    mut output: W, mut events: mpsc::Receiver<String>, dropped: Arc<AtomicU64>, failed: Arc<tokio::sync::Notify>,
) {
    while let Some(event) = events.recv().await {
        let lagged = dropped.swap(0, Ordering::Relaxed);
        if lagged > 0 {
            let notice = serde_json::json!({ "type": "lagged", "dropped": lagged }).to_string();
            if write_line(&mut output, &notice).await.is_err() {
                failed.notify_one();
                return;
            }
        }
        if write_line(&mut output, &event).await.is_err() {
            failed.notify_one();
            return;
        }
    }
}

/// `{"id":n,"method":"shutdown"}` 的 id;别的请求为 None。
fn shutdown_request_id(line: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    (value.get("method").and_then(serde_json::Value::as_str) == Some("shutdown"))
        .then(|| value.get("id").cloned().unwrap_or(serde_json::Value::Null))
}

/// 进程的 stdin / stdout 上的一条连接。先打一行 `{"type":"ready","transport":"stdio"}`,宿主据此开始发请求。
pub async fn serve_stdio(
    server: RuntimeServer, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    write_line(&mut output, &serde_json::json!({ "type": "ready", "transport": "stdio" }).to_string()).await?;
    serve(server, input, output, shutdown).await
}

async fn write_line<W: AsyncWrite + Unpin>(output: &mut W, line: &str) -> std::io::Result<()> {
    output.write_all(line.as_bytes()).await?;
    output.write_all(b"\n").await?;
    output.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use alwith_runtime_core::{AgentMetadata, Journal, Runtime};
    use alwith_runtime_ws::{AgentLauncher, NoExtension};
    use async_trait::async_trait;
    use serde::Deserialize;
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    struct CatLauncher;

    #[async_trait]
    impl AgentLauncher for CatLauncher {
        async fn prepare(
            &self, _agent_id: &str, owner_id: &str, _params: Value,
        ) -> Result<(Command, AgentMetadata), String> {
            Ok((
                Command::new("cat"),
                AgentMetadata {
                    owner_id: owner_id.into(),
                    engine: "cat".into(),
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

    #[derive(Deserialize)]
    struct ResponseView {
        id: u64,
        result: Option<Value>,
        error: Option<alwith_runtime_core::ResponseError>,
    }

    fn server() -> RuntimeServer {
        let root = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root).unwrap()));
        RuntimeServer::new(runtime, Arc::new(CatLauncher), Arc::new(NoExtension), String::new())
    }

    /// 起一条连接:返回 (写请求, 读帧, 服务任务)。读端跳过事件帧,只回响应。
    async fn connect() -> (tokio::io::DuplexStream, BufReader<tokio::io::DuplexStream>, tokio::task::JoinHandle<()>) {
        let (client_in, server_out) = tokio::io::duplex(64 * 1024);
        let (server_in, client_out) = tokio::io::duplex(64 * 1024);
        let task = tokio::spawn(async move {
            serve(server(), BufReader::new(server_in), server_out, std::future::pending()).await.unwrap();
        });
        (client_out, BufReader::new(client_in), task)
    }

    async fn call(
        writer: &mut tokio::io::DuplexStream, reader: &mut BufReader<tokio::io::DuplexStream>, id: u64, method: &str,
        params: Value,
    ) -> ResponseView {
        let request = serde_json::json!({ "id": id, "method": method, "params": params }).to_string() + "\n";
        writer.write_all(request.as_bytes()).await.unwrap();
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).await.unwrap() > 0, "connection closed");
            let value: Value = serde_json::from_str(&line).unwrap();
            if value.get("type").is_some() {
                continue;
            }
            return serde_json::from_value(value).unwrap();
        }
    }

    #[tokio::test]
    async fn requests_and_responses_are_one_json_per_line() {
        let (mut writer, mut reader, _task) = connect().await;
        let pong = call(&mut writer, &mut reader, 1, "ping", Value::Null).await;
        assert_eq!(pong.id, 1);
        assert_eq!(pong.result, Some(Value::String("pong".into())));
        let states = call(&mut writer, &mut reader, 2, "runStates", Value::Null).await;
        assert_eq!(states.result, Some(Value::Array(Vec::new())));
        let unknown = call(&mut writer, &mut reader, 3, "nothing", Value::Null).await;
        let unknown = unknown.error.unwrap();
        assert_eq!(unknown.code, alwith_runtime_core::ResponseError::METHOD_NOT_FOUND);
        assert!(unknown.message.contains("unknown runtime method nothing"));
    }

    #[tokio::test]
    async fn malformed_lines_answer_with_the_original_id() {
        let (mut writer, mut reader, _task) = connect().await;
        writer.write_all(b"{\"id\": 9, \"method\": 5}\n").await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let response: ResponseView = serde_json::from_str(&line).unwrap();
        assert_eq!(response.id, 9);
        assert_eq!(response.error.unwrap().code, alwith_runtime_core::ResponseError::INVALID_PARAMS);
        assert!(response.result.is_none());
    }

    #[tokio::test]
    async fn stdin_eof_ends_the_connection_and_kills_agents() {
        let (mut writer, mut reader, task) = connect().await;
        #[cfg(unix)]
        {
            let started =
                call(&mut writer, &mut reader, 1, "start", serde_json::json!({ "agentId": "cat", "launch": {} })).await;
            assert!(started.error.is_none(), "{:?}", started.error);
        }
        drop(writer);
        tokio::time::timeout(std::time::Duration::from_secs(5), task).await.expect("serve returns on EOF").unwrap();
    }

    #[tokio::test]
    async fn shutdown_request_is_answered_then_ends_the_connection() {
        let (mut writer, mut reader, task) = connect().await;
        let receipt = call(&mut writer, &mut reader, 7, "shutdown", Value::Null).await;
        assert_eq!(receipt.id, 7);
        assert!(receipt.error.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("serve returns after shutdown")
            .unwrap();
    }

    /// 宿主不读 stdout 时写端会卡住;主循环不写,所以 shutdown 照样立刻生效。
    #[tokio::test]
    async fn a_host_that_stops_reading_cannot_block_shutdown() {
        let (_client_in, server_out) = tokio::io::duplex(256);
        let (server_in, mut client_out) = tokio::io::duplex(64 * 1024);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            serve(server(), BufReader::new(server_in), server_out, async move {
                let _ = shutdown_receiver.await;
            })
            .await
        });
        // 一批 ping 的响应远超 256 字节的管道:写端在第几条上堵住都行,主循环必须还在读。
        for id in 0..200u64 {
            let request = serde_json::json!({ "id": id, "method": "ping" }).to_string() + "\n";
            client_out.write_all(request.as_bytes()).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        shutdown_sender.send(()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .expect("shutdown must not wait for a host that stopped reading")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn agent_frames_arrive_as_events_on_the_same_stream() {
        let (mut writer, mut reader, _task) = connect().await;
        let started =
            call(&mut writer, &mut reader, 1, "start", serde_json::json!({ "agentId": "cat", "launch": {} })).await;
        assert!(started.error.is_none(), "{:?}", started.error);
        let prompt = r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#;
        let sent =
            call(&mut writer, &mut reader, 2, "send", serde_json::json!({ "agentId": "cat", "line": prompt })).await;
        assert!(sent.error.is_none(), "{:?}", sent.error);
        // cat 回显后 Runtime 广播 `lines` 事件;它必须出现在同一条 stdout 上。
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let mut line = String::new();
            let read =
                tokio::time::timeout_at(deadline, reader.read_line(&mut line)).await.expect("event before deadline");
            assert!(read.unwrap() > 0);
            let value: Value = serde_json::from_str(&line).unwrap();
            if value.get("type").and_then(Value::as_str) == Some("inbound") {
                break;
            }
        }
    }
}
