//! The Runtime stdio entry. The host spawns `alwith-runtime`; requests come in on stdin one frame per line,
//! responses and events go out on stdout.
//!
//! Frame shapes are exactly the same as `alwith-runtime-ws` (requests `{id, method, params}`, responses
//! `{id, result | error}`, events carry `type`). Only protocol goes to stdout; logs always go to stderr.
//!
//! It differs from ws in only three ways:
//! - One process, one connection, with the owner fixed to `stdio` and no token handshake: the pipe itself
//!   is the authorization.
//! - stdin reaching EOF means the host has left: stop extensions, `kill_all`, return.
//! - The event queue is bounded. On ws a slow connection is disconnected and the client catches up by
//!   sequence; stdio cannot be disconnected, so when the queue is full events are dropped and counted, and
//!   once the queue drains a `{"type":"lagged","dropped":n}` is sent first, on which the client replays
//!   via `sessionEvents`. Approvals and errors do not vanish silently: they are all in the session journal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alwith_runtime_core::{RuntimeEvent, RuntimeEventSink, protocol_lines};
use alwith_runtime_ws::{RuntimeServer, SHUTDOWN_FORCE_WINDOW, SharedConnectionState, process_request_shared};
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

pub const OWNER_ID: &str = "stdio";
pub const EVENT_QUEUE_CAPACITY: usize = 8_192;
/// Window for the writer to drain the queue after shutdown; give up when the host has stopped reading.
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

/// Runs one Runtime connection over any read/write pair; `serve_stdio` wires it to the process's
/// stdin / stdout.
///
/// The main loop only reads, never writes: writing is done by a separate task, so a stdout nobody reads
/// (a stuck host) cannot block `shutdown`, disconnects or stdin handling.
/// The host can leave on its own by two routes with the same effect: stdin EOF, or a
/// `{"id":n,"method":"shutdown"}` request (the receipt arrives first, then shutdown proceeds)
/// -- a host that spawns a child cannot always close its end of stdin (Tauri's sidecar cannot), so a
/// method is provided.
/// On return every agent has been `kill_all`ed.
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
                    // The receipt is queued (the writer will send it), then the same shutdown as stdin EOF runs.
                    let receipt = serde_json::json!({ "id": id, "result": null, "error": null }).to_string();
                    let _ = event_sender.send(receipt).await;
                    break Ok(());
                }
                // Requests are handled concurrently (as on ws): the response returns to the writer through the same
                // event queue. Use send rather than try_send; responses must not be dropped.
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
    // Give the writer a window to flush the receipt and the last events; do not wait if the host is not
    // reading.
    drop(sink);
    drop(event_sender);
    let _ = tokio::time::timeout(WRITER_DRAIN_WINDOW, &mut writer).await;
    writer.abort();
    result
}

/// The writer: events and responses leave through the same queue. If the queue was ever full, a `lagged`
/// is sent first. A write failure means the host is gone; notify the main loop.
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

/// The id of `{"id":n,"method":"shutdown"}`; None for any other request.
fn shutdown_request_id(line: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    (value.get("method").and_then(serde_json::Value::as_str) == Some("shutdown"))
        .then(|| value.get("id").cloned().unwrap_or(serde_json::Value::Null))
}

/// A connection over the process's stdin / stdout. It first prints a line
/// `{"type":"ready","transport":"stdio"}`, on which the host starts sending requests.
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

    /// Starts a connection: returns (request writer, frame reader, service task). The reader skips event
    /// frames and returns only responses.
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

    /// The writer stalls when the host does not read stdout; the main loop does not write, so shutdown
    /// still takes effect immediately.
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
        // The responses to a batch of pings far exceed a 256-byte pipe: the writer may block on any of them,
        // but the main loop must still be reading.
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
        // After cat echoes, Runtime broadcasts a `lines` event; it must appear on the same stdout.
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
