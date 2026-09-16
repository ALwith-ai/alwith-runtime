//! runtime-client 对着真 Runtime 的往返测试。客户端本身在同级仓 alwith-api,
//! 这些测试要一个 Runtime 才能跑,所以留在这里。

use std::sync::Arc;

use alwith_api::{ClientEvent, RequestHandler, ResponseError, RuntimeClient, RuntimeEvent};
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
mod tests {
    use super::*;
    use alwith_runtime_core::{AgentMetadata, Journal, Runtime};
    use alwith_runtime_ws::{AgentLauncher, NoExtension, RuntimeServer};
    use async_trait::async_trait;

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

    /// 客户端对着进程内的 stdio 入口:两条 duplex 管道模拟子进程的 stdin / stdout。
    fn connect() -> (RuntimeClient, JoinHandle<()>) {
        let root = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root).unwrap()));
        let server = RuntimeServer::new(runtime, Arc::new(CatLauncher), Arc::new(NoExtension), String::new());
        let (client_reader, server_writer) = tokio::io::duplex(64 * 1024);
        let (server_reader, client_writer) = tokio::io::duplex(64 * 1024);
        let task = tokio::spawn(async move {
            alwith_runtime_stdio::serve(server, BufReader::new(server_reader), server_writer, std::future::pending())
                .await
                .unwrap();
        });
        (RuntimeClient::over(client_reader, client_writer), task)
    }

    #[tokio::test]
    async fn requests_round_trip_and_errors_carry_the_method() {
        let (client, _task) = connect();
        client.ping().await.unwrap();
        assert_eq!(client.version().await.unwrap(), "0.0.0-test");
        assert!(client.run_states().await.unwrap().is_empty());
        let error = client.request("nothing", None).await.unwrap_err();
        assert_eq!(error.code, ResponseError::METHOD_NOT_FOUND, "{error}");
        assert!(error.message.starts_with("unknown runtime method nothing"), "{error}");
        let denied = client.send("ghost", "{}", None).await.unwrap_err();
        assert_eq!(denied.code, ResponseError::RUNTIME, "{denied}");
        assert!(denied.message.contains("not owned"), "{denied}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn agent_lines_arrive_as_runtime_events() {
        let (client, _task) = connect();
        let mut events = client.events();
        client.start("cat", serde_json::json!({ "engine": "cat" }), Some("w1")).await.unwrap();
        let prompt = r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#;
        client.send("cat", prompt, Some("w1")).await.unwrap();
        assert!(client.send("cat", prompt, Some("w2")).await.unwrap_err().message.contains("not owned"));
        assert_eq!(client.agent_for_session("s1").await.unwrap().as_deref(), Some("cat"));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let event = tokio::time::timeout_at(deadline, events.recv()).await.expect("event in time").unwrap();
            if let ClientEvent::Runtime(RuntimeEvent::Inbound { agent_id, lines }) = event {
                assert_eq!(agent_id, "cat");
                assert!(lines[0].contains("session/prompt"));
                break;
            }
        }
        let frames = client.session_events("s1", None, None).await.unwrap();
        assert!(frames.len() >= 2);
        assert!(!client.session_replay("s1").await.unwrap().is_empty());
        client.stop("cat", None).await.unwrap();
    }

    /// 模块子进程的形态:对方(Runtime)发请求进来,handler 答;自己也能发事件出去。
    #[tokio::test]
    async fn handles_incoming_requests_and_emits_events() {
        use tokio::io::AsyncBufReadExt as _;
        let (peer_reader, client_writer) = tokio::io::duplex(64 * 1024);
        let (client_reader, mut peer_writer) = tokio::io::duplex(64 * 1024);
        let handler: RequestHandler = Arc::new(|method, params| {
            Box::pin(async move {
                if method == "echo" { Ok(params) } else { Err(ResponseError::method_not_found(&method)) }
            })
        });
        let client = RuntimeClient::over_with_handler(client_reader, client_writer, Some(handler));
        client.emit(serde_json::json!({ "type": "hello" })).unwrap();
        peer_writer.write_all(b"{\"id\":5,\"method\":\"echo\",\"params\":{\"x\":1}}\n").await.unwrap();
        peer_writer.write_all(b"{\"id\":6,\"method\":\"nope\"}\n").await.unwrap();
        let mut lines = BufReader::new(peer_reader).lines();
        let mut seen = Vec::new();
        while seen.len() < 3 {
            let line = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            seen.push(serde_json::from_str::<Value>(&line).unwrap());
        }
        assert!(seen.iter().any(|v| v["type"] == "hello"));
        assert!(seen.iter().any(|v| v["id"] == 5 && v["result"]["x"] == 1));
        assert!(seen.iter().any(|v| v["id"] == 6
            && v["error"]["code"] == ResponseError::METHOD_NOT_FOUND
            && v["error"]["message"].as_str().unwrap().contains("unknown runtime method nope")));
    }

    #[tokio::test]
    async fn closing_the_stream_fails_pending_requests() {
        let (client, task) = connect();
        client.ping().await.unwrap();
        task.abort();
        let _ = task.await;
        let error = client.ping().await.unwrap_err();
        assert_eq!(error.code, ResponseError::CONNECTION_LOST, "{error}");
        assert!(error.message.contains("connection closed") || error.message.contains("write failed"), "{error}");
        assert!(client.closed().is_some());
    }

    /// 一个用 python 写的假 ACP agent:握手、建会话、prompt 时先推一条 session/update、再问一次权限,
    /// 拿到答复后回合结束。
    const FAKE_AGENT: &str = r#"#!/usr/bin/env python3
import json, sys
def emit(o):
    print(json.dumps(o), flush=True)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    m = json.loads(line)
    method = m.get("method")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": m["id"], "result": {"protocolVersion": 2, "agentInfo": {"name": "fake", "version": "0"}}})
    elif method == "session/new":
        emit({"jsonrpc": "2.0", "id": m["id"], "result": {"sessionId": "s-1"}})
    elif method == "session/prompt":
        sid = m["params"]["sessionId"]
        emit({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": sid, "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "hi"}}}})
        emit({"jsonrpc": "2.0", "id": 77, "method": "session/request_permission", "params": {"sessionId": sid, "options": [{"optionId": "ok", "kind": "allow_once"}]}})
        pending = m["id"]
        for answer in sys.stdin:
            a = json.loads(answer)
            if a.get("id") == 77:
                emit({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": sid, "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": " there"}}}})
                emit({"jsonrpc": "2.0", "id": pending, "result": {"stopReason": "end_turn", "_meta": {"answered": a["result"]["outcome"]["optionId"]}}})
                break
    elif method == "slow":
        pending = m["id"]
        for other in sys.stdin:
            o = json.loads(other)
            if o.get("method") == "$/cancel_request" and o["params"]["requestId"] == pending:
                emit({"jsonrpc": "2.0", "id": pending, "error": {"code": -32800, "message": "Request cancelled"}})
                break
"#;

    struct ScriptLauncher(std::path::PathBuf);

    #[async_trait]
    impl AgentLauncher for ScriptLauncher {
        async fn prepare(
            &self, _agent_id: &str, owner_id: &str, _params: Value,
        ) -> Result<(Command, AgentMetadata), String> {
            Ok((
                Command::new(&self.0),
                AgentMetadata {
                    owner_id: owner_id.into(),
                    engine: "fake".into(),
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

    #[cfg(unix)]
    #[tokio::test]
    async fn the_runtime_speaks_acp_for_the_app() {
        if !std::path::Path::new("/usr/bin/env").exists() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("agent.py");
        std::fs::write(&script, FAKE_AGENT).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let root = tempfile::tempdir().unwrap().keep();
        let runtime = Arc::new(Runtime::with_journal(Journal::open(root).unwrap()));
        let server =
            RuntimeServer::new(runtime, Arc::new(ScriptLauncher(script)), Arc::new(NoExtension), String::new());
        let (client_reader, server_writer) = tokio::io::duplex(64 * 1024);
        let (server_reader, client_writer) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            alwith_runtime_stdio::serve(server, BufReader::new(server_reader), server_writer, std::future::pending())
                .await
                .unwrap();
        });
        let client = Arc::new(RuntimeClient::over(client_reader, client_writer));
        let mut events = client.events();
        client.start("a", serde_json::json!({}), None).await.unwrap();

        // 建会话:Runtime 自己握手,应用只看到 sessionId。
        let session = client.session_new("a", "/tmp", vec![], None, None).await.unwrap();
        assert_eq!(session["sessionId"], "s-1");

        // 一轮对话:内容经 acpNotification,权限请求经 acpAgentRequest,应用答完回合才结束。
        let prompter = Arc::clone(&client);
        let turn = tokio::spawn(async move {
            prompter.prompt("a", "s-1", serde_json::json!([{ "type": "text", "text": "hello" }]), None, None).await
        });
        let mut chunks = Vec::new();
        let mut answered = false;
        let mut inbound_saw_runtime_response = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !answered || chunks.len() < 2 {
            let event = tokio::time::timeout_at(deadline, events.recv()).await.expect("events in time").unwrap();
            match event {
                ClientEvent::Runtime(RuntimeEvent::AcpNotification { method, params, .. })
                    if method == "session/update" =>
                {
                    if let Some(text) = params.pointer("/update/content/text").and_then(Value::as_str) {
                        chunks.push(text.to_string());
                    }
                }
                ClientEvent::Runtime(RuntimeEvent::AcpAgentRequest { agent_id, id, method, .. }) => {
                    assert_eq!(method, "session/request_permission");
                    client
                        .acp_respond(
                            &agent_id,
                            id,
                            Some(serde_json::json!({ "outcome": { "outcome": "selected", "optionId": "ok" } })),
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    answered = true;
                }
                // Runtime 自己发的请求的响应不进 inbound(应用没发过它们)。
                ClientEvent::Runtime(RuntimeEvent::Inbound { lines, .. })
                    if lines.iter().any(|line| line.contains("\"id\":\"rt-")) =>
                {
                    inbound_saw_runtime_response = true;
                }
                _ => {}
            }
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), turn).await.unwrap().unwrap().unwrap();
        assert_eq!(result["stopReason"], "end_turn");
        assert_eq!(result["_meta"]["answered"], "ok");
        assert_eq!(chunks, vec!["hi".to_string(), " there".to_string()]);
        assert!(!inbound_saw_runtime_response);
        // 第二次建会话不再握手:initialize 走缓存(假 agent 收到第二个 initialize 也会答,但 Runtime 不该发)。
        let again = client.session_new("a", "/tmp", vec![], None, None).await.unwrap();
        assert_eq!(again["sessionId"], "s-1");
        // 运行态照常:Runtime 代发的 prompt 也被记账。
        let states = client.run_states().await.unwrap();
        assert_eq!(states.len(), 1);

        // 应用自选 id:撤回(`$/cancel_request`)要靠它;还没答完的 id 不能再用;agent 回的 -32800 原样到应用。
        let waiter = Arc::clone(&client);
        let slow = tokio::spawn(async move {
            waiter.acp_request("a", "slow", serde_json::json!({}), Some(serde_json::json!("app-1")), None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let duplicate = client
            .acp_request("a", "slow", serde_json::json!({}), Some(serde_json::json!("app-1")), None)
            .await
            .unwrap_err();
        assert_eq!(duplicate.code, ResponseError::INVALID_PARAMS, "{duplicate}");
        client.acp_notify("a", "$/cancel_request", serde_json::json!({ "requestId": "app-1" }), None).await.unwrap();
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_secs(10), slow).await.unwrap().unwrap().unwrap_err();
        assert_eq!(cancelled.code, -32800);
        assert_eq!(cancelled.message, "Request cancelled");
        assert_eq!(states[0].session_id, "s-1");
    }
}
