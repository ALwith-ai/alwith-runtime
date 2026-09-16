use super::*;
use alwith_runtime_ws::NoExtension;
use serde_json::json;
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, ServiceExtension) {
    let root = tempfile::tempdir().unwrap();
    let settings = root.path().join("settings.json");
    std::fs::write(&settings, "{}").unwrap();
    let extension = ServiceExtension::configured(
        Arc::new(NoExtension),
        serde_json::from_value(json!({"schemaVersion":1,
            "fs":{"roots":[root.path()]},"git":{"roots":[root.path()]},"shell":{"roots":[root.path()]},"qa":{"roots":[root.path()]},
            "store":{"dataDir":root.path(),"cacheDir":root.path().join("cache")}})).unwrap(),
        providers::ProviderManager::configured(Some(settings), None),
    )
    .unwrap();
    (root, extension)
}

fn connection(extension: &ServiceExtension) -> (ConnectionState, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(16);
    (extension.connect("test", sender, Arc::new(Notify::new())), receiver)
}

#[tokio::test]
async fn services_are_optional_and_do_not_restrict_replaceable_clis() {
    let extension = ServiceExtension {
        providers: providers::ProviderManager::configured(None, None),
        services: None,
        next: Arc::new(NoExtension),
    };
    let (state, _events) = connection(&extension);
    let launch = json!({"engine":"custom-cli", "args":["--acp"], "env":{}});
    assert_eq!(extension.resolve_launch(launch.clone()).unwrap(), launch);
    assert!(extension.call("test", &state, "fs/readText", json!({"path":"/x"})).await.is_err());
    assert_eq!(extension.call("test", &state, "unrelated", Value::Null).await.unwrap(), None);
    assert!(extension.resolve_launch(json!({"providerConfig":{}})).is_err());
    assert!(extension.resolve_launch(json!({"providerConfig":{}, "args":[]})).is_err());
}

#[tokio::test]
async fn domain_rpc_reads_and_writes_only_authorized_paths() {
    let (root, extension) = fixture();
    let outside = tempfile::tempdir().unwrap();
    let (state, _events) = connection(&extension);
    let path = root.path().join("note.txt");
    extension.call("test", &state, "fs/writeText", json!({"path":path,"content":"one source"})).await.unwrap();
    assert_eq!(
        extension.call("test", &state, "fs/readText", json!({"path":path})).await.unwrap(),
        Some(json!("one source"))
    );
    assert!(
        extension
            .call("test", &state, "fs/writeText", json!({"path":outside.path().join("x"),"content":"no"}))
            .await
            .is_err()
    );
    assert!(extension.call("test", &state, "fs/unknown", Value::Null).await.is_err());
    extension.disconnect(&state).await;
    assert!(extension.call("test", &state, "fs/readText", json!({"path":path})).await.is_err());
}

#[tokio::test]
async fn stores_broadcast_scope_and_persist_without_a_second_database() {
    let (root, extension) = fixture();
    let (first, mut first_events) = connection(&extension);
    let (second, mut second_events) = connection(&extension);
    extension
        .call(
            "test",
            &first,
            "store/set",
            json!({
                "scope":"data", "store":"preferences", "key":"theme", "value":"dark"
            }),
        )
        .await
        .unwrap();
    for receiver in [&mut first_events, &mut second_events] {
        let line = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            frame,
            json!({"type":"service","event":{
                "type":"storeChanged","scope":"data","store":"preferences","key":"theme","value":"dark"
            }})
        );
    }
    assert_eq!(
        extension.call("test", &second, "store/entries", json!({"scope":"data","store":"preferences"})).await.unwrap(),
        Some(json!({"theme":"dark"}))
    );
    assert!(root.path().join("preferences.json").is_file());
    extension.disconnect(&first).await;
    extension.disconnect(&second).await;
}

#[tokio::test]
async fn watchers_are_connection_scoped_and_release_before_outstanding_calls_drop() {
    let (root, extension) = fixture();
    let (first, _first_events) = connection(&extension);
    let (second, _second_events) = connection(&extension);
    let request = json!({"watcherId":"same-id","path":root.path(),"recursive":false});
    extension.call("one", &first, "fs/watch", request.clone()).await.unwrap();
    extension.call("two", &second, "fs/watch", request.clone()).await.unwrap();
    assert!(extension.call("one", &first, "fs/watch", request).await.is_err());
    extension.disconnect(&first).await;
    assert!(first.downcast_ref::<ServiceConnection>().unwrap().watchers.lock().await.watchers.is_empty());
    assert_eq!(second.downcast_ref::<ServiceConnection>().unwrap().watchers.lock().await.watchers.len(), 1);
    extension.disconnect(&second).await;
    assert!(second.downcast_ref::<ServiceConnection>().unwrap().watchers.lock().await.watchers.is_empty());
}

#[tokio::test]
async fn watcher_events_are_camel_case_and_slow_connections_are_disconnected() {
    let (sender, mut receiver) = mpsc::channel(1);
    let disconnect = Arc::new(Notify::new());
    let sink = EventSink::Connection { events: sender, disconnect: Arc::clone(&disconnect) };
    sink.emit(
        serde_json::to_value(FileChangedEvent {
            event_type: "fsChanged",
            watcher_id: "watch-1".into(),
            paths: vec!["/test".into()],
        })
        .unwrap(),
    )
    .unwrap();
    assert!(sink.emit(json!({"type":"fsChanged"})).is_err());
    tokio::time::timeout(Duration::from_secs(3), disconnect.notified()).await.unwrap();
    let frame: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
    assert_eq!(frame["event"]["watcherId"], "watch-1");
    assert!(frame["event"].get("watcher_id").is_none());
}

#[tokio::test]
async fn secret_templates_are_not_public_rpc_methods() {
    let (_root, extension) = fixture();
    let (state, _events) = connection(&extension);
    for method in ["engines/prepare", "engines/alwithDevTemplate"] {
        assert!(extension.call("test", &state, method, Value::Null).await.is_err());
    }
}

struct NoLauncher;

#[tokio::test]
async fn each_resource_capability_can_be_enabled_without_the_others() {
    let root = tempfile::tempdir().unwrap();
    for capability in ["fs", "git", "shell", "qa", "store"] {
        let mut config = json!({"schemaVersion":1});
        config[capability] = if capability == "store" {
            json!({"dataDir":root.path().join("data"),"cacheDir":root.path().join("cache")})
        } else { json!({"roots":[root.path()]}) };
        let extension = ServiceExtension::configured(Arc::new(NoExtension), serde_json::from_value(config).unwrap(),
            providers::ProviderManager::configured(None, None)).unwrap();
        let (state, _events) = connection(&extension);
        assert_eq!(extension.call("test", &state, "services/capabilities", Value::Null).await.unwrap(),
            Some(json!({"schemaVersion":1,"enabled":[capability]})));
        for other in ["fs", "git", "shell", "qa", "store"] {
            if other != capability {
                let error = extension.call("test", &state, &format!("{other}/unknown"), Value::Null).await.unwrap_err();
                assert!(error.message.contains("disabled"), "{other}: {error:?}");
            }
        }
        if capability == "store" {
            extension.call("test", &state, "store/set", json!({"scope":"data","store":"preferences","key":"test","value":true})).await.unwrap();
            assert!(root.path().join("data/preferences.json").is_file());
        }
        if capability == "fs" {
            assert_eq!(extension.call("test", &state, "fs/capabilityRoots", Value::Null).await.unwrap(), Some(json!([root.path().canonicalize().unwrap()])));
        }
        extension.disconnect(&state).await;
    }
}

#[tokio::test]
async fn capabilities_have_independent_path_grants() {
    let first = tempfile::tempdir().unwrap(); let second = tempfile::tempdir().unwrap();
    let config = json!({"schemaVersion":1,"fs":{"roots":[first.path()]},"git":{"roots":[second.path()]}});
    let extension = ServiceExtension::configured(Arc::new(NoExtension), serde_json::from_value(config).unwrap(),
        providers::ProviderManager::configured(None, None)).unwrap();
    let (state, _events) = connection(&extension);
    assert!(extension.call("test", &state, "fs/readText", json!({"path":second.path().join("x")})).await.is_err());
    assert!(extension.call("test", &state, "git/currentBranch", json!({"path":first.path()})).await.is_err());
    extension.disconnect(&state).await;
}

#[test]
fn resource_configuration_rejects_unknown_or_incomplete_grants() {
    for config in [json!({"schemaVersion":1,"unknown":{}}), json!({"fs":{"roots":[]}}), json!({"schemaVersion":1,"store":{"dataDir":"/data"}})] {
        assert!(serde_json::from_value::<ServicesConfig>(config).is_err());
    }
    for config in [json!({"schemaVersion":2}), json!({"schemaVersion":1,"fs":{"roots":[]}}), json!({"schemaVersion":1,"git":{"roots":["relative"]}})] {
        assert!(serde_json::from_value::<ServicesConfig>(config).unwrap().validate().is_err());
    }
}

#[async_trait]
impl alwith_runtime_ws::AgentLauncher for NoLauncher {
    async fn prepare(&self, _: &str, _: &str, _: Value) -> Result<(Command, AgentMetadata), String> {
        Err("this test does not start an Agent".into())
    }
    fn version(&self) -> String {
        "test".into()
    }
}

fn server(root: &Path, extension: ServiceExtension) -> alwith_runtime_ws::RuntimeServer {
    let runtime =
        alwith_runtime_core::Runtime::with_journal(alwith_runtime_core::Journal::open(root.join("journal")).unwrap());
    alwith_runtime_ws::RuntimeServer::new(Arc::new(runtime), Arc::new(NoLauncher), Arc::new(extension), "secret".into())
}

#[tokio::test]
async fn stdio_exposes_services_without_a_host_process() {
    let (root, extension) = fixture();
    let server = server(root.path(), extension);
    let (client_stream, server_stream) = tokio::io::duplex(65536);
    let (input, output) = tokio::io::split(server_stream);
    let task = tokio::spawn(alwith_runtime_stdio::serve(
        server,
        tokio::io::BufReader::new(input),
        output,
        std::future::pending(),
    ));
    let (input, output) = tokio::io::split(client_stream);
    let client = alwith_api::RuntimeClient::over(input, output);
    let response = client.request("fs/capabilityRoots", None).await.unwrap();
    assert_eq!(response, json!([root.path().canonicalize().unwrap()]));
    drop(client);
    tokio::time::timeout(Duration::from_secs(3), task).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn websocket_exposes_the_same_services_without_desktop() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    let (root, extension) = fixture();
    let server = server(root.path(), extension);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut request = format!("ws://{}", listener.local_addr().unwrap()).into_client_request().unwrap();
    request.headers_mut().insert("Sec-WebSocket-Protocol", "alwith-runtime-v1, alwith-auth.secret".parse().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_with_shutdown(listener, axum::Router::new(), async {
                stopped.await.unwrap();
            })
            .await
            .unwrap();
    });
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket.send(Message::Text(json!({"id":1,"method":"fs/capabilityRoots"}).to_string().into())).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(&frame.into_text().unwrap()).unwrap();
            if value.get("id") == Some(&json!(1)) {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(response["result"], json!([root.path().canonicalize().unwrap()]));
    assert!(response["error"].is_null());
    socket.close(None).await.unwrap();
    drop(socket);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task).await.unwrap().unwrap();
}
