//! Runtime-owned provider selection and credential resolution. Independent of filesystem/store/QA RPC.
use alwith_api::AgentMetadata;
use alwith_cli_adapters::{
    ProviderConfig, apply_provider_environment, provider_connection, resolve_provider_environment,
};
use alwith_fs::FileSystem;
use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// The engine id identifies an installed executable, not a hard-coded CLI family.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum ProviderAdapter {
    Raw {},
    Alwith {},
    Dsh {},
    Environment {
        env: std::collections::BTreeMap<String, ProviderField>,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default, rename = "envRemove")]
        env_remove: Vec<String>,
    },
}

impl ProviderAdapter {
    fn kind(&self) -> &'static str { match self { Self::Raw {} => "raw", Self::Alwith {} => "alwith", Self::Dsh {} => "dsh", Self::Environment { .. } => "environment" } }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ProviderField { ProviderId, ModelId, FastModelId, ApiKey, BaseUrl }

fn native_adapters() -> std::collections::HashMap<String, ProviderAdapter> {
    [("alwith".into(), ProviderAdapter::Alwith {}), ("dsh".into(), ProviderAdapter::Dsh {})].into()
}

/// 前端 `start` 时给的供应商配置(与原 web 模式 的 StartProvider 相同)。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartProvider {
    engine: String,
    #[serde(flatten)]
    provider: ProviderConfig,
    workspace_root: Option<String>,
    harness_preset: Option<String>,
    dsh_provider_route: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchOverrides {
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: std::collections::HashMap<String, String>,
    #[serde(default)]
    env_remove: Vec<String>,
    cwd: Option<String>,
}

pub struct ProviderManager {
    adapters: std::collections::HashMap<String, ProviderAdapter>,
    settings_path: Option<PathBuf>,
    file_system: Option<FileSystem>,
    cli_path: String,
    bun_path: Option<PathBuf>,
    dsh_entry: Option<PathBuf>,
    gateway_base_url: String,
    projects_root: Option<PathBuf>,
}

impl ProviderManager {
    fn configure_adapters(&mut self, table: std::collections::BTreeMap<String, Value>) -> Result<(), String> {
        let mut adapters = std::collections::HashMap::new();
        for (engine, spec) in table {
            let object = spec.as_object().ok_or_else(|| format!("engine {engine} must be an object"))?;
            let adapter = match object.get("providerAdapter") {
                Some(value) => serde_json::from_value(value.clone()).map_err(|error| format!("engine {engine} providerAdapter: {error}"))?,
                None => match engine.as_str() { "alwith" => ProviderAdapter::Alwith {}, "dsh" => ProviderAdapter::Dsh {}, _ => ProviderAdapter::Raw {} },
            };
            if let ProviderAdapter::Environment { env, env_remove, .. } = &adapter {
                for key in env.keys().chain(env_remove) {
                    if key.is_empty() || key.contains(['=', '\0']) { return Err(format!("engine {engine} has an invalid environment variable name")); }
                }
            }
            adapters.insert(engine, adapter);
        }
        self.adapters = adapters;
        Ok(())
    }

    pub fn capabilities(&self) -> Value {
        let mut entries: Vec<_> = self.adapters.iter().map(|(engine, adapter)| serde_json::json!({"engine":engine,"providerAdapter":adapter.kind()})).collect();
        entries.sort_by(|a,b| a["engine"].as_str().cmp(&b["engine"].as_str()));
        serde_json::json!({"schemaVersion":1,"engines":entries})
    }

    pub async fn status(&self) -> Result<Value, String> {
        let output = tokio::time::timeout(std::time::Duration::from_secs(5), Command::new(&self.cli_path).arg("--version").output())
            .await.map_err(|_| "CLI version probe timed out")?.map_err(|error| format!("CLI version probe: {error}"))?;
        if !output.status.success() { return Err(format!("CLI version probe exited {}", output.status)); }
        let version = String::from_utf8_lossy(&output.stdout).split_whitespace().last().ok_or("CLI returned an empty version")?.to_owned();
        Ok(serde_json::json!({"version":version,"local":{"path":self.cli_path,"fromSource":false}}))
    }

    pub async fn probe_capabilities(&self, params: Value) -> Result<Value, String> {
        let provider: ProviderConfig =
            serde_json::from_value(params).map_err(|error| format!("provider config: {error}"))?;
        let settings = self.settings()?;
        let api_key = settings
            .get("ai.providerConfigs")
            .and_then(|value| value.get(&provider.provider_id))
            .and_then(|value| value.get("apiKey"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let environment = resolve_provider_environment(provider, api_key, &self.gateway_base_url)?;
        serde_json::to_value(alwith_cli_adapters::capability_probe::probe_provider_capabilities(environment).await?)
            .map_err(|error| error.to_string())
    }

    pub fn from_env() -> Result<Self, String> {
        let roots = std::env::var_os("ALWITH_FS_ROOTS")
            .map(|roots| FileSystem::new(std::env::split_paths(&roots)))
            .transpose()?;
        let mut manager = Self::configured(std::env::var_os("ALWITH_SETTINGS_PATH").map(PathBuf::from), roots);
        match std::env::var("ALWITH_RUNTIME_ENGINES") {
            Ok(table) => manager.configure_adapters(serde_json::from_str(&table).map_err(|error| format!("engine table: {error}"))?)?,
            Err(std::env::VarError::NotPresent) => {},
            Err(error) => return Err(format!("engine table: {error}")),
        }
        Ok(manager)
    }

    pub(super) fn configured(settings_path: Option<PathBuf>, file_system: Option<FileSystem>) -> Self {
        Self {
            adapters: native_adapters(),
            settings_path,
            file_system,
            cli_path: std::env::var("ALWITH_CLI_PATH").unwrap_or_else(|_| "alwith-cli".into()),
            bun_path: std::env::var_os("ALWITH_BUN_PATH").map(PathBuf::from),
            dsh_entry: std::env::var_os("ALWITH_DSH_ENTRY").map(PathBuf::from),
            gateway_base_url: std::env::var("ALWITH_AI_GATEWAY_BASE_URL").unwrap_or_default(),
            projects_root: std::env::var_os("ALWITH_PROJECTS_DIR").map(PathBuf::from),
        }
    }

    /// No public RPC returns resolved credentials. Raw launches retain support for arbitrary CLIs.
    pub fn resolve_launch(&self, mut launch: Value) -> Result<Value, String> {
        let object = launch.as_object_mut().ok_or("launch must be an object")?;
        let Some(provider) = object.remove("providerConfig") else { return Ok(launch) };
        let overrides: LaunchOverrides = object
            .remove("overrides")
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| format!("launch overrides: {error}"))?
            .unwrap_or_default();
        if !object.is_empty() {
            return Err("providerConfig accepts only explicit overrides, not raw launch parameters".into());
        }
        let request = serde_json::from_value(provider).map_err(|error| format!("providerConfig: {error}"))?;
        let mut resolved = self.agent_start_params(request)?;
        resolved["args"].as_array_mut().expect("adapter args").extend(overrides.args.into_iter().map(Value::String));
        for key in &overrides.env_remove {
            resolved["env"].as_object_mut().expect("adapter env").remove(key);
        }
        resolved["envRemove"]
            .as_array_mut()
            .expect("adapter env removals")
            .extend(overrides.env_remove.into_iter().map(Value::String));
        resolved["env"]
            .as_object_mut()
            .expect("adapter env")
            .extend(overrides.env.into_iter().map(|(k, v)| (k, Value::String(v))));
        if let Some(cwd) = overrides.cwd {
            if let Some(fs) = &self.file_system {
                fs.authorize_watch_path(Path::new(&cwd))?;
            }
            resolved["cwd"] = Value::String(cwd);
        }
        Ok(resolved)
    }

    fn settings(&self) -> Result<Value, String> {
        let path = self.settings_path.as_ref().ok_or("providerConfig requires ALWITH_SETTINGS_PATH")?;
        match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(serde_json::json!({})),
            Err(error) => Err(format!("read {}: {error}", path.display())),
        }
    }

    /// 与原 web 模式 的 DesktopLauncher::prepare 相同,只是不 spawn:返回 Runtime `start` 的参数。
    fn agent_start_params(&self, request: StartProvider) -> Result<Value, String> {
        let settings = self.settings()?;
        let StartProvider { engine, provider, workspace_root, harness_preset, dsh_provider_route } = request;
        let api_key = settings
            .pointer(&format!("/ai.providerConfigs/{}/apiKey", escape_pointer(&provider.provider_id)))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(root) = &workspace_root
            && let Some(fs) = &self.file_system
        {
            fs.authorize_watch_path(Path::new(root))?;
        }
        let adapter = self.adapters.get(&engine).ok_or_else(|| format!("unknown engine {engine}"))?;
        let (mut command, metadata) = match adapter {
            ProviderAdapter::Raw {} => return Err(format!("engine {engine} has no provider adapter; use a raw launch or configure providerAdapter")),
            ProviderAdapter::Environment { env, args, env_remove } => {
                let mut command = Command::new(&engine);
                command.args(args);
                for key in env_remove { command.env_remove(key); }
                for (key, field) in env {
                    let value = match field {
                        ProviderField::ProviderId => Some(provider.provider_id.as_str()),
                        ProviderField::ModelId => provider.model_id.as_deref(),
                        ProviderField::FastModelId => provider.fast_model_id.as_deref(),
                        ProviderField::ApiKey => {
                            if api_key.trim().is_empty() { return Err(format!("provider {} requires an API key for engine {engine}", provider.provider_id)); }
                            Some(api_key.as_str())
                        },
                        ProviderField::BaseUrl => provider.base_url.as_deref(),
                    };
                    match value { Some(value) => { command.env(key, value); }, None => { command.env_remove(key); } }
                }
                let metadata = AgentMetadata { owner_id: String::new(), engine: engine.clone(), provider: provider.provider_id,
                    model: provider.model_id.unwrap_or_default(), connection: "API".into() };
                (command, metadata)
            }
            ProviderAdapter::Dsh {} => {
                let entry = self.dsh_entry.as_ref().ok_or("ALWITH_DSH_ENTRY is required for the Web harness")?;
                if !entry.is_file() {
                    return Err(format!("harness entry is missing: {}", entry.display()));
                }
                let bun = self.bun_path.as_ref().ok_or("ALWITH_BUN_PATH is required for the Web harness")?;
                let mut command = Command::new(bun);
                command.arg(entry);
                alwith_cli_adapters::harness_env::configure_dsh_env(
                    &mut command,
                    alwith_cli_adapters::harness_env::DshEnvironment {
                        provider_id: &provider.provider_id,
                        api_key: &api_key,
                        model_id: provider.model_id.as_deref(),
                        base_url: provider.base_url.as_deref(),
                        workspace_root: workspace_root.as_deref(),
                        harness_preset: harness_preset.as_deref(),
                        pi_route: dsh_provider_route.as_deref(),
                    },
                    self.settings_path
                        .as_ref()
                        .ok_or("providerConfig requires ALWITH_SETTINGS_PATH")?
                        .parent()
                        .ok_or("settings path requires a parent")?,
                    &self.projects_root.clone().map(Ok).unwrap_or_else(|| {
                        dirs::home_dir().map(|p| p.join(".alwith/projects")).ok_or("home directory is required")
                    })?,
                )?;
                let metadata = AgentMetadata {
                    owner_id: String::new(),
                    engine: engine.as_str().to_string(),
                    provider: provider.provider_id,
                    model: provider.model_id.unwrap_or_default(),
                    connection: "DeepSeek API".to_string(),
                };
                (command, metadata)
            }
            ProviderAdapter::Alwith {} => {
                let environment = resolve_provider_environment(provider, api_key, &self.gateway_base_url)?;
                let metadata = AgentMetadata {
                    owner_id: String::new(),
                    engine: engine.as_str().to_string(),
                    provider: environment.provider_id.clone(),
                    model: environment.model.clone(),
                    connection: provider_connection(&environment),
                };
                let mut command = Command::new(&self.cli_path);
                command.arg("--acp").env("CLAUDE_CODE_ACP_ALLOW_BYPASS_PERMISSIONS", "true");
                apply_provider_environment(&mut command, &environment);
                (command, metadata)
            }
        };
        if let Some(root) = &workspace_root {
            command.current_dir(root);
        }
        Ok(start_params(engine.as_str(), &command, &metadata))
    }
}

fn escape_pointer(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}
/// Command → Runtime `start` 参数(程序在引擎表里,这里只有参数、环境、cwd 和标签)。
fn start_params(engine: &str, command: &Command, metadata: &AgentMetadata) -> Value {
    let std_command = command.as_std();
    let args: Vec<String> = std_command.get_args().map(|arg| arg.to_string_lossy().to_string()).collect();
    let mut env = serde_json::Map::new();
    let mut env_remove = Vec::new();
    for (key, value) in std_command.get_envs() {
        if let Some(value) = value {
            env.insert(key.to_string_lossy().to_string(), Value::String(value.to_string_lossy().to_string()));
        } else {
            env_remove.push(key.to_string_lossy().to_string());
        }
    }
    let mut params = serde_json::json!({
        "engine": engine, "args": args, "env": env, "envRemove": env_remove,
        "provider": metadata.provider, "model": metadata.model, "connection": metadata.connection,
    });
    if let Some(cwd) = std_command.get_current_dir() {
        params["cwd"] = Value::String(cwd.to_string_lossy().to_string());
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn arbitrary_engine_names_select_declared_adapters() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings.json");
        std::fs::write(&path, json!({"ai.providerConfigs":{"custom":{"apiKey":"test-only-key"}}}).to_string()).unwrap();
        let mut manager = ProviderManager::configured(Some(path), None);
        manager.configure_adapters(serde_json::from_value(json!({
            "company-cli":{"providerAdapter":{"kind":"alwith"}},
            "other-cli":{"providerAdapter":{"kind":"environment", "env":{
                "CUSTOM_KEY":"apiKey", "CUSTOM_MODEL":"modelId", "CUSTOM_URL":"baseUrl", "CUSTOM_PROVIDER":"providerId"
            }, "args":["--acp"], "envRemove":["STALE_KEY"]}},
            "plain-cli":{}
        })).unwrap()).unwrap();
        let launch = manager.resolve_launch(json!({"providerConfig":{"engine":"company-cli","providerId":"claude-plan"}})).unwrap();
        assert_eq!(launch["engine"], "company-cli");
        assert_eq!(launch["args"], json!(["--acp"]));
        let launch = manager.resolve_launch(json!({"providerConfig":{"engine":"other-cli","providerId":"custom","modelId":"chosen"}})).unwrap();
        assert_eq!(launch["engine"], "other-cli");
        assert_eq!(launch["env"]["CUSTOM_KEY"], "test-only-key");
        assert_eq!(launch["env"]["CUSTOM_MODEL"], "chosen");
        assert_eq!(launch["env"]["CUSTOM_PROVIDER"], "custom");
        assert!(launch["envRemove"].as_array().unwrap().contains(&json!("CUSTOM_URL")));
        assert!(launch["envRemove"].as_array().unwrap().contains(&json!("STALE_KEY")));
        assert_eq!(manager.capabilities()["engines"].as_array().unwrap().len(), 3);
        assert!(!manager.capabilities().to_string().contains("test-only-key"));
        assert!(manager.resolve_launch(json!({"providerConfig":{"engine":"plain-cli","providerId":"custom"}})).unwrap_err().contains("no provider adapter"));
        let raw = json!({"engine":"plain-cli","args":["--own-config"]});
        assert_eq!(manager.resolve_launch(raw.clone()).unwrap(), raw);
        assert!(manager.resolve_launch(json!({"providerConfig":{"engine":"other-cli","providerId":"missing"}})).unwrap_err().contains("requires an API key"));
    }

    #[test]
    fn adapter_configuration_rejects_invalid_fields_without_partial_updates() {
        let mut manager = ProviderManager::configured(None, None);
        let before = manager.capabilities();
        for adapter in [json!({"kind":"typo"}), json!({"kind":"raw","typo":true}),
            json!({"kind":"environment","env":{"KEY":"unknown"}}),
            json!({"kind":"environment","env":{"BAD=KEY":"apiKey"}})] {
            assert!(manager.configure_adapters(serde_json::from_value(json!({"custom":{"providerAdapter":adapter}})).unwrap()).is_err());
            assert_eq!(manager.capabilities(), before);
        }
    }

    #[test]
    fn providers_work_without_fs_store_or_qa_services_and_reread_credentials() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings.json");
        let manager = ProviderManager::configured(Some(path.clone()), None);
        let request = json!({"providerConfig":{"engine":"alwith", "providerId":"deepseek", "modelId":"chosen"}});
        for key in ["first-test-key", "updated-test-key"] {
            std::fs::write(&path, json!({"ai.providerConfigs":{"deepseek":{"apiKey":key}}}).to_string()).unwrap();
            let launch = manager.resolve_launch(request.clone()).unwrap();
            assert_eq!(launch["engine"], "alwith");
            assert_eq!(launch["model"], "chosen");
            assert_eq!(launch["env"]["ANTHROPIC_AUTH_TOKEN"], key);
            assert_eq!(launch["args"], json!(["--acp"]));
            assert!(launch["envRemove"].as_array().unwrap().contains(&json!("ANTHROPIC_API_KEY")));
        }
    }

    #[test]
    fn execution_overrides_are_explicit_and_authorized() {
        let root = tempfile::tempdir().unwrap();
        let manager = ProviderManager::configured(
            Some(root.path().join("settings.json")),
            Some(FileSystem::new([root.path().into()]).unwrap()),
        );
        let request = json!({"providerConfig":{"engine":"alwith", "providerId":"claude-plan"},
            "overrides":{"args":["--tools","Read"],"env":{"APP_FEATURE":"on"},"cwd":root.path()}});
        let launch = manager.resolve_launch(request.clone()).unwrap();
        assert_eq!(launch["args"], json!(["--acp", "--tools", "Read"]));
        assert_eq!(launch["env"]["APP_FEATURE"], "on");
        let mut bad = request.clone();
        bad["overrides"]["engine"] = json!("other");
        assert!(manager.resolve_launch(bad).is_err());
        let outside = tempfile::tempdir().unwrap();
        let mut bad = request;
        bad["overrides"]["cwd"] = json!(outside.path());
        assert!(manager.resolve_launch(bad).is_err());
    }

    #[test]
    fn corrupted_settings_and_unknown_adapters_fail_without_silent_fallback() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings.json");
        let manager = ProviderManager::configured(Some(path.clone()), None);
        std::fs::write(&path, "invalid-json").unwrap();
        assert!(manager.resolve_launch(json!({"providerConfig":{"engine":"alwith","providerId":"deepseek"}})).is_err());
        assert!(
            manager.resolve_launch(json!({"providerConfig":{"engine":"custom-cli","providerId":"deepseek"}})).is_err()
        );
        let raw = json!({"engine":"custom-cli","args":["--custom"]});
        assert_eq!(manager.resolve_launch(raw.clone()).unwrap(), raw);
    }
}
