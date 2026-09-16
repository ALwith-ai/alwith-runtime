//! Resource services are opt-in independently; omitted capabilities expose no methods.
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServicesConfig {
    pub schema_version: u32,
    pub fs: Option<PathServiceConfig>,
    pub git: Option<PathServiceConfig>,
    pub shell: Option<PathServiceConfig>,
    pub qa: Option<PathServiceConfig>,
    pub store: Option<StoreConfig>,
}

impl Default for ServicesConfig {
    fn default() -> Self { Self { schema_version: 1, fs: None, git: None, shell: None, qa: None, store: None } }
}

impl ServicesConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 { return Err(format!("unsupported service config schemaVersion {}", self.schema_version)); }
        for paths in [&self.fs, &self.git, &self.shell, &self.qa].into_iter().flatten() {
            if paths.roots.is_empty() || paths.roots.iter().any(|root| !root.is_absolute()) {
                return Err("resource capability roots must be nonempty absolute paths".into());
            }
        }
        if let Some(store) = &self.store
            && (!store.data_dir.is_absolute() || !store.cache_dir.is_absolute() || store.data_dir == store.cache_dir) {
            return Err("store dataDir/cacheDir must be distinct absolute paths".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathServiceConfig { pub roots: Vec<PathBuf> }

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoreConfig { pub data_dir: PathBuf, pub cache_dir: PathBuf }
