//! Contracts between the control plane and the data plane (docs/architecture.md §3.2).
//! Consumers keep the newest valid doc they have seen and never treat a missing entry as a delete.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

pub const SCHEMA: u32 = 1;

pub trait Versioned {
    fn schema(&self) -> u32;
    fn version(&self) -> u64;
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HostDoc {
    pub schema: u32,
    pub version: u64,
    #[serde(default)]
    pub instances: Vec<InstanceSpec>,
    /// The only way to remove an instance.
    #[serde(default)]
    pub tombstones: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InstanceSpec {
    pub id: String,
    pub service: String,
    pub deployment: String,
    pub image: String,
    pub port: u16,
    #[serde(default = "one")]
    pub vcpus: u8,
    #[serde(default = "mem")]
    pub memory_mib: u32,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub health: Health,
    #[serde(default)]
    pub desired: Desired,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Health {
    /// HTTP path that must answer 2xx/3xx; TCP connect if unset.
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "ready_timeout")]
    pub ready_timeout_secs: u64,
    #[serde(default = "liveness")]
    pub liveness_secs: u64,
}

impl Default for Health {
    fn default() -> Self {
        Health { path: None, ready_timeout_secs: ready_timeout(), liveness_secs: liveness() }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Desired {
    #[default]
    Running,
    /// Snapshot and stop; a later `running` restores the snapshot.
    Sleeping,
}

fn one() -> u8 {
    1
}
fn mem() -> u32 {
    256
}
fn ready_timeout() -> u64 {
    60
}
fn liveness() -> u64 {
    5
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct HostStatus {
    pub applied_version: u64,
    pub instances: Vec<InstanceStatus>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InstanceStatus {
    pub id: String,
    pub deployment: String,
    pub state: InstanceState,
    /// Where the edge should send traffic once Ready.
    #[serde(default)]
    pub addr: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Booting,
    Ready,
    Sleeping,
    Failed,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct EdgeDoc {
    pub schema: u32,
    pub version: u64,
    #[serde(default)]
    pub routes: Vec<Route>,
    /// Hostnames to remove.
    #[serde(default)]
    pub tombstones: Vec<String>,
}

/// A route is replaced as a unit: `backends` is the full set for that hostname.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Route {
    pub host: String,
    pub service: String,
    pub backends: Vec<Backend>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Backend {
    pub instance: String,
    #[serde(default)]
    pub deployment: String,
    pub addr: String,
    pub weight: u32,
    pub state: BackendState,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendState {
    Active,
    /// No new requests; in-flight ones finish.
    Draining,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct EdgeStatus {
    pub applied_version: u64,
    pub backends: Vec<BackendStats>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BackendStats {
    pub instance: String,
    pub deployment: String,
    pub in_flight: u64,
    pub requests: u64,
    pub errors: u64,
    pub ejected: bool,
}

macro_rules! versioned {
    ($($t:ty),*) => {$(
        impl Versioned for $t {
            fn schema(&self) -> u32 { self.schema }
            fn version(&self) -> u64 { self.version }
        }
    )*};
}
versioned!(HostDoc, EdgeDoc);

/// Read `path`; return it only if it is valid and newer than `current`, persisting it to `last_good`.
pub fn load_newer<T: DeserializeOwned + Serialize + Versioned>(path: &Path, last_good: &Path, current: u64) -> Result<Option<T>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let doc: T = serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    if !(SCHEMA.saturating_sub(1)..=SCHEMA + 1).contains(&doc.schema()) {
        bail!("unsupported schema {}", doc.schema());
    }
    if doc.version() <= current {
        return Ok(None);
    }
    write_atomic(last_good, &doc)?;
    Ok(Some(doc))
}

/// The doc applied before the last restart, applied first so nothing depends on the control plane.
pub fn load_last_good<T: DeserializeOwned>(last_good: &Path) -> Option<T> {
    serde_json::from_slice(&std::fs::read(last_good).ok()?).ok()
}

pub fn write_atomic<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_or_broken_docs_are_ignored() {
        let dir = std::env::temp_dir().join(format!("proto-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (p, lg) = (dir.join("doc.json"), dir.join("lg.json"));
        write_atomic(&p, &EdgeDoc { schema: 1, version: 5, ..Default::default() }).unwrap();
        assert_eq!(load_newer::<EdgeDoc>(&p, &lg, 4).unwrap().unwrap().version, 5);
        assert!(load_newer::<EdgeDoc>(&p, &lg, 5).unwrap().is_none());
        std::fs::write(&p, "{garbage").unwrap();
        assert!(load_newer::<EdgeDoc>(&p, &lg, 5).is_err());
        write_atomic(&p, &EdgeDoc { schema: 9, version: 6, ..Default::default() }).unwrap();
        assert!(load_newer::<EdgeDoc>(&p, &lg, 5).is_err());
        assert_eq!(load_last_good::<EdgeDoc>(&lg).unwrap().version, 5);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
