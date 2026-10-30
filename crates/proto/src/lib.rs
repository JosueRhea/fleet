//! Contracts between the control plane and the data plane (docs/architecture.md §3.2).
//! Consumers keep the newest valid doc they have seen and never treat a missing entry as a delete.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    /// Secret env vars, sealed (libsodium sealed box) to this host's `box_pub`; only hostd can open it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_env: Option<String>,
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
        Health {
            path: None,
            ready_timeout_secs: ready_timeout(),
            liveness_secs: liveness(),
        }
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
    #[serde(default)]
    pub host_id: String,
    #[serde(default)]
    pub reported_at: u64,
    #[serde(default)]
    pub memory_mib_total: u32,
    #[serde(default)]
    pub memory_mib_used: u32,
    pub applied_version: u64,
    pub instances: Vec<InstanceStatus>,
    /// Hex X25519 public key the publisher seals this host's secrets to.
    #[serde(default)]
    pub box_pub: String,
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
    pub host: String,
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
    #[serde(default)]
    pub edge_id: String,
    #[serde(default)]
    pub reported_at: u64,
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

/// Where a doc is read from or a status report is written to: a local path or `s3://bucket/key`
/// (endpoint from `CCLOUD_S3_ENDPOINT`, credentials from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
pub enum Loc {
    File(PathBuf),
    S3 {
        bucket: Box<s3::Bucket>,
        key: String,
    },
}

impl Loc {
    pub fn parse(s: &str) -> Result<Loc> {
        let Some(rest) = s.strip_prefix("s3://") else {
            return Ok(Loc::File(s.into()));
        };
        let (bucket, key) = rest.split_once('/').context("want s3://bucket/key")?;
        let endpoint =
            std::env::var("CCLOUD_S3_ENDPOINT").context("CCLOUD_S3_ENDPOINT is not set")?;
        let region = s3::Region::Custom {
            region: "us-east-1".into(),
            endpoint,
        };
        let creds = s3::creds::Credentials::from_env()?;
        let mut bucket = s3::Bucket::new(bucket, region, creds)?.with_path_style();
        bucket.set_request_timeout(Some(Duration::from_secs(5)));
        Ok(Loc::S3 {
            bucket,
            key: key.into(),
        })
    }

    /// `None` if the object does not exist yet.
    pub async fn get(&self) -> Result<Option<Vec<u8>>> {
        match self {
            Loc::File(p) => match std::fs::read(p) {
                Ok(b) => Ok(Some(b)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e).with_context(|| format!("reading {}", p.display())),
            },
            Loc::S3 { bucket, key } => match bucket.get_object(key).await {
                Ok(r) => Ok(Some(r.to_vec())),
                Err(s3::error::S3Error::HttpFailWithBody(404, _)) => Ok(None),
                Err(e) => Err(e).with_context(|| format!("GET s3 {key}")),
            },
        }
    }

    pub async fn put_json<T: Serialize>(&self, v: &T) -> Result<()> {
        match self {
            Loc::File(p) => write_atomic(p, v),
            Loc::S3 { bucket, key } => {
                let b = serde_json::to_vec(v)?;
                bucket
                    .put_object_with_content_type(key, &b, "application/json")
                    .await
                    .with_context(|| format!("PUT s3 {key}"))?;
                Ok(())
            }
        }
    }
}

/// Signed doc as written by the publisher: `payload` is the doc's exact JSON bytes.
#[derive(Serialize, Deserialize)]
pub struct Envelope {
    pub key_id: String,
    pub payload: String,
    pub sig: String,
}

/// Publisher keys this consumer accepts docs from, by key id.
pub struct Trust(HashMap<String, VerifyingKey>);

pub fn key_id(k: &VerifyingKey) -> String {
    use sha2::Digest;
    let h = sha2::Sha256::digest(k.as_bytes());
    h[..4].iter().map(|b| format!("{b:02x}")).collect()
}

impl Trust {
    /// Each path holds one hex-encoded ed25519 public key.
    pub fn load(paths: &[PathBuf]) -> Result<Trust> {
        let mut keys = HashMap::new();
        for p in paths {
            let hex =
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            let k = VerifyingKey::from_bytes(
                &from_hex::<32>(hex.trim())
                    .with_context(|| format!("{}: want a hex ed25519 public key", p.display()))?,
            )?;
            keys.insert(key_id(&k), k);
        }
        if keys.is_empty() {
            bail!("no trusted publisher keys");
        }
        Ok(Trust(keys))
    }

    pub fn from_keys(keys: &[VerifyingKey]) -> Trust {
        Trust(keys.iter().map(|k| (key_id(k), *k)).collect())
    }

    /// Verify an envelope and decode the doc inside it.
    pub fn open<T: DeserializeOwned + Versioned>(&self, bytes: &[u8]) -> Result<T> {
        let env: Envelope = serde_json::from_slice(bytes).context("not a signed envelope")?;
        let key = self
            .0
            .get(&env.key_id)
            .with_context(|| format!("untrusted key {}", env.key_id))?;
        let payload = B64.decode(&env.payload).context("payload")?;
        let sig = Signature::from_slice(&B64.decode(&env.sig).context("sig")?)?;
        key.verify_strict(&payload, &sig).context("bad signature")?;
        let doc: T = serde_json::from_slice(&payload).context("parsing doc")?;
        if !(SCHEMA.saturating_sub(1)..=SCHEMA + 1).contains(&doc.schema()) {
            bail!("unsupported schema {}", doc.schema());
        }
        Ok(doc)
    }
}

fn from_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Fetch the doc; return it only if it verifies and is newer than `current`, persisting the signed
/// bytes to `last_good`. A missing doc is not an error: it never means "delete everything".
pub async fn load_newer<T: DeserializeOwned + Versioned>(
    loc: &Loc,
    trust: &Trust,
    last_good: &Path,
    current: u64,
) -> Result<Option<T>> {
    let Some(bytes) = loc.get().await? else {
        return Ok(None);
    };
    let doc: T = trust.open(&bytes)?;
    if doc.version() <= current {
        return Ok(None);
    }
    let tmp = last_good.with_extension("tmp");
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, last_good)?;
    Ok(Some(doc))
}

/// The doc applied before the last restart (re-verified), applied first so a restart needs no control plane.
pub fn load_last_good<T: DeserializeOwned + Versioned>(
    last_good: &Path,
    trust: &Trust,
) -> Option<T> {
    trust.open(&std::fs::read(last_good).ok()?).ok()
}

pub fn write_atomic<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn seal(k: &SigningKey, doc: &EdgeDoc) -> Vec<u8> {
        let payload = serde_json::to_vec(doc).unwrap();
        let env = Envelope {
            key_id: key_id(&k.verifying_key()),
            payload: B64.encode(&payload),
            sig: B64.encode(k.sign(&payload).to_bytes()),
        };
        serde_json::to_vec(&env).unwrap()
    }

    #[tokio::test]
    async fn only_newer_signed_docs_apply() {
        let dir = std::env::temp_dir().join(format!("proto-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (p, lg) = (dir.join("doc.json"), dir.join("lg.json"));
        let loc = Loc::File(p.clone());
        let (k, other) = (
            SigningKey::from_bytes(&[7; 32]),
            SigningKey::from_bytes(&[8; 32]),
        );
        let trust = Trust::from_keys(&[k.verifying_key()]);
        let doc = |v| EdgeDoc {
            schema: 1,
            version: v,
            ..Default::default()
        };

        assert!(
            load_newer::<EdgeDoc>(&loc, &trust, &lg, 0)
                .await
                .unwrap()
                .is_none(),
            "missing doc is not an error"
        );
        std::fs::write(&p, seal(&k, &doc(5))).unwrap();
        assert_eq!(
            load_newer::<EdgeDoc>(&loc, &trust, &lg, 4)
                .await
                .unwrap()
                .unwrap()
                .version,
            5
        );
        assert!(load_newer::<EdgeDoc>(&loc, &trust, &lg, 5)
            .await
            .unwrap()
            .is_none());
        std::fs::write(&p, "{garbage").unwrap();
        assert!(load_newer::<EdgeDoc>(&loc, &trust, &lg, 5).await.is_err());
        std::fs::write(&p, seal(&other, &doc(6))).unwrap();
        assert!(
            load_newer::<EdgeDoc>(&loc, &trust, &lg, 5).await.is_err(),
            "untrusted key"
        );
        let mut env: Envelope = serde_json::from_slice(&seal(&k, &doc(6))).unwrap();
        env.payload = B64.encode(serde_json::to_vec(&doc(99)).unwrap());
        std::fs::write(&p, serde_json::to_vec(&env).unwrap()).unwrap();
        assert!(
            load_newer::<EdgeDoc>(&loc, &trust, &lg, 5).await.is_err(),
            "tampered payload"
        );
        std::fs::write(
            &p,
            seal(
                &k,
                &EdgeDoc {
                    schema: 9,
                    version: 7,
                    ..Default::default()
                },
            ),
        )
        .unwrap();
        assert!(load_newer::<EdgeDoc>(&loc, &trust, &lg, 5).await.is_err());
        assert_eq!(load_last_good::<EdgeDoc>(&lg, &trust).unwrap().version, 5);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

/// The host's X25519 key for opening secrets sealed to it. Generated on first start, never leaves the host.
pub struct BoxKey(crypto_box::SecretKey);

impl BoxKey {
    pub fn load_or_create(path: &Path) -> Result<BoxKey> {
        if let Ok(hex) = std::fs::read_to_string(path) {
            let b = from_hex::<32>(hex.trim())
                .with_context(|| format!("{}: want a hex X25519 key", path.display()))?;
            return Ok(BoxKey(crypto_box::SecretKey::from(b)));
        }
        let mut b = [0u8; 32];
        std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut b)?;
        // Write a temp file and link it into place, so a crash never leaves an empty or partial key.
        use std::os::unix::fs::OpenOptionsExt;
        let tmp = path.with_extension("tmp");
        let _ = std::fs::remove_file(&tmp);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        std::io::Write::write_all(&mut f, to_hex(&b).as_bytes())?;
        f.sync_all()?;
        // hard_link (unlike rename) fails if the key exists, e.g. present but unreadable: never replace it.
        let linked = std::fs::hard_link(&tmp, path);
        let _ = std::fs::remove_file(&tmp);
        linked.with_context(|| format!("creating {}", path.display()))?;
        Ok(BoxKey(crypto_box::SecretKey::from(b)))
    }

    pub fn public_hex(&self) -> String {
        to_hex(self.0.public_key().as_bytes())
    }

    pub fn unseal_env(&self, sealed: &str) -> Result<BTreeMap<String, String>> {
        let ct = B64.decode(sealed).context("sealed_env is not base64")?;
        let pt = self
            .0
            .unseal(&ct)
            .map_err(|_| anyhow::anyhow!("cannot open sealed_env (sealed to another host key?)"))?;
        serde_json::from_slice(&pt).context("sealed_env payload")
    }
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod box_tests {
    use super::*;

    #[test]
    fn opens_what_is_sealed_to_it_only() {
        let dir = std::env::temp_dir().join(format!("boxkey-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let k = BoxKey::load_or_create(&dir.join("a")).unwrap();
        assert_eq!(
            BoxKey::load_or_create(&dir.join("a")).unwrap().public_hex(),
            k.public_hex()
        );
        let other = BoxKey::load_or_create(&dir.join("b")).unwrap();
        let ct =
            k.0.public_key()
                .seal(&mut crypto_box::aead::OsRng, br#"{"DB_PASSWORD":"s3cret"}"#)
                .unwrap();
        let b64 = B64.encode(ct);
        assert_eq!(k.unseal_env(&b64).unwrap()["DB_PASSWORD"], "s3cret");
        assert!(other.unseal_env(&b64).is_err());
    }
}
