//! Minimal Firecracker API client (HTTP/1.1 over the API unix socket) and process handle.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct Firecracker {
    pub sock: PathBuf,
    child: Child,
}

impl Firecracker {
    pub fn spawn(bin: &Path, id: &str, sock: &Path, log: &Path) -> Result<Self> {
        let _ = std::fs::remove_file(sock);
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)?;
        let child = Command::new(bin)
            .arg("--api-sock")
            .arg(sock)
            .arg("--id")
            .arg(id)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("spawning {}", bin.display()))?;
        let fc = Self {
            sock: sock.to_path_buf(),
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while UnixStream::connect(sock).is_err() {
            if Instant::now() > deadline {
                bail!("firecracker API socket never came up");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(fc)
    }

    pub fn put(&self, path: &str, body: Value) -> Result<()> {
        self.request("PUT", path, body)
    }

    pub fn patch(&self, path: &str, body: Value) -> Result<()> {
        self.request("PATCH", path, body)
    }

    fn request(&self, method: &str, path: &str, body: Value) -> Result<()> {
        let mut s = UnixStream::connect(&self.sock)?;
        let b = body.to_string();
        write!(
            s,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n{b}",
            b.len()
        )?;
        let mut r = BufReader::new(s);
        let mut status = String::new();
        r.read_line(&mut status)?;
        let mut len = 0usize;
        loop {
            let mut h = String::new();
            r.read_line(&mut h)?;
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                len = v.trim().parse().unwrap_or(0);
            }
        }
        let mut resp = vec![0; len];
        r.read_exact(&mut resp)?;
        let code: u16 = status
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        if !(200..300).contains(&code) {
            bail!(
                "{method} {path} -> {code}: {}",
                String::from_utf8_lossy(&resp)
            );
        }
        Ok(())
    }

    pub fn boot(&self, cfg: &BootConfig) -> Result<()> {
        self.put(
            "/machine-config",
            json!({"vcpu_count": cfg.vcpus, "mem_size_mib": cfg.memory_mib}),
        )?;
        self.put(
            "/boot-source",
            json!({"kernel_image_path": cfg.kernel, "boot_args": cfg.boot_args}),
        )?;
        self.put(
            "/drives/rootfs",
            json!({"drive_id": "rootfs", "path_on_host": cfg.rootfs, "is_root_device": true, "is_read_only": false}),
        )?;
        if let Some(s) = &cfg.secrets {
            self.put(
                "/drives/secrets",
                json!({"drive_id": "secrets", "path_on_host": s, "is_root_device": false, "is_read_only": true}),
            )?;
        }
        self.put(
            "/network-interfaces/eth0",
            json!({"iface_id": "eth0", "guest_mac": cfg.mac, "host_dev_name": cfg.tap}),
        )?;
        self.put("/actions", json!({"action_type": "InstanceStart"}))
    }

    pub fn snapshot(&self, snap: &Path, mem: &Path) -> Result<()> {
        self.patch("/vm", json!({"state": "Paused"}))?;
        self.put(
            "/snapshot/create",
            json!({"snapshot_type": "Full", "snapshot_path": snap, "mem_file_path": mem}),
        )
    }

    pub fn restore(&self, snap: &Path, mem: &Path) -> Result<()> {
        self.put(
            "/snapshot/load",
            json!({
                "snapshot_path": snap,
                "mem_backend": {"backend_type": "File", "backend_path": mem},
                "resume_vm": true
            }),
        )
    }

    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.sock);
    }

    pub fn exited(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)))
    }
}

pub struct BootConfig {
    pub kernel: PathBuf,
    pub boot_args: String,
    pub rootfs: PathBuf,
    pub secrets: Option<PathBuf>,
    pub tap: String,
    pub mac: String,
    pub vcpus: u8,
    pub memory_mib: u32,
}
