//! One workload = one microVM: cold boot, sleep (snapshot to disk + kill), wake (restore).

use crate::firecracker::{BootConfig, Firecracker};
use crate::net::Slot;
use anyhow::{bail, Result};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct Paths {
    pub firecracker: PathBuf,
    pub kernel: PathBuf,
}

pub struct Vm {
    pub name: String,
    pub slot: Slot,
    pub dir: PathBuf,
    pub vcpus: u8,
    pub memory_mib: u32,
    fc: Option<Firecracker>,
}

impl Vm {
    pub fn new(name: &str, idx: u8, state_dir: &Path, vcpus: u8, memory_mib: u32) -> Result<Self> {
        let dir = state_dir.join("vms").join(name);
        std::fs::create_dir_all(&dir)?;
        let slot = Slot { idx };
        slot.create()?;
        Ok(Self { name: name.into(), slot, dir, vcpus, memory_mib, fc: None })
    }

    pub fn log_path(&self) -> PathBuf {
        self.dir.join("console.log")
    }
    fn sock(&self) -> PathBuf {
        self.dir.join("fc.sock")
    }
    fn snap(&self) -> (PathBuf, PathBuf) {
        (self.dir.join("vm.snap"), self.dir.join("vm.mem"))
    }
    pub fn is_running(&self) -> bool {
        self.fc.is_some()
    }

    /// Fresh writable copy of the image rootfs, then a full kernel boot.
    pub fn cold_boot(&mut self, paths: &Paths, image_rootfs: &Path) -> Result<()> {
        self.halt();
        let rootfs = self.dir.join("rootfs.ext4");
        let ok = std::process::Command::new("cp")
            .args(["--sparse=always", "--reflink=auto"])
            .arg(image_rootfs)
            .arg(&rootfs)
            .status()?
            .success();
        if !ok {
            bail!("copying rootfs failed");
        }
        let fc = Firecracker::spawn(&paths.firecracker, &self.name, &self.sock(), &self.log_path())?;
        fc.boot(&BootConfig {
            kernel: paths.kernel.clone(),
            boot_args: format!(
                "console=ttyS0 reboot=k panic=1 pci=off quiet init=/sbin/microinit {}",
                self.slot.kernel_ip_arg()
            ),
            rootfs,
            tap: self.slot.tap(),
            mac: self.slot.mac(),
            vcpus: self.vcpus,
            memory_mib: self.memory_mib,
        })?;
        self.fc = Some(fc);
        Ok(())
    }

    /// Pause, write a full snapshot (vCPU/device state + guest RAM), and free the host resources.
    pub fn sleep(&mut self) -> Result<()> {
        let Some(fc) = self.fc.take() else { return Ok(()) };
        let (snap, mem) = self.snap();
        let res = fc.snapshot(&snap, &mem);
        fc.kill();
        res
    }

    pub fn has_snapshot(&self) -> bool {
        let (snap, mem) = self.snap();
        snap.exists() && mem.exists()
    }

    pub fn wake(&mut self, paths: &Paths) -> Result<()> {
        if self.fc.is_some() {
            return Ok(());
        }
        let (snap, mem) = self.snap();
        let fc = Firecracker::spawn(&paths.firecracker, &self.name, &self.sock(), &self.log_path())?;
        if let Err(e) = fc.restore(&snap, &mem) {
            fc.kill();
            return Err(e);
        }
        self.fc = Some(fc);
        Ok(())
    }

    pub fn crashed(&mut self) -> bool {
        let exited = self.fc.as_mut().is_some_and(|fc| fc.exited());
        if exited {
            self.fc = None;
        }
        exited
    }

    pub fn halt(&mut self) {
        if let Some(fc) = self.fc.take() {
            fc.kill();
        }
    }

    pub fn destroy(&mut self) {
        self.halt();
        self.slot.destroy();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Startup/readiness gate: polls the health check, failing fast if the VM exits first.
pub fn wait_ready(vm: &mut Vm, port: u16, health_path: Option<&str>, timeout: Duration) -> Result<Duration> {
    let addr = SocketAddr::from((vm.slot.guest_ip(), port));
    let start = Instant::now();
    while start.elapsed() < timeout {
        if vm.crashed() {
            bail!("VM exited during startup after {:?} (app exited or crashed)", start.elapsed());
        }
        if probe(addr, health_path, Duration::from_millis(500)) {
            return Ok(start.elapsed());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    bail!("{addr} not ready after {timeout:?}")
}

/// TCP connect, or an HTTP GET that must answer 2xx/3xx when `path` is set.
pub fn probe(addr: SocketAddr, path: Option<&str>, timeout: Duration) -> bool {
    let Ok(mut s) = TcpStream::connect_timeout(&addr, timeout) else { return false };
    let Some(path) = path else { return true };
    let _ = s.set_read_timeout(Some(timeout));
    let _ = s.set_write_timeout(Some(timeout));
    if write!(s, "GET {path} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 12];
    let mut n = 0;
    while n < buf.len() {
        match s.read(&mut buf[n..]) {
            Ok(0) | Err(_) => break,
            Ok(k) => n += k,
        }
    }
    let line = String::from_utf8_lossy(&buf[..n]);
    line.starts_with("HTTP/") && matches!(line.split_whitespace().nth(1).and_then(|c| c.chars().next()), Some('2' | '3'))
}

/// Kill Firecracker processes left by a previous hostd (killed without cleanup) whose VM dirs live under
/// `vms_dir`. Without this a restarted agent boots fresh copies and the old VMs run on unmanaged.
pub fn reap_stale(vms_dir: &std::path::Path) {
    let prefix = vms_dir.to_string_lossy().into_owned();
    let Ok(procs) = std::fs::read_dir("/proc") else { return };
    for p in procs.flatten() {
        let Some(pid) = p.file_name().to_str().and_then(|s| s.parse::<i32>().ok()) else { continue };
        let Ok(cmd) = std::fs::read(p.path().join("cmdline")) else { continue };
        let args: Vec<String> = cmd.split(|b| *b == 0).map(|a| String::from_utf8_lossy(a).into_owned()).collect();
        let is_fc = args.first().is_some_and(|a| a.ends_with("firecracker"));
        if is_fc && args.windows(2).any(|w| w[0] == "--api-sock" && w[1].starts_with(&prefix)) {
            crate::log(format!("killing stale firecracker pid {pid} from a previous agent"));
            let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
}
