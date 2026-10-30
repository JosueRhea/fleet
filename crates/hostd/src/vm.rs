//! One workload = one microVM: cold boot, sleep (snapshot to disk + kill), wake (restore).

use crate::firecracker::{BootConfig, Firecracker};
use crate::net::Slot;
use anyhow::{bail, Result};
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

    pub fn destroy(mut self) {
        self.halt();
        self.slot.destroy();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Poll until something accepts TCP on `addr`; returns how long it took.
pub fn wait_tcp(addr: SocketAddr, timeout: Duration) -> Result<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return Ok(start.elapsed());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    bail!("{addr} not reachable after {timeout:?}")
}
