mod firecracker;
mod image;
mod net;
mod vm;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use vm::{wait_tcp, Paths, Vm};

#[derive(Parser)]
#[command(about = "ccloud host agent")]
struct Cli {
    #[arg(long, default_value = "/var/lib/ccloud", global = true)]
    state_dir: PathBuf,
    #[arg(long, default_value = "/usr/local/bin/firecracker", global = true)]
    firecracker: PathBuf,
    #[arg(long, env = "CCLOUD_KERNEL", default_value = "/var/lib/ccloud/vmlinux", global = true)]
    kernel: PathBuf,
    /// Guest PID 1 binary; defaults to `microinit` next to this executable.
    #[arg(long, env = "CCLOUD_INIT", global = true)]
    init: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Convert an OCI image into a bootable rootfs.
    Image { reference: String },
    /// Run services from a desired-state file; re-reads it and reconciles every few seconds.
    Up { spec: PathBuf },
    /// Measure cold boot vs snapshot sleep/wake for one image.
    Bench {
        image: String,
        #[arg(long)]
        port: u16,
        #[arg(long, default_value_t = 5)]
        runs: usize,
        #[arg(long, default_value_t = 256)]
        memory_mib: u32,
        #[arg(long, default_value_t = 1)]
        vcpus: u8,
    },
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct Spec {
    services: Vec<ServiceSpec>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct ServiceSpec {
    name: String,
    image: String,
    /// Port the app listens on inside the VM.
    port: u16,
    /// Port on the host that proxies to the VM (and wakes it).
    public_port: u16,
    #[serde(default = "default_vcpus")]
    vcpus: u8,
    #[serde(default = "default_mem")]
    memory_mib: u32,
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// Snapshot and stop the VM after this many idle seconds; 0 = never.
    #[serde(default)]
    idle_sleep_secs: u64,
}

fn default_vcpus() -> u8 {
    1
}
fn default_mem() -> u32 {
    256
}

struct Ctx {
    paths: Paths,
    init: PathBuf,
    state_dir: PathBuf,
}

impl Ctx {
    fn image(&self, reference: &str, env: &BTreeMap<String, String>, hostname: &str) -> Result<image::Image> {
        let env: Vec<_> = env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        image::prepare(reference, &env, hostname, &self.init, &self.state_dir.join("images"))
    }
}

fn log(msg: impl AsRef<str>) {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    eprintln!("[{}.{:03}] {}", t.as_secs(), t.subsec_millis(), msg.as_ref());
}

fn ms(d: Duration) -> String {
    format!("{:.1}ms", d.as_secs_f64() * 1000.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if unsafe { libc_geteuid() } != 0 {
        bail!("hostd needs root (TAP devices, iptables, image extraction)");
    }
    let init = match cli.init {
        Some(p) => p,
        None => std::env::current_exe()?.with_file_name("microinit"),
    };
    if !init.exists() {
        bail!("guest init not found at {} (run scripts/build.sh)", init.display());
    }
    if !cli.kernel.exists() {
        bail!("guest kernel not found at {} (run scripts/fetch-kernel.sh)", cli.kernel.display());
    }
    let ctx = Arc::new(Ctx {
        paths: Paths { firecracker: cli.firecracker, kernel: cli.kernel },
        init,
        state_dir: cli.state_dir,
    });
    match cli.cmd {
        Cmd::Image { reference } => {
            let t = Instant::now();
            let img = ctx.image(&reference, &BTreeMap::new(), "app")?;
            println!("{} ({}) in {}", img.rootfs.display(), img.id, ms(t.elapsed()));
        }
        Cmd::Bench { image, port, runs, memory_mib, vcpus } => {
            tokio::task::spawn_blocking(move || bench(&ctx, &image, port, runs, vcpus, memory_mib)).await??
        }
        Cmd::Up { spec } => up(ctx, spec).await?,
    }
    Ok(())
}

extern "C" {
    #[link_name = "geteuid"]
    fn libc_geteuid() -> u32;
}

/// Time from sending a request to receiving the first response bytes.
fn http_get(addr: SocketAddr, timeout: Duration) -> Result<(Duration, String)> {
    let start = Instant::now();
    loop {
        let attempt = (|| -> Result<String> {
            let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(100))?;
            s.set_read_timeout(Some(Duration::from_secs(5)))?;
            write!(s, "GET / HTTP/1.0\r\nHost: app\r\n\r\n")?;
            let mut buf = [0u8; 512];
            let n = s.read(&mut buf)?;
            if n == 0 {
                bail!("empty response");
            }
            Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
        })();
        match attempt {
            Ok(body) => return Ok((start.elapsed(), body)),
            Err(e) if start.elapsed() > timeout => return Err(e.context(format!("{addr} never answered"))),
            Err(_) => std::thread::sleep(Duration::from_millis(2)),
        }
    }
}

fn bench(ctx: &Ctx, reference: &str, port: u16, runs: usize, vcpus: u8, memory_mib: u32) -> Result<()> {
    net::init_host()?;
    let t = Instant::now();
    let img = ctx.image(reference, &BTreeMap::new(), "bench")?;
    log(format!("image ready in {} -> {}", ms(t.elapsed()), img.rootfs.display()));
    let mut vm = Vm::new("bench", 250, &ctx.state_dir, vcpus, memory_mib)?;
    let addr = SocketAddr::from((vm.slot.guest_ip(), port));
    let (mut cold, mut sleep, mut wake) = (vec![], vec![], vec![]);
    let result = (|| -> Result<()> {
        for i in 0..runs {
            let t = Instant::now();
            vm.cold_boot(&ctx.paths, &img.rootfs)?;
            let (_, first) = http_get(addr, Duration::from_secs(60))?;
            let c = t.elapsed();
            std::thread::sleep(Duration::from_millis(500));
            let t = Instant::now();
            vm.sleep()?;
            let s = t.elapsed();
            let t = Instant::now();
            vm.wake(&ctx.paths)?;
            let (_, second) = http_get(addr, Duration::from_secs(10))?;
            let w = t.elapsed();
            log(format!(
                "run {}: cold boot -> first response {} | sleep {} | wake -> first response {} | status {:?}",
                i + 1,
                ms(c),
                ms(s),
                ms(w),
                second.lines().next().unwrap_or("")
            ));
            if i == 0 {
                log(format!("first response: {:?}", first.lines().next().unwrap_or("")));
            }
            cold.push(c);
            sleep.push(s);
            wake.push(w);
            vm.halt();
        }
        Ok(())
    })();
    let mem = std::fs::metadata(vm.dir.join("vm.mem")).map(|m| m.len()).unwrap_or(0);
    let log_tail = std::fs::read_to_string(vm.log_path()).unwrap_or_default();
    vm.destroy();
    if let Err(e) = result {
        eprintln!("--- console ---\n{}", tail(&log_tail, 40));
        return Err(e);
    }
    let med = |v: &mut Vec<Duration>| {
        v.sort();
        v[v.len() / 2]
    };
    println!(
        "{}",
        serde_json::json!({
            "image": reference,
            "vcpus": vcpus,
            "memory_mib": memory_mib,
            "runs": runs,
            "median_ms": {
                "cold_boot_to_first_response": med(&mut cold).as_secs_f64() * 1e3,
                "sleep_snapshot": med(&mut sleep).as_secs_f64() * 1e3,
                "wake_to_first_response": med(&mut wake).as_secs_f64() * 1e3,
            },
            "memory_snapshot_bytes": mem,
        })
    );
    Ok(())
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<_> = s.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

// ---- `up`: desired-state reconciler + waking L4 proxy ----

struct Service {
    spec: ServiceSpec,
    vm: Mutex<Vm>,
    rootfs: PathBuf,
    active: AtomicUsize,
    last_activity: std::sync::Mutex<Instant>,
}

struct Running {
    svc: Arc<Service>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Read the spec; if it is missing or broken, fall back to the last spec that worked.
fn load_spec(path: &Path, last_good: &Path) -> Result<Spec> {
    let fresh = std::fs::read(path)
        .context("reading spec")
        .and_then(|b| serde_json::from_slice::<Spec>(&b).context("parsing spec"));
    match fresh {
        Ok(spec) => {
            std::fs::write(last_good, serde_json::to_vec_pretty(&spec)?)?;
            Ok(spec)
        }
        Err(e) => {
            let spec = serde_json::from_slice(&std::fs::read(last_good).context("no last-known-good spec")?)?;
            log(format!("spec unusable ({e:#}); keeping last-known-good"));
            Ok(spec)
        }
    }
}

async fn up(ctx: Arc<Ctx>, spec_path: PathBuf) -> Result<()> {
    net::init_host()?;
    net::remove_stale_taps();
    std::fs::create_dir_all(&ctx.state_dir)?;
    let last_good = ctx.state_dir.join("last-good-spec.json");
    let mut running: HashMap<String, Running> = HashMap::new();
    let mut last_applied: Option<Spec> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(3));
    use tokio::signal::unix::{signal, SignalKind};
    let (mut sigint, mut sigterm) = (signal(SignalKind::interrupt())?, signal(SignalKind::terminate())?);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        }
        let spec = match load_spec(&spec_path, &last_good) {
            Ok(s) => s,
            Err(e) => {
                log(format!("no usable spec: {e:#}"));
                continue;
            }
        };
        if last_applied.as_ref() == Some(&spec) {
            continue;
        }
        let wanted: HashMap<_, _> = spec.services.iter().map(|s| (s.name.clone(), s.clone())).collect();
        let stale: Vec<String> = running
            .iter()
            .filter(|(name, r)| wanted.get(*name) != Some(&r.svc.spec))
            .map(|(n, _)| n.clone())
            .collect();
        for name in stale {
            stop(running.remove(&name).unwrap()).await;
            log(format!("{name}: stopped"));
        }
        for (name, s) in wanted {
            if running.contains_key(&name) {
                continue;
            }
            let used: Vec<u8> = running.values().map(|r| r.svc.vm.try_lock().map(|v| v.slot.idx).unwrap_or(0)).collect();
            let idx = (1..=249u8).find(|i| !used.contains(i)).context("out of VM slots")?;
            match start(ctx.clone(), s, idx).await {
                Ok(r) => {
                    running.insert(name, r);
                }
                Err(e) => log(format!("{name}: deploy failed: {e:#}")),
            }
        }
        last_applied = Some(spec);
    }
    log("shutting down");
    for (_, r) in running.drain() {
        stop(r).await;
    }
    Ok(())
}

async fn stop(r: Running) {
    for t in &r.tasks {
        t.abort();
    }
    for t in r.tasks {
        let _ = t.await;
    }
    if let Ok(svc) = Arc::try_unwrap(r.svc) {
        let vm = svc.vm.into_inner();
        tokio::task::spawn_blocking(move || vm.destroy()).await.ok();
    }
}

async fn start(ctx: Arc<Ctx>, spec: ServiceSpec, idx: u8) -> Result<Running> {
    let name = spec.name.clone();
    let listener = TcpListener::bind(("0.0.0.0", spec.public_port)).await?;
    let (vm, rootfs) = tokio::task::spawn_blocking({
        let (ctx, spec) = (ctx.clone(), spec.clone());
        move || -> Result<(Vm, PathBuf)> {
            let t = Instant::now();
            let img = ctx.image(&spec.image, &spec.env, &spec.name)?;
            log(format!("{}: image {} ready in {}", spec.name, spec.image, ms(t.elapsed())));
            let mut vm = Vm::new(&spec.name, idx, &ctx.state_dir, spec.vcpus, spec.memory_mib)?;
            let t = Instant::now();
            vm.cold_boot(&ctx.paths, &img.rootfs)?;
            let addr = SocketAddr::from((vm.slot.guest_ip(), spec.port));
            if let Err(e) = wait_tcp(addr, Duration::from_secs(60)) {
                let console = std::fs::read_to_string(vm.log_path()).unwrap_or_default();
                vm.destroy();
                bail!("{e:#}\n--- console ---\n{}", tail(&console, 30));
            }
            log(format!("{}: cold boot -> listening on {addr} in {}", spec.name, ms(t.elapsed())));
            Ok((vm, img.rootfs))
        }
    })
    .await??;
    log(format!("{name}: serving 0.0.0.0:{} -> {}:{}", spec.public_port, vm.slot.guest_ip(), spec.port));
    let svc = Arc::new(Service {
        spec,
        vm: Mutex::new(vm),
        rootfs,
        active: AtomicUsize::new(0),
        last_activity: std::sync::Mutex::new(Instant::now()),
    });
    let accept = tokio::spawn(accept_loop(ctx.clone(), svc.clone(), listener));
    let watch = tokio::spawn(watchdog(ctx, svc.clone()));
    Ok(Running { svc, tasks: vec![accept, watch] })
}

async fn accept_loop(ctx: Arc<Ctx>, svc: Arc<Service>, listener: TcpListener) {
    loop {
        let Ok((mut client, _)) = listener.accept().await else { continue };
        let (ctx, svc) = (ctx.clone(), svc.clone());
        tokio::spawn(async move {
            svc.active.fetch_add(1, Ordering::SeqCst);
            let res = async {
                let guest = ensure_awake(&ctx, &svc).await?;
                let mut upstream = tokio::net::TcpStream::connect(guest).await?;
                tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
                anyhow::Ok(())
            }
            .await;
            if let Err(e) = res {
                log(format!("{}: proxy error: {e:#}", svc.spec.name));
            }
            *svc.last_activity.lock().unwrap() = Instant::now();
            svc.active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

async fn ensure_awake(ctx: &Ctx, svc: &Service) -> Result<SocketAddr> {
    let mut vm = svc.vm.lock().await;
    let addr = SocketAddr::from((vm.slot.guest_ip(), svc.spec.port));
    if vm.is_running() {
        return Ok(addr);
    }
    let t = Instant::now();
    tokio::task::block_in_place(|| -> Result<()> {
        if vm.has_snapshot() {
            vm.wake(&ctx.paths)
        } else {
            vm.cold_boot(&ctx.paths, &svc.rootfs)
        }?;
        wait_tcp(addr, Duration::from_secs(60)).map(|_| ())
    })?;
    log(format!("{}: woke on request in {}", svc.spec.name, ms(t.elapsed())));
    Ok(addr)
}

/// Local restart + idle sleep. Runs on the host with no control plane involved.
async fn watchdog(ctx: Arc<Ctx>, svc: Arc<Service>) {
    let idle = Duration::from_secs(svc.spec.idle_sleep_secs);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut vm = svc.vm.lock().await;
        if vm.crashed() {
            log(format!("{}: VM exited; cold booting", svc.spec.name));
            if let Err(e) = tokio::task::block_in_place(|| vm.cold_boot(&ctx.paths, &svc.rootfs)) {
                log(format!("{}: restart failed: {e:#}", svc.spec.name));
            }
            continue;
        }
        let quiet = svc.last_activity.lock().unwrap().elapsed();
        if !idle.is_zero() && vm.is_running() && svc.active.load(Ordering::SeqCst) == 0 && quiet > idle {
            let t = Instant::now();
            match tokio::task::block_in_place(|| vm.sleep()) {
                Ok(()) => log(format!("{}: idle {}s, snapshotted and stopped in {}", svc.spec.name, quiet.as_secs(), ms(t.elapsed()))),
                Err(e) => log(format!("{}: sleep failed: {e:#}", svc.spec.name)),
            }
        }
    }
}

