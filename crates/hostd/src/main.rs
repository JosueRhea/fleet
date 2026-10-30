mod agent;
mod firecracker;
mod image;
mod net;
mod vm;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use vm::{wait_ready, Paths, Vm};

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
    /// Run the instances in a host doc (from the control plane) and write status reports.
    Agent {
        #[arg(long)]
        doc: PathBuf,
        #[arg(long)]
        status: PathBuf,
    },
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
    /// HTTP path that must answer 2xx/3xx for readiness and liveness; plain TCP connect if unset.
    #[serde(default)]
    health_path: Option<String>,
    #[serde(default = "default_ready_timeout")]
    ready_timeout_secs: u64,
    /// How long a replaced instance may keep finishing in-flight connections.
    #[serde(default = "default_drain")]
    drain_secs: u64,
    #[serde(default = "default_liveness")]
    liveness_secs: u64,
}

fn default_ready_timeout() -> u64 {
    60
}
fn default_drain() -> u64 {
    30
}
fn default_liveness() -> u64 {
    5
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
    slots: std::sync::Mutex<HashSet<u8>>,
    next_gen: AtomicU64,
}

impl Ctx {
    fn alloc_slot(&self) -> Result<u8> {
        let mut used = self.slots.lock().unwrap();
        let idx = (1..=249u8).find(|i| !used.contains(i)).context("out of VM slots")?;
        used.insert(idx);
        Ok(idx)
    }
    fn gen_id(&self, name: &str) -> String {
        format!("{name}-{}", self.next_gen.fetch_add(1, Ordering::SeqCst))
    }
    fn free_slot(&self, idx: u8) {
        self.slots.lock().unwrap().remove(&idx);
    }

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
    let head = std::fs::read(&init)?;
    if head.windows(8).take(4096).any(|w| w == b"/lib64/l" || w == b"/lib/ld-") {
        bail!("guest init {} is dynamically linked; build it for musl (scripts/build.sh)", init.display());
    }
    if !cli.kernel.exists() {
        bail!("guest kernel not found at {} (run scripts/fetch-kernel.sh)", cli.kernel.display());
    }
    let ctx = Arc::new(Ctx {
        slots: Default::default(),
        next_gen: Default::default(),
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
        Cmd::Agent { doc, status } => agent::run(ctx, doc, status).await?,
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
    let mut vm = vm;
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

/// Last `n` console lines, skipping the kernel's panic dump that follows init exiting.
fn tail(s: &str, n: usize) -> String {
    let s = s.find("Kernel panic").map_or(s, |i| &s[..i]);
    let lines: Vec<_> = s.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

// ---- `up`: desired-state reconciler, zero-downtime swaps, waking L4 proxy ----

/// One booted microVM serving one version of a service.
struct Instance {
    id: String,
    spec: ServiceSpec,
    vm: Mutex<Vm>,
    rootfs: PathBuf,
    active: AtomicUsize,
    last_activity: std::sync::Mutex<Instant>,
    destroyed: AtomicBool,
}

impl Instance {
    async fn destroy(&self, ctx: &Ctx) {
        if self.destroyed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut vm = self.vm.lock().await;
        let idx = vm.slot.idx;
        tokio::task::block_in_place(|| vm.destroy());
        ctx.free_slot(idx);
    }
}

type Backend = Arc<std::sync::RwLock<Arc<Instance>>>;

/// A service on this host: a stable public listener whose backend instance gets swapped on release.
struct Running {
    spec: ServiceSpec,
    backend: Backend,
    accept: JoinHandle<()>,
    watch: JoinHandle<()>,
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
    let _ = std::fs::remove_dir_all(ctx.state_dir.join("vms"));
    std::fs::create_dir_all(&ctx.state_dir)?;
    let last_good = ctx.state_dir.join("last-good-spec.json");
    let mut running: HashMap<String, Running> = HashMap::new();
    // A spec that failed to deploy is not retried until it changes.
    let mut failed: HashMap<String, ServiceSpec> = HashMap::new();
    let mut retired: Vec<Arc<Instance>> = Vec::new();
    let mut drains = tokio::task::JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(3));
    use tokio::signal::unix::{signal, SignalKind};
    let (mut sigint, mut sigterm) = (signal(SignalKind::interrupt())?, signal(SignalKind::terminate())?);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        }
        while drains.try_join_next().is_some() {}
        retired.retain(|i| !i.destroyed.load(Ordering::SeqCst));
        let spec = match load_spec(&spec_path, &last_good) {
            Ok(s) => s,
            Err(e) => {
                log(format!("no usable spec: {e:#}"));
                continue;
            }
        };
        let wanted: HashMap<_, _> = spec.services.iter().map(|s| (s.name.clone(), s.clone())).collect();
        let gone: Vec<String> = running.keys().filter(|n| !wanted.contains_key(*n)).cloned().collect();
        for name in gone {
            stop_service(&ctx, running.remove(&name).unwrap()).await;
            log(format!("{name}: removed"));
        }
        failed.retain(|n, s| wanted.get(n) == Some(s));
        for (name, s) in wanted {
            if failed.get(&name) == Some(&s) {
                continue;
            }
            match running.get_mut(&name) {
                Some(r) if r.spec == s => {}
                Some(r) if r.spec.public_port == s.public_port => {
                    log(format!("{name}: releasing {} (old instance keeps serving)", s.image));
                    let t = Instant::now();
                    match boot_instance(&ctx, &s, ctx.gen_id(&s.name)).await {
                        Ok(new) => {
                            let old = std::mem::replace(&mut *r.backend.write().unwrap(), new.clone());
                            r.watch.abort();
                            r.watch = tokio::spawn(watchdog(ctx.clone(), new.clone()));
                            r.spec = s.clone();
                            log(format!(
                                "{name}: traffic switched {} -> {} after {}; draining old (up to {}s)",
                                old.id,
                                new.id,
                                ms(t.elapsed()),
                                s.drain_secs
                            ));
                            retired.push(old.clone());
                            drains.spawn(drain(ctx.clone(), old, Duration::from_secs(s.drain_secs)));
                        }
                        Err(e) => {
                            log(format!("{name}: release failed, old instance keeps serving: {e:#}"));
                            failed.insert(name, s);
                        }
                    }
                }
                _ => {
                    if let Some(r) = running.remove(&name) {
                        stop_service(&ctx, r).await;
                    }
                    match start_service(ctx.clone(), s.clone()).await {
                        Ok(r) => {
                            running.insert(name, r);
                        }
                        Err(e) => {
                            log(format!("{name}: deploy failed: {e:#}"));
                            failed.insert(name, s);
                        }
                    }
                }
            }
        }
    }
    log("shutting down");
    drains.shutdown().await;
    for (_, r) in running.drain() {
        stop_service(&ctx, r).await;
    }
    for i in retired {
        i.destroy(&ctx).await;
    }
    Ok(())
}

/// Boot a new instance and return it only once it passes its readiness check.
async fn boot_instance(ctx: &Arc<Ctx>, spec: &ServiceSpec, id: String) -> Result<Arc<Instance>> {
    let idx = ctx.alloc_slot()?;
    let booted = tokio::task::spawn_blocking({
        let (ctx, spec, id) = (ctx.clone(), spec.clone(), id.clone());
        move || -> Result<(Vm, PathBuf)> {
            let t = Instant::now();
            let img = ctx.image(&spec.image, &spec.env, &spec.name)?;
            log(format!("{id}: image {} ready in {}", spec.image, ms(t.elapsed())));
            let mut vm = Vm::new(&id, idx, &ctx.state_dir, spec.vcpus, spec.memory_mib)?;
            let t = Instant::now();
            let ready = vm.cold_boot(&ctx.paths, &img.rootfs).and_then(|_| {
                wait_ready(&mut vm, spec.port, spec.health_path.as_deref(), Duration::from_secs(spec.ready_timeout_secs))
            });
            if let Err(e) = ready {
                let console = std::fs::read_to_string(vm.log_path()).unwrap_or_default();
                vm.destroy();
                bail!("{e:#}\n--- console ---\n{}", tail(&console, 15));
            }
            log(format!("{id}: cold boot -> ready in {}", ms(t.elapsed())));
            Ok((vm, img.rootfs))
        }
    })
    .await?;
    let (vm, rootfs) = match booted {
        Ok(b) => b,
        Err(e) => {
            ctx.free_slot(idx);
            return Err(e);
        }
    };
    Ok(Arc::new(Instance {
        id,
        spec: spec.clone(),
        vm: Mutex::new(vm),
        rootfs,
        active: AtomicUsize::new(0),
        last_activity: std::sync::Mutex::new(Instant::now()),
        destroyed: AtomicBool::new(false),
    }))
}

async fn start_service(ctx: Arc<Ctx>, spec: ServiceSpec) -> Result<Running> {
    let listener = TcpListener::bind(("0.0.0.0", spec.public_port)).await?;
    let inst = boot_instance(&ctx, &spec, ctx.gen_id(&spec.name)).await?;
    log(format!("{}: serving 0.0.0.0:{} -> {}", spec.name, spec.public_port, inst.id));
    let backend: Backend = Arc::new(std::sync::RwLock::new(inst.clone()));
    let accept = tokio::spawn(accept_loop(ctx.clone(), backend.clone(), listener));
    let watch = tokio::spawn(watchdog(ctx, inst));
    Ok(Running { spec, backend, accept, watch })
}

async fn stop_service(ctx: &Ctx, r: Running) {
    r.accept.abort();
    r.watch.abort();
    let _ = r.accept.await;
    let _ = r.watch.await;
    let inst = r.backend.read().unwrap().clone();
    inst.destroy(ctx).await;
}

/// Let a replaced instance finish its in-flight connections, then destroy it.
async fn drain(ctx: Arc<Ctx>, old: Arc<Instance>, timeout: Duration) {
    let t = Instant::now();
    while old.active.load(Ordering::SeqCst) > 0 && t.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cut = old.active.load(Ordering::SeqCst);
    log(format!("{}: drained in {} ({cut} connections cut); destroying", old.id, ms(t.elapsed())));
    old.destroy(&ctx).await;
}

async fn accept_loop(ctx: Arc<Ctx>, backend: Backend, listener: TcpListener) {
    loop {
        let Ok((mut client, _)) = listener.accept().await else { continue };
        let inst = {
            let b = backend.read().unwrap();
            b.active.fetch_add(1, Ordering::SeqCst);
            b.clone()
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let res = async {
                let guest = ensure_awake(&ctx, &inst).await?;
                let mut upstream = tokio::net::TcpStream::connect(guest).await?;
                tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
                anyhow::Ok(())
            }
            .await;
            if let Err(e) = res {
                let benign = e.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset)
                });
                if !benign {
                    log(format!("{}: proxy error: {e:#}", inst.id));
                }
            }
            *inst.last_activity.lock().unwrap() = Instant::now();
            inst.active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

async fn ensure_awake(ctx: &Ctx, inst: &Instance) -> Result<SocketAddr> {
    let mut vm = inst.vm.lock().await;
    if inst.destroyed.load(Ordering::SeqCst) {
        bail!("instance {} is gone", inst.id);
    }
    let addr = SocketAddr::from((vm.slot.guest_ip(), inst.spec.port));
    if vm.is_running() {
        return Ok(addr);
    }
    let t = Instant::now();
    tokio::task::block_in_place(|| -> Result<()> {
        if vm.has_snapshot() {
            vm.wake(&ctx.paths)
        } else {
            vm.cold_boot(&ctx.paths, &inst.rootfs)
        }?;
        wait_ready(&mut vm, inst.spec.port, None, Duration::from_secs(inst.spec.ready_timeout_secs)).map(|_| ())
    })?;
    log(format!("{}: woke on request in {}", inst.id, ms(t.elapsed())));
    Ok(addr)
}

/// Local restart on crash or failed liveness, plus idle sleep. No control plane involved.
async fn watchdog(ctx: Arc<Ctx>, inst: Arc<Instance>) {
    let idle = Duration::from_secs(inst.spec.idle_sleep_secs);
    let every = Duration::from_secs(inst.spec.liveness_secs.max(1));
    let mut last_probe = Instant::now();
    let mut fails = 0u32;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut restart = false;
        if let Some(path) = inst.spec.health_path.clone().filter(|_| last_probe.elapsed() >= every) {
            last_probe = Instant::now();
            let target = {
                let vm = inst.vm.lock().await;
                vm.is_running().then(|| SocketAddr::from((vm.slot.guest_ip(), inst.spec.port)))
            };
            if let Some(addr) = target {
                let ok = tokio::task::spawn_blocking(move || vm::probe(addr, Some(&path), Duration::from_secs(2)))
                    .await
                    .unwrap_or(false);
                fails = if ok { 0 } else { fails + 1 };
                if fails > 0 {
                    log(format!("{}: liveness check failed ({fails}/3)", inst.id));
                }
                restart = fails >= 3;
            }
        }
        let mut vm = inst.vm.lock().await;
        if inst.destroyed.load(Ordering::SeqCst) {
            return;
        }
        if vm.crashed() {
            log(format!("{}: VM exited; cold booting", inst.id));
            restart = true;
        } else if restart {
            log(format!("{}: unhealthy; restarting", inst.id));
        }
        if restart {
            fails = 0;
            let (port, hp, to) = (inst.spec.port, inst.spec.health_path.clone(), Duration::from_secs(inst.spec.ready_timeout_secs));
            let res = tokio::task::block_in_place(|| {
                vm.cold_boot(&ctx.paths, &inst.rootfs).and_then(|_| wait_ready(&mut vm, port, hp.as_deref(), to))
            });
            if let Err(e) = res {
                log(format!("{}: restart failed: {e:#}", inst.id));
            }
            last_probe = Instant::now();
            continue;
        }
        let quiet = inst.last_activity.lock().unwrap().elapsed();
        if !idle.is_zero() && vm.is_running() && inst.active.load(Ordering::SeqCst) == 0 && quiet > idle {
            let t = Instant::now();
            match tokio::task::block_in_place(|| vm.sleep()) {
                Ok(()) => log(format!("{}: idle {}s, snapshotted and stopped in {}", inst.id, quiet.as_secs(), ms(t.elapsed()))),
                Err(e) => log(format!("{}: sleep failed: {e:#}", inst.id)),
            }
        }
    }
}
