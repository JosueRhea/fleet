//! `hostd agent`: runs the instances in the host doc and reports their state. It does no routing;
//! the edge sends traffic to Ready instances. Nothing here needs the control plane to be up.

use super::*;
use ccloud_proto::{Desired, HostDoc, InstanceSpec, InstanceState, InstanceStatus};

pub struct Config {
    pub doc: proto::Loc,
    pub status: proto::Loc,
    pub trust: proto::Trust,
    pub host_id: String,
    pub memory_mib: u32,
    pub box_key: proto::BoxKey,
}

struct Managed {
    spec: InstanceSpec,
    state: InstanceState,
    error: Option<String>,
    addr: Option<String>,
    boot: Option<JoinHandle<Result<Arc<Instance>>>>,
    inst: Option<Arc<Instance>>,
    watch: Option<JoinHandle<()>>,
}

fn service_spec(i: &InstanceSpec, key: &proto::BoxKey) -> Result<ServiceSpec> {
    if let Some(f) = i.unsupported_feature() {
        anyhow::bail!("instance requires {f:?}, which this hostd does not support (upgrade hostd)");
    }
    let secret_env = match &i.sealed_env {
        Some(sealed) => key.unseal_env(sealed)?,
        None => BTreeMap::new(),
    };
    Ok(ServiceSpec {
        name: i.id.clone(),
        image: i.image.clone(),
        port: i.port,
        public_port: 0,
        vcpus: i.vcpus,
        memory_mib: i.memory_mib,
        env: i.env.clone(),
        idle_sleep_secs: 0,
        health_path: i.health.path.clone(),
        ready_timeout_secs: i.health.ready_timeout_secs,
        drain_secs: 0,
        liveness_secs: i.health.liveness_secs,
        secret_env,
    })
}

/// Instances are immutable; only `desired` may change. Removal only via tombstone.
fn merge(wanted: &mut BTreeMap<String, InstanceSpec>, doc: HostDoc) {
    for i in doc.instances {
        wanted.insert(i.id.clone(), i);
    }
    for t in &doc.tombstones {
        wanted.remove(t);
    }
}

async fn teardown(ctx: &Ctx, m: Managed) {
    if let Some(w) = m.watch {
        w.abort();
        let _ = w.await;
    }
    let inst = match m.boot {
        Some(b) => b.await.ok().and_then(|r| r.ok()),
        None => m.inst,
    };
    if let Some(inst) = inst {
        inst.destroy(ctx).await;
    }
}

pub async fn run(ctx: Arc<Ctx>, cfg: Config) -> Result<()> {
    net::init_host()?;
    crate::vm::reap_stale(&ctx.state_dir.join("vms"));
    net::remove_stale_taps(&ctx.slot_range);
    let _ = std::fs::remove_dir_all(ctx.state_dir.join("vms"));
    std::fs::create_dir_all(&ctx.state_dir)?;
    let last_good = ctx.state_dir.join("last-good-host.json");
    let mut applied = 0;
    let mut wanted = BTreeMap::new();
    if let Some(doc) = proto::load_last_good::<HostDoc>(&last_good, &cfg.trust) {
        log(format!(
            "restoring last-known-good host doc v{}",
            doc.version
        ));
        applied = doc.version;
        merge(&mut wanted, doc);
    }
    let mut managed: HashMap<String, Managed> = HashMap::new();
    let mut last_err = String::new();
    let (mut last_report, mut reported_at) = (None::<proto::HostStatus>, 0u64);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    use tokio::signal::unix::{signal, SignalKind};
    let (mut sigint, mut sigterm) = (
        signal(SignalKind::interrupt())?,
        signal(SignalKind::terminate())?,
    );
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        }
        match proto::load_newer::<HostDoc>(&cfg.doc, &cfg.trust, &last_good, applied).await {
            Ok(Some(doc)) => {
                log(format!(
                    "applied host doc v{} ({} instances, {} tombstones)",
                    doc.version,
                    doc.instances.len(),
                    doc.tombstones.len()
                ));
                applied = doc.version;
                merge(&mut wanted, doc);
                last_err.clear();
            }
            Ok(None) => {}
            Err(e) => {
                let e = format!("{e:#}");
                if e != last_err {
                    log(format!("host doc unusable ({e}); keeping v{applied}"));
                    last_err = e;
                }
            }
        }
        let gone: Vec<String> = managed
            .keys()
            .filter(|id| !wanted.contains_key(*id))
            .cloned()
            .collect();
        for id in gone {
            teardown(&ctx, managed.remove(&id).unwrap()).await;
            log(format!("{id}: stopped (tombstoned)"));
        }
        for (id, spec) in &wanted {
            let m = managed.entry(id.clone()).or_insert_with(|| {
                log(format!(
                    "{id}: booting {} (deployment {})",
                    spec.image, spec.deployment
                ));
                let s = match service_spec(spec, &cfg.box_key) {
                    Ok(s) => s,
                    Err(e) => {
                        log(format!("{id}: {e:#}"));
                        return Managed {
                            spec: spec.clone(),
                            state: InstanceState::Failed,
                            error: Some(format!("{e:#}")),
                            addr: None,
                            boot: None,
                            inst: None,
                            watch: None,
                        };
                    }
                };
                let ctx = ctx.clone();
                let iid = id.clone();
                Managed {
                    spec: spec.clone(),
                    state: InstanceState::Booting,
                    error: None,
                    addr: None,
                    boot: Some(tokio::spawn(
                        async move { boot_instance(&ctx, &s, iid).await },
                    )),
                    inst: None,
                    watch: None,
                }
            });
            if m.boot.as_ref().is_some_and(|b| b.is_finished()) {
                match m.boot.take().unwrap().await {
                    Ok(Ok(inst)) => {
                        let ip = inst.vm.lock().await.slot.guest_ip();
                        m.addr = Some(format!("{ip}:{}", m.spec.port));
                        m.state = InstanceState::Ready;
                        m.watch = Some(tokio::spawn(watchdog(ctx.clone(), inst.clone())));
                        m.inst = Some(inst);
                        log(format!(
                            "{id}: ready at {}",
                            m.addr.as_deref().unwrap_or("?")
                        ));
                    }
                    Ok(Err(e)) => {
                        m.state = InstanceState::Failed;
                        log(format!(
                            "{id}: failed: {}",
                            format!("{e:#}").lines().next().unwrap_or("")
                        ));
                        m.error = Some(format!("{e:#}"));
                    }
                    Err(e) => {
                        m.state = InstanceState::Failed;
                        m.error = Some(format!("boot task: {e}"));
                    }
                }
            }
            let Some(inst) = m.inst.clone() else { continue };
            match (spec.desired, m.state) {
                (Desired::Sleeping, InstanceState::Ready) => {
                    let t = Instant::now();
                    let mut vm = inst.vm.lock().await;
                    match tokio::task::block_in_place(|| vm.sleep()) {
                        Ok(()) => {
                            m.state = InstanceState::Sleeping;
                            log(format!(
                                "{id}: snapshotted and stopped in {}",
                                ms(t.elapsed())
                            ));
                        }
                        Err(e) => log(format!("{id}: sleep failed: {e:#}")),
                    }
                }
                (Desired::Running, InstanceState::Sleeping) => {
                    let t = Instant::now();
                    let mut vm = inst.vm.lock().await;
                    let (port, hp, to) = (
                        m.spec.port,
                        m.spec.health.path.clone(),
                        Duration::from_secs(m.spec.health.ready_timeout_secs),
                    );
                    let res = tokio::task::block_in_place(|| {
                        vm.wake(&ctx.paths)
                            .and_then(|_| wait_ready(&mut vm, port, hp.as_deref(), to))
                    });
                    match res {
                        Ok(_) => {
                            m.state = InstanceState::Ready;
                            log(format!("{id}: woke and ready in {}", ms(t.elapsed())));
                        }
                        Err(e) => log(format!("{id}: wake failed: {e:#}")),
                    }
                }
                _ => {}
            }
            m.spec.desired = spec.desired;
        }
        let mut instances: Vec<InstanceStatus> = managed
            .iter()
            .map(|(id, m)| InstanceStatus {
                id: id.clone(),
                deployment: m.spec.deployment.clone(),
                state: m.state,
                addr: m.addr.clone(),
                error: m.error.clone(),
            })
            .collect();
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let memory_mib_used = managed
            .values()
            .filter(|m| m.state != InstanceState::Sleeping)
            .map(|m| m.spec.memory_mib)
            .sum();
        let mut st = proto::HostStatus {
            host_id: cfg.host_id.clone(),
            reported_at,
            memory_mib_total: cfg.memory_mib,
            memory_mib_used,
            applied_version: applied,
            instances,
            box_pub: cfg.box_key.public_hex(),
        };
        // Report on change, and every 2 s so the scheduler knows this host is alive.
        let now = proto::unix_now();
        let changed = last_report
            .as_ref()
            .is_none_or(|l| l.applied_version != st.applied_version || l.instances != st.instances);
        if changed || now >= reported_at + 2 {
            st.reported_at = now;
            match cfg.status.put_json(&st).await {
                Ok(()) => (last_report, reported_at) = (Some(st), now),
                Err(e) => log(format!("writing status: {e:#}")),
            }
        }
    }
    log("shutting down");
    for (_, m) in managed.drain() {
        teardown(&ctx, m).await;
    }
    Ok(())
}
