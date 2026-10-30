# ccloud

Single-host spike for a microVM-first Railway alternative. It turns an OCI image into a Firecracker microVM, puts it on its own TAP network, proxies a host port to it, and scales it to zero with memory snapshots.

## What's here

| Piece | What it does |
|---|---|
| `crates/hostd` | Host agent (Rust). Converts images, boots VMs through the Firecracker API, sets up TAP + NAT, runs an L4 proxy that wakes sleeping VMs, and reconciles a desired-state file. |
| `crates/microinit` | Static musl PID 1 for the guest. Mounts `/proc`, `/sys`, `/dev`; brings up loopback; runs the image's entrypoint/cmd/env/workdir/user from `/etc/ccloud/run.json`; reaps zombies; powers off when the app exits. |
| `examples/` | `services.json` desired-state file, and a small Node app that keeps an in-memory counter (to show RAM survives sleep/wake). |

### How a deploy works
1. `docker export` the image, inject `microinit` + `run.json`, then `mkfs.ext4 -d` it into a rootfs. Cached by image ID + run config.
2. Copy the rootfs per VM (sparse/reflink, so it's immutable per deploy). Start Firecracker; configure machine, kernel, drive, and NIC over the API socket.
3. Guest networking comes from the kernel `ip=` arg: each VM gets a `/30` on `172.30.N.0` and a TAP `mrN`, with egress MASQUERADE.
4. A host listener on `public_port` proxies to `guest_ip:port`.

### Zero-downtime releases and health checks
When a service's spec changes (same `public_port`), hostd:
1. boots the new version as a separate instance (`<name>-<gen>`, its own slot/TAP/rootfs) while the old one keeps serving,
2. waits for readiness: `GET health_path` must answer 2xx/3xx (TCP connect if unset), up to `ready_timeout_secs` (60). If the VM exits first the release fails immediately,
3. atomically points the public listener at the new instance (new connections only),
4. drains the old instance: in-flight connections may finish for up to `drain_secs` (30), then the VM is destroyed.

A release that fails readiness is discarded and the old instance keeps serving; that spec is not retried until it changes. With `health_path` set, a liveness probe runs every `liveness_secs` (5); 3 consecutive failures restart the instance locally.

```json
{ "name": "web", "image": "nginx:alpine", "port": 80, "public_port": 8080,
  "health_path": "/", "drain_secs": 30, "ready_timeout_secs": 60, "liveness_secs": 5 }
```

Verified end to end (real Firecracker VMs, curl every 10 ms):
- 2 releases + 1 broken release under load: **2815/2815 requests 200**, ~1.15 s from spec change to traffic switch, broken release (app exits) rejected in ~1.3 s.
- 24 MB download (~8 s) started before a release finished intact on the old instance (drained in 5.3 s); with `drain_secs: 2` it was cut at 2 s.
- Liveness: `/health` starts failing → restarted after 3 failed probes, healthy again.
- Removal and shutdown leave 0 Firecracker processes, 0 TAPs, 0 VM dirs.

### Scale to zero
After `idle_sleep_secs` with no open connections, hostd pauses the VM, writes a full snapshot (device state + RAM), and kills Firecracker. The next connection is held, the snapshot is restored, and then the connection is forwarded.

### Static stability (the main lesson from Railway's incidents)
- `up` re-reads the spec every 3s. If the spec is missing or invalid, hostd keeps running the **last-known-good** copy (`/var/lib/ccloud/last-good-spec.json`) instead of tearing anything down.
- A crashed VM is cold-booted again locally by the watchdog. This needs no control plane.

## Results (nested KVM on an 8-vCPU AWS VM, Firecracker v1.17, guest kernel 6.1, 1 vCPU / 256 MiB)

`hostd bench public.ecr.aws/docker/library/nginx:alpine --port 80 --runs 3`

| | median |
|---|---|
| Cold boot → first HTTP response | **1121 ms** |
| Sleep (pause + full snapshot, 256 MiB) | **187 ms** |
| Wake (restore) → first HTTP response | **18 ms** |
| Image → rootfs (first time) | ~0.5–1.2 s |

From `hostd up examples/services.json`:
- The Node API slept after 5s idle (150–210 ms), and the next request woke it in **11.5 ms**.
- Its in-memory `hits` counter and `pid` carried across every sleep/wake.
- Overwriting the spec with garbage kept both services running.
- `kill`ing the web VM's Firecracker process brought it back via cold boot.
- Removing the web VM from the spec stopped it.

This was measured on nested virtualization; expect bare metal to be faster. Cold boot is dominated by guest userspace startup plus the kernel. The restore cost scales with the size of the snapshotted RAM (it is a full, file-backed snapshot).

## One cell: Temporal Cloud → signed docs in S3 → 2 hostd agents + edge (`e2e/cell.sh`)

The worker is the cell's publisher: it writes ed25519-signed host and edge docs to an S3-compatible bucket (MinIO in the test) and reads the status reports that hostd agents and edges `PUT` there. Each hostd and the edge pull their doc, reject anything that doesn't verify against `--trust`, and keep the last good signed copy on disk. The scheduler spreads replicas across live hosts (hosts that reported in the last 10 s) by free memory.

The test runs two hostd agents on one VM (`--slots 1-100` / `101-200`), with curl through the edge every 10 ms:

| Step | Result | Clients |
|---|---|---|
| v1, then v1 → v2 (2 replicas) | active, one replica per host | 0 errors |
| app crashes on start | rolled back | 0 errors |
| edge doc payload rewritten without re-signing | edge logs `bad signature`, keeps serving; the deploy built on that doc doesn't run | 0 errors |
| host-b killed (agent + its VMs) | edge retries onto host-a; ~10 s later the worker's watcher sees host-b's status go stale and starts a `Repair` workflow, which boots a replacement on host-a, gates on Ready, routes to it and tombstones the lost instance (routes on host-a only ~13 s after the kill, no deploy) | 0 errors |
| deploy right after repair | the CLI waits for any running deploy/repair of the service, re-reads what's running, then rolls out | 0 errors |
| host-b restarted | restores its last-good doc, then applies the newer tombstones and runs nothing | 0 errors |

3,500 requests, all 200. 0 Firecracker processes, TAPs or VM dirs after shutdown.

## Run it

```bash
./scripts/fetch-kernel.sh          # firecracker -> /usr/local/bin, kernel -> /var/lib/ccloud/vmlinux
./scripts/build.sh                 # target/release/{hostd,microinit}
docker build -t hello-node examples/hello-node
sudo ./target/release/hostd bench public.ecr.aws/docker/library/nginx:alpine --port 80
sudo ./target/release/hostd up examples/services.json
curl localhost:8080; curl localhost:8081
```

Requires Linux with `/dev/kvm`, Docker (for pulling/exporting images), `iptables`, `e2fsprogs`, and root.

## Architecture

The core design (components, multi-tenancy, zero-downtime releases, failure behaviour) is in [docs/architecture.md](docs/architecture.md).

## Not done yet / next steps
- Releases are single-instance per host; multi-replica rolling across hosts is the orchestrator's job (`orchestrator/`), and the edge doesn't exist yet, so the host listener is the router. Liveness restarts are in place (brief blip for that instance).
- Old instances are killed after draining rather than sent SIGTERM first.
- No jailer/seccomp/cgroup limits on Firecracker; VMs run as root on the host.
- Images go through Docker. Next step: pull OCI layers directly and build rootfs without a daemon (or use a block-level layer cache).
- Restore uses a full RAM file. Next: diff snapshots + UFFD/lazy page loading, plus pre-warmed snapshots taken right after "port ready" so cold deploys become restores.
- Networking is IPv4 /30 + NAT per VM. The design calls for IPv6 per VM plus a WireGuard mesh between hosts.
- No volumes, no metrics, no log shipping (the console log is at `/var/lib/ccloud/vms/<name>/console.log`), and no cross-host scheduler or control plane.
- Cloud Hypervisor backend for long-lived services.

### Reliability run (`e2e/chaos.sh`)

Same real cell as `e2e/cell.sh`; breaks one component at a time under curl every 10 ms through the edge.

| Fault | Result | Client errors |
|---|---|---|
| worker `kill -9` mid-deploy, restarted 6 s later | rollout resumes on the new worker (activities heartbeat, `HeartbeatTimeout` 10 s) and goes active | 0 |
| state bucket (MinIO) down 20 s | traffic keeps serving from last-good; no false repairs when it returns; next deploy works | 0 |
| edge-1 `kill -9` (2 edges behind haproxy on :9000), deploy while it's dead, restart | balancer fails over to edge-2; `WaitEdgeAck` only waits on edges with fresh status; edge-1 comes back with last-good routes | 0 |
| hostd `kill -9` + restart | stale Firecracker processes from the old agent are reaped, no leaked VMs | 0 |
| host killed mid-deploy | `WaitReady` returns `HostLost`; the batch is stopped and re-placed on the surviving host (up to `MaxReplacements`), rollout goes active; host comes back running nothing | 0 |
| 6 back-to-back deploys + a crash deploy | all active / rolled back | 0 |
| canary deploy of an app that returns 500 (`-canary 10 -canary-bake 8s`) | one new instance gets ~10% of traffic (edge weights); the edge ejects it after a few 5xx; `canary: error rate 100%` rolls back before any old instance drains | 10 of 555 in that phase (vs. every request for the whole bake without a canary) |
| canary deploy of a good app | canary promoted to full weight, rollout continues | 0 |

Drained backends are removed from the edge doc after each drain. Outside the bad-canary phase 0 of 10644 requests failed; shutdown leaves `procs=0 taps=0 vmdirs=0`. Needs `haproxy` for the balancer.
