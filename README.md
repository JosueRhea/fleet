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
- No jailer/seccomp/cgroup limits on Firecracker; VMs run as root on the host.
- Images go through Docker. Next step: pull OCI layers directly and build rootfs without a daemon (or use a block-level layer cache).
- Restore uses a full RAM file. Next: diff snapshots + UFFD/lazy page loading, plus pre-warmed snapshots taken right after "port ready" so cold deploys become restores.
- Networking is IPv4 /30 + NAT per VM. The design calls for IPv6 per VM plus a WireGuard mesh between hosts.
- No volumes, no metrics, no log shipping (the console log is at `/var/lib/ccloud/vms/<name>/console.log`), and no cross-host scheduler or control plane.
- Cloud Hypervisor backend for long-lived services.
