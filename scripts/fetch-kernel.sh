#!/usr/bin/env bash
# Installs Firecracker + the Firecracker CI guest kernel into the default hostd paths.
set -euo pipefail
FC_VERSION=${FC_VERSION:-v1.17.0}
KERNEL_URL=${KERNEL_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155}
arch=$(uname -m)
tmp=$(mktemp -d)
curl -fsSL "https://github.com/firecracker-microvm/firecracker/releases/download/${FC_VERSION}/firecracker-${FC_VERSION}-${arch}.tgz" | tar -xz -C "$tmp"
sudo install -m755 "$tmp/release-${FC_VERSION}-${arch}/firecracker-${FC_VERSION}-${arch}" /usr/local/bin/firecracker
sudo mkdir -p /var/lib/microrail
sudo curl -fsSL -o /var/lib/microrail/vmlinux "$KERNEL_URL"
rm -rf "$tmp"
firecracker --version | head -1
