#!/usr/bin/env bash
# Builds hostd and the static guest init, placing microinit next to hostd.
set -euo pipefail
cd "$(dirname "$0")/.."
rustup target add x86_64-unknown-linux-musl >/dev/null
cargo build --release -p microinit --target x86_64-unknown-linux-musl
cargo build --release -p hostd
cp target/x86_64-unknown-linux-musl/release/microinit target/release/microinit
echo "built target/release/{hostd,microinit}"
