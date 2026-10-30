//! Per-VM TAP device on its own /30, NAT out through the host.

use anyhow::{bail, Result};
use std::net::Ipv4Addr;
use std::process::Command;

pub const SUBNET: &str = "172.30.0.0/16";

pub struct Slot {
    pub idx: u8,
}

impl Slot {
    pub fn tap(&self) -> String {
        format!("mr{}", self.idx)
    }
    pub fn host_ip(&self) -> Ipv4Addr {
        Ipv4Addr::new(172, 30, self.idx, 1)
    }
    pub fn guest_ip(&self) -> Ipv4Addr {
        Ipv4Addr::new(172, 30, self.idx, 2)
    }
    pub fn mac(&self) -> String {
        format!("06:00:ac:1e:{:02x}:02", self.idx)
    }
    pub fn kernel_ip_arg(&self) -> String {
        format!(
            "ip={}::{}:255.255.255.252::eth0:off",
            self.guest_ip(),
            self.host_ip()
        )
    }

    pub fn create(&self) -> Result<()> {
        let tap = self.tap();
        let _ = sh(&["ip", "link", "del", &tap]);
        sh(&["ip", "tuntap", "add", "dev", &tap, "mode", "tap"])?;
        sh(&[
            "ip",
            "addr",
            "add",
            &format!("{}/30", self.host_ip()),
            "dev",
            &tap,
        ])?;
        sh(&["ip", "link", "set", &tap, "up"])
    }

    pub fn destroy(&self) {
        let _ = sh(&["ip", "link", "del", &self.tap()]);
    }
}

/// Drop TAPs left behind by a previous hostd that died without cleaning up.
pub fn remove_stale_taps(range: &std::ops::RangeInclusive<u8>) {
    let Ok(out) = Command::new("ip").args(["-o", "link", "show"]).output() else {
        return;
    };
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if let Some(name) = line.split(':').nth(1).map(str::trim) {
            if name
                .strip_prefix("mr")
                .and_then(|n| n.parse::<u8>().ok())
                .is_some_and(|n| range.contains(&n))
            {
                let _ = sh(&["ip", "link", "del", name]);
            }
        }
    }
}

/// Idempotent host-wide setup: forwarding + masquerade for guest egress.
pub fn init_host() -> Result<()> {
    std::fs::write("/proc/sys/net/ipv4/ip_forward", "1")?;
    ensure_rule(&[
        "-t",
        "nat",
        "POSTROUTING",
        "-s",
        SUBNET,
        "!",
        "-o",
        "mr+",
        "-j",
        "MASQUERADE",
    ])?;
    // Docker sets FORWARD policy to DROP; let guest traffic through.
    ensure_rule(&["FORWARD", "-i", "mr+", "-j", "ACCEPT"])?;
    ensure_rule(&[
        "FORWARD",
        "-o",
        "mr+",
        "-m",
        "conntrack",
        "--ctstate",
        "RELATED,ESTABLISHED",
        "-j",
        "ACCEPT",
    ])
}

fn ensure_rule(rule: &[&str]) -> Result<()> {
    let (table, rest): (Vec<&str>, &[&str]) = if rule[0] == "-t" {
        (rule[..2].to_vec(), &rule[2..])
    } else {
        (vec![], rule)
    };
    let (chain, spec) = rest.split_first().unwrap();
    let mut check = vec!["iptables"];
    check.extend(&table);
    check.extend(["-C", chain]);
    check.extend(spec);
    if sh(&check).is_ok() {
        return Ok(());
    }
    let mut add = vec!["iptables"];
    add.extend(&table);
    add.extend(["-I", chain]);
    add.extend(spec);
    sh(&add)
}

pub fn sh(argv: &[&str]) -> Result<()> {
    let out = Command::new(argv[0]).args(&argv[1..]).output()?;
    if !out.status.success() {
        bail!(
            "{}: {}",
            argv.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}
