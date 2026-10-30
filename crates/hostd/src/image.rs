//! OCI image -> bootable ext4 rootfs with microinit as PID 1. Cached by image ID.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct Image {
    pub id: String,
    pub rootfs: PathBuf,
}

fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker").args(args).output()?;
    if !out.status.success() {
        bail!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

pub fn prepare(
    reference: &str,
    extra_env: &[(String, String)],
    hostname: &str,
    init: &Path,
    cache: &Path,
) -> Result<Image> {
    let inspect = match docker(&["image", "inspect", reference]) {
        Ok(s) => s,
        Err(_) => {
            docker(&["pull", "-q", reference])?;
            docker(&["image", "inspect", reference])?
        }
    };
    let meta: Value = serde_json::from_str::<Vec<Value>>(&inspect)?.remove(0);
    let cfg = &meta["Config"];
    let strs = |v: &Value| -> Vec<String> {
        v.as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut argv = strs(&cfg["Entrypoint"]);
    argv.extend(strs(&cfg["Cmd"]));
    let mut env = strs(&cfg["Env"]);
    for (k, v) in extra_env {
        env.retain(|e| !e.starts_with(&format!("{k}=")));
        env.push(format!("{k}={v}"));
    }
    let run = json!({
        "argv": argv,
        "env": env,
        "cwd": cfg["WorkingDir"].as_str().unwrap_or(""),
        "user": cfg["User"].as_str().unwrap_or(""),
        "hostname": hostname,
    });

    let image_id = meta["Id"]
        .as_str()
        .context("image has no Id")?
        .trim_start_matches("sha256:")[..16]
        .to_string();
    // Cache key covers the image plus the run config baked into it.
    let key = format!(
        "{image_id}-{:016x}",
        fnv(run.to_string().as_bytes()) ^ fnv(&std::fs::read(init)?)
    );
    std::fs::create_dir_all(cache)?;
    let rootfs = cache.join(format!("{key}.ext4"));
    if rootfs.exists() {
        return Ok(Image {
            id: image_id,
            rootfs,
        });
    }

    let work = cache.join(format!("{key}.tmp"));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)?;
    let cid = docker(&["create", reference])?;
    let export = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "docker export {cid} | tar -x -C {}",
            work.display()
        ))
        .status();
    let _ = docker(&["rm", &cid]);
    if !export?.success() {
        bail!("exporting {reference} failed");
    }

    std::fs::create_dir_all(work.join("sbin"))?;
    std::fs::create_dir_all(work.join("etc/ccloud"))?;
    std::fs::copy(init, work.join("sbin/microinit"))?;
    std::fs::write(
        work.join("etc/ccloud/run.json"),
        serde_json::to_vec_pretty(&run)?,
    )?;
    let resolv = work.join("etc/resolv.conf");
    let _ = std::fs::remove_file(&resolv);
    std::fs::write(resolv, "nameserver 1.1.1.1\nnameserver 8.8.8.8\n")?;
    std::fs::write(work.join("etc/hostname"), format!("{hostname}\n"))?;

    let du = Command::new("du").args(["-sb"]).arg(&work).output()?;
    let bytes: u64 = String::from_utf8_lossy(&du.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("0")
        .parse()?;
    let size_mib = bytes * 13 / 10 / (1 << 20) + 128;
    let tmp_img = cache.join(format!("{key}.ext4.tmp"));
    let ok = Command::new("mkfs.ext4")
        .args(["-q", "-F", "-L", "rootfs", "-d"])
        .arg(&work)
        .arg(&tmp_img)
        .arg(format!("{size_mib}M"))
        .status()?
        .success();
    std::fs::remove_dir_all(&work)?;
    if !ok {
        bail!("mkfs.ext4 failed");
    }
    std::fs::rename(&tmp_img, &rootfs)?;
    Ok(Image {
        id: image_id,
        rootfs,
    })
}

fn fnv(b: &[u8]) -> u64 {
    b.iter().fold(0xcbf29ce484222325u64, |h, &x| {
        (h ^ x as u64).wrapping_mul(0x100000001b3)
    })
}
