//! Guest PID 1. hostd bakes `/etc/ccloud/run.json` (the OCI image config) into the rootfs.

use serde::Deserialize;
use std::ffi::CString;
use std::os::unix::process::CommandExt;
use std::process::Command;

#[derive(Deserialize)]
struct Run {
    argv: Vec<String>,
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    hostname: String,
}

fn mount(src: &str, target: &str, fstype: &str, flags: libc::c_ulong, data: &str) {
    let _ = std::fs::create_dir_all(target);
    let (s, t, f, d) = (cstr(src), cstr(target), cstr(fstype), cstr(data));
    let rc = unsafe {
        libc::mount(
            s.as_ptr(),
            t.as_ptr(),
            f.as_ptr(),
            flags,
            d.as_ptr() as *const _,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EBUSY) {
            eprintln!("microinit: mount {target}: {err}");
        }
    }
}

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[repr(C)]
struct IfReq {
    name: [u8; 16],
    flags: libc::c_short,
    _pad: [u8; 22],
}

fn loopback_up() {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return;
        }
        let mut req = IfReq {
            name: [0; 16],
            flags: 0,
            _pad: [0; 22],
        };
        req.name[..2].copy_from_slice(b"lo");
        if libc::ioctl(fd, libc::SIOCGIFFLAGS as _, &mut req) == 0 {
            req.flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
            libc::ioctl(fd, libc::SIOCSIFFLAGS as _, &mut req);
        }
        libc::close(fd);
    }
}

fn lookup_user(user: &str) -> Option<(u32, u32)> {
    if user.is_empty() {
        return None;
    }
    let (u, g) = match user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user, None),
    };
    let field = |file: &str, name: &str, col: usize| -> Option<u32> {
        let text = std::fs::read_to_string(file).ok()?;
        text.lines()
            .map(|l| l.split(':').collect::<Vec<_>>())
            .find(|f| f.first() == Some(&name))
            .and_then(|f| f.get(col)?.parse().ok())
    };
    let uid = u.parse().ok().or_else(|| field("/etc/passwd", u, 2))?;
    let gid = match g {
        Some(g) => g.parse().ok().or_else(|| field("/etc/group", g, 2))?,
        None => field("/etc/passwd", u, 3).unwrap_or(uid),
    };
    Some((uid, gid))
}

fn main() {
    mount("proc", "/proc", "proc", 0, "");
    mount("sysfs", "/sys", "sysfs", 0, "");
    mount("devtmpfs", "/dev", "devtmpfs", 0, "");
    mount("devpts", "/dev/pts", "devpts", 0, "");
    mount("tmpfs", "/dev/shm", "tmpfs", 0, "");
    mount("tmpfs", "/run", "tmpfs", 0, "");
    loopback_up();

    let run: Run = match std::fs::read("/etc/ccloud/run.json")
        .map_err(|e| e.to_string())
        .and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()))
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("microinit: bad /etc/ccloud/run.json: {e}");
            shutdown();
        }
    };
    if !run.hostname.is_empty() {
        unsafe { libc::sethostname(run.hostname.as_ptr() as *const _, run.hostname.len()) };
    }
    let Some((prog, args)) = run.argv.split_first() else {
        eprintln!("microinit: image has no entrypoint/cmd");
        shutdown();
    };

    let mut cmd = Command::new(prog);
    cmd.args(args).env_clear();
    for kv in &run.env {
        if let Some((k, v)) = kv.split_once('=') {
            cmd.env(k, v);
        }
    }
    // Secrets arrive on their own read-only drive (NUL-padded JSON), never in the image.
    if let Ok(b) = std::fs::read("/dev/vdb") {
        let end = b.iter().rposition(|&c| c != 0).map_or(0, |i| i + 1);
        match serde_json::from_slice::<std::collections::BTreeMap<String, String>>(&b[..end]) {
            Ok(m) => {
                for (k, v) in m {
                    cmd.env(k, v);
                }
            }
            Err(e) => eprintln!("microinit: bad secrets drive: {e}"),
        }
    }
    if !run.cwd.is_empty() {
        cmd.current_dir(&run.cwd);
    }
    if let Some((uid, gid)) = lookup_user(&run.user) {
        cmd.uid(uid).gid(gid);
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("microinit: exec {prog}: {e}");
            shutdown();
        }
    };
    let main_pid = child.id() as libc::pid_t;

    loop {
        let mut status = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, 0) };
        if pid == main_pid {
            if libc::WIFEXITED(status) {
                eprintln!(
                    "microinit: app exited with code {}",
                    libc::WEXITSTATUS(status)
                );
            } else {
                eprintln!("microinit: app killed by signal {}", libc::WTERMSIG(status));
            }
            break;
        }
        if pid < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            break;
        }
    }
    shutdown();
}

fn shutdown() -> ! {
    unsafe {
        libc::sync();
        // With `reboot=k` on the kernel cmdline this makes Firecracker exit.
        libc::reboot(libc::RB_AUTOBOOT);
    }
    loop {
        std::thread::park();
    }
}
