//! Do not move a compiler out of a caller's Linux isolation domain. A caller
//! with an uninspectable filter keeps the local execution path before Commit.

use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

#[cfg(test)]
pub(super) fn peer_may_delegate(
    proc_root: &Path,
    kernel_pid: Option<u32>,
    reported_pid: u32,
) -> bool {
    peer_may_delegate_with_trust(proc_root, kernel_pid, reported_pid, false)
}

pub(super) fn peer_may_delegate_with_trust(
    proc_root: &Path,
    kernel_pid: Option<u32>,
    reported_pid: u32,
    trusted_pipeline: bool,
) -> bool {
    kernel_pid == Some(reported_pid)
        && permits_delegation_with_trust(proc_root, reported_pid, trusted_pipeline).unwrap_or(false)
}

fn field<'a>(status: &'a str, name: &str) -> Option<&'a str> {
    status.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key == name).then(|| value.trim())
    })
}

#[cfg(test)]
pub(super) fn permits_delegation(proc_root: &Path, pid: u32) -> io::Result<bool> {
    permits_delegation_with_trust(proc_root, pid, false)
}

fn permits_delegation_with_trust(
    proc_root: &Path,
    pid: u32,
    trusted_pipeline: bool,
) -> io::Result<bool> {
    let host = proc_root.join("self");
    let peer = proc_root.join(pid.to_string());
    let host_status = std::fs::read_to_string(host.join("status"))?;
    let peer_status = std::fs::read_to_string(peer.join("status"))?;
    // The installed BPF program is not available through ordinary /proc reads.
    // Matching mode or filter count cannot prove equivalent restrictions.
    for name in ["Seccomp", "NoNewPrivs"] {
        let allowed = |value| {
            matches!(
                (trusted_pipeline, name, value),
                (_, _, Some("0")) | (true, "Seccomp", Some("2")) | (true, "NoNewPrivs", Some("1"))
            )
        };
        if !allowed(field(&host_status, name)) || !allowed(field(&peer_status, name)) {
            return Ok(false);
        }
    }
    for name in [
        "Uid", "Gid", "Groups", "CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb", "Umask",
    ] {
        let Some(host_value) = field(&host_status, name) else {
            return Ok(false);
        };
        if field(&peer_status, name) != Some(host_value) {
            return Ok(false);
        }
    }
    for name in ["user", "mnt", "pid", "net", "ipc", "uts"] {
        let host_ns = std::fs::metadata(host.join("ns").join(name))?;
        let peer_ns = std::fs::metadata(peer.join("ns").join(name))?;
        if (host_ns.dev(), host_ns.ino()) != (peer_ns.dev(), peer_ns.ino()) {
            return Ok(false);
        }
    }
    let host_label = std::fs::read(host.join("attr/current"))?;
    let peer_label = std::fs::read(peer.join("attr/current"))?;
    Ok(host_label == peer_label)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = "Uid:\t10 10 10 10\nGid:\t20 20 20 20\nGroups:\t20 30\nCapInh:\t0000\nCapPrm:\t0000\nCapEff:\t0000\nCapBnd:\tffff\nCapAmb:\t0000\nUmask:\t0022\nNoNewPrivs:\t0\nSeccomp:\t0\n";

    #[test]
    fn trusted_pipeline_still_requires_kernel_identity_credentials_and_namespaces() {
        let root = fixture();
        let filtered = STATUS
            .replace("NoNewPrivs:\t0", "NoNewPrivs:\t1")
            .replace("Seccomp:\t0", "Seccomp:\t2");
        for side in ["self", "42"] {
            std::fs::write(root.path().join(side).join("status"), &filtered).unwrap();
        }
        assert!(!peer_may_delegate_with_trust(
            root.path(),
            Some(42),
            42,
            false
        ));
        assert!(peer_may_delegate_with_trust(
            root.path(),
            Some(42),
            42,
            true
        ));
        assert!(!peer_may_delegate_with_trust(
            root.path(),
            Some(7),
            42,
            true
        ));
        assert!(!peer_may_delegate_with_trust(root.path(), None, 42, true));
        for line in filtered.lines() {
            let (key, _) = line.split_once(':').unwrap();
            std::fs::write(
                root.path().join("42/status"),
                filtered.replace(line, &format!("{key}: invalid")),
            )
            .unwrap();
            assert!(
                !peer_may_delegate_with_trust(root.path(), Some(42), 42, true),
                "{key}"
            );
        }
        std::fs::write(root.path().join("42/status"), &filtered).unwrap();
        std::fs::remove_file(root.path().join("42/ns/mnt")).unwrap();
        std::fs::write(root.path().join("42/ns/mnt"), "other namespace").unwrap();
        assert!(!peer_may_delegate_with_trust(
            root.path(),
            Some(42),
            42,
            true
        ));
    }

    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for name in ["self", "42"] {
            let path = root.path().join(name);
            std::fs::create_dir_all(path.join("ns")).unwrap();
            std::fs::create_dir(path.join("attr")).unwrap();
            std::fs::write(path.join("status"), STATUS).unwrap();
            std::fs::write(path.join("attr/current"), b"unconfined\n").unwrap();
        }
        for name in ["user", "mnt", "pid", "net", "ipc", "uts"] {
            let host = root.path().join("self/ns").join(name);
            std::fs::write(&host, b"namespace").unwrap();
            std::fs::hard_link(host, root.path().join("42/ns").join(name)).unwrap();
        }
        root
    }

    #[test]
    fn execution_linux_rejects_uninspectable_filters_and_changed_credentials() {
        let root = fixture();
        assert!(permits_delegation(root.path(), 42).unwrap());
        for side in ["self", "42"] {
            let path = root.path().join(side).join("status");
            for line in STATUS.lines() {
                let (name, _) = line.split_once(':').unwrap();
                let modified = STATUS.replace(line, &format!("{name}: changed"));
                std::fs::write(&path, modified).unwrap();
                assert!(
                    !permits_delegation(root.path(), 42).unwrap(),
                    "{side} {name}"
                );
                let omitted = STATUS.replace(&format!("{line}\n"), "");
                std::fs::write(&path, omitted).unwrap();
                assert!(
                    !permits_delegation(root.path(), 42).unwrap(),
                    "missing {side} {name}"
                );
                std::fs::write(&path, STATUS).unwrap();
            }
        }
    }

    #[test]
    fn delegation_requires_both_the_kernel_pid_and_an_eligible_process() {
        let root = fixture();
        assert!(peer_may_delegate(root.path(), Some(42), 42));
        assert!(!peer_may_delegate(root.path(), Some(7), 42));
        assert!(!peer_may_delegate(root.path(), None, 42));
        std::fs::write(
            root.path().join("42/status"),
            STATUS.replace("Seccomp:\t0", "Seccomp:\t2"),
        )
        .unwrap();
        assert!(!peer_may_delegate(root.path(), Some(42), 42));
        assert!(!peer_may_delegate(root.path(), Some(999), 999));
    }

    #[test]
    fn execution_linux_requires_the_same_namespaces_and_security_labels() {
        for name in ["user", "mnt", "pid", "net", "ipc", "uts"] {
            let root = fixture();
            let path = root.path().join("42/ns").join(name);
            std::fs::remove_file(&path).unwrap();
            std::fs::write(&path, b"other namespace").unwrap();
            assert!(!permits_delegation(root.path(), 42).unwrap(), "{name}");
        }
        let root = fixture();
        std::fs::write(root.path().join("42/attr/current"), b"restricted\n").unwrap();
        assert!(!permits_delegation(root.path(), 42).unwrap());
        assert!(permits_delegation(root.path(), 999).is_err());
        std::fs::remove_file(root.path().join("42/status")).unwrap();
        assert!(permits_delegation(root.path(), 42).is_err());
    }
}
