use super::Check;
use std::path::PathBuf;

pub(super) const FILE_HASH_MEMO: &str = "File-hash memo";

/// Whether unchanged key inputs under each root can be served from the
/// file-hash memo. On Windows that needs NTFS file identity; elsewhere every
/// root passes, so the check is only shown on Windows.
pub(super) fn memo_volume_check(roots: &[(&str, PathBuf)]) -> Check {
    let problems: Vec<_> = roots
        .iter()
        .filter(|(_, path)| path.exists())
        .filter_map(|(name, path)| {
            kache_store::file_hash::memo_identity_problem(path)
                .map(|problem| (*name, path.as_path(), problem))
        })
        .collect();
    memo_volume_result(&problems)
}

fn memo_volume_result(problems: &[(&str, &std::path::Path, String)]) -> Check {
    if problems.is_empty() {
        return Check {
            label: FILE_HASH_MEMO,
            pass: true,
            detail: "key inputs are on NTFS; an unchanged file is hashed once".into(),
            fix: None,
        };
    }
    let detail = problems
        .iter()
        .map(|(name, path, problem)| format!("{name} {}: {problem}", crate::term::home_path(path)))
        .collect::<Vec<_>>()
        .join("; ");
    Check {
        label: FILE_HASH_MEMO,
        pass: false,
        detail,
        fix: Some(
            "every cache key reads these files again, which antivirus scanning makes slow; \
             keep them on NTFS (ReFS, including Dev Drive, FAT and exFAT are not memoised)"
                .into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn each_root_without_file_identity_is_named() {
        let pass = memo_volume_result(&[]);
        assert!(pass.pass);

        let check = memo_volume_result(&[
            (
                "workspace",
                Path::new("D:/src/app"),
                "ReFS gives no file identity".into(),
            ),
            (
                "target directory",
                Path::new("E:/target"),
                "FAT32 gives no file identity".into(),
            ),
        ]);
        assert!(!check.pass);
        assert!(
            check.detail.contains("workspace D:/src/app: ReFS"),
            "{}",
            check.detail
        );
        assert!(
            check.detail.contains("target directory E:/target: FAT32"),
            "{}",
            check.detail
        );
        assert!(check.fix.unwrap().contains("Dev Drive"));
    }

    #[test]
    fn missing_roots_are_skipped() {
        let check = memo_volume_check(&[("target directory", PathBuf::from("/no/such/kache/dir"))]);
        assert!(check.pass);
    }
}
