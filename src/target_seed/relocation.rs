//! Target-local metadata translated before a seeded unit is published.

use std::borrow::Cow;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::{Layout, Unit, build_script_out_dir};

// Metadata beyond this bound costs a normal Cargo rebuild, rather than an
// unbounded allocation on the target-info probe's seeding path.
const MAX_METADATA_BYTES: u64 = 16 << 20;

pub(super) struct Relocation {
    from: PathBuf,
    to: PathBuf,
    depinfo_dirs: Vec<PathBuf>,
    script: Option<Script>,
}

struct Script {
    stdout: PathBuf,
    root_output: PathBuf,
    translated_stdout: String,
    translated_root: String,
}

impl Relocation {
    pub(super) fn for_unit(
        from: &Path,
        to: &Path,
        layout: Layout,
        unit: &Unit,
        deadline: Instant,
    ) -> io::Result<Self> {
        let out_dir = build_script_out_dir(from, layout, unit);
        let stdout = crate::cargo_layout::build_script_stdout(&out_dir)
            .ok_or_else(|| io::Error::other("unknown seeded build-script layout"))?;
        let root_output = stdout.with_file_name("root-output");
        let root = read_text(&root_output, deadline)?;
        let output = read_text(&stdout, deadline)?;
        // Generated OUT_DIR files are script-owned data, even when their
        // names end in .d. Only rustc's direct output locations are dep-info.
        let depinfo_dirs = match layout {
            Layout::Shared => vec![
                from.join("deps"),
                from.join("build")
                    .join(format!("{}-{}", unit.package, unit.hash)),
            ],
            Layout::PerUnit if output.is_none() => vec![out_dir],
            Layout::PerUnit => Vec::new(),
        };
        let script = match (root, output) {
            (Some(root), Some(output)) => {
                let recorded = root.trim();
                let named =
                    crate::cargo_layout::out_dir_unit_name(Path::new(recorded), &unit.package);
                let expected = format!("{}-{}", unit.package, unit.hash);
                if named.as_deref() != Some(expected.as_str()) {
                    return Err(io::Error::other("unrecognized seeded root-output"));
                }
                let current = build_script_out_dir(to, layout, unit);
                let current = current
                    .to_str()
                    .ok_or_else(|| io::Error::other("seeded OUT_DIR is not UTF-8"))?;
                Some(Script {
                    stdout,
                    root_output,
                    translated_stdout: rebase_stdout(&output, recorded, current),
                    translated_root: current.to_owned(),
                })
            }
            (None, None) => None,
            _ => return Err(io::Error::other("unpaired seeded build-script records")),
        };
        Ok(Self {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
            depinfo_dirs,
            script,
        })
    }

    pub(super) fn contents(
        &self,
        path: &Path,
        deadline: Instant,
    ) -> io::Result<Option<Cow<'_, [u8]>>> {
        if let Some(script) = &self.script {
            if path == script.stdout {
                return Ok(Some(Cow::Borrowed(script.translated_stdout.as_bytes())));
            }
            if path == script.root_output {
                return Ok(Some(Cow::Borrowed(script.translated_root.as_bytes())));
            }
        }
        if path.extension() != Some(std::ffi::OsStr::new("d"))
            || !self
                .depinfo_dirs
                .iter()
                .any(|dir| path.parent() == Some(dir.as_path()))
        {
            return Ok(None);
        }
        let original = read_text(path, deadline)?
            .ok_or_else(|| io::Error::other("seeded dep-info disappeared"))?;
        let translated = kache_store::link::rebase_depinfo_content(&original, &self.from, &self.to)
            .ok_or_else(|| io::Error::other("seeded dep-info does not belong to this donor"))?;
        Ok((translated != original).then(|| Cow::Owned(translated.into_bytes())))
    }
}

fn read_text(path: &Path, deadline: Instant) -> io::Result<Option<String>> {
    if Instant::now() >= deadline {
        return Err(io::ErrorKind::TimedOut.into());
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() > MAX_METADATA_BYTES {
        return Err(io::Error::other("unsupported seeded metadata"));
    }
    read_bounded_text(std::fs::File::open(path)?).map(Some)
}

fn read_bounded_text(reader: impl Read) -> io::Result<String> {
    let mut text = String::new();
    reader
        .take(MAX_METADATA_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAX_METADATA_BYTES {
        return Err(io::Error::other("seeded metadata grew beyond its limit"));
    }
    Ok(text)
}

/// Cargo rewrites the recorded OUT_DIR in directive values before parsing them
/// (cargo/src/compiler/custom_build.rs, BuildOutput::parse). Non-directive
/// stdout is opaque and does not participate in its replay.
fn rebase_stdout(output: &str, recorded: &str, current: &str) -> String {
    output
        .split_inclusive('\n')
        .map(|line| {
            if line.trim_start().starts_with("cargo:") {
                line.replace(recorded, current)
            } else {
                line.to_owned()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_metadata_file_at_the_limit_is_read_but_growth_is_bounded() {
        let exact = vec![b'a'; 16 << 20];
        assert_eq!(
            read_bounded_text(exact.as_slice()).unwrap().as_bytes(),
            exact
        );
        assert!(
            read_bounded_text(std::io::repeat(b'a')).is_err(),
            "even a growing source is capped"
        );
        assert!(
            read_bounded_text(&[0xff][..]).is_err(),
            "binary metadata cannot be relocated"
        );
    }

    #[test]
    fn reading_metadata_distinguishes_missing_unsupported_and_expired() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        assert_eq!(read_text(&missing, deadline).unwrap(), None);
        assert!(read_text(dir.path(), deadline).is_err());
        let exact = dir.path().join("exact");
        std::fs::File::create(&exact)
            .unwrap()
            .set_len(16 << 20)
            .unwrap();
        assert_eq!(
            read_text(&exact, deadline).unwrap().unwrap().len(),
            16 << 20
        );
        assert_eq!(
            read_text(&missing, Instant::now()).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[cfg(unix)]
    #[test]
    fn reading_metadata_refuses_a_leaf_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let regular = dir.path().join("stdout");
        let linked = dir.path().join("linked-stdout");
        let text = "cargo:rustc-cfg=seeded\n";
        std::fs::write(&regular, text).unwrap();
        std::os::unix::fs::symlink(&regular, &linked).unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(60);

        assert_eq!(
            read_text(&regular, deadline).unwrap().as_deref(),
            Some(text)
        );
        assert!(
            read_text(&linked, deadline).is_err(),
            "seed metadata must not follow a leaf symlink"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reading_metadata_propagates_non_missing_lookup_errors() {
        let dir = tempfile::tempdir().unwrap();
        let anchor = dir.path().join("stdout");
        std::fs::write(&anchor, "cargo:rustc-cfg=seeded\n").unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(60);

        assert_eq!(
            read_text(&dir.path().join("missing"), deadline).unwrap(),
            None
        );
        assert_eq!(
            read_text(&anchor.join("child"), deadline)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotADirectory
        );
    }

    #[test]
    fn stdout_relocation_matches_cargos_directive_value_replay() {
        let original = "opaque /old/out\ncargo:warning=/old/out/value\n cargo::rustc-env=PATH=/old/out/native\ncargo::rerun-if-changed=/old/out/source\n";
        assert_eq!(
            rebase_stdout(original, "/old/out", "/new/out"),
            "opaque /old/out\ncargo:warning=/new/out/value\n cargo::rustc-env=PATH=/new/out/native\ncargo::rerun-if-changed=/new/out/source\n"
        );
        assert_eq!(rebase_stdout("", "/old/out", "/new/out"), "");
    }

    #[test]
    fn a_script_recorded_before_the_donor_moves_is_rebased_from_its_recorded_root() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let from = dir.path().join("donor/debug");
            let to = dir.path().join("consumer/debug");
            let unit = Unit {
                package: "dep".into(),
                hash: "0123456789abcdef".into(),
            };
            let old = build_script_out_dir(&dir.path().join("removed/debug"), layout, &unit);
            let stdout = crate::cargo_layout::build_script_stdout(&build_script_out_dir(
                &from, layout, &unit,
            ))
            .unwrap();
            std::fs::create_dir_all(stdout.parent().unwrap()).unwrap();
            std::fs::write(
                &stdout,
                format!("cargo:rerun-if-changed={}/input\n", old.display()),
            )
            .unwrap();
            let root = stdout.with_file_name("root-output");
            std::fs::write(&root, old.to_str().unwrap()).unwrap();
            let relocation = Relocation::for_unit(
                &from,
                &to,
                layout,
                &unit,
                Instant::now() + std::time::Duration::from_secs(60),
            )
            .unwrap();
            let current = build_script_out_dir(&to, layout, &unit);
            let generated_data = build_script_out_dir(&from, layout, &unit).join("config.d");
            std::fs::create_dir_all(generated_data.parent().unwrap()).unwrap();
            let data = format!("value: {}/opaque\n", from.display());
            std::fs::write(&generated_data, &data).unwrap();
            assert!(
                relocation
                    .contents(&generated_data, Instant::now())
                    .unwrap()
                    .is_none(),
                "script-generated .d data is not compiler dep-info"
            );
            assert_eq!(
                relocation
                    .contents(&root, Instant::now())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                current.to_str().unwrap().as_bytes()
            );
            assert_eq!(
                relocation
                    .contents(&stdout, Instant::now())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                format!("cargo:rerun-if-changed={}/input\n", current.display()).as_bytes()
            );
        }
    }
}
