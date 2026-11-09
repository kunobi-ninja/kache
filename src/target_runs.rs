//! When a program a target directory built last ran.
//!
//! Cargo puts each binary, example and shared library it builds at the top
//! of its profile directory (`<target>/debug/app`, `<target>/debug/examples/
//! demo`), where people and scripts run them. A target whose programs run
//! between builds is in use even when no build touched it.
//!
//! Running a program reads it. On a filesystem that records reads, the read
//! moves the file's access time past the modification time the build gave
//! it: Linux's `relatime` does so on the first read after a write and then
//! at most daily, macOS on the first read after a write. So the newest such
//! access time is the last run kache can see, and a run since may not show
//! for up to a day: a run counts as a use for a day longer. Nothing here
//! changes an access time, so the reading never hides a later run from the
//! next look.
//!
//! Only programs count, not libraries, and not a file linked to more names
//! than Cargo gives it (its unit's file and the uplifted copy): a restore
//! can share that inode with a cache blob, which other checkouts read.

use std::path::{Path, PathBuf};

/// How late `relatime` can show a run.
const RUN_LAG_SECS: i64 = 86_400;

/// Directories under a profile that hold build units, not programs.
const UNIT_DIRS: [&str; 5] = ["deps", "build", "incremental", "examples", ".fingerprint"];

/// Unix seconds of the newest run of a program in `target_dir`: a program
/// file read after it was last written. `None` when none was.
pub(crate) fn last_run(target_dir: &Path) -> Option<i64> {
    program_dirs(target_dir)
        .iter()
        .flat_map(|dir| std::fs::read_dir(dir).into_iter().flatten().flatten())
        .filter_map(|entry| run_time(&entry.path(), &entry.metadata().ok()?))
        .max()
}

/// The directories programs land in: `<target>/<profile>` and
/// `<target>/<triple>/<profile>`, each with its `examples/`.
fn program_dirs(target_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for dir in subdirectories(target_dir) {
        for nested in subdirectories(&dir) {
            if !UNIT_DIRS.iter().any(|name| nested.ends_with(name)) {
                dirs.push(nested.join("examples"));
                dirs.push(nested);
            }
        }
        dirs.push(dir.join("examples"));
        dirs.push(dir);
    }
    dirs
}

fn subdirectories(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| !name.as_encoded_bytes().starts_with(b"."))
        })
        .collect()
}

/// When `path` was read after its last write, if it is a program.
fn run_time(path: &Path, metadata: &std::fs::Metadata) -> Option<i64> {
    if !metadata.is_file() || !is_program(path, metadata) {
        return None;
    }
    let accessed = filetime::FileTime::from_last_access_time(metadata);
    let modified = filetime::FileTime::from_last_modification_time(metadata);
    (accessed > modified).then(|| accessed.unix_seconds())
}

/// One body for every platform, so the Linux mutation lane builds all of it.
fn is_program(path: &Path, metadata: &std::fs::Metadata) -> bool {
    let library = path.extension().is_some_and(|extension| {
        ["so", "dylib", "dll"]
            .iter()
            .any(|library| extension.eq_ignore_ascii_case(library))
    });
    #[cfg(unix)]
    let runnable = {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        metadata.permissions().mode() & 0o111 != 0 && metadata.nlink() <= 2
    };
    #[cfg(not(unix))]
    let runnable = {
        let _ = metadata;
        path.extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
    };
    runnable && !library
}

/// `idle` seconds since a build used a target, shortened to the time since
/// its last program run at `now`, less the day a run can take to show.
pub(crate) fn idle_since_run(idle: i64, last_run: Option<i64>, now: i64) -> i64 {
    last_run.map_or(idle, |run| {
        idle.min(now.saturating_sub(run).saturating_sub(RUN_LAG_SECS).max(0))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use filetime::{FileTime, set_file_times};
    use std::fs;

    fn program(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"program").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    fn program_name(name: &str) -> String {
        if cfg!(windows) {
            format!("{name}.exe")
        } else {
            name.to_string()
        }
    }

    #[test]
    fn the_newest_program_read_after_its_build_is_the_last_run() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path();
        let built = FileTime::from_unix_time(1_000_000, 0);
        let app = target.join("debug").join(program_name("app"));
        let example = target
            .join("x86_64-unknown-linux-gnu/release/examples")
            .join(program_name("demo"));
        let unit = target.join("debug/deps").join(program_name("app-1234abcd"));
        for path in [&app, &example, &unit] {
            program(path);
        }
        // Built, never run.
        set_file_times(&app, built, built).unwrap();
        set_file_times(&example, built, built).unwrap();
        set_file_times(&unit, FileTime::from_unix_time(3_000_000, 0), built).unwrap();
        assert_eq!(last_run(target), None, "a unit's own file is not a run");

        set_file_times(&app, FileTime::from_unix_time(1_500_000, 0), built).unwrap();
        assert_eq!(last_run(target), Some(1_500_000));
        set_file_times(&example, FileTime::from_unix_time(2_000_000, 0), built).unwrap();
        assert_eq!(last_run(target), Some(2_000_000));
    }

    #[cfg(unix)]
    #[test]
    fn a_file_that_cannot_run_is_not_a_program() {
        let temp = tempfile::tempdir().unwrap();
        let notes = temp.path().join("debug/app.d");
        fs::create_dir_all(notes.parent().unwrap()).unwrap();
        fs::write(&notes, b"deps").unwrap();
        set_file_times(
            &notes,
            FileTime::from_unix_time(2_000_000, 0),
            FileTime::from_unix_time(1_000_000, 0),
        )
        .unwrap();
        assert_eq!(last_run(temp.path()), None);
    }

    #[test]
    fn a_run_shortens_how_long_a_target_counts_as_idle() {
        let day = RUN_LAG_SECS;
        assert_eq!(idle_since_run(5 * day, None, 10 * day), 5 * day);
        assert_eq!(idle_since_run(5 * day, Some(7 * day), 10 * day), 2 * day);
        assert_eq!(idle_since_run(day, Some(7 * day), 10 * day), day);
        assert_eq!(idle_since_run(5 * day, Some(9 * day + 10), 10 * day), 0);
        assert_eq!(idle_since_run(5 * day, Some(11 * day), 10 * day), 0);
    }

    #[cfg(unix)]
    #[test]
    fn libraries_and_files_shared_beyond_cargo_are_not_runs() {
        let temp = tempfile::tempdir().unwrap();
        let debug = temp.path().join("debug");
        let library = debug.join("libplugin.so");
        let shared = debug.join("tool");
        program(&library);
        program(&shared);
        std::fs::hard_link(&shared, debug.join("deps-tool")).unwrap();
        std::fs::hard_link(&shared, temp.path().join("blob")).unwrap();
        let read = FileTime::from_unix_time(2_000_000, 0);
        let built = FileTime::from_unix_time(1_000_000, 0);
        set_file_times(&library, read, built).unwrap();
        set_file_times(&shared, read, built).unwrap();
        assert_eq!(last_run(temp.path()), None);
        std::fs::remove_file(temp.path().join("blob")).unwrap();
        assert_eq!(
            last_run(temp.path()),
            Some(2_000_000),
            "Cargo's own two names"
        );
    }
}
