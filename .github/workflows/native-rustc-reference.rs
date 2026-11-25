use std::{
    env,
    ffi::OsStr,
    fs, io,
    path::PathBuf,
    process::{self, Command},
};

fn main() -> io::Result<()> {
    let real = env::var_os("KACHE_PROBE_REAL_RUSTC").expect("rustup rustc");
    let arguments: Vec<_> = env::args_os().skip(1).collect();
    let status = Command::new(&real).args(&arguments).status()?;
    if !status.success() {
        process::exit(status.code().unwrap_or(1));
    }

    let value = |flag: &str| arguments.windows(2).find(|p| p[0] == flag).map(|p| &p[1]);
    let crate_name = env::var("KACHE_PROBE_CRATE").expect("probe crate");
    let emits_link = arguments.iter().any(|a| {
        a.to_str()
            .and_then(|s| s.strip_prefix("--emit="))
            .is_some_and(|kinds| kinds.split(',').any(|kind| kind == "link"))
    });
    if value("--crate-name").map(|v| v.as_os_str()) != Some(OsStr::new(&crate_name)) || !emits_link
    {
        return Ok(());
    }

    let out = PathBuf::from(value("--out-dir").expect("Cargo output directory"));
    let prefix = format!("lib{crate_name}-");
    let archives: Vec<_> = fs::read_dir(&out)?
        .filter_map(Result::ok)
        .map(|item| item.path())
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".rlib"))
        })
        .collect();
    assert_eq!(
        archives.len(),
        1,
        "expected one rlib for {crate_name} in {out:?}"
    );
    let before = fs::read(&archives[0])?;
    fs::remove_file(&archives[0])?;
    let replay = Command::new(real).args(&arguments).output()?;
    assert!(
        replay.status.success(),
        "direct replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(
        before == fs::read(&archives[0])?,
        "the fresh direct compiler produced different rlib bytes"
    );

    let mut report = format!("direct-replay-equal {} {}\n", crate_name, before.len());
    for arg in &arguments {
        if let Some(s) = arg
            .to_str()
            .filter(|s| s.starts_with("--remap-path-prefix="))
        {
            report.push_str(s);
            report.push('\n');
        }
    }
    fs::write(
        env::var_os("KACHE_PROBE_RECEIPT").expect("probe receipt"),
        report,
    )?;
    Ok(())
}
