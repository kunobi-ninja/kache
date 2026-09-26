use super::*;
use crate::args::RustcArgs;
use crate::test_support::process_state_test_lock;

const GNU_RUSTC_VERSION: &str = "rustc 1.90.0\nhost: x86_64-unknown-linux-gnu\nrelease: 1.90.0\n";
const DARWIN_RUSTC_VERSION: &str = "rustc 1.90.0\nhost: aarch64-apple-darwin\nrelease: 1.90.0\n";

#[test]
fn source_identity_uses_a_stable_configured_root() {
    let dir = tempfile::tempdir().unwrap();
    let checkout = dir.path().join("checkout");
    let source = checkout.join("src/lib.rs");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, "pub fn value() {}\n").unwrap();
    let normalizer =
        PathNormalizer::empty().with_base_dirs(&[checkout.to_string_lossy().into_owned()]);

    let mut expected = b"<BASE_DIR_0>/".to_vec();
    expected.extend_from_slice(
        source
            .strip_prefix(&checkout)
            .unwrap()
            .to_string_lossy()
            .as_bytes(),
    );
    assert_eq!(
        source_path_identity(&source, &normalizer).unwrap(),
        expected
    );
}

#[test]
fn source_identity_uses_a_configured_external_root_losslessly() {
    let dir = tempfile::tempdir().unwrap();
    let external = dir.path().join("external");
    let source = external.join("generated/value.rs");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, "pub const VALUE: u8 = 1;\n").unwrap();
    let normalizer =
        PathNormalizer::empty().with_base_dirs(&[external.to_string_lossy().into_owned()]);

    let mut expected = b"<BASE_DIR_0>/".to_vec();
    expected.extend_from_slice(
        source
            .strip_prefix(&external)
            .unwrap()
            .to_string_lossy()
            .as_bytes(),
    );
    assert_eq!(
        source_path_identity(&source, &normalizer).unwrap(),
        expected
    );
}

#[cfg(unix)]
#[test]
fn source_identity_keeps_distinct_symlink_spellings_of_one_inode() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real.rs");
    let alias = dir.path().join("alias.rs");
    std::fs::write(&real, "pub const VALUE: u8 = 1;\n").unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();

    let real_identity = source_path_identity(&real, &PathNormalizer::empty()).unwrap();
    let alias_identity = source_path_identity(&alias, &PathNormalizer::empty()).unwrap();
    assert_ne!(real_identity, alias_identity);
    assert!(real_identity.starts_with(b"<OPAQUE_PATH>/"));
    assert!(alias_identity.starts_with(b"<OPAQUE_PATH>/"));
}

#[cfg(target_os = "linux")]
#[test]
fn source_identity_opaque_fallback_preserves_non_utf8_bytes() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let external = dir.path().join("external");
    std::fs::create_dir_all(&external).unwrap();
    let path_a = external.join(std::ffi::OsString::from_vec(vec![b'a', 0x80]));
    let path_b = external.join(std::ffi::OsString::from_vec(vec![b'a', 0x81]));
    std::fs::write(&path_a, b"same").unwrap();
    std::fs::write(&path_b, b"same").unwrap();
    let identity_a = source_path_identity(&path_a, &PathNormalizer::empty()).unwrap();
    let identity_b = source_path_identity(&path_b, &PathNormalizer::empty()).unwrap();
    assert_ne!(identity_a, identity_b);
    assert!(identity_a.starts_with(b"<OPAQUE_PATH>/"));
    assert!(identity_b.starts_with(b"<OPAQUE_PATH>/"));
}

/// #131 load-bearing invariant: the grouped tee must produce EXACTLY the
/// digest a plain blake3 hasher produces over the same update sequence —
/// per-field tracing can never change a cache key.
#[test]
fn grouped_hasher_main_digest_matches_plain_blake3() {
    let mut plain = blake3::Hasher::new();
    let mut grouped = GroupedHasher::new("compiler");
    for (group, chunk) in [
        ("compiler", b"rustc_version:1.90".as_slice()),
        ("args", b"emit:link\n"),
        ("sources", b"source:abc\n"),
        ("args", b"RUSTFLAGS:-Copt-level=3\n"),
        ("link", b"linker:ld64\n"),
    ] {
        plain.update(chunk);
        grouped.set_group(group);
        grouped.update(chunk);
    }
    let (hash, fields) = grouped.finalize_with_fields();
    assert_eq!(hash, plain.finalize(), "grouping must not perturb the key");
    assert_eq!(
        fields.keys().collect::<Vec<_>>(),
        ["args", "compiler", "link", "sources"],
        "only groups that received bytes appear",
    );
    assert!(fields.values().all(|v| v.len() == KEY_FIELD_HEX));
}

/// #131: bytes route to the CURRENT group, non-contiguous segments of the
/// same group accumulate, and only the touched group's digest changes.
#[test]
fn grouped_hasher_isolates_changes_to_their_group() {
    let build = |rustflags: &[u8]| {
        let mut h = GroupedHasher::new("compiler");
        h.update(b"rustc_version:1.90\n");
        h.set_group("sources");
        h.update(b"source:abc\n");
        h.set_group("args");
        h.update(b"emit:link\n");
        h.update(rustflags);
        h.finalize_with_fields()
    };
    let (key_a, fields_a) = build(b"RUSTFLAGS:-Copt-level=3\n");
    let (key_b, fields_b) = build(b"RUSTFLAGS:-Copt-level=2\n");
    assert_ne!(key_a, key_b);
    assert_ne!(fields_a["args"], fields_b["args"], "args group must differ");
    assert_eq!(fields_a["compiler"], fields_b["compiler"]);
    assert_eq!(fields_a["sources"], fields_b["sources"]);
}

fn parsed_crate_type(crate_type: &str, target: Option<&str>) -> RustcArgs {
    let mut argv = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "probe".to_string(),
        "--crate-type".to_string(),
        crate_type.to_string(),
        "src/lib.rs".to_string(),
    ];
    if let Some(target) = target {
        argv.push("--target".to_string());
        argv.push(target.to_string());
    }
    RustcArgs::parse(&argv).unwrap()
}

fn libc_fold_key(
    args: &RustcArgs,
    rustc_version: &str,
    running_on_linux: bool,
    signature: &str,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"base-key");
    fold_native_host_libc_signature(&mut hasher, args, rustc_version, running_on_linux, |_| {
        Ok(signature.to_string())
    })?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[test]
fn native_linux_linked_outputs_key_host_libc_version() {
    for crate_type in ["bin", "dylib", "cdylib", "proc-macro"] {
        let args = parsed_crate_type(crate_type, None);
        let old = libc_fold_key(&args, GNU_RUSTC_VERSION, true, "2.36").unwrap();
        let new = libc_fold_key(&args, GNU_RUSTC_VERSION, true, "2.39").unwrap();
        assert_ne!(old, new, "{crate_type} must re-key across libc versions");
    }
}

#[test]
fn rlibs_and_cross_targets_do_not_key_host_libc() {
    let rlib = parsed_crate_type("rlib", None);
    assert_eq!(
        libc_fold_key(&rlib, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
        libc_fold_key(&rlib, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
        "portable rlibs must not be tied to the host libc"
    );

    let cross = parsed_crate_type("bin", Some("aarch64-unknown-linux-gnu"));
    assert_eq!(
        libc_fold_key(&cross, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
        libc_fold_key(&cross, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
        "cross-target output must not be tied to the build host libc"
    );

    let explicit_native = parsed_crate_type("bin", Some("x86_64-unknown-linux-gnu"));
    assert_ne!(
        libc_fold_key(&explicit_native, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
        libc_fold_key(&explicit_native, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
        "an explicit rustc-host target is still a native output"
    );
}

#[test]
fn host_libc_probe_is_linux_only_and_fails_closed() {
    let bin = parsed_crate_type("bin", None);
    assert_eq!(
        libc_fold_key(&bin, GNU_RUSTC_VERSION, false, "2.36").unwrap(),
        libc_fold_key(&bin, GNU_RUSTC_VERSION, false, "2.39").unwrap(),
        "non-Linux hosts must not gain a Linux libc component"
    );

    let mut hasher = blake3::Hasher::new();
    let err = fold_native_host_libc_signature(&mut hasher, &bin, GNU_RUSTC_VERSION, true, |_| {
        anyhow::bail!("probe failed")
    })
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("determining native Linux gnu-libc")
    );

    let missing_host = "rustc 1.90.0\nrelease: 1.90.0\n";
    let err = libc_fold_key(&bin, missing_host, true, "2.39").unwrap_err();
    assert!(err.to_string().contains("no host triple"));
}

#[test]
fn metadata_only_outputs_do_not_probe_or_key_host_libc() {
    let mut metadata = parsed_crate_type("bin", None);
    metadata.emit = vec!["metadata".to_string()];
    let mut hasher = blake3::Hasher::new();
    fold_native_host_libc_signature(&mut hasher, &metadata, GNU_RUSTC_VERSION, true, |_| {
        panic!("metadata-only output must not probe libc")
    })
    .unwrap();

    let baseline = blake3::Hasher::new().finalize().to_hex().to_string();
    assert_eq!(hasher.finalize().to_hex().to_string(), baseline);
}

#[test]
fn double_wrapper_uses_inner_rustc_host_banner() {
    let mut bin = parsed_crate_type("bin", None);
    bin.inner_rustc = Some(PathBuf::from("/toolchain/bin/rustc"));
    let version = rustc_version_for_native_link(&bin, "clippy 0.1.90\n", |path| {
        assert_eq!(path, Path::new("/toolchain/bin/rustc"));
        Ok(GNU_RUSTC_VERSION.to_string())
    })
    .unwrap();

    assert_eq!(
        rustc_host_triple(&version),
        Some("x86_64-unknown-linux-gnu")
    );
}

fn dummy_absolute_linker() -> String {
    std::env::temp_dir()
        .join("kache-dummy-cc")
        .to_string_lossy()
        .into_owned()
}

fn parsed_linked_bin(target: Option<&str>) -> RustcArgs {
    let mut argv = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "probe".to_string(),
        "--crate-type".to_string(),
        "bin".to_string(),
        "src/lib.rs".to_string(),
        format!("-Clinker={}", dummy_absolute_linker()),
    ];
    if let Some(target) = target {
        argv.push("--target".to_string());
        argv.push(target.to_string());
    }
    RustcArgs::parse(&argv).unwrap()
}

fn crt_fold_key(
    args: &RustcArgs,
    rustc_version: &str,
    linux: bool,
    macos: bool,
    crt: &str,
    sdk: &str,
    deployment_target: Option<&str>,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"base-key");
    fold_native_link_runtime_identity(
        &mut hasher,
        args,
        rustc_version,
        linux,
        macos,
        |_| {
            let mut objects = BTreeMap::new();
            for pair in crt.split(';').filter(|pair| !pair.is_empty()) {
                let (name, digest) = pair.split_once('=').unwrap();
                objects.insert(name.to_string(), digest.to_string());
            }
            Ok(objects)
        },
        |_| Ok(sdk.to_string()),
        &KeyEnv::from_parts(
            deployment_target.map(|target| ("MACOSX_DEPLOYMENT_TARGET", target)),
            None,
        ),
    )?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[test]
fn native_linux_linked_outputs_key_crt_object_hashes() {
    let args = parsed_linked_bin(None);
    let old = crt_fold_key(
        &args,
        GNU_RUSTC_VERSION,
        true,
        false,
        "crt1.o=aaa;libc.so.6=bbb",
        "",
        None,
    )
    .unwrap();
    let new = crt_fold_key(
        &args,
        GNU_RUSTC_VERSION,
        true,
        false,
        "crt1.o=aaa;libc.so.6=ccc",
        "",
        None,
    )
    .unwrap();
    assert_ne!(old, new, "libc object bytes must re-key the native link");
}

#[test]
fn rlibs_and_cross_targets_do_not_key_crt_or_sdk() {
    let rlib = parsed_crate_type("rlib", None);
    assert_eq!(
        crt_fold_key(
            &rlib,
            GNU_RUSTC_VERSION,
            true,
            true,
            "crt1.o=aaa;libc.so.6=bbb",
            "14.0 (a)",
            Some("11.0"),
        )
        .unwrap(),
        crt_fold_key(
            &rlib,
            GNU_RUSTC_VERSION,
            true,
            true,
            "crt1.o=zzz;libc.so.6=yyy",
            "15.0 (b)",
            Some("12.0"),
        )
        .unwrap(),
        "portable rlibs must not be tied to CRT/SDK identity"
    );

    let cross = parsed_linked_bin(Some("aarch64-unknown-linux-gnu"));
    assert_eq!(
        crt_fold_key(
            &cross,
            GNU_RUSTC_VERSION,
            true,
            false,
            "crt1.o=aaa;libc.so.6=bbb",
            "",
            None,
        )
        .unwrap(),
        crt_fold_key(
            &cross,
            GNU_RUSTC_VERSION,
            true,
            false,
            "crt1.o=zzz;libc.so.6=yyy",
            "",
            None,
        )
        .unwrap(),
        "cross-target output must not be tied to the build host CRT"
    );
}

#[test]
fn native_linux_crt_probe_fails_closed() {
    let bin = parsed_linked_bin(None);
    let mut hasher = blake3::Hasher::new();
    let err = fold_native_link_runtime_identity(
        &mut hasher,
        &bin,
        GNU_RUSTC_VERSION,
        true,
        false,
        |_| anyhow::bail!("no startup object"),
        |_| unreachable!("linux fold must not probe the macOS SDK"),
        &KeyEnv::default(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("determining native Linux CRT/libc identity")
    );
}

#[test]
fn native_macos_linked_outputs_key_sdk_identity() {
    let args = parsed_linked_bin(None);
    let old = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        false,
        true,
        "",
        "14.0 (23A344)",
        None,
    )
    .unwrap();
    let new = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        false,
        true,
        "",
        "15.0 (24A348)",
        None,
    )
    .unwrap();
    assert_ne!(old, new, "SDK identity must re-key the native macOS link");

    let with_dt = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        false,
        true,
        "",
        "14.0 (23A344)",
        Some("11.0"),
    )
    .unwrap();
    assert_ne!(
        old, with_dt,
        "MACOSX_DEPLOYMENT_TARGET must re-key when set"
    );
    let empty_dt = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        false,
        true,
        "",
        "14.0 (23A344)",
        Some(""),
    )
    .unwrap();
    assert_eq!(old, empty_dt, "an empty MACOSX_DEPLOYMENT_TARGET is unset");
}

const WINDOWS_RUSTC_VERSION: &str = "rustc 1.90.0\nhost: x86_64-pc-windows-msvc\nrelease: 1.90.0\n";
const WINDOWS_GNU_RUSTC_VERSION: &str =
    "rustc 1.90.0\nhost: x86_64-pc-windows-gnu\nrelease: 1.90.0\n";

fn windows_fold_key_on_host(
    args: &RustcArgs,
    version: &str,
    identity: &str,
    running_on_windows: bool,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"base-key");
    fold_native_windows_msvc_identity(&mut hasher, args, version, running_on_windows, |_, _| {
        Ok(identity.to_string())
    })?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn windows_fold_key(args: &RustcArgs, version: &str, identity: &str) -> Result<String> {
    windows_fold_key_on_host(args, version, identity, true)
}

#[test]
fn native_windows_msvc_identity_keys_only_native_link_outputs() {
    let bin = parsed_crate_type("bin", None);
    let old = windows_fold_key(&bin, WINDOWS_RUSTC_VERSION, "toolset=14.4").unwrap();
    let new = windows_fold_key(&bin, WINDOWS_RUSTC_VERSION, "toolset=14.5").unwrap();
    assert_ne!(old, new);

    let mut metadata = bin.clone();
    metadata.emit = vec!["metadata".into()];
    assert_eq!(
        windows_fold_key(&metadata, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
        windows_fold_key(&metadata, WINDOWS_RUSTC_VERSION, "anything").unwrap()
    );

    let cross = parsed_crate_type("bin", Some("aarch64-pc-windows-msvc"));
    assert!(
        windows_fold_key(&cross, WINDOWS_RUSTC_VERSION, "probe must not run").is_err(),
        "cross-target Windows MSVC links must pass through until target identity is modeled"
    );

    let gnu = parsed_crate_type("bin", Some("x86_64-pc-windows-gnu"));
    assert_eq!(
        windows_fold_key(&gnu, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
        windows_fold_key(&gnu, WINDOWS_RUSTC_VERSION, "anything").unwrap()
    );

    let rlib = parsed_crate_type("rlib", None);
    assert_eq!(
        windows_fold_key(&rlib, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
        windows_fold_key(&rlib, WINDOWS_RUSTC_VERSION, "anything").unwrap()
    );

    assert_eq!(
        windows_fold_key_on_host(&bin, WINDOWS_RUSTC_VERSION, "probe must not run", false).unwrap(),
        windows_fold_key_on_host(&bin, WINDOWS_RUSTC_VERSION, "anything", false).unwrap(),
        "a non-Windows host must not probe native MSVC inputs"
    );
}

#[test]
fn native_windows_msvc_detection_requires_a_windows_linked_executable() {
    let bin = parsed_crate_type("bin", None);
    let mut metadata = bin.clone();
    metadata.emit = vec!["metadata".into()];
    let rlib = parsed_crate_type("rlib", None);

    for (args, running_on_windows) in [(&bin, false), (&metadata, true), (&rlib, true)] {
        assert!(
            !is_native_windows_msvc_link(
                args,
                WINDOWS_RUSTC_VERSION,
                running_on_windows,
                |_| unreachable!("the supplied rustc version must be reused"),
            )
            .unwrap()
        );
    }

    // Both halves of the admission must hold: the target must be the host
    // and the host must be MSVC. A cross target on an MSVC host and a
    // native build on a GNU host each fail one half.
    let cross = parsed_crate_type("bin", Some("x86_64-unknown-linux-gnu"));
    for (args, version) in [
        (&cross, WINDOWS_RUSTC_VERSION),
        (&bin, WINDOWS_GNU_RUSTC_VERSION),
    ] {
        assert!(
            !is_native_windows_msvc_link(args, version, true, |_| unreachable!(
                "the supplied rustc version must be reused"
            ))
            .unwrap(),
            "{version:?} must not admit {:?}",
            args.target
        );
    }
}

#[test]
fn native_windows_msvc_ignores_unrelated_generic_cc_identity() {
    let bin = parsed_crate_type("bin", None);
    assert!(
        is_native_windows_msvc_link(&bin, WINDOWS_RUSTC_VERSION, true, |_| unreachable!(
            "non-nested rustc must use the supplied version"
        ),)
        .unwrap()
    );

    let fold = |identity: &str| {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"base-key");
        fold_generic_linker_identity(&mut hasher, &bin, true, |_| Some(identity.to_string()));
        hasher.finalize()
    };
    assert_eq!(
        fold("unrelated MinGW cc"),
        fold("no cc installed"),
        "native MSVC keys must not depend on an unrelated generic cc probe"
    );

    let mut generic = blake3::Hasher::new();
    generic.update(b"base-key");
    fold_generic_linker_identity(&mut generic, &bin, false, |_| {
        Some("actual generic linker".into())
    });
    assert_ne!(generic.finalize(), fold("anything"));
}

#[test]
fn native_windows_msvc_identity_probe_failure_is_cache_failure() {
    let bin = parsed_crate_type("bin", None);
    let mut hasher = blake3::Hasher::new();
    let error = fold_native_windows_msvc_identity(
        &mut hasher,
        &bin,
        WINDOWS_RUSTC_VERSION,
        true,
        |_, _| anyhow::bail!("ambiguous toolchain"),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native Windows MSVC link identity")
    );
}

#[test]
fn windows_native_link_search_dirs_include_l_and_libpath_and_reject_ambiguity() {
    let dir = tempfile::tempdir().unwrap();
    let native = dir.path().join("native");
    let libpath = dir.path().join("libpath");
    std::fs::create_dir_all(&native).unwrap();
    std::fs::create_dir_all(&libpath).unwrap();
    // `-l static=foo` resolves to `foo.lib` through the modeled search
    // dirs and is hashed by the identity; only `-C link-arg` inputs are
    // classified.
    let args = RustcArgs::parse(&[
        "rustc".into(),
        "--crate-type=bin".into(),
        "-l".into(),
        "static=foo".into(),
        "-L".into(),
        format!("native={}", native.display()),
        format!("-Clink-arg=/LIBPATH:{}", libpath.display()),
    ])
    .unwrap();
    assert_eq!(args.link_libs, vec!["static=foo".to_string()]);
    assert_eq!(
        windows_native_link_search_dirs(&args).unwrap(),
        WindowsNativeLinkSearchDirs {
            rustc: vec![native.clone()],
            linker: vec![native, libpath],
        }
    );

    let ambiguous = RustcArgs::parse(&[
        "rustc".into(),
        "--crate-type=bin".into(),
        "-Clink-args=/DEFAULTLIB:foo /LIBPATH".into(),
    ])
    .unwrap();
    assert!(windows_native_link_search_dirs(&ambiguous).is_err());

    for linker_arg in [
        "foo.lib",
        "/DEFAULTLIB:foo",
        "-defaultlib:foo",
        "/DEFAULTLIB:foo.lib",
        "app.res",
        "APP.RES",
        "extra.obj",
        "exports.exp",
        "/DEF:exports.def",
        "-def:exports.def",
        "-Wl,/def:exports.def",
        "/MANIFESTINPUT:extra.manifest",
        "/MANIFESTFILE:app.exe.manifest",
        "/PDBSTRIPPED:app.public.pdb",
    ] {
        let args = RustcArgs::parse(&[
            "rustc".into(),
            "--crate-type=bin".into(),
            format!("-Clink-arg={linker_arg}"),
        ])
        .unwrap();
        let error = windows_native_link_search_dirs(&args).unwrap_err();
        assert!(
            error.to_string().contains("not hashed"),
            "unmodeled Windows link input must fail closed: {linker_arg}: {error:#}"
        );
    }
}

#[test]
fn windows_libpath_parser_accepts_one_exact_argument() {
    for (argument, expected) in [
        (r"/LIBPATH:C:\sdk\lib", r"C:\sdk\lib"),
        (r"/libpath=C:\sdk\lib", r"C:\sdk\lib"),
        (r"-LIBPATH:C:\lld\lib", r"C:\lld\lib"),
        (r"-libpath=C:\lld\lib", r"C:\lld\lib"),
        (
            r#"/LIBPATH:"C:\Program Files\SDK\lib""#,
            r"C:\Program Files\SDK\lib",
        ),
        (
            r#"-Wl,/LIBPATH:"C:\Program Files\SDK\lib""#,
            r"C:\Program Files\SDK\lib",
        ),
        (
            r#"-Wl,-LiBpAtH:"C:\Program Files\LLVM\lib""#,
            r"C:\Program Files\LLVM\lib",
        ),
    ] {
        assert_eq!(
            windows_libpath_argument(argument).unwrap().as_deref(),
            Some(expected),
            "{argument}"
        );
    }
    assert_eq!(windows_libpath_argument("/DEBUG").unwrap(), None);

    for argument in [
        "/LIBPATH:",
        r#"/LIBPATH:"C:\unterminated"#,
        r#"/LIBPATH:"C:\sdk\lib" /DEBUG"#,
        r"/LIBPATH:C:\Program Files\SDK\lib",
        r"/DEBUG /LIBPATH:C:\sdk\lib",
        r"/DEBUG -LIBPATH:C:\lld\lib",
        r"-Wl,/DEBUG,-LIBPATH:C:\lld\lib",
    ] {
        assert!(
            windows_libpath_argument(argument).is_err(),
            "ambiguous or empty argument must fail closed: {argument}"
        );
    }
}

#[test]
fn windows_lld_libpath_preserves_shadowing_order() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("lld-first");
    let second = directory.path().join("link-second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(first.join("shadowed.lib"), b"first").unwrap();
    std::fs::write(second.join("shadowed.lib"), b"second").unwrap();
    let args = RustcArgs::parse(&[
        "rustc".into(),
        "--crate-type=bin".into(),
        format!("-Clink-arg=-LIBPATH:{}", first.display()),
        format!("-Clink-arg=/LIBPATH:{}", second.display()),
    ])
    .unwrap();

    assert_eq!(
        windows_native_link_search_dirs(&args).unwrap(),
        WindowsNativeLinkSearchDirs {
            rustc: Vec::new(),
            linker: vec![first, second],
        }
    );
}

#[test]
fn windows_native_link_search_dirs_honor_only_linker_visible_l_kinds() {
    let directory = tempfile::tempdir().unwrap();
    let native = directory.path().join("native");
    let all = directory.path().join("all");
    let bare = directory.path().join("bare");
    let libpath = directory.path().join("path with spaces");
    let unknown = format!("custom={}", directory.path().join("unknown").display());
    for path in [&native, &all, &bare, &libpath] {
        std::fs::create_dir_all(path).unwrap();
    }
    let args = RustcArgs::parse(&[
        "rustc".into(),
        "--crate-type=bin".into(),
        "-L".into(),
        format!("native={}", native.display()),
        format!("-Lall={}", all.display()),
        format!("-L{}", bare.display()),
        format!("-L{unknown}"),
        format!(
            "-Ldependency={}",
            directory.path().join("dependency").display()
        ),
        format!("-Lcrate={}", directory.path().join("crate").display()),
        format!(
            "-Lframework={}",
            directory.path().join("framework").display()
        ),
        format!(r#"-Clink-arg=/LIBPATH:"{}""#, libpath.display()),
        "-Clink-arg=/DEBUG".into(),
    ])
    .unwrap();
    assert_eq!(
        windows_native_link_search_dirs(&args).unwrap(),
        WindowsNativeLinkSearchDirs {
            rustc: vec![
                native.clone(),
                all.clone(),
                bare.clone(),
                PathBuf::from(unknown.clone())
            ],
            linker: vec![native, all, bare, PathBuf::from(unknown), libpath],
        }
    );
}

#[test]
fn native_macos_sdk_probe_fails_closed() {
    let bin = parsed_linked_bin(None);
    let mut hasher = blake3::Hasher::new();
    let err = fold_native_link_runtime_identity(
        &mut hasher,
        &bin,
        DARWIN_RUSTC_VERSION,
        false,
        true,
        |_| unreachable!("macOS fold must not probe Linux CRT"),
        |_| anyhow::bail!("sdk missing"),
        &KeyEnv::default(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("determining macOS SDK identity for cache key")
    );
}

#[test]
fn the_sdk_probe_gets_sdkroot_from_the_snapshot() {
    let bin = parsed_linked_bin(None);
    let probed = |env: &KeyEnv| {
        let mut sdkroot = None;
        fold_native_link_runtime_identity(
            &mut blake3::Hasher::new(),
            &bin,
            DARWIN_RUSTC_VERSION,
            false,
            true,
            |_| unreachable!("macOS fold must not probe Linux CRT"),
            |value| {
                sdkroot = Some(value);
                Ok("sdk".to_string())
            },
            env,
        )
        .unwrap();
        sdkroot.expect("the SDK was probed")
    };
    assert_eq!(
        probed(&env_with("SDKROOT", "/sdk")).as_deref(),
        Some("/sdk")
    );
    assert_eq!(probed(&KeyEnv::default()), None);
}

/// An unremapped build bakes its working directory into DWARF, so the
/// snapshot's cwd is part of its identity.
#[test]
fn unremapped_identity_folds_the_snapshot_cwd() {
    let args = RustcArgs::parse(&["rustc".to_string(), "lib.rs".to_string()]).unwrap();
    let fold = |cwd: Option<&str>| {
        let mut hasher = blake3::Hasher::new();
        let env = KeyEnv::from_parts([] as [(&str, &str); 0], cwd.map(PathBuf::from));
        fold_unremapped_path_identity(&mut hasher, &args, &PathNormalizer::empty(), &env);
        hasher.finalize()
    };
    assert_ne!(fold(Some("/work/a")), fold(Some("/work/b")));
    assert_ne!(fold(Some("/work/a")), fold(None));
    assert_eq!(fold(Some("/work/a")), fold(Some("/work/a")));
}

#[test]
fn metadata_only_outputs_do_not_probe_crt_or_sdk() {
    let mut metadata = parsed_linked_bin(None);
    metadata.emit = vec!["metadata".to_string()];
    let mut hasher = blake3::Hasher::new();
    fold_native_link_runtime_identity(
        &mut hasher,
        &metadata,
        GNU_RUSTC_VERSION,
        true,
        true,
        |_| panic!("metadata-only output must not probe CRT"),
        |_| panic!("metadata-only output must not probe SDK"),
        &KeyEnv::default(),
    )
    .unwrap();
}

#[test]
fn crt_fold_requires_linux_os_and_linux_rustc_host() {
    let args = parsed_linked_bin(None);
    let a = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        true,
        false,
        "crt1.o=aaa;libc.so.6=bbb",
        "",
        None,
    )
    .unwrap();
    let b = crt_fold_key(
        &args,
        DARWIN_RUSTC_VERSION,
        true,
        false,
        "crt1.o=zzz;libc.so.6=yyy",
        "",
        None,
    )
    .unwrap();
    assert_eq!(
        a, b,
        "a Darwin rustc hosted on Linux must not fold Linux CRT objects"
    );
}

#[test]
fn sdk_fold_requires_macos_os_and_darwin_rustc_host() {
    let args = parsed_linked_bin(None);
    let a = crt_fold_key(&args, GNU_RUSTC_VERSION, false, true, "", "14.0 (a)", None).unwrap();
    let b = crt_fold_key(&args, GNU_RUSTC_VERSION, false, true, "", "15.0 (b)", None).unwrap();
    assert_eq!(
        a, b,
        "a GNU rustc hosted on macOS must not fold the Darwin SDK"
    );
}

#[test]
fn windows_hosts_do_not_key_linux_crt_or_macos_sdk() {
    let bin = parsed_linked_bin(None);
    assert_eq!(
        crt_fold_key(
            &bin,
            GNU_RUSTC_VERSION,
            false,
            false,
            "crt1.o=aaa;libc.so.6=bbb",
            "14.0 (a)",
            Some("11.0"),
        )
        .unwrap(),
        crt_fold_key(
            &bin,
            DARWIN_RUSTC_VERSION,
            false,
            false,
            "crt1.o=zzz;libc.so.6=yyy",
            "15.0 (b)",
            Some("12.0"),
        )
        .unwrap(),
        "Windows hosts keep the existing linker --version identity only"
    );
}

#[test]
fn libc_probe_output_parsing_is_strict_and_canonical() {
    assert_eq!(
        parse_getconf_gnu_libc("glibc 2.39\n").as_deref(),
        Some("2.39")
    );
    assert_eq!(parse_getconf_gnu_libc("musl 1.2.5\n"), None);
    assert_eq!(parse_getconf_gnu_libc("glibc unknown\n"), None);

    assert_eq!(
        parse_ldd_libc("ldd (Debian GLIBC 2.36-9) 2.36\nCopyright ..."),
        Some((LinuxLibcFamily::Gnu, "2.36".to_string()))
    );
    assert_eq!(
        parse_ldd_libc("musl libc (x86_64)\nVersion 1.2.5\nDynamic Program Loader"),
        Some((LinuxLibcFamily::Musl, "1.2.5".to_string()))
    );
    assert_eq!(parse_ldd_libc("BusyBox ldd\n"), None);
}

#[test]
fn is_valid_cache_key_accepts_real_blake3_hex() {
    // A real key is 64 lowercase hex chars (blake3 to_hex).
    let key = fold_labeled("seed".into(), "label", "value");
    assert_eq!(key.len(), 64);
    assert!(is_valid_cache_key(&key));
    assert!(is_valid_cache_key(&"a".repeat(64)));
    assert!(is_valid_cache_key(&"0123456789abcdef".repeat(4)));
}

#[test]
fn apply_key_salt_no_salt_is_identity() {
    let base = "deadbeef".to_string();
    // None and empty/whitespace are both treated as "unsalted" and
    // must return the base key byte-for-byte (no CACHE_KEY_VERSION
    // bump, no effect for projects that never set it).
    assert_eq!(apply_key_salt(base.clone(), None, "crate"), base);
    assert_eq!(apply_key_salt(base.clone(), Some(""), "crate"), base);
}

#[test]
fn apply_key_salt_changes_key_and_is_salt_specific() {
    let base = "deadbeef".to_string();
    let a = apply_key_salt(base.clone(), Some("toolchain-A"), "crate");
    let b = apply_key_salt(base.clone(), Some("toolchain-B"), "crate");
    // A salt re-keys, and distinct salts produce distinct keys.
    assert_ne!(a, base);
    assert_ne!(b, base);
    assert_ne!(a, b);
    // Deterministic: same (base, salt) → same key.
    assert_eq!(a, apply_key_salt(base, Some("toolchain-A"), "crate"));
}

/// Set `name` for the duration of the guard, restoring the previous value
/// (or absence) on drop. Callers must hold [`key_test_lock`]: the process
/// environment is global and `apply_key_env_vars` reads all of it.
struct ScopedEnv {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnv {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        // SAFETY: single-threaded test body under `key_test_lock`.
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }

    fn unset(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        // SAFETY: single-threaded test body under `key_test_lock`.
        unsafe { std::env::remove_var(name) };
        Self { name, previous }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        // SAFETY: single-threaded test body under `key_test_lock`.
        unsafe {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }
}

#[test]
fn key_env_var_matches_exact_prefix_and_case() {
    let patterns = vec!["BOLTFFI_*".to_string(), "MODE".to_string()];
    // Trailing `*` is a prefix glob, including the degenerate zero-suffix case.
    assert!(key_env_var_matches(&patterns, "BOLTFFI_BINDING_EXPANSION"));
    assert!(key_env_var_matches(&patterns, "BOLTFFI_"));
    // A bare name matches only itself, not names it is a prefix of.
    assert!(key_env_var_matches(&patterns, "MODE"));
    assert!(!key_env_var_matches(&patterns, "MODE_EXTRA"));
    assert!(!key_env_var_matches(&patterns, "BOLTFF"));
    assert!(!key_env_var_matches(&patterns, "UNRELATED"));
    // ASCII case-insensitive, so a Windows environment behaves like a Unix one.
    assert!(key_env_var_matches(&patterns, "mode"));
    assert!(key_env_var_matches(&patterns, "boltffi_root"));
}

#[test]
fn key_env_var_matches_treats_interior_star_literally() {
    // Only a trailing `*` globs; an interior one is a literal character that
    // no real env var name carries. `normalize_key_env_vars` warns about it.
    let patterns = vec!["A*B".to_string()];
    assert!(!key_env_var_matches(&patterns, "AXB"));
    assert!(!key_env_var_matches(&patterns, "AB"));
    assert!(key_env_var_matches(&patterns, "A*B"));
}

#[test]
fn apply_key_env_vars_no_patterns_is_identity() {
    let base = "deadbeef".to_string();
    // Feature off must leave the key byte-for-byte unchanged — that is what
    // lets this ship without a CACHE_KEY_VERSION bump.
    assert_eq!(apply_key_env_vars(base.clone(), &[], "crate"), base);
    assert_eq!(key_env_guard(&[]), None);
}

#[test]
fn adaptive_key_env_guard_changes_with_the_selected_value() {
    let _lock = key_test_lock();
    let patterns = vec!["KACHE_TEST_ADAPTIVE_ENV".to_string()];
    let first = {
        let _guard = ScopedEnv::set("KACHE_TEST_ADAPTIVE_ENV", "one");
        key_env_guard(&patterns).unwrap()
    };
    let second = {
        let _guard = ScopedEnv::set("KACHE_TEST_ADAPTIVE_ENV", "two");
        key_env_guard(&patterns).unwrap()
    };
    assert_ne!(first, second);
}

#[test]
fn apply_key_env_vars_separates_set_from_unset() {
    let _lock = key_test_lock();
    let base = "deadbeef".to_string();
    let patterns = vec!["KACHE_TEST_EXPANSION".to_string()];

    let unset = {
        let _guard = ScopedEnv::unset("KACHE_TEST_EXPANSION");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    let set_empty = {
        let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    let set_one = {
        let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "1");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    let set_two = {
        let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "2");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };

    // The #635 case: a proc macro branching on this var produces different
    // artifacts from a byte-identical rustc command line, so the two modes
    // must not share a key.
    assert_ne!(unset, set_one);
    assert_ne!(set_one, set_two);
    // `var("X")` distinguishes unset from set-to-empty, so the key must too.
    assert_ne!(unset, set_empty);
    // Declaring the var re-keys even when it is unset. Without this the
    // unset build would land back on the poisoned entry that motivated
    // the declaration in the first place.
    assert_ne!(unset, base);
}

#[test]
fn apply_key_env_vars_matches_by_prefix_glob() {
    let _lock = key_test_lock();
    let base = "deadbeef".to_string();
    let patterns = vec!["KACHE_TEST_PREFIX_*".to_string()];

    let none = {
        let _a = ScopedEnv::unset("KACHE_TEST_PREFIX_MODE");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    let one = {
        let _a = ScopedEnv::set("KACHE_TEST_PREFIX_MODE", "on");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    assert_ne!(none, one);
}

#[test]
fn apply_key_env_vars_is_declaration_order_and_case_independent() {
    use crate::config::normalize_key_env_vars;

    let _lock = key_test_lock();
    let base = "deadbeef".to_string();
    let _a = ScopedEnv::set("KACHE_TEST_ORDER_A", "1");
    let _b = ScopedEnv::set("KACHE_TEST_ORDER_B", "2");

    // The patterns are folded, so any two lists that SELECT the same
    // variables have to reduce to the same bytes — otherwise the feature
    // splits the cache between teammates instead of correcting it.
    // `normalize_key_env_vars` (applied by `Config::load`) does the
    // canonicalizing; this pins that the fold actually depends on it.
    let canonical = normalize_key_env_vars(
        [
            "KACHE_TEST_ORDER_A".to_string(),
            "KACHE_TEST_ORDER_B".to_string(),
        ],
        "test",
    );
    let expected = apply_key_env_vars(base.clone(), &canonical, "crate");
    for spelling in [
        // reordered
        vec![
            "KACHE_TEST_ORDER_B".to_string(),
            "KACHE_TEST_ORDER_A".to_string(),
        ],
        // differently cased (matching is case-insensitive)
        vec![
            "kache_test_order_a".to_string(),
            "Kache_Test_Order_B".to_string(),
        ],
        // duplicated, with stray whitespace
        vec![
            " KACHE_TEST_ORDER_A ".to_string(),
            "KACHE_TEST_ORDER_A".to_string(),
            "KACHE_TEST_ORDER_B".to_string(),
        ],
    ] {
        let normalized = normalize_key_env_vars(spelling.clone(), "test");
        assert_eq!(
            apply_key_env_vars(base.clone(), &normalized, "crate"),
            expected,
            "equivalent declaration {spelling:?} must fold identically"
        );
    }
}

/// `(name, value)` pair from string literals, for the digest tests.
fn pair(name: &str, value: &str) -> (Vec<u8>, Vec<u8>) {
    (name.as_bytes().to_vec(), value.as_bytes().to_vec())
}

#[test]
fn key_env_digest_ignores_environ_order() {
    let patterns = vec!["X*".to_string()];
    // `vars_os` order is platform-defined. Two machines listing the same
    // variables in a different order describe the same environment and must
    // land on the same entry, or the feature splits the cache by host.
    let forward = vec![pair("XA", "1"), pair("XB", "2"), pair("XC", "3")];
    let shuffled = vec![pair("XC", "3"), pair("XA", "1"), pair("XB", "2")];
    assert_eq!(
        key_env_digest(&patterns, forward),
        key_env_digest(&patterns, shuffled)
    );
}

#[test]
fn key_env_digest_preserves_order_when_a_name_repeats() {
    let patterns = vec!["X*".to_string()];
    // A duplicated name is only reachable through a hand-built `envp`, but
    // there `getenv` returns the FIRST occurrence — so the order is
    // semantically observable and sorting it away would be a wrong-hit.
    let first_wins = vec![pair("XA", "1"), pair("XA", "2")];
    let second_wins = vec![pair("XA", "2"), pair("XA", "1")];
    assert_ne!(
        key_env_digest(&patterns, first_wins),
        key_env_digest(&patterns, second_wins)
    );
}

#[test]
fn key_env_digest_separates_names_from_values() {
    let patterns = vec!["X*".to_string()];
    // Swapping which name holds which value is a different environment.
    // Folding the pairs (rather than a bag of values) is what pins that.
    assert_ne!(
        key_env_digest(&patterns, vec![pair("XA", "1"), pair("XB", "2")]),
        key_env_digest(&patterns, vec![pair("XA", "2"), pair("XB", "1")])
    );
}

#[test]
fn env_name_key_bytes_distinguishes_names() {
    use std::ffi::OsStr;
    let a = env_name_key_bytes(OsStr::new("KACHE_TEST_NAME_A"));
    let b = env_name_key_bytes(OsStr::new("KACHE_TEST_NAME_B"));
    // A name that folded to a constant would merge every declared variable
    // into one key component.
    assert!(!a.is_empty());
    assert_ne!(a, b);
}

#[test]
fn env_name_key_bytes_case_policy_follows_the_platform() {
    use std::ffi::OsStr;
    let upper = env_name_key_bytes(OsStr::new("KACHE_TEST_CASE"));
    let mixed = env_name_key_bytes(OsStr::new("Kache_Test_Case"));
    if cfg!(windows) {
        // One variable on Windows: folding the OS's reported casing would
        // split the cache between machines describing the same environment.
        assert_eq!(upper, mixed);
    } else {
        // Two genuinely different variables on Unix.
        assert_ne!(upper, mixed);
    }
}

#[test]
fn env_os_key_bytes_distinguishes_values() {
    use std::ffi::OsStr;
    assert_ne!(
        env_os_key_bytes(OsStr::new("expansion")),
        env_os_key_bytes(OsStr::new("normal"))
    );
    assert!(env_os_key_bytes(OsStr::new("")).is_empty());
}

/// Every rustup mechanism that can redirect an unchanged shim binary
/// must move the tool-version memo key: `RUSTUP_TOOLCHAIN`, the
/// nearest `rust-toolchain{,.toml}` up from the cwd, and the
/// `settings.toml` behind `rustup default`.
#[test]
fn toolchain_selector_fingerprint_tracks_every_selection_source() {
    use std::ffi::OsStr;
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("workspace").join("member");
    std::fs::create_dir_all(&project).unwrap();

    let base = toolchain_selector_fingerprint(None, Some(&project), None);
    assert_eq!(base, "", "no selection state folds to a stable empty");

    let pinned = toolchain_selector_fingerprint(Some(OsStr::new("1.93.0")), Some(&project), None);
    assert_ne!(pinned, base);
    assert_ne!(
        toolchain_selector_fingerprint(Some(OsStr::new("nightly")), Some(&project), None),
        pinned,
        "different overrides must fingerprint differently"
    );

    // The nearest directory with a toolchain file ends the ancestor
    // walk, and BOTH spellings fold so rustup's precedence between
    // them never matters: editing either one moves the fingerprint.
    std::fs::write(dir.path().join("workspace").join("rust-toolchain"), "1.88").unwrap();
    std::fs::write(
        dir.path().join("workspace").join("rust-toolchain.toml"),
        "[toolchain]\nchannel = \"1.90\"\n",
    )
    .unwrap();
    let with_files = toolchain_selector_fingerprint(None, Some(&project), None);
    assert_eq!(with_files.matches(";file:").count(), 2, "{with_files}");
    std::fs::write(dir.path().join("workspace").join("rust-toolchain"), "1.89").unwrap();
    let with_edited = toolchain_selector_fingerprint(None, Some(&project), None);
    assert_ne!(
        with_edited, with_files,
        "editing the bare file must move the fingerprint even though \
             the .toml sibling is untouched"
    );

    let settings = dir.path().join("settings.toml");
    std::fs::write(&settings, "default_toolchain = \"stable\"").unwrap();
    let with_default = toolchain_selector_fingerprint(None, Some(&project), Some(&settings));
    assert!(with_default.contains("default:"), "{with_default}");
    assert_ne!(with_default, base);
    std::fs::write(&settings, "default_toolchain = \"beta\"").unwrap();
    assert_ne!(
        toolchain_selector_fingerprint(None, Some(&project), Some(&settings)),
        with_default,
        "a rustup default switch must move the fingerprint"
    );
}

/// Both halves have to hold: a build that asked for predictions but has no
/// index DB (the daemon's store-free hasher) cannot read one.
#[test]
fn predictions_need_both_the_request_and_a_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    assert!(
        FileHasher::persistent(&db)
            .with_input_predictions(true)
            .uses_input_predictions()
    );
    assert!(
        !FileHasher::persistent(&db).uses_input_predictions(),
        "a hasher nobody asked must not read records"
    );
    assert!(
        !FileHasher::new()
            .with_input_predictions(true)
            .uses_input_predictions(),
        "asking is not enough without a table to read"
    );
    assert!(!FileHasher::new().uses_input_predictions());
}

/// Each refusal reaches a human only through the key trace, so the names
/// have to be distinct and stable enough to grep a build log for.
#[test]
fn every_rejection_names_itself_distinctly() {
    let all = [
        Rejection::Disabled,
        Rejection::NotEligible,
        Rejection::NoRecord,
        Rejection::Missing,
        Rejection::NotRegular,
        Rejection::EnvChanged,
        Rejection::Sibling,
    ];
    let mut names: Vec<&str> = all.iter().map(|r| r.as_str()).collect();
    assert!(
        names.iter().all(|name| !name.is_empty()),
        "a nameless refusal tells a reader nothing: {names:?}"
    );
    let before = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(before, names.len(), "two refusals share a name: {names:?}");
    assert_eq!(Rejection::Disabled.as_str(), "disabled");
    assert_eq!(Rejection::Sibling.as_str(), "sibling");
}

/// The wrapper reads this to decide whether a key still owes a
/// re-derivation, and a stale `true` would make it recompute a key that
/// was never predicted.
#[test]
fn the_prediction_marker_is_taken_once() {
    assert!(
        !take_last_key_used_prediction(),
        "nothing has been predicted on this thread"
    );
    LAST_KEY_USED_PREDICTION.with(|stash| stash.set(true));
    assert!(take_last_key_used_prediction());
    assert!(
        !take_last_key_used_prediction(),
        "taking must clear it, or the next key inherits this one's answer"
    );
}

/// The verification switch comes from the snapshot. With it on, a
/// validated prediction is checked against the pre-pass, and the pass's
/// answer is used instead.
#[test]
fn prediction_verification_reads_the_snapshot() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    // Cargo and nextest set kache's own OUT_DIR on the test process.
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", None);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, "pub fn f() {}\n").unwrap();
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "x".to_string(),
        source.to_str().unwrap().to_string(),
        "--edition=2021".to_string(),
        "--emit=dep-info,metadata".to_string(),
    ])
    .unwrap();
    let db = dir.path().join("index.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
        )
        .unwrap();
    let hasher = FileHasher::persistent(&db).with_input_predictions(true);
    let closure = DepInfo {
        source_files: vec![source.clone()],
        env_deps: Vec::new(),
    };
    let identity = rustc_prediction_identity(&args).unwrap();
    hasher.record_input_prediction(&identity, Some("x"), &closure, None);
    let resolve = |env: &KeyEnv| {
        take_last_key_used_prediction();
        let inputs = resolve_key_inputs(&args, &hasher, "x", env).unwrap();
        (inputs, take_last_key_used_prediction())
    };

    assert_eq!(resolve(&KeyEnv::default()), (Some(closure), true));
    let (verified, predicted) = resolve(&env_with("KACHE_VERIFY_INPUT_PREDICTIONS", "always"));
    assert!(!predicted, "the pre-pass answered, not the record");
    assert!(verified.is_some());
}

/// With no record and deferral allowed, the key stops instead of running
/// the pre-pass; the closure the wrapper hands back afterwards is used as
/// is.
#[test]
fn discovery_defers_to_the_compile_and_takes_the_emitted_closure() {
    let _lock = key_test_lock();
    // Exercise a unit without generated inputs. Cargo and nextest set
    // kache's own OUT_DIR on the test process, so clear it for this unit.
    let out_dir = std::env::var_os("OUT_DIR");
    // SAFETY: the key-test lock serialises environment edits.
    unsafe { std::env::remove_var("OUT_DIR") };
    struct RestoreOutDir(Option<std::ffi::OsString>);
    impl Drop for RestoreOutDir {
        fn drop(&mut self) {
            if let Some(value) = self.0.take() {
                // SAFETY: still under the key-test lock, dropped first.
                unsafe { std::env::set_var("OUT_DIR", value) };
            }
        }
    }
    let _restore = RestoreOutDir(out_dir);
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let out_dir = dir.path().join("target/debug/deps");
    let args = RustcArgs::parse(
        &[
            "rustc",
            "--crate-name",
            "x",
            "src/lib.rs",
            "--edition",
            "2021",
            "--emit=dep-info,metadata",
            "--out-dir",
            &out_dir.display().to_string(),
        ]
        .iter()
        .map(|a| (*a).to_string())
        .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(
        rustc_shared_prediction_identity(&args).is_some(),
        "a target directory gives the unit a shared record identity"
    );
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
        )
        .unwrap();
    let on = FileHasher::persistent(&db)
        .with_input_predictions(true)
        .with_prediction_flights(Some(dir.path().join("cache")));

    set_defer_discovery(true);
    let deferred = resolve_key_inputs(&args, &on, "x", &KeyEnv::default());
    set_defer_discovery(false);
    let error = deferred.expect_err("no record and deferral allowed: no pre-pass");
    assert!(
        error.downcast_ref::<DeferredDiscovery>().is_some(),
        "{error:#}"
    );
    // A broken handoff must fail instead of waiting on our own flight.
    drop(on.take_discovery_flight());

    let emitted = dir.path().join("lib.d");
    std::fs::write(
        &emitted,
        "/w/target/debug/deps/lib.rmeta: /w/src/lib.rs /w/src/inner.rs\n\n\
             /w/src/lib.rs:\n/w/src/inner.rs:\n# env-dep:CARGO_PKG_NAME=lib\n",
    )
    .unwrap();
    let closure = dep_info_from_emitted(&emitted, Path::new("/w/src/lib.rs")).unwrap();
    assert_eq!(
        closure.source_files,
        vec![
            PathBuf::from("/w/src/inner.rs"),
            PathBuf::from("/w/src/lib.rs")
        ]
    );
    assert_eq!(
        closure.env_deps,
        vec![("CARGO_PKG_NAME".to_string(), "lib".to_string())]
    );
    provide_dep_info(closure.clone());
    let used = resolve_key_inputs(&args, &on, "x", &KeyEnv::default()).unwrap();
    assert_eq!(used, Some(closure));
}

/// The discovery flight names the unit whether or not predictions are on,
/// and stays off for a unit predictions cannot describe.
#[test]
fn discovery_flight_identity_names_the_unit_with_or_without_predictions() {
    // Identity helpers read cwd and environment, which other tests mutate.
    let _lock = key_test_lock();
    let _workspace = crate::config::tests::set_env_for_test(
        "CARGO_MANIFEST_DIR",
        Some(std::ffi::OsStr::new("/w/a")),
    );
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let parse = |args: &[&str]| {
        RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let out_dir = dir.path().join("target/debug/deps").display().to_string();
    let a = parse(&[
        "rustc",
        "--crate-name",
        "a",
        "src/a.rs",
        "--out-dir",
        &out_dir,
    ]);
    let b = parse(&[
        "rustc",
        "--crate-name",
        "b",
        "src/b.rs",
        "--out-dir",
        &out_dir,
    ]);
    let with_macro = parse(&[
        "rustc",
        "--crate-name",
        "a",
        "src/a.rs",
        "--out-dir",
        &out_dir,
        "--extern",
        "my_macro=/t/debug/deps/libmy_macro-3.so",
    ]);

    let off = FileHasher::persistent(&db);
    let a_off = discovery_flight_identity(&a, &off).expect("predictions off still names the unit");
    assert_eq!(
        a_off,
        rustc_shared_prediction_identity(&a).unwrap(),
        "the flight is the unit's shared identity"
    );
    assert_ne!(a_off, discovery_flight_identity(&b, &off).unwrap());
    assert!(rustc_shared_prediction_identity(&with_macro).is_none());
    assert_eq!(
        discovery_flight_identity(&with_macro, &off),
        rustc_prediction_identity(&with_macro),
        "a proc-macro dependent still gets a name: the flight is only a lock"
    );

    let on = FileHasher::persistent(&db).with_input_predictions(true);
    assert_eq!(
        discovery_flight_identity(&a, &on),
        prediction_discovery_identity(&a, &on),
        "with predictions on the flight is the record identity"
    );
    assert!(prediction_discovery_identity(&with_macro, &on).is_none());
    assert_eq!(
        discovery_flight_identity(&with_macro, &on),
        rustc_prediction_identity(&with_macro),
        "no record identity, so the unit's name serves as the flight"
    );

    // In a registry package the same unit shares its record under the
    // tree guard, so it flies under the shared identity.
    let _manifest = crate::config::tests::set_env_for_test(
        "CARGO_MANIFEST_DIR",
        Some(std::ffi::OsStr::new("/h/registry/src/index-1/a-1.0.0")),
    );
    let shared = rustc_shared_prediction_identity(&with_macro)
        .expect("a registry proc-macro dependent has a shared identity");
    assert_eq!(
        discovery_flight_identity(&with_macro, &off),
        Some(shared.clone())
    );
    assert_eq!(discovery_flight_identity(&with_macro, &on), Some(shared));
}

/// A crate the store has never held is a certain miss under any key, so
/// discovery defers even where no record can vouch for it: an OUT_DIR
/// unit, and a build with predictions off.
#[test]
fn a_crate_the_store_never_held_defers_discovery() {
    let _lock = key_test_lock();
    let out_dir = std::env::var_os("OUT_DIR");
    // SAFETY: the key-test lock serialises environment edits.
    unsafe { std::env::set_var("OUT_DIR", "/t/debug/build/x-1/out") };
    struct RestoreOutDir(Option<std::ffi::OsString>);
    impl Drop for RestoreOutDir {
        fn drop(&mut self) {
            // SAFETY: still under the key-test lock, dropped first.
            match self.0.take() {
                Some(value) => unsafe { std::env::set_var("OUT_DIR", value) },
                None => unsafe { std::env::remove_var("OUT_DIR") },
            }
        }
    }
    let _restore = RestoreOutDir(out_dir);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
        )
        .unwrap();
    let args = RustcArgs::parse(
        &[
            "rustc",
            "--crate-name",
            "x",
            "src/lib.rs",
            "--edition",
            "2021",
            "--emit=dep-info,metadata",
            "--out-dir",
            &dir.path().join("target/debug/deps").display().to_string(),
        ]
        .iter()
        .map(|a| (*a).to_string())
        .collect::<Vec<_>>(),
    )
    .unwrap();
    let deferred = |hasher: &FileHasher<'_>| {
        set_defer_discovery(true);
        let outcome = resolve_key_inputs(&args, hasher, "x", &KeyEnv::default());
        set_defer_discovery(false);
        outcome.is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some())
    };
    let flights = Some(dir.path().join("cache"));
    for predictions in [false, true] {
        let hasher = FileHasher::persistent(&db)
            .with_input_predictions(predictions)
            .with_prediction_flights(flights.clone());
        assert!(deferred(&hasher), "predictions={predictions}");
    }
    {
        // Scoped: the hasher holds the unit's discovery flight until it
        // is dropped, and the hashers below need to take it.
        let hasher = FileHasher::persistent(&db).with_prediction_flights(flights.clone());
        let not_allowed = resolve_key_inputs(&args, &hasher, "x", &KeyEnv::default());
        assert!(
            !not_allowed.is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some()),
            "the wrapper did not allow deferral"
        );
    }
    assert!(
        !deferred(&FileHasher::persistent(&db)),
        "without a flight nobody owns the unit, so nobody compiles first"
    );

    rusqlite::Connection::open(&db)
        .unwrap()
        .execute(
            "INSERT INTO entries (cache_key, crate_name) VALUES ('k', 'x')",
            [],
        )
        .unwrap();
    for predictions in [false, true] {
        let hasher = FileHasher::persistent(&db)
            .with_input_predictions(predictions)
            .with_prediction_flights(flights.clone());
        assert!(
            !deferred(&hasher),
            "an entry for the crate may match: predictions={predictions}"
        );
    }
}

/// Every build script is `build_script_build`; Cargo's `-C metadata` hash
/// is what tells them apart, and the presence probe must read it.
#[test]
fn a_unit_the_store_never_held_defers_even_when_its_name_is_taken() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL,
                                       unit_id TEXT NOT NULL DEFAULT '');
                 INSERT INTO entries VALUES ('k', 'build_script_build', 'unit-a');",
        )
        .unwrap();
    let parse = |unit: &str| {
        RustcArgs::parse(
            &[
                "rustc",
                "--crate-name",
                "build_script_build",
                "build.rs",
                "--edition",
                "2021",
                "--emit=dep-info,link",
                "-C",
                &format!("metadata={unit}"),
                "--out-dir",
                &dir.path().join("target/debug/build").display().to_string(),
            ]
            .iter()
            .map(|a| (*a).to_string())
            .collect::<Vec<_>>(),
        )
        .unwrap()
    };
    let deferred = |args: &RustcArgs| {
        let hasher =
            FileHasher::persistent(&db).with_prediction_flights(Some(dir.path().join("cache")));
        set_defer_discovery(true);
        let outcome = resolve_key_inputs(args, &hasher, "build_script_build", &KeyEnv::default());
        set_defer_discovery(false);
        outcome.is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some())
    };
    assert!(
        deferred(&parse("unit-b")),
        "another unit of the same name is absent"
    );
    assert!(!deferred(&parse("unit-a")), "this unit was stored");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("INSERT INTO entries VALUES ('k2', 'build_script_build', '');")
        .unwrap();
    assert!(
        !deferred(&parse("unit-b")),
        "a row that never learned its unit stands for every unit of the name"
    );
}

/// The two gates in front of a record lookup, each refusing for its own
/// reason so the trace can say which.
#[test]
fn a_record_is_only_consulted_for_an_eligible_invocation() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let parse = |args: &[&str]| {
        RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let plain = parse(&["rustc", "src/lib.rs", "--edition", "2021"]);
    let with_macro = parse(&[
        "rustc",
        "src/lib.rs",
        "--edition",
        "2021",
        "--extern",
        "my_macro=/t/debug/deps/libmy_macro-3.so",
    ]);

    let off = FileHasher::persistent(&db);
    assert_eq!(
        predicted_key_inputs(&plain, &off),
        Err(Rejection::Disabled),
        "predictions off must not touch the table"
    );

    let on = FileHasher::persistent(&db).with_input_predictions(true);
    assert_eq!(
        predicted_key_inputs(&with_macro, &on),
        Err(Rejection::NotEligible),
        "a proc-macro dependency outside a registry package is refused before any lookup"
    );
    if get_rustc_version(Path::new("rustc")).is_ok() {
        assert_eq!(
            predicted_key_inputs(&plain, &on),
            Err(Rejection::NoRecord),
            "an eligible invocation with nothing recorded falls back"
        );
    }
}

/// A proc macro can scan a directory and emit `include_str!` per entry, so
/// a file can join the closure with nothing already in it changing. The
/// pre-pass sees it; a prediction would not. Cargo hands rustc a proc
/// macro as a dynamic library, and that is the test.
#[test]
fn predictions_do_not_apply_to_units_with_a_dynamic_library_dependency() {
    let dep = |path: &str| crate::args::ExternDep {
        name: "dep".to_string(),
        path: Some(PathBuf::from(path)),
    };
    let plain = vec![
        dep("/t/debug/deps/libserde-1.rlib"),
        dep("/t/debug/deps/libcore-2.rmeta"),
    ];
    assert!(
        prediction_applies(&plain),
        "rlib and rmeta dependencies cannot scan the filesystem"
    );
    assert!(
        prediction_applies(&[]),
        "a unit with no dependencies is eligible"
    );

    for macro_lib in [
        "/t/debug/deps/libmy_macro-3.so",
        "/t/debug/deps/libmy_macro-3.dylib",
        "/t/debug/deps/my_macro-3.dll",
    ] {
        let mut with_macro = plain.clone();
        with_macro.push(dep(macro_lib));
        assert!(
            !prediction_applies(&with_macro),
            "{macro_lib} may generate includes the record cannot know about"
        );
    }

    // A dependency cargo passed without a path tells us nothing either way
    // and must not be read as a proc macro.
    assert!(prediction_applies(&[crate::args::ExternDep {
        name: "std".to_string(),
        path: None,
    }]));
}

/// `src/foo.rs` and `src/foo/mod.rs` both answer `mod foo;`, and rustc
/// refuses to choose. Only the two spellings of a module have a sibling;
/// a crate root is named by argv, so it has none.
#[test]
fn mod_sibling_is_the_other_spelling_of_the_same_module() {
    assert_eq!(
        mod_sibling_candidate(Path::new("src/foo.rs")),
        Some(PathBuf::from("src/foo/mod.rs"))
    );
    assert_eq!(
        mod_sibling_candidate(Path::new("src/foo/mod.rs")),
        Some(PathBuf::from("src/foo.rs"))
    );
    for root in ["src/lib.rs", "src/main.rs"] {
        assert_eq!(
            mod_sibling_candidate(Path::new(root)),
            None,
            "{root} is named by argv, not by a mod item"
        );
    }
    assert_eq!(
        mod_sibling_candidate(Path::new("assets/data.json")),
        None,
        "an included asset is not a module"
    );
}

/// The verification knob is the measurement of the soundness argument on
/// real code, so its parsing has to be exact about what turns it on.
#[test]
fn verify_predictions_parses_its_three_modes() {
    for on in ["always", "ALWAYS", "1", "true", "True"] {
        assert_eq!(
            parse_verify_predictions(Some(on)),
            VerifyPredictions::Always,
            "{on} must verify every prediction"
        );
    }
    for sampled in ["sampled", "SAMPLED"] {
        assert_eq!(
            parse_verify_predictions(Some(sampled)),
            VerifyPredictions::Sampled
        );
    }
    for off in [Some("off"), Some("0"), Some("false"), Some(""), None] {
        assert_eq!(
            parse_verify_predictions(off),
            VerifyPredictions::Off,
            "{off:?} must not cost a pre-pass"
        );
    }

    assert!(!should_verify_this_prediction(
        VerifyPredictions::Off,
        "unit"
    ));
    assert!(should_verify_this_prediction(
        VerifyPredictions::Always,
        "unit"
    ));

    // Sampling is decided by the identity, because the wrapper is a fresh
    // process per compile. A rolling counter would start at zero every
    // time and select every unit, which is how `sampled` silently became
    // `always` and made a nightly measure the cost of both paths.
    let identities: Vec<String> = (0..VERIFY_PREDICTION_RATE * 20)
        .map(|i| format!("identity-{i}"))
        .collect();
    let chosen = identities
        .iter()
        .filter(|id| should_verify_this_prediction(VerifyPredictions::Sampled, id))
        .count();
    assert!(
        (5..=40).contains(&chosen),
        "about 1 in {VERIFY_PREDICTION_RATE} of {} should be checked, got {chosen}",
        identities.len()
    );
    assert!(
        identities
            .iter()
            .any(|id| !should_verify_this_prediction(VerifyPredictions::Sampled, id)),
        "sampling that selects everything is not sampling"
    );

    // Stable: the same unit is checked on every build, so a disagreement
    // is reproducible instead of a one-off nobody can chase.
    let first = identities
        .iter()
        .find(|id| should_verify_this_prediction(VerifyPredictions::Sampled, id))
        .expect("some identity must be selected");
    for _ in 0..5 {
        assert!(should_verify_this_prediction(
            VerifyPredictions::Sampled,
            first
        ));
    }
}

/// Where a unit's package lives, which decides how Cargo runs rustc.
#[derive(Clone, Copy)]
enum SamplePlace {
    /// A workspace member: run from the workspace root, relative source.
    Member,
    /// A path dependency outside the workspace root: run from its package
    /// root, absolute source.
    OutsideWorkspace,
    /// A registry package under the Cargo home, run the same way.
    Registry,
}

/// One Cargo unit, placed in a checkout by [`SampleUnit::in_checkout`].
struct SampleUnit {
    package: String,
    version: &'static str,
    place: SamplePlace,
    crate_name: &'static str,
    source: &'static str,
    crate_type: &'static str,
    target: Option<&'static str>,
}

/// What the wrapper sees for one unit: argv, the working directory as
/// `getcwd` reports it, and the environment Cargo sets.
struct SampleCall {
    args: RustcArgs,
    cwd: PathBuf,
    env: HashMap<&'static str, std::ffi::OsString>,
}

impl SampleCall {
    fn identity(&self) -> String {
        prediction_sample_identity(&self.args, Some(&self.cwd), |var| {
            self.env.get(var).cloned()
        })
    }

    fn verified(&self) -> bool {
        verifies_prediction(
            VerifyPredictions::Sampled,
            &self.args,
            Some(&self.cwd),
            |var| self.env.get(var).cloned(),
        )
    }
}

/// `path` as macOS `getcwd` spells it: `/tmp` is a symlink there.
fn physical(path: &str) -> String {
    match path.strip_prefix("/tmp/") {
        Some(rest) => format!("/private/tmp/{rest}"),
        None => path.to_string(),
    }
}

impl SampleUnit {
    /// This unit in the checkout at `root`, whose workspace is `rust/`,
    /// with its registry under `cargo_home`.
    ///
    /// Cargo hashes a registry package id the same way everywhere, but a
    /// path dependency outside the workspace root by its absolute path,
    /// and every member depending on one inherits that in its
    /// `-C metadata`. The metadata here does the same. A target spec is
    /// passed as an absolute path, as Cargo does.
    fn in_checkout(&self, root: &str, cargo_home: &str) -> SampleCall {
        let workspace = format!("{root}/rust");
        let package_dir = match self.place {
            SamplePlace::Member => format!("{workspace}/{}", self.package),
            SamplePlace::OutsideWorkspace => format!("{root}/{}", self.package),
            SamplePlace::Registry => format!(
                "{cargo_home}/registry/src/index.crates.io-0000/{}-{}",
                self.package, self.version
            ),
        };
        let (cwd, source, hashed) = match self.place {
            SamplePlace::Member => (
                workspace.clone(),
                format!("{}/{}", self.package, self.source),
                package_dir.clone(),
            ),
            SamplePlace::OutsideWorkspace => (
                package_dir.clone(),
                format!("{package_dir}/{}", self.source),
                package_dir.clone(),
            ),
            SamplePlace::Registry => (
                package_dir.clone(),
                format!("{package_dir}/{}", self.source),
                format!("{}-{}", self.package, self.version),
            ),
        };
        let metadata = blake3::hash(hashed.as_bytes()).to_hex()[..16].to_string();
        let deps = format!("{workspace}/target/debug/deps");
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            self.crate_name.to_string(),
            "--edition=2021".to_string(),
            source,
            "--error-format=json".to_string(),
            "--crate-type".to_string(),
            self.crate_type.to_string(),
            "--emit=dep-info,metadata,link".to_string(),
            "--cfg".to_string(),
            "feature=\"default\"".to_string(),
            "-C".to_string(),
            format!("metadata={metadata}"),
            "-C".to_string(),
            format!("extra-filename=-{metadata}"),
            "--out-dir".to_string(),
            deps.clone(),
            "-L".to_string(),
            format!("dependency={deps}"),
            "--extern".to_string(),
            format!("dep={deps}/libdep-0123456789abcdef.rmeta"),
        ];
        if let Some(target) = self.target {
            argv.push("--target".to_string());
            argv.push(if target.ends_with(".json") {
                format!("{workspace}/{target}")
            } else {
                target.to_string()
            });
        }
        let env = HashMap::from([
            ("CARGO_MANIFEST_DIR", package_dir.into()),
            ("CARGO_PKG_NAME", self.package.clone().into()),
            ("CARGO_PKG_VERSION", self.version.into()),
        ]);
        SampleCall {
            args: RustcArgs::parse(&argv).unwrap(),
            cwd: PathBuf::from(physical(&cwd)),
            env,
        }
    }
}

/// The units `sampled` picks in one checkout.
fn sampled_units(units: &[SampleUnit], root: &str, cargo_home: &str) -> Vec<usize> {
    units
        .iter()
        .enumerate()
        .filter(|(_, unit)| unit.in_checkout(root, cargo_home).verified())
        .map(|(i, _)| i)
        .collect()
}

/// Two checkouts of one project, as a perf gate builds base and head. The
/// base Cargo home sits behind a symlink, as anything under `/tmp` does on
/// macOS.
const SAMPLE_BASE: (&str, &str) = ("/scratch/base/hk", "/tmp/cargo");
const SAMPLE_HEAD: (&str, &str) = ("/scratch/head/hk", "/home/ci/.cargo");

/// A perf gate builds base and head in separate checkouts. Sampling by
/// the prediction identity checked different units in each, because that
/// identity folds the working directory and absolute paths: hk counted 7
/// checks against 2, and eza showed a significant speedup that was only 3
/// checks against none. Both checkouts must check the same units.
#[test]
fn prediction_sample_is_the_same_in_every_checkout() {
    let units: Vec<SampleUnit> = (0..VERIFY_PREDICTION_RATE * 8)
        .map(|i| SampleUnit {
            package: format!("pkg{i}"),
            version: "1.0.0",
            place: [
                SamplePlace::Member,
                SamplePlace::OutsideWorkspace,
                SamplePlace::Registry,
            ][i % 3],
            crate_name: ["alpha", "beta", "build_script_build", "delta"][i % 4],
            source: ["src/lib.rs", "src/main.rs", "build.rs", "src/lib.rs"][i % 4],
            crate_type: ["lib", "bin", "bin", "lib"][i % 4],
            target: [
                None,
                Some("aarch64-apple-darwin"),
                Some("specs/os.json"),
                None,
                None,
            ][i % 5],
        })
        .collect();
    for (i, unit) in units.iter().enumerate() {
        assert_eq!(
            unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1).identity(),
            unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1).identity(),
            "unit {i} must be sampled the same way in both checkouts"
        );
    }
    let in_base = sampled_units(&units, SAMPLE_BASE.0, SAMPLE_BASE.1);
    assert!(!in_base.is_empty(), "some unit must be checked");
    assert_eq!(in_base, sampled_units(&units, SAMPLE_HEAD.0, SAMPLE_HEAD.1));
}

/// A path dependency outside the workspace root, such as `rust/`
/// depending on `../proto`, gets a `-C metadata` that names the checkout,
/// and so does every member that depends on it. The sample must not read
/// it.
#[test]
fn prediction_sample_ignores_metadata_that_names_the_checkout() {
    for place in [SamplePlace::OutsideWorkspace, SamplePlace::Member] {
        let unit = SampleUnit {
            package: "proto".to_string(),
            version: "0.1.0",
            place,
            crate_name: "proto",
            source: "src/lib.rs",
            crate_type: "lib",
            target: None,
        };
        let base = unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1);
        let head = unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1);
        assert_ne!(
            base.args.get_codegen_opt("metadata"),
            head.args.get_codegen_opt("metadata")
        );
        assert_eq!(base.identity(), head.identity());
    }
}

/// Cargo names a registry package's root and its source through the Cargo
/// home as given, but `getcwd` resolves symlinks. Stripping only the
/// working directory left the source absolute behind a symlinked Cargo
/// home, and every registry unit sampled differently.
#[test]
fn prediction_sample_strips_the_package_root_as_cargo_spells_it() {
    let unit = SampleUnit {
        package: "serde".to_string(),
        version: "1.0.0",
        place: SamplePlace::Registry,
        crate_name: "serde",
        source: "src/lib.rs",
        crate_type: "lib",
        target: None,
    };
    let base = unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1);
    assert!(base.cwd.starts_with("/private/tmp"));
    assert!(base.args.source_file.as_ref().unwrap().starts_with("/tmp"));
    assert_eq!(
        base.identity(),
        unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1).identity()
    );
}

/// The same in every checkout must not mean one decision for a whole
/// crate name: every registry build script is `build_script_build` from
/// `build.rs` in its package root, and only its package tells them apart.
#[test]
fn prediction_sample_spreads_across_units_that_share_a_name() {
    let build_scripts: Vec<SampleUnit> = (0..VERIFY_PREDICTION_RATE * 20)
        .map(|i| SampleUnit {
            package: format!("pkg{i}"),
            version: "1.0.0",
            place: SamplePlace::Registry,
            crate_name: "build_script_build",
            source: "build.rs",
            crate_type: "bin",
            target: None,
        })
        .collect();
    let chosen = sampled_units(&build_scripts, SAMPLE_BASE.0, SAMPLE_BASE.1);
    assert!(
        (5..=40).contains(&chosen.len()),
        "about 1 in {VERIFY_PREDICTION_RATE} of {} should be checked, got {}",
        build_scripts.len(),
        chosen.len()
    );
    assert_eq!(
        chosen,
        sampled_units(&build_scripts, SAMPLE_HEAD.0, SAMPLE_HEAD.1),
        "another checkout checks the same build scripts"
    );

    // Each part of the unit's name separates it from its neighbours.
    let sample = |argv: &str, package: [&str; 2]| {
        let argv: Vec<String> = argv.split(' ').map(str::to_string).collect();
        let env = |var: &str| match var {
            "CARGO_PKG_NAME" => Some(package[0].into()),
            "CARGO_PKG_VERSION" => Some(package[1].into()),
            _ => None,
        };
        prediction_sample_identity(
            &RustcArgs::parse(&argv).unwrap(),
            Some(Path::new("/w")),
            env,
        )
    };
    let unit = "rustc --crate-name a a.rs --crate-type lib";
    let package = ["a", "1.0.0"];
    let base = sample(unit, package);
    for (part, other) in [
        ("package name", sample(unit, ["b", "1.0.0"])),
        ("package version", sample(unit, ["a", "2.0.0"])),
        (
            "crate type",
            sample(&format!("{unit} --crate-type rlib"), package),
        ),
        ("test harness", sample(&format!("{unit} --test"), package)),
        (
            "target",
            sample(&format!("{unit} --target wasm32-wasip1"), package),
        ),
        (
            "cfg",
            sample(&format!("{unit} --cfg feature=\"std\""), package),
        ),
        (
            "crate name",
            sample("rustc --crate-name b a.rs --crate-type lib", package),
        ),
        (
            "source",
            sample("rustc --crate-name a b.rs --crate-type lib", package),
        ),
    ] {
        assert_ne!(other, base, "{part} must separate units");
    }
    assert_ne!(
        sample(&format!("{unit} --target /w/a.json"), package),
        sample(&format!("{unit} --target /w/b.json"), package),
        "target specs must separate units"
    );
}

/// Cargo passes a target spec as an absolute path; rustc names the target
/// by the file's stem.
#[test]
fn target_name_is_a_spec_file_stem() {
    assert_eq!(target_name("wasm32-wasip1"), "wasm32-wasip1");
    assert_eq!(target_name("/scratch/base/hk/specs/os.json"), "os");
    assert_eq!(target_name("os.json"), "os");
}

/// The source a unit is sampled by is the one every checkout spells the
/// same way.
#[test]
fn checkout_relative_drops_the_first_root_the_path_is_under() {
    let package = Path::new("/tmp/cargo/registry/src/index/serde-1.0.0");
    let resolved = Path::new("/private/tmp/cargo/registry/src/index/serde-1.0.0");
    let source = package.join("src/lib.rs");
    let relative = Path::new("src/lib.rs");
    assert_eq!(
        checkout_relative(&source, &[Some(package), Some(resolved)]),
        relative
    );
    assert_eq!(
        checkout_relative(&source, &[Some(resolved), Some(package)]),
        relative
    );
    assert_eq!(checkout_relative(&source, &[None, Some(resolved)]), source);
    assert_eq!(checkout_relative(&source, &[]), source);
    assert_eq!(checkout_relative(relative, &[Some(package)]), relative);
    assert_eq!(
        checkout_relative(Path::new("/elsewhere/src/lib.rs"), &[Some(package)]),
        Path::new("/elsewhere/src/lib.rs")
    );
}

/// Two closures naming the same files agree however they are ordered: the
/// key sorts both before folding, so order cannot change a key and must
/// not be reported as a disagreement.
#[test]
fn closures_agree_on_content_not_order() {
    let dep = |sources: &[&str], env: &[(&str, &str)]| DepInfo {
        source_files: sources.iter().map(PathBuf::from).collect(),
        env_deps: env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    };
    let a = dep(&["src/lib.rs", "src/helper.rs"], &[("OUT_DIR", "/t")]);
    let reordered = dep(&["src/helper.rs", "src/lib.rs"], &[("OUT_DIR", "/t")]);
    assert!(closures_agree(&a, &reordered));

    assert!(!closures_agree(
        &a,
        &dep(
            &["src/lib.rs", "src/helper.rs", "src/extra.rs"],
            &[("OUT_DIR", "/t")]
        )
    ));
    assert!(!closures_agree(
        &a,
        &dep(&["src/lib.rs"], &[("OUT_DIR", "/t")])
    ));
    assert!(!closures_agree(
        &a,
        &dep(&["src/lib.rs", "src/helper.rs"], &[])
    ));
    assert!(!closures_agree(
        &a,
        &dep(&["src/lib.rs", "src/helper.rs"], &[("OUT_DIR", "/other")])
    ));
}

/// The rules, one refusal at a time. Each is a claim that the recorded
/// closure no longer describes what rustc would read.
#[test]
fn a_prediction_is_validated_against_the_tree_as_it_is_now() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let lib = src.join("lib.rs");
    let helper = src.join("helper.rs");
    std::fs::write(&lib, "mod helper;\n").unwrap();
    std::fs::write(&helper, "pub fn n() {}\n").unwrap();

    let record = InputPrediction {
        schema: PREDICTION_SCHEMA,
        sources: vec![lib.clone(), helper.clone()],
        env_deps: vec![("OUT_DIR".to_string(), "/t/build/out".to_string())],
        tree: None,
    };
    let stat = |path: &Path| std::fs::metadata(path).ok();
    let exists = |path: &Path| path.exists();
    let env = |var: &str| (var == "OUT_DIR").then(|| "/t/build/out".to_string());

    let accepted = validate_prediction(&record, stat, exists, env)
        .expect("an unchanged tree must reuse the recorded closure");
    assert_eq!(accepted.source_files, record.sources);
    assert_eq!(accepted.env_deps, record.env_deps);

    // The value the key normalises to a sentinel still has to match raw:
    // a moved OUT_DIR is how an included generated file changes identity
    // without any recorded file changing.
    assert_eq!(
        validate_prediction(&record, stat, exists, |var| (var == "OUT_DIR")
            .then(|| "/t/build/other".to_string())),
        Err(Rejection::EnvChanged)
    );
    assert_eq!(
        validate_prediction(&record, stat, exists, |_| None),
        Err(Rejection::EnvChanged),
        "an unset variable is not the value that was recorded"
    );

    // A file that is gone: the pre-pass would fail too, and the build
    // passes through to rustc's own error.
    std::fs::remove_file(&helper).unwrap();
    assert_eq!(
        validate_prediction(&record, stat, exists, env),
        Err(Rejection::Missing)
    );

    // A path that is no longer a regular file.
    std::fs::create_dir(&helper).unwrap();
    assert_eq!(
        validate_prediction(&record, stat, exists, env),
        Err(Rejection::NotRegular)
    );
    std::fs::remove_dir(&helper).unwrap();
    std::fs::write(&helper, "pub fn n() {}\n").unwrap();

    // Both spellings of `mod helper;` present: rustc errors (E0761), so
    // replaying a recorded success would restore an artifact for a build
    // that should fail.
    std::fs::create_dir(src.join("helper")).unwrap();
    std::fs::write(src.join("helper/mod.rs"), "pub fn n() {}\n").unwrap();
    assert_eq!(
        validate_prediction(&record, stat, exists, env),
        Err(Rejection::Sibling)
    );
}

/// The dep-info parser emits `VAR=` both when the variable is unset and
/// when it is empty, so validation must accept both for an empty record:
/// otherwise any crate reading an unset `option_env!` would reject every
/// record and pay a pre-pass on every warm build. A recorded value still
/// rejects an unset variable.
#[test]
fn an_unset_variable_matches_an_empty_record() {
    let dir = tempfile::tempdir().unwrap();
    let lib = dir.path().join("lib.rs");
    std::fs::write(&lib, "pub fn f() {}\n").unwrap();
    let stat = |path: &Path| std::fs::metadata(path).ok();
    let exists = |path: &Path| path.exists();
    let empty = InputPrediction {
        schema: PREDICTION_SCHEMA,
        sources: vec![lib.clone()],
        env_deps: vec![("KACHE_PROBE_UNSET".to_string(), String::new())],
        tree: None,
    };
    assert!(
        validate_prediction(&empty, stat, exists, |_| None).is_ok(),
        "unset matches empty, exactly as the parser would fold it"
    );
    let valued = InputPrediction {
        schema: PREDICTION_SCHEMA,
        sources: vec![lib],
        env_deps: vec![("KACHE_PROBE_UNSET".to_string(), "1".to_string())],
        tree: None,
    };
    assert_eq!(
        validate_prediction(&valued, stat, exists, |_| None),
        Err(Rejection::EnvChanged)
    );
}

/// A prediction is only reusable by an invocation that would discover the
/// same files. Every part of the identity is one such "would discover the
/// same files" claim, so perturbing any of them has to produce a different
/// row rather than a wrong answer.
#[test]
fn prediction_identity_separates_every_part_it_folds() {
    let base_args: Vec<String> = ["--edition", "2021", "--crate-name", "demo"]
        .iter()
        .map(|a| (*a).to_string())
        .collect();
    let base_env = |extra: &[(&str, &str)]| -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        let mut env: Vec<(std::ffi::OsString, std::ffi::OsString)> = vec![(
            std::ffi::OsString::from("CARGO_CFG_TARGET_OS"),
            std::ffi::OsString::from("linux"),
        )];
        env.extend(
            extra
                .iter()
                .map(|(k, v)| (std::ffi::OsString::from(k), std::ffi::OsString::from(v))),
        );
        env
    };
    let base_parts = PredictionIdentityParts {
        rustc_version: "rustc 1.95.0",
        inner_rustc: None,
        current_dir: Some(Path::new("/w/one")),
        source_file: Path::new("src/lib.rs"),
        closure_args: &base_args,
        skip_path_remap: false,
    };
    let base = prediction_identity_in_env(&base_parts, base_env(&[]));

    assert_eq!(
        prediction_identity_in_env(&base_parts, base_env(&[])),
        base,
        "the same invocation twice is the same identity"
    );

    let other_args: Vec<String> = ["--edition", "2024", "--crate-name", "demo"]
        .iter()
        .map(|a| (*a).to_string())
        .collect();
    let perturbed: Vec<(&str, PredictionIdentityParts<'_>)> = vec![
        (
            "a different compiler build",
            PredictionIdentityParts {
                rustc_version: "rustc 1.96.0",
                ..base_parts
            },
        ),
        (
            "a wrapped inner compiler",
            PredictionIdentityParts {
                inner_rustc: Some(Path::new("/usr/bin/rustc")),
                ..base_parts
            },
        ),
        (
            "another working directory, which relative args resolve against",
            PredictionIdentityParts {
                current_dir: Some(Path::new("/w/two")),
                ..base_parts
            },
        ),
        (
            "another crate root",
            PredictionIdentityParts {
                source_file: Path::new("src/main.rs"),
                ..base_parts
            },
        ),
        (
            "an edition change, which changes what resolves",
            PredictionIdentityParts {
                closure_args: &other_args,
                ..base_parts
            },
        ),
        (
            "path remapping turned off",
            PredictionIdentityParts {
                skip_path_remap: true,
                ..base_parts
            },
        ),
    ];
    for (what, parts) in perturbed {
        assert_ne!(
            prediction_identity_in_env(&parts, base_env(&[])),
            base,
            "{what} must not reuse another invocation's record"
        );
    }

    // The environment the key already folds, one variable at a time.
    for var in PREDICTION_ENV {
        assert_ne!(
            prediction_identity_in_env(&base_parts, base_env(&[(var, "value")])),
            base,
            "{var} must separate identities"
        );
    }
    // Set-but-empty is not unset: `env!` can tell them apart.
    assert_ne!(
        prediction_identity_in_env(&base_parts, base_env(&[("RUSTFLAGS", "")])),
        base,
        "an empty RUSTFLAGS is not an absent one"
    );
    // A cfg change can add or remove whole modules.
    let mut other_cfg = base_env(&[]);
    other_cfg[0].1 = std::ffi::OsString::from("windows");
    assert_ne!(
        prediction_identity_in_env(&base_parts, other_cfg),
        base,
        "CARGO_CFG_* changes which modules compile"
    );
    // Unrelated environment must NOT separate identities: folding it
    // wholesale is what kept the cc preprocessor memo from ever hitting
    // (kunobi-ninja/kache#927).
    assert_eq!(
        prediction_identity_in_env(&base_parts, base_env(&[("PWD", "/somewhere/else")])),
        base,
        "variables that do not change what rustc reads must not be folded"
    );
}

/// An invocation with no crate root discovers no closure, so it has no
/// identity — and two invocations that read different files must not share
/// one.
///
/// The comparisons run against one environment snapshot rather than the
/// live process environment: other tests in this binary set and unset
/// variables concurrently, which would otherwise decide the result.
#[test]
fn rustc_prediction_identity_needs_a_crate_root_and_separates_units() {
    // The identity also reads cwd, which other tests change under this lock.
    let _lock = crate::test_support::process_state_test_lock();
    let parse = |args: &[&str]| {
        RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    assert_eq!(
        rustc_prediction_identity(&parse(&["rustc", "--version"])),
        None,
        "a query invocation compiles nothing and predicts nothing"
    );

    if get_rustc_version(Path::new("rustc")).is_err() {
        return; // no compiler on this host; nothing to identify against
    }
    let env: Vec<(std::ffi::OsString, std::ffi::OsString)> = vec![(
        std::ffi::OsString::from("CARGO_CFG_TARGET_OS"),
        std::ffi::OsString::from("linux"),
    )];
    let identity = |args: &[&str]| {
        rustc_prediction_identity_in_env(&parse(args), env.clone())
            .expect("a crate root gives an identity")
    };

    let lib = identity(&["rustc", "src/lib.rs", "--edition", "2021"]);
    assert!(!lib.is_empty());
    assert_ne!(
        lib,
        identity(&["rustc", "src/main.rs", "--edition", "2021"]),
        "different crate roots read different files"
    );
    assert_ne!(
        lib,
        identity(&["rustc", "src/lib.rs", "--edition", "2024"]),
        "a different edition resolves differently"
    );

    // Output naming is not identity: these two read the same files.
    assert_eq!(
        lib,
        identity(&[
            "rustc",
            "src/lib.rs",
            "--edition",
            "2021",
            "-C",
            "extra-filename=-abc123",
        ]),
        "two units of one crate share a record"
    );
}

/// The identity is the argv that shapes the closure, not the argv that
/// names the output. Two units of one crate differing only in where their
/// artifacts land read exactly the same files.
#[test]
fn prediction_identity_ignores_output_naming_flags() {
    let closure = |extra: &[&str]| -> Vec<String> {
        let mut args: Vec<String> = ["--edition", "2021"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        args.extend(extra.iter().map(|a| (*a).to_string()));
        closure_shaping_args(Path::new("src/lib.rs"), &args)
    };
    let plain = closure(&[]);
    for naming in [
        vec!["-C", "extra-filename=-abc123"],
        vec!["--out-dir", "/target/debug/deps"],
        vec!["--emit=metadata,link"],
    ] {
        assert_eq!(
            closure(&naming),
            plain,
            "{naming:?} decides where output goes, not what rustc reads"
        );
    }
    assert_ne!(
        closure(&["--cfg", "feature=\"extra\""]),
        plain,
        "a cfg can add a module, so it stays in the identity"
    );
}

/// A build script's `OUT_DIR` identifies the unit, not the build
/// directory: the same unit in two target directories shares a record,
/// two feature sets keep their own, and a value outside the target
/// directory (or one that escapes it) is left exactly as it was.
#[test]
fn shared_predictions_place_out_dir_inside_its_target() {
    let vars = |target: &str, out_dir: &str| {
        let pairs = vec![
            (
                std::ffi::OsString::from("OUT_DIR"),
                std::ffi::OsString::from(out_dir),
            ),
            (
                std::ffi::OsString::from("CARGO_MANIFEST_DIR"),
                std::ffi::OsString::from("/registry/libc-0.2"),
            ),
        ];
        shared_prediction_vars(pairs.into_iter(), Path::new(target))
    };
    let value = |v: &[(std::ffi::OsString, std::ffi::OsString)], name: &str| {
        v.iter()
            .find(|(n, _)| n == name)
            .map(|(_, value)| value.to_string_lossy().into_owned())
            .unwrap()
    };

    let a = vars(
        "/a/target",
        "/a/target/debug/build/libc-abc123def4567890/out",
    );
    let b = vars(
        "/b/target",
        "/b/target/debug/build/libc-abc123def4567890/out",
    );
    assert_eq!(value(&a, "OUT_DIR"), value(&b, "OUT_DIR"));
    assert_eq!(
        value(&a, "OUT_DIR"),
        "kache-target-relative:debug/build/libc-abc123def4567890/out"
    );
    assert_eq!(
        value(&a, "CARGO_MANIFEST_DIR"),
        "/registry/libc-0.2",
        "only OUT_DIR is rewritten"
    );
    let other_features = vars(
        "/a/target",
        "/a/target/debug/build/libc-0123456789abcdef/out",
    );
    assert_ne!(
        value(&a, "OUT_DIR"),
        value(&other_features, "OUT_DIR"),
        "Cargo's metadata hash still separates two feature sets"
    );
    for outside in [
        "/elsewhere/build/libc-abc123def4567890/out",
        "/a/target/../sneaky/out",
    ] {
        assert_eq!(
            value(&vars("/a/target", outside), "OUT_DIR"),
            outside,
            "a value the target directory does not contain is left alone"
        );
    }
    assert!(
        target_relative_env_value(
            std::ffi::OsStr::new("/a/target/debug/build/x/out"),
            Path::new("/a/target")
        )
        .is_some()
    );
    assert!(
        target_relative_env_value(std::ffi::OsStr::new("/a/target"), Path::new("/a/target"))
            .is_some(),
        "the target directory itself is relative to itself"
    );
}

#[test]
fn shared_predictions_only_virtualize_dependency_paths() {
    let map = |target: &str, args: &[&str]| {
        shared_prediction_args(
            &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            Path::new(target),
        )
    };
    for (left, right) in [
        (
            vec!["--extern", "dep=/a/target/debug/libdep.rlib"],
            vec!["--extern", "dep=/b/target/debug/libdep.rlib"],
        ),
        (
            vec!["--extern=dep=/a/target/debug/libdep.rmeta"],
            vec!["--extern=dep=/b/target/debug/libdep.rmeta"],
        ),
        (
            vec!["-L", "dependency=/a/target/debug/deps"],
            vec!["-L", "dependency=/b/target/debug/deps"],
        ),
        (
            vec!["-Lnative=/a/target/debug/build/out"],
            vec!["-Lnative=/b/target/debug/build/out"],
        ),
        (
            vec!["-L/a/target/debug/deps"],
            vec!["-L/b/target/debug/deps"],
        ),
    ] {
        assert_eq!(map("/a/target", &left), map("/b/target", &right));
    }
    for (left, right) in [
        (
            vec!["--cfg", "path=\"/a/target/value\""],
            vec!["--cfg", "path=\"/b/target/value\""],
        ),
        (
            vec!["/a/target/generated.rs"],
            vec!["/b/target/generated.rs"],
        ),
        (
            vec!["--extern", "a=/a/target/lib.rlib"],
            vec!["--extern", "b=/b/target/lib.rlib"],
        ),
        (
            vec!["--extern", "a=/a/target/lib.rlib"],
            vec!["--extern", "a=/b/target/other.rlib"],
        ),
        (
            vec!["-Lnative=/a/target/lib"],
            vec!["-Ldependency=/b/target/lib"],
        ),
        (
            vec!["--extern", "a=/a/target-extra/lib.rlib"],
            vec!["--extern", "a=/b/target-extra/lib.rlib"],
        ),
        (
            vec!["--extern", "a=/a/target/../lib.rlib"],
            vec!["--extern", "a=/b/target/../lib.rlib"],
        ),
    ] {
        assert_ne!(map("/a/target", &left), map("/b/target", &right));
    }
    let encoded = map("/a/target", &["-L/a/target/lib"]);
    assert_ne!(encoded, map("/a/target", &[&encoded[0]]));
}

#[test]
fn shared_predictions_reject_target_sources() {
    let _lock = crate::test_support::process_state_test_lock();
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let args = RustcArgs::parse(&[
        "rustc".into(),
        "src/lib.rs".into(),
        "--out-dir".into(),
        target.join("debug/deps").to_string_lossy().into_owned(),
    ])
    .unwrap();
    let mut dep = DepInfo {
        source_files: vec![root.path().join("src/lib.rs")],
        env_deps: vec![],
    };
    // A workspace package, whatever package runs this test.
    let can_record = |args: &RustcArgs, dep: &DepInfo| {
        shared_prediction_can_record_in(args, dep, Some(root.path()))
    };
    assert!(can_record(&args, &dep));
    dep.source_files
        .push(target.join("debug/build/pkg/out/generated.rs"));
    assert!(!can_record(&args, &dep));
    let no_target = RustcArgs::parse(&["rustc".into(), "src/lib.rs".into()]).unwrap();
    assert!(!can_record(&no_target, &dep));
}

const REGISTRY_PACKAGE_DIR: &str = "/h/registry/src/index-1/kt-1.0.0";

fn os_vars(pairs: &[(&str, &str)]) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    pairs
        .iter()
        .map(|(name, value)| (name.into(), value.into()))
        .collect()
}

/// An absolute target directory for checkout `name` on any platform.
fn checkout_target(name: &str) -> String {
    if cfg!(windows) {
        format!("C:/{name}/target")
    } else {
        format!("/{name}/target")
    }
}

fn registry_unit_args(checkout: &str, externs: &[&str]) -> RustcArgs {
    let target = checkout_target(checkout);
    let mut argv = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "kt".to_string(),
        format!("{REGISTRY_PACKAGE_DIR}/src/lib.rs"),
        "--out-dir".to_string(),
        format!("{target}/debug/deps"),
    ];
    for name in externs {
        argv.push("--extern".to_string());
        argv.push(format!("{name}={target}/debug/deps/lib{name}-1.so"));
    }
    RustcArgs::parse(&argv).unwrap()
}

fn unit_vars(checkout: &str, manifest_dir: &str) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let target = checkout_target(checkout);
    os_vars(&[
        ("CARGO_MANIFEST_DIR", manifest_dir),
        ("OUT_DIR", &format!("{target}/debug/build/kt-1/out")),
    ])
}

#[test]
fn shared_prediction_eligibility_takes_either_rule() {
    let dep = |path: &str| crate::args::ExternDep {
        name: "dep".to_string(),
        path: Some(PathBuf::from(path)),
    };
    let plain = [dep("/t/debug/deps/libserde-1.rlib")];
    let with_macro = [dep("/t/debug/deps/libpm-1.so")];
    let registry = Path::new(REGISTRY_PACKAGE_DIR);
    let workspace = Path::new("/w/kt");
    assert!(shared_prediction_eligible(&plain, Some(workspace)));
    assert!(shared_prediction_eligible(&plain, None));
    assert!(
        shared_prediction_eligible(&with_macro, Some(registry)),
        "a registry unit's record carries the tree guard"
    );
    assert!(!shared_prediction_eligible(&with_macro, Some(workspace)));
    assert!(!shared_prediction_eligible(&with_macro, None));
}

/// A proc-macro dependent in a registry package gets one shared identity
/// whichever target directory builds it; in a workspace it gets none.
#[test]
fn shared_identity_covers_tree_guarded_registry_units() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let identity = |checkout: &str, manifest_dir: &str| {
        rustc_shared_prediction_identity_in(
            &registry_unit_args(checkout, &["pm"]),
            unit_vars(checkout, manifest_dir),
        )
    };
    let a = identity("a", REGISTRY_PACKAGE_DIR);
    assert!(
        a.as_deref()
            .is_some_and(|a| a.starts_with("shared-target-v2:"))
    );
    assert_eq!(a, identity("b", REGISTRY_PACKAGE_DIR));
    assert_eq!(identity("a", "/w/kt"), None);
}

/// The relocated row is filed under the shared hash, the same for every
/// target directory, and only for a registry unit with an `OUT_DIR`.
#[test]
fn a_relocatable_identity_is_the_shared_hash_of_a_registry_unit_with_an_out_dir() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let args = registry_unit_args("a", &[]);
    let vars = unit_vars("a", REGISTRY_PACKAGE_DIR);
    let shared = rustc_shared_prediction_identity_in(&args, vars.clone()).unwrap();
    let relocatable = relocatable_prediction_identity(&args, vars).unwrap();
    assert_eq!(
        relocatable.strip_prefix("shared-out-dir-v1:"),
        shared.strip_prefix("shared-target-v2:")
    );
    assert_eq!(
        relocatable_prediction_identity(
            &registry_unit_args("b", &[]),
            unit_vars("b", REGISTRY_PACKAGE_DIR)
        ),
        Some(relocatable)
    );
    assert_eq!(
        relocatable_prediction_identity(
            &args,
            os_vars(&[("CARGO_MANIFEST_DIR", REGISTRY_PACKAGE_DIR)])
        ),
        None,
        "no OUT_DIR to relocate"
    );
    let workspace = unit_vars("a", "/w/kt");
    assert!(rustc_shared_prediction_identity_in(&args, workspace.clone()).is_some());
    assert_eq!(
        relocatable_prediction_identity(&args, workspace),
        None,
        "a workspace unit"
    );
}

#[test]
fn the_registry_src_root_is_two_levels_above_a_registry_package() {
    assert_eq!(
        registry_src_root(Path::new(REGISTRY_PACKAGE_DIR)),
        Some(Path::new("/h/registry/src"))
    );
    assert_eq!(registry_src_root(Path::new("/w/kt")), None);
}

#[test]
fn an_out_dir_suffix_is_the_bytes_after_the_directory() {
    let suffix =
        |value: &str, root: &str| out_dir_suffix(std::ffi::OsStr::new(value), Path::new(root));
    assert_eq!(
        suffix("/o", "/o").as_deref(),
        Some(""),
        "the directory itself"
    );
    assert_eq!(suffix("/o/x/y.rs", "/o").as_deref(), Some("/x/y.rs"));
    assert_eq!(suffix("/o\\x\\y.rs", "/o").as_deref(), Some("\\x\\y.rs"));
    assert_eq!(
        suffix("/o2/x", "/o"),
        None,
        "a longer name is another directory"
    );
    assert_eq!(suffix("/p/x", "/o"), None);
    assert_eq!(suffix("/o/../x", "/o"), None);
    assert_eq!(suffix("/o\\..\\x", "/o"), None);
    assert_eq!(suffix("/o/./x", "/o").as_deref(), Some("/./x"));
    assert_eq!(suffix("/o/./../x", "/o"), None);
    assert_eq!(suffix("/o//x", "/o"), None);
    assert_eq!(suffix("/o/", "/o"), None);
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        assert_eq!(
            out_dir_suffix(std::ffi::OsStr::from_bytes(b"/o/\xff"), Path::new("/o")),
            None,
            "a suffix that is not UTF-8"
        );
    }
    assert!(under_registry_src(
        Path::new("/r/src/i/p/lib.rs"),
        Path::new("/r/src")
    ));
    assert!(
        !under_registry_src(Path::new("/r/src"), Path::new("/r/src")),
        "the root is not a file under it"
    );
}

/// Every source of a registry unit's shared row is a registry file; the
/// rule for other units is unchanged.
#[test]
fn a_registry_unit_shares_only_sources_under_the_registry() {
    let target = Path::new("/w/target");
    let can = |sources: &[&str], manifest_dir: Option<&str>| {
        let sources: Vec<PathBuf> = sources.iter().map(PathBuf::from).collect();
        shared_sources_can_record(&sources, target, manifest_dir.map(Path::new))
    };
    let lib = "/h/registry/src/index-1/kt-1.0.0/src/lib.rs";
    let registry = Some(REGISTRY_PACKAGE_DIR);
    assert!(can(
        &[lib, "/h/registry/src/index-2/other-1.0.0/x.rs"],
        registry
    ));
    assert!(!can(&[lib, "/w/Cargo.lock"], registry), "a checkout file");
    assert!(!can(&["/h/registry/src/index-1/../../x.rs"], registry));
    assert!(can(&["/w/kt/src/lib.rs"], Some("/w/kt")));
    assert!(can(&["/w/kt/src/lib.rs"], None));
    assert!(
        !can(&["/w/target/debug/build/kt-1/out/gen.rs"], Some("/w/kt")),
        "no unit shares a target source"
    );
}

/// A `..` counts only from deeper than `floor`, and a walk that leaves
/// never comes back in.
#[test]
fn a_walk_stays_below_its_floor() {
    let walk = |path: &str, floor| stays_below(path.split('/'), floor);
    assert!(walk("a/../b", 0));
    assert!(!walk("../b", 0), "a `..` at the floor leaves it");
    assert!(!walk("a/../../a/b", 0), "leaving and coming back");
    assert!(walk("i/p/src/../README.md", 2));
    assert!(walk("i/p/src/a/b/../../x.rs", 2));
    assert!(!walk("i/p/src/../../q/x.rs", 2));
    assert!(!walk("i/../j/p/x.rs", 2), "below the floor too");
    assert!(walk("a/./b", 0), "a `.` stays where it is");
    assert!(walk("./a", 0));
    assert!(!walk("./../b", 0), "a `.` does not go down a level");
    assert!(!walk("i/p/./../q/x.rs", 2));
    assert!(!walk("a//b", 0));
}

/// rustc reports `include_str!("../README.md")` as `src/../README.md`
/// and never normalizes it. Refusing every `..` kept such a package's
/// record out of other checkouts; one that stays inside the directory is
/// as portable as a path without it.
#[test]
fn an_out_dir_suffix_keeps_a_parent_component_that_stays_inside() {
    let suffix =
        |value: &str, root: &str| out_dir_suffix(std::ffi::OsStr::new(value), Path::new(root));
    assert_eq!(
        suffix("/o/x/../y.rs", "/o").as_deref(),
        Some("/x/../y.rs"),
        "spelled as rustc spelled it"
    );
    assert_eq!(
        suffix("/o\\x\\..\\y.rs", "/o").as_deref(),
        Some("\\x\\..\\y.rs")
    );
    assert_eq!(suffix("/o/x/../../y.rs", "/o"), None);
    assert_eq!(
        suffix("/o/x\\y/../../z.rs", "/o"),
        None,
        "on Unix `x\\y` is one level, so this leaves `/o`"
    );
    assert_eq!(
        suffix("/o/x\\..\\..\\y.rs", "/o"),
        None,
        "on Windows this leaves `/o`"
    );
}

/// Path shapes rustc reported for registry dependencies, all refused
/// while any `..` or `.` was: the `..` ones from hk, `src/./init.js` from
/// `include_str!("./init.js")`. A `..` that leaves the package is still
/// refused, even into another registry package.
#[test]
fn a_registry_unit_shares_a_parent_path_that_stays_in_its_package() {
    let target = Path::new("/w/target");
    let can = |source: &str| {
        shared_sources_can_record(
            &[PathBuf::from(source)],
            target,
            Some(Path::new(REGISTRY_PACKAGE_DIR)),
        )
    };
    let index = "/h/registry/src/index-1";
    for inside in [
        "kt-1.0.0/src/../README.md",
        "kt-1.0.0/src/crypto/aws_lc_rs/../ring/hash.rs",
        "kt-1.0.0/src/new/glibc/sysdeps/nptl/bits/../../x86/mod.rs",
        "other-1.0.0/src/../data/mod.rs",
        "kt-1.0.0/src/./init.js",
    ] {
        assert!(can(&format!("{index}/{inside}")), "{inside}");
    }
    for outside in [
        "kt-1.0.0/src/../../other-1.0/lib.rs",
        "kt-1.0.0/../other-1.0/lib.rs",
        "kt-1.0.0/src/../../../../../w/src/lib.rs",
        "kt-1.0.0/./../other-1.0/lib.rs",
    ] {
        assert!(!can(&format!("{index}/{outside}")), "{outside}");
    }
}

fn portable_roots_for_test() -> PortableRoots {
    PortableRoots {
        out_dir: PathBuf::from("/t/debug/build/kt-1/out"),
        target: PathBuf::from("/t"),
        canonical_target: PathBuf::from("/private/t"),
        registry_src: PathBuf::from("/h/registry/src"),
    }
}

fn portable_test_record() -> PortablePrediction {
    PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources: vec![
            Portable::Literal("/h/registry/src/index-1/kt-1.0.0/src/lib.rs".to_string()),
            Portable::OutDir("\\version.expr".to_string()),
            Portable::OutDir("/gen.rs".to_string()),
        ],
        env_deps: vec![
            ("OUT_DIR".to_string(), Portable::OutDir(String::new())),
            (
                "CARGO_PKG_NAME".to_string(),
                Portable::Literal("kt".to_string()),
            ),
        ],
        tree: "tree-1".to_string(),
    }
}

/// A workspace at `/w`, reached through `/private/w` as macOS spells
/// temporary directories, with its target inside.
fn workspace_test_roots() -> WorkspaceRoots {
    WorkspaceRoots {
        root: PathBuf::from("/w"),
        cwd: String::new(),
        canonical_root: PathBuf::from("/private/w"),
        target: PathBuf::from("/w/target"),
        canonical_target: PathBuf::from("/private/w/target"),
        out_dir: Some(PathBuf::from("/w/target/debug/build/kt-1/out")),
    }
}

#[test]
fn a_workspace_row_keeps_its_guard_only_while_the_closure_stays_inside() {
    let tree = || Some("tree".to_string());
    assert_eq!(same_tree_guard(tree(), true, true), tree());
    assert_eq!(same_tree_guard(tree(), true, false), None);
    assert_eq!(
        same_tree_guard(tree(), false, false),
        tree(),
        "registry and other units"
    );
    assert_eq!(same_tree_guard(tree(), false, true), tree());
    assert_eq!(same_tree_guard(None, true, true), None);
}

/// A two-member-free workspace at `dir/<name>` with its target inside,
/// and the member invocation Cargo would run there.
fn workspace_invocation(dir: &Path, name: &str, crate_name: &str) -> (PathBuf, RustcArgs) {
    let root = dir.join(name);
    std::fs::create_dir_all(root.join("kt/src")).unwrap();
    std::fs::create_dir_all(root.join("target/debug/deps")).unwrap();
    std::fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
    let deps = root.join("target/debug/deps").display().to_string();
    let argv: Vec<String> = [
        "rustc",
        "--crate-name",
        crate_name,
        "--edition=2021",
        "kt/src/lib.rs",
        "--crate-type",
        "lib",
        "--out-dir",
        &deps,
        "-L",
        &format!("dependency={deps}"),
    ]
    .iter()
    .map(|arg| arg.to_string())
    .collect();
    (root, RustcArgs::parse(&argv).unwrap())
}

fn manifest_vars(manifest_dir: &Path) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    vec![(
        "CARGO_MANIFEST_DIR".into(),
        manifest_dir.as_os_str().to_owned(),
    )]
}

#[test]
fn only_a_package_inside_the_workspace_is_a_workspace_unit() {
    let dir = tempfile::tempdir().unwrap();
    let (root, args) = workspace_invocation(dir.path(), "a", "kt");
    let inside = workspace_roots_in(&args, &manifest_vars(&root.join("kt")), &root).unwrap();
    assert_eq!(inside.cwd, "");
    assert_eq!(inside.target, root.join("target"));
    let member_cwd = workspace_roots_in(&args, &manifest_vars(&root.join("kt")), &root.join("kt"));
    assert_eq!(
        member_cwd.map(|roots| roots.cwd),
        Some(format!("{}kt", std::path::MAIN_SEPARATOR))
    );
    assert!(
        workspace_roots_in(&args, &manifest_vars(&dir.path().join("elsewhere")), &root).is_none(),
        "a package outside the workspace"
    );
    let registry = dir.path().join("home/registry/src/index-1/kt-1.0.0");
    assert!(workspace_roots_in(&args, &manifest_vars(&registry), &root).is_none());
    assert!(
        workspace_roots_in(&args, &[], &root).is_none(),
        "no package at all"
    );
}

#[test]
fn a_workspace_identity_is_the_same_in_every_checkout_and_per_unit() {
    let dir = tempfile::tempdir().unwrap();
    let identity = |name: &str, crate_name: &str| {
        let (root, args) = workspace_invocation(dir.path(), name, crate_name);
        let vars = manifest_vars(&root.join("kt"));
        let roots = workspace_roots_in(&args, &vars, &root).unwrap();
        workspace_prediction_identity(&args, vars, &roots).unwrap()
    };
    let a = identity("a", "kt");
    assert!(a.starts_with(WORKSPACE_PREDICTION_PREFIX), "{a}");
    assert_eq!(a, identity("b", "kt"), "another checkout of the same unit");
    assert_ne!(a, identity("c", "other"), "another unit");
}

#[test]
fn a_remote_row_is_kept_only_once_it_checks_out() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let hasher = FileHasher::persistent(&db);
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "kt".to_string(),
        "src/lib.rs".to_string(),
    ])
    .unwrap();
    let record = InputPrediction {
        schema: PREDICTION_SCHEMA,
        sources: vec![PathBuf::from("/r/i/kt-1.0.0/src/lib.rs")],
        env_deps: vec![],
        tree: None,
    };
    let dep_info = DepInfo {
        source_files: record.sources.clone(),
        env_deps: vec![],
    };
    keep_remote_plain_row(&hasher, "refused", &args, &record, &Err(Rejection::Missing));
    assert_eq!(hasher.input_prediction("refused"), None);
    keep_remote_plain_row(&hasher, "kept", &args, &record, &Ok(dep_info));
    assert_eq!(
        hasher.input_prediction("kept").map(|kept| kept.sources),
        Some(record.sources.clone())
    );
}

#[test]
fn a_workspace_value_is_relocated_refused_or_kept() {
    let roots = workspace_test_roots();
    let value = |value: &str| workspace_portable_value(std::ffi::OsStr::new(value), &roots);
    assert_eq!(
        value("/w/target/debug/build/kt-1/out/gen.rs"),
        Some(Portable::OutDir("/gen.rs".to_string()))
    );
    assert_eq!(
        value("/w/target/debug/deps/libx.rlib"),
        None,
        "another unit's output"
    );
    assert_eq!(value("/private/w/target/debug/deps/libx.rlib"), None);
    assert_eq!(
        value("/w/kt/src/lib.rs"),
        Some(Portable::Workspace("/kt/src/lib.rs".to_string()))
    );
    assert_eq!(
        value("/private/w/assets/a.txt"),
        Some(Portable::Workspace("/assets/a.txt".to_string()))
    );
    assert_eq!(value("/w"), Some(Portable::Workspace(String::new())));
    assert_eq!(
        value("/w2/x"),
        Some(Portable::Literal("/w2/x".to_string())),
        "a sibling that merely starts the same is not the workspace"
    );
    assert_eq!(value("kt"), Some(Portable::Literal("kt".to_string())));
}

#[test]
fn a_relative_source_must_stay_in_the_workspace() {
    let mut roots = workspace_test_roots();
    let source = |spelled: &str, roots: &WorkspaceRoots| {
        workspace_relative_source(Path::new(spelled), roots)
    };
    let kept = |spelled: &str| Some(Portable::Literal(spelled.to_string()));
    assert_eq!(source("kt/src/lib.rs", &roots), kept("kt/src/lib.rs"));
    assert_eq!(
        source("kt/src/../../assets/a.txt", &roots),
        kept("kt/src/../../assets/a.txt")
    );
    assert_eq!(source("../outside.txt", &roots), None);
    assert_eq!(source("kt//lib.rs", &roots), None);
    assert_eq!(
        source("kt\\..\\..\\outside.txt", &roots),
        None,
        "a Windows walk that leaves, which a `/` split alone would miss"
    );
    roots.cwd = "/kt".to_string();
    assert_eq!(source("../assets/a.txt", &roots), kept("../assets/a.txt"));
    assert_eq!(source("../../outside.txt", &roots), None);
}

#[test]
fn a_workspace_record_needs_the_guard_and_every_source_inside() {
    let roots = workspace_test_roots();
    let dep_info = |sources: &[&str]| DepInfo {
        source_files: sources.iter().map(PathBuf::from).collect(),
        env_deps: vec![("CARGO_MANIFEST_DIR".to_string(), "/w/kt".to_string())],
    };
    let inside = dep_info(&["kt/src/lib.rs", "/w/target/debug/build/kt-1/out/gen.rs"]);
    assert_eq!(
        workspace_portable_prediction(&inside, &roots, Some("tree-1")),
        Some(PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![
                Portable::Literal("kt/src/lib.rs".to_string()),
                Portable::OutDir("/gen.rs".to_string()),
            ],
            env_deps: vec![(
                "CARGO_MANIFEST_DIR".to_string(),
                Portable::Workspace("/kt".to_string())
            )],
            tree: "tree-1".to_string(),
        })
    );
    assert_eq!(workspace_portable_prediction(&inside, &roots, None), None);
    for outside in ["/elsewhere/x.rs", "../x.rs", "/w/target/debug/deps/x.rs"] {
        let closure = dep_info(&["kt/src/lib.rs", outside]);
        assert_eq!(
            workspace_portable_prediction(&closure, &roots, Some("t")),
            None,
            "{outside}"
        );
    }
    let mut env_in_target = dep_info(&["kt/src/lib.rs"]);
    env_in_target.env_deps = vec![("X".to_string(), "/w/target/debug/x".to_string())];
    assert_eq!(
        workspace_portable_prediction(&env_in_target, &roots, Some("t")),
        None
    );
}

#[test]
fn a_workspace_entry_resolves_only_where_this_invocation_has_a_workspace() {
    let record = PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources: vec![Portable::Workspace("/kt/src/lib.rs".to_string())],
        env_deps: vec![],
        tree: "t".to_string(),
    };
    let places = |out_dir, workspace| Places {
        out_dir,
        workspace,
        registry: None,
    };
    assert_eq!(record.resolve(&places(Some("/o"), None)), None);
    assert_eq!(
        record
            .resolve(&places(None, Some("/v")))
            .map(|resolved| resolved.sources),
        Some(vec![PathBuf::from("/v/kt/src/lib.rs")])
    );
    assert_eq!(
        portable_test_record().resolve(&places(None, Some("/v"))),
        None,
        "an OUT_DIR entry needs an OUT_DIR"
    );
}

#[test]
fn the_workspace_guard_covers_everything_but_target_and_git() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("w");
    let write = |path: &str, content: &str| {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write("Cargo.toml", "[workspace]\n");
    write("kt/src/lib.rs", "pub fn a() {}\n");
    write("assets/a.txt", "a\n");
    write("target/debug/build/kt-1/out/gen.rs", "// gen\n");
    let roots = WorkspaceRoots {
        root: root.clone(),
        cwd: String::new(),
        canonical_root: root.canonicalize().unwrap(),
        target: root.join("target"),
        canonical_target: root.join("target").canonicalize().unwrap(),
        out_dir: Some(root.join("target/debug/build/kt-1/out")),
    };
    let hasher = FileHasher::new();
    let digest = || workspace_tree_digest(&roots, &hasher).unwrap();
    let baseline = digest();

    write("target/debug/deps/libx.rlib", "x");
    write(".git/HEAD", "ref");
    assert_eq!(
        digest(),
        baseline,
        "target and .git are not what a macro reads"
    );

    write("docs/new.md", "new");
    assert_ne!(digest(), baseline, "a new file anywhere in the workspace");
    std::fs::remove_file(root.join("docs/new.md")).unwrap();
    std::fs::remove_dir(root.join("docs")).unwrap();
    assert_eq!(digest(), baseline);

    write("assets/a.txt", "changed");
    assert_ne!(digest(), baseline, "content");
    write("assets/a.txt", "a\n");
    write("target/debug/build/kt-1/out/gen.rs", "// other\n");
    assert_ne!(digest(), baseline, "OUT_DIR, though it lies under target");
}

#[test]
fn the_workspace_guard_gives_up_past_its_entry_budget() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("w");
    std::fs::create_dir_all(root.join("target")).unwrap();
    for name in ["a", "b", "c"] {
        std::fs::write(root.join(name), name).unwrap();
    }
    let roots = WorkspaceRoots {
        root: root.clone(),
        cwd: String::new(),
        canonical_root: root.clone(),
        target: root.join("target"),
        canonical_target: root.join("target"),
        out_dir: None,
    };
    let hasher = FileHasher::new();
    // Three files; `target` is skipped before it is counted.
    assert!(workspace_tree_digest_within(&roots, &hasher, 3).is_some());
    assert!(workspace_tree_digest_within(&roots, &hasher, 2).is_none());
}

#[test]
fn a_portable_record_relocates_out_dir_and_keeps_registry_paths() {
    let dep_info = DepInfo {
        source_files: vec![
            PathBuf::from("/h/registry/src/index-1/kt-1.0.0/src/lib.rs"),
            PathBuf::from("/t/debug/build/kt-1/out\\version.expr"),
            PathBuf::from("/t/debug/build/kt-1/out/gen.rs"),
        ],
        env_deps: vec![
            ("OUT_DIR".to_string(), "/t/debug/build/kt-1/out".to_string()),
            ("CARGO_PKG_NAME".to_string(), "kt".to_string()),
        ],
    };
    assert_eq!(
        portable_prediction(&dep_info, &portable_roots_for_test(), Some("tree-1")),
        Some(portable_test_record())
    );
}

#[test]
fn a_portable_record_refuses_anything_that_could_name_another_file() {
    let roots = portable_roots_for_test();
    let lib = "/h/registry/src/index-1/kt-1.0.0/src/lib.rs";
    let generated = "/t/debug/build/kt-1/out/gen.rs";
    let portable = |sources: &[&str], env: &[(&str, &str)], tree: Option<&str>| {
        let dep_info = DepInfo {
            source_files: sources.iter().map(PathBuf::from).collect(),
            env_deps: env
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        };
        portable_prediction(&dep_info, &roots, tree)
    };
    assert!(portable(&[lib, generated], &[], Some("tree")).is_some());
    for (source, why) in [
        ("/t/debug/build/other-2/out/x.rs", "another unit's output"),
        (
            "/private/t/debug/build/kt-1/out/gen.rs",
            "a canonical spelling of the target",
        ),
        ("/w/src/lib.rs", "a checkout path"),
        ("src/lib.rs", "a relative path"),
        (
            "/t/debug/build/kt-1/out/../../other-2/out/x.rs",
            "a parent component that leaves OUT_DIR",
        ),
        (
            "/h/registry/src/../x.rs",
            "a parent component under the registry",
        ),
    ] {
        assert_eq!(
            portable(&[lib, generated, source], &[], Some("tree")),
            None,
            "{why}"
        );
    }
    assert_eq!(portable(&[lib, generated], &[], None), None, "no guard");
    assert_eq!(
        portable(&[lib], &[], Some("tree")),
        None,
        "nothing to relocate"
    );
    for value in ["/t/debug/deps", "/private/t/debug/build/kt-1/out"] {
        assert_eq!(
            portable(&[lib, generated], &[("X", value)], Some("tree")),
            None,
            "{value} spells the target"
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let dep_info = DepInfo {
            source_files: vec![
                PathBuf::from(generated),
                PathBuf::from(std::ffi::OsStr::from_bytes(b"/h/registry/src/i/\xff.rs")),
            ],
            env_deps: Vec::new(),
        };
        assert_eq!(
            portable_prediction(&dep_info, &roots, Some("tree")),
            None,
            "a literal that is not UTF-8"
        );
    }
}

/// The relocated row follows the shared row's rule: a `..` inside the
/// package stays a literal, one inside `OUT_DIR` is relocated as spelled,
/// and one that leaves either refuses the row.
#[test]
fn a_portable_record_keeps_a_parent_path_inside_out_dir_or_the_package() {
    let roots = portable_roots_for_test();
    let readme = "/h/registry/src/index-1/kt-1.0.0/src/../README.md";
    let generated = "/t/debug/build/kt-1/out/sub/../gen.rs";
    let portable = |sources: &[&str]| {
        let dep_info = DepInfo {
            source_files: sources.iter().map(PathBuf::from).collect(),
            env_deps: Vec::new(),
        };
        portable_prediction(&dep_info, &roots, Some("tree"))
    };
    assert_eq!(
        portable(&[readme, generated]).map(|record| record.sources),
        Some(vec![
            Portable::Literal(readme.to_string()),
            Portable::OutDir("/sub/../gen.rs".to_string()),
        ])
    );
    for (source, why) in [
        (
            "/h/registry/src/index-1/kt-1.0.0/src/../../other-1.0/lib.rs",
            "leaves the package",
        ),
        ("/t/debug/build/kt-1/out/../out/gen.rs", "leaves OUT_DIR"),
    ] {
        assert_eq!(portable(&[generated, source]), None, "{why}");
    }
}

#[test]
fn a_portable_record_resolves_to_this_out_dir_byte_for_byte() {
    assert_eq!(
        portable_test_record().resolve(&Places {
            out_dir: Some("/u/debug/build/kt-1/out"),
            workspace: None,
            registry: None,
        }),
        Some(InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec![
                PathBuf::from("/h/registry/src/index-1/kt-1.0.0/src/lib.rs"),
                PathBuf::from("/u/debug/build/kt-1/out\\version.expr"),
                PathBuf::from("/u/debug/build/kt-1/out/gen.rs"),
            ],
            env_deps: vec![
                ("OUT_DIR".to_string(), "/u/debug/build/kt-1/out".to_string()),
                ("CARGO_PKG_NAME".to_string(), "kt".to_string()),
            ],
            tree: Some("tree-1".to_string()),
        })
    );
}

/// Relocation replaces the prefix only; the raw env value still has to
/// agree, and the guard has to be the one the row was made with.
#[test]
fn a_relocated_record_is_checked_against_this_out_dir_and_guard() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let record = PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources: vec![Portable::OutDir("/gen.rs".to_string())],
        env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
        tree: "tree-1".to_string(),
    };
    let check = |guard: &str, env_out_dir: &str| {
        validate_portable_prediction(
            &record,
            guard,
            &Places {
                out_dir: Some("/u/out"),
                workspace: None,
                registry: None,
            },
            |_| std::fs::metadata(file.path()).ok(),
            |_| false,
            |var| (var == "OUT_DIR").then(|| env_out_dir.to_string()),
        )
    };
    assert_eq!(
        check("tree-1", "/u/out"),
        Ok(DepInfo {
            source_files: vec![PathBuf::from("/u/out/gen.rs")],
            env_deps: vec![("OUT_DIR".to_string(), "/u/out".to_string())],
        })
    );
    assert_eq!(check("tree-1", "/v/out"), Err(Rejection::EnvChanged));
    assert_eq!(check("tree-2", "/u/out"), Err(Rejection::TreeChanged));
}

#[test]
fn mentions_root_finds_raw_canonical_and_escaped_spellings() {
    let raw = Path::new("/t/target");
    let canonical = Path::new("/private/t/target");
    assert!(mentions_root(b"// built in /t/target/debug\n", &[raw]));
    assert!(
        mentions_root(
            b"x /private/t/target/y",
            &[Path::new("/nowhere"), canonical]
        ),
        "any root"
    );
    assert!(!mentions_root(b"pub fn generated() {}", &[raw, canonical]));
    let windows = Path::new("C:\\t\\target");
    assert!(mentions_root(b"C:\\t\\target\\x", &[windows]), "as-is");
    assert!(
        mentions_root(b"\"C:\\\\t\\\\target\\\\x.rs\"", &[windows]),
        "escaped in a string literal"
    );
    assert!(!mentions_root(b"C:/t/target", &[windows]));
}

#[test]
fn an_out_dir_is_relocatable_only_when_no_file_names_the_target() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let out = target.join("debug/build/kt-1/out");
    std::fs::create_dir_all(out.join("nested")).unwrap();
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    std::fs::write(out.join("nested/more.rs"), "pub fn h() {}\n").unwrap();
    let roots = [target.as_path()];
    assert!(out_dir_spells_no_root(&out, &roots));
    std::fs::write(
        out.join("nested/more.rs"),
        format!("// {}\n", target.display()),
    )
    .unwrap();
    assert!(!out_dir_spells_no_root(&out, &roots), "a nested file");
    std::fs::write(out.join("nested/more.rs"), "pub fn h() {}\n").unwrap();
    #[cfg(unix)]
    {
        let link = out.join("link.rs");
        std::os::unix::fs::symlink(target.join("elsewhere.rs"), &link).unwrap();
        assert!(!out_dir_spells_no_root(&out, &roots), "a symlink's text");
        std::fs::remove_file(link).unwrap();
    }
    assert!(out_dir_spells_no_root(&out, &roots));
    assert!(
        !out_dir_spells_no_root(&dir.path().join("absent"), &roots),
        "a directory that cannot be read"
    );
    // 3 entries so far; the cap counts every entry.
    for index in 3..OUT_DIR_TREE_MAX_ENTRIES {
        std::fs::write(out.join(format!("f{index}")), "x").unwrap();
    }
    assert!(out_dir_spells_no_root(&out, &roots), "exactly at the cap");
    std::fs::write(out.join("one-more"), "x").unwrap();
    assert!(!out_dir_spells_no_root(&out, &roots), "past the cap");
}

#[test]
fn the_out_dir_guard_follows_content_and_stops_at_its_cap() {
    let dir = tempfile::tempdir().unwrap();
    let hasher = FileHasher::new();
    let a = dir.path().join("a/out");
    let b = dir.path().join("b/out");
    for out in [&a, &b] {
        std::fs::create_dir_all(out).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    }
    let guard = out_dir_tree_digest(&a, &hasher).unwrap();
    assert_eq!(
        out_dir_tree_digest(&b, &hasher).as_deref(),
        Some(guard.as_str())
    );
    std::fs::write(b.join("extra.rs"), "").unwrap();
    assert_ne!(
        out_dir_tree_digest(&b, &hasher).unwrap(),
        guard,
        "a new file"
    );
    for index in 2..OUT_DIR_TREE_MAX_ENTRIES {
        std::fs::write(b.join(format!("f{index}")), "x").unwrap();
    }
    assert!(
        out_dir_tree_digest(&b, &hasher).is_some(),
        "exactly at the cap"
    );
    std::fs::write(b.join("one-more"), "x").unwrap();
    assert_eq!(out_dir_tree_digest(&b, &hasher), None, "past the cap");
}

/// A build script can put anything in `OUT_DIR`, a nested `target` or a
/// git checkout included, and a macro scanning `OUT_DIR` reads it all.
/// Both guards that cover `OUT_DIR` digest it all; only the crate
/// directory skips those names.
#[test]
fn the_out_dir_guards_cover_target_and_git_under_out_dir() {
    let dir = tempfile::tempdir().unwrap();
    let hasher = FileHasher::new();
    let package = dir.path().join("registry/src/index/kt-1.0.0");
    let out = dir.path().join("out");
    for directory in [&package, &out] {
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(directory.join("lib.rs"), "pub fn g() {}\n").unwrap();
    }
    let guards = || {
        (
            out_dir_tree_digest(&out, &hasher).unwrap(),
            crate_tree_digest_in(package.clone(), Some(out.clone()), &hasher).unwrap(),
        )
    };
    let before = guards();
    for name in ["target", ".git"] {
        std::fs::create_dir_all(package.join(name)).unwrap();
        std::fs::write(package.join(name).join("x"), "x").unwrap();
    }
    assert_eq!(guards().1, before.1, "the crate directory skips them");
    for name in ["target", ".git"] {
        let nested = out.join(name);
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("x"), "x").unwrap();
        let after = guards();
        assert_ne!(after.0, before.0, "{name} under OUT_DIR, OUT_DIR guard");
        assert_ne!(after.1, before.1, "{name} under OUT_DIR, crate tree guard");
        std::fs::remove_dir_all(nested).unwrap();
    }
    assert_eq!(guards(), before);
}

/// A relocated row round-trips, and neither decoder accepts the other's
/// schema, whatever identity it is looked up under.
#[test]
fn a_portable_prediction_round_trips_and_no_other_decoder_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let hasher = FileHasher::persistent(&db);
    let record = portable_test_record();
    assert_eq!(hasher.portable_prediction("unit"), None);
    hasher.record_portable_prediction("unit", Some("kt"), &record);
    assert_eq!(
        FileHasher::persistent(&db).portable_prediction("unit"),
        Some(record)
    );
    assert_eq!(hasher.input_prediction("unit"), None);
    hasher.record_input_prediction(
        "plain",
        Some("kt"),
        &DepInfo {
            source_files: vec![PathBuf::from("/w/src/lib.rs")],
            env_deps: Vec::new(),
        },
        Some("tree".to_string()),
    );
    assert_eq!(hasher.portable_prediction("plain"), None);
    let cache = hasher.cache.as_ref().unwrap();
    cache
        .put_input_prediction("corrupt", PORTABLE_PREDICTION_SCHEMA, None, "not json")
        .unwrap();
    assert_eq!(hasher.portable_prediction("corrupt"), None);
    let stale = serde_json::to_string(&PortablePrediction {
        schema: PREDICTION_SCHEMA,
        ..portable_test_record()
    })
    .unwrap();
    cache
        .put_input_prediction("mismatched", PORTABLE_PREDICTION_SCHEMA, None, &stale)
        .unwrap();
    assert_eq!(hasher.portable_prediction("mismatched"), None);

    let no_db = FileHasher::new();
    no_db.record_portable_prediction("unit", None, &portable_test_record());
    assert_eq!(no_db.portable_prediction("unit"), None);
}

/// A tree the recorder wrote the relocated row from, in a temporary
/// registry: the package, and target `a` with its OUT_DIR.
fn relocatable_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let package = dir.path().join("home/registry/src/index-1/kt-1.0.0");
    std::fs::create_dir_all(package.join("src")).unwrap();
    std::fs::write(package.join("src/lib.rs"), "include!(\"x\");\n").unwrap();
    let target = dir.path().join("a/target");
    let out = target.join("debug/build/kt-1/out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(target.join("debug/deps")).unwrap();
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    (dir, package, target, out)
}

fn unit_args_in(package: &Path, target: &Path) -> RustcArgs {
    RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "kt".to_string(),
        package.join("src/lib.rs").display().to_string(),
        "--out-dir".to_string(),
        target.join("debug/deps").display().to_string(),
    ])
    .unwrap()
}

#[test]
fn a_relocatable_record_needs_a_registry_unit_and_a_clean_out_dir() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let (dir, package, target, out) = relocatable_fixture();
    let args = unit_args_in(&package, &target);
    let vars = |manifest_dir: &Path| {
        vec![
            ("CARGO_MANIFEST_DIR".into(), manifest_dir.into()),
            ("OUT_DIR".into(), out.clone().into()),
        ]
    };
    let dep_info = DepInfo {
        source_files: vec![package.join("src/lib.rs"), out.join("gen.rs")],
        env_deps: Vec::new(),
    };
    let (identity, record) =
        relocatable_record_in(&args, &dep_info, Some("tree"), vars(&package)).unwrap();
    assert_eq!(
        Some(identity),
        relocatable_prediction_identity(&args, vars(&package))
    );
    assert_eq!(
        record.sources,
        vec![
            Portable::Literal(package.join("src/lib.rs").display().to_string()),
            Portable::OutDir(format!("{}gen.rs", std::path::MAIN_SEPARATOR)),
        ]
    );
    assert_eq!(
        relocatable_record_in(
            &args,
            &dep_info,
            Some("tree"),
            vars(&dir.path().join("w/kt"))
        ),
        None,
        "a workspace unit"
    );
    let canonical = std::fs::canonicalize(&target).unwrap();
    for spelled in [&target, &canonical] {
        std::fs::write(out.join("gen.rs"), format!("// {}\n", spelled.display())).unwrap();
        assert_eq!(
            relocatable_record_in(&args, &dep_info, Some("tree"), vars(&package)),
            None,
            "{} is spelled in OUT_DIR",
            spelled.display()
        );
    }
}

/// Through a symlink, the canonical target shares nothing with the raw
/// spelling, so the scan has to look for it on its own.
#[cfg(unix)]
#[test]
fn an_out_dir_naming_only_the_canonical_target_is_not_relocated() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let (dir, package, _, _) = relocatable_fixture();
    let real = std::fs::canonicalize(dir.path()).unwrap().join("real");
    std::fs::create_dir_all(real.join("debug/build/kt-1/out")).unwrap();
    std::fs::create_dir_all(real.join("debug/deps")).unwrap();
    let target = dir.path().join("b/target");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&real, &target).unwrap();
    let out = target.join("debug/build/kt-1/out");
    let args = unit_args_in(&package, &target);
    let vars = || {
        vec![
            ("CARGO_MANIFEST_DIR".into(), package.clone().into()),
            ("OUT_DIR".into(), out.clone().into()),
        ]
    };
    let dep_info = DepInfo {
        source_files: vec![package.join("src/lib.rs"), out.join("gen.rs")],
        env_deps: Vec::new(),
    };
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    assert!(relocatable_record_in(&args, &dep_info, Some("tree"), vars()).is_some());

    let content = format!("// {}\n", real.display());
    assert!(!mentions_root(content.as_bytes(), &[&target]));
    std::fs::write(out.join("gen.rs"), content).unwrap();
    assert_eq!(
        relocatable_record_in(&args, &dep_info, Some("tree"), vars()),
        None
    );
}

/// A workspace unit never reads a relocated row, even one filed under the
/// identity it would have: its package is not the same files in another
/// checkout.
#[test]
fn a_workspace_unit_never_reads_a_relocated_record() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let package = dir.path().join("w/kt");
    std::fs::create_dir_all(package.join("src")).unwrap();
    std::fs::write(package.join("src/lib.rs"), "include!(\"x\");\n").unwrap();
    let target = dir.path().join("b/target");
    let out = target.join("debug/build/kt-1/out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    let _manifest =
        crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
    let args = unit_args_in(&package, &target);
    let hasher = FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
    let shared = rustc_shared_prediction_identity(&args).unwrap();
    let would_be = format!(
        "{RELOCATABLE_PREDICTION_PREFIX}{}",
        shared.strip_prefix(SHARED_PREDICTION_PREFIX).unwrap()
    );
    let record = PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources: vec![
            Portable::Literal(package.join("src/lib.rs").display().to_string()),
            Portable::OutDir("/gen.rs".to_string()),
        ],
        env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
        tree: out_dir_tree_digest(&out, &hasher).unwrap(),
    };
    hasher.record_portable_prediction(&would_be, Some("kt"), &record);
    assert_eq!(
        predicted_key_inputs(&args, &hasher),
        Err(Rejection::NoRecord)
    );
}

/// After this checkout's row and the shared one miss, a registry unit
/// reads the relocated row under its OUT_DIR guard, and stashes that guard
/// before the lookup so the record it makes carries it.
#[test]
fn a_registry_unit_reads_a_relocated_record_under_its_out_dir_guard() {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let (dir, package, _, _) = relocatable_fixture();
    let target = dir.path().join("b/target");
    let out = target.join("debug/build/kt-1/out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    let _manifest =
        crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
    let args = unit_args_in(&package, &target);
    let hasher = FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
    let guard = out_dir_tree_digest(&out, &hasher).unwrap();

    take_last_tree_digest();
    assert_eq!(
        predicted_key_inputs(&args, &hasher),
        Err(Rejection::NoRecord)
    );
    assert_eq!(
        take_last_tree_digest().as_deref(),
        Some(guard.as_str()),
        "a cold build records the guard it saw before compiling"
    );

    let identity = relocatable_prediction_identity(&args, std::env::vars_os().collect()).unwrap();
    let record = PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources: vec![
            Portable::Literal(package.join("src/lib.rs").display().to_string()),
            Portable::OutDir("/gen.rs".to_string()),
        ],
        env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
        tree: guard.clone(),
    };
    hasher.record_portable_prediction(&identity, Some("kt"), &record);
    let dep_info = predicted_key_inputs(&args, &hasher).unwrap();
    assert_eq!(
        dep_info.source_files,
        vec![package.join("src/lib.rs"), out.join("gen.rs")]
    );
    assert_eq!(
        dep_info.env_deps,
        vec![("OUT_DIR".to_string(), out.display().to_string())]
    );
    assert_eq!(take_last_tree_digest(), Some(guard));

    std::fs::write(out.join("extra.rs"), "").unwrap();
    assert_eq!(
        predicted_key_inputs(&args, &hasher),
        Err(Rejection::TreeChanged),
        "a file added under OUT_DIR changes the guard"
    );
}

/// An aliased `OUT_DIR` (see `out_dir_alias`) is one path outside every
/// target directory, so the shared identity keeps it as it is. Two target
/// directories that alias the unit read one shared row, and a build with
/// its own `OUT_DIR` never reads that row.
///
/// With `macro_dep` the unit links a proc macro, so the row is read under
/// the crate tree guard, and the guard digests the alias as the unit's
/// `OUT_DIR`.
fn an_aliased_out_dir_takes_the_shared_row(macro_dep: bool) {
    let _lock = key_test_lock();
    if get_rustc_version(Path::new("rustc")).is_err() {
        return;
    }
    let (dir, package, target_a, _) = relocatable_fixture();
    let target_b = dir.path().join("b/target");
    let own_out_dir = target_b.join("debug/build/kt-1/out");
    std::fs::create_dir_all(&own_out_dir).unwrap();
    std::fs::create_dir_all(target_b.join("debug/deps")).unwrap();
    let alias = dir
        .path()
        .join("cache/out-dirs/v1/d/kt-0123456789abcdef/out");
    std::fs::create_dir_all(&alias).unwrap();
    let vars = |out_dir: &Path| {
        vec![
            ("CARGO_MANIFEST_DIR".into(), package.clone().into()),
            ("OUT_DIR".into(), out_dir.into()),
        ]
    };
    let args_in = |target: &Path| {
        let deps = target.join("debug/deps");
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "kt".to_string(),
            package.join("src/lib.rs").display().to_string(),
            "--out-dir".to_string(),
            deps.display().to_string(),
        ];
        if macro_dep {
            argv.push("--extern".to_string());
            argv.push(format!("pm={}", deps.join("libpm-1.so").display()));
        }
        RustcArgs::parse(&argv).unwrap()
    };
    let args_a = args_in(&target_a);
    let args_b = args_in(&target_b);
    assert_eq!(prediction_applies(&args_a.externs), !macro_dep);
    let shared = |args: &RustcArgs, out_dir: &Path| {
        rustc_shared_prediction_identity_in(args, vars(out_dir)).unwrap()
    };
    assert_eq!(shared(&args_a, &alias), shared(&args_b, &alias));
    assert_ne!(shared(&args_b, &alias), shared(&args_b, &own_out_dir));

    let debug_dir = alias.join("debug");
    let dep_info = DepInfo {
        source_files: vec![package.join("src/lib.rs")],
        env_deps: vec![
            ("OUT_DIR".to_string(), alias.display().to_string()),
            (
                "DEBUG_OUTPUT_DIR".to_string(),
                debug_dir.display().to_string(),
            ),
        ],
    };
    for args in [&args_a, &args_b] {
        assert!(shared_prediction_can_record_in(
            args,
            &dep_info,
            Some(&package)
        ));
    }

    let _manifest =
        crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(alias.as_os_str()));
    let _debug =
        crate::config::tests::set_env_for_test("DEBUG_OUTPUT_DIR", Some(debug_dir.as_os_str()));
    let hasher = FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
    let tree = macro_dep.then(|| crate_tree_digest(&hasher).unwrap());
    let identity = rustc_shared_prediction_identity(&args_a).unwrap();
    hasher.record_input_prediction(&identity, Some("kt"), &dep_info, tree);
    assert_eq!(predicted_key_inputs(&args_b, &hasher), Ok(dep_info));

    if macro_dep {
        std::fs::write(alias.join("stray.rs"), "").unwrap();
        assert_eq!(
            predicted_key_inputs(&args_b, &hasher),
            Err(Rejection::TreeChanged),
            "the tree guard reads the alias as OUT_DIR"
        );
        std::fs::remove_file(alias.join("stray.rs")).unwrap();
    }

    let _own = crate::config::tests::set_env_for_test("OUT_DIR", Some(own_out_dir.as_os_str()));
    assert_eq!(
        predicted_key_inputs(&args_b, &hasher),
        Err(Rejection::NoRecord),
        "a build with its own OUT_DIR has another identity"
    );
}

#[test]
fn an_aliased_out_dir_takes_the_shared_row_without_a_macro_dependency() {
    an_aliased_out_dir_takes_the_shared_row(false);
}

#[test]
fn an_aliased_out_dir_takes_the_shared_row_under_the_tree_guard() {
    an_aliased_out_dir_takes_the_shared_row(true);
}

/// What is written has to be exactly what comes back, or a prediction
/// would reproduce a different `sources` group than the pre-pass did.
#[test]
fn input_prediction_round_trips_through_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let dep_info = DepInfo {
        source_files: vec![
            PathBuf::from("/w/src/lib.rs"),
            PathBuf::from("/w/src/generated.rs"),
        ],
        env_deps: vec![("OUT_DIR".to_string(), "/target/build/out".to_string())],
    };

    let hasher = FileHasher::persistent(&db);
    assert!(hasher.supports_input_predictions());
    assert_eq!(
        hasher.input_prediction("unit"),
        None,
        "an identity never recorded has no prediction"
    );
    hasher.record_input_prediction("unit", Some("demo"), &dep_info, None);

    let record = FileHasher::persistent(&db)
        .input_prediction("unit")
        .expect("the recorded closure must survive a new process");
    assert_eq!(record.schema, PREDICTION_SCHEMA);
    assert_eq!(record.sources, dep_info.source_files);
    assert_eq!(record.env_deps, dep_info.env_deps);

    // Re-recording replaces rather than accumulates.
    let narrower = DepInfo {
        source_files: vec![PathBuf::from("/w/src/lib.rs")],
        env_deps: Vec::new(),
    };
    FileHasher::persistent(&db).record_input_prediction(
        "unit",
        Some("demo"),
        &narrower,
        Some("tree-digest".to_string()),
    );
    let record = FileHasher::persistent(&db)
        .input_prediction("unit")
        .unwrap();
    assert_eq!(record.sources, narrower.source_files);
    assert!(record.env_deps.is_empty());
    assert_eq!(record.tree.as_deref(), Some("tree-digest"));
}

/// The tree guard: a record for a proc-macro-dependent unit is only usable
/// while the crate's files are exactly what they were, wherever the tree
/// lives.
#[test]
fn crate_tree_digest_tracks_content_names_and_exclusions() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("registry/src/index-abc/pkg-1.0.0");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(root.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
    std::fs::write(root.join("Cargo.toml"), "[package]\n").unwrap();
    let hasher = FileHasher::new();
    // SAFETY: the process-state lock serialises environment edits.
    unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &root) };
    unsafe { std::env::remove_var("OUT_DIR") };
    let baseline = crate_tree_digest(&hasher).unwrap();

    std::fs::write(root.join("src/lib.rs"), "pub fn b() {}\n").unwrap();
    assert_ne!(crate_tree_digest(&hasher).unwrap(), baseline, "content");
    std::fs::write(root.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
    assert_eq!(
        crate_tree_digest(&hasher).unwrap(),
        baseline,
        "restored content"
    );

    std::fs::write(root.join("src/extra.txt"), "x").unwrap();
    assert_ne!(crate_tree_digest(&hasher).unwrap(), baseline, "a new file");
    std::fs::remove_file(root.join("src/extra.txt")).unwrap();

    std::fs::create_dir_all(root.join("target/debug")).unwrap();
    std::fs::write(root.join("target/debug/x"), "x").unwrap();
    std::fs::create_dir_all(root.join(".git")).unwrap();
    std::fs::write(root.join(".git/HEAD"), "ref").unwrap();
    assert_eq!(
        crate_tree_digest(&hasher).unwrap(),
        baseline,
        "build and git dirs"
    );

    // The same tree elsewhere digests the same: the guard follows content.
    let copy = dir.path().join("other/registry/src/index-def/pkg-1.0.0");
    std::fs::create_dir_all(copy.join("src")).unwrap();
    std::fs::write(copy.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
    std::fs::write(copy.join("Cargo.toml"), "[package]\n").unwrap();
    unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &copy) };
    assert_eq!(
        crate_tree_digest(&hasher).unwrap(),
        baseline,
        "relocated tree"
    );
    let workspace = dir.path().join("workspace/member");
    std::fs::create_dir_all(workspace.join("src")).unwrap();
    unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &workspace) };
    assert!(
        crate_tree_digest(&hasher).is_none(),
        "a workspace crate keeps the pre-pass"
    );
    unsafe { std::env::remove_var("CARGO_MANIFEST_DIR") };
    assert!(
        crate_tree_digest(&hasher).is_none(),
        "no crate directory, no guard"
    );
}

#[test]
fn emitted_closure_keeps_the_precompile_tree_guard() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let package = dir.path().join("registry/src/index/guarded-1.0.0");
    std::fs::create_dir_all(package.join("src")).unwrap();
    let source = package.join("src/lib.rs");
    std::fs::write(&source, "pub fn v() {}\n").unwrap();
    let macro_path = dir.path().join("libmacro.so");
    std::fs::write(&macro_path, "macro artifact").unwrap();
    let out = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(&out).unwrap();
    let _manifest =
        crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", None);
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "guarded".to_string(),
        "--crate-type=lib".to_string(),
        "--emit=dep-info,metadata".to_string(),
        source.display().to_string(),
        "--out-dir".to_string(),
        out.display().to_string(),
        "--extern".to_string(),
        format!("my_macro={}", macro_path.display()),
    ])
    .unwrap();
    let db = dir.path().join("index.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
        )
        .unwrap();
    let hasher = FileHasher::persistent(&db)
        .with_input_predictions(true)
        .with_prediction_flights(Some(dir.path().join("cache")));
    let original_tree = crate_tree_digest(&hasher).unwrap();
    set_defer_discovery(true);
    let deferred = compute_cache_key(&args, &hasher, &PathNormalizer::empty(), &KeyEnv::default());
    set_defer_discovery(false);
    assert!(deferred.unwrap_err().is::<DeferredDiscovery>());
    // Let a broken handoff reach the pre-pass and fail, not self-deadlock.
    drop(hasher.take_discovery_flight());

    let closure = DepInfo {
        source_files: vec![source],
        env_deps: Vec::new(),
    };
    // A macro input changed during compilation. Recording the newer tree
    // would make the old emitted closure appear valid for those new files.
    std::fs::write(package.join("macro-input.txt"), "changed").unwrap();
    provide_dep_info(closure.clone());
    compute_cache_key(&args, &hasher, &PathNormalizer::empty(), &KeyEnv::default()).unwrap();
    let tree = take_last_tree_digest();
    assert_eq!(tree.as_deref(), Some(original_tree.as_str()));
    assert!(take_last_tree_digest().is_none());
    let identity = rustc_prediction_identity(&args).unwrap();
    hasher.record_input_prediction(&identity, Some("guarded"), &closure, tree);
    assert_eq!(
        predicted_key_inputs(&args, &hasher),
        Err(Rejection::TreeChanged)
    );
    std::fs::remove_file(package.join("macro-input.txt")).unwrap();
    assert_eq!(predicted_key_inputs(&args, &hasher).unwrap(), closure);
    // An emitted closure without a prior guarded discovery must not
    // inherit the preceding invocation's tree.
    take_last_tree_digest();
    provide_dep_info(closure);
    compute_cache_key(&args, &hasher, &PathNormalizer::empty(), &KeyEnv::default()).unwrap();
    assert!(take_last_tree_digest().is_none());
}

/// A row this build cannot vouch for reads as absent. The cost of that is
/// one pre-pass; the cost of guessing at an unknown encoding is a key
/// derived from someone else's rules.
#[test]
fn a_prediction_from_another_schema_is_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let hasher = FileHasher::persistent(&db);
    let cache = hasher.cache.as_ref().unwrap();

    cache
        .put_input_prediction("future", PREDICTION_SCHEMA + 1, None, "{\"anything\":1}")
        .unwrap();
    assert_eq!(hasher.input_prediction("future"), None);

    // A row whose column says the right schema but whose bytes do not
    // decode is the same answer.
    cache
        .put_input_prediction("corrupt", PREDICTION_SCHEMA, None, "not json")
        .unwrap();
    assert_eq!(hasher.input_prediction("corrupt"), None);

    // And a row whose stored schema disagrees with its own payload.
    let stale = format!(
        "{{\"schema\":{},\"sources\":[],\"env_deps\":[]}}",
        PREDICTION_SCHEMA + 1
    );
    cache
        .put_input_prediction("mismatched", PREDICTION_SCHEMA, None, &stale)
        .unwrap();
    assert_eq!(hasher.input_prediction("mismatched"), None);
}

/// A hasher with no database has nowhere to keep a record. The daemon's
/// store-free hasher is exactly this case, and it must not panic or
/// pretend to have recorded anything.
#[test]
fn recording_without_a_database_is_a_no_op() {
    let hasher = FileHasher::new();
    assert!(!hasher.supports_input_predictions());
    hasher.record_input_prediction(
        "unit",
        None,
        &DepInfo {
            source_files: vec![PathBuf::from("/w/src/lib.rs")],
            env_deps: Vec::new(),
        },
        None,
    );
    assert_eq!(hasher.input_prediction("unit"), None);
}

#[test]
fn cargo_cfg_pairs_filters_and_sorts() {
    use std::ffi::OsString;
    let pairs = cargo_cfg_pairs(
        [
            (OsString::from("CARGO_CFG_ZED"), OsString::from("1")),
            (OsString::from("PATH"), OsString::from("/usr/bin")),
            (OsString::from("CARGO_CFG_ABI"), OsString::from("eabi")),
            (OsString::from("CARGO_PKG_NAME"), OsString::from("x")),
        ]
        .into_iter(),
    );
    assert_eq!(
        pairs,
        vec![
            (OsString::from("CARGO_CFG_ABI"), OsString::from("eabi")),
            (OsString::from("CARGO_CFG_ZED"), OsString::from("1")),
        ]
    );
}

/// A non-UTF-8 variable anywhere in the environment used to panic the
/// whole key computation via `std::env::vars()`; now the unrelated
/// variable is filtered out and a `CARGO_CFG_*` pair survives with its
/// exact bytes.
#[cfg(unix)]
#[test]
fn cargo_cfg_pairs_tolerates_non_utf8_environments() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    let invalid = OsString::from_vec(vec![b'a', 0xff, b'b']);
    let pairs = cargo_cfg_pairs(
        [
            (OsString::from_vec(vec![0xff, 0xfe]), invalid.clone()),
            (OsString::from("CARGO_CFG_RAW"), invalid.clone()),
        ]
        .into_iter(),
    );
    assert_eq!(pairs, vec![(OsString::from("CARGO_CFG_RAW"), invalid)]);
}

/// Valid UTF-8 folds byte-identically to the old `vars()`-string
/// hashing (no key change without a version bump); invalid sequences
/// fold losslessly and distinctly instead of merging under U+FFFD.
#[test]
fn env_text_key_bytes_preserves_utf8_and_distinguishes_invalid() {
    use std::ffi::OsStr;
    assert_eq!(
        env_text_key_bytes(OsStr::new("target_os=\"linux\"")),
        b"target_os=\"linux\"".to_vec()
    );
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let a = env_text_key_bytes(OsStr::from_bytes(&[b'x', 0xff]));
        let b = env_text_key_bytes(OsStr::from_bytes(&[b'x', 0xfe]));
        assert_ne!(a, b);
        // Tagged: cannot collide with any valid-UTF-8 value's bytes.
        assert_eq!(a[0], 0xff);
    }
}

/// The write persists where the read looks, creating the cache directory
/// on a fresh machine. Skipped where that directory cannot be created
/// (a sandboxed build with no writable home).
#[test]
fn tool_version_cache_write_is_read_back() {
    let cache_dir = crate::config::default_cache_dir();
    if std::fs::create_dir_all(&cache_dir).is_err() || tempfile::tempfile_in(&cache_dir).is_err() {
        eprintln!("skipping: {} is not writable", cache_dir.display());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let binary = dir.path().join("rustc-probe");
    std::fs::write(&binary, b"not really rustc").unwrap();
    let prefix = "kache-test-version";
    let cache_file = tool_version_cache_path(&binary, prefix).unwrap();
    let _ = std::fs::remove_file(&cache_file);
    assert_eq!(read_tool_version_cache(&binary, prefix), None);

    write_tool_version_cache(&binary, prefix, "rustc 1.0.0 (test)");
    assert_eq!(
        read_tool_version_cache(&binary, prefix).as_deref(),
        Some("rustc 1.0.0 (test)")
    );
    assert_eq!(
        std::fs::read_to_string(&cache_file).unwrap(),
        "rustc 1.0.0 (test)",
        "the file is exactly the version string, as before"
    );
    let _ = std::fs::remove_file(&cache_file);
}

#[cfg(unix)]
#[test]
fn clippy_identity_follows_version_config_and_lint_arguments() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let driver = dir.path().join("clippy-driver");
    kache_fs::testutil::write_executable(
        &driver,
        "#!/bin/sh\necho 'clippy 0.1.98 (abc 2026-09-01)'\n",
    );
    let workspace = dir.path().join("ws");
    let member = workspace.join("member");
    std::fs::create_dir_all(&member).unwrap();
    let env = |vars: Vec<(&'static str, String)>| {
        move |name: &str| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| std::ffi::OsString::from(v))
        }
    };
    let manifest = vec![("CARGO_MANIFEST_DIR", member.display().to_string())];

    let bare = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
    assert!(bare.starts_with("clippy 0.1.98"));
    assert!(bare.contains("config:none"));
    assert!(bare.contains("manifest:none"));

    // The package manifest feeds the `cargo` lint group.
    std::fs::write(member.join("Cargo.toml"), "[package]\nname = \"m\"\n").unwrap();
    let with_manifest = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
    assert_ne!(with_manifest, bare);
    std::fs::write(
        member.join("Cargo.toml"),
        "[package]\nname = \"m\"\ndescription = \"d\"\n",
    )
    .unwrap();
    assert_ne!(
        clippy_identity_in(&driver, env(manifest.clone()), None).unwrap(),
        with_manifest
    );
    let bare = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();

    // A configuration file above the member is found and keyed by content.
    std::fs::write(workspace.join("clippy.toml"), "msrv = \"1.80\"\n").unwrap();
    let configured = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
    assert_ne!(configured, bare);
    assert!(configured.contains("config:clippy.toml:"));
    std::fs::write(workspace.join("clippy.toml"), "msrv = \"1.85\"\n").unwrap();
    let edited = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
    assert_ne!(edited, configured);

    // `CLIPPY_CONF_DIR` wins over the manifest directory, and the lint
    // arguments Cargo passes through the environment are part of it.
    let elsewhere = dir.path().join("conf");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join(".clippy.toml"), "").unwrap();
    let mut with_conf_dir = manifest.clone();
    with_conf_dir.push(("CLIPPY_CONF_DIR", elsewhere.display().to_string()));
    let redirected = clippy_identity_in(&driver, env(with_conf_dir.clone()), None).unwrap();
    assert!(redirected.contains("config:.clippy.toml:"));
    with_conf_dir.push(("CLIPPY_ARGS", "-Dclippy::all".to_string()));
    let with_args = clippy_identity_in(&driver, env(with_conf_dir), None).unwrap();
    assert_ne!(with_args, redirected);
    assert!(with_args.contains("CLIPPY_ARGS=-Dclippy::all"));
}

#[test]
fn tool_version_cache_path_is_a_named_file_in_the_cache_dir() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let binary = dir.path().join("rustc");
    std::fs::write(&binary, b"test rustc").unwrap();

    let cache_file = tool_version_cache_path(&binary, "rustc-ver")
        .expect("a readable binary must produce a cache path");
    let cache_dir = crate::config::default_cache_dir();
    assert_eq!(cache_file.parent(), Some(cache_dir.as_path()));

    let file_name = cache_file
        .file_name()
        .expect("cache path must name a file")
        .to_string_lossy();
    let digest = file_name
        .strip_prefix("rustc-ver-")
        .and_then(|name| name.strip_suffix(".txt"))
        .expect("cache file must retain its prefix and extension");
    assert_eq!(digest.len(), 16, "cache file uses the short BLAKE3 digest");
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

#[test]
#[ignore = "spawned by the explicit RUSTUP_HOME regression"]
fn rustup_settings_path_explicit_home_fixture() {
    let expected_home = std::env::var_os("KACHE_TEST_RUSTUP_HOME")
        .map(std::path::PathBuf::from)
        .expect("fixture requires its isolated expected home");

    assert_eq!(
        rustup_settings_path(),
        Some(expected_home.join("settings.toml"))
    );
}

#[test]
fn tool_version_cache_uses_settings_under_explicit_rustup_home() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(
        std::env::current_exe().expect("resolve cache-key test executable"),
    )
    .args([
        "--ignored",
        "--exact",
        "cache_key::tests::rustup_settings_path_explicit_home_fixture",
        "--test-threads=1",
    ])
    .env("RUSTUP_HOME", dir.path())
    .env("KACHE_TEST_RUSTUP_HOME", dir.path())
    .output()
    .expect("spawn isolated RUSTUP_HOME fixture");

    assert!(
        output.status.success(),
        "isolated RUSTUP_HOME fixture failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn apply_key_env_vars_keeps_distinct_path_values_distinct() {
    let _lock = key_test_lock();
    let base = "deadbeef".to_string();
    let patterns = vec!["KACHE_TEST_ROOT".to_string()];

    // Deliberately NOT path-normalized. A declared variable is an opaque
    // semantic input — a macro may paste its value straight into the code
    // it emits — so two checkout paths that would collapse to the same
    // `<BASE_DIR>` sentinel must still key apart. This costs cross-machine
    // hits for path-valued declarations and buys back the exact miscompile
    // the feature exists to prevent.
    let alice = {
        let _guard = ScopedEnv::set("KACHE_TEST_ROOT", "/home/alice/proj/src");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    let bob = {
        let _guard = ScopedEnv::set("KACHE_TEST_ROOT", "/srv/build/bob/checkout/src");
        apply_key_env_vars(base.clone(), &patterns, "crate")
    };
    assert_ne!(alice, bob, "declared env values must be folded exactly");
}

#[cfg(unix)]
#[test]
fn apply_key_env_vars_distinguishes_non_utf8_values() {
    use std::os::unix::ffi::OsStrExt;

    let _lock = key_test_lock();
    let base = "deadbeef".to_string();
    let patterns = vec!["KACHE_TEST_RAW".to_string()];

    // Both byte strings lossy-convert to the same U+FFFD text, and a proc
    // macro reading `var_os` can still tell them apart — so folding the
    // lossy form would be a wrong-hit path.
    let key_for = |bytes: &[u8]| {
        let previous = std::env::var_os("KACHE_TEST_RAW");
        // SAFETY: single-threaded test body under `key_test_lock`.
        unsafe { std::env::set_var("KACHE_TEST_RAW", std::ffi::OsStr::from_bytes(bytes)) };
        let key = apply_key_env_vars(base.clone(), &patterns, "crate");
        // SAFETY: as above.
        unsafe {
            match previous {
                Some(value) => std::env::set_var("KACHE_TEST_RAW", value),
                None => std::env::remove_var("KACHE_TEST_RAW"),
            }
        }
        key
    };
    assert_ne!(key_for(&[0xff]), key_for(&[0xfe]));
}

#[test]
fn apply_key_salt_distinguishes_base_keys() {
    // The same salt over different base keys stays distinct (the
    // base is mixed into the hash, not just the salt).
    let salt = Some("nix-rev-abc");
    assert_ne!(
        apply_key_salt("aaaa".to_string(), salt, "crate"),
        apply_key_salt("bbbb".to_string(), salt, "crate"),
    );
}

#[test]
fn source_scanner_detects_runtime_env_use_and_skips_literals() {
    use super::SourceEnvDepUse::{IncludeLocator, RuntimeValue, Unused};
    use super::source_env_dep_use as scan;

    // A bare runtime env! use is a real dependency.
    assert_eq!(
        scan(r#"const X: &str = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let v = option_env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"const N: usize = env!("MYVAR").len();"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"const P: usize = env!("MYVAR").len() % 2;"#, "MYVAR"),
        RuntimeValue
    );
    // A different var name doesn't match, and proves nothing.
    assert_eq!(scan(r#"env!("OTHER")"#, "MYVAR"), Unused);
    assert_eq!(
        scan(r#"include!(concat!(env!("OTHER"), "/gen.rs"));"#, "MYVAR"),
        Unused
    );
    assert_eq!(scan("pub fn f() {}", "MYVAR"), Unused);

    // env! nested inside include!(concat!(...)) is a compile-time include,
    // not a runtime value, and is the positive proof normalization needs.
    assert_eq!(
        scan(r#"include!(concat!(env!("MYVAR"), "/gen.rs"));"#, "MYVAR"),
        IncludeLocator
    );
    assert_eq!(
        scan(r#"include_bytes![env!("MYVAR")];"#, "MYVAR"),
        IncludeLocator
    );
    assert_eq!(
        scan(r#"include_str! { env! { "MYVAR" } }"#, "MYVAR"),
        IncludeLocator
    );
    // The include context ends at the include's own closing delimiter.
    assert_eq!(
        scan(
            r#"include!{ concat!(env!("MYVAR"), "/gen.rs") } const X: &str = env!("MYVAR");"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"f(include!(env!("MYVAR"))); [env!("MYVAR")];"#, "MYVAR"),
        RuntimeValue
    );

    // A name the scanner cannot read counts against every var outside an
    // include, and proves nothing inside one.
    assert_eq!(
        scan(r#"const S: &str = env!(concat!("MY", "VAR"));"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"const S: &str = env!(concat!("OT", "HER"));"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(
            r#"macro_rules! e { ($v:literal) => { env!($v) } } const X: &str = e!("MYVAR");"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    assert_eq!(scan(r#"env!("MY\x56AR")"#, "MYVAR"), RuntimeValue);
    assert_eq!(scan(r##"env!(r#"MYVAR"#)"##, "MYVAR"), RuntimeValue);
    assert_eq!(scan(r#"env!("MYVAR"#, "MYVAR"), RuntimeValue);
    assert_eq!(scan("env!(", "MYVAR"), RuntimeValue);
    assert_eq!(
        scan(
            r#"include!(concat!(env!(concat!("MY", "VAR")), "/gen.rs"));"#,
            "MYVAR"
        ),
        Unused
    );

    // Whitespace and comments between the macro tokens do not hide a use.
    assert_eq!(
        scan(
            r#"env /* c */ ! // c
            ( "MYVAR" )"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    // `!` that is not a macro bang opens no macro group.
    assert_eq!(
        scan(r#"if a != (b) { include!(env!("MYVAR")); }"#, "MYVAR"),
        IncludeLocator
    );

    // Occurrences inside string / char / raw-string literals and comments
    // are not real uses — the scanner must skip them.
    assert_eq!(scan(r#"let s = "env!(\"MYVAR\")";"#, "MYVAR"), Unused);
    assert_eq!(scan(r###"let s = r#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
    assert_eq!(scan(r###"let s = br#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
    assert_eq!(scan(r###"let s = cr#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
    assert_eq!(scan(r#"// env!("MYVAR")"#, "MYVAR"), Unused);
    assert_eq!(scan(r#"/* env!("MYVAR") */"#, "MYVAR"), Unused);
    assert_eq!(scan(r#"/* /* */ env!("MYVAR") */"#, "MYVAR"), Unused);
    assert_eq!(scan(r#"/* unterminated env!("MYVAR")"#, "MYVAR"), Unused);

    // Nested block comments end at the matching `*/` only.
    assert_eq!(
        scan(
            r#"/* /* */ include!( */ const X: &str = env!("MYVAR");"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    assert_eq!(scan(r#"/*/ env!("MYVAR") */"#, "MYVAR"), Unused);

    // A char literal earlier in the line must not derail scanning of a real
    // use that follows it.
    assert_eq!(
        scan(r#"let c = '"'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = '\''; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    // An escape longer than one character, and a char literal whose
    // closing quote is the byte before another quote: both decide where
    // the literal ends, and ending it in the wrong place swallows the
    // code after it.
    assert_eq!(
        scan(r#"let c = '\u{41}'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = '\x41'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let v = ['\n','"']; env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = b'\\'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = 'é'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = '"'; let s = "env!(\"MYVAR\")";"#, "MYVAR"),
        Unused
    );
    // A lifetime or label is not a char literal: skipping to the next quote
    // would hide the use.
    assert_eq!(
        scan(
            r#"fn f(_: &'static str) -> usize { env!("MYVAR").len() }"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    assert_eq!(
        scan(
            r#"fn f<'a>(_: &'a str) { 'outer: loop { env!("MYVAR"); } }"#,
            "MYVAR"
        ),
        RuntimeValue
    );
    assert_eq!(scan("'", "MYVAR"), Unused);
    // A real use after a raw string is still found (exercises skip_raw_string).
    assert_eq!(
        scan(r###"let r = r#"noise"#; env!("MYVAR")"###, "MYVAR"),
        RuntimeValue
    );
    // A raw C string ending in a backslash has no escape to swallow the
    // closing quote.
    assert_eq!(
        scan(
            r#"let c = cr"\"; let x = env!("MYVAR"); let t = "";"#,
            "MYVAR"
        ),
        RuntimeValue
    );
}

#[test]
fn source_scanner_tokenizes_operators_numbers_and_idents_like_rustc() {
    use super::SourceEnvDepUse::{IncludeLocator, RuntimeValue, Unused};
    use super::source_env_dep_use as scan;

    // A division is not a comment.
    assert_eq!(
        scan(r#"let q = a / b; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    // Nor between macro tokens: `env / 2 */ !(..)` is no macro call.
    assert_eq!(scan(r#"let q = env / 2 */ !("MYVAR");"#, "MYVAR"), Unused);
    // An identifier that merely starts like a raw string prefix.
    assert_eq!(scan(r#"rinclude!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
    // A variable named `env` compared with `!=` opens no macro.
    assert_eq!(scan("fn f(env: u8) -> bool { env != 0 }", "MYVAR"), Unused);
    // Plain groups inside an include keep the include context open.
    assert_eq!(
        scan(r#"include!(concat!(("a"), ("b"), env!("MYVAR")))"#, "MYVAR"),
        IncludeLocator
    );
    assert_eq!(
        scan(r#"include!(m!('a'('b')), env!("MYVAR"))"#, "MYVAR"),
        IncludeLocator
    );
    // Closing an include leaves its context; nested includes count down.
    assert_eq!(
        scan(r#"include!(()) const X: &str = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"include!(include!("a"), env!("MYVAR"))"#, "MYVAR"),
        IncludeLocator
    );

    // Char literals: escaped quote, non-ASCII, and a lone quote at EOF.
    assert_eq!(
        scan(r#"let c = '\"'; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(
        scan(r#"let c = ['é','"']; let x = env!("MYVAR");"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(scan("let s = '", "MYVAR"), Unused);

    // A number's suffix cannot start a raw string.
    assert_eq!(
        scan(
            r##"m!{ 1r#"x" } const P: &str = env!("MYVAR"); // "#"##,
            "MYVAR"
        ),
        RuntimeValue
    );
    // Non-ASCII identifier bytes belong to the identifier.
    assert_eq!(scan(r#"éinclude!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
    assert_eq!(scan(r#"include!(envé!("MYVAR"))"#, "MYVAR"), Unused);
    // Rust whitespace beyond ASCII space separates tokens.
    for space in [
        "\x0B", "\x0C", "\u{85}", "\u{200E}", "\u{200F}", "\u{2028}", "\u{2029}",
    ] {
        assert_eq!(
            scan(&format!("env{space}!{space}(\"MYVAR\")"), "MYVAR"),
            RuntimeValue,
            "{space:?}"
        );
        assert_eq!(
            scan(&format!("{space}env!(\"MYVAR\")"), "MYVAR"),
            RuntimeValue,
            "leading {space:?}"
        );
    }
    // U+00A9 shares U+0085's lead byte but is not whitespace.
    assert_eq!(scan("env\u{A9}!(\"MYVAR\")", "MYVAR"), Unused);
    // A byte-order mark does not glue to the identifier after it.
    assert_eq!(scan("\u{FEFF}env!(\"MYVAR\")", "MYVAR"), RuntimeValue);

    // A file that ends inside an identifier or a number has no byte after
    // it, and a skipped token never hides the use that follows it.
    assert_eq!(scan("let n = 1", "MYVAR"), Unused);
    assert_eq!(
        scan(r#"const X: &str = env!("MYVAR"); mod tail"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(scan(r#"let s = "x"; env!("MYVAR")"#, "MYVAR"), RuntimeValue);
    assert_eq!(
        scan(r#"let s = "a\"b"; env!("MYVAR")"#, "MYVAR"),
        RuntimeValue
    );
    assert_eq!(scan("// c\nenv!(\"MYVAR\")", "MYVAR"), RuntimeValue);
    assert_eq!(scan("/* c */ env!(\"MYVAR\")", "MYVAR"), RuntimeValue);
    assert_eq!(scan("/* /* c */ */ env!(\"MYVAR\")", "MYVAR"), RuntimeValue);
    assert_eq!(
        scan(r###"let r = r##"x"##; env!("MYVAR")"###, "MYVAR"),
        RuntimeValue
    );

    // Identifiers and numbers are consumed from their first byte, so a
    // one-byte token keeps its boundary.
    assert_eq!(scan(r#"m!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
    assert_eq!(scan(r#"e!("MYVAR")"#, "MYVAR"), Unused);
    assert_eq!(scan(r#"let n = 1; env!("MYVAR");"#, "MYVAR"), RuntimeValue);
    assert_eq!(
        scan(r#"include!(1, env!("MYVAR"))"#, "MYVAR"),
        IncludeLocator
    );
}

#[test]
fn unescape_env_dep_value_undoes_rustc_escaping() {
    // rustc's `escape_dep_env`: `\`→`\\`, newline→`\n`, CR→`\r`.
    // A Windows OUT_DIR arrives doubled; unescaping restores the
    // single-backslash path so the normalizer's rules can match it.
    assert_eq!(
        unescape_env_dep_value(r"C:\\actions-runner\\proj\\target\\out"),
        r"C:\actions-runner\proj\target\out"
    );
    assert_eq!(unescape_env_dep_value(r"a\nb\rc"), "a\nb\rc");
    // Forward-slash / plain values (the Unix case) are untouched.
    assert_eq!(
        unescape_env_dep_value("/home/u/proj/out"),
        "/home/u/proj/out"
    );
    assert_eq!(unescape_env_dep_value("plain-value"), "plain-value");
}

#[test]
fn parse_env_dep_info_unescapes_windows_paths() {
    let dep = "# env-dep:OUT_DIR=C:\\\\proj\\\\build\\\\out\n";
    let deps = parse_env_dep_info(dep);
    assert_eq!(
        deps,
        vec![("OUT_DIR".to_string(), r"C:\proj\build\out".to_string())]
    );
}

#[test]
fn test_cache_key_deterministic() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition=2021".to_string(),
        "-C".to_string(),
        "opt-level=2".to_string(),
    ];

    let parsed1 = RustcArgs::parse(&args_vec).unwrap();
    let parsed2 = RustcArgs::parse(&args_vec).unwrap();

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();
    assert_eq!(key1, key2);
}

/// Regression for Finding B from the Firefox bench: mozbuild sets
/// `-Clinker=/abs/path/to/clang++`, and v6 baked that path into the
/// key — every clone hashed differently. The linker's semantic
/// identity is still captured via `linker:<--version>` so the key
/// stays sensitive to a different toolchain.
#[test]
fn cache_key_ignores_linker_path() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mk = |linker: &str| -> Vec<String> {
        vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "-C".to_string(),
            format!("linker={linker}"),
        ]
    };

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let a = compute_cache_key(
        &RustcArgs::parse(&mk("/Users/alice/clang++")).unwrap(),
        &fh,
        &pn,
        &KeyEnv::default(),
    )
    .unwrap();
    let b = compute_cache_key(
        &RustcArgs::parse(&mk("/home/runner/clang++")).unwrap(),
        &fh,
        &pn,
        &KeyEnv::default(),
    )
    .unwrap();
    assert_eq!(a, b, "linker path must not affect the cache key");
}

/// Shared base argv for the flag-keying regression tests below.
fn flag_base(source: &Path, extra: &[&str]) -> Vec<String> {
    let mut v = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "cdylib".to_string(),
    ];
    v.extend(extra.iter().map(|s| s.to_string()));
    v
}

fn key_of(args: &[String]) -> String {
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    compute_cache_key(
        &RustcArgs::parse(args).unwrap(),
        &fh,
        &pn,
        &KeyEnv::default(),
    )
    .unwrap()
}

/// Compute a key for tests that check whether a *flag* (sysroot, custom
/// target spec, `-Z` codegen flag, cross `--target`, ...) affects the key,
/// independent of source discovery. Such flags can make the real
/// `--emit=dep-info` pre-pass fail (a bogus `--sysroot` where rustc can't
/// find `std`, a `-Z` flag on a stable toolchain, an uninstalled
/// cross-target's missing `std`); since kunobi-ninja/kache#323 a failing
/// pre-pass is a hard error (the invocation becomes non-cacheable), so this
/// clears `source_file` to exercise flag hashing directly.
fn key_of_flags(args: &[String]) -> String {
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let mut parsed = RustcArgs::parse(args).unwrap();
    parsed.source_file = None;
    compute_cache_key(&parsed, &fh, &pn, &KeyEnv::default()).unwrap()
}

/// H1: build-script `-l` link libs reach rustc on argv (not via
/// RUSTFLAGS); a different native lib must diverge the key.
/// Generic `-l` keying, checked on hosts that do not probe native MSVC
/// inputs. On a Windows host the library must exist (fail closed), so the
/// Windows shape lives in the `windows_*` tests with an injected probe.
#[cfg(not(windows))]
#[test]
fn link_lib_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let none = key_of(&flag_base(&source, &[]));
    let ssl = key_of(&flag_base(&source, &["-l", "ssl"]));
    let crypto = key_of(&flag_base(&source, &["-l", "crypto"]));
    assert_ne!(none, ssl, "adding -l must change the key");
    assert_ne!(ssl, crypto, "a different -l lib must change the key");
    // Attached form parses identically to the separate form.
    assert_eq!(ssl, key_of(&flag_base(&source, &["-lssl"])));
}

#[test]
fn static_lib_spec_models_kind_modifiers_and_rename() {
    let archive = |files: &[&str], bundle, whole_archive| StaticLibSpec::Archive {
        files: files.iter().map(|file| file.to_string()).collect(),
        bundle,
        whole_archive,
    };
    let plain = || archive(&["libfoo.a", "foo.lib"], true, false);
    let whole = || archive(&["libfoo.a", "foo.lib"], true, true);
    assert_eq!(static_lib_spec("static=foo"), plain());
    // Modifiers that change how the archive links, not which file it is.
    assert_eq!(static_lib_spec("static:+whole-archive=foo"), whole());
    // The last `±whole-archive` wins.
    assert_eq!(
        static_lib_spec("static:+whole-archive,-whole-archive=foo"),
        plain()
    );
    assert_eq!(
        static_lib_spec("static:-bundle=foo"),
        archive(&["libfoo.a", "foo.lib"], false, false)
    );
    assert_eq!(static_lib_spec("static:+bundle,-as-needed=foo"), plain());
    // The last `±bundle` wins.
    assert_eq!(static_lib_spec("static:-bundle,+bundle=foo"), plain());
    // `+verbatim` names the file exactly; a later `-verbatim` undoes it.
    assert_eq!(
        static_lib_spec("static:+whole-archive,+verbatim=foo.a"),
        archive(&["foo.a"], true, true)
    );
    assert_eq!(static_lib_spec("static:+verbatim,-verbatim=foo"), plain());
    // Rename, unknown modifiers and an empty name are not modelled.
    assert_eq!(
        static_lib_spec("static=foo:bar"),
        StaticLibSpec::Unmodeled("static=foo:bar")
    );
    assert_eq!(
        static_lib_spec("static:+link-arg=foo"),
        StaticLibSpec::Unmodeled("static:+link-arg=foo")
    );
    assert_eq!(
        static_lib_spec("static="),
        StaticLibSpec::Unmodeled("static=")
    );
    // Other kinds are referenced, not bundled.
    assert_eq!(static_lib_spec("dylib=foo"), StaticLibSpec::NotStatic);
    assert_eq!(
        static_lib_spec("dylib:+verbatim=foo"),
        StaticLibSpec::NotStatic
    );
    assert_eq!(static_lib_spec("foo"), StaticLibSpec::NotStatic);
    // A kindless or `dylib` rename can retarget an attribute's static
    // library, so it is refused like a static one. Frameworks cannot.
    assert_eq!(
        static_lib_spec("foo:bar"),
        StaticLibSpec::Unmodeled("foo:bar")
    );
    assert_eq!(
        static_lib_spec("dylib=foo:bar"),
        StaticLibSpec::Unmodeled("dylib=foo:bar")
    );
    assert_eq!(
        static_lib_spec("framework=Foo:Bar"),
        StaticLibSpec::NotStatic
    );
}

#[test]
fn resolve_native_static_lib_hashes_only_unambiguous_static_archives() {
    let fh = FileHasher::new();
    let dir = tempfile::tempdir().unwrap();
    let lib = dir.path().join("libfoo.a");
    std::fs::write(&lib, b"v1 archive bytes").unwrap();
    let dirs = vec![dir.path().to_path_buf()];

    // A `static=` lib present in a search dir resolves and content-hashes.
    let (path, h1) =
        resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .unwrap()
            .expect("static lib in a search dir must resolve");
    assert_eq!(path, lib);

    // `cc` emits the same OUT_DIR once per compiled archive. Repeating an
    // identical `-L native=...` must still resolve the one physical file.
    let duplicate_dirs = vec![dir.path().to_path_buf(), dir.path().to_path_buf()];
    let (duplicate_path, _) =
        resolve_native_static_lib("static=foo", &duplicate_dirs, &fh, false, |_| {
            StaticLibUse::Bundled
        })
        .unwrap()
        .expect("duplicate search dirs must not make one archive ambiguous");
    assert_eq!(duplicate_path, lib);

    // Changed bytes → different hash (this is the false hit we close).
    std::fs::write(&lib, b"v2 different bytes").unwrap();
    let (_, h2) =
        resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .unwrap()
            .unwrap();
    assert_ne!(h1, h2, "content change must change the resolved hash");

    // `dylib=`/bare are referenced not bundled → never content-hashed, and
    // a missing lib does not resolve.
    assert!(
        resolve_native_static_lib("dylib=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .unwrap()
            .is_none()
    );
    assert!(
        resolve_native_static_lib("foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .unwrap()
            .is_none()
    );
    assert!(
        resolve_native_static_lib("static=absent", &dirs, &fh, false, |_| {
            StaticLibUse::Bundled
        })
        .unwrap()
        .is_none()
    );

    // Distinct matches remain uncacheable, so we never hash a file other
    // than the one rustc actually picked.
    let other_dir = tempfile::tempdir().unwrap();
    std::fs::write(other_dir.path().join("libfoo.a"), b"different archive").unwrap();
    assert!(
        resolve_native_static_lib(
            "static=foo",
            &[dir.path().to_path_buf(), other_dir.path().to_path_buf()],
            &fh,
            false,
            |_| StaticLibUse::Bundled,
        )
        .is_err(),
        "distinct search-dir matches must fail closed"
    );

    // Both platform filename conventions in one directory are also
    // ambiguous.
    std::fs::write(dir.path().join("foo.lib"), b"msvc import lib").unwrap();
    assert!(
        resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .is_err(),
        "ambiguous .a/.lib match must fail closed"
    );
}

#[test]
fn resolve_native_static_lib_hashes_archives_named_with_modifiers() {
    let fh = FileHasher::new();
    let dir = tempfile::tempdir().unwrap();
    let dirs = vec![dir.path().to_path_buf()];
    let lib = dir.path().join("libfoo.a");
    std::fs::write(&lib, b"v1 archive bytes").unwrap();

    // `cargo:rustc-link-lib=static:+whole-archive=foo` bundles libfoo.a
    // just like `static=foo`, so a rebuilt archive must change the hash.
    let hash_of = |spec| {
        resolve_native_static_lib(spec, &dirs, &fh, false, |_| StaticLibUse::Bundled)
            .unwrap()
            .expect("modifier spec must resolve its archive")
    };
    let (path, h1) = hash_of("static:+whole-archive=foo");
    assert_eq!(path, lib);
    std::fs::write(&lib, b"v2 different bytes").unwrap();
    let (_, h2) = hash_of("static:+whole-archive=foo");
    assert_ne!(h1, h2, "rebuilt whole-archive lib must change the hash");

    // `+verbatim` resolves the exact file name and nothing else.
    std::fs::write(dir.path().join("foo.a"), b"verbatim bytes").unwrap();
    let (verbatim, _) = hash_of("static:+verbatim=foo.a");
    assert_eq!(verbatim, dir.path().join("foo.a"));

    // A rename is refused rather than keyed by its name.
    assert!(
        resolve_native_static_lib("static=foo:bar", &dirs, &fh, false, |_| {
            StaticLibUse::Bundled
        })
        .is_err()
    );
}

#[test]
fn native_linker_side_files_fail_closed() {
    let parsed = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-C".to_string(),
        "link-arg=-Wl,-order_file,/tmp/order.txt".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(&parsed));

    let response_file = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Clink-arg=@/tmp/ld.rsp".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(&response_file));

    for apple_dynamic_path in [
        "link-arg=-Wl,-rpath,@loader_path",
        "link-arg=-Wl,-rpath,@loader_path/../lib",
        "link-arg=-Wl,-rpath,@rpath",
        "link-arg=-Wl,-install_name,@executable_path",
        "link-arg=-Wl,-install_name,@executable_path/lib/libfoo.dylib",
    ] {
        let parsed = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-C".to_string(),
            apple_dynamic_path.to_string(),
            "--target=aarch64-apple-darwin".to_string(),
        ])
        .unwrap();
        assert!(
            !native_linker_side_files_are_unmodeled(&parsed),
            "Apple dynamic path is not a response file: {apple_dynamic_path}"
        );
    }

    let forwarded_response_file = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-C".to_string(),
        "link-arg=-Wl,@/tmp/ld.rsp".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(
        &forwarded_response_file
    ));

    for long_codegen_response_file in [
        "--codegen=link-arg=@/tmp/ld.rsp",
        "--codegen=link-args=-Wl,@/tmp/ld.rsp",
    ] {
        let parsed = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            long_codegen_response_file.to_string(),
        ])
        .unwrap();
        assert!(
            native_linker_side_files_are_unmodeled(&parsed),
            "long codegen response file must fail closed: {long_codegen_response_file}"
        );
    }

    let non_apple_at_rpath = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Clink-arg=-Wl,@rpath".to_string(),
        "--target=x86_64-unknown-linux-gnu".to_string(),
    ])
    .unwrap();
    assert!(
        native_linker_side_files_are_unmodeled(&non_apple_at_rpath),
        "Apple dynamic-token exemptions must not hide non-Apple response files"
    );

    let custom_apple_named_target = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Clink-arg=-Wl,@rpath".to_string(),
        "--target=/tmp/aarch64-apple-darwin.json".to_string(),
    ])
    .unwrap();
    assert!(
        native_linker_side_files_are_unmodeled(&custom_apple_named_target),
        "an Apple-looking custom target is not proof of Apple linker semantics"
    );

    let rust_target_path_name = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Clink-arg=-Wl,@rpath".to_string(),
        "--target=aarch64-apple-custom".to_string(),
    ])
    .unwrap();
    assert!(
        native_linker_side_files_are_unmodeled(&rust_target_path_name),
        "a RUST_TARGET_PATH name is not proof of built-in Apple semantics"
    );

    let map_file = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Clink-arg=-Wl,-map,/tmp/link.map".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(&map_file));

    let lld_map_file = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "--codegen=link-arg=-Wl,--Map=/tmp/link.map".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(&lld_map_file));

    let coff_map_file = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        r"--codegen=link-arg=/MAP:C:\tmp\link.map".to_string(),
    ])
    .unwrap();
    assert!(native_linker_side_files_are_unmodeled(&coff_map_file));

    for ordering_file in [
        "--codegen=link-arg=-Wl,--symbol-ordering-file=/tmp/order.txt",
        r"--codegen=link-arg=/call-graph-ordering-file:C:\tmp\order.txt",
        r"--codegen=link-arg=/ORDER:@C:\tmp\order.txt",
    ] {
        let parsed = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            ordering_file.to_string(),
        ])
        .unwrap();
        assert!(
            native_linker_side_files_are_unmodeled(&parsed),
            "linker ordering files must fail closed: {ordering_file}"
        );
    }

    let ordinary = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/lib.rs".to_string(),
        "-Copt-level=2".to_string(),
    ])
    .unwrap();
    assert!(!native_linker_side_files_are_unmodeled(&ordinary));

    let fh = FileHasher::new();
    let dir = tempfile::tempdir().unwrap();
    let lib = dir.path().join("libfoo.a");
    std::fs::write(&lib, gnu_ar_one_object(b"object")).unwrap();
    // Exact member identity is retained even without a side-file option,
    // because this archive may be bundled into an rlib and linked later.
    std::fs::write(&lib, gnu_ar_named_object("foo.o", b"same object")).unwrap();
    let named_foo = fh.hash_static_lib(&lib).unwrap();
    std::fs::write(&lib, gnu_ar_named_object("bar.o", b"same object")).unwrap();
    let named_bar = fh.hash_static_lib(&lib).unwrap();
    assert_ne!(named_foo, named_bar);

    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn probe() {}").unwrap();
    let guarded = RustcArgs::parse(&flag_base(
        &source,
        &[
            "-l",
            "static=foo",
            "-C",
            "link-arg=-Wl,-order_file,/tmp/order.txt",
        ],
    ))
    .unwrap();
    let error =
        compute_cache_key(&guarded, &fh, &PathNormalizer::empty(), &KeyEnv::default()).unwrap_err();
    assert!(error.to_string().contains("side files are not cacheable"));
}

fn elf64le_relocatable(payload: &[u8]) -> Vec<u8> {
    let mut object = vec![0_u8; 64];
    object[..4].copy_from_slice(b"\x7fELF");
    object[4] = 2; // ELFCLASS64
    object[5] = 1; // ELFDATA2LSB
    object[6] = 1; // EV_CURRENT
    object[16..18].copy_from_slice(&1_u16.to_le_bytes()); // ET_REL
    object[18..20].copy_from_slice(&62_u16.to_le_bytes()); // EM_X86_64
    object[20..24].copy_from_slice(&1_u32.to_le_bytes());
    object[52..54].copy_from_slice(&64_u16.to_le_bytes());
    object[58..60].copy_from_slice(&64_u16.to_le_bytes());
    object[60..62].copy_from_slice(&3_u16.to_le_bytes());
    object[62..64].copy_from_slice(&2_u16.to_le_bytes());

    let payload_offset = object.len();
    object.extend_from_slice(payload);
    let names_offset = object.len();
    let names = b"\0.data\0.shstrtab\0";
    object.extend_from_slice(names);
    while !object.len().is_multiple_of(8) {
        object.push(0);
    }
    let section_offset = object.len();
    object.resize(section_offset + 3 * 64, 0);
    object[40..48].copy_from_slice(&(section_offset as u64).to_le_bytes());

    let payload_header = section_offset + 64;
    object[payload_header..payload_header + 4].copy_from_slice(&1_u32.to_le_bytes());
    object[payload_header + 4..payload_header + 8].copy_from_slice(&1_u32.to_le_bytes());
    object[payload_header + 24..payload_header + 32]
        .copy_from_slice(&(payload_offset as u64).to_le_bytes());
    object[payload_header + 32..payload_header + 40]
        .copy_from_slice(&(payload.len() as u64).to_le_bytes());
    object[payload_header + 48..payload_header + 56].copy_from_slice(&1_u64.to_le_bytes());

    let names_header = section_offset + 2 * 64;
    object[names_header..names_header + 4].copy_from_slice(&7_u32.to_le_bytes());
    object[names_header + 4..names_header + 8].copy_from_slice(&3_u32.to_le_bytes());
    object[names_header + 24..names_header + 32]
        .copy_from_slice(&(names_offset as u64).to_le_bytes());
    object[names_header + 32..names_header + 40]
        .copy_from_slice(&(names.len() as u64).to_le_bytes());
    object[names_header + 48..names_header + 56].copy_from_slice(&1_u64.to_le_bytes());
    object
}

/// A minimal single-object GNU `ar` archive (no symtab / long-name table).
fn gnu_ar_raw_named_object(name: &str, object: &[u8]) -> Vec<u8> {
    assert!(!name.is_empty() && name.len() <= 15 && !name.contains('/'));
    let mut a = b"!<arch>\n".to_vec();
    let member_name = format!("{name}/");
    let mut h = format!("{member_name:<16}").into_bytes();
    h.extend_from_slice(format!("{:<12}", 0).as_bytes()); // mtime
    h.extend_from_slice(format!("{:<6}", 0).as_bytes()); // uid
    h.extend_from_slice(format!("{:<6}", 0).as_bytes()); // gid
    h.extend_from_slice(format!("{:<8}", "100644").as_bytes()); // mode
    h.extend_from_slice(format!("{:<10}", object.len()).as_bytes()); // size
    h.extend_from_slice(b"`\n");
    assert_eq!(h.len(), 60);
    a.extend_from_slice(&h);
    a.extend_from_slice(object);
    if object.len() % 2 == 1 {
        a.push(b'\n');
    }
    a
}

fn gnu_ar_one_object(payload: &[u8]) -> Vec<u8> {
    gnu_ar_named_object("object.o", payload)
}

fn gnu_ar_named_object(name: &str, payload: &[u8]) -> Vec<u8> {
    gnu_ar_raw_named_object(name, &elf64le_relocatable(payload))
}

#[test]
fn hash_static_lib_caches_and_is_namespace_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::persistent(&dir.path().join("index.db"));
    let lib = dir.path().join("libbig.a");
    // >MIN_PERSISTED_HASH_BYTES so it takes the cached path.
    std::fs::write(&lib, gnu_ar_one_object(&vec![0x41u8; 70_000])).unwrap();

    let portable = fh.hash_static_lib(&lib).unwrap();
    assert!(
        portable.starts_with("gnu-ar-v2:"),
        "a GNU archive gets the structural member digest"
    );
    // Second call returns the SAME value (cache round-trip, not corruption).
    assert_eq!(fh.hash_static_lib(&lib).unwrap(), portable);

    // Namespace isolation: a whole-file hash of the SAME path is a plain-hex
    // digest under a different cache row — it must not be the static-lib value.
    let whole = fh.hash(&lib).unwrap();
    assert!(!whole.starts_with("gnu-ar-v2:"));
    assert_ne!(whole, portable);
    // And the static-lib digest is still the portable one after `hash` ran.
    assert_eq!(fh.hash_static_lib(&lib).unwrap(), portable);
}

#[test]
fn hash_static_lib_gnu_longname_archive_is_checkout_independent() {
    // Every `cc` archive on Linux has a `//` table whose header GNU `ar`
    // leaves blank apart from name and size. Two checkouts that build the
    // same members must get the same structural digest, not a path-bound one.
    let bytes = crate::native_archive::gnu_crs_longname_archive_for_tests(b"payload");
    let fh = FileHasher::new();
    let mut digests = Vec::new();
    for checkout in ["checkout-a", "checkout-b"] {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join(checkout).join("libprobe.a");
        std::fs::create_dir_all(lib.parent().unwrap()).unwrap();
        std::fs::write(&lib, &bytes).unwrap();
        for usage in [StaticLibUse::Bundled, StaticLibUse::Linked] {
            let digest = fh.hash_static_lib_for(&lib, usage).unwrap();
            assert!(digest.starts_with("gnu-ar-v2:"), "{usage:?}: {digest}");
            digests.push(digest);
        }
    }
    assert!(digests.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn hash_static_lib_ignores_legacy_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::persistent(&dir.path().join("index.db"));
    let lib = dir.path().join("libbig.a");
    std::fs::write(&lib, gnu_ar_one_object(&vec![0x41_u8; 70_000])).unwrap();

    let fingerprint = FileFingerprint::from_path(&lib).unwrap();
    let legacy_key = FileFingerprint {
        path: format!("static-ar-v1\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let legacy_v2_key = FileFingerprint {
        path: format!("static-ar-v2\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let legacy_v3_key = FileFingerprint {
        path: format!("static-ar-v3\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let legacy_v4_key = FileFingerprint {
        path: format!("static-ar-v4\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let legacy_v5_key = FileFingerprint {
        path: format!("static-ar-v5\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let legacy_v6_key = FileFingerprint {
        path: format!("static-ar-v6\0{}", fingerprint.path),
        ..fingerprint.clone()
    };
    let current_key = FileFingerprint {
        path: format!("static-ar-v7\0{}", fingerprint.path),
        ..fingerprint
    };
    let cache = fh.cache.as_ref().expect("persistent cache opens");
    cache
        .put(&legacy_key, "legacy-whole-file-sentinel")
        .unwrap();
    cache.put(&legacy_v2_key, "legacy-member-sentinel").unwrap();
    cache
        .put(&legacy_v3_key, "legacy-unguarded-object-sentinel")
        .unwrap();
    cache
        .put(&legacy_v4_key, "legacy-unguarded-macho-sentinel")
        .unwrap();
    cache
        .put(&legacy_v5_key, "legacy-path-bound-dwarf-sentinel")
        .unwrap();
    cache
        .put(&legacy_v6_key, "legacy-path-bound-blank-longname-sentinel")
        .unwrap();

    let computed = fh.hash_static_lib(&lib).unwrap();
    assert!(computed.starts_with("gnu-ar-v2:"));
    assert_ne!(computed, "legacy-whole-file-sentinel");
    assert_ne!(computed, "legacy-member-sentinel");
    assert_ne!(computed, "legacy-unguarded-object-sentinel");
    assert_ne!(computed, "legacy-unguarded-macho-sentinel");
    assert_ne!(computed, "legacy-path-bound-dwarf-sentinel");
    assert_ne!(computed, "legacy-path-bound-blank-longname-sentinel");
    fh.flush_memo_as_if_settled();
    assert_eq!(cache.get(&current_key).unwrap(), Some(computed.clone()));
    assert_eq!(fh.hash_static_lib(&lib).unwrap(), computed);
}

#[test]
fn hash_static_lib_v3_memo_cannot_bypass_object_gate() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::persistent(&dir.path().join("index.db"));
    let lib = dir.path().join("libbitcode.a");
    let mut bitcode = vec![0_u8; 70_000];
    bitcode[..4].copy_from_slice(b"BC\xc0\xde");
    std::fs::write(&lib, gnu_ar_raw_named_object("bitcode.o", &bitcode)).unwrap();

    let fingerprint = FileFingerprint::from_path(&lib).unwrap();
    let stale_key = FileFingerprint {
        path: format!("static-ar-v3\0{}", fingerprint.path),
        ..fingerprint
    };
    let cache = fh.cache.as_ref().expect("persistent cache opens");
    cache.put(&stale_key, "gnu-ar-v2:unguarded").unwrap();

    let computed = fh.hash_static_lib(&lib).unwrap();
    assert!(computed.starts_with("path-ar-v1:"));
    assert_ne!(computed, "gnu-ar-v2:unguarded");
}

#[test]
fn static_lib_fallback_binds_lexical_archive_path() {
    let dir = tempfile::tempdir().unwrap();
    let first_dir = dir.path().join("PerfUtils");
    let second_dir = dir.path().join("OtherName");
    std::fs::create_dir_all(&first_dir).unwrap();
    std::fs::create_dir_all(&second_dir).unwrap();
    let first = first_dir.join("libsame.a");
    let second = second_dir.join("libsame.a");
    std::fs::write(&first, b"unsupported but identical archive bytes").unwrap();
    std::fs::write(&second, b"unsupported but identical archive bytes").unwrap();

    let fh = FileHasher::new();
    let first_hash = fh.hash_static_lib(&first).unwrap();
    let second_hash = fh.hash_static_lib(&second).unwrap();
    assert!(first_hash.starts_with("path-ar-v1:"));
    assert!(second_hash.starts_with("path-ar-v1:"));
    assert_ne!(first_hash, second_hash);
}

/// A link through Cargo's `OUT_DIR` symlink into a sealed hermetic
/// directory keys the archive by content; anything else stays resolved.
#[cfg(unix)]
#[test]
fn an_archive_linked_through_a_sealed_out_dir_counts_as_under_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("w/target/debug");
    let sealed = dir.path().join("cache/out-dirs/v2").join("ef".repeat(16));
    let shared = sealed.join("debug/build/z-1/out");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::write(shared.join("libz.a"), b"!<arch>\n").unwrap();
    std::fs::create_dir_all(profile.join("build/z-1")).unwrap();
    let out = profile.join("build/z-1/out");
    std::os::unix::fs::symlink(&shared, &out).unwrap();
    let archive = out.join("libz.a");
    let resolved = resolved_path(&archive);

    assert!(
        !hermetic_archive_under(&archive, &resolved, &profile),
        "not sealed"
    );
    std::fs::write(sealed.join(".kache-sealed"), b"{}").unwrap();
    assert!(hermetic_archive_under(&archive, &resolved, &profile));
    assert!(
        !hermetic_archive_under(&archive, &resolved, &dir.path().join("elsewhere")),
        "spelled outside the root"
    );
    let plain = profile.join("deps/libq.a");
    std::fs::create_dir_all(plain.parent().unwrap()).unwrap();
    std::fs::write(&plain, b"!<arch>\n").unwrap();
    assert!(
        !hermetic_archive_under(&plain, &resolved_path(&plain), &resolved_path(&profile)),
        "an archive that resolves under the root needs no exception"
    );

    let bin = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/main.rs".to_string(),
        "--crate-type".to_string(),
        "bin".to_string(),
    ])
    .unwrap();
    let root = resolved_path(&profile);
    assert_eq!(
        oso_archive_use(&bin, &archive, &profile, &root),
        StaticLibUse::Bundled
    );
    assert_eq!(
        oso_archive_use(&bin, &resolved, &profile, &root),
        StaticLibUse::Linked,
        "named by the sealed path itself, the link keeps that path"
    );
    assert_eq!(
        oso_archive_use(&bin, &plain, &profile, &root),
        StaticLibUse::Bundled
    );
    assert_eq!(
        oso_archive_use(&bin, Path::new("/opt/lib/libz.a"), &profile, &root),
        StaticLibUse::Linked
    );
}

/// A unit that does not link bundles the archive. A link reads it by path
/// unless the injected `-oso_prefix` root covers it.
#[test]
fn linked_archive_use_follows_output_and_oso_root() {
    let root = Path::new("/w/target/debug");
    let under = Path::new("/w/target/debug/build/s-1/out/libfoo.a");
    let outside = Path::new("/opt/lib/libfoo.a");
    let use_of = |crate_type: &str, test: bool, archive: &Path, oso_root: Option<&Path>| {
        let mut argv = vec![
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "--crate-type".to_string(),
            crate_type.to_string(),
        ];
        if test {
            argv.push("--test".to_string());
        }
        linked_archive_use(&RustcArgs::parse(&argv).unwrap(), archive, oso_root)
    };
    for crate_type in ["lib", "rlib", "staticlib"] {
        assert_eq!(
            use_of(crate_type, false, outside, None),
            StaticLibUse::Bundled,
            "{crate_type}"
        );
    }
    for crate_type in ["bin", "cdylib", "proc-macro"] {
        assert_eq!(
            use_of(crate_type, false, outside, None),
            StaticLibUse::Linked,
            "{crate_type}"
        );
    }
    assert_eq!(use_of("lib", true, outside, None), StaticLibUse::Linked);
    assert_eq!(
        use_of("bin", false, under, Some(root)),
        StaticLibUse::Bundled,
        "the injected prefix strips an archive under its root"
    );
    assert_eq!(
        use_of("bin", false, outside, Some(root)),
        StaticLibUse::Linked
    );
    assert_eq!(use_of("bin", false, under, None), StaticLibUse::Linked);
}

/// A DWARF-bearing Mach-O archive shares its structural digest across
/// checkouts only when rustc bundles it into an rlib. An invocation that
/// links it itself writes the archive's absolute path into `N_OSO`, so
/// the digest stays path-bound there.
#[test]
fn linked_dwarf_archive_stays_path_bound() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::new();
    let hash_in = |name: &str, bytes: &[u8], usage: StaticLibUse| {
        let lib_dir = dir.path().join(name);
        std::fs::create_dir_all(&lib_dir).unwrap();
        let lib = lib_dir.join("libprobe.a");
        std::fs::write(&lib, bytes).unwrap();
        fh.hash_static_lib_for(&lib, usage).unwrap()
    };

    let dwarf = crate::native_archive::dwarf_bsd_archive_for_tests(b"debug");
    let bundled = hash_in("a", &dwarf, StaticLibUse::Bundled);
    assert!(bundled.starts_with("bsd-ar-v2:"));
    assert_eq!(bundled, hash_in("b", &dwarf, StaticLibUse::Bundled));

    let linked = hash_in("a", &dwarf, StaticLibUse::Linked);
    assert!(linked.starts_with("path-ar-v1:"));
    assert_ne!(linked, hash_in("b", &dwarf, StaticLibUse::Linked));
    // The unqualified method is the linked reading.
    assert_eq!(
        fh.hash_static_lib(&dir.path().join("a/libprobe.a"))
            .unwrap(),
        linked
    );

    // Without DWARF, a linked archive keeps the structural digest.
    let plain = gnu_ar_one_object(b"object");
    let linked_plain = hash_in("c", &plain, StaticLibUse::Linked);
    assert!(linked_plain.starts_with("gnu-ar-v2:"));
    assert_eq!(linked_plain, hash_in("d", &plain, StaticLibUse::Linked));
}

#[test]
fn hash_static_lib_memo_rows_are_per_use() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::persistent(&dir.path().join("index.db"));
    let lib = dir.path().join("libbig.a");
    // Large enough for the persistent memo.
    std::fs::write(
        &lib,
        crate::native_archive::dwarf_bsd_archive_for_tests(&vec![0x41_u8; 70_000]),
    )
    .unwrap();

    let bundled = fh.hash_static_lib_for(&lib, StaticLibUse::Bundled).unwrap();
    let linked = fh.hash_static_lib_for(&lib, StaticLibUse::Linked).unwrap();
    assert!(bundled.starts_with("bsd-ar-v2:"));
    assert!(linked.starts_with("path-ar-v1:"));
    assert_eq!(
        fh.hash_static_lib_for(&lib, StaticLibUse::Bundled).unwrap(),
        bundled
    );

    fh.flush_memo_as_if_settled();
    let fingerprint = FileFingerprint::from_path(&lib).unwrap();
    let cache = fh.cache.as_ref().expect("persistent cache opens");
    for (namespace, expected) in [
        ("static-ar-v7-bundled", &bundled),
        ("static-ar-v7", &linked),
    ] {
        let key = FileFingerprint {
            path: format!("{namespace}\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        assert_eq!(cache.get(&key).unwrap().as_ref(), Some(expected));
    }
}

/// Archives under `MIN_PERSISTED_HASH_BYTES` are hashed without a memo
/// row; one of exactly that size gets a row.
#[test]
fn hash_static_lib_memo_threshold_is_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let fh = FileHasher::persistent(&dir.path().join("index.db"));
    let cache = fh.cache.as_ref().expect("persistent cache opens");
    let threshold = usize::try_from(MIN_PERSISTED_HASH_BYTES).unwrap();
    for (name, len, memoized) in [
        ("libsmall.a", threshold - 1, false),
        ("libexact.a", threshold, true),
    ] {
        let lib = dir.path().join(name);
        std::fs::write(&lib, vec![b'x'; len]).unwrap();
        let hash = fh.hash_static_lib(&lib).unwrap();
        fh.flush_memo_as_if_settled();
        let fingerprint = FileFingerprint::from_path(&lib).unwrap();
        let key = FileFingerprint {
            path: format!("static-ar-v7\0{}", fingerprint.path),
            ..fingerprint
        };
        let expected = memoized.then_some(hash);
        assert_eq!(cache.get(&key).unwrap(), expected, "{name}");
    }
}

#[test]
fn thin_static_archive_is_uncacheable() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("libthin.a");
    std::fs::write(&archive, b"!<thin>\n").unwrap();

    let error = FileHasher::new().hash_static_lib(&archive).unwrap_err();
    assert!(error.to_string().contains("external members"));
}

/// The cardinal #421 false hit: a `static=` native lib whose content changes
/// in place (same `-l` name, same normalized `-L` path) must change the key.
#[test]
fn native_static_lib_content_change_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    let lib = libdir.join("libfoo.a");
    std::fs::write(&lib, b"v1 archive bytes").unwrap();
    let search = format!("native={}", libdir.display());
    let flags = ["-L", search.as_str(), "-l", "static=foo"];

    let k1 = key_of(&flag_base(&source, &flags));
    std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
    let k2 = key_of(&flag_base(&source, &flags));
    assert_ne!(
        k1, k2,
        "a native static lib content change must change the key (#421)"
    );
    // Content-addressed: the original bytes reproduce the original key.
    std::fs::write(&lib, b"v1 archive bytes").unwrap();
    let k3 = key_of(&flag_base(&source, &flags));
    assert_eq!(k1, k3, "identical bytes must reproduce the key");
}

/// `cc::Build::compile` emits its OUT_DIR for every archive it creates.
/// Repeating that directory must not make the one archive uncacheable.
#[test]
fn duplicate_native_search_dir_keeps_static_lib_cacheable() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    std::fs::write(libdir.join("libfoo.a"), b"archive bytes").unwrap();
    let search = format!("native={}", libdir.display());
    let flags = [
        "-L",
        search.as_str(),
        "-L",
        search.as_str(),
        "-l",
        "static=foo",
    ];

    assert!(!key_of(&flag_base(&source, &flags)).is_empty());
}

/// A `dylib=` lib is referenced at runtime, not bundled into the output, so
/// its content must NOT enter the key (guards against over-keying).
/// Generic `-l` keying, checked on hosts that do not probe native MSVC
/// inputs. On a Windows host the library must exist (fail closed), so the
/// Windows shape lives in the `windows_*` tests with an injected probe.
#[cfg(not(windows))]
#[test]
fn native_dylib_content_does_not_change_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    let lib = libdir.join("libfoo.so");
    std::fs::write(&lib, b"so v1").unwrap();
    let search = format!("native={}", libdir.display());
    let flags = ["-L", search.as_str(), "-l", "dylib=foo"];

    let k1 = key_of(&flag_base(&source, &flags));
    std::fs::write(&lib, b"so v2 changed").unwrap();
    let k2 = key_of(&flag_base(&source, &flags));
    assert_eq!(k1, k2, "a dynamic lib's content must not key the consumer");
}

/// rustc's default `-L` kind is `all`, which searches native libs too, so a
/// `static=` lib found under `-L all=<dir>` must be content-keyed (#421).
#[test]
fn native_static_lib_in_all_search_dir_is_keyed() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    let lib = libdir.join("libfoo.a");
    std::fs::write(&lib, b"v1 archive bytes").unwrap();
    let search = format!("all={}", libdir.display());
    let flags = ["-L", search.as_str(), "-l", "static=foo"];

    let k1 = key_of(&flag_base(&source, &flags));
    std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
    let k2 = key_of(&flag_base(&source, &flags));
    assert_ne!(
        k1, k2,
        "a static lib under `-L all=` must be content-keyed too (#421)"
    );
}

/// Argv for an rlib, where a build-script archive does its damage: rustc
/// copies the archive into the rlib and every later link reads that copy.
fn rlib_base(source: &Path, extra: &[&str]) -> Vec<String> {
    let mut args: Vec<String> = ["rustc", "--crate-name", "mylib", "--crate-type", "lib"]
        .map(String::from)
        .into();
    args.push(source.to_string_lossy().into_owned());
    args.extend(extra.iter().map(|s| s.to_string()));
    args
}

/// A build script can name its archive with link modifiers:
/// `static:+whole-archive=foo` still reads `libfoo.a`, and
/// `static:+verbatim=foo.a` reads `foo.a`. Either is bundled like a plain
/// `static=foo`, so rebuilding the archive in place must change the key.
#[test]
fn native_static_lib_named_with_modifiers_is_content_keyed() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    let search = format!("native={}", libdir.display());

    for (spec, file) in [
        ("static:+whole-archive=foo", "libfoo.a"),
        ("static:+verbatim=foo.a", "foo.a"),
    ] {
        let lib = libdir.join(file);
        let flags = ["-L", search.as_str(), "-l", spec];
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let k1 = key_of(&rlib_base(&source, &flags));
        std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
        let k2 = key_of(&rlib_base(&source, &flags));
        assert_ne!(k1, k2, "{spec}: a rebuilt archive must change the key");
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let k3 = key_of(&rlib_base(&source, &flags));
        assert_eq!(k1, k3, "{spec}: identical bytes must reproduce the key");
        std::fs::remove_file(&lib).unwrap();
    }
}

/// `static=foo:bar` makes rustc link `bar` wherever a `#[link]` attribute
/// names `foo`. Which archive that reads is not modelled, and keying it by
/// name could restore an rlib bundling an older archive, so the key fails.
#[test]
fn renamed_native_static_lib_is_not_cacheable() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    std::fs::write(libdir.join("libfoo.a"), b"archive bytes").unwrap();
    std::fs::write(libdir.join("libbar.a"), b"archive bytes").unwrap();
    let search = format!("native={}", libdir.display());
    let flags = ["-L", search.as_str(), "-l", "static=foo:bar"];

    let parsed = RustcArgs::parse(&rlib_base(&source, &flags)).unwrap();
    let error = compute_cache_key(
        &parsed,
        &FileHasher::new(),
        &PathNormalizer::empty(),
        &KeyEnv::default(),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("is not cacheable"),
        "a renamed static lib must fail the key: {error:#}"
    );
}

/// A `dylib` lib is referenced, not copied into the rlib, so its bytes
/// stay out of the key while `static` specs with modifiers are hashed.
/// `+verbatim` names the file itself, so a `dylib` wrongly taken for a
/// `static` would be found and hashed.
#[test]
fn native_dylib_stays_name_only_for_an_rlib() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let libdir = dir.path().join("out");
    std::fs::create_dir_all(&libdir).unwrap();
    let lib = libdir.join("libfoo.so");
    let search = format!("native={}", libdir.display());

    for spec in ["dylib=foo", "dylib:+verbatim=libfoo.so"] {
        let flags = ["-L", search.as_str(), "-l", spec];
        std::fs::write(&lib, b"so v1").unwrap();
        let k1 = key_of(&rlib_base(&source, &flags));
        std::fs::write(&lib, b"so v2 changed").unwrap();
        let k2 = key_of(&rlib_base(&source, &flags));
        assert_eq!(
            k1, k2,
            "{spec}: a dynamic lib's content must not key the rlib"
        );
    }
}

/// A Cargo build tree: `<tmp>/target/debug/deps` for outputs and a build
/// script's `<tmp>/target/debug/build/s-1/out`, plus a dir outside it.
struct NativeTree {
    _tmp: tempfile::TempDir,
    target: PathBuf,
    deps: PathBuf,
    out: PathBuf,
    elsewhere: PathBuf,
}

fn native_tree() -> NativeTree {
    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("target");
    let deps = target.join("debug/deps");
    let out = target.join("debug/build/s-1/out");
    let elsewhere = tmp.path().join("elsewhere/lib");
    for dir in [&deps, &out, &elsewhere] {
        std::fs::create_dir_all(dir).unwrap();
    }
    NativeTree {
        _tmp: tmp,
        target,
        deps,
        out,
        elsewhere,
    }
}

/// Argv for a `crate_type` unit (`test` for a `--test` harness) writing
/// into `out_dir`. Keyed with [`key_of_flags`], so no source is read.
fn unit_args(crate_type: &str, out_dir: &Path, extra: &[&str]) -> Vec<String> {
    let mut args: Vec<String> = ["rustc", "--crate-name", "mylib", "src/lib.rs"]
        .map(String::from)
        .into();
    args.push("--out-dir".to_string());
    args.push(out_dir.display().to_string());
    if crate_type == "test" {
        args.push("--test".to_string());
    } else {
        args.push(format!("--crate-type={crate_type}"));
    }
    args.extend(extra.iter().map(|s| s.to_string()));
    args
}

/// [`key_of_flags`] for a key that may fail.
#[cfg(not(windows))]
fn try_key_of_flags(args: &[String]) -> Result<String> {
    let mut parsed = RustcArgs::parse(args).unwrap();
    parsed.source_file = None;
    compute_cache_key(
        &parsed,
        &FileHasher::new(),
        &PathNormalizer::empty(),
        &KeyEnv::default(),
    )
}

/// Key `argv`, rewrite `file` with new bytes, and key it again.
#[cfg(not(windows))]
fn keys_around_rewrite(argv: &[String], file: &Path) -> (String, String) {
    std::fs::write(file, b"v1 native bytes").unwrap();
    let before = key_of_flags(argv);
    std::fs::write(file, b"v2 native bytes, different").unwrap();
    (before, key_of_flags(argv))
}

/// A Unix link context for `target` with no default dirs, no OUT_DIR, no
/// build tree, no system library dirs and no path rules.
fn unix_context<'a>(target: &'a str, normalizer: &'a PathNormalizer) -> NativeLinkContext<'a> {
    NativeLinkContext {
        native_windows_msvc: false,
        target,
        default_dirs: Vec::new(),
        out_dir: None,
        build_tree: Vec::new(),
        system_dirs: Vec::new(),
        path_normalizer: normalizer,
    }
}

/// [`fold_native_link_inputs`] for `argv` with explicit search dirs and
/// context: the fold's digest and what it reports.
fn fold_inputs(
    argv: &[String],
    native: &[PathBuf],
    framework: &[PathBuf],
    context: &NativeLinkContext<'_>,
) -> Result<(String, KeyedNativeArchives)> {
    let args = RustcArgs::parse(argv).unwrap();
    let mut hasher = blake3::Hasher::new();
    let keyed = fold_native_link_inputs(
        &mut hasher,
        &args,
        &NativeSearchDirs { native, framework },
        context,
        &FileHasher::new(),
    )?;
    Ok((hasher.finalize().to_hex().to_string(), keyed))
}

/// Whether rewriting `file` with new `ar` bytes changes the fold.
fn fold_rewrites(file: &Path, fold: impl Fn() -> Result<(String, KeyedNativeArchives)>) -> bool {
    std::fs::write(file, b"!<arch>\nv1 native bytes").unwrap();
    let before = fold().unwrap().0;
    std::fs::write(file, b"!<arch>\nv2 native bytes, different").unwrap();
    before != fold().unwrap().0
}

/// Cargo passes a build script's `-L` to every dependent, so a binary two
/// crates above a sys crate sees its OUT_DIR with no `-l`. The archive
/// there reaches the binary through the sys crate's rlib, whose bytes the
/// binary's `--extern`s do not cover.
#[cfg(not(windows))]
#[test]
fn linked_output_keys_build_tree_archives_without_link_spec() {
    let _lock = key_test_lock();
    let tree = native_tree();
    let search = format!("native={}", tree.out.display());
    for crate_type in ["bin", "test", "cdylib", "staticlib"] {
        let argv = unit_args(crate_type, &tree.deps, &["-L", &search]);
        let (before, after) = keys_around_rewrite(&argv, &tree.out.join("libfoo.a"));
        assert_ne!(before, after, "{crate_type}: a rebuilt archive must re-key");
    }
}

/// Every archive in a `-L` dir away from the system library dirs keys a
/// linked output, inside the build tree or not: a monorepo's native build
/// above the workspace, a path dependency beside it, or `OPENSSL_DIR`.
/// Cargo hands a build script's `-L` to every dependent, and a
/// dependency's rlib can carry those archives with no `-l` of this unit
/// naming them. The same holds for a link-argument `-L` and for a static
/// framework in a `framework=` dir.
#[cfg(not(windows))]
#[test]
fn linked_output_scans_dirs_in_and_outside_the_workspace() {
    let _lock = key_test_lock();
    let tree = native_tree();
    let in_tree = tree.target.join("native");
    std::fs::create_dir_all(&in_tree).unwrap();
    for dir in [&in_tree, &tree.elsewhere] {
        let shown = dir.display();
        let archive = dir.join("libssl.a");
        let search = format!("native={shown}");
        for crate_type in ["bin", "test", "cdylib", "staticlib"] {
            let argv = unit_args(crate_type, &tree.deps, &["-L", &search]);
            let (before, after) = keys_around_rewrite(&argv, &archive);
            assert_ne!(before, after, "{shown}: {crate_type}");
        }
        let link_search = format!("-Clink-arg=-L{shown}");
        let (before, after) =
            keys_around_rewrite(&unit_args("bin", &tree.deps, &[&link_search]), &archive);
        assert_ne!(before, after, "{shown}: a link-argument -L dir");

        let bundle = dir.join("Foo.framework");
        std::fs::create_dir_all(&bundle).unwrap();
        let frameworks = format!("framework={shown}");
        let bin = unit_args("bin", &tree.deps, &["-L", &frameworks]);
        std::fs::write(bundle.join("Foo"), b"!<arch>\nv1 framework").unwrap();
        let before = key_of_flags(&bin);
        std::fs::write(bundle.join("Foo"), b"!<arch>\nv2 framework").unwrap();
        assert_ne!(before, key_of_flags(&bin), "{shown}: a static framework");
    }
}

/// A system library dir outside the build tree (`/opt/homebrew/lib`) is
/// not scanned: rewriting an archive there that no `-l` names leaves a
/// linked output's fold alone, and rewriting one an `-l` resolves to
/// re-keys it, in each spelling of `-l`. A `-L` in the link arguments
/// follows the same rule.
#[test]
fn system_dir_archives_key_only_by_name() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = NativeLinkContext {
        system_dirs: vec![resolved_path(&tree.elsewhere)],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    std::fs::write(tree.elsewhere.join("libssl.a"), b"!<arch>\nssl").unwrap();
    let archive = tree.elsewhere.join("libz.a");
    let rewrites = |crate_type: &str, native: &[PathBuf], extra: &[&str]| {
        let argv = unit_args(crate_type, &tree.deps, extra);
        fold_rewrites(&archive, || fold_inputs(&argv, native, &[], &context))
    };
    let native = [tree.elsewhere.clone()];
    for crate_type in ["bin", "test", "cdylib", "staticlib"] {
        assert!(!rewrites(crate_type, &native, &[]), "{crate_type}: no -l");
        assert!(
            rewrites(crate_type, &native, &["-l", "static=z"]),
            "{crate_type}: a static spec"
        );
    }
    for spec in ["z", "dylib=z"] {
        assert!(rewrites("bin", &native, &["-l", spec]), "{spec}");
    }

    let link_search = format!("-Clink-arg=-L{}", tree.elsewhere.display());
    assert!(!rewrites("bin", &[], &[&link_search]), "a link-argument -L");
    for link_arg in [
        "-Clink-arg=-lz",
        "-Clink-arg=-Wl,-hidden-lz",
        "-Clink-arg=-Wl,-force-lz",
    ] {
        assert!(
            rewrites("bin", &[], &[&link_search, link_arg]),
            "{link_arg}"
        );
    }
}

/// A thin archive or an unreadable dir under a system library dir refuses
/// a link only when an `-l` resolves to it; anywhere else the scan reads
/// it and refuses.
#[test]
fn system_dir_thin_archive_refuses_only_a_link_that_names_it() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let system = NativeLinkContext {
        system_dirs: vec![resolved_path(&tree.elsewhere)],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    let elsewhere = unix_context("x86_64-unknown-linux-gnu", &normalizer);
    std::fs::write(tree.elsewhere.join("libthin.a"), b"!<thin>\n").unwrap();
    let fold = |context: &NativeLinkContext<'_>, dir: &Path, extra: &[&str]| {
        let argv = unit_args("bin", &tree.deps, extra);
        fold_inputs(&argv, &[dir.to_path_buf()], &[], context)
    };
    let unreadable = tree.elsewhere.join("libthin.a");
    assert!(fold(&system, &tree.elsewhere, &[]).is_ok());
    assert!(fold(&system, &tree.elsewhere, &["-l", "static=thin"]).is_err());
    assert!(fold(&system, &unreadable, &[]).is_ok());
    assert!(fold(&elsewhere, &tree.elsewhere, &[]).is_err());
    assert!(fold(&elsewhere, &unreadable, &[]).is_err());
}

/// A dir under a system library dir is still scanned inside the build
/// tree. Each is compared resolved: a dir is judged by where it leads,
/// and a build tree spelled through a symlink (macOS reaches `/var` as
/// `/private/var`) still holds the build script's OUT_DIR.
#[cfg(unix)]
#[test]
fn system_dirs_and_the_build_tree_are_compared_resolved() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let system = tree.target.with_file_name("system");
    std::fs::create_dir_all(&system).unwrap();
    let leads_out = system.join("out");
    std::os::unix::fs::symlink(&tree.out, &leads_out).unwrap();
    let alias = tree.target.with_file_name("alias");
    std::os::unix::fs::symlink(&tree.target, &alias).unwrap();
    let bin = unit_args("bin", &tree.deps, &[]);
    let rewrites = |dir: &Path, system_dirs: &[&Path], build_tree: &[&Path]| {
        let context = NativeLinkContext {
            system_dirs: system_dirs.iter().map(|dir| resolved_path(dir)).collect(),
            build_tree: build_tree.iter().map(|root| root.to_path_buf()).collect(),
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        let dirs = [dir.to_path_buf()];
        fold_rewrites(&tree.out.join("libfoo.a"), || {
            fold_inputs(&bin, &dirs, &[], &context)
        })
    };
    let target: &Path = &tree.target;
    assert!(rewrites(&leads_out, &[&system], &[]), "leads out of it");
    let leads_in = alias.join("debug/build/s-1/out");
    assert!(!rewrites(&leads_in, &[target], &[]), "leads into it");
    assert!(rewrites(&tree.out, &[target], &[&alias]), "an aliased tree");
    assert!(!rewrites(&tree.out, &[target], &[&tree.elsewhere]));
}

/// The scan keys linked outputs only, only archives, and no system
/// library dir outside the build tree.
#[test]
fn archive_scan_leaves_other_units_and_files_alone() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
    let dirs = [tree.out.clone()];
    let rewrites = |argv: Vec<String>, file: &Path| {
        fold_rewrites(file, || fold_inputs(&argv, &dirs, &[], &context))
    };
    let archive = tree.out.join("libfoo.a");
    assert!(!rewrites(unit_args("rlib", &tree.deps, &[]), &archive));
    assert!(!rewrites(
        unit_args("bin", &tree.deps, &["--emit=metadata"]),
        &archive
    ));
    let bin = unit_args("bin", &tree.deps, &[]);
    assert!(rewrites(bin.clone(), &archive));
    assert!(!rewrites(bin.clone(), &tree.out.join("libfoo.so")));
    assert!(!rewrites(bin.clone(), &tree.out.join("foo.o")));

    let system = NativeLinkContext {
        system_dirs: vec![resolved_path(&tree.target)],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    assert!(!fold_rewrites(&archive, || fold_inputs(
        &bin,
        &dirs,
        &[],
        &system
    )));
}

/// A `#[link(kind = "static")]` attribute bundles an archive into an rlib
/// with no `-l` on argv. The archives under the unit's own OUT_DIR key its
/// rlib, so the audit need not refuse it; dirs elsewhere do not.
#[test]
fn rlib_keys_archives_under_its_own_out_dir() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let unit_out = tree.out.clone();
    let own = tree.out.join("x64");
    std::fs::create_dir_all(&own).unwrap();
    let dirs = [own.clone(), tree.elsewhere.clone()];
    let rlib = unit_args("rlib", &tree.deps, &[]);
    let fold_with = |argv: &[String], out_dir: Option<&Path>| {
        let context = NativeLinkContext {
            out_dir,
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        fold_inputs(argv, &dirs, &[], &context)
    };
    let archive = own.join("libWebView2LoaderStatic.a");
    assert!(fold_rewrites(&archive, || fold_with(
        &rlib,
        Some(&unit_out)
    )));
    assert_eq!(
        fold_with(&rlib, Some(&unit_out)).unwrap().1.archives,
        std::slice::from_ref(&archive)
    );
    assert!(!fold_rewrites(&tree.elsewhere.join("libdep.a"), || {
        fold_with(&rlib, Some(&unit_out))
    }));
    assert!(!fold_rewrites(&archive, || fold_with(&rlib, None)));
    let metadata = unit_args("rlib", &tree.deps, &["--emit=metadata"]);
    assert!(!fold_rewrites(&archive, || fold_with(
        &metadata,
        Some(&unit_out)
    )));

    // Cargo's OUT_DIR and the build script's `-L` may spell the same dir
    // differently.
    #[cfg(unix)]
    {
        let alias = tree.elsewhere.join("alias");
        std::os::unix::fs::symlink(&unit_out, &alias).unwrap();
        assert!(fold_rewrites(&archive, || fold_with(&rlib, Some(&alias))));
    }
}

#[test]
fn native_scan_dirs_follow_the_unit_and_skip_cargo_packages() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let unit_out = tree.out.parent().unwrap().to_path_buf();
    let context = NativeLinkContext {
        out_dir: Some(&unit_out),
        system_dirs: vec![resolved_path(&tree.elsewhere)],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    let native = tree.target.join("native");
    std::fs::create_dir_all(&native).unwrap();
    let dirs = [
        tree.out.clone(),
        tree.elsewhere.clone(),
        native.clone(),
        tree.out.clone(),
    ];
    let scan = |crate_type: &str| {
        let args = RustcArgs::parse(&unit_args(crate_type, &tree.deps, &[])).unwrap();
        native_scan_dirs(&args, &dirs, &context)
    };
    assert_eq!(scan("bin"), [tree.out.clone(), native.clone()]);
    assert_eq!(scan("rlib"), std::slice::from_ref(&tree.out));
    assert_eq!(scan("rlib,staticlib"), scan("bin"));
    assert_eq!(context.scanned_dirs(&dirs), scan("bin"));

    let package = PathBuf::from("/home/u/.cargo/registry/src/index-1/windows_x-0.52.6/lib");
    let dirs = [package.clone(), tree.elsewhere.clone()];
    let kept: Vec<&PathBuf> =
        dirs_outside_packages(&dirs, |dir| dir.replace("/home/u/.cargo", "<CARGO_HOME>")).collect();
    assert_eq!(kept, [&tree.elsewhere]);
}

#[test]
fn cargo_package_dirs_are_registry_sources_and_git_checkouts() {
    assert!(is_cargo_package_dir(
        "<CARGO_HOME>/registry/src/index.crates.io-1/foo-1.0.0/lib"
    ));
    assert!(is_cargo_package_dir(
        "<CARGO_HOME>\\registry\\src\\index.crates.io-1\\foo-1.0.0\\lib"
    ));
    assert!(is_cargo_package_dir(
        "<CARGO_HOME>/git/checkouts/foo-1a2b/3c4d/lib"
    ));
    assert!(!is_cargo_package_dir("<CARGO_HOME>/registry/cache/x"));
    assert!(!is_cargo_package_dir("<CARGO_HOME>/git/db/x"));
    assert!(!is_cargo_package_dir("<CARGO_HOME>/src/registry"));
    assert!(!is_cargo_package_dir("/w/registry/src/index/foo-1.0.0/lib"));
    assert!(!is_cargo_package_dir("<WORKSPACE>/registry/src/x"));
}

#[test]
fn dirs_under_compare_resolved_paths() {
    let tree = native_tree();
    let root = tree.out.parent().unwrap();
    let dirs = [tree.out.clone(), tree.elsewhere.clone()];
    assert_eq!(dirs_under(&dirs, root), std::slice::from_ref(&tree.out));
    assert!(dirs_under(&dirs, &tree.deps).is_empty());
    assert!(dirs_under(&[], root).is_empty());
    assert_eq!(
        unique_dirs(&[tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()]),
        [tree.out.clone(), tree.elsewhere.clone()]
    );

    // A root or a dir spelled through a symlink is compared by its target.
    #[cfg(unix)]
    {
        let alias = tree.target.with_file_name("alias");
        std::os::unix::fs::symlink(&tree.target, &alias).unwrap();
        assert_eq!(dirs_under(&dirs, &alias), std::slice::from_ref(&tree.out));
        let aliased = [alias.join("debug/build/s-1/out"), tree.elsewhere.clone()];
        assert_eq!(
            dirs_under(&aliased, &tree.target),
            std::slice::from_ref(&aliased[0])
        );
    }
}

#[test]
fn build_tree_roots_are_the_target_dir_and_the_workspace() {
    let deps = Path::new("/w/target/debug/deps");
    let workspace = Path::new("/w");
    assert_eq!(
        build_tree_roots_of(Some(deps), None, Some(workspace)),
        [PathBuf::from("/w/target"), PathBuf::from("/w")]
    );
    assert_eq!(
        build_tree_roots_of(Some(Path::new("/w/target/debug/build/s-1")), None, None),
        [PathBuf::from("/w/target")]
    );
    assert_eq!(
        build_tree_roots_of(
            Some(Path::new("/w/target/debug/build/s/0123456789abcdef/out")),
            None,
            None
        ),
        [PathBuf::from("/w/target")],
        "Cargo 1.100's per-unit layout"
    );
    assert_eq!(
        build_tree_roots_of(Some(Path::new("/tmp/out")), None, Some(workspace)),
        [PathBuf::from("/w")],
        "an --out-dir outside Cargo's layout"
    );
    assert!(build_tree_roots_of(None, None, None).is_empty());

    let triple = "x86_64-unknown-linux-gnu";
    let cross = Path::new("/w/target/x86_64-unknown-linux-gnu/debug");
    assert_eq!(
        cargo_target_dir(cross, Some(triple)),
        Path::new("/w/target")
    );
    assert_eq!(
        cargo_target_dir(Path::new("/w/target/spec/debug"), Some("targets/spec.json")),
        Path::new("/w/target"),
        "a target spec file names its dir by its stem"
    );
    assert_eq!(
        cargo_target_dir(cross, None),
        Path::new("/w/target/x86_64-unknown-linux-gnu")
    );
    assert_eq!(
        cargo_target_dir(Path::new("/w/target/debug"), Some(triple)),
        Path::new("/w/target"),
        "a host unit of a cross build"
    );
    assert_eq!(
        cargo_target_dir(Path::new("/debug"), Some("debug")),
        Path::new("/")
    );
    assert_eq!(cargo_target_dir(Path::new("/"), None), Path::new("/"));
}

#[test]
fn build_tree_roots_read_the_unit_out_dir_and_target() {
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "src/main.rs".to_string(),
        "--out-dir".to_string(),
        "/w/target/x86_64-unknown-linux-gnu/debug/deps".to_string(),
        "--target".to_string(),
        "x86_64-unknown-linux-gnu".to_string(),
    ])
    .unwrap();
    assert_eq!(
        build_tree_roots(&args).first(),
        Some(&PathBuf::from("/w/target")),
        "the target dir comes from --out-dir and --target"
    );
}
#[test]
fn native_dir_archives_list_regular_archives_in_order() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "libz.a",
        "foo.LIB",
        "liba.A",
        "libfoo.so",
        "foo.o",
        "notes.txt",
    ] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    std::fs::create_dir(dir.path().join("sub.a")).unwrap();
    assert_eq!(
        native_dir_archives(dir.path()).unwrap(),
        ["foo.LIB", "liba.A", "libz.a"].map(|name| dir.path().join(name))
    );
    assert!(
        native_dir_archives(&dir.path().join("absent"))
            .unwrap()
            .is_empty(),
        "a missing dir holds no archives"
    );
    // Windows reports a file read as a dir as a missing path.
    #[cfg(unix)]
    assert!(
        native_dir_archives(&dir.path().join("libz.a")).is_err(),
        "a dir that cannot be read is an error"
    );
    assert!(is_native_archive_name("libfoo.a"));
    assert!(is_native_archive_name("FOO.Lib"));
    assert!(!is_native_archive_name("libfoo.rlib.bak"));
    assert!(!is_native_archive_name("libfoo.so"));
}

/// ld64 links a static framework's binary into the output like an
/// archive; a dynamic framework, or a dir that is no framework, is not
/// one.
#[test]
fn native_dir_static_frameworks_list_archive_binaries() {
    let dir = tempfile::tempdir().unwrap();
    for (framework, bytes) in [
        ("Static", &b"!<arch>\nmembers"[..]),
        ("Dynamic", b"\xcf\xfa\xed\xfe dylib"),
    ] {
        let bundle = dir.path().join(format!("{framework}.framework"));
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join(framework), bytes).unwrap();
    }
    std::fs::create_dir_all(dir.path().join("Empty.framework")).unwrap();
    std::fs::create_dir_all(dir.path().join("Other")).unwrap();
    std::fs::write(dir.path().join("Other/Other"), b"!<arch>\nx").unwrap();
    assert_eq!(
        native_dir_static_frameworks(dir.path()).unwrap(),
        [dir.path().join("Static.framework/Static")]
    );
    assert!(
        native_dir_static_frameworks(&dir.path().join("absent"))
            .unwrap()
            .is_empty()
    );
}

/// A linking unit hashes the static frameworks in its `framework=` dirs,
/// and under a system library dir the one a `framework` spec names; an
/// rlib does not link them.
#[test]
fn linking_unit_keys_static_frameworks() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = NativeLinkContext {
        system_dirs: vec![resolved_path(&tree.elsewhere)],
        ..unix_context("aarch64-apple-darwin", &normalizer)
    };
    let rewrites = |dir: &Path, crate_type: &str, extra: &[&str]| {
        let bundle = dir.join("Foo.framework");
        std::fs::create_dir_all(&bundle).unwrap();
        let argv = unit_args(crate_type, &tree.deps, extra);
        let frameworks = [dir.to_path_buf()];
        fold_rewrites(&bundle.join("Foo"), || {
            fold_inputs(&argv, &[], &frameworks, &context)
        })
    };
    assert!(rewrites(&tree.out, "bin", &[]));
    assert!(!rewrites(&tree.out, "rlib", &[]));
    assert!(!rewrites(&tree.elsewhere, "bin", &[]));
    assert!(rewrites(&tree.elsewhere, "bin", &["-l", "framework=Foo"]));
    assert!(!rewrites(&tree.elsewhere, "rlib", &["-l", "framework=Foo"]));
}

/// A dir under a system library dir is scanned only inside the build
/// tree; every other dir is scanned, once.
#[test]
fn scanned_dirs_skip_system_dirs_outside_the_build_tree() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let scanned = |system: &Path, build_tree: &[&Path]| {
        let context = NativeLinkContext {
            system_dirs: vec![resolved_path(system)],
            build_tree: build_tree.iter().map(|root| root.to_path_buf()).collect(),
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        context.scanned_dirs(&[tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()])
    };
    let tmp = tree.target.parent().unwrap();
    let target: &Path = &tree.target;
    assert_eq!(scanned(tmp, &[target]), std::slice::from_ref(&tree.out));
    assert!(scanned(tmp, &[]).is_empty());
    assert_eq!(
        scanned(&tree.elsewhere, &[]),
        std::slice::from_ref(&tree.out)
    );
    assert_eq!(
        scanned(&tree.deps, &[]),
        [tree.out.clone(), tree.elsewhere.clone()]
    );
}

#[test]
fn system_library_dirs_hold_os_and_package_manager_prefixes() {
    let dirs = system_library_dirs();
    for dir in ["/usr/lib", "/usr/local/lib", "/opt/homebrew", "/nix/store"] {
        assert!(dirs.contains(&PathBuf::from(dir)), "{dir}");
    }
    for local in [
        "/home/u/mono/build/lib",
        "/usr/src/app/build",
        "/opt/app/lib",
    ] {
        assert!(!is_under_any(Path::new(local), &dirs), "{local}");
    }
}

#[test]
fn is_under_any_compares_whole_components() {
    let roots = [PathBuf::from("/x"), PathBuf::from("/a")];
    assert!(is_under_any(Path::new("/a/b"), &roots));
    assert!(is_under_any(Path::new("/a"), &roots));
    assert!(!is_under_any(Path::new("/ab"), &roots));
    assert!(!is_under_any(Path::new("/a"), &[]));
}

/// The bundle audit reads an rlib's scanned `-L` dirs, so a system
/// library dir outside the build tree cannot refuse every rlib that sees
/// it. Other units report none.
#[test]
fn audit_dirs_are_an_rlibs_scanned_dirs() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = NativeLinkContext {
        system_dirs: vec![resolved_path(&tree.elsewhere)],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    let dirs = [tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()];
    let audit_dirs = |crate_type: &str| {
        let argv = unit_args(crate_type, &tree.deps, &[]);
        fold_inputs(&argv, &dirs, &[], &context).unwrap().1.dirs
    };
    assert_eq!(audit_dirs("rlib"), std::slice::from_ref(&tree.out));
    assert!(audit_dirs("bin").is_empty());
}

/// rustc hands the value of a `link-arg` `-l` spec to the linker as is,
/// so a linking unit with one is refused; an rlib only records it.
#[test]
fn link_arg_lib_spec_refuses_a_link() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
    let fold = |crate_type: &str, spec: &str| {
        let argv = unit_args(crate_type, &tree.deps, &["-l", spec]);
        fold_inputs(&argv, &[], &[], &context)
    };
    assert!(fold("bin", "link-arg=-lfoo").is_err());
    assert!(fold("cdylib", "link-arg:+verbatim=-Wl,-hidden-lfoo").is_err());
    assert!(fold("rlib", "link-arg=-lfoo").is_ok());
    assert!(fold("bin", "dylib=foo").is_ok());

    assert!(is_link_arg_spec("link-arg=-lfoo"));
    assert!(is_link_arg_spec("link-arg:+verbatim=-lfoo"));
    assert!(!is_link_arg_spec("static=link-arg"));
    assert!(!is_link_arg_spec("link-arg"));
    assert!(!is_link_arg_spec("dylib=foo"));
}

#[test]
fn framework_request_reads_framework_specs() {
    assert_eq!(framework_request("framework=Foo"), Some("Foo"));
    assert_eq!(framework_request("framework:-as-needed=Foo"), Some("Foo"));
    assert_eq!(framework_request("framework=Foo:Bar"), Some("Bar"));
    assert_eq!(framework_request("framework="), None);
    assert_eq!(framework_request("dylib=Foo"), None);
    assert_eq!(framework_request("Foo"), None);
}

/// ld64 takes the first `Foo.framework/Foo` in its `-F` dirs; only an
/// `ar` archive there is static.
#[test]
fn resolve_static_framework_takes_the_first_bundle() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let dirs = [first.path().to_path_buf(), second.path().to_path_buf()];
    let bundle = |dir: &Path, bytes: &[u8]| {
        let bundle = dir.join("Foo.framework");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("Foo"), bytes).unwrap();
        bundle.join("Foo")
    };
    assert_eq!(resolve_static_framework("Foo", &dirs).unwrap(), None);
    std::fs::create_dir_all(first.path().join("Foo.framework")).unwrap();
    let later = bundle(second.path(), b"!<arch>\nstatic");
    assert_eq!(
        resolve_static_framework("Foo", &dirs).unwrap(),
        Some(later),
        "a bundle with no binary is skipped"
    );
    bundle(first.path(), b"\xcf\xfa\xed\xfe dylib");
    assert_eq!(
        resolve_static_framework("Foo", &dirs).unwrap(),
        None,
        "a dynamic framework first hides a later static one"
    );
    let static_first = bundle(first.path(), b"!<arch>\nstatic");
    assert_eq!(
        resolve_static_framework("Foo", &dirs).unwrap(),
        Some(static_first)
    );
}

/// Each scanned archive folds under its dir index and its path in the
/// dir; a thin archive, whose members live elsewhere, refuses the key.
#[test]
fn fold_native_dir_archives_names_dir_index_and_file() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    std::fs::write(first.path().join("liba.a"), b"a").unwrap();
    std::fs::write(second.path().join("libb.a"), b"b").unwrap();
    let dirs = [first.path().to_path_buf(), second.path().to_path_buf()];
    let fold = |dirs: &[PathBuf]| {
        let mut hasher = blake3::Hasher::new();
        let hashed = fold_native_dir_archives(&mut hasher, dirs, native_dir_archives, |path| {
            Ok(std::fs::read_to_string(path)?)
        })
        .unwrap();
        (hasher.finalize(), hashed)
    };
    let mut expected = blake3::Hasher::new();
    fold_field(&mut expected, b"native_dir_archive.v1:", b"0/liba.a=a");
    fold_field(&mut expected, b"native_dir_archive.v1:", b"1/libb.a=b");
    let (digest, hashed) = fold(&dirs);
    assert_eq!(digest, expected.finalize());
    assert_eq!(
        hashed,
        [first.path().join("liba.a"), second.path().join("libb.a")]
    );
    assert_eq!(fold(&[]).0, blake3::Hasher::new().finalize());

    // A framework binary names its bundle too.
    let bundle = first.path().join("Foo.framework");
    std::fs::create_dir_all(&bundle).unwrap();
    std::fs::write(bundle.join("Foo"), b"!<arch>\nf").unwrap();
    let mut hasher = blake3::Hasher::new();
    fold_native_dir_archives(
        &mut hasher,
        &dirs[..1],
        native_dir_static_frameworks,
        |_| Ok("d".to_string()),
    )
    .unwrap();
    let mut expected = blake3::Hasher::new();
    let binary = Path::new("Foo.framework").join("Foo");
    fold_field(
        &mut expected,
        b"native_dir_archive.v1:",
        format!("0/{}=d", binary.display()).as_bytes(),
    );
    assert_eq!(hasher.finalize(), expected.finalize());

    std::fs::write(first.path().join("libthin.a"), b"!<thin>\n").unwrap();
    let mut hasher = blake3::Hasher::new();
    let thin = fold_native_dir_archives(&mut hasher, &dirs, native_dir_archives, |path| {
        FileHasher::new().hash_static_lib(path)
    });
    assert!(thin.is_err(), "a thin archive refuses the key");
}

/// An rlib or staticlib leaves a `-bundle` archive out of its output, so
/// the archive must not key it by its spec; the unit that links it later
/// does.
#[test]
fn unbundled_static_lib_keys_only_the_unit_that_links_it() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
    let dirs = [tree.elsewhere.clone()];
    let archive = tree.elsewhere.join("libfoo.a");
    let rewrites = |crate_type: &str, spec: &str| {
        let argv = unit_args(crate_type, &tree.deps, &["-l", spec]);
        fold_rewrites(&archive, || fold_inputs(&argv, &dirs, &[], &context))
    };
    assert!(!rewrites("rlib", "static:-bundle=foo"));
    assert!(rewrites("rlib", "static:-bundle,+bundle=foo"));
    assert!(rewrites("bin", "static:-bundle=foo"));
}

/// A Unix linker looks for `-l foo` or `-l dylib=foo` in the `-L` dirs,
/// then in its default dirs (`/usr/local/lib`, ...), and takes
/// `libfoo.a` when the first dir that has the name holds no shared
/// library, or always under `+crt-static`.
#[test]
fn unix_link_keys_a_library_only_a_default_dir_holds() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let archive = tree.elsewhere.join("libfoo.a");
    let fold_with = |crate_type: &str, spec: &str, extra: &[&str], target: &str| {
        let mut flags = vec!["-l", spec];
        flags.extend_from_slice(extra);
        let argv = unit_args(crate_type, &tree.deps, &flags);
        let context = NativeLinkContext {
            default_dirs: vec![tree.elsewhere.clone()],
            ..unix_context(target, &normalizer)
        };
        let native = [tree.out.clone()];
        fold_rewrites(&archive, || fold_inputs(&argv, &native, &[], &context))
    };
    let gnu = "x86_64-unknown-linux-gnu";
    assert!(fold_with("bin", "foo", &[], gnu));
    assert!(fold_with("bin", "dylib=foo", &[], gnu));
    assert!(!fold_with("rlib", "foo", &[], gnu), "an rlib does not link");

    std::fs::write(tree.elsewhere.join("libfoo.so"), b"shared").unwrap();
    assert!(
        !fold_with("bin", "foo", &[], gnu),
        "the shared library wins in its dir"
    );
    assert!(fold_with(
        "bin",
        "foo",
        &["-Ctarget-feature=+crt-static"],
        gnu
    ));
    assert!(
        fold_with("bin", "foo", &[], "x86_64-unknown-linux-musl"),
        "musl links statically by default"
    );
    std::fs::remove_file(tree.elsewhere.join("libfoo.so")).unwrap();

    // A shared library in a `-L` dir comes first.
    std::fs::write(tree.out.join("libfoo.so"), b"shared").unwrap();
    assert!(!fold_with("bin", "foo", &[], gnu));
}

/// A library a link argument names is looked up like `-l`, and its
/// first archive counts even beside a shared library, since the link
/// arguments may say `-Bstatic`.
#[test]
fn unix_link_keys_libraries_named_in_link_arguments() {
    let tree = native_tree();
    let normalizer = PathNormalizer::empty();
    let context = NativeLinkContext {
        default_dirs: vec![tree.elsewhere.clone()],
        ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
    };
    std::fs::write(tree.elsewhere.join("libfoo.so"), b"shared").unwrap();
    let rewrites = |crate_type: &str, flag: &str, file: &Path| {
        let argv = unit_args(crate_type, &tree.deps, &[flag]);
        fold_rewrites(file, || fold_inputs(&argv, &[], &[], &context))
    };
    let archive = tree.elsewhere.join("libfoo.a");
    assert!(rewrites(
        "bin",
        "-Clink-arg=-Wl,-Bstatic,-lfoo,-Bdynamic",
        &archive
    ));
    assert!(rewrites("bin", "-Clink-arg=-l:libfoo.a", &archive));
    assert!(!rewrites("rlib", "-Clink-arg=-lfoo", &archive));
    assert!(!rewrites(
        "bin",
        "-Clink-arg=-lbar",
        &tree.elsewhere.join("libbaz.a")
    ));
}

#[test]
fn resolve_unix_library_follows_the_first_dir_with_a_candidate() {
    let first = PathBuf::from("/one");
    let second = PathBuf::from("/two");
    let dirs = [first.clone(), second.clone()];
    let all = ["so", "dylib", "tbd"];
    let resolve = |present: &[&str], name: &str, verbatim: bool, prefer_static: bool| {
        resolve_unix_library(name, verbatim, &dirs, prefer_static, &all, |path| {
            present.iter().any(|file| path == Path::new(file))
        })
    };
    let archive_in = |dir: &Path| Some(dir.join("libfoo.a"));

    assert_eq!(
        resolve(&["/two/libfoo.a"], "foo", false, false),
        archive_in(&second)
    );
    assert_eq!(
        resolve(&["/one/libfoo.a", "/two/libfoo.a"], "foo", false, false),
        archive_in(&first)
    );
    assert_eq!(resolve(&[], "foo", false, false), None);
    for shared in ["/one/libfoo.so", "/one/libfoo.dylib", "/one/libfoo.tbd"] {
        assert_eq!(
            resolve(&[shared, "/one/libfoo.a"], "foo", false, false),
            None,
            "{shared}"
        );
        assert_eq!(
            resolve(&[shared, "/one/libfoo.a"], "foo", false, true),
            archive_in(&first),
            "{shared} under a static link"
        );
    }
    // A shared library alone in the first dir hides a later archive,
    // except from a static link, which only looks for archives.
    let hidden = ["/one/libfoo.so", "/two/libfoo.a"];
    assert_eq!(resolve(&hidden, "foo", false, false), None);
    assert_eq!(resolve(&hidden, "foo", false, true), archive_in(&second));

    assert_eq!(
        resolve(&["/two/foo.a"], "foo.a", true, false),
        Some(second.join("foo.a"))
    );
    assert_eq!(resolve(&["/one/libfoo.so"], "libfoo.so", true, false), None);

    // An ELF linker does not read a stray `.dylib` beside the archive.
    let elf = resolve_unix_library(
        "foo",
        false,
        &dirs,
        false,
        shared_library_extensions("x86_64-unknown-linux-gnu"),
        |path| path == Path::new("/one/libfoo.dylib") || path == Path::new("/one/libfoo.a"),
    );
    assert_eq!(elf, archive_in(&first));
}

#[test]
fn shared_library_extensions_follow_the_object_format() {
    assert_eq!(
        shared_library_extensions("aarch64-apple-darwin"),
        ["tbd", "dylib", "so"]
    );
    assert_eq!(
        shared_library_extensions("x86_64-unknown-linux-gnu"),
        ["so"]
    );
}

#[test]
fn static_link_preference_follows_crt_static() {
    let gnu = "x86_64-unknown-linux-gnu";
    let musl = "x86_64-unknown-linux-musl";
    assert!(!prefers_static_libraries(&[], gnu));
    assert!(prefers_static_libraries(&["+crt-static"], gnu));
    assert!(prefers_static_libraries(&["+sse2, +crt-static"], gnu));
    assert!(!prefers_static_libraries(
        &["+crt-static", "-crt-static"],
        gnu
    ));
    assert!(prefers_static_libraries(&[], musl));
    assert!(!prefers_static_libraries(&["-crt-static"], musl));
    assert!(prefers_static_libraries(&["+sse2"], musl));
}

/// With no `--target`, a link is for the wrapped rustc's host, as `rustc
/// -vV` reports it, not for whatever kache was built for: a musl host
/// links statically by default.
#[test]
fn link_target_defaults_to_the_wrapped_rustc_host() {
    let version = "rustc 1.98.0\nhost: x86_64-unknown-linux-musl\nrelease: 1.98.0\n";
    let host = rustc_host_triple(version);
    assert_eq!(link_target(None, host), "x86_64-unknown-linux-musl");
    assert!(prefers_static_libraries(&[], link_target(None, host)));
    assert_eq!(
        link_target(Some("aarch64-unknown-linux-gnu"), host),
        "aarch64-unknown-linux-gnu"
    );
    assert_eq!(link_target(None, None), "unknown");
}

/// A host-native Unix link searches `LIBRARY_PATH` and then the system
/// and local install dirs; a cross link and other targets search none of
/// them.
#[test]
fn default_library_dirs_follow_a_host_native_unix_target() {
    let linux = "x86_64-unknown-linux-gnu";
    let dirs = |target: &str, host: &str, library_path: Option<&str>| {
        default_library_dirs(target, Some(host), library_path.map(std::ffi::OsStr::new))
    };
    let expected: Vec<PathBuf> = [
        "/usr/local/lib/x86_64-linux-gnu",
        "/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/local/lib64",
        "/lib64",
        "/usr/lib64",
        "/usr/local/lib",
        "/lib",
        "/usr/lib",
    ]
    .map(PathBuf::from)
    .into();
    assert_eq!(dirs(linux, linux, None), expected);
    #[cfg(unix)]
    {
        let mut with_path = vec![PathBuf::from("/opt/a"), PathBuf::from("/opt/b")];
        with_path.extend(expected.clone());
        assert_eq!(dirs(linux, linux, Some("/opt/a::/opt/b")), with_path);
    }
    let apple = "aarch64-apple-darwin";
    assert_eq!(
        dirs(apple, apple, None),
        [PathBuf::from("/usr/lib"), PathBuf::from("/usr/local/lib")]
    );
    assert!(dirs("aarch64-unknown-linux-gnu", linux, None).is_empty());
    assert!(default_library_dirs(linux, None, None).is_empty());
    let windows = "x86_64-pc-windows-gnu";
    assert!(dirs(windows, windows, Some("/opt/a")).is_empty());
    assert_eq!(
        multiarch_name("aarch64-unknown-linux-gnu"),
        "aarch64-linux-gnu"
    );
    assert_eq!(
        multiarch_name("x86_64-linux-android"),
        "x86_64-linux-android"
    );
}

#[test]
fn unix_library_request_reads_kindless_and_dylib_specs() {
    assert_eq!(unix_library_request("foo"), Some(("foo", false)));
    assert_eq!(unix_library_request("dylib=foo"), Some(("foo", false)));
    assert_eq!(
        unix_library_request("dylib:-as-needed=foo"),
        Some(("foo", false))
    );
    assert_eq!(
        unix_library_request("dylib:+verbatim=libfoo.so"),
        Some(("libfoo.so", true))
    );
    assert_eq!(
        unix_library_request("dylib:+verbatim,-verbatim=foo"),
        Some(("foo", false))
    );
    assert_eq!(unix_library_request("static=foo"), None);
    assert_eq!(unix_library_request("framework=Foo"), None);
    assert_eq!(unix_library_request("dylib="), None);
}

/// Files a link argument names are linker inputs: rebuilding one in place
/// must re-key the binary.
#[cfg(not(windows))]
#[test]
fn unix_link_keys_files_named_in_link_arguments() {
    let _lock = key_test_lock();
    let tree = native_tree();
    let archive = tree.elsewhere.join("libfoo.a");
    let object = tree.elsewhere.join("extra.o");
    let a = archive.display().to_string();
    let o = object.display().to_string();
    for (flag, file) in [
        (format!("-Clink-arg={a}"), &archive),
        (format!("-Clink-arg=-Wl,--whole-archive,{a}"), &archive),
        (format!("-Clink-args=-force_load {a}"), &archive),
        (format!("-Clink-arg={o}"), &object),
    ] {
        let bin = unit_args("bin", &tree.deps, &[&flag]);
        let (before, after) = keys_around_rewrite(&bin, file);
        assert_ne!(before, after, "{flag}");
        let rlib = unit_args("rlib", &tree.deps, &[&flag]);
        let (before, after) = keys_around_rewrite(&rlib, file);
        assert_eq!(before, after, "an rlib does not link: {flag}");
    }

    // An archive is hashed as one, so a thin archive refuses the key; any
    // other input is hashed as a file, whatever its bytes.
    std::fs::write(tree.elsewhere.join("libthin.a"), b"!<thin>\n").unwrap();
    std::fs::write(tree.elsewhere.join("thin.o"), b"!<thin>\n").unwrap();
    let thin_object = format!("-Clink-arg={}", tree.elsewhere.join("thin.o").display());
    assert!(try_key_of_flags(&unit_args("bin", &tree.deps, &[&thin_object])).is_ok());

    for flag in [
        format!("-Clink-arg={}", tree.elsewhere.join("absent.a").display()),
        "-Clink-arg=relative/libfoo.a".to_string(),
        format!("-Clink-arg={}", tree.elsewhere.join("libthin.a").display()),
    ] {
        assert!(
            try_key_of_flags(&unit_args("bin", &tree.deps, &[&flag])).is_err(),
            "{flag}"
        );
    }
}

/// A linking unit's `static` lib that no `-L` dir holds may come from a
/// system dir the key never sees. A `-L` in the link arguments counts.
#[cfg(not(windows))]
#[test]
fn unresolved_static_lib_refuses_a_linking_unit() {
    let _lock = key_test_lock();
    let tree = native_tree();
    let error =
        try_key_of_flags(&unit_args("bin", &tree.deps, &["-l", "static=nope"])).unwrap_err();
    assert!(
        format!("{error:#}").contains("is in no -L directory"),
        "{error:#}"
    );
    assert!(try_key_of_flags(&unit_args("rlib", &tree.deps, &["-l", "static=nope"])).is_ok());

    let link_search = format!("-Clink-arg=-L{}", tree.elsewhere.display());
    let bin = unit_args("bin", &tree.deps, &[&link_search, "-l", "static=nope"]);
    let (before, after) = keys_around_rewrite(&bin, &tree.elsewhere.join("libnope.a"));
    assert_ne!(before, after, "found through the link argument and keyed");
}

#[test]
fn unresolved_static_lib_is_an_error_only_for_non_msvc_links() {
    assert!(unresolved_static_lib_is_error(true, false));
    assert!(!unresolved_static_lib_is_error(true, true));
    assert!(!unresolved_static_lib_is_error(false, false));
}

#[test]
fn packed_bundled_libs_flag_is_read_in_either_spelling() {
    let flags = |items: &[&str]| {
        items
            .iter()
            .map(|flag| flag.to_string())
            .collect::<Vec<_>>()
    };
    assert!(packs_bundled_libs(&flags(&["packed-bundled-libs"])));
    assert!(packs_bundled_libs(&flags(&[
        "share-generics",
        "packed_bundled_libs=yes"
    ])));
    assert!(!packs_bundled_libs(&flags(&[
        "share-generics=packed-bundled-libs"
    ])));
    assert!(!packs_bundled_libs(&[]));
}

/// An rlib carries a `static` spec's archive unless `-bundle`; it packs
/// one under `+whole-archive` or `-Z packed-bundled-libs`.
#[test]
fn rlib_bundle_follows_bundle_and_packing() {
    let spec = static_lib_spec;
    assert_eq!(rlib_bundle(&spec("static=foo"), true, false), Some(false));
    assert_eq!(
        rlib_bundle(&spec("static:+whole-archive=foo"), true, false),
        Some(true)
    );
    assert_eq!(rlib_bundle(&spec("static=foo"), true, true), Some(true));
    assert_eq!(rlib_bundle(&spec("static:-bundle=foo"), true, false), None);
    assert_eq!(rlib_bundle(&spec("static=foo"), false, false), None);
    assert_eq!(rlib_bundle(&spec("foo"), true, false), None);
}

/// The bundle-audit marker keys rlibs with a native dir apart from
/// entries stored without the audit, and the stash reports what the key
/// hashed and what the rlib carries.
///
/// A dir outside the workspace counts like one inside it (see
/// [`audit_dirs_are_an_rlibs_scanned_dirs`] for system library dirs).
#[test]
fn native_bundle_audit_marks_rlibs_with_a_native_dir() {
    let _lock = key_test_lock();
    let tree = native_tree();
    std::fs::write(tree.out.join("libfoo.a"), b"archive").unwrap();
    std::fs::write(tree.elsewhere.join("libbar.a"), b"archive").unwrap();
    let search = format!("native={}", tree.out.display());
    let outside = format!("native={}", tree.elsewhere.display());
    let marked = |crate_type: &str, extra: &[&str]| {
        key_of_flags(&unit_args(crate_type, &tree.deps, extra));
        take_last_key_fields()
            .unwrap()
            .contains_key("native_bundle_audit")
    };
    assert!(marked("rlib", &["-L", &search]));
    assert!(!marked("rlib", &[]));
    assert!(marked("rlib", &["-L", &outside]));
    assert!(!marked("rlib", &["-L", &search, "--emit=metadata"]));
    #[cfg(not(windows))]
    assert!(!marked("bin", &["-L", &search]));

    key_of_flags(&unit_args(
        "rlib",
        &tree.deps,
        &[
            "-L",
            &search,
            "-L",
            &outside,
            "-L",
            &search,
            "-l",
            "static:+whole-archive=foo",
            "-l",
            "static=bar",
        ],
    ));
    assert_eq!(
        take_last_key_native_archives(),
        Some(KeyedNativeArchives {
            archives: vec![tree.out.join("libfoo.a"), tree.elsewhere.join("libbar.a")],
            bundled: vec![
                BundledArchive {
                    path: tree.out.join("libfoo.a"),
                    packed: true,
                },
                BundledArchive {
                    path: tree.elsewhere.join("libbar.a"),
                    packed: false,
                },
            ],
            dirs: vec![tree.out.clone(), tree.elsewhere.clone()],
        })
    );
    assert_eq!(take_last_key_native_archives(), None, "taken once");
}

/// Rustc's `-O` / `-g` shorthands must share keys with their exact `-C`
/// equivalents rather than living in the unmodeled residual bucket.
#[test]
fn codegen_shorthands_match_explicit_forms() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let base = key_of_flags(&flag_base(&source, &[]));
    let debug = key_of_flags(&flag_base(&source, &["-g"]));
    let opt = key_of_flags(&flag_base(&source, &["-O"]));
    let explicit_debug = key_of_flags(&flag_base(&source, &["-Cdebuginfo=2"]));
    let explicit_opt = key_of_flags(&flag_base(&source, &["-Copt-level=3"]));
    let long_debug = key_of_flags(&flag_base(&source, &["--codegen=debuginfo=2"]));
    let long_opt = key_of_flags(&flag_base(&source, &["--codegen", "opt-level=3"]));
    assert_ne!(base, debug, "`-g` must change the key");
    assert_ne!(base, opt, "`-O` must change the key");
    assert_ne!(debug, opt, "`-g` and `-O` must produce distinct keys");
    assert_eq!(debug, explicit_debug, "`-g` is `-Cdebuginfo=2`");
    assert_eq!(opt, explicit_opt, "`-O` is `-Copt-level=3`");
    assert_eq!(debug, long_debug, "`--codegen` is the long `-C` alias");
    assert_eq!(opt, long_opt, "separated `--codegen` must match `-C`");
}

#[test]
fn codegen_shorthand_override_order_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let shorthand_then_explicit =
        key_of_flags(&flag_base(&source, &["-O", "--codegen=opt-level=0"]));
    let explicit_then_shorthand =
        key_of_flags(&flag_base(&source, &["--codegen", "opt-level=0", "-O"]));

    assert_ne!(
        shorthand_then_explicit, explicit_then_shorthand,
        "rustc applies optimization flags last-wins, so opposite orders must not collide"
    );
}

#[test]
fn frontend_jobs_spellings_share_a_key_and_values_remain_ordered() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let none = key_of_flags(&flag_base(&source, &[]));
    let separated = key_of_flags(&flag_base(&source, &["--jobs-frontend", "16"]));
    let attached = key_of_flags(&flag_base(&source, &["--jobs-frontend=16"]));
    let different = key_of_flags(&flag_base(&source, &["--jobs-frontend=8"]));
    let order_4_8 = key_of_flags(&flag_base(
        &source,
        &["--jobs-frontend=4", "--jobs-frontend=8"],
    ));
    let order_8_4 = key_of_flags(&flag_base(
        &source,
        &["--jobs-frontend=8", "--jobs-frontend=4"],
    ));

    assert_ne!(none, attached, "frontend jobs must affect the key");
    assert_eq!(separated, attached, "both rustc spellings are equivalent");
    assert_ne!(attached, different, "worker count must affect the key");
    assert_ne!(
        order_4_8, order_8_4,
        "repeated last-wins values must preserve argv order"
    );
}

#[test]
fn response_file_flags_share_inline_key_and_track_contents() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let response = dir.path().join("rustc.args");
    let at_response = format!("@{}", response.display());

    std::fs::write(&response, "--cfg\nresponse_v1\n-C\nopt-level=1\n").unwrap();
    let inline = key_of_flags(&flag_base(
        &source,
        &["--cfg", "response_v1", "-C", "opt-level=1"],
    ));
    let response_v1 = key_of_flags(&flag_base(&source, &[&at_response]));
    assert_eq!(
        response_v1, inline,
        "transporting identical flags through @file must not change the key"
    );

    std::fs::write(&response, "--cfg\nresponse_v2\n-C\nopt-level=2\n").unwrap();
    let response_v2 = key_of_flags(&flag_base(&source, &[&at_response]));
    assert_ne!(
        response_v1, response_v2,
        "rewriting the same response-file path must change the effective key"
    );
}

/// kunobi-ninja/kache#324: residual tokens are sorted before folding, so
/// argv order does not perturb the key (the fold is a coarse safety net for
/// unmodeled flags, not an order-sensitive channel).
#[test]
fn residual_args_are_order_independent() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let a = key_of_flags(&flag_base(
        &source,
        &["--unmodeled-a", "alpha", "--unmodeled-b", "beta"],
    ));
    let b = key_of_flags(&flag_base(
        &source,
        &["--unmodeled-b", "beta", "--unmodeled-a", "alpha"],
    ));
    assert_eq!(a, b, "residual argv order must not change the key");
}

/// kunobi-ninja/kache#324: diagnostics / lint / query / already-keyed path
/// flags are stripped during parsing, so they must NOT reach the residual
/// fold and over-key the result. Guards the same invariant as the
/// `key_matrix_*_does_not_change_key` tests for flags cargo passes routinely.
///
/// Outcome-affecting lint configuration is deliberately NOT in this list:
/// every lint level and `--check-cfg` must change the key —
/// see `key_matrix_outcome_lint_configuration_changes_key`.
#[test]
fn residual_strips_diagnostic_and_query_flags() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let base = key_of_flags(&flag_base(&source, &[]));
    for extra in [
        vec!["--diagnostic-width=80"],
        vec!["--json=artifacts"],
        vec!["--color", "always"],
        vec!["--verbose"],
    ] {
        assert_eq!(
            base,
            key_of_flags(&flag_base(&source, &extra)),
            "diagnostics/query flag {extra:?} must not change the key"
        );
    }
}

/// kunobi-ninja/kache#399: lexical `.`/`..` collapse + separator unification.
/// Output uses the host separator (so it lines up with canonical rule
/// prefixes); built with MAIN_SEPARATOR so the test is host-independent.
#[test]
fn lexically_resolve_path_collapses_dot_dot() {
    let s = std::path::MAIN_SEPARATOR_STR;
    let j = |parts: &[&str]| parts.join(s);

    // Unix-style absolute.
    assert_eq!(lexically_resolve_path("/a/b/../c"), j(&["", "a", "c"]));
    assert_eq!(lexically_resolve_path("/a/./b"), j(&["", "a", "b"]));
    // `..` cannot escape the root.
    assert_eq!(lexically_resolve_path("/../a"), j(&["", "a"]));

    // Windows drive + mixed separators + unresolved `..` (the #399 input
    // shape: a relative CARGO_TARGET_DIR joined literally with `/`).
    assert_eq!(
        lexically_resolve_path(r"C:\proj\pkg\..\oot-target\x"),
        format!("C:{}", j(&["", "proj", "oot-target", "x"]))
    );
    assert_eq!(
        lexically_resolve_path(r"C:\u\src\../oot-target\rel\deps"),
        format!("C:{}", j(&["", "u", "oot-target", "rel", "deps"]))
    );

    // Relative paths keep a leading `..`.
    assert_eq!(lexically_resolve_path("../a/b"), j(&["..", "a", "b"]));
    assert_eq!(lexically_resolve_path("."), ".");

    // Already-resolved paths are unchanged on the host (the Linux case).
    let resolved = format!("{}home{}u{}oot{}out", s, s, s, s);
    assert_eq!(lexically_resolve_path(&resolved), resolved);
}

/// kunobi-ninja/kache#399 (the core property): two out-of-tree build paths
/// that differ only in the package-dir component cancelled by `..` resolve
/// so the suffix below their respective roots is identical. That suffix is
/// what survives after the workspace-root prefix is stripped, so the cache
/// key converges across build locations.
#[test]
fn lexically_resolve_path_makes_out_of_tree_suffix_converge() {
    // Cold and a relocate (different drive subtree, different package-dir
    // name) of the same out-of-tree build. The `..` cancels the package dir,
    // so oot-target attaches directly to the package's parent (= the
    // workspace root). The segment from oot-target onward is then identical,
    // which is what survives after the <WORKSPACE> prefix is stripped.
    let cold = lexically_resolve_path(r"C:\proj\scenario\source\..\oot-target\release\build\x\out");
    let reloc = lexically_resolve_path(r"C:\Temp\.tmpAB\..\oot-target\release\build\x\out");
    assert!(!cold.contains(".."), "unresolved .. in {cold}");
    assert!(!reloc.contains(".."), "unresolved .. in {reloc}");
    let from_oot = |p: &str| p[p.find("oot-target").unwrap()..].to_string();
    assert_eq!(from_oot(&cold), from_oot(&reloc));
    let s = std::path::MAIN_SEPARATOR_STR;
    assert_eq!(
        from_oot(&cold),
        ["oot-target", "release", "build", "x", "out"].join(s)
    );
}

/// kunobi-ninja/kache#399 end-to-end at the env-dep layer: an out-of-tree
/// OUT_DIR that arrives with an unresolved `..` (as Windows cargo leaves it
/// for a relative CARGO_TARGET_DIR) must normalize to the same
/// <WORKSPACE>-anchored sentinel regardless of absolute build location, so
/// the key converges and a relocated build hits. Regression for the bug
/// where `normalize_env_dep_value` returned `Unchanged` before resolving:
/// the raw `..`-bearing path matched no canonical rule prefix, so the build
/// location leaked into the key and only the resolved-then-normalized form
/// (matching the rules' own `canonical_string`) converges.
#[test]
fn out_of_tree_out_dir_env_dep_converges_across_locations() {
    let _lock = key_test_lock();

    // Build a real out-of-tree OUT_DIR under `root` with a `..` in the
    // path (root/pkg/../oot-target/...) and return its normalized env-dep
    // value, anchoring <WORKSPACE> at the oot-target dir.
    fn normalized(root: &std::path::Path) -> String {
        let target = root.join("oot-target");
        let out = target
            .join("release")
            .join("build")
            .join("pkg-0000000000000000")
            .join("out");
        std::fs::create_dir_all(&out).unwrap();
        // `pkg` must exist for canonicalize to traverse `pkg/..`.
        std::fs::create_dir_all(root.join("pkg")).unwrap();
        let generated = out.join("generated.rs");
        // Path-only include payload (no `env!("OUT_DIR")` runtime use), and
        // a crate root whose include proves the locator use, so the value
        // is safe to normalize.
        std::fs::write(&generated, b"pub fn marker() -> u8 { 7 }\n").unwrap();
        let lib = root.join("pkg").join("lib.rs");
        std::fs::write(
            &lib,
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
        )
        .unwrap();

        // The value as Windows cargo hands it over: the package dir is
        // cancelled by a literal `..` rather than pre-resolved.
        let value = root
            .join("pkg")
            .join("..")
            .join("oot-target")
            .join("release")
            .join("build")
            .join("pkg-0000000000000000")
            .join("out")
            .to_string_lossy()
            .into_owned();

        let pn = PathNormalizer::from_env(Some(&target));
        normalize_env_dep_value("test_crate", "OUT_DIR", &value, &[lib, generated], &pn).value
    }

    let cold = tempfile::tempdir().unwrap();
    let reloc = tempfile::tempdir().unwrap();
    let v_cold = normalized(cold.path());
    let v_reloc = normalized(reloc.path());

    assert_eq!(
        v_cold, v_reloc,
        "out-of-tree OUT_DIR must normalize identically across build locations"
    );
    assert!(
        v_cold.contains("<WORKSPACE>"),
        "expected the workspace sentinel, got `{v_cold}`"
    );
    assert!(
        !v_cold.contains(".."),
        "the `..` must be resolved away, got `{v_cold}`"
    );
}

/// H1: a build-script native search path must diverge the key, but
/// cargo's redundant `-L dependency=` (covered by content-hashed
/// `--extern`) must NOT — else every target-dir move busts the cache.
/// Generic `-l` keying, checked on hosts that do not probe native MSVC
/// inputs. On a Windows host the library must exist (fail closed), so the
/// Windows shape lives in the `windows_*` tests with an injected probe.
#[cfg(not(windows))]
#[test]
fn link_search_native_keys_but_dependency_does_not() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let a = key_of(&flag_base(&source, &["-L", "native=/opt/a/lib"]));
    let b = key_of(&flag_base(&source, &["-L", "native=/opt/b/lib"]));
    assert_ne!(a, b, "a different native -L must change the key");

    let dep_x = key_of(&flag_base(&source, &["-L", "dependency=/x/deps"]));
    let dep_y = key_of(&flag_base(&source, &["-L", "dependency=/y/deps"]));
    assert_eq!(
        dep_x, dep_y,
        "cargo's -L dependency= must not affect the key"
    );
}

/// With Cargo's new build-dir layout, a `-sys` crate's
/// `cargo:rustc-link-search=native=$OUT_DIR` reaches every dependent as
/// `<target>/<profile>/build/<pkg>/<hash>/out`. The target directory
/// derived from the dependent's own `--out-dir` must cover it, or the
/// checkout path stays in the key and the dependent misses in every other
/// checkout. The unit is an rlib, as such dependents usually are: a linked
/// output would run the native Windows MSVC link probe, which fails closed
/// on the made-up search directories.
#[test]
fn link_search_into_new_layout_out_dir_is_the_same_in_every_checkout() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let key_in = |checkout: &str| {
        let target = checkout_target(checkout);
        let out_dir = format!("{target}/debug/build/mylib/0123456789abcdef/out");
        let native = format!("native={target}/debug/build/foo-sys/fedcba9876543210/out");
        let argv: Vec<String> = [
            "rustc",
            "--crate-name",
            "mylib",
            &source.to_string_lossy(),
            "--crate-type",
            "lib",
            "--out-dir",
            &out_dir,
            "-L",
            &native,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let args = RustcArgs::parse(&argv).unwrap();
        let pn = PathNormalizer::empty().with_target_dir(args.target_dir().as_deref());
        compute_cache_key(&args, &FileHasher::new(), &pn, &KeyEnv::default()).unwrap()
    };

    assert_eq!(key_in("checkout-a"), key_in("checkout-b"));
}

/// Executable (`bin`) outputs key the linker identity (a different linker
/// can produce a different binary). A resolvable `-Clinker` is folded in;
/// an unresolvable one isn't. Exercises compute_cache_key's
/// is_executable_output() linker branch (716-723) + get_linker_identity.
///
/// Unix-only: the test relies on `cc` resolving on PATH (it folds `cc
/// --version`), which isn't guaranteed on the Windows CI runner — there
/// both linkers fail to resolve and the keys match. The branch is still
/// covered on Linux/macOS CI.
#[cfg(unix)]
#[test]
fn bin_output_keys_linker_identity() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("main.rs");
    std::fs::write(&source, b"fn main() {}").unwrap();
    let bin = |extra: &[&str]| {
        let mut v = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "app".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "bin".to_string(),
        ];
        v.extend(extra.iter().map(|s| s.to_string()));
        v
    };

    // A resolvable linker (cc on PATH) folds its version and, on Linux,
    // CRT identity. An unresolvable linker cannot place CRT/startup
    // objects, so the invocation is uncacheable rather than keyed with
    // an empty runtime identity.
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key = |args: Vec<String>| {
        let mut parsed = RustcArgs::parse(&args).unwrap();
        parsed.source_file = None;
        compute_cache_key(&parsed, &fh, &pn, &KeyEnv::default())
    };
    let with_cc = key(bin(&["-Clinker=cc"]));
    let with_missing = key(bin(&["-Clinker=/nonexistent/kache-linker-xyz"]));
    match (with_cc, with_missing) {
        (Ok(cc), Ok(missing)) => {
            assert_ne!(cc, missing, "linker choice must affect a bin's cache key");
            assert_eq!(cc, key(bin(&["-Clinker=cc"])).unwrap());
        }
        (Ok(_), Err(_)) => {}
        (Err(_), Err(_)) => {}
        (Err(err), Ok(_)) => {
            panic!("unresolvable linker produced a key while cc failed: {err:#}")
        }
    }
}

/// A readable `--extern name=path` rlib is content-hashed into the key (not
/// path-hashed): the same path with different artifact bytes must diverge.
/// Exercises compute_cache_key's extern Ok(dep_hash) branch (552-557).
#[test]
fn extern_artifact_content_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let dep = dir.path().join("libdep.rlib");
    let extern_arg = format!("foo={}", dep.to_str().unwrap());

    std::fs::write(&dep, b"rlib content A").unwrap();
    let key_a = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));

    std::fs::write(&dep, b"rlib content B (different)").unwrap();
    let key_b = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));
    assert_ne!(
        key_a, key_b,
        "extern artifact content must change the key (content-hashed)"
    );

    std::fs::write(&dep, b"rlib content A").unwrap();
    let key_a2 = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));
    assert_eq!(key_a, key_a2, "same extern content -> same key");
}

/// H1: `-Z` codegen flags arriving on argv must be keyed.
#[test]
fn fold_field_is_unambiguous_across_value_boundaries() {
    // kunobi-ninja/kache#324: length-prefixing free-text key fields removes
    // delimiter/boundary ambiguity that the old `\n`/`=` form allowed.
    let h = |parts: &[(&[u8], &[u8])]| {
        let mut hasher = blake3::Hasher::new();
        for (l, v) in parts {
            fold_field(&mut hasher, l, v);
        }
        hasher.finalize().to_hex().to_string()
    };

    // Same label, value bytes shifted across the boundary: ("a","bc") vs
    // ("ab","c") must not collide.
    assert_ne!(
        h(&[(b"x:", b"a"), (b"x:", b"bc")]),
        h(&[(b"x:", b"ab"), (b"x:", b"c")]),
    );

    // The exact old-encoding collision: a single cfg value that embeds the
    // `\n` delimiter must not equal two separate cfgs.
    assert_ne!(
        h(&[(b"cfg:", b"a\ncfg:b")]),
        h(&[(b"cfg:", b"a"), (b"cfg:", b"b")]),
    );
}

#[test]
fn unstable_flag_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let none = key_of_flags(&flag_base(&source, &[]));
    let san = key_of_flags(&flag_base(&source, &["-Z", "sanitizer=address"]));
    assert_ne!(none, san, "a -Z codegen flag must change the key");
    assert_eq!(
        san,
        key_of_flags(&flag_base(&source, &["-Zsanitizer=address"]))
    );
}

/// A `--target` value can be a path to a custom target JSON spec (Firefox /
/// embedded toolchains). Its file CONTENT — data-layout, target features,
/// linker, panic strategy — must be folded into the key, so the same path
/// with different content diverges. Exercises compute_cache_key's
/// `target_path.is_file()` spec-hashing branch.
#[test]
fn custom_target_spec_file_content_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let spec = dir.path().join("my-target.json");
    let spec_arg = format!("--target={}", spec.to_str().unwrap());

    std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-A"}"#).unwrap();
    let key_a = key_of_flags(&flag_base(&source, &[&spec_arg]));

    // Same --target path, different spec content -> different key.
    std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-B"}"#).unwrap();
    let key_b = key_of_flags(&flag_base(&source, &[&spec_arg]));
    assert_ne!(
        key_a, key_b,
        "custom target spec file content must change the key"
    );

    // Restoring the original content reproduces the original key.
    std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-A"}"#).unwrap();
    let key_a2 = key_of_flags(&flag_base(&source, &[&spec_arg]));
    assert_eq!(key_a, key_a2, "same spec content -> same key");
}

/// A trusted codegen backend dylib is keyed by content: a rebuild in place
/// changes the key, the same bytes at another checkout's path do not.
/// A toolchain backend name stays keyed as written.
#[test]
fn codegen_backend_dylib_is_keyed_by_content_not_path() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let clone_a = dir.path().join("a/librustc_codegen_x.so");
    let clone_b = dir.path().join("b/librustc_codegen_x.so");
    for backend in [&clone_a, &clone_b] {
        std::fs::create_dir_all(backend.parent().unwrap()).unwrap();
        std::fs::write(backend, b"backend-v1").unwrap();
    }
    let flag = |backend: &Path| format!("-Zcodegen-backend={}", backend.display());

    let a = key_of_flags(&flag_base(&source, &[&flag(&clone_a)]));
    let b = key_of_flags(&flag_base(&source, &[&flag(&clone_b)]));
    assert_eq!(a, b, "identical backends at different paths share a key");

    std::fs::write(&clone_a, b"backend-v2").unwrap();
    let rebuilt = key_of_flags(&flag_base(&source, &[&flag(&clone_a)]));
    assert_ne!(a, rebuilt, "a rebuilt backend must change the key");

    let cranelift = key_of_flags(&flag_base(&source, &["-Zcodegen-backend=cranelift"]));
    let gcc = key_of_flags(&flag_base(&source, &["-Zcodegen-backend=gcc"]));
    assert_ne!(cranelift, gcc, "toolchain backend names stay keyed");
    assert_ne!(cranelift, a);

    let missing = dir.path().join("missing.so");
    let mut parsed = RustcArgs::parse(&flag_base(&source, &[&flag(&missing)])).unwrap();
    parsed.source_file = None;
    assert!(
        compute_cache_key(
            &parsed,
            &FileHasher::new(),
            &PathNormalizer::empty(),
            &KeyEnv::default()
        )
        .is_err(),
        "an unreadable backend must not produce a key"
    );
}

/// H2: `--sysroot` selects which std rustc links against; with the
/// same rustc version, a different sysroot must diverge the key.
#[test]
fn sysroot_changes_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let a = key_of_flags(&flag_base(&source, &["--sysroot", "/opt/std-a"]));
    let b = key_of_flags(&flag_base(&source, &["--sysroot", "/opt/std-b"]));
    let none = key_of_flags(&flag_base(&source, &[]));
    assert_ne!(a, b, "a different --sysroot must change the key");
    assert_ne!(none, a, "adding --sysroot must change the key");
    assert_eq!(
        a,
        key_of_flags(&flag_base(&source, &["--sysroot=/opt/std-a"]))
    );
}

/// H3: a `--target` custom JSON spec must be keyed by its CONTENTS,
/// so editing the spec in place diverges the key (path string alone
/// would not).
#[test]
fn target_spec_contents_change_key() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let spec = dir.path().join("custom.json");

    std::fs::write(&spec, br#"{"llvm-target":"x86_64","data-layout":"e-m:e"}"#).unwrap();
    let args = flag_base(&source, &["--target", &spec.to_string_lossy()]);
    let before = key_of_flags(&args);

    // Edit the spec in place — same path, different codegen contract.
    std::fs::write(
        &spec,
        br#"{"llvm-target":"x86_64","data-layout":"DIFFERENT"}"#,
    )
    .unwrap();
    let after = key_of_flags(&args);
    assert_ne!(before, after, "editing the target spec must change the key");
}

#[test]
fn test_cache_key_changes_with_source() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");

    // First version
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let parsed1 = RustcArgs::parse(&args_vec).unwrap();
    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();

    // Modified source
    std::fs::write(&source, b"pub fn hello() { println!(\"hi\"); }").unwrap();
    let parsed2 = RustcArgs::parse(&args_vec).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(key1, key2);
}

#[test]
fn test_unreadable_dep_produces_stable_key() {
    let _lock = key_test_lock();
    // Simulate unreadable deps (sysroot crates) from two different paths —
    // the cache key should be identical because we use a sentinel, not the path.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    // Create two "dep" paths that both point to non-existent files (will fail hash_file)
    let dep_a = std::path::PathBuf::from("/home/runner/.rustup/toolchains/stable/lib/libstd.rlib");
    let dep_b = std::path::PathBuf::from("/Users/dev/.rustup/toolchains/stable/lib/libstd.rlib");

    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];

    let mut parsed_a = RustcArgs::parse(&args_vec).unwrap();
    parsed_a.externs.push(crate::args::ExternDep {
        name: "std".to_string(),
        path: Some(dep_a),
    });

    let mut parsed_b = RustcArgs::parse(&args_vec).unwrap();
    parsed_b.externs.push(crate::args::ExternDep {
        name: "std".to_string(),
        path: Some(dep_b),
    });

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key_a = compute_cache_key(&parsed_a, &fh, &pn, &KeyEnv::default()).unwrap();
    let key_b = compute_cache_key(&parsed_b, &fh, &pn, &KeyEnv::default()).unwrap();
    assert_eq!(
        key_a, key_b,
        "unreadable deps with different paths should produce the same key"
    );
}

/// [`path_is_only_used_for_includes`] with both sides in probe form, as the
/// key computation calls it.
fn locates_includes(value: &str, source_files: &[PathBuf]) -> bool {
    let paths = EnvDepPaths::new(None, source_files);
    path_is_only_used_for_includes(EnvDepValue::new(value).probe(), paths.source_probes())
}

#[test]
fn path_is_only_used_for_includes_detects_include_pattern() {
    // serde-style include!() puts a build.rs-generated file into
    // dep-info source_files. The OUT_DIR value is the parent dir
    // of that file → safe to normalize.
    let dir = tempfile::tempdir().unwrap();
    let out_dir = dir.path().join("build/serde-abc/out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let included = out_dir.join("private.rs");
    std::fs::write(&included, b"// generated").unwrap();
    let source_files = vec![std::path::PathBuf::from("/src/lib.rs"), included.clone()];
    assert!(
        locates_includes(out_dir.to_str().unwrap(), &source_files),
        "OUT_DIR contains an included source file → safe to normalize"
    );
}

#[test]
fn path_is_only_used_for_includes_rejects_env_value_pattern() {
    // out-dir-runtime fixture: const X: &str = env!("OUT_DIR");
    // No source file under OUT_DIR → conservatively keep
    // absolute so cache keys diverge across worktrees.
    let dir = tempfile::tempdir().unwrap();
    let out_dir = dir.path().join("build/foo/out");
    std::fs::create_dir_all(&out_dir).unwrap();
    // dep-info source_files contains only the crate root,
    // nothing under OUT_DIR.
    let source_files = vec![std::path::PathBuf::from("/src/main.rs")];
    assert!(
        !locates_includes(out_dir.to_str().unwrap(), &source_files),
        "no source under OUT_DIR → unsafe to normalize"
    );
}

#[test]
fn path_is_only_used_for_includes_handles_macos_symlink_form() {
    // The same canonicalization concern that motivated
    // PathNormalizer: on macOS the OUT_DIR value may be in
    // /private/tmp/... form while source paths report /tmp/...
    // (or vice versa). Canonical-path comparison must succeed
    // either way.
    if !cfg!(target_os = "macos") {
        return;
    }
    let unique = format!("kache-cache-key-test-{}", std::process::id());
    let real_out = std::path::Path::new("/tmp").join(&unique).join("out");
    std::fs::create_dir_all(&real_out).unwrap();
    let included = real_out.join("private.rs");
    std::fs::write(&included, b"// generated").unwrap();

    // OUT_DIR comes from cargo as /private/tmp/... form
    let out_dir_value = format!("/private/tmp/{unique}/out");
    // source_files reports /tmp/... (the symlink form)
    let source_files = vec![included];

    let result = locates_includes(&out_dir_value, &source_files);
    let _ = std::fs::remove_dir_all(std::path::Path::new("/tmp").join(&unique));
    assert!(
        result,
        "canonical-path comparison must see through the symlink"
    );
}

#[test]
fn out_dir_probe_needs_an_absolute_out_dir() {
    assert_eq!(out_dir_probe(None), None);
    assert_eq!(out_dir_probe(Some(OsStr::new(""))), None);
    assert_eq!(
        out_dir_probe(Some(OsStr::new("kache-no-such-dir/out"))),
        None
    );
    let dir = tempfile::tempdir().unwrap();
    let canonical = dir.path().canonicalize().unwrap();
    assert_eq!(
        out_dir_probe(Some(dir.path().as_os_str())),
        Some(canonical.clone())
    );
    // An absolute OUT_DIR that does not exist anchors in its raw form.
    let missing = canonical.join("missing/out");
    assert_eq!(out_dir_probe(Some(missing.as_os_str())), Some(missing));
}

#[test]
fn out_dir_suffix_is_relative_to_out_dir_by_components() {
    let out = Path::new("/o/out");
    let suffix = |value: &str| out_dir_relative_suffix(Path::new(value), out);
    assert_eq!(suffix("/o/out"), Some(String::new()));
    assert_eq!(suffix("/o/out/gen/x.rs"), Some("gen/x.rs".to_string()));
    assert_eq!(suffix("/o/out2/x.rs"), None);
    assert_eq!(suffix("/o"), None);
    // A backslash separates on Windows and is part of a name on Unix; the
    // suffix spells it `/` either way.
    assert_eq!(suffix("/o/out/gen\\x.rs"), Some("gen/x.rs".to_string()));
}

#[test]
fn env_dep_paths_anchor_only_on_a_usable_out_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().canonicalize().unwrap().join("pkg-1/out");
    std::fs::create_dir_all(&out).unwrap();
    let generated = out.join("consts.rs");
    std::fs::write(&generated, b"").unwrap();
    let generated = generated.to_str().unwrap();
    let suffix = |out_dir: Option<&OsStr>, value: &str| {
        EnvDepPaths::new(out_dir, &[]).out_dir_suffix(&EnvDepValue::new(value))
    };

    let out_dir = Some(out.as_os_str());
    assert_eq!(suffix(out_dir, generated), Some("consts.rs".to_string()));
    assert_eq!(suffix(out_dir, out.to_str().unwrap()), Some(String::new()));
    assert_eq!(suffix(None, generated), None);
    assert_eq!(suffix(Some(OsStr::new("pkg-1/out")), generated), None);
    // A value that does not exist compares in its raw form.
    let gone = out.join("gone.rs");
    assert_eq!(
        suffix(out_dir, gone.to_str().unwrap()),
        Some("gone.rs".to_string())
    );

    assert_eq!(out_dir_unit(out_dir), "pkg-1");
    assert_eq!(out_dir_unit(Some(OsStr::new("out"))), "");
    assert_eq!(out_dir_unit(None), "");
}

#[cfg(unix)]
#[test]
fn env_dep_paths_see_through_a_symlinked_out_dir() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().canonicalize().unwrap().join("real");
    let out = real.join("pkg-1/out");
    std::fs::create_dir_all(&out).unwrap();
    let generated = out.join("consts.rs");
    std::fs::write(&generated, b"").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let linked_out = link.join("pkg-1/out");
    let linked_generated = linked_out.join("consts.rs");

    // OUT_DIR through the link and the value real, then the reverse.
    let via_link = EnvDepPaths::new(Some(linked_out.as_os_str()), &[]);
    assert_eq!(
        via_link.out_dir_suffix(&EnvDepValue::new(generated.to_str().unwrap())),
        Some("consts.rs".to_string())
    );
    let direct = EnvDepPaths::new(Some(out.as_os_str()), &[]);
    assert_eq!(
        direct.out_dir_suffix(&EnvDepValue::new(linked_generated.to_str().unwrap())),
        Some("consts.rs".to_string())
    );
    // The unit name comes from OUT_DIR as given.
    assert_eq!(out_dir_unit(via_link.out_dir), "pkg-1");

    // Source files resolve through the link too.
    assert!(locates_includes(out.to_str().unwrap(), &[linked_generated]));
    assert!(locates_includes(linked_out.to_str().unwrap(), &[generated]));
}

#[test]
fn source_probes_fall_back_to_the_raw_path() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let out = root.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let sources = [root.join("src/lib.rs"), out.join("gone.rs")];

    let paths = EnvDepPaths::new(None, &sources);
    assert_eq!(paths.source_probes(), &sources);
    let out = out.to_str().unwrap();
    assert!(locates_includes(out, &sources));
    assert!(!locates_includes(out, &sources[..1]));
}

/// A value that does not exist but resolves lexically to a path under
/// OUT_DIR is compared in its resolved form, not as written.
#[cfg(unix)]
#[test]
fn a_missing_value_compares_in_its_resolved_form() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().canonicalize().unwrap().join("pkg-1/out");
    std::fs::create_dir_all(&out).unwrap();
    let value = format!("{}/sub/../gen.rs", out.display());
    let pn = PathNormalizer::empty().with_path_only_env_vars(vec!["c:GEN".to_string()]);

    let dep = normalize_env_dep_value_in("c", "GEN", &value, &[], &pn, Some(out.as_os_str()));
    assert_eq!(dep.decision, EnvDepNormalizationDecision::ForcedPathOnly);
    assert_eq!(dep.value, "<OUT_DIR:pkg-1>/gen.rs");
}

#[test]
fn source_env_dep_use_detector_allows_include_locators() {
    let source = r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
include_str!(concat!(env ! ( "OUT_DIR" ), "/template.txt"));
include_bytes!(env!("BLOB_PATH"));
"#;
    assert_eq!(
        source_env_dep_use(source, "OUT_DIR"),
        SourceEnvDepUse::IncludeLocator
    );
    assert_eq!(
        source_env_dep_use(source, "BLOB_PATH"),
        SourceEnvDepUse::IncludeLocator
    );
}

#[test]
fn source_env_dep_use_detector_rejects_runtime_values() {
    let source = r#"
const OUT_DIR: &str = env!("OUT_DIR");
const MAYBE_OUT_DIR: Option<&str> = option_env!("OUT_DIR");
const PATH: &str = concat!(env!("OUT_DIR"), "/data.txt");
"#;
    assert_eq!(
        source_env_dep_use(source, "OUT_DIR"),
        SourceEnvDepUse::RuntimeValue
    );
}

#[test]
fn source_env_dep_use_detector_rejects_dual_pattern() {
    let source = r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");
"#;
    assert_eq!(
        source_env_dep_use(source, "OUT_DIR"),
        SourceEnvDepUse::RuntimeValue
    );
}

#[test]
fn source_env_dep_use_detector_ignores_comments_and_strings() {
    let source = r##"
// const X: &str = env!("OUT_DIR");
/* const Y: &str = env!("OUT_DIR"); */
const TEXT: &str = "env!(\"OUT_DIR\")";
const RAW: &str = r#"env!("OUT_DIR")"#;
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
"##;
    assert_eq!(
        source_env_dep_use(source, "OUT_DIR"),
        SourceEnvDepUse::IncludeLocator
    );
}

#[test]
fn env_dep_normalization_decision_trace_labels_are_stable() {
    for (decision, expected) in [
        (EnvDepNormalizationDecision::Unchanged, "unchanged"),
        (
            EnvDepNormalizationDecision::NormalizedPathOnly,
            "normalized path-only",
        ),
        (
            EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly,
            "kept absolute: not a path-only var",
        ),
        (
            EnvDepNormalizationDecision::KeptAbsoluteManifestDir,
            "kept absolute: CARGO_MANIFEST_DIR is never path-only",
        ),
        (
            EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
            "kept absolute: no include proof",
        ),
        (
            EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse,
            "kept absolute: value use in source",
        ),
        (
            EnvDepNormalizationDecision::KeptAbsoluteScanError,
            "kept absolute: source scan failed",
        ),
        (
            EnvDepNormalizationDecision::ForcedPathOnly,
            "forced path-only (user-asserted)",
        ),
        (
            EnvDepNormalizationDecision::AliasedOutDir,
            "aliased OUT_DIR",
        ),
    ] {
        assert_eq!(decision.as_str(), expected);
    }
}

/// The same unit in two checkouts gets two OUT_DIRs. Pointed at one
/// shared alias, both its OUT_DIR and a `rustc-env` var under it key the
/// same, and never through a sentinel.
#[test]
fn aliased_out_dir_values_key_the_same_in_every_checkout() {
    let alias = Path::new("/cache/out-dirs/v1/d/dmac-0123456789abcdef/out");
    // A rule over the cache dir must not turn the alias into a sentinel.
    let pn = PathNormalizer::empty().with_base_dirs(&["/cache".to_string()]);
    assert_ne!(
        pn.normalize(alias.to_str().unwrap()),
        alias.to_str().unwrap()
    );
    let keyed = |checkout: &str| {
        let real = PathBuf::from(format!(
            "/{checkout}/target/debug/build/dmac-0123456789abcdef/out"
        ));
        let env = vec![
            ("OUT_DIR".into(), real.clone().into_os_string()),
            ("DEBUG_OUTPUT_DIR".into(), real.clone().into_os_string()),
        ];
        let rewrites = crate::out_dir_alias::env_rewrites(&env, &[], &[&real], alias)
            .expect("nothing else mentions OUT_DIR");
        rewrites
            .iter()
            .map(|(var, value)| {
                normalize_env_dep_value_with_hasher(
                    "dmac",
                    var.to_str().unwrap(),
                    &EnvDepValue::new(value.to_str().unwrap()),
                    &EnvDepPaths::new(None, &[]),
                    &FileHasher::new(),
                    &pn,
                    Some(alias),
                )
            })
            .collect::<Vec<_>>()
    };
    let a = keyed("a");
    let b = keyed("b");
    assert_eq!(a, b);
    assert_eq!(a.len(), 2);
    for dep in &a {
        assert_eq!(dep.decision, EnvDepNormalizationDecision::AliasedOutDir);
        assert_eq!(dep.value, alias.to_str().unwrap());
    }
}

#[test]
fn only_values_at_or_under_the_alias_take_the_aliased_decision() {
    let alias = Path::new("/c/d/out");
    let pn = PathNormalizer::empty();
    let decide = |value: &str, aliased: Option<&Path>| {
        normalize_env_dep_value_with_hasher(
            "dmac",
            "V",
            &EnvDepValue::new(value),
            &EnvDepPaths::new(None, &[]),
            &FileHasher::new(),
            &pn,
            aliased,
        )
        .decision
    };
    assert_eq!(
        decide("/c/d/out/gen.rs", Some(alias)),
        EnvDepNormalizationDecision::AliasedOutDir
    );
    assert_ne!(
        decide("/c/d/out2", Some(alias)),
        EnvDepNormalizationDecision::AliasedOutDir
    );
    assert_ne!(
        decide("/c/d/out", None),
        EnvDepNormalizationDecision::AliasedOutDir
    );
    assert!(value_at_or_under("/c/d/out", alias));
    assert!(value_at_or_under("/c/d/out/", alias));
    assert!(!value_at_or_under("/c/d", alias));
    assert!(!value_at_or_under("x/c/d/out", alias));
}

#[test]
fn env_dep_bakes_out_dir_when_an_out_dir_path_stays_literal() {
    use EnvDepNormalizationDecision as D;
    let literal = [
        D::Unchanged,
        D::KeptAbsoluteNotPathOnly,
        D::KeptAbsoluteManifestDir,
        D::KeptAbsoluteNoIncludeProof,
        D::KeptAbsoluteRuntimeUse,
        D::KeptAbsoluteScanError,
        D::AliasedOutDir,
    ];
    for decision in literal {
        assert!(decision.keeps_literal_value(), "{decision:?}");
        assert!(env_dep_bakes_out_dir("OUT_DIR", decision, || false));
        assert!(env_dep_bakes_out_dir("GEN", decision, || true));
        assert!(!env_dep_bakes_out_dir("GEN", decision, || false));
    }
    for decision in [D::NormalizedPathOnly, D::ForcedPathOnly] {
        assert!(!decision.keeps_literal_value(), "{decision:?}");
        assert!(!env_dep_bakes_out_dir("OUT_DIR", decision, || true));
    }
}

/// The key says whether it kept an OUT_DIR path, and a later key that
/// did not starts clean.
#[test]
fn key_computation_stashes_whether_it_bakes_out_dir() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, "pub fn f() {}").unwrap();
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "helper".to_string(),
        source.to_str().unwrap().to_string(),
    ])
    .unwrap();
    let key_with = |env_deps: Vec<(String, String)>| {
        provide_dep_info(DepInfo {
            source_files: vec![source.clone()],
            env_deps,
        });
        compute_cache_key(
            &args,
            &FileHasher::new(),
            &PathNormalizer::empty(),
            &KeyEnv::default(),
        )
        .unwrap();
    };

    key_with(vec![("OUT_DIR".into(), "/nowhere/out".into())]);
    assert!(take_last_key_bakes_out_dir());
    assert!(!take_last_key_bakes_out_dir(), "taken, not copied");

    key_with(vec![("CARGO_PKG_NAME".into(), "helper".into())]);
    assert!(!take_last_key_bakes_out_dir());

    // Any one baking value is enough, however many there are.
    key_with(vec![
        ("OUT_DIR".into(), "/nowhere/out".into()),
        ("OUT_DIR".into(), "/nowhere/out".into()),
    ]);
    assert!(take_last_key_bakes_out_dir());

    key_with(vec![("OUT_DIR".into(), "/nowhere/out".into())]);
    let mut no_source = args.clone();
    no_source.source_file = None;
    compute_cache_key(
        &no_source,
        &FileHasher::new(),
        &PathNormalizer::empty(),
        &KeyEnv::default(),
    )
    .unwrap();
    assert!(!take_last_key_bakes_out_dir(), "reset by the next key");
}

/// The unit's OUT_DIR comes from the snapshot: a value under it is baked
/// only when the snapshot names that OUT_DIR.
#[test]
fn key_reads_out_dir_from_the_snapshot() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, "pub fn f() {}").unwrap();
    let args = RustcArgs::parse(&[
        "rustc".to_string(),
        "--crate-name".to_string(),
        "helper".to_string(),
        source.to_str().unwrap().to_string(),
    ])
    .unwrap();
    // Absent directories, so both sides compare as given; absolute on
    // every platform, or OUT_DIR anchors nothing.
    let out_dir = dir.path().join("absent-out");
    let generated = out_dir.join("gen.rs").to_str().unwrap().to_string();
    let bakes = |env: &KeyEnv| {
        provide_dep_info(DepInfo {
            source_files: vec![source.clone()],
            env_deps: vec![("GEN".into(), generated.clone())],
        });
        compute_cache_key(&args, &FileHasher::new(), &PathNormalizer::empty(), env).unwrap();
        take_last_key_bakes_out_dir()
    };
    let out_env = |path: &Path| KeyEnv::from_parts([("OUT_DIR", path)], None);
    assert!(bakes(&out_env(&out_dir)));
    assert!(!bakes(&KeyEnv::default()));
    assert!(!bakes(&out_env(&dir.path().join("elsewhere"))));
}

#[test]
fn env_dep_policy_normalizes_out_dir_include_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let src = workspace.join("src");
    let out_dir = workspace.join("target/debug/build/pkg/out");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    let lib = src.join("lib.rs");
    std::fs::write(
        &lib,
        r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
    )
    .unwrap();
    let included = out_dir.join("generated.rs");
    std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();

    let source_files = vec![lib, included];
    let path_normalizer = PathNormalizer::from_env(Some(&workspace));
    let out_dir_value = out_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let env_dep = normalize_env_dep_value(
        "test_crate",
        "OUT_DIR",
        &out_dir_value,
        &source_files,
        &path_normalizer,
    );

    assert_eq!(
        env_dep.decision,
        EnvDepNormalizationDecision::NormalizedPathOnly
    );
    assert_ne!(env_dep.value, out_dir_value);
    assert!(
        env_dep.value.contains("<WORKSPACE>"),
        "OUT_DIR include pattern should normalize to the workspace sentinel: {env_dep:?}"
    );
}

#[test]
fn env_dep_policy_keeps_out_dir_dual_pattern_absolute() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let src = workspace.join("src");
    let out_dir = workspace.join("target/debug/build/pkg/out");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    let lib = src.join("lib.rs");
    std::fs::write(
        &lib,
        r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");
"#,
    )
    .unwrap();
    let included = out_dir.join("generated.rs");
    std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();

    let source_files = vec![lib, included];
    let path_normalizer = PathNormalizer::from_env(Some(&workspace));
    let out_dir_value = out_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let env_dep = normalize_env_dep_value(
        "test_crate",
        "OUT_DIR",
        &out_dir_value,
        &source_files,
        &path_normalizer,
    );

    assert_eq!(
        env_dep.decision,
        EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
    );
    assert_eq!(env_dep.value, out_dir_value);
}

/// The OUT_DIR decision for a crate whose root source is `lib_source` and
/// whose dep-info lists a generated include under OUT_DIR, the shape that
/// makes OUT_DIR a normalization candidate at all.
fn out_dir_decision_for(
    lib_source: &str,
    file_hasher: &FileHasher<'_>,
) -> EnvDepNormalizationDecision {
    out_dir_decision_for_files(&[("src/lib.rs", Some(lib_source))], file_hasher)
}

/// Like [`out_dir_decision_for`], with every workspace-relative file in
/// `files` listed in dep-info. A `None` body is listed but never written.
fn out_dir_decision_for_files(
    files: &[(&str, Option<&str>)],
    file_hasher: &FileHasher<'_>,
) -> EnvDepNormalizationDecision {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let out_dir = workspace.join("target/debug/build/pkg/out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let mut source_files = Vec::new();
    for (relative, body) in files {
        let path = workspace.join(relative);
        if let Some(body) = body {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body).unwrap();
        }
        source_files.push(path);
    }
    let included = out_dir.join("generated.rs");
    std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();
    source_files.push(included);
    let out_dir_value = out_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    normalize_env_dep_value_with_hasher(
        "test_crate",
        "OUT_DIR",
        &EnvDepValue::new(&out_dir_value),
        &EnvDepPaths::new(None, &source_files),
        file_hasher,
        &PathNormalizer::from_env(Some(&workspace)),
        None,
    )
    .decision
}

#[test]
fn env_dep_policy_keeps_out_dir_absolute_for_uses_the_scanner_cannot_prove() {
    // Every source below includes a generated file through OUT_DIR, so
    // dep-info alone would allow normalization, and every one also derives
    // something from the absolute OUT_DIR that ends up in the artifact. A
    // normalized key would restore one checkout's artifact in another.
    const INCLUDE: &str = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
    let value_uses: &[(&str, String)] = &[
        (
            "value length",
            format!(r#"{INCLUDE} pub const N: usize = env!("OUT_DIR").len();"#),
        ),
        (
            "value arithmetic",
            format!(r#"{INCLUDE} pub const P: usize = env!("OUT_DIR").len() % 2;"#),
        ),
        (
            "computed name",
            format!(r#"{INCLUDE} pub const S: &str = env!(concat!("OUT", "_DIR"));"#),
        ),
        (
            "computed name, option_env",
            format!(
                r#"{INCLUDE} pub const S: Option<&str> = option_env!(concat!("OUT", "_DIR"));"#
            ),
        ),
        (
            "forwarding macro",
            format!(
                r#"{INCLUDE}
macro_rules! e {{ ($v:literal) => {{ env!($v) }} }}
pub const X: &str = e!("OUT_DIR");"#
            ),
        ),
        (
            "brace delimiter",
            format!(r#"{INCLUDE} pub const S: &str = env!{{"OUT_DIR"}};"#),
        ),
        (
            "bracket delimiter",
            format!(r#"{INCLUDE} pub const S: &str = env!["OUT_DIR"];"#),
        ),
        (
            "escaped name",
            format!(r#"{INCLUDE} pub const S: &str = env!("OUT\x5FDIR");"#),
        ),
        (
            "raw string name",
            format!(r##"{INCLUDE} pub const S: &str = env!(r#"OUT_DIR"#);"##),
        ),
        (
            "comment before the bang",
            format!(r#"{INCLUDE} pub const S: &str = env /* x */ !("OUT_DIR");"#),
        ),
        (
            "non-ASCII whitespace before the bang",
            format!("{INCLUDE} pub const S: &str = env\u{200E}!(\"OUT_DIR\");"),
        ),
        (
            "vertical tab before the bang",
            format!("{INCLUDE} pub const S: &str = env\x0B!(\"OUT_DIR\");"),
        ),
        (
            "lifetime before the use",
            format!(r#"{INCLUDE} pub fn f(_: &'static str) -> usize {{ env!("OUT_DIR").len() }}"#),
        ),
        (
            "nested block comment",
            format!(
                r#"{INCLUDE} /* /* */ include!( */ pub const N: usize = env!("OUT_DIR").len();"#
            ),
        ),
        (
            "raw C string ending in a backslash",
            format!(
                r#"{INCLUDE} pub const C: &core::ffi::CStr = cr"\"; pub const N: usize = env!("OUT_DIR").len(); pub const T: &str = "";"#
            ),
        ),
        (
            "number suffix before a raw-string look-alike",
            format!(r##"{INCLUDE} m!{{ 1r#"x" }} pub const P: &str = env!("OUT_DIR"); // "#"##),
        ),
        (
            "non-ASCII prefix on an include look-alike",
            format!(
                r#"{INCLUDE}
macro_rules! éinclude {{ ($e:expr) => {{ pub const P: &str = $e; }} }}
éinclude!(env!("OUT_DIR"));"#
            ),
        ),
    ];
    // No visible include use: the env dep may come from another crate's
    // macro that bakes the value.
    let unproven: &[(&str, String)] = &[
        (
            "no env! in the crate",
            "pub fn f() -> &'static str { some_dep::out_dir!() }".to_string(),
        ),
        (
            "only a computed name inside include",
            r#"include!(concat!(env!(concat!("OUT", "_DIR")), "/generated.rs"));"#.to_string(),
        ),
    ];
    let hasher = FileHasher::new();
    let wrong: Vec<(&str, EnvDepNormalizationDecision)> = value_uses
        .iter()
        .map(|case| (case, EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse))
        .chain(unproven.iter().map(|case| {
            (
                case,
                EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
            )
        }))
        .filter_map(|((label, source), expected)| {
            let decision = out_dir_decision_for(source, &hasher);
            (decision != expected).then_some((*label, decision))
        })
        .collect();
    assert!(wrong.is_empty(), "unexpected OUT_DIR decisions: {wrong:?}");
}

#[test]
fn env_dep_policy_takes_include_proof_only_from_rust_sources() {
    // `#![doc = include_str!("../README.md")]` puts the README in dep-info.
    // A code block in it that shows the include pattern is not code.
    let readme = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
    let lib = r#"#![doc = include_str!("../README.md")] some_dep::out_dir!();"#;
    let hasher = FileHasher::new();
    assert_eq!(
        out_dir_decision_for_files(
            &[("src/lib.rs", Some(lib)), ("README.md", Some(readme))],
            &hasher
        ),
        EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
    );
    // The same text in a Rust file is proof.
    assert_eq!(
        out_dir_decision_for_files(
            &[("src/lib.rs", Some(lib)), ("src/gen.rs", Some(readme))],
            &hasher
        ),
        EnvDepNormalizationDecision::NormalizedPathOnly
    );
    // A value use in any file still counts.
    assert_eq!(
        out_dir_decision_for_files(
            &[
                ("src/lib.rs", Some(readme)),
                (
                    "README.md",
                    Some(r#"const N: usize = env!("OUT_DIR").len();"#)
                ),
            ],
            &hasher
        ),
        EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
    );
}

#[test]
fn env_dep_policy_keeps_out_dir_absolute_when_a_source_cannot_be_scanned() {
    let include = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
    assert_eq!(
        out_dir_decision_for_files(
            &[("src/lib.rs", Some(include)), ("src/missing.rs", None)],
            &FileHasher::new()
        ),
        EnvDepNormalizationDecision::KeptAbsoluteScanError
    );
}

#[test]
fn env_dep_policy_normalizes_out_dir_proven_include_locators() {
    let cases: &[(&str, &str)] = &[
        (
            "include concat",
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
        ),
        (
            "include in a local macro",
            r#"macro_rules! generated { ($f:literal) => { include!(concat!(env!("OUT_DIR"), "/", $f)); } }
generated!("generated.rs");"#,
        ),
        (
            "brace-delimited include",
            r#"include!{ concat!(env!("OUT_DIR"), "/generated.rs") }"#,
        ),
        (
            "lifetime and char literals around the include",
            r#"pub fn f<'a>(x: &'a str) -> char { let _ = x; 'x' }
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub fn g(_: &'static str) {}"#,
        ),
    ];
    let hasher = FileHasher::new();
    let kept: Vec<&str> = cases
        .iter()
        .filter(|(_, source)| {
            out_dir_decision_for(source, &hasher) != EnvDepNormalizationDecision::NormalizedPathOnly
        })
        .map(|(label, _)| *label)
        .collect();
    assert!(kept.is_empty(), "OUT_DIR must normalize for: {kept:?}");
}

#[test]
fn env_dep_policy_ignores_env_use_memo_rows_from_the_old_scanner() {
    // Before the scanner learned computed names, it recorded "no runtime
    // use" for this source. That row is keyed by content hash only, so an
    // upgraded wrapper would reuse it for the unchanged file unless the
    // memo is versioned.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let source = concat!(
        r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
        r#" pub const S: &str = env!(concat!("OUT", "_DIR"));"#
    );
    let content_hash = blake3::hash(source.as_bytes()).to_hex().to_string();
    drop(FileHasher::persistent(&db));
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS source_env_runtime_uses (
                content_hash    TEXT NOT NULL,
                env_var         TEXT NOT NULL,
                has_runtime_use INTEGER NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (content_hash, env_var)
            );",
    )
    .unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO source_env_runtime_uses
             (content_hash, env_var, has_runtime_use) VALUES (?1, 'OUT_DIR', 0)",
        rusqlite::params![content_hash],
    )
    .unwrap();
    drop(conn);

    let hasher = FileHasher::persistent(&db);
    assert_eq!(
        out_dir_decision_for(source, &hasher),
        EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
    );
}

#[test]
fn env_dep_policy_normalizes_allowlisted_var_but_not_unlisted() {
    // A non-OUT_DIR var that only locates an `include!`'d file (a source
    // file lives under it) is normalized ONLY when opted into the path-only
    // allowlist; otherwise kept absolute. This is the
    // KACHE_PATH_ONLY_ENV_VARS / `[cache] path_only_env_vars` contract.
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let src = workspace.join("src");
    let gen_dir = workspace.join("objdir/build/rust/mozbuild");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&gen_dir).unwrap();
    let lib = src.join("lib.rs");
    std::fs::write(&lib, r#"include!(env!("BUILDCONFIG_RS"));"#).unwrap();
    let included = gen_dir.join("buildconfig.rs");
    std::fs::write(&included, b"pub const X: u8 = 1;").unwrap();
    let source_files = vec![lib, included.clone()];
    let value = included
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Not allowlisted -> kept absolute.
    let pn_off = PathNormalizer::from_env(Some(&workspace));
    let off = normalize_env_dep_value(
        "test_crate",
        "BUILDCONFIG_RS",
        &value,
        &source_files,
        &pn_off,
    );
    assert_eq!(
        off.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly
    );
    assert_eq!(off.value, value);

    // Allowlisted -> normalized (the same gate as OUT_DIR still applies).
    let pn_on = PathNormalizer::from_env(Some(&workspace))
        .with_path_only_env_vars(vec!["BUILDCONFIG_RS".to_string()]);
    let on = normalize_env_dep_value(
        "test_crate",
        "BUILDCONFIG_RS",
        &value,
        &source_files,
        &pn_on,
    );
    assert_eq!(on.decision, EnvDepNormalizationDecision::NormalizedPathOnly);
    assert!(
        on.value.contains("<WORKSPACE>"),
        "allowlisted include locator should normalize: {on:?}"
    );
}

#[test]
fn env_dep_policy_normalizes_rustc_env_var_pointing_under_out_dir() {
    // kunobi-ninja/kache#431, the typenum cascade root: a build script sets
    // `cargo:rustc-env=GEN_BUILD_CONSTS=$OUT_DIR/consts.rs` and the crate does
    // `include!(env!("GEN_BUILD_CONSTS"))`. The var is NOT named OUT_DIR and is
    // NOT allowlisted, but its value lives UNDER OUT_DIR and only locates a
    // generated include — so it must normalize like OUT_DIR (else typenum
    // re-keys per checkout and the whole substrate stack misses cross-clone).
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let out_dir = workspace.join("target/release/build/genlib-abc123/out");
    let src = workspace.join("src");
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::create_dir_all(&src).unwrap();
    let lib = src.join("lib.rs");
    std::fs::write(&lib, r#"include!(env!("GEN_BUILD_CONSTS"));"#).unwrap();
    let generated = out_dir.join("consts.rs");
    std::fs::write(&generated, b"pub const N: u32 = 42;").unwrap();
    let source_files = vec![lib, generated.clone()];
    let value = generated
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let path_normalizer = PathNormalizer::from_env(Some(&workspace));

    let under = normalize_env_dep_value_in(
        "test_crate",
        "GEN_BUILD_CONSTS",
        &value,
        &source_files,
        &path_normalizer,
        Some(out_dir.as_os_str()),
    );
    // With OUT_DIR unset there is no anchor, so the same var must stay
    // absolute — proves the gate is the under-OUT_DIR test, not the var name.
    let no_anchor = normalize_env_dep_value(
        "test_crate",
        "GEN_BUILD_CONSTS",
        &value,
        &source_files,
        &path_normalizer,
    );

    assert_eq!(
        under.decision,
        EnvDepNormalizationDecision::NormalizedPathOnly,
        "a rustc-env var pointing under OUT_DIR, used only as an include locator, \
             must normalize: {under:?}"
    );
    let unit = out_dir
        .canonicalize()
        .unwrap()
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(
        under.value,
        format!("<OUT_DIR:{unit}>/consts.rs"),
        "an OUT_DIR-locator value normalizes relative to OUT_DIR (#330), keeping \
             the per-unit component (file!() observability) but not the location: {under:?}"
    );
    assert_eq!(
        no_anchor.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly,
        "without an OUT_DIR anchor the same non-allowlisted var must stay absolute"
    );
}

#[test]
fn env_dep_policy_keeps_out_dir_runtime_value_absolute() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let out_dir = workspace.join("target/debug/build/pkg/out");
    std::fs::create_dir_all(&out_dir).unwrap();

    let source_files = vec![workspace.join("src/main.rs")];
    let path_normalizer = PathNormalizer::from_env(Some(&workspace));
    let out_dir_value = out_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let env_dep = normalize_env_dep_value(
        "test_crate",
        "OUT_DIR",
        &out_dir_value,
        &source_files,
        &path_normalizer,
    );

    assert_eq!(
        env_dep.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
    );
    assert_eq!(env_dep.value, out_dir_value);
}

#[test]
fn env_dep_policy_force_list_overrides_runtime_value_scan() {
    // A crate whose source uses env!("OUT_DIR") as a runtime value is
    // normally kept absolute — but a user-asserted force entry normalizes
    // it anyway (the deployment guarantees the embedding branch is dead).
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let out_dir = dir.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, b"pub fn p() -> &'static str { env!(\"OUT_DIR\") }").unwrap();
    let out_dir_value = out_dir.to_string_lossy().to_string();
    let source_files = vec![src];

    let _out_dir = ScopedEnv::set("OUT_DIR", &out_dir_value);

    let pn_plain = PathNormalizer::from_env(Some(dir.path()));
    let kept = normalize_env_dep_value(
        "cef_dll_sys",
        "OUT_DIR",
        &out_dir_value,
        &source_files,
        &pn_plain,
    );

    let pn_forced = PathNormalizer::from_env(Some(dir.path()))
        .with_path_only_env_vars(vec!["cef_dll_sys:OUT_DIR".to_string()]);
    let forced = normalize_env_dep_value(
        "cef_dll_sys",
        "OUT_DIR",
        &out_dir_value,
        &source_files,
        &pn_forced,
    );

    assert_eq!(
        kept.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
    );
    assert_eq!(
        forced.decision,
        EnvDepNormalizationDecision::ForcedPathOnly,
        "a force-listed var must normalize despite the runtime-value scan: {forced:?}"
    );
    assert!(
        forced.value.starts_with("<OUT_DIR:") || forced.value.starts_with("<WORKSPACE>"),
        "forced OUT_DIR normalizes to a location-free sentinel form              (either the #330 OUT_DIR sentinel or a generic prefix rule): {forced:?}"
    );
    assert!(
        !forced.value.contains(dir.path().to_string_lossy().as_ref()),
        "no absolute build location may survive in a forced value: {forced:?}"
    );
}

#[test]
fn env_dep_policy_force_list_crate_scope_matches_only_that_crate() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let out_dir = dir.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, b"pub fn p() -> &'static str { env!(\"OUT_DIR\") }").unwrap();
    let out_dir_value = out_dir.to_string_lossy().to_string();
    let source_files = vec![src];

    let _out_dir = ScopedEnv::set("OUT_DIR", &out_dir_value);

    let pn = PathNormalizer::from_env(Some(dir.path()))
        .with_path_only_env_vars(vec!["cef_dll_sys:OUT_DIR".to_string()]);
    let scoped_match =
        normalize_env_dep_value("cef_dll_sys", "OUT_DIR", &out_dir_value, &source_files, &pn);
    let scoped_other =
        normalize_env_dep_value("other_crate", "OUT_DIR", &out_dir_value, &source_files, &pn);

    assert_eq!(
        scoped_match.decision,
        EnvDepNormalizationDecision::ForcedPathOnly
    );
    assert_eq!(
        scoped_other.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
        "a crate-scoped force entry must not leak to other crates: {scoped_other:?}"
    );
}

#[test]
fn env_dep_policy_refuses_manifest_dir_in_every_allowlist_form() {
    // A crate's own sources live under CARGO_MANIFEST_DIR, so the include
    // proof is trivially satisfied and only the refusal keeps #167 shut.
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let manifest_dir = workspace.join("helper");
    let src = manifest_dir.join("src");
    std::fs::create_dir_all(&src).unwrap();
    let lib = src.join("lib.rs");
    std::fs::write(
        &lib,
        br#"include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/gen.rs"));"#,
    )
    .unwrap();

    let source_files = vec![lib];
    let manifest_dir_value = manifest_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    for (entry, var) in [
        ("test_crate:CARGO_MANIFEST_DIR", "CARGO_MANIFEST_DIR"),
        ("CARGO_MANIFEST_DIR", "CARGO_MANIFEST_DIR"),
        // Windows resolves env names case-insensitively, so dep-info can
        // carry any spelling of the same variable.
        ("cargo_manifest_dir", "cargo_manifest_dir"),
    ] {
        let path_normalizer = PathNormalizer::from_env(Some(&workspace))
            .with_path_only_env_vars(vec![entry.to_string()]);
        let env_dep = normalize_env_dep_value(
            "test_crate",
            var,
            &manifest_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteManifestDir,
            "CARGO_MANIFEST_DIR must stay absolute, listed as `{entry}`"
        );
        assert_eq!(env_dep.value, manifest_dir_value);
    }
}

#[test]
fn env_dep_policy_keeps_user_path_env_absolute_when_normalized() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let config_dir = workspace.join("config");
    std::fs::create_dir_all(&config_dir).unwrap();

    let source_files = vec![workspace.join("src/lib.rs")];
    let path_normalizer = PathNormalizer::from_env(Some(&workspace));
    let config_dir_value = config_dir
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let env_dep = normalize_env_dep_value(
        "test_crate",
        "CUSTOM_CONFIG_DIR",
        &config_dir_value,
        &source_files,
        &path_normalizer,
    );

    assert_eq!(
        env_dep.decision,
        EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly
    );
    assert_eq!(env_dep.value, config_dir_value);
}

// `test_normalize_flags` removed: normalize_flags itself is gone,
// replaced by PathNormalizer (covered by tests in path_normalizer
// module). The cache_key consumer-side normalization is exercised
// via the e2e relocate phase + the `path_is_only_used_for_includes` tests.

#[test]
fn test_cache_key_changes_with_features() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args1: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--cfg".to_string(),
        "feature=\"std\"".to_string(),
    ];

    let mut args2 = args1.clone();
    args2.push("--cfg".to_string());
    args2.push("feature=\"derive\"".to_string());

    let parsed1 = RustcArgs::parse(&args1).unwrap();
    let parsed2 = RustcArgs::parse(&args2).unwrap();

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(key1, key2);
}

#[test]
fn test_cache_key_changes_with_instrument_coverage() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_normal: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];

    let mut args_coverage = args_normal.clone();
    args_coverage.push("-Cinstrument-coverage".to_string());

    let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
    let parsed_coverage = RustcArgs::parse(&args_coverage).unwrap();

    assert!(!parsed_normal.has_coverage_instrumentation());
    assert!(parsed_coverage.has_coverage_instrumentation());

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key_normal = compute_cache_key(&parsed_normal, &fh, &pn, &KeyEnv::default()).unwrap();
    let key_coverage = compute_cache_key(&parsed_coverage, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(
        key_normal, key_coverage,
        "coverage-instrumented builds must have different cache keys"
    );
}

#[test]
fn test_cache_key_changes_with_instrument_coverage_two_arg() {
    let _lock = key_test_lock();
    // Same test but with -C instrument-coverage (two-arg form)
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_normal: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];

    let mut args_coverage = args_normal.clone();
    args_coverage.extend(["-C".to_string(), "instrument-coverage".to_string()]);

    let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
    let parsed_coverage = RustcArgs::parse(&args_coverage).unwrap();

    assert!(parsed_coverage.has_coverage_instrumentation());

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key_normal = compute_cache_key(&parsed_normal, &fh, &pn, &KeyEnv::default()).unwrap();
    let key_coverage = compute_cache_key(&parsed_coverage, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(
        key_normal, key_coverage,
        "two-arg form -C instrument-coverage must also produce different cache keys"
    );
}

#[test]
fn test_cache_key_changes_with_tarpaulin_cfg() {
    let _lock = key_test_lock();
    // Tarpaulin also passes --cfg=tarpaulin; verify it affects the key
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_normal: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];

    let mut args_tarpaulin = args_normal.clone();
    args_tarpaulin.extend(["--cfg".to_string(), "tarpaulin".to_string()]);

    let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
    let parsed_tarpaulin = RustcArgs::parse(&args_tarpaulin).unwrap();

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key_normal = compute_cache_key(&parsed_normal, &fh, &pn, &KeyEnv::default()).unwrap();
    let key_tarpaulin = compute_cache_key(&parsed_tarpaulin, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(
        key_normal, key_tarpaulin,
        "--cfg=tarpaulin must produce a different cache key"
    );
}

#[test]
fn test_coverage_keys_consistent_across_remap_forms() {
    let _lock = key_test_lock();
    // Both joined and two-arg forms of instrument-coverage should produce
    // the same cache key (both map to codegen opt "instrument-coverage")
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_joined: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "-Cinstrument-coverage".to_string(),
    ];

    let mut args_two = args_joined[..6].to_vec();
    args_two.extend(["-C".to_string(), "instrument-coverage".to_string()]);

    let parsed_joined = RustcArgs::parse(&args_joined).unwrap();
    let parsed_two = RustcArgs::parse(&args_two).unwrap();

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key_joined = compute_cache_key(&parsed_joined, &fh, &pn, &KeyEnv::default()).unwrap();
    let key_two = compute_cache_key(&parsed_two, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_eq!(
        key_joined, key_two,
        "joined and two-arg forms of instrument-coverage should produce identical keys"
    );
}

#[test]
fn test_cache_key_version_affects_key() {
    let _lock = key_test_lock();
    // Verify that the key version is hashed by checking that the hasher
    // receives the version string. We do this indirectly: compute a key
    // and then verify the same inputs produce the same key (determinism).

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
    ];

    // Compute twice — must be deterministic (version baked in)
    let parsed1 = RustcArgs::parse(&args_vec).unwrap();
    let parsed2 = RustcArgs::parse(&args_vec).unwrap();
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();
    assert_eq!(
        key1, key2,
        "key must be deterministic with version baked in"
    );

    // Prove that different version values produce different hashes by
    // simulating what compute_cache_key does with version=N vs version=N+1.
    // We can't change the const, but we can replicate the hashing logic
    // to prove the version input is material.
    let payload = b"rustc_version:1.80.0\n";
    for (v_a, v_b) in [(1u32, 2u32), (0, 1), (1, 100)] {
        let hash = |version: u32| {
            let mut h = blake3::Hasher::new();
            h.update(b"key_version:");
            h.update(version.to_string().as_bytes());
            h.update(b"\n");
            h.update(payload);
            h.finalize().to_hex().to_string()
        };
        assert_ne!(
            hash(v_a),
            hash(v_b),
            "version {} vs {} must produce different hashes",
            v_a,
            v_b
        );
    }
}

// --- parse_dep_info tests (pure parser, no I/O) ---

#[test]
fn test_parse_dep_info_basic() {
    let input = "target.d: src/lib.rs src/server.rs src/utils.rs\n";
    let files = parse_dep_info(input);
    assert_eq!(files.len(), 3);
    assert_eq!(files[0], std::path::PathBuf::from("src/lib.rs"));
    assert_eq!(files[1], std::path::PathBuf::from("src/server.rs"));
    assert_eq!(files[2], std::path::PathBuf::from("src/utils.rs"));
}

#[test]
fn test_parse_dep_info_escaped_spaces() {
    let input = "target.d: src/my\\ file.rs src/lib.rs\n";
    let files = parse_dep_info(input);
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|p| p == &std::path::PathBuf::from("src/my file.rs"))
    );
    assert!(
        files
            .iter()
            .any(|p| p == &std::path::PathBuf::from("src/lib.rs"))
    );
}

#[test]
fn test_parse_dep_info_empty() {
    assert!(parse_dep_info("").is_empty());
    assert!(parse_dep_info("target.d:").is_empty());
    assert!(parse_dep_info("no colon here").is_empty());
}

#[test]
fn test_parse_dep_info_single_file() {
    let input = "deps.d: src/main.rs\n";
    let files = parse_dep_info(input);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], std::path::PathBuf::from("src/main.rs"));
}

#[test]
fn test_parse_dep_info_absolute_paths() {
    let input = "deps.d: /home/user/project/src/lib.rs /home/user/project/src/mod.rs\n";
    let files = parse_dep_info(input);
    assert_eq!(files.len(), 2);
    assert_eq!(
        files[0],
        std::path::PathBuf::from("/home/user/project/src/lib.rs")
    );
    assert_eq!(
        files[1],
        std::path::PathBuf::from("/home/user/project/src/mod.rs")
    );
}

// --- parse_env_dep_info tests (pure parser, no I/O) ---

#[test]
fn test_parse_env_deps_basic() {
    let input =
        "deps.d: src/lib.rs\n# env-dep:CARGO_PKG_VERSION=1.0.0\n# env-dep:OUT_DIR=/tmp/out\n";
    let env_deps = parse_env_dep_info(input);
    assert_eq!(env_deps.len(), 2);
    assert!(
        env_deps
            .iter()
            .any(|(k, v)| k == "CARGO_PKG_VERSION" && v == "1.0.0")
    );
    assert!(env_deps.iter().any(|(k, _)| k == "OUT_DIR"));
}

#[test]
fn test_parse_env_deps_returns_raw_values() {
    // Parser stores values verbatim; the normalization decision
    // belongs to `compute_cache_key` (which knows whether OUT_DIR
    // can be safely sentinelized — see `path_is_only_used_for_includes`).
    // Pre-normalizing here would erase the absolute-path
    // information the discriminator needs to read.
    let input = "deps.d: src/lib.rs\n# env-dep:OUT_DIR=/some/abs/path/target/debug/build/foo\n";
    let env_deps = parse_env_dep_info(input);
    assert_eq!(env_deps.len(), 1);
    assert_eq!(env_deps[0].0, "OUT_DIR");
    assert_eq!(env_deps[0].1, "/some/abs/path/target/debug/build/foo");
}

#[test]
fn test_parse_env_deps_empty() {
    let input = "deps.d: src/lib.rs\n";
    let env_deps = parse_env_dep_info(input);
    assert!(env_deps.is_empty());
}

#[test]
fn test_parse_env_deps_no_value() {
    let input = "deps.d: src/lib.rs\n# env-dep:UNSET_VAR\n";
    let env_deps = parse_env_dep_info(input);
    assert_eq!(env_deps.len(), 1);
    assert_eq!(env_deps[0].0, "UNSET_VAR");
}

// --- FileHasher tests ---

#[test]
fn test_file_hasher_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("test.rs");
    std::fs::write(&file, b"fn main() {}").unwrap();

    let hasher = FileHasher::new();
    let hash1 = hasher.hash(&file).unwrap();
    let hash2 = hasher.hash(&file).unwrap();
    assert_eq!(hash1, hash2, "FileHasher must be deterministic");
}

#[test]
fn env_dep_use_memo_reuses_every_scan_answer() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let source = dir.path().join("lib.rs");
    std::fs::write(
        &source,
        br#"include!(env!("GEN")); pub const OUT: &str = env!("OUT_DIR"); pub const N: usize = 1;"#,
    )
    .unwrap();

    let hasher = FileHasher::persistent(&db);
    let content_hash = hasher.hash(&source).unwrap();
    let answers = [
        ("OUT_DIR", SourceEnvDepUse::RuntimeValue),
        ("GEN", SourceEnvDepUse::IncludeLocator),
        ("OTHER_DIR", SourceEnvDepUse::Unused),
    ];
    for (var, expected) in answers {
        assert_eq!(hasher.env_dep_use(&source, var).unwrap(), expected, "{var}");
    }
    drop(hasher);

    // A fresh wrapper can answer every decision from SQLite using the
    // content hash it already obtained while building the cache key. The
    // source is gone, so any attempted reread would fail this test.
    std::fs::remove_file(&source).unwrap();
    let fresh = FileHasher::persistent(&db);
    for (var, expected) in answers {
        assert_eq!(
            fresh
                .env_dep_use_for_hash(&source, var, &content_hash)
                .unwrap(),
            expected,
            "{var}"
        );
    }
}

#[test]
fn env_dep_use_memo_rescans_rows_from_other_scanner_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let source = dir.path().join("lib.rs");
    std::fs::write(
        &source,
        br#"pub const S: &str = env!(concat!("OUT", "_DIR"));"#,
    )
    .unwrap();
    let content_hash = hash_file(&source).unwrap();

    let cache = FileHashCache::open(&db).unwrap();
    let unused = SourceEnvDepUse::Unused.memo_code();
    cache
        .put_source_env_dep_use(
            &content_hash,
            "OUT_DIR",
            SOURCE_ENV_DEP_SCANNER_VERSION - 1,
            unused,
        )
        .unwrap();
    cache
        .put_source_env_dep_use(&content_hash, "OTHER", SOURCE_ENV_DEP_SCANNER_VERSION, 7)
        .unwrap();
    drop(cache);

    let hasher = FileHasher::persistent(&db);
    assert_eq!(
        hasher
            .env_dep_use_for_hash(&source, "OUT_DIR", &content_hash)
            .unwrap(),
        SourceEnvDepUse::RuntimeValue,
        "an older scanner's answer must not be reused"
    );
    assert_eq!(
        hasher
            .env_dep_use_for_hash(&source, "OTHER", &content_hash)
            .unwrap(),
        SourceEnvDepUse::RuntimeValue,
        "an unknown stored code must be rescanned"
    );
    drop(hasher);

    let cache = FileHashCache::open(&db).unwrap();
    for var in ["OUT_DIR", "OTHER"] {
        assert_eq!(
            cache
                .get_source_env_dep_use(&content_hash, var, SOURCE_ENV_DEP_SCANNER_VERSION)
                .unwrap(),
            Some(SourceEnvDepUse::RuntimeValue.memo_code()),
            "the rescan replaces the stale row for {var}"
        );
    }
}

#[test]
fn env_dep_use_memo_codes_round_trip() {
    for answer in [
        SourceEnvDepUse::Unused,
        SourceEnvDepUse::IncludeLocator,
        SourceEnvDepUse::RuntimeValue,
    ] {
        assert_eq!(
            SourceEnvDepUse::from_memo_code(answer.memo_code()),
            Some(answer)
        );
    }
    assert_eq!(
        [
            SourceEnvDepUse::Unused.memo_code(),
            SourceEnvDepUse::IncludeLocator.memo_code(),
            SourceEnvDepUse::RuntimeValue.memo_code(),
        ],
        [0, 1, 2]
    );
    assert_eq!(SourceEnvDepUse::from_memo_code(3), None);
    assert_eq!(SourceEnvDepUse::from_memo_code(-1), None);
}

#[test]
fn runtime_env_use_scan_rejects_content_changed_after_hashing() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, br#"include!(concat!(env!("OUT_DIR"), "/x.rs"));"#).unwrap();

    let hasher = FileHasher::new();
    hasher.hash(&source).unwrap();
    std::fs::write(&source, br#"pub const OUT: &str = env!("OUT_DIR");"#).unwrap();

    let error = hasher.env_dep_use(&source, "OUT_DIR").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("changed between content hashing"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn cc_preprocess_memo_support_requires_persistent_cache() {
    assert!(!FileHasher::new().supports_cc_preprocess_memo());

    let dir = tempfile::tempdir().unwrap();
    let persistent = FileHasher::persistent(&dir.path().join("idx.sqlite"));
    assert!(persistent.supports_cc_preprocess_memo());
}

/// The mapped hash is memoised by raw content and map set: a second unit
/// that reads the same bytes, at any path, takes the memo instead of
/// reading and rewriting the file; another map set or an empty key does
/// not; a memo error or a missing store still computes.
#[test]
fn mapped_hashes_are_memoised_by_content_and_map_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let header = dir.path().join("header.h");
    let copy = dir.path().join("copy.h");
    let other = dir.path().join("other.h");
    // Above MIN_PERSISTED_HASH_BYTES so the raw hash itself is memoised.
    let body = "#define VALUE 1\n".repeat(64);
    std::fs::write(&header, &body).unwrap();
    std::fs::write(&copy, &body).unwrap();
    std::fs::write(&other, format!("{body}#define OTHER 2\n")).unwrap();
    let reads = std::cell::Cell::new(0usize);
    let counting = |path: &Path| -> Option<String> {
        reads.set(reads.get() + 1);
        let bytes = std::fs::read(path).ok()?;
        Some(
            blake3::hash(&[b"mapped:".as_slice(), &bytes].concat())
                .to_hex()
                .to_string(),
        )
    };
    let name = |path: &Path| (path.to_string_lossy().into_owned(), path.to_path_buf());

    let hasher = FileHasher::persistent(&db);
    let first = hasher
        .cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
        .unwrap();
    assert_eq!(reads.get(), 1);
    let second = hasher
        .cc_preprocess_fingerprints(&[name(&copy), name(&other)], "maps-a", &counting)
        .unwrap();
    assert_eq!(
        reads.get(),
        2,
        "the copy took the memo, the other file did not"
    );
    let by_name: std::collections::HashMap<_, _> =
        second.iter().map(|i| (i.name.as_str(), i)).collect();
    assert_eq!(by_name[name(&copy).0.as_str()].mapped, first[0].mapped);
    assert_ne!(by_name[name(&other).0.as_str()].mapped, first[0].mapped);

    // A fresh process (new hasher on the same index) still has the memo.
    let later = FileHasher::persistent(&db);
    later
        .cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
        .unwrap();
    assert_eq!(reads.get(), 2, "the memo survives the process");
    // Another map set rewrites bytes differently and computes again.
    later
        .cc_preprocess_fingerprints(&[name(&header)], "maps-b", &counting)
        .unwrap();
    assert_eq!(reads.get(), 3);
    // An empty key never memoises.
    later
        .cc_preprocess_fingerprints(&[name(&header)], "", &counting)
        .unwrap();
    later
        .cc_preprocess_fingerprints(&[name(&header)], "", &counting)
        .unwrap();
    assert_eq!(reads.get(), 5);
    // Without a store, every unit computes.
    let bare = FileHasher::new();
    bare.cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
        .unwrap();
    assert_eq!(reads.get(), 6);
}

/// The assembler scan runs once per distinct content and its verdict is
/// memoised across processes; an unreadable file is skipped and not
/// recorded.
#[test]
fn assembler_scans_are_memoised_by_content() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let clean = dir.path().join("clean.h");
    let twin = dir.path().join("twin.h");
    let pasted = dir.path().join("pasted.h");
    let body = "#define VALUE 1\n".repeat(64);
    std::fs::write(&clean, &body).unwrap();
    std::fs::write(&twin, &body).unwrap();
    std::fs::write(
        &pasted,
        format!("{body}__asm__(\".incbin \\\"blob\\\"\");\n"),
    )
    .unwrap();
    let scans = std::cell::Cell::new(0usize);
    let scan = |path: &Path| -> Option<Option<&'static str>> {
        scans.set(scans.get() + 1);
        let text = std::fs::read_to_string(path).ok()?;
        Some(text.contains(".incbin").then_some(".incbin"))
    };
    let name = |path: &Path| (path.to_string_lossy().into_owned(), path.to_path_buf());
    let hasher = FileHasher::persistent(&db);
    let inputs = hasher
        .cc_preprocess_fingerprints(&[name(&clean), name(&twin)], "", &|_| Some(String::new()))
        .unwrap();
    assert_eq!(hasher.cc_inputs_hide_assembler_input(&inputs, &scan), None);
    assert_eq!(scans.get(), 1, "twins share one scan");
    let later = FileHasher::persistent(&db);
    let inputs = later
        .cc_preprocess_fingerprints(&[name(&clean), name(&pasted)], "", &|_| Some(String::new()))
        .unwrap();
    assert_eq!(
        later
            .cc_inputs_hide_assembler_input(&inputs, &scan)
            .as_deref(),
        Some(".incbin")
    );
    assert_eq!(
        scans.get(),
        2,
        "the clean verdict survived the process; only the new file was scanned"
    );
    assert_eq!(
        later
            .cc_inputs_hide_assembler_input(&inputs, &scan)
            .as_deref(),
        Some(".incbin"),
        "the construct verdict is memoised too"
    );
    assert_eq!(scans.get(), 2);
    // An unreadable file is skipped and left for the next run.
    let mut gone = inputs.clone();
    gone[0].fingerprint.path = dir.path().join("absent.h").to_string_lossy().into_owned();
    gone[0].content = "c".repeat(64);
    assert_eq!(
        later.cc_inputs_hide_assembler_input(&gone[..1], &scan),
        None
    );
    assert_eq!(
        later.cc_inputs_hide_assembler_input(&gone[..1], &scan),
        None
    );
    assert_eq!(
        scans.get(),
        4,
        "an unreadable file is scanned again next time"
    );
}

#[test]
fn cc_preprocess_memo_requires_every_input_fingerprint_to_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let source = dir.path().join("source.c");
    let header = dir.path().join("header.h");
    std::fs::write(&source, "#include \"header.h\"\n").unwrap();
    std::fs::write(&header, "#define VALUE 1\n").unwrap();

    let hasher = FileHasher::persistent(&db);
    let inputs = hasher
        .cc_preprocess_fingerprints(
            &[
                (source.to_string_lossy().into_owned(), source.clone()),
                (header.to_string_lossy().into_owned(), header.clone()),
            ],
            "",
            &no_mapping,
        )
        .unwrap();
    let pp_hash = "a".repeat(64);
    hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);
    assert_eq!(
        hasher
            .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
            .map(|(hash, _)| hash)
            .as_deref(),
        Some(pp_hash.as_str())
    );

    let inputs_json = serde_json::to_string(&inputs).unwrap();
    let cache = hasher.cache.as_ref().unwrap();
    cache
        .put_cc_preprocess_memo("short-hash", "a", &inputs_json)
        .unwrap();
    cache
        .put_cc_preprocess_memo("non-hex-hash", &"z".repeat(64), &inputs_json)
        .unwrap();
    assert_eq!(
        hasher.cc_preprocess_memo_lookup("short-hash", no_remap, &no_mapping),
        None
    );
    assert_eq!(
        hasher.cc_preprocess_memo_lookup("non-hex-hash", no_remap, &no_mapping),
        None
    );

    // An input written inside the invocation window is what a fresh
    // checkout looks like. It used to refuse both halves of the memo,
    // which meant CI could never memoise anything; the content hash makes
    // the recency irrelevant.
    let mut fresh_hasher = FileHasher::persistent(&db);
    fresh_hasher.arm_too_new_guard(i64::MAX, 0);
    assert_eq!(
        fresh_hasher
            .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
            .map(|(hash, _)| hash)
            .as_deref(),
        Some(pp_hash.as_str()),
        "unchanged bytes must hit however recently they were written"
    );
    fresh_hasher.cc_preprocess_memo_record_if_unchanged("too-new", &pp_hash, &inputs, &no_mapping);
    assert!(
        fresh_hasher
            .cache
            .as_ref()
            .unwrap()
            .get_cc_preprocess_memo("too-new")
            .unwrap()
            .is_some(),
        "a fresh checkout must still be able to publish a memo"
    );

    std::fs::write(&header, "#define VALUE 12345\n").unwrap();
    assert_eq!(
        hasher.cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping),
        None,
        "a changed transitive header must force preprocessing"
    );
    hasher.cc_preprocess_memo_record_if_unchanged("changed", &pp_hash, &inputs, &no_mapping);
    assert!(
        hasher
            .cache
            .as_ref()
            .unwrap()
            .get_cc_preprocess_memo("changed")
            .unwrap()
            .is_none(),
        "changed inputs must not publish a memo"
    );
}

/// Tests that do not exercise prefix maps hash contents as they are.
fn no_mapping(path: &Path) -> Option<String> {
    hash_file(path).ok()
}

/// A resolver for tests that record and read in one place: the recorded
/// name is the path.
fn no_remap(name: &str) -> Vec<PathBuf> {
    vec![PathBuf::from(name)]
}

/// The case the benchmark exposed: a generated header names its own build
/// directory, so two checkouts hold different bytes that the prefix maps
/// rewrite to the same thing. The expansion is hashed after mapping, so
/// the key already treats them as equal; comparing raw bytes alone left
/// the memo stricter than the key it feeds, and every `-sys` unit behind
/// such a header preprocessed again in the second checkout.
#[test]
fn cc_preprocess_memo_compares_contents_as_the_expansion_sees_them() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    for tree in [&a, &b] {
        std::fs::create_dir_all(tree).unwrap();
    }
    // Each tree's header names its own directory, as a build script writes.
    std::fs::write(a.join("gen.h"), format!("#define P \"{}\"\n", a.display())).unwrap();
    std::fs::write(b.join("gen.h"), format!("#define P \"{}\"\n", b.display())).unwrap();

    // The maps each tree would use: its own root onto one shared sentinel.
    let map_under = |root: &std::path::Path| {
        let root = root.to_string_lossy().into_owned();
        move |path: &Path| -> Option<String> {
            let text = std::fs::read_to_string(path).ok()?;
            Some(
                blake3::hash(text.replace(&root, "<root>").as_bytes())
                    .to_hex()
                    .to_string(),
            )
        }
    };

    let hasher = FileHasher::persistent(&db);
    let inputs = hasher
        .cc_preprocess_fingerprints(
            &[("<root>/gen.h".to_string(), a.join("gen.h"))],
            "",
            &map_under(&a),
        )
        .unwrap();
    assert_ne!(
        inputs[0].content, inputs[0].mapped,
        "raw and mapped hashes differ for a file that names its own path"
    );
    let pp_hash = "d".repeat(64);
    hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &map_under(&a));

    let resolve_in_b = |name: &str| vec![b.join(name.trim_start_matches("<root>/"))];
    assert_eq!(
        FileHasher::persistent(&db)
            .cc_preprocess_memo_lookup("memo-key", resolve_in_b, &map_under(&b))
            .map(|(hash, _)| hash)
            .as_deref(),
        Some(pp_hash.as_str()),
        "the same header under another root must reuse the expansion"
    );

    // A real difference still misses, mapping or no mapping.
    std::fs::write(
        b.join("gen.h"),
        format!("#define P \"{}\"\n#define EXTRA 1\n", b.display()),
    )
    .unwrap();
    assert_eq!(
        FileHasher::persistent(&db).cc_preprocess_memo_lookup(
            "memo-key",
            resolve_in_b,
            &map_under(&b)
        ),
        None,
        "a header that gained a definition must force a fresh preprocess"
    );
}

/// The hole the relocate-modified e2e phase found: a second checkout with
/// an edited copy must not be answered by the unedited original, which is
/// still sitting on disk where the memo recorded it. Reusing that
/// expansion would derive the unedited key and serve a stale artifact for
/// modified source.
#[test]
fn cc_preprocess_memo_ignores_the_recording_tree_when_reading_another() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let original = dir.path().join("a");
    let relocated = dir.path().join("b");
    for tree in [&original, &relocated] {
        std::fs::create_dir_all(tree).unwrap();
    }
    let source_of = |tree: &std::path::Path| tree.join("source.c");
    std::fs::write(source_of(&original), "int v(void) { return 1; }\n").unwrap();

    // Record in the original tree, under a mapped name both trees share.
    let hasher = FileHasher::persistent(&db);
    let inputs = hasher
        .cc_preprocess_fingerprints(
            &[("<root>/source.c".to_string(), source_of(&original))],
            "",
            &no_mapping,
        )
        .unwrap();
    let pp_hash = "c".repeat(64);
    hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);

    // The relocated tree has an EDITED copy. The original still exists,
    // untouched, at the path the record names.
    std::fs::write(source_of(&relocated), "int v(void) { return 999; }\n").unwrap();
    let resolve_in_relocated =
        |name: &str| vec![relocated.join(name.trim_start_matches("<root>/"))];
    assert_eq!(
        FileHasher::persistent(&db).cc_preprocess_memo_lookup(
            "memo-key",
            resolve_in_relocated,
            &no_mapping
        ),
        None,
        "the edited copy must miss even though the original is unchanged"
    );

    // The same resolver on an identical copy is a hit: this is not just
    // rejecting everything from another tree.
    std::fs::write(source_of(&relocated), "int v(void) { return 1; }\n").unwrap();
    assert_eq!(
        FileHasher::persistent(&db)
            .cc_preprocess_memo_lookup("memo-key", resolve_in_relocated, &no_mapping)
            .map(|(hash, _)| hash)
            .as_deref(),
        Some(pp_hash.as_str()),
        "an identical copy in another tree must still reuse the expansion"
    );
}

/// The case the memo exists for and used to miss: the same bytes at new
/// metadata. A second worktree gives every file a new inode and mtime,
/// and a build script that regenerates a header rewrites it identically.
/// Neither changes what the preprocessor would produce.
#[test]
fn cc_preprocess_memo_survives_new_metadata_for_unchanged_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let source = dir.path().join("source.c");
    let header = dir.path().join("header.h");
    let source_bytes = "#include \"header.h\"\nint v(void) { return VALUE; }\n";
    let header_bytes = "#define VALUE 1\n";
    std::fs::write(&source, source_bytes).unwrap();
    std::fs::write(&header, header_bytes).unwrap();

    let hasher = FileHasher::persistent(&db);
    let inputs = hasher
        .cc_preprocess_fingerprints(
            &[
                (source.to_string_lossy().into_owned(), source.clone()),
                (header.to_string_lossy().into_owned(), header.clone()),
            ],
            "",
            &no_mapping,
        )
        .unwrap();
    assert!(
        inputs.iter().all(|input| input.content.len() == 64),
        "every recorded input carries a content hash"
    );
    let pp_hash = "b".repeat(64);
    hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);

    // Rewrite identical bytes with different metadata. Filesystems can
    // reuse the inode and timestamp on an immediate rewrite, so set the
    // mtime explicitly to exercise content-based memo validation.
    for (path, bytes) in [(&source, source_bytes), (&header, header_bytes)] {
        std::fs::remove_file(path).unwrap();
        std::fs::write(path, bytes).unwrap();
        filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(1, 0)).unwrap();
    }
    let rewritten = FileFingerprint::from_path(&header).unwrap();
    assert_ne!(
        rewritten, inputs[0].fingerprint,
        "the rewrite must actually change the metadata this test is about"
    );

    let reader = FileHasher::persistent(&db);
    assert_eq!(
        reader
            .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
            .map(|(hash, _)| hash)
            .as_deref(),
        Some(pp_hash.as_str()),
        "identical bytes at new metadata must reuse the expansion"
    );

    // One byte of difference is still a miss, whatever the metadata says.
    std::fs::write(&header, "#define VALUE 2\n").unwrap();
    filetime::set_file_mtime(&header, filetime::FileTime::from_unix_time(2, 0)).unwrap();
    assert_eq!(
        FileHasher::persistent(&db).cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping),
        None,
        "changed bytes must force a fresh preprocess"
    );
}

#[test]
fn too_new_guard_flags_inputs_modified_after_build_start() {
    // kunobi-ninja/kache#324: when armed, the guard flags any hashed input
    // whose mtime/ctime is at/after the build's start (its content is racy
    // vs what the compiler reads). Disabled by default.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("idx.sqlite");
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    // Disabled (default) → never flagged.
    let off = FileHasher::persistent(&db);
    off.hash(&file).unwrap();
    assert!(!off.too_new(), "guard is off by default");

    // Build "started" in the future → the file predates it → not too-new.
    let mut before = FileHasher::persistent(&db);
    before.arm_too_new_guard(now_ns + 60_000_000_000, 0);
    before.hash(&file).unwrap();
    assert!(
        !before.too_new(),
        "a file modified before the build started is not too-new"
    );

    // Build "started" in the past → the file was modified after → too-new.
    let mut after = FileHasher::persistent(&db);
    after.arm_too_new_guard(now_ns - 60_000_000_000, 0);
    after.hash(&file).unwrap();
    assert!(
        after.too_new(),
        "a file modified after the build started must be flagged too-new"
    );

    // The store-free/daemon wrapper path uses `FileHasher::new()`. Its
    // guard must not silently become a no-op just because no local hash
    // memo is open.
    let mut cacheless = FileHasher::new();
    cacheless.arm_too_new_guard(now_ns - 60_000_000_000, 0);
    cacheless.hash(&file).unwrap();
    assert!(
        cacheless.too_new(),
        "a cacheless hasher must enforce the same too-new guard"
    );
}

#[test]
fn guarded_inputs_record_only_while_armed() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();

    let disarmed = FileHasher::new();
    disarmed.hash(&file).unwrap();
    assert!(
        disarmed.take_guarded_inputs().is_empty(),
        "a disarmed hasher records nothing to verify"
    );

    let mut armed = FileHasher::new();
    armed.arm_too_new_guard(1, 0);
    armed.hash(&file).unwrap();
    armed.hash(&file).unwrap();
    assert_eq!(
        armed.take_guarded_inputs().len(),
        2,
        "every hash while armed is recorded for post-compile verification"
    );
    assert!(
        armed.take_guarded_inputs().is_empty(),
        "taking the snapshot drains it"
    );
}

#[test]
fn guarded_inputs_empty_set_never_excuses() {
    assert!(
        !FileHasher::guarded_inputs_unchanged_since_hash(&[]),
        "a vacuous check must not waive a tripped guard"
    );
}

#[test]
fn guarded_inputs_reject_changed_or_missing_files() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();
    let recorded = FileFingerprint::from_path(&file).unwrap();

    std::fs::write(&file, b"pub fn x() { 1 }").unwrap();
    assert!(
        !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
        "rewritten bytes must fail verification even when the wall clock cannot tell"
    );

    std::fs::remove_file(&file).unwrap();
    assert!(
        !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
        "a file that vanished mid-build must fail verification"
    );
}

#[test]
fn guarded_inputs_reject_weak_identity() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();
    let mut recorded = FileFingerprint::from_path(&file).unwrap();
    recorded.inode = 0;
    assert!(
        !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
        "without an inode a replace-by-rename is invisible, so verification must fail closed"
    );
}

#[cfg(unix)]
#[test]
fn guarded_inputs_verify_despite_future_mtimes() {
    // The clock-domain case: the filesystem clock runs ahead of the host
    // (NFS skew, a fresh checkout stamped in the future), so the
    // wall-clock guard trips on files the build never touched. Identical
    // fingerprints before and after still prove nothing changed.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();
    filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(2_000_000_000, 0)).unwrap();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let mut hasher = FileHasher::new();
    hasher.arm_too_new_guard(now_ns, 0);
    hasher.hash(&file).unwrap();
    assert!(
        hasher.too_new(),
        "a future mtime must still trip the wall-clock guard"
    );
    assert!(
        FileHasher::guarded_inputs_unchanged_since_hash(&hasher.take_guarded_inputs()),
        "untouched bytes verify despite the skewed clock"
    );
}

#[test]
fn a_units_headers_come_from_one_memo_lookup_once_settled() {
    let dir = tempfile::tempdir().unwrap();
    // Small on purpose: `hash` never memoises these, the capture does.
    let headers: Vec<(String, PathBuf)> = (0..5)
        .map(|i| {
            let path = dir.path().join(format!("h{i}.h"));
            std::fs::write(&path, format!("#define H{i} {i}\n")).unwrap();
            (format!("h{i}.h"), path)
        })
        .collect();
    let db = rusqlite::Connection::open_in_memory().unwrap();
    ensure_file_hash_cache_schema(&db).unwrap();
    let mapped = |path: &Path| std::fs::read_to_string(path).ok();

    let mut first = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    first.arm_too_new_guard(1, 0);
    let cold = first
        .cc_preprocess_fingerprints(&headers, "maps", &mapped)
        .unwrap();
    assert_eq!(first.stats().cache_misses, 5);
    assert_eq!(first.stats().cache_hits, 0);
    // Every header is registered for the post-compile revalidation and
    // remembered for later users in this process.
    assert_eq!(first.guard_inputs.borrow().len(), 5);
    for (_, path) in &headers {
        assert!(
            first
                .recent_hashes
                .borrow()
                .contains_key(&absolute_path(path))
        );
    }
    first.flush_memo_as_if_settled();

    let second = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    let warm = second
        .cc_preprocess_fingerprints(&headers, "maps", &mapped)
        .unwrap();
    assert_eq!(second.stats().cache_hits, 5, "all five from the memo");
    assert_eq!(second.stats().cache_misses, 0);
    assert_eq!(second.stats().bytes_hashed, 0);
    let hashes = |inputs: &[CcPreprocessMemoInput]| {
        inputs.iter().map(|i| i.content.clone()).collect::<Vec<_>>()
    };
    assert_eq!(hashes(&warm), hashes(&cold));

    // A header rewritten since then is hashed again, not served stale.
    std::fs::write(&headers[0].1, "#define H0 changed\n").unwrap();
    let third = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    let after = third
        .cc_preprocess_fingerprints(&headers, "maps", &mapped)
        .unwrap();
    assert_eq!(third.stats().cache_misses, 1);
    assert_ne!(hashes(&after)[0], hashes(&cold)[0]);
}

#[test]
fn an_unarmed_guard_keeps_no_headers_for_revalidation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("h.h");
    std::fs::write(&path, "#define H 1\n").unwrap();
    let headers = vec![("h.h".to_string(), path)];
    let mapped = |path: &Path| std::fs::read_to_string(path).ok();

    let hasher = FileHasher::new();
    hasher
        .cc_preprocess_fingerprints(&headers, "maps", &mapped)
        .unwrap();
    assert!(hasher.take_guarded_inputs().is_empty());
}

#[test]
fn flushing_on_the_real_clock_keeps_a_long_settled_stamp() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    ensure_file_hash_cache_schema(&db).unwrap();
    // Last changed at the epoch: settled by any clock this runs under.
    let stamp = FileFingerprint {
        path: "/old/header.h".to_string(),
        size: 12,
        mtime_ns: 1,
        ctime_ns: 1,
        inode: 7,
    };
    let hasher = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    hasher
        .pending_memo
        .borrow_mut()
        .push((stamp.clone(), "hash".to_string()));
    hasher.flush_memo();

    let reader = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    let memo = reader.memoised_hashes(std::iter::once(&stamp));
    assert_eq!(memo.get("/old/header.h").map(String::as_str), Some("hash"));
}

#[test]
fn a_file_changed_within_the_settle_window_is_hashed_but_not_memoised() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("fresh.h");
    std::fs::write(&file, vec![1u8; 70 * 1024]).unwrap();
    let db = rusqlite::Connection::open_in_memory().unwrap();
    ensure_file_hash_cache_schema(&db).unwrap();
    let hasher = FileHasher::from_cache(FileHashCache::Borrowed(&db));
    let hash = hasher.hash(&file).unwrap();
    hasher.flush_memo();
    let stamp = FileFingerprint::from_path(&file).unwrap();
    let cache = FileHashCache::Borrowed(&db);
    assert_eq!(
        cache.get(&stamp).unwrap(),
        None,
        "a second write in this timestamp tick could reuse this stamp"
    );
    // Once settled, the same flush records it.
    hasher
        .pending_memo
        .borrow_mut()
        .push((stamp.clone(), hash.clone()));
    hasher.flush_memo_as_if_settled();
    assert_eq!(cache.get(&stamp).unwrap(), Some(hash));
}

#[test]
fn test_file_hasher_persistent_cache_invalidates_on_metadata_change() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("index.db");
    let file = dir.path().join("large.rlib");
    std::fs::write(&file, vec![1u8; 70 * 1024]).unwrap();

    let hasher = FileHasher::persistent(&db_path);
    let first = hasher.hash(&file).unwrap();
    hasher.flush_memo_as_if_settled();
    let first_stats = hasher.stats();
    assert_eq!(first_stats.cache_hits, 0);
    assert_eq!(first_stats.cache_misses, 1);
    assert!(first_stats.bytes_hashed > 0);

    let second_hasher = FileHasher::persistent(&db_path);
    let second = second_hasher.hash(&file).unwrap();
    let second_stats = second_hasher.stats();
    assert_eq!(first, second);
    assert_eq!(second_stats.cache_hits, 1);
    assert_eq!(second_stats.cache_misses, 0);

    std::fs::write(&file, vec![2u8; 70 * 1024]).unwrap();
    let changed = FileHasher::persistent(&db_path).hash(&file).unwrap();
    assert_ne!(first, changed);
}

#[test]
fn test_file_hasher_persistent_cache_skips_small_files() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("index.db");
    let file = dir.path().join("small.rs");
    std::fs::write(&file, b"fn main() {}").unwrap();

    let hasher = FileHasher::persistent(&db_path);
    let first = hasher.hash(&file).unwrap();
    let second = hasher.hash(&file).unwrap();
    let stats = hasher.stats();
    assert_eq!(first, second);
    assert_eq!(stats.cache_hits, 0);
    assert_eq!(stats.cache_misses, 2);
}

// --- dep-info pre-pass integration test ---

#[test]
fn test_dep_info_finds_modules() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();

    std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
    std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

    let rustc = std::path::PathBuf::from("rustc");
    let source = src.join("lib.rs");
    let args = vec![
        "--crate-name".to_string(),
        "testcrate".to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition".to_string(),
        "2021".to_string(),
    ];

    let runs_before = crate::opcounts::dep_info_runs();
    let ms_before = crate::opcounts::dep_info_ms();
    let dep_info = run_dep_info_pass(&rustc, None, &source, &args, false).unwrap();
    // The pre-pass is a real rustc start; the event log must see it.
    assert!(crate::opcounts::dep_info_runs() > runs_before);
    assert!(
        crate::opcounts::dep_info_ms() > ms_before,
        "a rustc spawn takes more than a millisecond"
    );

    assert!(
        dep_info.source_files.len() >= 2,
        "expected at least 2 files, got {:?}",
        dep_info.source_files
    );
    assert!(dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")));
    assert!(
        dep_info
            .source_files
            .iter()
            .any(|p| p.ends_with("server.rs"))
    );
}

#[test]
fn run_dep_info_pass_errors_on_compile_failure() {
    // A failing dep-info pre-pass must return Err, NOT a crate-root-only
    // DepInfo: keying off an incomplete input set risks a stale-artifact
    // false hit (kunobi-ninja/kache#323). The wrapper turns this Err into a
    // passthrough (real compile, no store).
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    // Syntactically invalid Rust → rustc exits non-zero on the dep-info pass.
    std::fs::write(src.join("lib.rs"), b"fn broken( { this is not valid rust").unwrap();

    let rustc = std::path::PathBuf::from("rustc");
    let source = src.join("lib.rs");
    let args = vec![
        "--crate-name".to_string(),
        "testcrate".to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition".to_string(),
        "2021".to_string(),
    ];

    let runs_before = crate::opcounts::dep_info_runs();
    let Err(err) = run_dep_info_pass(&rustc, None, &source, &args, false) else {
        panic!("expected Err on a failing dep-info pass (source has a syntax error)");
    };
    assert!(
        crate::opcounts::dep_info_runs() > runs_before,
        "a failed pre-pass still spawned rustc and must be counted"
    );
    // The error must CARRY rustc's own reason, not just say the pass
    // failed: the wrapper logs `{e:#}`, and a diagnostic that drops the
    // cause is how the substrate bench's 60 refusals stayed unexplained
    // for two months (kunobi-ninja/kache#431).
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("dep-info pre-pass failed (exit"),
        "the cause must name the failing exit status: {rendered}"
    );
    assert!(
        rendered.contains("error"),
        "the cause must carry rustc's own first stderr line: {rendered}"
    );
}

#[test]
fn dep_info_pass_args_drops_output_naming_flags() {
    // Cargo's real lib-target argv, plus the `-C extra-filename` the
    // pre-pass used to carry into its own `-o` invocation
    // (kunobi-ninja/kache#896).
    let args: Vec<String> = [
        "--crate-name",
        "mylib",
        "--edition=2021",
        "mylib/src/lib.rs",
        "--error-format=json",
        "--crate-type",
        "lib",
        "--emit=dep-info,metadata,link",
        "-C",
        "metadata=1b2c9f1c31209a4a",
        "-C",
        "extra-filename=-02749d16b52ff8b3",
        "--out-dir",
        "/w/target/debug/deps",
        "-C",
        "incremental=/w/target/debug/incremental",
        "-L",
        "dependency=/w/target/debug/deps",
    ]
    .iter()
    .map(|arg| (*arg).to_string())
    .collect();

    let dep_args = dep_info_pass_args(
        Path::new("mylib/src/lib.rs"),
        &args,
        Path::new("/tmp/kache-depinfo/deps.d"),
    );

    assert_eq!(
        dep_args.first().map(String::as_str),
        Some("mylib/src/lib.rs"),
        "the source file leads the argv exactly once: {dep_args:?}"
    );
    assert_eq!(
        dep_args.iter().filter(|a| a.contains("lib.rs")).count(),
        1,
        "cargo's own positional source must not be re-added: {dep_args:?}"
    );
    assert!(
        !dep_args.iter().any(|a| a.contains("extra-filename")),
        "-C extra-filename names outputs the pre-pass discards, and rustc \
             warns about it as soon as -o is present: {dep_args:?}"
    );
    assert!(
        !dep_args.iter().any(|a| a.contains("incremental")),
        "incremental flags go through the canonical filter: {dep_args:?}"
    );
    assert!(
        !dep_args.iter().any(|a| a.starts_with("--emit=")),
        "cargo's --emit is superseded by the pre-pass's own: {dep_args:?}"
    );
    assert!(
        !dep_args.iter().any(|a| a == "--out-dir"),
        "--out-dir is the other flag rustc reports as ignored due to -o: {dep_args:?}"
    );
    assert!(
        !dep_args.iter().any(|a| a.starts_with("/w/target")),
        "--out-dir's value must go with it: {dep_args:?}"
    );

    // Everything that shapes the source closure survives.
    for kept in [
        "--crate-name",
        "mylib",
        "--edition=2021",
        "--error-format=json",
        "--crate-type",
        "lib",
        "-C",
        "metadata=1b2c9f1c31209a4a",
        "-L",
        "dependency=/w/target/debug/deps",
    ] {
        assert!(
            dep_args.iter().any(|a| a == kept),
            "{kept} shapes the input set and must survive: {dep_args:?}"
        );
    }
    assert_eq!(
        dep_args.iter().filter(|a| a.as_str() == "-C").count(),
        1,
        "only extra-filename's own -C is dropped, not every -C: {dep_args:?}"
    );

    let tail = &dep_args[dep_args.len() - 4..];
    assert_eq!(
        tail,
        [
            "--emit",
            "dep-info",
            "-o",
            "/tmp/kache-depinfo/deps.d".to_string().as_str()
        ]
        .map(String::from),
        "the pre-pass appends exactly one output configuration"
    );
}

#[test]
fn dep_info_pass_args_drops_every_extra_filename_spelling() {
    // rustc accepts four spellings of a codegen option; RUSTFLAGS and
    // hand-rolled invocations use the joined ones cargo never emits.
    for spelling in [
        vec!["-C", "extra-filename=-abc123"],
        vec!["-Cextra-filename=-abc123"],
        vec!["--codegen", "extra-filename=-abc123"],
        vec!["--codegen=extra-filename=-abc123"],
        // rustc normalises `_` to `-` in option names, so the underscore
        // spelling reaches the same flag.
        vec!["-C", "extra_filename=-abc123"],
        vec!["-Cextra_filename=-abc123"],
        vec!["--codegen", "extra_filename=-abc123"],
        vec!["--codegen=extra_filename=-abc123"],
    ] {
        let mut args: Vec<String> = vec!["--crate-type".into(), "lib".into()];
        args.extend(spelling.iter().map(|arg| (*arg).to_string()));
        args.push("--crate-name".into());
        args.push("mylib".into());

        let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

        assert!(
            !dep_args
                .iter()
                .any(|a| a.contains("extra-filename") || a.contains("extra_filename")),
            "{spelling:?} must be dropped: {dep_args:?}"
        );
        assert!(
            dep_args.iter().any(|a| a == "--crate-name"),
            "{spelling:?} must not swallow the following flag: {dep_args:?}"
        );
    }
}

#[test]
fn dep_info_pass_args_keeps_bare_trailing_codegen_flag() {
    // A trailing `-C` with no value is malformed, but the pre-pass must
    // hand it to rustc unchanged rather than guess — rustc's own error is
    // the honest outcome.
    let args = vec![
        "--crate-type".to_string(),
        "lib".to_string(),
        "-C".to_string(),
    ];

    let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

    assert!(
        dep_args.iter().any(|a| a == "-C"),
        "a valueless -C is not an extra-filename: {dep_args:?}"
    );
}

#[test]
fn dep_info_pass_args_drops_joined_output_flag() {
    // rustc accepts `-o` with its value attached, and reads single-dash
    // `-out-dir` as `-o` plus junk ("option `-o` has no space between
    // flag name and value"). A leftover joins the pre-pass's own `-o`
    // and rustc exits 1 with "Option 'o' given more than once" — every
    // build using that spelling stays a passthrough
    // (kunobi-ninja/kache#896).
    let args: Vec<String> = [
        "--crate-name",
        "mylib",
        "-o/tmp/original.rlib",
        "--edition=2021",
        "-O",
        "--out-dir=/tmp/original-deps",
        "-out-dir",
        "/tmp/still-positional",
        "src/lib.rs",
    ]
    .iter()
    .map(|arg| (*arg).to_string())
    .collect();

    let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

    let outputs: Vec<&String> = dep_args.iter().filter(|a| a.starts_with("-o")).collect();
    assert_eq!(
        outputs,
        ["-o"],
        "only the pre-pass's own -o may survive: {dep_args:?}"
    );
    for dropped in [
        "-o/tmp/original.rlib",
        "-out-dir",
        "--out-dir=/tmp/original-deps",
    ] {
        assert!(
            !dep_args.iter().any(|a| a == dropped),
            "{dropped} names an output the pre-pass discards: {dep_args:?}"
        );
    }
    assert!(
        dep_args.iter().any(|a| a == "/tmp/still-positional"),
        "single-dash -out-dir takes no separate value: rustc reads the next \
             token as a positional, and the pre-pass must fail on it exactly as \
             the real build does: {dep_args:?}"
    );
    assert!(
        dep_args.iter().any(|a| a == "-O"),
        "capital -O is opt-level, not output: {dep_args:?}"
    );
    assert!(
        dep_args.iter().any(|a| a == "--edition=2021"),
        "the token after a joined -o is a real flag, not its value: {dep_args:?}"
    );
    let tail = &dep_args[dep_args.len() - 4..];
    assert_eq!(
        tail,
        [
            "--emit",
            "dep-info",
            "-o",
            "/tmp/deps.d".to_string().as_str()
        ]
        .map(String::from),
        "the pre-pass appends exactly one output configuration"
    );
}

#[test]
fn dep_info_pass_args_strips_every_incremental_spelling() {
    // All four `-C incremental` spellings rustc accepts must go through
    // the canonical filter before the pre-pass runs: a leftover would aim
    // the dep-info run at cargo's incremental dir
    // (kunobi-ninja/kache#896). A bare `-C incremental` (no `=value`) is
    // not valid rustc — it stays, so rustc rejects it exactly as it
    // rejects the real build ("requires a string").
    let args: Vec<String> = [
        "--crate-name",
        "mylib",
        "-Cincremental=/tmp/incr-joined",
        "-C",
        "incremental=/tmp/incr-split",
        "--codegen=incremental=/tmp/incr-long-joined",
        "--codegen",
        "incremental=/tmp/incr-long-split",
        "src/lib.rs",
    ]
    .iter()
    .map(|arg| (*arg).to_string())
    .collect();

    let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

    assert!(
        !dep_args.iter().any(|a| a.contains("incremental")),
        "no incremental spelling may reach the pre-pass: {dep_args:?}"
    );
    assert!(
        dep_args.iter().any(|a| a == "mylib"),
        "stripping must not swallow neighbouring flags: {dep_args:?}"
    );

    let bare: Vec<String> = ["--crate-name", "mylib", "-C", "incremental", "src/lib.rs"]
        .iter()
        .map(|arg| (*arg).to_string())
        .collect();
    let dep_bare = dep_info_pass_args(Path::new("src/lib.rs"), &bare, Path::new("/tmp/deps.d"));

    assert!(
        dep_bare.iter().any(|a| a == "incremental"),
        "a valueless incremental is rustc's to reject, not the pre-pass's to guess: {dep_bare:?}"
    );
}

#[test]
fn dep_info_pass_prepass_succeeds_through_response_file() {
    // The `use_response_file` path — taken whenever cargo's own argv
    // arrived via `@file` — had no coverage. A crate under a path with
    // spaces exercises the verbatim one-arg-per-line round-trip, which is
    // exactly what an expanded largest-crate argv looks like
    // (kunobi-ninja/kache#896).
    let dir = tempfile::Builder::new()
        .prefix("kache depinfo space ")
        .tempdir()
        .unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
    std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

    let rustc = std::path::PathBuf::from("rustc");
    let source = src.join("lib.rs");
    let args = vec![
        "--crate-name".to_string(),
        "testcrate".to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition".to_string(),
        "2021".to_string(),
    ];

    let dep_info = run_dep_info_pass(&rustc, None, &source, &args, true)
        .expect("the response-file pre-pass must match the direct one");

    assert!(
        dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")),
        "expected the crate root: {:?}",
        dep_info.source_files
    );
    assert!(
        dep_info
            .source_files
            .iter()
            .any(|p| p.ends_with("server.rs")),
        "an incomplete source list is what makes a crate uncacheable: {:?}",
        dep_info.source_files
    );
}

#[test]
fn read_dep_info_file_rejects_non_utf8() {
    // A non-UTF8 filename or env value in the source closure lands
    // verbatim in rustc's dep-info output. The pre-pass must refuse it
    // with the encoding named — not a bare "stream did not contain valid
    // UTF-8" against an unnamed input set (kunobi-ninja/kache#896).
    let dir = tempfile::tempdir().unwrap();
    let dep_file = dir.path().join("deps.d");
    std::fs::write(&dep_file, b"/tmp/x.d: src/lib.rs\n").unwrap();
    assert_eq!(
        read_dep_info_file(&dep_file).unwrap(),
        "/tmp/x.d: src/lib.rs\n"
    );

    std::fs::write(&dep_file, b"/tmp/x.d: src/\xfflib.rs\n").unwrap();
    let err = format!("{:#}", read_dep_info_file(&dep_file).unwrap_err());
    assert!(
        err.contains("not valid UTF-8"),
        "the refusal must name the encoding: {err}"
    );
}

#[test]
fn dep_info_pass_prepass_succeeds_with_extra_filename() {
    // End-to-end regression for kunobi-ninja/kache#896: a lib-target argv
    // shaped like cargo's, carrying `-C extra-filename`, must produce a
    // clean pre-pass and the crate's full source closure — not a refusal.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
    std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

    let rustc = std::path::PathBuf::from("rustc");
    let source = src.join("lib.rs");
    let out_dir = dir.path().join("deps");
    let args: Vec<String> = vec![
        "--crate-name".into(),
        "testcrate".into(),
        "--edition=2021".into(),
        source.to_string_lossy().into_owned(),
        "--error-format=json".into(),
        "--crate-type".into(),
        "lib".into(),
        "--emit=dep-info,metadata,link".into(),
        "-C".into(),
        "metadata=92ee3169a2c3b7b0".into(),
        "-C".into(),
        "extra-filename=-2cb6bc2ef2b88725".into(),
        "--out-dir".into(),
        out_dir.to_string_lossy().into_owned(),
        "-C".into(),
        "incremental=/nonexistent/incremental".into(),
    ];

    let dep_info = run_dep_info_pass(&rustc, None, &source, &args, false)
        .expect("a cargo-shaped lib argv must not fail the pre-pass");

    assert!(
        dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")),
        "expected the crate root: {:?}",
        dep_info.source_files
    );
    assert!(
        dep_info
            .source_files
            .iter()
            .any(|p| p.ends_with("server.rs")),
        "an incomplete source list is what makes a crate uncacheable: {:?}",
        dep_info.source_files
    );
}

#[test]
fn first_rustc_error_line_skips_leading_json_warnings() {
    // The exact shape of the kunobi-ninja/kache#896 report: rustc emits
    // session-level warnings before any crate diagnostic, so the first
    // line is never the reason the run aborted.
    let stderr = concat!(
        r#"{"$message_type":"diagnostic","message":"ignoring -C extra-filename flag due to -o flag","level":"warning"}"#,
        "\n",
        r#"{"$message_type":"diagnostic","message":"cannot find macro `frobnicate`","level":"error"}"#,
        "\n",
        r#"{"$message_type":"diagnostic","message":"aborting due to 1 previous error","level":"error"}"#,
        "\n",
    );

    let line = first_rustc_error_line(stderr).expect("an error line is present");

    assert!(
        line.contains("cannot find macro"),
        "the first error-level diagnostic wins: {line}"
    );
}

#[test]
fn first_rustc_error_line_skips_leading_human_warnings() {
    let stderr = "warning: ignoring -C extra-filename flag due to -o flag\n\
                      \n\
                      error[E0433]: failed to resolve: use of undeclared crate `nope`\n\
                      error: aborting due to 1 previous error\n";

    let line = first_rustc_error_line(stderr).expect("an error line is present");

    assert_eq!(
        line,
        "error[E0433]: failed to resolve: use of undeclared crate `nope`"
    );
}

#[test]
fn first_rustc_error_line_matches_unnumbered_human_errors() {
    let stderr = "warning: unused import: `std::io`\nerror: expected one of `!` or `::`\n";

    let line = first_rustc_error_line(stderr).expect("an error line is present");

    assert_eq!(line, "error: expected one of `!` or `::`");
}

#[test]
fn first_rustc_error_line_falls_back_to_the_first_content_line() {
    // rustc killed by a signal, or a wrapper that failed before rustc ran,
    // leaves no error-level diagnostic. Report something rather than
    // "(no output)" — a refusal with no stated cause is what
    // kunobi-ninja/kache#431 was about.
    let line = first_rustc_error_line("\n\nwarning: something odd\nnote: more\n");

    assert_eq!(line, Some("warning: something odd"));
    assert_eq!(
        first_rustc_error_line("   \n \n"),
        None,
        "blank is no cause"
    );
    assert_eq!(first_rustc_error_line(""), None);
}

// --- cache key module-change detection test ---

#[test]
fn test_cache_key_changes_with_module_file() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();

    std::fs::write(src.join("lib.rs"), b"mod utils;\npub fn hello() {}").unwrap();
    std::fs::write(src.join("utils.rs"), b"pub fn helper() {}").unwrap();

    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mylib".to_string(),
        src.join("lib.rs").to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition=2021".to_string(),
    ];

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();

    let parsed1 = RustcArgs::parse(&args_vec).unwrap();
    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();

    // Modify the module file (NOT lib.rs)
    std::fs::write(
        src.join("utils.rs"),
        b"pub fn helper() { println!(\"changed\"); }",
    )
    .unwrap();

    let parsed2 = RustcArgs::parse(&args_vec).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_ne!(
        key1, key2,
        "cache key must change when a module file changes"
    );
}

// --- cache key determinism with multiple source files ---

#[test]
fn test_cache_key_stable_with_module_files() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();

    std::fs::write(src.join("lib.rs"), b"mod a;\nmod b;\npub fn lib_fn() {}").unwrap();
    std::fs::write(src.join("a.rs"), b"pub fn a_fn() {}").unwrap();
    std::fs::write(src.join("b.rs"), b"pub fn b_fn() {}").unwrap();

    let args_vec: Vec<String> = vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "testcrate".to_string(),
        src.join("lib.rs").to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition=2021".to_string(),
    ];

    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();

    let parsed1 = RustcArgs::parse(&args_vec).unwrap();
    let parsed2 = RustcArgs::parse(&args_vec).unwrap();

    let key1 = compute_cache_key(&parsed1, &fh, &pn, &KeyEnv::default()).unwrap();
    let key2 = compute_cache_key(&parsed2, &fh, &pn, &KeyEnv::default()).unwrap();

    assert_eq!(
        key1, key2,
        "cache key must be deterministic with multiple source files"
    );
}

// ────────────────────────────────────────────────────────────────
// rustc cache-key correctness matrix
//
// Property under test: the key must react to every input that
// changes rustc's *output artifact*, and must NOT react to inputs
// that only affect diagnostics. A missed input is a miscache (a
// false hit serving the wrong artifact); a spurious change is
// over-keying (a missed hit, wasted work).
//
// The testable seam is `compute_cache_key` itself: it forks rustc
// (`get_rustc_version`, the `--emit=dep-info` pre-pass) and reads
// source files, so each case builds a real temp `.rs` file plus a
// `RustcArgs` and compares two keys. Cases that depend on env vars
// (`RUSTFLAGS`) mutate the process environment and are isolated
// onto a serial mutex so parallel test threads can't interleave.
// ────────────────────────────────────────────────────────────────

/// Serializes every test that computes a cache key through the shared
/// process-state lock also used by argument-parser tests.
///
/// `compute_cache_key` reads process-wide env (`RUSTFLAGS`,
/// `CARGO_ENCODED_RUSTFLAGS`, `CARGO_CFG_*`); the env-mutating
/// matrix case below temporarily changes `RUSTFLAGS`. `cargo test`
/// runs tests as parallel threads of one process, so without a
/// shared lock any test's two key computations can straddle that
/// mutation and observe a spurious key difference — that is exactly
/// how the `assert_eq` tests `test_cache_key_deterministic` and
/// `test_coverage_keys_consistent_across_remap_forms` flaked on CI.
///
/// The lock is therefore NOT matrix-scoped: every test that calls
/// `compute_cache_key` — matrix or not — holds it for its full
/// duration. New key tests must do the same; that is the price of
/// `compute_cache_key` reading process-global env directly.
/// Keep the local name used throughout this large test module while the
/// underlying lock remains shared with other process-state observers.
fn key_test_lock() -> crate::test_support::ProcessStateTestGuard {
    process_state_test_lock()
}

/// Key computation stashes this compile's unit identity and its per-extern
/// producer ids, and each is TAKEN — a second read yields `None`, so a
/// compile that computes no rustc key cannot inherit the previous one's
/// identities from the same process (kunobi-ninja/kache#627).
#[test]
fn key_computation_stashes_unit_identity_and_yields_it_once() {
    let _lock = key_test_lock();
    let args: Vec<String> = [
        "rustc",
        "--crate-name",
        "app",
        "src/lib.rs",
        "-C",
        "extra-filename=-843f02d6a46ebef1",
        "--extern",
        "foo_old=/w/target/debug/deps/libfoo-0532daf0ee3516f0.rlib",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let mut parsed = RustcArgs::parse(&args).unwrap();
    // Skip the dep-info pre-pass: this is about what the externs loop
    // records, not source discovery.
    parsed.source_file = None;

    compute_cache_key(
        &parsed,
        &FileHasher::new(),
        &PathNormalizer::empty(),
        &KeyEnv::default(),
    )
    .unwrap();

    assert_eq!(
        take_last_key_unit_id().as_deref(),
        Some("843f02d6a46ebef1"),
        "the compile's own `-C extra-filename`"
    );
    assert_eq!(take_last_key_unit_id(), None, "taken, not copied");

    let units = take_last_key_extern_units().expect("recorded with the digests");
    assert_eq!(
        units.get("foo_old").map(String::as_str),
        // Keyed by the alias the consumer used; the value is the producer's
        // identity, recovered from the artifact filename even though that
        // file does not exist here.
        Some("0532daf0ee3516f0")
    );
    assert_eq!(take_last_key_extern_units(), None, "taken, not copied");
}

/// The key stashes are per-thread, so a concurrent key computation can
/// neither overwrite nor consume another's (kunobi-ninja/kache#777).
///
/// Before the stashes were thread-local this was the shape that made
/// `key_computation_stashes_unit_identity_and_yields_it_once` flaky under
/// the default `cargo test` parallelism: any other thread computing a key
/// between a compute and its take would win the race. The outer
/// `key_test_lock` still serializes this group against the env-mutating
/// matrix; what runs concurrently here is the stash access itself.
#[test]
fn key_stashes_do_not_leak_across_threads() {
    let _lock = key_test_lock();

    // Distinct unit ids per thread, so a leak shows up as another thread's
    // value rather than as an absence.
    let units = ["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"];
    std::thread::scope(|scope| {
        for unit in units {
            scope.spawn(move || {
                let extra = format!("extra-filename=-{unit}");
                let extern_arg = format!("dep_{unit}=/w/target/debug/deps/libdep-{unit}.rlib");
                let args: Vec<String> = [
                    "rustc",
                    "--crate-name",
                    "app",
                    "src/lib.rs",
                    "-C",
                    &extra,
                    "--extern",
                    &extern_arg,
                ]
                .iter()
                .map(|s| s.to_string())
                .collect();
                let mut parsed = RustcArgs::parse(&args).unwrap();
                // Skip the dep-info pre-pass, as above: forking rustc from
                // every thread would swamp the point being made.
                parsed.source_file = None;

                // Loop so the interleaving windows overlap rather than each
                // thread racing through its compute/take once.
                for _ in 0..16 {
                    compute_cache_key(
                        &parsed,
                        &FileHasher::new(),
                        &PathNormalizer::empty(),
                        &KeyEnv::default(),
                    )
                    .unwrap();

                    assert_eq!(
                        take_last_key_unit_id().as_deref(),
                        Some(unit),
                        "each thread sees its own unit id"
                    );
                    assert_eq!(take_last_key_unit_id(), None, "taken, not copied");

                    let recorded = take_last_key_extern_units().expect("recorded with the digests");
                    assert_eq!(
                        recorded.get(&format!("dep_{unit}")).map(String::as_str),
                        Some(unit),
                        "each thread sees its own extern units"
                    );
                    assert_eq!(take_last_key_extern_units(), None, "taken, not copied");

                    assert!(
                        take_last_key_externs().is_some(),
                        "digests ride the same thread as their identities"
                    );
                    assert!(take_last_key_fields().is_some(), "per-group digests too");
                }
            });
        }
    });
}

/// True if a bare `rustc` is invocable. `compute_cache_key` forks
/// rustc for the version probe and dep-info pre-pass; without it
/// the key still computes (the pre-pass falls back) but the
/// matrix's intent is to exercise the real path. Guard-skip when
/// absent, consistent with other compiler-forking tests.
fn rustc_available() -> bool {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[cfg(unix)]
fn rustc_exe_name() -> &'static str {
    "rustc"
}

#[cfg(unix)]
fn rustc_path_on_path() -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(rustc_exe_name()))
        .find(|path| path.is_file())
}

#[cfg(unix)]
fn shell_single_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

#[cfg(unix)]
fn write_rustc_version_wrapper(root: &Path, subdir: &str, version: &str) -> PathBuf {
    let real_rustc = rustc_path_on_path().expect("rustc should be on PATH");
    let dir = root.join(subdir);
    std::fs::create_dir_all(&dir).unwrap();
    let wrapper = dir.join("rustc");
    let script = format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"--version\" ] && [ \"$2\" = \"--verbose\" ]; then\n\
cat <<'KACHE_RUSTC_VERSION'\n\
{version}\n\
KACHE_RUSTC_VERSION\n\
exit 0\n\
fi\n\
exec {} \"$@\"\n",
        shell_single_quote(&real_rustc)
    );
    kache_fs::testutil::write_executable(&wrapper, script);
    wrapper
}

/// Build a minimal lib-crate arg vector around a temp source file.
/// Callers push the dimension-under-test onto the returned vec.
fn base_args(source: &Path) -> Vec<String> {
    vec![
        "rustc".to_string(),
        "--crate-name".to_string(),
        "mxcrate".to_string(),
        source.to_string_lossy().to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        "--edition=2021".to_string(),
    ]
}

/// Compute a key for an arg vector. The source file path is
/// already embedded in `args` (positional) — `RustcArgs::parse`
/// picks it up — so no separate source argument is needed.
fn key_for(args: &[String]) -> String {
    key_with(args, &KeyEnv::capture())
}

/// [`key_for`] with the environment `env`.
fn key_with(args: &[String], env: &KeyEnv) -> String {
    let parsed = RustcArgs::parse(args).unwrap();
    let fh = FileHasher::new();
    let pn = PathNormalizer::empty();
    compute_cache_key(&parsed, &fh, &pn, env).unwrap()
}

/// A snapshot holding only `name=value`.
fn env_with(name: &str, value: &str) -> KeyEnv {
    KeyEnv::from_parts([(name, value)], None)
}

fn restore_env_var(key: &str, old: Option<std::ffi::OsString>) {
    match old {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

/// `KACHE_RUSTC_PATH_NORMALIZE=0` bakes real machine-local paths into DWARF
/// (no `--remap-path-prefix`), so the opt-out key MUST be path-local:
/// otherwise two checkouts at different paths compute the same key and a
/// shared cache serves one checkout's real-path artifact to another
/// (kunobi-ninja/kache#480). This pins the exact regression — two builds
/// that differ ONLY in cwd (the DWARF `comp_dir`) must get different opt-out
/// keys, while the default (remapped) build stays cwd-portable.
#[test]
fn opt_out_key_is_path_local_but_default_stays_portable() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
    let old_cwd = std::env::current_dir().unwrap();

    // Two identical crates at DIFFERENT paths; source arg is relative
    // ("lib.rs") so cwd is the only thing that varies — exactly what cargo
    // passes and what makes `comp_dir` the discriminator.
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    std::fs::write(dir_a.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
    std::fs::write(dir_b.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
    let args = base_args(Path::new("lib.rs"));

    // SAFETY: env access is serialized by the process-state test lock; restored below.
    unsafe { std::env::set_var("KACHE_RUSTC_PATH_NORMALIZE", "0") };
    std::env::set_current_dir(dir_a.path()).unwrap();
    let optout_a = key_for(&args);
    std::env::set_current_dir(dir_b.path()).unwrap();
    let optout_b = key_for(&args);

    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);
    std::env::set_current_dir(dir_a.path()).unwrap();
    let default_a = key_for(&args);
    std::env::set_current_dir(dir_b.path()).unwrap();
    let default_b = key_for(&args);

    std::env::set_current_dir(&old_cwd).unwrap();
    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);

    assert_ne!(
        optout_a, optout_b,
        "opt-out builds bake real paths, so keys must be cwd-local"
    );
    assert_eq!(
        default_a, default_b,
        "default (remapped) builds must stay cwd-portable"
    );
    assert_ne!(
        optout_a, default_a,
        "opt-out must be a separate namespace from remapped builds"
    );
}

/// The opt-out fold must cover EVERY prefix the normalizer would have
/// remapped — not just cwd/$HOME. A build-script `OUT_DIR` lives under
/// `$CARGO_TARGET_DIR`, which the default key normalizes to `<TARGET>`; if
/// the opt-out fold missed that prefix, two builds under different target
/// dirs (each baking a different real `OUT_DIR` path into DWARF) would
/// collide on one opt-out key. Folding the normalizer's own `raw_prefixes`
/// closes that gap by construction. Pins it: changing only
/// `$CARGO_TARGET_DIR` changes the opt-out key but not the (portable)
/// default key.
#[test]
fn opt_out_key_folds_all_normalizer_prefixes_not_just_home() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
    let old_target = std::env::var_os("CARGO_TARGET_DIR");

    let ws = tempfile::tempdir().unwrap();
    let src = ws.path().join("lib.rs");
    std::fs::write(&src, "pub fn f() {}\n").unwrap();
    let target_a = tempfile::tempdir().unwrap();
    let target_b = tempfile::tempdir().unwrap();
    // Absolute source (same for both variants) so the pre-pass finds it
    // regardless of cwd; $CARGO_TARGET_DIR is then the only thing that varies.
    let args = base_args(&src);

    // Build the normalizer AFTER setting CARGO_TARGET_DIR so its `<TARGET>`
    // rule reflects the current target dir (from_env reads the env).
    let key = || {
        let parsed = RustcArgs::parse(&args).unwrap();
        let fh = FileHasher::new();
        let pn = PathNormalizer::from_env(Some(ws.path()));
        compute_cache_key(&parsed, &fh, &pn, &KeyEnv::default()).unwrap()
    };

    // SAFETY: env access is serialized by the process-state test lock; restored below.
    unsafe { std::env::set_var("KACHE_RUSTC_PATH_NORMALIZE", "0") };
    unsafe { std::env::set_var("CARGO_TARGET_DIR", target_a.path()) };
    let optout_a = key();
    unsafe { std::env::set_var("CARGO_TARGET_DIR", target_b.path()) };
    let optout_b = key();

    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);
    unsafe { std::env::set_var("CARGO_TARGET_DIR", target_a.path()) };
    let default_a = key();
    unsafe { std::env::set_var("CARGO_TARGET_DIR", target_b.path()) };
    let default_b = key();

    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);
    restore_env_var("CARGO_TARGET_DIR", old_target);

    assert_ne!(
        optout_a, optout_b,
        "opt-out key must fold the raw $CARGO_TARGET_DIR prefix (OUT_DIR lives under it)"
    );
    assert_eq!(
        default_a, default_b,
        "default build normalizes $CARGO_TARGET_DIR to <TARGET>, so it stays portable"
    );
}

/// Coverage builds skip remap injection too (llvm-cov / tarpaulin need real
/// paths in the profraw), so they bake real machine-local paths into DWARF
/// exactly like the `KACHE_RUSTC_PATH_NORMALIZE=0` opt-out. Their `remap:none`
/// key must therefore be path-local as well, or a shared cache serves one
/// checkout's real-path coverage artifact to another (the coverage analog of
/// kunobi-ninja/kache#480). Pins it: two coverage builds differing only in
/// cwd must get different keys.
#[test]
fn coverage_key_is_path_local() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let old_cwd = std::env::current_dir().unwrap();
    // Force the opt-out OFF so this test exercises the COVERAGE `remap:none`
    // path specifically. If `KACHE_RUSTC_PATH_NORMALIZE=0` were set in the
    // ambient env, `path_normalize_disabled` would be true and the fold
    // would fire via the opt-out — passing even if coverage regressed to the
    // old opt-out-only condition. Removing it pins the coverage path.
    let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    std::fs::write(dir_a.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
    std::fs::write(dir_b.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
    let mut args = base_args(Path::new("lib.rs"));
    args.push("-Cinstrument-coverage".to_string());

    // Sanity-check the fixture is the coverage path, not the opt-out path.
    let parsed = RustcArgs::parse(&args).unwrap();
    assert!(parsed.has_coverage_instrumentation());
    assert!(
        !parsed.path_normalize_disabled,
        "test must exercise the coverage remap:none path, not the opt-out path"
    );

    std::env::set_current_dir(dir_a.path()).unwrap();
    let cov_a = key_for(&args);
    std::env::set_current_dir(dir_b.path()).unwrap();
    let cov_b = key_for(&args);

    std::env::set_current_dir(&old_cwd).unwrap();
    restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);

    assert_ne!(
        cov_a, cov_b,
        "coverage builds bake real paths, so their keys must be cwd-local"
    );
}

#[test]
fn key_rustc_bootstrap_presence_changes_key_but_empty_is_identity() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, "pub fn f() {}\n").unwrap();
    let args = base_args(&src);

    let key_unset = key_with(&args, &KeyEnv::default());
    // Empty must hash identically to unset: that is what keeps existing
    // caches valid (no CACHE_KEY_VERSION bump for the common case).
    let key_empty = key_with(&args, &env_with("RUSTC_BOOTSTRAP", ""));
    let key_set = key_with(&args, &env_with("RUSTC_BOOTSTRAP", "1"));
    let key_other = key_with(&args, &env_with("RUSTC_BOOTSTRAP", "some_crate"));

    assert_eq!(
        key_unset, key_empty,
        "empty RUSTC_BOOTSTRAP must equal unset"
    );
    assert_ne!(key_unset, key_set, "RUSTC_BOOTSTRAP=1 must change the key");
    assert_ne!(
        key_set, key_other,
        "different RUSTC_BOOTSTRAP values must differ"
    );
}

#[test]
fn key_cargo_encoded_rustflags_changes_key() {
    // cargo passes flags via CARGO_ENCODED_RUSTFLAGS (\x1f-separated); they
    // affect codegen, so they must be folded into the key.
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, "pub fn f() {}\n").unwrap();
    let args = base_args(&src);

    let key_unset = key_with(&args, &KeyEnv::default());
    let flags = |value| env_with("CARGO_ENCODED_RUSTFLAGS", value);
    let key_set = key_with(&args, &flags("-C\x1ftarget-cpu=native"));
    let key_other = key_with(&args, &flags("-C\x1ftarget-cpu=x86-64-v3"));

    assert_ne!(
        key_unset, key_set,
        "setting CARGO_ENCODED_RUSTFLAGS must change the key"
    );
    assert_ne!(
        key_set, key_other,
        "different encoded rustflags must diverge the key"
    );
}

#[test]
fn key_cargo_cfg_env_changes_key() {
    // CARGO_CFG_* vars (cargo's reflection of `--cfg`) are folded into the
    // key so a build-script cfg change diverges it.
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }
    let var = "CARGO_CFG_KACHE_TEST_FLAG";
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, "pub fn f() {}\n").unwrap();
    let args = base_args(&src);

    let key_unset = key_with(&args, &KeyEnv::default());
    let key_set = key_with(&args, &env_with(var, "1"));
    let key_other = key_with(&args, &env_with(var, "2"));

    assert_ne!(key_unset, key_set, "a CARGO_CFG_* var must change the key");
    assert_ne!(
        key_set, key_other,
        "a different CARGO_CFG_* value must diverge the key"
    );
}

// ── "should change" cases — varying a codegen-affecting input ──

#[cfg(unix)]
#[test]
fn key_matrix_rustc_version_changes_key() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let rustc_a = write_rustc_version_wrapper(
        dir.path(),
        "toolchain-a",
        "rustc 1.95.0-test-a\nbinary: test-a\ncommit-hash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let rustc_b = write_rustc_version_wrapper(
        dir.path(),
        "toolchain-b",
        "rustc 1.95.0-test-b\nbinary: test-b\ncommit-hash: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );

    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut args_a = base_args(&source);
    args_a[0] = rustc_a.to_string_lossy().into_owned();
    let mut args_b = base_args(&source);
    args_b[0] = rustc_b.to_string_lossy().into_owned();

    assert_ne!(
        key_for(&args_a),
        key_for(&args_b),
        "`rustc --version --verbose` output must affect the cache key"
    );
}

#[test]
fn key_matrix_manifest_dir_runtime_env_path_changes_key_across_workspaces() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }

    let old_manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    let dir = tempfile::tempdir().unwrap();
    let workspace_a = dir.path().join("checkout-a");
    let workspace_b = dir.path().join("checkout-b");

    fn write_helper(workspace: &Path) -> PathBuf {
        let src = workspace.join("helper/src");
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            r#"pub fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
"#,
        )
        .unwrap();
        lib
    }

    let source_a = write_helper(&workspace_a);
    let source_b = write_helper(&workspace_b);
    let fh = FileHasher::new();

    let manifest_a = workspace_a.join("helper").canonicalize().unwrap();
    unsafe {
        std::env::set_var("CARGO_MANIFEST_DIR", manifest_a);
    }
    let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
    let pn_a = PathNormalizer::from_env(Some(&workspace_a));
    let key_a = compute_cache_key(&parsed_a, &fh, &pn_a, &KeyEnv::capture()).unwrap();

    let manifest_b = workspace_b.join("helper").canonicalize().unwrap();
    unsafe {
        std::env::set_var("CARGO_MANIFEST_DIR", manifest_b);
    }
    let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
    let pn_b = PathNormalizer::from_env(Some(&workspace_b));
    let key_b = compute_cache_key(&parsed_b, &fh, &pn_b, &KeyEnv::capture()).unwrap();

    restore_env_var("CARGO_MANIFEST_DIR", old_manifest_dir);

    assert_ne!(
        key_a, key_b,
        "CARGO_MANIFEST_DIR is a runtime env! value and must stay checkout-specific"
    );
}

#[test]
fn key_matrix_out_dir_include_pattern_stays_stable_across_workspaces() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }

    let old_out_dir = std::env::var_os("OUT_DIR");
    let old_manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    let dir = tempfile::tempdir().unwrap();
    let workspace_a = dir.path().join("checkout-a");
    let workspace_b = dir.path().join("checkout-b");

    fn write_generated_include_crate(workspace: &Path) -> (PathBuf, PathBuf) {
        let src = workspace.join("src");
        let out_dir = workspace.join("target/debug/build/include-crate/out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();

        let generated = out_dir.join("generated.rs");
        std::fs::write(&generated, b"pub fn generated() -> u8 { 7 }\n").unwrap();

        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub fn value() -> u8 {
    generated()
}
"#,
        )
        .unwrap();
        (lib, out_dir)
    }

    let (source_a, out_a) = write_generated_include_crate(&workspace_a);
    let (source_b, out_b) = write_generated_include_crate(&workspace_b);
    let fh = FileHasher::new();

    let out_a = out_a.canonicalize().unwrap();
    unsafe {
        std::env::set_var("OUT_DIR", &out_a);
        std::env::set_var("CARGO_MANIFEST_DIR", &workspace_a);
    }
    let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
    let pn_a = PathNormalizer::from_env(Some(&workspace_a));
    let key_a = compute_cache_key(&parsed_a, &fh, &pn_a, &KeyEnv::capture()).unwrap();

    let out_b = out_b.canonicalize().unwrap();
    unsafe {
        std::env::set_var("OUT_DIR", &out_b);
        std::env::set_var("CARGO_MANIFEST_DIR", &workspace_b);
    }
    let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
    let pn_b = PathNormalizer::from_env(Some(&workspace_b));
    let key_b = compute_cache_key(&parsed_b, &fh, &pn_b, &KeyEnv::capture()).unwrap();

    restore_env_var("OUT_DIR", old_out_dir);
    restore_env_var("CARGO_MANIFEST_DIR", old_manifest_dir);

    assert_eq!(
        key_a, key_b,
        "OUT_DIR include!() paths should stay portable across workspaces"
    );
}

#[test]
fn key_matrix_out_dir_dual_pattern_diverges_across_workspaces() {
    let _lock = key_test_lock();
    if !rustc_available() {
        return;
    }

    let old_out_dir = std::env::var_os("OUT_DIR");
    let dir = tempfile::tempdir().unwrap();
    let workspace_a = dir.path().join("checkout-a");
    let workspace_b = dir.path().join("checkout-b");

    fn write_dual_pattern_crate(workspace: &Path) -> (PathBuf, PathBuf) {
        let src = workspace.join("src");
        let out_dir = workspace.join("target/debug/build/dual-crate/out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();

        let generated = out_dir.join("generated.rs");
        std::fs::write(&generated, b"pub fn generated() -> u8 { 7 }\n").unwrap();

        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");

pub fn value() -> (&'static str, u8) {
    (OUT_DIR_AT_COMPILE_TIME, generated())
}
"#,
        )
        .unwrap();
        (lib, out_dir)
    }

    let (source_a, out_a) = write_dual_pattern_crate(&workspace_a);
    let (source_b, out_b) = write_dual_pattern_crate(&workspace_b);
    let fh = FileHasher::new();

    let out_a = out_a.canonicalize().unwrap();
    unsafe {
        std::env::set_var("OUT_DIR", &out_a);
    }
    let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
    let pn_a = PathNormalizer::from_env(Some(&workspace_a));
    let key_a = compute_cache_key(&parsed_a, &fh, &pn_a, &KeyEnv::capture()).unwrap();

    let out_b = out_b.canonicalize().unwrap();
    unsafe {
        std::env::set_var("OUT_DIR", &out_b);
    }
    let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
    let pn_b = PathNormalizer::from_env(Some(&workspace_b));
    let key_b = compute_cache_key(&parsed_b, &fh, &pn_b, &KeyEnv::capture()).unwrap();

    restore_env_var("OUT_DIR", old_out_dir);

    assert_ne!(
        key_a, key_b,
        "OUT_DIR dual pattern must stay checkout-specific: include!() alone is path-only, \
             but env!(\"OUT_DIR\") as a runtime value bakes the absolute path into the artifact"
    );
}

#[test]
fn key_matrix_emit_changes_key() {
    // `cargo check` runs rustc --emit=metadata (-> .rmeta);
    // `cargo build` runs --emit=link (-> .rlib). Same crate, same
    // everything else the key hashes => without hashing `emit` the
    // two collide and a check entry could be served to a build.
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut metadata = base_args(&source);
    metadata.push("--emit=metadata".to_string());
    let mut link = base_args(&source);
    link.push("--emit=link".to_string());

    assert_ne!(
        key_for(&metadata),
        key_for(&link),
        "`--emit=metadata` vs `--emit=link` must produce different keys"
    );
}

#[test]
fn key_matrix_opt_level_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut o0 = base_args(&source);
    o0.extend(["-C".to_string(), "opt-level=0".to_string()]);
    let mut o3 = base_args(&source);
    o3.extend(["-C".to_string(), "opt-level=3".to_string()]);

    assert_ne!(
        key_for(&o0),
        key_for(&o3),
        "`-C opt-level` must affect the key"
    );
}

#[test]
fn key_matrix_debug_assertions_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut on = base_args(&source);
    on.extend(["-C".to_string(), "debug-assertions=on".to_string()]);
    let mut off = base_args(&source);
    off.extend(["-C".to_string(), "debug-assertions=off".to_string()]);

    assert_ne!(
        key_for(&on),
        key_for(&off),
        "`-C debug-assertions` must affect the key"
    );
}

#[test]
fn key_matrix_cfg_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let base = base_args(&source);
    let mut with_cfg = base_args(&source);
    with_cfg.extend(["--cfg".to_string(), "extra_feature".to_string()]);

    assert_ne!(
        key_for(&base),
        key_for(&with_cfg),
        "a `--cfg` value must affect the key"
    );
}

#[test]
fn key_matrix_feature_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut std_feat = base_args(&source);
    std_feat.extend(["--cfg".to_string(), "feature=\"std\"".to_string()]);
    let mut both = std_feat.clone();
    both.extend(["--cfg".to_string(), "feature=\"derive\"".to_string()]);

    assert_ne!(
        key_for(&std_feat),
        key_for(&both),
        "adding a feature must affect the key"
    );
}

#[test]
fn key_matrix_edition_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let mut e2018 = base_args(&source);
    e2018.retain(|a| a != "--edition=2021");
    e2018.push("--edition=2018".to_string());
    let e2021 = base_args(&source); // already --edition=2021

    assert_ne!(
        key_for(&e2018),
        key_for(&e2021),
        "`--edition` must affect the key"
    );
}

#[test]
fn key_matrix_target_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    // Two distinct triples — neither needs to be installed; the
    // key hashes the `--target` string, it does not invoke a
    // cross-compile.
    let mut t1 = base_args(&source);
    t1.push("--target=x86_64-unknown-linux-gnu".to_string());
    let mut t2 = base_args(&source);
    t2.push("--target=aarch64-apple-darwin".to_string());

    assert_ne!(
        key_of_flags(&t1),
        key_of_flags(&t2),
        "`--target` must affect the key"
    );
}

#[test]
fn key_matrix_crate_type_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let rlib = base_args(&source); // --crate-type lib
    let mut staticlib = base_args(&source);
    for a in staticlib.iter_mut() {
        if a == "lib" {
            *a = "staticlib".to_string();
        }
    }

    assert_ne!(
        key_for(&rlib),
        key_for(&staticlib),
        "`--crate-type` must affect the key"
    );
}

#[test]
fn key_matrix_rustflags_env_changes_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let args = base_args(&source);

    let key_none = key_with(&args, &KeyEnv::default());
    let key_set = key_with(&args, &env_with("RUSTFLAGS", "-C target-cpu=native"));

    assert_ne!(key_none, key_set, "`RUSTFLAGS` env var must affect the key");
}

/// Direct test of the `normalize_rustflags` helper.
#[test]
fn normalize_rustflags_collapses_whitespace() {
    assert_eq!(normalize_rustflags("-C a -C b"), "-C a -C b");
    // Multiple spaces between tokens → single space.
    assert_eq!(normalize_rustflags("-C a    -C b"), "-C a -C b");
    // Leading / trailing whitespace stripped.
    assert_eq!(normalize_rustflags("  -C a   -C b  "), "-C a -C b");
    // Mixed whitespace (tabs, newlines) treated as whitespace.
    assert_eq!(normalize_rustflags("-C a\t\t-C b"), "-C a -C b");
    // Order is preserved (rustc resolves later flags over earlier ones).
    assert_eq!(normalize_rustflags("-Cfoo=b   -Cfoo=a"), "-Cfoo=b -Cfoo=a");
    assert_ne!(
        normalize_rustflags("-Cfoo=a -Cfoo=b"),
        normalize_rustflags("-Cfoo=b -Cfoo=a")
    );
}

/// Direct test of the `scrub_remap_from_prefixes` helper.
#[test]
fn scrub_remap_from_prefixes_collapses_from_keeps_to() {
    let scrub = |s: &str| scrub_remap_from_prefixes(s.split_whitespace()).join(" ");

    // The core case: two checkouts converge, TO preserved.
    assert_eq!(
        scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
        "--remap-path-prefix=<REMAP_FROM>=/topsrcdir/"
    );
    assert_eq!(
        scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
        scrub("--remap-path-prefix=/abs/clone-b/=/topsrcdir/"),
        "different checkout `from` paths must collapse identically"
    );

    // Space-separated form: value is the next token.
    assert_eq!(
        scrub("--remap-path-prefix /abs/clone-a/=/topsrcdir/"),
        "--remap-path-prefix <REMAP_FROM>=/topsrcdir/"
    );

    // The clang `-f*-prefix-map` family (equals form).
    for flag in [
        "-ffile-prefix-map",
        "-fdebug-prefix-map",
        "-fmacro-prefix-map",
    ] {
        assert_eq!(
            scrub(&format!("{flag}=/abs/clone-a/=/virt/")),
            format!("{flag}=<REMAP_FROM>=/virt/")
        );
    }

    // Split on the LAST `=` (FROM may contain `=`), matching rustc/clang.
    assert_eq!(
        scrub("--remap-path-prefix=/a=b/clone-a/=/topsrcdir/"),
        "--remap-path-prefix=<REMAP_FROM>=/topsrcdir/"
    );

    // Changing TO must NOT be scrubbed away — it stays in the result.
    assert_ne!(
        scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
        scrub("--remap-path-prefix=/abs/clone-a/=/other/")
    );

    // Non-remap flags pass through verbatim.
    assert_eq!(
        scrub("-C opt-level=2 -C debuginfo=2"),
        "-C opt-level=2 -C debuginfo=2"
    );
    // A remap value with no `=` is malformed and left untouched.
    assert_eq!(
        scrub("--remap-path-prefix=garbage"),
        "--remap-path-prefix=garbage"
    );
}

#[test]
fn normalize_direct_remap_normalizes_known_from_but_keeps_to() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = PathBuf::from(crate::path_normalizer::canonical_string(&workspace).unwrap());
    let normalizer = PathNormalizer::from_env(Some(&workspace));
    let from = workspace.join("dir=with=equals");
    let to = workspace.join("literal-to");
    let value = format!("{}={}", from.display(), to.display());

    assert_eq!(
        normalize_direct_remap_value(&value, &normalizer),
        format!(
            "{}={}",
            Path::new("<WORKSPACE>").join("dir=with=equals").display(),
            to.display()
        ),
        "only FROM is normalized; TO remains verbatim"
    );
    assert_eq!(
        normalize_direct_remap_value("malformed", &normalizer),
        "malformed"
    );
}

#[test]
fn key_matrix_direct_remap_path_prefix_is_keyed_portably_and_in_order() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let none = key_of_flags(&flag_base(&source, &[]));
    let separated = key_of_flags(&flag_base(
        &source,
        &["--remap-path-prefix", "/work/clone-a=/virtual/src"],
    ));
    let attached = key_of_flags(&flag_base(
        &source,
        &["--remap-path-prefix=/work/clone-a=/virtual/src"],
    ));
    let unrelated_from = key_of_flags(&flag_base(
        &source,
        &["--remap-path-prefix=/work/clone-b=/virtual/src"],
    ));
    let other_target = key_of_flags(&flag_base(
        &source,
        &["--remap-path-prefix=/work/clone-a=/virtual/other"],
    ));
    let remap_root = format!("--remap-path-prefix={}=/virtual/root", dir.path().display());
    let remap_source = format!("--remap-path-prefix={}=/virtual/source", source.display());
    let order_ab = key_of_flags(&flag_base(&source, &[&remap_root, &remap_source]));
    let order_ba = key_of_flags(&flag_base(&source, &[&remap_source, &remap_root]));

    assert_ne!(none, separated, "adding a direct remap must change the key");
    assert_eq!(separated, attached, "both rustc spellings are equivalent");
    assert_ne!(
        separated, unrelated_from,
        "without a matching normalization rule, FROM remains semantic"
    );
    assert_ne!(
        separated, other_target,
        "the remap target is embedded in artifacts and must remain key-visible"
    );
    assert_ne!(order_ab, order_ba, "overlapping remap order is semantic");

    let clone_a = dir.path().join("clone-a");
    let clone_b = dir.path().join("clone-b");
    std::fs::create_dir_all(&clone_a).unwrap();
    std::fs::create_dir_all(&clone_b).unwrap();
    let portable_key = |workspace: &Path| {
        let workspace = workspace.canonicalize().unwrap();
        let remap = format!("--remap-path-prefix={}=/virtual/src", workspace.display());
        let mut parsed = RustcArgs::parse(&flag_base(&source, &[&remap])).unwrap();
        parsed.source_file = None;
        compute_cache_key(
            &parsed,
            &FileHasher::new(),
            &PathNormalizer::from_env(Some(&workspace)),
            &KeyEnv::default(),
        )
        .unwrap()
    };
    assert_eq!(
        portable_key(&clone_a),
        portable_key(&clone_b),
        "known workspace prefixes normalize portably across checkouts"
    );
}

/// The cross-checkout fix (v14): a build system's own `--remap-path-prefix`
/// (Firefox `--enable-path-remapping`) carries the checkout path on its
/// `from` side, so two clones at different paths must still hash identically
/// — and changing the stable `to` target must still diverge the key.
#[test]
fn key_matrix_rustflags_remap_path_prefix_stable_across_checkouts() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let args = base_args(&source);

    let key = |flags| key_with(&args, &env_with("RUSTFLAGS", flags));
    let key_a = key("--remap-path-prefix=/work/clone-a/=/topsrcdir/");
    let key_b = key("--remap-path-prefix=/work/clone-b/=/topsrcdir/");
    // Same flag, different stable target → must diverge.
    let key_other_to = key("--remap-path-prefix=/work/clone-a/=/elsewhere/");

    assert_eq!(
        key_a, key_b,
        "different checkout paths under the same remap target must not change the key"
    );
    assert_ne!(
        key_a, key_other_to,
        "changing the remap target (`to`) must still change the key"
    );
}

/// Cosmetic whitespace differences between cargo / mach assemblies
/// of the same logical RUSTFLAGS must not change the cache key.
/// Firefox bench surfaced this as the dominant source of leaf
/// cache-key divergence; this test pins the fix.
#[test]
fn key_matrix_rustflags_whitespace_does_not_change_key() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();
    let args = base_args(&source);

    let key = |flags| key_with(&args, &env_with("RUSTFLAGS", flags));
    let key_tight = key("-C debuginfo=2 -C codegen-units=1");
    // Same flags, cosmetically different whitespace.
    let key_loose = key("-C debuginfo=2    -C codegen-units=1");
    // Same flags, leading whitespace.
    let key_padded = key("  -C debuginfo=2 -C codegen-units=1  ");

    assert_eq!(
        key_tight, key_loose,
        "RUSTFLAGS extra-whitespace must not change the key"
    );
    assert_eq!(
        key_tight, key_padded,
        "RUSTFLAGS leading/trailing whitespace must not change the key"
    );
}

// ── "should NOT change" cases — diagnostics-only inputs ──
//
// These flags steer only what rustc *prints*, never the emitted
// artifact bytes. If the key changes for one of them, that is
// over-keying (a missed hit) — the test will fail and surface it
// rather than silently weakening the key.
//
// All lint configuration is deliberately absent from this section: it
// cannot change successful artifact bytes, but it can change whether the
// compile FAILS, and a hit replays success. See the tests below.

#[test]
fn key_matrix_outcome_lint_configuration_changes_key() {
    // `-D warnings` promotes warnings to hard errors: two builds
    // differing only here can disagree about whether the compile
    // succeeded while emitting byte-identical objects on success.
    // Since a hit replays success, the key MUST move (review
    // finding #2) — otherwise an entry stored without the gate
    // serves green to a build the gate should have failed.
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let base = base_args(&source);
    let mut with_lint = base_args(&source);
    with_lint.extend(["-D".to_string(), "warnings".to_string()]);
    let mut with_forbid = base_args(&source);
    with_forbid.extend(["--forbid".to_string(), "warnings".to_string()]);
    let mut with_cap = base_args(&source);
    with_cap.extend(["--cap-lints".to_string(), "allow".to_string()]);
    let mut with_attached = base_args(&source);
    with_attached.push("-Dwarnings".to_string());
    let mut with_warn = base_args(&source);
    with_warn.extend(["-W".to_string(), "unused".to_string()]);
    let mut with_allow = base_args(&source);
    with_allow.extend(["-A".to_string(), "dead_code".to_string()]);
    let mut with_check_cfg = base_args(&source);
    with_check_cfg.extend(["--check-cfg".to_string(), "cfg(foo)".to_string()]);
    let mut with_other_check_cfg = base_args(&source);
    with_other_check_cfg.push("--check-cfg=cfg(bar)".to_string());

    assert_ne!(
        key_for(&base),
        key_for(&with_lint),
        "an outcome-affecting lint gate (`-D warnings`) changes whether \
             the compile fails and MUST change the key"
    );
    assert_ne!(
        key_for(&base),
        key_for(&with_forbid),
        "`--forbid` is outcome-affecting and must change the key"
    );
    assert_ne!(
        key_for(&base),
        key_for(&with_cap),
        "`--cap-lints` re-levels every lint and must change the key"
    );
    assert_ne!(
        key_for(&with_lint),
        key_for(&with_attached),
        "separated (`-D warnings`) and attached (`-Dwarnings`) spellings \
             carry different tokens; each keys distinctly by design"
    );
    assert_ne!(
        key_for(&base),
        key_for(&with_warn),
        "-W can activate a lint that a deny group makes fatal"
    );
    assert_ne!(
        key_for(&base),
        key_for(&with_allow),
        "-A can relax an otherwise fatal lint"
    );
    assert_ne!(
        key_for(&base),
        key_for(&with_check_cfg),
        "--check-cfg controls the unexpected_cfgs outcome"
    );
    assert_ne!(
        key_for(&with_check_cfg),
        key_for(&with_other_check_cfg),
        "different accepted cfg sets must not share a key"
    );
}

#[test]
fn check_cfg_values_are_not_path_normalized() {
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let root = tempfile::tempdir().unwrap();
    let key_at = |workspace: &Path, with_check_cfg: bool| {
        std::fs::create_dir_all(workspace).unwrap();
        let source = workspace.join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let mut args = base_args(&source);
        if with_check_cfg {
            let semantic_path = workspace
                .join("generated")
                .to_string_lossy()
                .replace('\\', "/");
            args.extend([
                "--check-cfg".to_string(),
                format!("cfg(build_path, values(\"{semantic_path}\"))"),
            ]);
        }
        let parsed = RustcArgs::parse(&args).unwrap();
        compute_cache_key(
            &parsed,
            &FileHasher::new(),
            &PathNormalizer::from_env(Some(workspace)),
            &KeyEnv::default(),
        )
        .unwrap()
    };

    let workspace_a = root.path().join("checkout-a");
    let workspace_b = root.path().join("checkout-b");
    assert_eq!(
        key_at(&workspace_a, false),
        key_at(&workspace_b, false),
        "the control must prove ordinary workspace paths normalize portably"
    );
    assert_ne!(
        key_at(&workspace_a, true),
        key_at(&workspace_b, true),
        "path-looking check-cfg values are semantic strings and must stay raw"
    );
}

#[test]
fn key_matrix_outcome_lint_gates_key_by_pairing_not_multiset() {
    // The gates are captured as a flat token stream, so folding them
    // sorted would collapse permutations that mean different things:
    // `-D unsafe_code -F warnings` denies unsafe_code (a `#[allow]` in
    // the crate can still re-allow it) and forbids warnings, while the
    // swap forbids unsafe_code (no `#[allow]` escape) and denies
    // warnings. Same token multiset, different outcomes — so the key
    // must distinguish them.
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let gated = |gates: &[&str]| {
        let mut args = base_args(&source);
        args.extend(gates.iter().map(|s| s.to_string()));
        key_for(&args)
    };

    assert_ne!(
        gated(&["-D", "unsafe_code", "-F", "warnings"]),
        gated(&["-F", "unsafe_code", "-D", "warnings"]),
        "swapping which lint is denied and which is forbidden changes \
             the outcome and MUST change the key"
    );
    // (`--force-warn` takes a single lint, never a group, so both sides
    // name concrete lints.)
    assert_ne!(
        gated(&["-D", "unused_mut", "--force-warn", "deprecated"]),
        gated(&["-D", "deprecated", "--force-warn", "unused_mut"]),
        "swapping the deny and force-warn targets changes the outcome \
             and MUST change the key"
    );
    // Argv order is kept as the conservative choice: rustc resolves
    // repeated levels for the same lint last-wins, so a reordered gate
    // list can be a different build. A given build config emits a stable
    // order, so preserving it costs no hits.
    assert_ne!(
        gated(&["-D", "warnings", "-A", "unused", "-D", "unused"]),
        gated(&["-D", "unused", "-A", "unused", "-D", "warnings"]),
        "gate order is preserved in the key"
    );
}

#[test]
fn key_matrix_error_format_does_not_change_key() {
    // `--error-format=json` changes how diagnostics are rendered
    // (cargo always passes it) — never the artifact. The key must
    // not move.
    if !rustc_available() {
        return;
    }
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("lib.rs");
    std::fs::write(&source, b"pub fn hello() {}").unwrap();

    let base = base_args(&source);
    let mut with_fmt = base_args(&source);
    with_fmt.push("--error-format=json".to_string());

    assert_eq!(
        key_for(&base),
        key_for(&with_fmt),
        "`--error-format` is diagnostics-only and must NOT change \
             the key — a change here is over-keying"
    );
}

#[test]
fn the_stashed_tree_digest_is_taken_once() {
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = None);
    assert_eq!(take_last_tree_digest(), None);
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some("tree-1".to_string()));
    assert_eq!(take_last_tree_digest().as_deref(), Some("tree-1"));
    assert_eq!(take_last_tree_digest(), None, "taken, not peeked");
}

#[test]
fn a_deferred_discovery_says_so_when_displayed() {
    let text = DeferredDiscovery.to_string();
    assert!(text.contains("deferred"), "{text}");
    let error: anyhow::Error = DeferredDiscovery.into();
    assert!(error.downcast_ref::<DeferredDiscovery>().is_some());
}

#[test]
fn a_shared_identity_needs_an_absolute_target_and_a_predictable_unit() {
    let _lock = key_test_lock();
    let _workspace = crate::config::tests::set_env_for_test(
        "CARGO_MANIFEST_DIR",
        Some(std::ffi::OsStr::new("/w/kt")),
    );
    let parse = |argv: &[&str]| {
        RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("debug").join("deps");
    std::fs::create_dir_all(&out).unwrap();
    let out_str = out.to_str().unwrap();
    let eligible = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--out-dir",
        out_str,
    ]);
    assert!(rustc_shared_prediction_identity(&eligible).is_some());
    let relative = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--out-dir",
        "target/debug/deps",
    ]);
    assert!(
        rustc_shared_prediction_identity(&relative).is_none(),
        "a relative target directory is not shareable"
    );
    let with_macro = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--out-dir",
        out_str,
        "--extern",
        "my_macro=/t/debug/deps/libmy_macro-3.so",
    ]);
    assert!(
        rustc_shared_prediction_identity(&with_macro).is_none(),
        "a proc-macro dependency outside the registry is not predictable by closure alone"
    );
}

#[test]
fn a_discovery_identity_is_the_shared_one_when_predictions_are_on() {
    let _lock = key_test_lock();
    let parse = |argv: &[&str]| {
        RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("debug").join("deps");
    std::fs::create_dir_all(&out).unwrap();
    let out_str = out.to_str().unwrap();
    let eligible = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--out-dir",
        out_str,
    ]);
    let with_macro = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--out-dir",
        out_str,
        "--extern",
        "my_macro=/t/debug/deps/libmy_macro-3.so",
    ]);
    let db = dir.path().join("index.db");
    let off = FileHasher::persistent(&db);
    assert_eq!(prediction_discovery_identity(&eligible, &off), None);
    let on = FileHasher::persistent(&db).with_input_predictions(true);
    let identity = prediction_discovery_identity(&eligible, &on).unwrap();
    assert_eq!(
        Some(identity.clone()),
        rustc_shared_prediction_identity(&eligible),
        "the shared spelling comes first"
    );
    assert!(!identity.is_empty());
    assert_eq!(
        prediction_discovery_identity(&with_macro, &on),
        None,
        "not shareable and not predictable: no flight"
    );
}

/// A record for a unit under the tree guard is usable only while the
/// crate tree it was made from is unchanged; a record without a tree
/// digest is not usable at all for such a unit.
#[test]
fn a_guarded_record_is_refused_when_the_tree_changed() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let package = dir
        .path()
        .join("registry")
        .join("src")
        .join("index-1")
        .join("kt-1.0.0");
    std::fs::create_dir_all(package.join("src")).unwrap();
    std::fs::write(package.join("src/lib.rs"), "pub fn v() {}\n").unwrap();
    let parse = |argv: &[&str]| {
        RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let out = dir.path().join("debug").join("deps");
    std::fs::create_dir_all(&out).unwrap();
    let with_macro = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        package.join("src/lib.rs").to_str().unwrap(),
        "--out-dir",
        out.to_str().unwrap(),
        "--extern",
        "my_macro=/t/debug/deps/libmy_macro-3.so",
    ]);
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    // SAFETY: the key test lock serialises environment edits.
    unsafe {
        std::env::set_var("CARGO_MANIFEST_DIR", &package);
        std::env::remove_var("OUT_DIR");
    }
    let on = FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
    let tree = crate_tree_digest(&on).expect("a registry package has a tree digest");
    let identity = rustc_prediction_identity(&with_macro).unwrap();
    let closure = DepInfo {
        source_files: vec![package.join("src/lib.rs")],
        env_deps: Vec::new(),
    };

    on.record_input_prediction(&identity, Some("kt"), &closure, None);
    assert_eq!(
        predicted_key_inputs(&with_macro, &on),
        Err(Rejection::NoRecord),
        "a record without a tree digest cannot guard a proc-macro unit"
    );

    on.record_input_prediction(&identity, Some("kt"), &closure, Some("stale".to_string()));
    assert_eq!(
        predicted_key_inputs(&with_macro, &on),
        Err(Rejection::TreeChanged)
    );

    on.record_input_prediction(&identity, Some("kt"), &closure, Some(tree.clone()));
    let current = predicted_key_inputs(&with_macro, &on);
    assert_ne!(current, Err(Rejection::TreeChanged), "{current:?}");
    assert_ne!(current, Err(Rejection::NoRecord), "{current:?}");
    assert_eq!(take_last_tree_digest(), Some(tree));

    std::fs::write(package.join("extra.txt"), "read by the macro").unwrap();
    assert_eq!(
        predicted_key_inputs(&with_macro, &on),
        Err(Rejection::TreeChanged),
        "a file added under the package changes the tree"
    );
    unsafe {
        match manifest_dir {
            Some(value) => std::env::set_var("CARGO_MANIFEST_DIR", value),
            None => std::env::remove_var("CARGO_MANIFEST_DIR"),
        }
    }
}

#[cfg(unix)]
#[test]
fn clippy_identity_reads_the_snapshot() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let driver = dir.path().join("clippy-driver");
    kache_fs::testutil::write_executable(
        &driver,
        "#!/bin/sh\necho 'clippy 0.1.98 (abc 2026-09-01)'\n",
    );
    let member = dir.path().join("member");
    std::fs::create_dir_all(&member).unwrap();
    std::fs::write(member.join("clippy.toml"), "msrv = \"1.70\"\n").unwrap();

    let from_manifest = clippy_identity(
        &driver,
        &KeyEnv::from_parts(
            [
                ("CARGO_MANIFEST_DIR", member.as_os_str()),
                ("CLIPPY_ARGS", OsStr::new("-Dwarnings")),
            ],
            None,
        ),
    )
    .unwrap();
    assert!(
        from_manifest.starts_with("clippy 0.1.98"),
        "{from_manifest}"
    );
    assert!(
        from_manifest.contains("config:clippy.toml:"),
        "{from_manifest}"
    );
    assert!(
        from_manifest.contains("CLIPPY_ARGS=-Dwarnings"),
        "{from_manifest}"
    );

    // With no manifest dir, the search starts at the working directory.
    let from_cwd = clippy_identity(
        &driver,
        &KeyEnv::from_parts([] as [(&str, &str); 0], Some(member.clone())),
    )
    .unwrap();
    assert!(from_cwd.contains("config:clippy.toml:"), "{from_cwd}");
    assert!(from_cwd.contains("CLIPPY_ARGS unset"), "{from_cwd}");
}

#[cfg(target_os = "linux")]
#[test]
fn the_linux_libc_signature_names_the_running_libc() {
    let family = if cfg!(target_env = "musl") {
        LinuxLibcFamily::Musl
    } else {
        LinuxLibcFamily::Gnu
    };
    let signature = probe_linux_libc_signature(family).unwrap();
    assert!(!signature.is_empty());
    assert!(
        signature.chars().any(|c| c.is_ascii_digit()),
        "a version, not a label: {signature}"
    );
}

#[cfg(unix)]
#[test]
fn linker_identity_is_the_first_version_line_of_the_configured_linker() {
    let _lock = key_test_lock();
    let dir = tempfile::tempdir().unwrap();
    // SAFETY: the key test lock serialises environment edits.
    unsafe { std::env::set_var("KACHE_CACHE_DIR", dir.path()) };
    let linker = dir.path().join("my-ld");
    kache_fs::testutil::write_executable(
        &linker,
        "#!/bin/sh\necho 'my-ld 9.9'\necho 'second line'\n",
    );
    let parse = |argv: &[&str]| {
        RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let args = parse(&[
        "rustc",
        "src/lib.rs",
        "-C",
        &format!("linker={}", linker.display()),
    ]);
    let identity = get_linker_identity(&args);
    let missing = get_linker_identity(&parse(&[
        "rustc",
        "src/lib.rs",
        "-C",
        &format!("linker={}", dir.path().join("absent").display()),
    ]));
    unsafe { std::env::remove_var("KACHE_CACHE_DIR") };
    assert_eq!(identity.as_deref(), Some("my-ld 9.9"));
    assert_eq!(missing, None, "a linker that cannot run has no identity");
}

#[test]
fn a_key_that_read_an_undeclared_variable_is_refused() {
    let env = crate::key_env::KeyEnv::from_parts([("RUSTFLAGS", "-a")], None);
    assert_eq!(complete_key(&env, "k".into()).unwrap(), "k");
    let _ = env.var_os("PATH");
    let err = complete_key(&env, "k".into()).unwrap_err();
    assert!(err.to_string().contains("KEY_ENV_VARS"), "{err}");
}
