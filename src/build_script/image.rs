//! Build-script records exchanged explicitly by trusted workers of one image.
//! Remote entries use ordinary flat artifact names. Only this decoder maps
//! them back under OUT_DIR. Host-local run records are never imported.
use super::*;
use anyhow::ensure;
use kache_store::ImportedPrediction;
use std::collections::{BTreeMap, BTreeSet};

const PREFIX: &str = "kache.build-script.v1.";
const DESCRIPTOR: &str = "image-run-v1.json";
const MAX_DESCRIPTOR: u64 = 4 << 20;

pub(crate) fn namespace(image: &str) -> String {
    format!("{PREFIX}{}.", image.trim_start_matches("sha256:"))
}

pub(crate) fn package_namespace(image: &str, package: &str) -> String {
    format!("{}{}", namespace(image), package.replace('-', "_"))
}

fn valid_package(package: &str) -> bool {
    !package.is_empty()
        && package.len() <= 255
        && package
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

pub(crate) fn reserved(name: &str) -> bool {
    name.starts_with("kache.build-script.")
}

pub(super) fn fold_image(hash: &mut blake3::Hasher, image: Option<&str>) {
    if let Some(image) = image {
        fold(hash, "build_script_image_v1", image.as_bytes());
    }
}

pub(super) fn identity(binary: &str, image: Option<&str>) -> String {
    let mut hash = blake3::Hasher::new();
    fold(&mut hash, "kind", b"kache-build-script-v1");
    fold(&mut hash, "binary", binary.as_bytes());
    fold_image(&mut hash, image);
    format!("{PREDICTION_PREFIX}{}", hash.finalize().to_hex())
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    image: String,
    package: String,
    binary: String,
    key: String,
    prediction: Prediction,
    outputs: Vec<String>,
}

fn portable_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && path.split('/').count() <= 32
        && path.split('/').all(|part| {
            // Keep a single spelling on NTFS, APFS and case-sensitive volumes.
            let stem = part
                .split('.')
                .next()
                .unwrap_or("")
                .trim_end_matches(' ')
                .to_ascii_uppercase();
            let device = matches!(
                stem.as_str(),
                "CON" | "AUX" | "NUL" | "PRN" | "CLOCK$" | "CONIN$" | "CONOUT$"
            ) || (stem
                .strip_prefix("COM")
                .or_else(|| stem.strip_prefix("LPT")))
            .is_some_and(|n| n.len() == 1 && n.as_bytes()[0].is_ascii_digit());
            !part.is_empty()
                && part.len() <= 255
                && !part.ends_with(['.', ' '])
                && !device
                && part.bytes().all(|b| {
                    b.is_ascii_alphanumeric()
                        || matches!(b, b'_' | b'-' | b'.' | b' ' | b'+' | b'@')
                })
        })
}

fn valid_prediction(p: &Prediction) -> bool {
    p.version == PREDICTION_SCHEMA
        && p.embedded_roots.as_deref() == Some(&[])
        && p.portable_out_dir != p.rewrites_text
        && p.external_link_inputs.is_empty()
        && p.inputs.len() <= MAX_INPUT_FILES
        && p.env.len() <= MAX_INPUT_FILES
        && p.inputs.iter().all(|path| {
            path == "${KACHE_MANIFEST_DIR}"
                || path
                    .strip_prefix("${KACHE_MANIFEST_DIR}/")
                    .is_some_and(portable_path)
        })
        && p.env.iter().all(|name| {
            !name.is_empty()
                && name.len() <= 256
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        })
}

// Raw linker arguments can name arbitrary files (including through response
// files). A portable run may name libraries and search its own OUT_DIR.
fn portable_link_directives(stdout: &str) -> bool {
    for directive in stdout.lines().filter_map(|line| {
        line.strip_prefix("cargo::")
            .or_else(|| line.strip_prefix("cargo:"))
    }) {
        let key = directive.split('=').next().unwrap_or(directive);
        if key == "rustc-flags"
            || key == "rustc-cdylib-link-arg"
            || key.starts_with("rustc-link-arg")
        {
            return false;
        }
    }
    outside::named_output_paths(stdout).iter().all(|path| {
        path == "${KACHE_OUT_DIR}"
            || path
                .strip_prefix("${KACHE_OUT_DIR}/")
                .is_some_and(portable_path)
    })
}

fn matches_declarations(p: &Prediction, m: &Manifest) -> bool {
    let declared = declarations::cargo_declarations(m.stdout.as_bytes(), None);
    let env = declared
        .env
        .into_iter()
        .filter(|v| !v.is_empty())
        .collect::<BTreeSet<_>>();
    let mut inputs = BTreeSet::new();
    for path in declared.paths {
        if path == "${KACHE_MANIFEST_DIR}" || path.starts_with("${KACHE_MANIFEST_DIR}/") {
            inputs.insert(path);
        } else if portable_path(&path) {
            inputs.insert(format!("${{KACHE_MANIFEST_DIR}}/{path}"));
        } else {
            return false;
        }
    }
    let default = inputs.is_empty() && env.is_empty();
    if default {
        inputs.insert("${KACHE_MANIFEST_DIR}".into());
    }
    portable_link_directives(&m.stdout)
        && p.default_package == default
        && p.inputs.iter().eq(&inputs)
        && p.env.iter().eq(&env)
        && p.rewrites_text != m.rewritten.is_empty()
}

fn regular_outputs(m: &Manifest, outputs: &[String]) -> bool {
    if !(m.symlinks.is_empty()
        && m.rooted_symlinks.is_empty()
        && m.outside_directories.is_empty()
        && m.outside_empty_files.is_empty()
        && m.outside_symlinks.is_empty()
        && m.version <= rooted_links::MANIFEST_VERSION
        && m.version > 0
        && outputs.len() + m.directories.len() + m.empty_files.len() <= MAX_OUT_DIR_FILES)
    {
        return false;
    }
    let mut files = BTreeSet::new();
    for name in outputs {
        if !portable_path(name) || !files.insert(name.as_str()) {
            return false;
        }
    }
    let mut rewritten = BTreeSet::new();
    for name in &m.rewritten {
        if !name
            .strip_prefix(OUT_PREFIX)
            .is_some_and(|p| files.contains(p))
            || !rewritten.insert(name)
        {
            return false;
        }
    }
    for file in &m.empty_files {
        if !portable_path(&file.name) || !files.insert(&file.name) {
            return false;
        }
    }
    let mut dirs = BTreeSet::new();
    for dir in &m.directories {
        if !portable_path(dir) || files.contains(dir.as_str()) || !dirs.insert(dir.as_str()) {
            return false;
        }
    }
    let mut spellings = BTreeMap::new();
    for name in files.iter().chain(&dirs) {
        for parent in Path::new(name).ancestors().filter_map(Path::to_str) {
            if spellings
                .insert(parent.to_ascii_lowercase(), parent)
                .is_some_and(|old| old != parent)
            {
                return false;
            }
        }
        if Path::new(name)
            .ancestors()
            .skip(1)
            .filter_map(Path::to_str)
            .any(|parent| files.contains(parent))
        {
            return false;
        }
    }
    true
}

/// When a run can travel, map its logical outputs to flat transport names.
pub(super) fn encode(
    run: &Run,
    key: &str,
    prediction: &Prediction,
    manifest: &Manifest,
    files: &mut Vec<(PathBuf, String)>,
    staging: &Path,
) -> Result<Option<String>> {
    let Some(image) = run.config.build_script_image.as_deref() else {
        return Ok(None);
    };
    let package = package_name();
    if !valid_package(&package) || !valid_prediction(prediction) {
        return Ok(None);
    }
    let Some(outputs) = files
        .iter()
        .map(|(_, name)| name.strip_prefix(OUT_PREFIX).map(str::to_owned))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    if !regular_outputs(manifest, &outputs) || !matches_declarations(prediction, manifest) {
        return Ok(None);
    }
    let d = Descriptor {
        image: image.into(),
        package: package.clone(),
        binary: run.binary_hash.clone(),
        key: key.into(),
        prediction: prediction.clone(),
        outputs,
    };
    let bytes = serde_json::to_vec(&d)?;
    if bytes.len() as u64 > MAX_DESCRIPTOR
        || serde_json::to_vec(manifest)?.len() as u64 > MAX_DESCRIPTOR
    {
        return Ok(None);
    }
    let path = staging.join(DESCRIPTOR);
    std::fs::write(&path, bytes)?;
    for (i, (_, name)) in files.iter_mut().enumerate() {
        *name = format!("output-{i}");
    }
    files.push((path, DESCRIPTOR.into()));
    Ok(Some(package_namespace(image, &package)))
}

fn read_json<T: serde::de::DeserializeOwned>(
    store: &Store,
    meta: &EntryMeta,
    name: &str,
) -> Result<T> {
    use std::io::Read as _;
    let file = meta
        .files
        .iter()
        .find(|f| f.name == name)
        .context("missing image run metadata")?;
    ensure!(
        file.size <= MAX_DESCRIPTOR
            && kache_format::is_blob_hash(&file.hash)
            && kache_format::is_valid_cache_key(&meta.cache_key),
        "invalid image run metadata"
    );
    let mut bytes = Vec::new();
    // A downloaded artifact stays beside meta.json until Store commits it.
    let source = match std::fs::File::open(store.blob_path(&file.hash)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::File::open(store.entry_dir(&meta.cache_key).join(name))?
        }
        other => other?,
    };
    source.take(MAX_DESCRIPTOR + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == file.size && blake3::hash(&bytes).to_hex().as_str() == file.hash,
        "invalid image run metadata size or checksum"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

fn decode(
    store: &Store,
    meta: &EntryMeta,
    image: &str,
) -> Result<(Descriptor, Manifest, EntryMeta)> {
    let d: Descriptor = read_json(store, meta, DESCRIPTOR)?;
    ensure!(
        d.image == image
            && valid_package(&d.package)
            && meta.crate_name == package_namespace(image, &d.package)
            && kache_format::is_blob_hash(&d.binary)
            && d.key == meta.cache_key
            && kache_format::is_valid_cache_key(&d.key)
            && valid_prediction(&d.prediction),
        "invalid image run binding or declarations"
    );
    let m: Manifest = read_json(store, meta, MANIFEST_NAME)?;
    ensure!(
        matches_declarations(&d.prediction, &m)
            && regular_outputs(&m, &d.outputs)
            && meta.files.len() == d.outputs.len() + 2,
        "invalid image run output map"
    );
    let mut logical = meta.clone();
    let mut seen = BTreeSet::new();
    for f in &mut logical.files {
        ensure!(seen.insert(f.name.clone()), "duplicate image run artifact");
        if f.name == MANIFEST_NAME || f.name == DESCRIPTOR {
            continue;
        }
        let i: usize = f
            .name
            .strip_prefix("output-")
            .context("unrecognized image run artifact")?
            .parse()?;
        ensure!(f.name == format!("output-{i}"), "invalid image run ordinal");
        f.name = format!(
            "{OUT_PREFIX}{}",
            d.outputs.get(i).context("image run output out of range")?
        );
    }
    Ok((d, m, logical))
}

/// A fresh runner must know the script's declared inputs before its first
/// lookup. Sync installs these rows only inside this image's prediction slot.
pub(crate) fn import(store: &Store, key: &str, image: &str) -> Result<()> {
    use std::io::Read as _;
    let decoded = (|| -> Result<Descriptor> {
        let mut bytes = Vec::new();
        std::fs::File::open(store.entry_dir(key).join("meta.json"))?
            .take(MAX_DESCRIPTOR * 4 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_DESCRIPTOR * 4,
            "image run entry metadata is too large"
        );
        let meta: EntryMeta = serde_json::from_slice(&bytes)?;
        ensure!(meta.cache_key == key, "image run entry key mismatch");
        let (d, _, _) = decode(store, &meta, image)?;
        Ok(d)
    })();
    let d = match decoded {
        Ok(d) => d,
        Err(error) => {
            store.discard_uncommitted_restored_entry(key)?;
            return Err(error);
        }
    };
    let prediction = ImportedPrediction {
        identity: &import_slot(&identity(&d.binary, Some(image))),
        schema: PREDICTION_SCHEMA,
        crate_name: CRATE_NAME,
        json: &serde_json::to_string(&d.prediction)?,
    };
    if store.contains(key) {
        store.record_imported_prediction(&prediction)
    } else {
        store.import_restored_entry_with_prediction(key, Some(&prediction))
    }
}

fn import_slot(identity: &str) -> String {
    format!("imported-image-v1:{identity}")
}

pub(super) fn imported_prediction(store: &Store, identity: &str) -> Result<Option<Prediction>> {
    let Some((PREDICTION_SCHEMA, json)) = store
        .file_hash_cache()
        .get_input_prediction(&import_slot(identity))?
    else {
        return Ok(None);
    };
    Ok(serde_json::from_str::<Option<Prediction>>(&json)?.filter(valid_prediction))
}

pub(super) fn restore(run: &Run, store: &Store, meta: &EntryMeta) -> Result<(Manifest, EntryMeta)> {
    let image = run
        .config
        .build_script_image
        .as_deref()
        .context("sharing build-script runs requires an image")?;
    let (d, m, logical) = decode(store, meta, image)?;
    ensure!(
        logical
            .files
            .iter()
            .filter(|f| f.name.starts_with(OUT_PREFIX))
            .try_fold(0u64, |n, f| n.checked_add(f.size))
            .is_some_and(|n| n <= run.config.build_script_out_dir_max_size),
        "image run exceeds local output limit"
    );
    ensure!(
        d.binary == run.binary_hash && d.package == package_name(),
        "image run belongs to a different build script"
    );
    let (inputs, env, default) = parse_declarations(
        &run.environment.denormalize_str(&m.stdout),
        &run.environment,
    )
    .context("image run declares an unconditional rerun")?;
    ensure!(
        inputs == d.prediction.inputs
            && env == d.prediction.env
            && default == d.prediction.default_package,
        "image run's stdout disagrees with its input declarations"
    );
    ensure!(
        run.action_key(&d.prediction)? == d.key,
        "image run inputs changed"
    );
    Ok((m, logical))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_output_paths_have_one_portable_spelling() {
        for path in [
            "../other", "x/../y", "/tmp/a", "x\\y", "x:y", "a\nlog", "a./b", "a /b", "prn.txt",
            "A/COM3.o", "CON .txt", "lpt9", "CONIN$", "x~1", "name*", "x//y", "x/./y", "é",
        ] {
            assert!(!portable_path(path), "{path:?}");
        }
        assert!(portable_path("include/zstd-1.5.7.h"));
        let m: Manifest = serde_json::from_value(serde_json::json!({
            "version": 1,
            "directories": [],
            "empty_files": [],
            "stdout": "",
            "stderr": "",
        }))
        .unwrap();
        for names in [["Dir/x", "dir/y"], ["a", "a/b"], ["foo", "FOO"], ["x", "x"]] {
            assert!(!regular_outputs(&m, &names.map(String::from)), "{names:?}");
        }
        assert!(regular_outputs(&m, &["dir/a".into(), "dir/b".into()]));
    }

    #[test]
    fn inconsistent_remote_descriptors_never_become_visible() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().into());
        let store = Store::open(&config).unwrap();
        let image = format!("sha256:{}", "a".repeat(64));
        let key = "b".repeat(64);
        let binary = "c".repeat(64);
        let name = package_namespace(&image, "producer");
        let prediction = recorded_prediction(
            vec!["${KACHE_MANIFEST_DIR}/input".into()],
            vec![],
            false,
            Vec::new(),
            false,
        );
        let slot = identity(&binary, Some(&image));
        for change in [
            "image",
            "package",
            "key",
            "binary",
            "traversal",
            "declarations",
            "external-search",
            "link-arg",
            "parent-search",
            "link",
            "ordinal",
            "checksum",
            "valid",
        ] {
            let staged = store.entry_dir(&key);
            std::fs::create_dir_all(&staged).unwrap();
            let mut descriptor = json!({"package":"producer", "image":image, "key":key, "binary":binary, "prediction":prediction, "outputs":["dir/file"]});
            let mut manifest = json!({
                "version": 1, "directories": ["dir"], "empty_files": [],
                "stdout": "cargo:rerun-if-changed=input\ncargo::rustc-link-search=native=${KACHE_OUT_DIR}/dir\n", "stderr": ""
            });
            let mut artifact = "output-0";
            match change {
                "image" | "package" | "key" | "binary" => descriptor[change] = json!("wrong"),
                "traversal" => descriptor["outputs"] = json!(["../outside"]),
                "declarations" => manifest["stdout"] = json!("cargo:rerun-if-changed=other"),
                "external-search" => {
                    manifest["stdout"] = json!(
                        "cargo:rerun-if-changed=input\ncargo:rustc-link-search=native=/host/lib"
                    )
                }
                "link-arg" => {
                    manifest["stdout"] =
                        json!("cargo:rerun-if-changed=input\ncargo::rustc-link-arg=@outside.rsp")
                }
                "parent-search" => {
                    manifest["stdout"] = json!(
                        "cargo:rerun-if-changed=input\ncargo::rustc-link-search=${KACHE_OUT_DIR}/../another"
                    )
                }

                "link" => {
                    manifest["symlinks"] = json!([{"name":"dir/link", "target":"../elsewhere"}])
                }
                "ordinal" => artifact = "output-01",
                "checksum" | "valid" => (),
                _ => unreachable!(),
            }
            let mut files = Vec::new();
            for (file, bytes) in [
                (DESCRIPTOR, descriptor.to_string().into_bytes()),
                (MANIFEST_NAME, manifest.to_string().into_bytes()),
                (artifact, b"compiled".to_vec()),
            ] {
                std::fs::write(staged.join(file), &bytes).unwrap();
                let hash = if change == "checksum" && file == DESCRIPTOR {
                    "f".repeat(64)
                } else {
                    blake3::hash(&bytes).to_hex().to_string()
                };
                files.push(json!({"name":file,"size":bytes.len(),"hash":hash,"executable":false}));
            }
            let meta = json!({"cache_key":key, "crate_name":name, "crate_types":["build-script"], "files":files,"stdout":"","stderr":""});
            std::fs::write(staged.join("meta.json"), meta.to_string()).unwrap();
            let result = import(&store, &key, &image);
            if change == "valid" {
                result.unwrap();
                assert!(store.contains(&key));
                assert_eq!(
                    imported_prediction(&store, &slot).unwrap(),
                    Some(prediction.clone())
                );
            } else {
                assert!(result.is_err(), "{change}");
                assert!(!store.contains(&key), "{change}");
                assert!(!staged.exists(), "{change}");
                assert!(
                    imported_prediction(&store, &slot).unwrap().is_none(),
                    "{change}"
                );
            }
        }
    }

    #[test]
    fn conflicting_remote_declarations_do_not_override_local_or_other_image_predictions() {
        let t = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(t.path().to_path_buf());
        let store = Store::open(&config).unwrap();
        let local = identity("compiler", Some("sha256:a"));
        let other = identity("compiler", Some("sha256:b"));
        let p = recorded_prediction(
            vec!["${KACHE_MANIFEST_DIR}/a".into()],
            vec![],
            false,
            vec![],
            false,
        );
        let q = Prediction {
            inputs: vec!["${KACHE_MANIFEST_DIR}/b".into()],
            ..p.clone()
        };
        let import = |id: &str, p: &Prediction| {
            store
                .record_imported_prediction(&ImportedPrediction {
                    identity: &import_slot(id),
                    schema: PREDICTION_SCHEMA,
                    crate_name: CRATE_NAME,
                    json: &serde_json::to_string(p).unwrap(),
                })
                .unwrap()
        };
        let json = serde_json::to_string(&p).unwrap();
        store
            .record_input_prediction(None, &local, PREDICTION_SCHEMA, Some(CRATE_NAME), &json)
            .unwrap();
        import(&local, &p);
        import(&other, &p);
        import(&local, &p);
        assert_eq!(
            imported_prediction(&store, &local).unwrap(),
            Some(p.clone())
        );
        import(&local, &q);
        import(&local, &p);
        assert!(imported_prediction(&store, &local).unwrap().is_none());
        assert_eq!(stored_prediction(&store, &local).unwrap(), Some(p.clone()));
        assert_eq!(imported_prediction(&store, &other).unwrap(), Some(p));
    }
}
