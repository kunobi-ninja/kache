//! Input prediction rows shared through the remote (kunobi-ninja/kache#1011).
//!
//! A fresh machine has no local rows, so its first build of every unit pays
//! rustc's dep-info pre-pass even when the remote holds every result. Two
//! kinds of row travel next to the entries:
//!
//! - a portable row, a workspace unit's or a registry unit's with a relocated
//!   `OUT_DIR`, which names nothing specific to the machine that made it;
//! - a registry unit's shared row. Its identity folds the Cargo home's path,
//!   so it only matches on a machine whose Cargo home has the same path, as CI
//!   runners built from one image do; anywhere else it is simply never asked
//!   for.
//!
//! No local path leaves the machine. Before a row is published, a path under
//! `<CARGO_HOME>/registry/src` is written relative to it
//! ([`Portable::Registry`]), and a row with any other absolute path in it is
//! not published at all. The reader places registry paths under its own
//! registry directory.
//!
//! A row from the remote is only a hint, used exactly like a local one: its
//! guard must equal this machine's own digest of the workspace or package,
//! every input it names is checked on disk, and the key hashes every input's
//! content. Nothing is stored under a key computed from it, and a local miss
//! re-derives the key with the pre-pass before claiming or storing. An honest
//! but wrong row therefore costs a miss. A malicious row could leave an input
//! out of the key, but that only matters together with a poisoned entry under
//! that incomplete key, and whoever can write rows to the remote can already
//! write entries there: the trust boundary is the remote's own.

use crate::cache_key::{
    InputPrediction, PORTABLE_PREDICTION_SCHEMA, PREDICTION_SCHEMA, Portable, PortablePrediction,
};
use std::path::Path;

/// Version of [`SharedRow`] on the remote.
pub(crate) const SHARED_ROW_SCHEMA: u32 = 1;

/// Largest row object read from the remote. A row lists a unit's inputs; even
/// a crate with thousands of modules stays far below this.
pub(crate) const SHARED_ROW_MAX_BYTES: u64 = 256 * 1024;

/// A row that may travel, in the shape its identity calls for.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SharedPrediction {
    Portable(PortablePrediction),
    Plain(PlainRow),
}

/// A registry unit's shared row ([`InputPrediction`]) as it travels, with its
/// registry paths made relative.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PlainRow {
    pub(crate) schema: u32,
    pub(crate) sources: Vec<Portable>,
    pub(crate) env_deps: Vec<(String, Portable)>,
    pub(crate) tree: Option<String>,
}

/// One path or value as it may travel: a path under `registry` made relative
/// to it, anything else with no root kept, and `None` for any other absolute
/// path, which would carry a local path to the remote.
fn for_remote_value(value: &str, registry: Option<&Path>) -> Option<Portable> {
    if !Path::new(value).has_root() {
        return Some(Portable::Literal(value.to_string()));
    }
    let suffix = crate::cache_key::registry_suffix(value, registry?)?;
    Some(Portable::Registry(suffix))
}

fn for_remote_entry(entry: &Portable, registry: Option<&Path>) -> Option<Portable> {
    match entry {
        Portable::Literal(value) => for_remote_value(value, registry),
        relative => Some(relative.clone()),
    }
}

/// `row` as it may travel, or `None` when it names an absolute path outside
/// the registry. `registry` is this unit's `<CARGO_HOME>/registry/src`.
pub(crate) fn for_remote_portable(
    row: &PortablePrediction,
    registry: Option<&Path>,
) -> Option<SharedPrediction> {
    Some(SharedPrediction::Portable(PortablePrediction {
        schema: row.schema,
        sources: row
            .sources
            .iter()
            .map(|source| for_remote_entry(source, registry))
            .collect::<Option<_>>()?,
        env_deps: row
            .env_deps
            .iter()
            .map(|(name, value)| Some((name.clone(), for_remote_entry(value, registry)?)))
            .collect::<Option<_>>()?,
        tree: row.tree.clone(),
    }))
}

/// A registry unit's shared row as it may travel; see [`for_remote_portable`].
pub(crate) fn for_remote_plain(row: &InputPrediction, registry: &Path) -> Option<SharedPrediction> {
    Some(SharedPrediction::Plain(PlainRow {
        schema: row.schema,
        sources: row
            .sources
            .iter()
            .map(|source| for_remote_value(source.to_str()?, Some(registry)))
            .collect::<Option<_>>()?,
        env_deps: row
            .env_deps
            .iter()
            .map(|(name, value)| Some((name.clone(), for_remote_value(value, Some(registry))?)))
            .collect::<Option<_>>()?,
        tree: row.tree.clone(),
    }))
}

/// The shared row `row` describes on this machine, with its registry paths
/// under this unit's `registry`. `None` for an entry of a kind a shared row
/// cannot have.
pub(crate) fn plain_from_remote(row: &PlainRow, registry: &Path) -> Option<InputPrediction> {
    let registry = registry.to_str()?;
    let place = |entry: &Portable| match entry {
        Portable::Literal(value) => Some(value.clone()),
        Portable::Registry(suffix) => Some(format!("{registry}{suffix}")),
        Portable::OutDir(_) | Portable::Workspace(_) => None,
    };
    Some(InputPrediction {
        schema: row.schema,
        sources: row
            .sources
            .iter()
            .map(|source| place(source).map(Into::into))
            .collect::<Option<_>>()?,
        env_deps: row
            .env_deps
            .iter()
            .map(|(name, value)| Some((name.clone(), place(value)?)))
            .collect::<Option<_>>()?,
        tree: row.tree.clone(),
    })
}

impl SharedPrediction {
    /// Is this the kind of row `identity` is filed with, in the format this
    /// build reads?
    fn fits(&self, identity: &str) -> bool {
        match self {
            SharedPrediction::Portable(row) => {
                crate::cache_key::is_portable_identity(identity)
                    && row.schema == PORTABLE_PREDICTION_SCHEMA
            }
            SharedPrediction::Plain(row) => {
                crate::cache_key::is_shared_target_identity(identity)
                    && row.schema == PREDICTION_SCHEMA
            }
        }
    }
}

/// A row as stored on the remote, bound to the identity it answers so an
/// object copied under another name is refused.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SharedRow {
    schema: u32,
    identity: String,
    row: SharedPrediction,
}

/// The object name a row for `identity` is stored under: its hash, so any
/// identity is a safe path component.
pub(crate) fn object_name(identity: &str) -> String {
    format!("{}.json", blake3::hash(identity.as_bytes()).to_hex())
}

/// The bytes to store for `row` under `identity`, or `None` when the row may
/// not travel: the wrong kind for the identity, or too large to be read back.
pub(crate) fn encode(identity: &str, row: &SharedPrediction) -> Option<Vec<u8>> {
    if !row.fits(identity) {
        return None;
    }
    let bytes = serde_json::to_vec(&SharedRow {
        schema: SHARED_ROW_SCHEMA,
        identity: identity.to_string(),
        row: row.clone(),
    })
    .ok()?;
    (bytes.len() as u64 <= SHARED_ROW_MAX_BYTES).then_some(bytes)
}

/// The row in `bytes`, when it is the current format, answers `identity` and
/// is the kind that identity is filed with. Anything else, malformed or
/// oversized included, is no row at all.
pub(crate) fn decode(bytes: &[u8], identity: &str) -> Option<SharedPrediction> {
    if bytes.len() as u64 > SHARED_ROW_MAX_BYTES {
        return None;
    }
    let shared: SharedRow = serde_json::from_slice(bytes).ok()?;
    (shared.schema == SHARED_ROW_SCHEMA && shared.identity == identity && shared.row.fits(identity))
        .then_some(shared.row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_key::Portable;

    const IDENTITY: &str = "shared-workspace-v1:abc";

    fn portable() -> PortablePrediction {
        PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![Portable::Literal("kt/src/lib.rs".to_string())],
            env_deps: vec![],
            tree: "tree".to_string(),
        }
    }

    fn row() -> SharedPrediction {
        SharedPrediction::Portable(portable())
    }

    fn plain() -> SharedPrediction {
        SharedPrediction::Plain(PlainRow {
            schema: PREDICTION_SCHEMA,
            sources: vec![Portable::Registry("/i/kt-1.0.0/src/lib.rs".into())],
            env_deps: vec![],
            tree: None,
        })
    }

    #[test]
    fn a_published_row_carries_no_local_path() {
        let registry = Path::new("/home/me/.cargo/registry/src");
        let local = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec!["/home/me/.cargo/registry/src/i/kt-1.0.0/src/lib.rs".into()],
            env_deps: vec![(
                "CARGO_MANIFEST_DIR".into(),
                "/home/me/.cargo/registry/src/i/kt-1.0.0".into(),
            )],
            tree: Some("t".into()),
        };
        let shared = for_remote_plain(&local, registry).unwrap();
        let bytes = encode("shared-target-v2:abc", &shared).unwrap();
        assert!(
            !String::from_utf8_lossy(&bytes).contains("/home/me"),
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let SharedPrediction::Plain(row) = decode(&bytes, "shared-target-v2:abc").unwrap() else {
            panic!("a plain row");
        };
        let elsewhere = plain_from_remote(&row, Path::new("/ci/.cargo/registry/src")).unwrap();
        assert_eq!(
            elsewhere.sources,
            vec![std::path::PathBuf::from(
                "/ci/.cargo/registry/src/i/kt-1.0.0/src/lib.rs"
            )]
        );
        assert_eq!(
            elsewhere.env_deps[0].1,
            "/ci/.cargo/registry/src/i/kt-1.0.0"
        );
        assert_eq!(elsewhere.tree.as_deref(), Some("t"));
    }

    #[test]
    fn a_row_naming_another_absolute_path_is_not_published() {
        let registry = Path::new("/home/me/.cargo/registry/src");
        let mut local = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec!["/home/me/.cargo/registry/src/i/kt-1.0.0/src/lib.rs".into()],
            env_deps: vec![("HOME".into(), "/home/me".into())],
            tree: None,
        };
        assert_eq!(for_remote_plain(&local, registry), None, "an env value");
        local.env_deps = vec![("PROFILE".into(), "debug".into())];
        local.sources.push("/etc/hosts".into());
        assert_eq!(for_remote_plain(&local, registry), None, "a source");
        local.sources.pop();
        local
            .sources
            .push("/home/me/.cargo/registry/src/i/kt-1.0.0/../../../../x".into());
        assert_eq!(
            for_remote_plain(&local, registry),
            None,
            "a walk out of the registry"
        );

        let workspace = PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![
                Portable::Literal("kt/src/lib.rs".into()),
                Portable::Workspace("/assets/a".into()),
            ],
            env_deps: vec![("OUT".into(), Portable::Literal("/home/me/out".into()))],
            tree: "t".into(),
        };
        assert_eq!(for_remote_portable(&workspace, None), None);
        let mut without_path = workspace.clone();
        without_path.env_deps.clear();
        assert_eq!(
            for_remote_portable(&without_path, None),
            Some(SharedPrediction::Portable(without_path.clone())),
            "relative entries travel as they are"
        );
        let registry_row = PortablePrediction {
            sources: vec![Portable::Literal(
                "/home/me/.cargo/registry/src/i/kt-1.0.0/src/lib.rs".into(),
            )],
            ..without_path
        };
        let Some(SharedPrediction::Portable(shared)) =
            for_remote_portable(&registry_row, Some(registry))
        else {
            panic!("a registry path travels relative");
        };
        assert_eq!(
            shared.sources,
            vec![Portable::Registry("/i/kt-1.0.0/src/lib.rs".into())]
        );
    }

    #[test]
    fn a_plain_row_with_a_foreign_entry_is_refused_on_arrival() {
        let row = PlainRow {
            schema: PREDICTION_SCHEMA,
            sources: vec![Portable::Workspace("/kt/src/lib.rs".into())],
            env_deps: vec![],
            tree: None,
        };
        assert_eq!(plain_from_remote(&row, Path::new("/r")), None);
    }

    #[test]
    fn a_row_round_trips_under_its_own_identity_only() {
        let bytes = encode(IDENTITY, &row()).unwrap();
        assert_eq!(decode(&bytes, IDENTITY), Some(row()));
        assert_eq!(
            decode(&bytes, "shared-workspace-v1:other"),
            None,
            "an object copied under another identity"
        );
    }

    #[test]
    fn each_identity_travels_with_its_own_kind_of_row() {
        assert!(encode("shared-out-dir-v1:abc", &row()).is_some());
        assert_eq!(encode("shared-target-v2:abc", &row()), None);
        assert_eq!(encode("abc", &row()), None);
        let bytes = encode("shared-target-v2:abc", &plain()).unwrap();
        assert_eq!(decode(&bytes, "shared-target-v2:abc"), Some(plain()));
        assert_eq!(encode(IDENTITY, &plain()), None);
        assert_eq!(encode("abc", &plain()), None);
        let bytes = encode(IDENTITY, &row()).unwrap();
        assert_eq!(decode(&bytes, "abc"), None);
    }

    #[test]
    fn a_malformed_or_foreign_object_is_no_row() {
        assert_eq!(decode(b"not json", IDENTITY), None);
        assert_eq!(decode(b"{}", IDENTITY), None);
        let mut shared: serde_json::Value =
            serde_json::from_slice(&encode(IDENTITY, &row()).unwrap()).unwrap();
        shared["schema"] = (SHARED_ROW_SCHEMA + 1).into();
        assert_eq!(
            decode(&serde_json::to_vec(&shared).unwrap(), IDENTITY),
            None
        );
        shared["schema"] = SHARED_ROW_SCHEMA.into();
        shared["row"]["portable"]["schema"] = (PORTABLE_PREDICTION_SCHEMA + 1).into();
        assert_eq!(
            decode(&serde_json::to_vec(&shared).unwrap(), IDENTITY),
            None
        );
    }

    #[test]
    fn an_object_past_the_cap_is_no_row() {
        let mut large = portable();
        let padding = "x".repeat(SHARED_ROW_MAX_BYTES as usize);
        large.sources.push(Portable::Literal(padding));
        let large = SharedPrediction::Portable(large);
        assert_eq!(encode(IDENTITY, &large), None);
        let bytes = serde_json::to_vec(&SharedRow {
            schema: SHARED_ROW_SCHEMA,
            identity: IDENTITY.to_string(),
            row: large,
        })
        .unwrap();
        assert_eq!(decode(&bytes, IDENTITY), None);
    }

    /// A row whose object is `extra` bytes longer than one with an empty
    /// padding source.
    fn row_padded(extra: usize) -> SharedPrediction {
        let mut padded = portable();
        padded.sources.push(Portable::Literal("x".repeat(extra)));
        SharedPrediction::Portable(padded)
    }

    #[test]
    fn an_object_exactly_at_the_cap_is_still_a_row() {
        let base = encode(IDENTITY, &row_padded(0)).unwrap().len();
        let at_cap = row_padded(SHARED_ROW_MAX_BYTES as usize - base);
        let bytes = encode(IDENTITY, &at_cap).unwrap();
        assert_eq!(bytes.len() as u64, SHARED_ROW_MAX_BYTES);
        assert_eq!(decode(&bytes, IDENTITY), Some(at_cap));

        let over = row_padded(SHARED_ROW_MAX_BYTES as usize - base + 1);
        assert_eq!(encode(IDENTITY, &over), None);
        let over_bytes = serde_json::to_vec(&SharedRow {
            schema: SHARED_ROW_SCHEMA,
            identity: IDENTITY.to_string(),
            row: over,
        })
        .unwrap();
        assert_eq!(over_bytes.len() as u64, SHARED_ROW_MAX_BYTES + 1);
        assert_eq!(decode(&over_bytes, IDENTITY), None);
    }

    #[test]
    fn the_row_cap_is_a_quarter_mebibyte() {
        assert_eq!(SHARED_ROW_MAX_BYTES, 262_144);
    }

    #[test]
    fn the_object_name_is_the_identity_hash() {
        let name = object_name(IDENTITY);
        assert!(name.ends_with(".json"));
        assert_eq!(name.len(), 64 + ".json".len());
        assert_ne!(name, object_name("shared-workspace-v1:abd"));
    }
}
