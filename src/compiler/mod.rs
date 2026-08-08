//! Compiler abstraction.
//!
//! Each supported compiler adapter (today: rustc and C-family compilers)
//! implements the [`Compiler`] trait and exports a [`CompilerAdapter`]
//! descriptor. Detection walks those descriptors instead of returning a closed
//! enum, so adding an adapter is adding a module-owned descriptor plus wrapper
//! dispatch, not growing a central taxonomy of future tool kinds.
//!
//! **Scope.** The trait covers the operations with a clean generic shape
//! today: `parse`, `refuse_reasons`, `cache_key`, `execute`, and
//! `classify_output` (per-file kind classification used by the wrapper to
//! dispatch link strategy and post-restore processing without filename
//! pattern matching). Storage metadata (crate types, features,
//! target/profile) and the restore loop's path resolution still touch
//! [`crate::args::RustcArgs`] fields directly in [`crate::wrapper`]; those
//! move behind the trait when adding a second compiler forces the
//! abstraction.

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::link::LinkStrategy;

pub mod cc;
pub mod flags;
pub mod platform;
pub mod rustc;

pub use platform::Platform;

pub use crate::compile::CompileResult;

/// Stable adapter identifier.
///
/// This is intentionally an open string newtype instead of a closed enum:
/// adapter ids name concrete implementations that exist today, while future
/// adapters bring their own ids without forcing kache to define an abstract
/// "kind" hierarchy up front.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompilerId(&'static str);

impl CompilerId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for CompilerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Module-owned adapter descriptor used for argv detection.
#[derive(Debug, Clone, Copy)]
pub struct CompilerAdapter {
    id: CompilerId,
    display_name: &'static str,
    recognizes: fn(&[String]) -> bool,
}

impl CompilerAdapter {
    pub const fn new(
        id: CompilerId,
        display_name: &'static str,
        recognizes: fn(&[String]) -> bool,
    ) -> Self {
        Self {
            id,
            display_name,
            recognizes,
        }
    }

    pub const fn id(self) -> CompilerId {
        self.id
    }

    pub const fn display_name(self) -> &'static str {
        self.display_name
    }

    pub fn recognizes(self, args: &[String]) -> bool {
        (self.recognizes)(args)
    }
}

/// Reason an invocation cannot be cached. Empty list = cacheable.
///
/// Two variants:
///
/// - `NotPrimary`: the invocation is a query / probe (`--print`,
///   `-vV`) that exists to provide information to the caller, not to
///   produce a build artifact for downstream consumption. Caching is
///   meaningless — the call is one-shot informational.
/// - `Unsupported`: kache could in principle cache this, but the
///   feature / flag / mode isn't modeled yet. EVERYTHING that's
///   technically cacheable-with-engineering-effort lands here:
///   link-mode caching, multi-source per-source split, preprocessor /
///   assembly variant outputs, output-to-stdout, response-file
///   expansion, PCH / modules, classifier gaps. Message MUST include
///   "(not yet supported)" or equivalent so users reading the bench
///   output can tell it's a deferral, not a permanent limitation.
///
/// There is deliberately no third "won't ever cache" variant. For cc
/// (and rustc) every deterministic input-to-output function IS
/// cacheable in principle — even `-E` preprocessor output, even `-S`
/// assembly output, even stdout bytes. What separates them from `-c`
/// today is engineering priority, not categorical impossibility. The
/// taxonomy reflects that honestly so future work to support any of
/// them can drop a row to `Unsupported` and find this comment
/// describing the deferral, rather than running into a "NotACompile"
/// variant whose name lies about feasibility.
#[derive(Debug, Clone)]
pub enum RefuseReason {
    /// Not a primary compilation (e.g. `--print`, `-vV`, query mode).
    NotPrimary,
    /// Kache could cache this with engineering effort but doesn't yet.
    /// Message should include "(not yet supported)" so the deferral
    /// nature is explicit. Examples: link mode, multi-source compile,
    /// preprocessor / assembly variant outputs, output-to-stdout,
    /// response files, PCH, modules, unmodeled classifier flags.
    Unsupported(&'static str),
}

impl RefuseReason {
    /// Stable, human-readable *detail* of why caching was refused — the
    /// specifics (`cc link mode (whole-program caching) — not yet`). Pairs
    /// with [`category`](Self::category), which gives the coarse class. Used
    /// by the wrapper for the structured passthrough reason and by reporting.
    /// The string is a contract — changing it is observable.
    pub fn description(&self) -> &'static str {
        match self {
            RefuseReason::NotPrimary => "query / probe (--print, -vV)",
            RefuseReason::Unsupported(detail) => detail,
        }
    }

    /// Coarse class of the refusal, for the passthrough report's `category`
    /// column. `not-a-compile` is a query/probe that is conceptually not a
    /// compilation at all; `unsupported` is a real compile kache could cache
    /// with engineering effort but doesn't model yet (its detail reads
    /// "— not yet"). Neither is a failure — the build runs the compiler.
    pub fn category(&self) -> &'static str {
        match self {
            RefuseReason::NotPrimary => "not-a-compile",
            RefuseReason::Unsupported(_) => "unsupported",
        }
    }
}

/// Compiler-agnostic context passed to [`Compiler::cache_key`].
pub struct KeyCtx<'a, 'db> {
    pub file_hasher: &'a crate::cache_key::FileHasher<'db>,
    /// Strips machine-local path prefixes from key inputs so the same
    /// source produces the same key across hosts and worktrees. Lives
    /// in the context (not as a free function) so future per-compiler
    /// impls can pass a normalizer with extra rules (e.g. cc-family
    /// might know about `$SDKROOT`).
    pub path_normalizer: &'a crate::path_normalizer::PathNormalizer,
    /// kache's cache directory. Compiler-probe results (e.g. the cc
    /// `--version` identity line) are memoized under here so a probe
    /// runs once per build instead of once per translation unit — see
    /// [`crate::probe`].
    pub cache_dir: &'a Path,
    /// Opaque user-declared salt folded into the final key by every
    /// compiler family (see [`crate::cache_key::apply_key_salt`]).
    /// `None` leaves the key byte-identical to the unsalted case.
    pub key_salt: Option<&'a str>,
    /// User-declared env-var name patterns folded into the final key by every
    /// compiler family, for expansion-time reads the compiler never reports
    /// (see [`crate::cache_key::apply_key_env_vars`]). Empty leaves the key
    /// byte-identical to the undeclared case.
    pub key_env_vars: &'a [String],
    /// Dep-info pre-pass memoization ([`crate::dep_info_memo`], rustc-family
    /// only; see [`crate::config::Config::dep_info_memo`]). The memo store
    /// lives under [`Self::cache_dir`].
    pub dep_info_memo: bool,
}

/// Categorization of a compiler output file.
///
/// Used by the wrapper to drive two decisions per restored file without
/// scattering filename pattern matching: which [`LinkStrategy`] to use, and
/// which post-restore processing to apply (dep-info path expansion, codesign,
/// etc.). Centralizing the dispatch on `ArtifactKind` is what makes "skip
/// codesign for `.o`" or "rewrite paths in `.d`" structurally enforced
/// instead of dependent on remembering to add a string-suffix check at every
/// call site.
///
/// Open enum: future compilers extend with [`ArtifactKind::Other`] without
/// touching shared code; the safe default for an unrecognized kind is
/// `Hardlink` + no post-processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    /// Linkable static library (`.rlib`, future C/C++ `.a` / `.lib`).
    Library,
    /// Dynamic library (`.dylib`, `.so`, `.dll`). Mutable post-build on
    /// macOS (codesigning).
    DynamicLibrary,
    /// Metadata-only artifact (Rust `.rmeta`).
    Metadata,
    /// Object file (`.o`, `.obj`, `.rcgu.o`). Linker input only — never loaded
    /// directly, never codesigned.
    Object,
    /// Dependency-info file (`.d` / `.pp`). Content references absolute paths
    /// that need rewriting on store/restore for cross-worktree portability.
    DepInfo,
    /// Executable. Mutable post-build (codesigning, stripping).
    Executable,
    /// A wasm target's linked module (`.wasm`) — the shape a `bin` or
    /// `cdylib` built for `wasm32-*` takes (kunobi-ninja/kache#431).
    ///
    /// Deliberately its own kind rather than a [`Self::DynamicLibrary`]:
    /// it shares that kind's *mutation* profile (build tooling such as
    /// substrate's wasm-builder post-processes the emitted module, so it
    /// must restore as an independent file, never a shared inode) but not
    /// its *loader* profile — a wasm module is never mapped by the OS
    /// loader, so it must not pick up the codesign post-restore action.
    WasmModule,
    /// Debug info sidecar (`.dwo`, `.pdb`, `.dSYM`).
    DebugSidecar,
    /// A kache-produced tar of a debug-info bundle *directory* — today the
    /// macOS `.dSYM` baked at store time (kunobi-ninja/kache#319). The store
    /// holds flat files only (single-component artifact names, file-level
    /// hashing/linking), so the bundle is tarred into one flat file at store
    /// time and unpacked next to the binary by
    /// [`PostRestoreAction::UnpackDebugBundle`] on restore.
    DebugBundle,
    /// Compiler-specific output that doesn't fit the categories above.
    /// Defaults to immutable handling.
    Other(&'static str),
}

impl ArtifactKind {
    /// Link strategy for restoring this kind. Mutable artifacts (executables,
    /// dynamic libraries) must end up as independent files on filesystems
    /// without CoW reflink, so post-build mutations don't propagate into the
    /// cache blob. Immutable kinds may share an inode (hardlink fallback).
    pub fn link_strategy(self) -> LinkStrategy {
        match self {
            ArtifactKind::Executable | ArtifactKind::DynamicLibrary | ArtifactKind::WasmModule => {
                LinkStrategy::Copy
            }
            _ => LinkStrategy::Hardlink,
        }
    }
}

/// One compiler output artifact.
///
/// `store_name` is the stable filename used inside a cache entry. It is
/// usually the basename of `path`, but it is explicit so adapters can
/// later represent directory/discovered outputs without making the store
/// infer names from paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub path: PathBuf,
    pub store_name: String,
    pub kind: ArtifactKind,
    pub required: bool,
}

/// Full output set produced by one compiler invocation.
///
/// Today the store still persists files as `(source_path, store_name)`
/// pairs. Keeping the richer artifact set at the compiler boundary lets
/// C/C++ and Rust grow side-output modeling without changing the cache
/// format in the same PR.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArtifactSet {
    outputs: Vec<Artifact>,
}

impl ArtifactSet {
    pub fn new(outputs: Vec<Artifact>) -> Self {
        Self { outputs }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_output_files(
        output_files: Vec<(PathBuf, String)>,
        classify: impl Fn(&str) -> ArtifactKind,
    ) -> Self {
        Self::new(
            output_files
                .into_iter()
                .map(|(path, store_name)| {
                    let kind = classify(&store_name);
                    Artifact {
                        path,
                        store_name,
                        kind,
                        required: true,
                    }
                })
                .collect(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Append one artifact kache produced itself (not a compiler output) —
    /// today the store-time debug bundle tar (kunobi-ninja/kache#319). The
    /// caller owns keeping `path` alive until the store put has hashed it.
    pub fn push(&mut self, artifact: Artifact) {
        self.outputs.push(artifact);
    }

    pub fn outputs(&self) -> &[Artifact] {
        &self.outputs
    }

    pub fn store_files(&self) -> Vec<(PathBuf, String)> {
        self.outputs
            .iter()
            .map(|artifact| (artifact.path.clone(), artifact.store_name.clone()))
            .collect()
    }

    pub fn total_size(&self) -> u64 {
        self.outputs
            .iter()
            .map(|artifact| {
                std::fs::metadata(&artifact.path)
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum()
    }
}

/// Best-guess classification from filename alone, no compile-context.
///
/// Used by callers that scan a directory of artifacts (e.g. analyzing
/// `target/` from the CLI) where there's no parsed [`Compiler::Parsed`]
/// to disambiguate. Extensionless files return
/// [`ArtifactKind::Other`]`("extensionless")` — callers in target-scan
/// contexts should treat that as `Executable` (the rustc convention for
/// bin output on Unix); callers without that context should fall back
/// to the safe default (immutable, no post-processing).
///
/// This is the single source of truth for "filename → artifact kind"
/// across kache: [`Compiler::classify_output`] implementations delegate
/// to it for the known-extension cases. Adding a new artifact extension
/// happens here, not at every call site that does suffix matching.
pub fn classify_by_filename(name: &str) -> ArtifactKind {
    // kache-produced debug-bundle tar (`<bin>.dsym.tar`, see #319). Checked
    // before the extension match because `Path::extension` sees only "tar",
    // which would land in `Other("unknown-ext")` — and restore dispatches the
    // unpack action off this classification.
    if name.ends_with(".dsym.tar") {
        return ArtifactKind::DebugBundle;
    }
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "rlib" => ArtifactKind::Library,
        "rmeta" => ArtifactKind::Metadata,
        "d" | "pp" => ArtifactKind::DepInfo,
        // Covers `.o` and compound `.rcgu.o` (Path::extension takes the
        // shortest tail, which is "o" for both).
        "o" | "obj" => ArtifactKind::Object,
        "dylib" | "so" | "dll" => ArtifactKind::DynamicLibrary,
        "wasm" => ArtifactKind::WasmModule,
        "dwo" | "pdb" | "dSYM" => ArtifactKind::DebugSidecar,
        "exe" => ArtifactKind::Executable,
        "" => ArtifactKind::Other("extensionless"),
        _ => ArtifactKind::Other("unknown-ext"),
    }
}

/// Canonical rustc `--emit` kind that a stored output filename satisfies, or
/// `None` if the file is not a recognized emit product (e.g. a `.dSYM` / `.pdb`
/// debug sidecar that no `--emit` kind requests directly).
///
/// This is the "filename → emit kind" sibling of [`classify_by_filename`] and
/// the single source of truth for the emit-coverage gate (kunobi-ninja/kache#325):
/// the store records the set of kinds an entry actually contains, and lookup
/// refuses an entry that doesn't cover what the invocation's `--emit` requested.
///
/// The returned strings match rustc's own `--emit` tokens (and the `emit` field
/// of its `artifact` JSON notifications), so they compare directly against
/// [`crate::args::RustcArgs::emit`]. A lib `--emit=link` legitimately also emits
/// `.rmeta`, so `metadata` may appear in an entry's covered set without having
/// been requested — the gate is superset-tolerant, so that is fine.
/// The canonical rustc `--emit` kinds the coverage gate reasons about — exactly
/// the values [`emit_kind_for_filename`] can return (kunobi-ninja/kache#325). A
/// requested kind outside this set is ignored by the gate so it never refuses on
/// a kind kache can't map to a stored file.
pub const GATED_EMIT_KINDS: [&str; 8] = [
    "link", "metadata", "obj", "dep-info", "asm", "llvm-ir", "llvm-bc", "mir",
];

pub fn emit_kind_for_filename(name: &str) -> Option<&'static str> {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        // Linked output: rlib / staticlib / dylib / cdylib / bin / proc-macro.
        // Linked output, plus `wasm` — a wasm32 target's `bin`/`cdylib`
        // link product (kunobi-ninja/kache#431). Without `wasm` here the
        // coverage gate saw an entry as not covering the `--emit=link` it
        // was built for, so every wasm module refused to store: on the
        // substrate bench that silently blocked the runtime crates, the
        // most expensive compiles in the build.
        "rlib" | "so" | "dylib" | "dll" | "exe" | "a" | "lib" | "wasm" => Some("link"),
        "rmeta" => Some("metadata"),
        "o" | "obj" => Some("obj"),
        "d" | "pp" => Some("dep-info"),
        "s" | "asm" => Some("asm"),
        "ll" => Some("llvm-ir"),
        "bc" => Some("llvm-bc"),
        "mir" => Some("mir"),
        // Extensionless file = bin executable (rustc's Unix convention).
        "" => Some("link"),
        _ => None,
    }
}

/// Why a signature is being applied. Today the only purpose is
/// [`SigningPurpose::OsLoading`], but `Sign(SigningPurpose)` is structured
/// this way so future cases (distribution signing, supply-chain attestation)
/// add a new variant rather than a new action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningPurpose {
    /// Re-establish a signature so the OS will load this artifact.
    /// macOS arm64 → ad-hoc codesign. Linux / Windows → no-op today.
    OsLoading,
}

/// One thing that needs to happen to a restored artifact before it's
/// ready for use. The wrapper composes a per-file plan via
/// [`plan_post_restore`].
///
/// An action is one of two kinds, distinguished by
/// [`PostRestoreAction::is_content_transform`]:
///   - a **content transform** — kache computes the new bytes itself
///     ([`PostRestoreAction::transform`]); applied in memory against the
///     store blob *before* the file is materialized, so the restored
///     file is written once already in final form.
///   - an **external mutation** — an OS tool rewrites the file in place
///     ([`PostRestoreAction::apply`]); run after the file is
///     materialized as a private, writable copy the tool can safely mutate.
///
/// Adding a new action variant means: classify it in
/// `is_content_transform`, one arm in `transform` or `apply`, one
/// condition in [`plan_post_restore`]. The wrapper restore loop does not
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostRestoreAction {
    /// Rewrite absolute paths inside a `.d` (dep-info) file so cargo's
    /// freshness stat()s find them in the current worktree's `target/`.
    ExpandDepInfoPaths,

    /// Apply a signature for the given purpose. Cross-platform — no-op on
    /// platforms that don't require it.
    Sign(SigningPurpose),

    /// Unpack a kache-produced debug-bundle tar ([`ArtifactKind::DebugBundle`])
    /// into a sibling bundle directory: `foo.dsym.tar` → `foo.dSYM` next to it,
    /// so lldb finds a UUID-matched `.dSYM` adjacent to the restored binary and
    /// the binary's stale `N_OSO` debug-map records become inert
    /// (kunobi-ninja/kache#319). The tar file itself stays materialized — it IS
    /// the cached artifact (a hardlinked store blob), and deleting it would
    /// break the blob accounting every other restored artifact follows.
    UnpackDebugBundle,
}

/// Compose the post-restore action sequence for an artifact, given its
/// kind. Pure function — testable per kind without filesystem.
///
/// Today the plan only depends on `kind`. When `Platform` lands as a
/// first-class abstraction, this signature gains `&platform` and signing
/// becomes conditional on the platform actually requiring it.
pub fn plan_post_restore(kind: ArtifactKind) -> Vec<PostRestoreAction> {
    let mut plan = Vec::new();
    if matches!(kind, ArtifactKind::DepInfo) {
        plan.push(PostRestoreAction::ExpandDepInfoPaths);
    }
    if matches!(
        kind,
        ArtifactKind::Executable | ArtifactKind::DynamicLibrary
    ) {
        plan.push(PostRestoreAction::Sign(SigningPurpose::OsLoading));
    }
    if matches!(kind, ArtifactKind::DebugBundle) {
        plan.push(PostRestoreAction::UnpackDebugBundle);
    }
    plan
}

impl PostRestoreAction {
    /// True if this action rewrites the artifact's *content*, with kache
    /// computing the new bytes itself (dep-info path expansion).
    ///
    /// Content transforms are applied **in memory against the store
    /// blob, before the file is materialized** ([`Self::transform`]) —
    /// the restore loop writes the result as a fresh file rather than
    /// linking the blob and patching it in place, which would fail on a
    /// read-only or inode-shared restore.
    ///
    /// False for actions that hand the file to an external OS tool
    /// (codesign), which needs a real, writable, private file on disk;
    /// those run via [`Self::apply`] after materialization.
    pub fn is_content_transform(self) -> bool {
        match self {
            PostRestoreAction::ExpandDepInfoPaths => true,
            // Unpacking creates *sibling* files on disk from an
            // already-materialized tar — it does not rewrite the tar's own
            // bytes, so it must run after materialization, not before.
            PostRestoreAction::Sign(_) => false,
            PostRestoreAction::UnpackDebugBundle => false,
        }
    }

    /// Apply this action as an in-memory content transform: store-blob
    /// bytes in, final restored bytes out.
    ///
    /// `anchor` is the directory dep-info (`.d`) relative paths expand
    /// against — cargo's target dir for *this* invocation (see
    /// [`crate::args::RustcArgs::target_dir`]). It MUST be the same kind
    /// of anchor the store side relativized with, or the
    /// relativize→expand round trip produces paths cargo's freshness
    /// `stat()`s cannot find.
    ///
    /// Only meaningful when [`Self::is_content_transform`] is true;
    /// other actions return the input unchanged.
    pub fn transform(self, content: Vec<u8>, anchor: &std::path::Path) -> Vec<u8> {
        match self {
            PostRestoreAction::ExpandDepInfoPaths => {
                // dep-info is UTF-8 text. If a `.d` somehow is not valid
                // UTF-8, pass it through untouched rather than risk
                // corrupting it.
                match String::from_utf8(content) {
                    Ok(text) => crate::link::rewrite_depinfo_content(
                        &text,
                        anchor,
                        crate::link::DepInfoMode::Expand,
                    )
                    .into_bytes(),
                    Err(e) => e.into_bytes(),
                }
            }
            PostRestoreAction::Sign(_) => content,
            PostRestoreAction::UnpackDebugBundle => content,
        }
    }

    /// Execute this action as an external mutation of an
    /// already-materialized file.
    ///
    /// The caller guarantees `path` is a **private, writable** file —
    /// not a shared link to a store blob — because external tools mutate
    /// the file in place and must never reach the cache blob. Only
    /// meaningful when [`Self::is_content_transform`] is false.
    ///
    /// `platform` is the host abstraction for OS-specific concerns
    /// (codesigning today; debug-path rewriting later). Passing it
    /// explicitly — rather than calling `platform::current()` here —
    /// keeps tests deterministic: a unit test can inject a counting /
    /// failing / no-op platform.
    pub fn apply(&self, path: &std::path::Path, platform: &dyn Platform) -> Result<()> {
        match self {
            PostRestoreAction::Sign(SigningPurpose::OsLoading) => {
                // Verify-then-sign lives inside the platform impl so
                // the kache-fork bug 59866c0 (mutating already-valid
                // signatures) can't be reintroduced from this site.
                platform.ensure_binary_loadable(path)
            }
            PostRestoreAction::UnpackDebugBundle => unpack_debug_bundle(path),
            PostRestoreAction::ExpandDepInfoPaths => {
                // A content transform — handled in memory via
                // `transform()` before materialization, never here.
                debug_assert!(
                    false,
                    "ExpandDepInfoPaths is a content transform; route it through transform()"
                );
                Ok(())
            }
        }
    }
}

/// Cap on total bytes written while unpacking one debug bundle. A `.dSYM`
/// is at most a few hundred MB even for very large binaries; anything past
/// 2 GiB is a corrupt or hostile archive, not debug info
/// (kunobi-ninja/kache#319; mirrors `remote_layout`'s extraction cap, #212).
const MAX_DEBUG_BUNDLE_BYTES: u64 = 2_147_483_648; // 2 GiB

/// Unpack a restored `<name>.dsym.tar` into a sibling `<name>.dSYM` bundle
/// directory (kunobi-ninja/kache#319).
///
/// The tar was produced by kache itself at store time
/// ([`Platform::package_debug_bundle`]) with entries relative to the bundle
/// root (`Contents/...`), but for a shared or MITM'd remote bucket the bytes
/// are attacker-influenced, so extraction is hardened like
/// `remote_layout::extract_entry_pack` (#211/#212): reject absolute/rooted
/// paths, `..` components, and links; cap total declared bytes. Extraction
/// goes to a private temp dir sibling first, then renames over the bundle
/// path, so a failed unpack never leaves a half-written `.dSYM` that lldb
/// would trust.
///
/// Errors propagate: the wrapper's restore loop treats any restore failure
/// as a clean miss and recompiles, which is exactly the right response to a
/// tampered or corrupt entry.
fn unpack_debug_bundle(tar_path: &std::path::Path) -> Result<()> {
    unpack_debug_bundle_with_cap(tar_path, MAX_DEBUG_BUNDLE_BYTES)
}

/// [`unpack_debug_bundle`] with an explicit byte cap, so the bomb guard is
/// testable without materializing a multi-GiB archive.
fn unpack_debug_bundle_with_cap(tar_path: &std::path::Path, max_bytes: u64) -> Result<()> {
    use anyhow::Context as _;

    let file_name = tar_path
        .file_name()
        .and_then(|n| n.to_str())
        .with_context(|| {
            format!(
                "debug bundle has no usable file name: {}",
                tar_path.display()
            )
        })?;
    let stem = file_name
        .strip_suffix(".dsym.tar")
        .with_context(|| format!("debug bundle artifact is not a `.dsym.tar`: {}", file_name))?;
    let parent = tar_path
        .parent()
        .with_context(|| format!("debug bundle has no parent dir: {}", tar_path.display()))?;
    let bundle_dir = parent.join(format!("{stem}.dSYM"));

    let tmp_dir = tempfile::Builder::new()
        .prefix(".kache-dsym-")
        .tempdir_in(parent)
        .context("creating temp dir for debug bundle unpack")?;

    let file = std::fs::File::open(tar_path)
        .with_context(|| format!("opening debug bundle {}", tar_path.display()))?;
    let mut archive = tar::Archive::new(file);
    let mut total_bytes = 0u64;
    for entry in archive.entries().context("reading debug bundle tar")? {
        let mut entry = entry.context("reading debug bundle tar entry")?;
        // Bomb guard: the declared entry sizes upper-bound what the tar
        // framing will ever yield, so reject before writing anything.
        total_bytes = total_bytes.saturating_add(entry.size());
        if total_bytes > max_bytes {
            anyhow::bail!(
                "debug bundle exceeds the {max_bytes}-byte extraction cap \
                 (corrupt or hostile archive)"
            );
        }
        let path = entry
            .path()
            .context("debug bundle entry path")?
            .to_path_buf();
        // A single portable check: on Unix an absolute path IS a leading
        // RootDir, and on Windows the Prefix arm also catches drive-relative
        // shapes (`C:x`) that `is_absolute()` misses — so the component test
        // subsumes `is_absolute()` on every platform.
        if matches!(
            path.components().next(),
            Some(std::path::Component::RootDir | std::path::Component::Prefix(_))
        ) {
            anyhow::bail!("debug bundle entry has absolute path: {}", path.display());
        }
        if path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        {
            anyhow::bail!("debug bundle entry has path traversal: {}", path.display());
        }
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            anyhow::bail!(
                "debug bundle entry is a link (rejected): {}",
                path.display()
            );
        }

        let dest = tmp_dir.path().join(&path);
        if entry_type.is_dir() {
            std::fs::create_dir_all(&dest)
                .with_context(|| format!("creating {}", dest.display()))?;
            continue;
        }
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        entry
            .unpack(&dest)
            .with_context(|| format!("unpacking debug bundle entry {}", path.display()))?;
    }

    // Replace any stale bundle atomically-ish: remove, then rename the fully
    // unpacked temp dir into place. A stale `.dSYM` from an earlier build at
    // this path would otherwise shadow the restored one for lldb.
    if bundle_dir.symlink_metadata().is_ok() {
        if bundle_dir.is_dir() {
            std::fs::remove_dir_all(&bundle_dir)
                .with_context(|| format!("removing stale bundle {}", bundle_dir.display()))?;
        } else {
            std::fs::remove_file(&bundle_dir)
                .with_context(|| format!("removing stale bundle {}", bundle_dir.display()))?;
        }
    }
    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, &bundle_dir).with_context(|| {
        format!(
            "publishing debug bundle {} -> {}",
            tmp_path.display(),
            bundle_dir.display()
        )
    })?;
    Ok(())
}

/// A cacheable compiler.
///
/// Implementations are state-light. Each owns its native parsed
/// representation as `Self::Parsed` so we don't flatten compiler-specific
/// shapes into one generic struct.
pub trait Compiler {
    type Parsed;

    fn id(&self) -> CompilerId;

    /// Parse raw argv into the compiler's native representation.
    /// Caller has already established this is the right compiler adapter via
    /// [`detect_compiler`].
    fn parse(&self, args: &[String]) -> Result<Self::Parsed>;

    /// Reasons (if any) this invocation must bypass the cache.
    /// Empty Vec = cacheable.
    fn refuse_reasons(&self, parsed: &Self::Parsed) -> Vec<RefuseReason>;

    /// Compute the cache key for a parsed invocation.
    fn cache_key(&self, parsed: &Self::Parsed, ctx: &KeyCtx<'_, '_>) -> Result<String>;

    /// Execute the compilation, capturing exit code, stdout, stderr, and
    /// the list of output files produced.
    fn execute(&self, parsed: &Self::Parsed) -> Result<CompileResult>;

    /// Classify an output file by its filename, given the parsed invocation
    /// for context (e.g. crate type to disambiguate executables from
    /// libraries when both share a no-extension shape).
    ///
    /// `name` is the filename only — no path components. Returns
    /// [`ArtifactKind::Other`] when the file doesn't match any known pattern;
    /// callers default to immutable / no-post-processing behavior in that
    /// case.
    fn classify_output(&self, parsed: &Self::Parsed, name: &str) -> ArtifactKind;
}

/// Adapter descriptors currently supported by kache.
///
/// Registration is deliberately concrete and local: adding an adapter means
/// adding its module-owned descriptor here, with no broad enum of possible
/// future tool kinds.
pub const COMPILER_ADAPTERS: &[CompilerAdapter] = &[rustc::ADAPTER, cc::ADAPTER];

/// Detect which compiler adapter an argv vector is invoking.
///
/// Each compiler impl owns its own `recognizes` rule; this function just walks
/// the descriptor list.
///
/// Returns `None` if no supported compiler matches — caller should
/// fall through to direct execution (or to compiler-family probe
/// handling via [`cc::CcCompiler::recognizes_family_probe`], which is its own
/// concern, not an adapter).
pub fn detect_compiler(args: &[String]) -> Option<&'static CompilerAdapter> {
    COMPILER_ADAPTERS
        .iter()
        .find(|adapter| adapter.recognizes(args))
}

/// Detect a `RUSTC_WRAPPER` + `RUSTC_WORKSPACE_WRAPPER` chain with an
/// unrecognized workspace wrapper. Cargo passes `<wrapper> rustc <args>`;
/// the wrapper may be an absolute path or a bare name resolved via PATH.
/// We match the inner rustc, not the wrapper name.
#[cfg(unix)]
fn is_executable(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &std::path::Path) -> bool {
    path.is_file()
}

pub(crate) fn is_kache_subcommand_or_flag(s: &str) -> bool {
    if s.starts_with('-') {
        return true;
    }
    use clap::CommandFactory;
    let mut cmd = crate::Cli::command();
    cmd.build();
    cmd.find_subcommand(s).is_some()
}

/// Do these compiler args (argv after the program) form a pure version/info
/// query rather than a compile? Cargo probes a toolchain this way — most
/// notably Kani's `kani-compiler -vV` (kunobi-ninja/kache#656). Such an
/// invocation compiles nothing, so there is nothing to cache; running an
/// *unknown* program just to sniff its `-E` family would add a spurious
/// invocation to a pure passthrough. Detection callers skip the probe for it.
///
/// Requires *every* arg to be a query flag (and at least one): a real compile
/// may carry a `-V`/`--version`-shaped **value** (e.g. an output file named
/// `-V`, `-o -V`, or `-MF --version`), which must still be recognized and
/// probed — matching any single arg would wrongly treat those as queries and
/// pass a cacheable compile through untouched. An empty arg list is a bare
/// invocation, not a query, so unknown compilers still probe.
pub(crate) fn is_version_or_info_query(args: &[String]) -> bool {
    !args.is_empty()
        && args.iter().all(|a| {
            matches!(
                a.as_str(),
                "-vV"
                    | "-V"
                    | "--version"
                    | "-dumpversion"
                    | "-dumpfullversion"
                    | "-dumpmachine"
                    | "-print-search-dirs"
                    | "--print-search-dirs"
            )
        })
}

pub(crate) fn resolve_program_on_path(program: &str) -> Option<std::path::PathBuf> {
    let path = std::env::var_os("PATH");
    let pathext = std::env::var_os("PATHEXT");
    resolve_program_on_path_with(program, path.as_deref(), pathext.as_deref())
}

fn resolve_program_on_path_with(
    program: &str,
    path: Option<&std::ffi::OsStr>,
    pathext: Option<&std::ffi::OsStr>,
) -> Option<std::path::PathBuf> {
    if program.contains('/') || program.contains('\\') {
        return Some(std::path::PathBuf::from(program));
    }
    let dirs: Vec<std::path::PathBuf> = std::env::split_paths(path?).collect();

    let extensions: Vec<String> = if cfg!(windows) {
        if let Some(pathext) = pathext {
            std::env::split_paths(pathext)
                .filter_map(|p| p.to_str().map(|s| s.to_string()))
                .collect()
        } else {
            vec![
                ".exe".to_string(),
                ".bat".to_string(),
                ".cmd".to_string(),
                ".com".to_string(),
            ]
        }
    } else {
        vec!["".to_string()]
    };

    for dir in dirs {
        let p = dir.join(program);
        if is_executable(&p) {
            return Some(p);
        }
        for ext in &extensions {
            if ext.is_empty() {
                continue;
            }
            let mut suffixed = p.clone().into_os_string();
            suffixed.push(ext);
            let suffixed_path = std::path::PathBuf::from(suffixed);
            if is_executable(&suffixed_path) {
                return Some(suffixed_path);
            }
        }
    }
    None
}

fn is_program_on_path(program: &str) -> bool {
    resolve_program_on_path(program).is_some()
}

/// Detect a real compiler invocation that must run uncached.
///
/// Cargo preserves `RUSTC` when it invokes `RUSTC_WRAPPER`, which identifies
/// custom drivers such as Kani's `kani-compiler` without maintaining a list of
/// tool names. `nvcc` remains an explicit passthrough because Kache supports it
/// as a compiler launcher but cannot safely cache its multi-phase outputs.
pub(crate) fn is_passthrough_compiler_invocation(args: &[String]) -> bool {
    let rustc = std::env::var_os("RUSTC");
    is_passthrough_compiler_invocation_with(args, rustc.as_deref())
}

pub(crate) fn is_passthrough_compiler_invocation_with(
    args: &[String],
    configured_rustc: Option<&std::ffi::OsStr>,
) -> bool {
    let Some(program) = args.first() else {
        return false;
    };
    if is_kache_subcommand_or_flag(program) {
        return false;
    }
    let is_configured_rustc =
        configured_rustc.is_some_and(|rustc| rustc == std::ffi::OsStr::new(program.as_str()));
    let is_nvcc = command_basename(program)
        .map(strip_windows_exe_suffix)
        .is_some_and(|name| name.eq_ignore_ascii_case("nvcc"));
    is_configured_rustc || is_nvcc
}

pub fn is_workspace_wrapper_chain(args: &[String]) -> bool {
    let workspace_wrapper = std::env::var_os("RUSTC_WORKSPACE_WRAPPER");
    is_workspace_wrapper_chain_with(args, workspace_wrapper.as_deref(), is_program_on_path)
}

fn is_workspace_wrapper_chain_with(
    args: &[String],
    workspace_wrapper: Option<&std::ffi::OsStr>,
    program_on_path: impl FnOnce(&str) -> bool,
) -> bool {
    if args.len() < 2 || !rustc::RustcCompiler::recognizes(&args[1..]) {
        return false;
    }
    if args[0].contains('/') || args[0].contains('\\') {
        return true;
    }
    if workspace_wrapper.is_some_and(|wrapper| wrapper == std::ffi::OsStr::new(&args[0])) {
        return true;
    }
    !is_kache_subcommand_or_flag(&args[0]) && program_on_path(&args[0])
}

/// Extract the bare command name from an `argv[0]`, splitting on both Unix
/// (`/`) and Windows (`\`) separators regardless of host OS.
///
/// [`std::path::Path::file_name`] is deliberately avoided: off-Windows it does
/// not treat `\` as a separator, so a Windows path like
/// `G:\…\bin\clippy-driver.exe` would come back whole. Every compiler adapter's
/// `recognizes` rule must see the same basename whether it runs on the target
/// platform or in a cross-platform test, so detection of e.g. `clippy-driver`
/// holds for both. Returns `None` when the trailing component is empty.
pub(crate) fn command_basename(arg0: &str) -> Option<&str> {
    arg0.rsplit(['/', '\\'])
        .next()
        .filter(|name| !name.is_empty())
}

/// Strip a trailing, case-insensitive `.exe` suffix (Windows executables) so
/// `rustc.exe` / `clippy-driver.exe` compare equal to their bare names.
pub(crate) fn strip_windows_exe_suffix(name: &str) -> &str {
    let bytes = name.as_bytes();
    if bytes.len() >= 4 && bytes[bytes.len() - 4..].eq_ignore_ascii_case(b".exe") {
        &name[..bytes.len() - 4]
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_or_info_query_needs_every_arg_to_be_a_query_flag() {
        let q = |a: &[&str]| {
            is_version_or_info_query(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        // Pure queries.
        assert!(q(&["-vV"]));
        assert!(q(&["--version"]));
        assert!(q(&["-dumpmachine"]));
        // A compile that merely carries a query-shaped *value* is NOT a query:
        // matching any single arg would wrongly skip recognition/probing and
        // pass a cacheable compile through untouched.
        assert!(!q(&["-c", "hello.c", "-o", "-V"]));
        assert!(!q(&["-MF", "--version"]));
        assert!(!q(&["-vV", "hello.c"]));
        // A bare invocation (no args) is not a query — unknown compilers must
        // still be probed (kunobi-ninja/kache#538).
        assert!(!q(&[]));
    }

    #[test]
    fn test_is_kache_subcommand_or_flag() {
        assert!(is_kache_subcommand_or_flag("help"));
        assert!(is_kache_subcommand_or_flag("-h"));
        assert!(is_kache_subcommand_or_flag("--help"));
        assert!(is_kache_subcommand_or_flag("-V"));
        assert!(is_kache_subcommand_or_flag("--version"));
        assert!(is_kache_subcommand_or_flag("gc"));
        assert!(is_kache_subcommand_or_flag("list"));
        assert!(!is_kache_subcommand_or_flag("not-a-subcommand"));
    }

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn detect_compiler_returns_none_for_empty_argv() {
        assert!(detect_compiler(&[]).is_none());
    }

    #[test]
    fn detect_compiler_recognizes_rustc_paths() {
        assert_eq!(
            detect_compiler(&s(&["rustc"])).map(|adapter| adapter.id()),
            Some(rustc::RUSTC_ID)
        );
        assert_eq!(
            detect_compiler(&s(&["/usr/bin/rustc", "src/lib.rs"])).map(|adapter| adapter.id()),
            Some(rustc::RUSTC_ID)
        );
        assert_eq!(
            detect_compiler(&s(&["clippy-driver"])).map(|adapter| adapter.id()),
            Some(rustc::RUSTC_ID)
        );
        // Regression for issue #287: the exact argv cargo passes for
        // `cargo clippy` on Windows. Detection must route this to the rustc
        // adapter (wrapper mode) rather than fall through to clap subcommand
        // parsing, which is what surfaced as "unrecognized subcommand".
        assert_eq!(
            detect_compiler(&s(&[
                r"G:\.rustup\toolchains\nightly-x86_64-pc-windows-msvc\bin\clippy-driver.exe",
                "rustc",
                "-vV",
            ]))
            .map(|adapter| adapter.id()),
            Some(rustc::RUSTC_ID)
        );
    }

    #[test]
    fn detect_compiler_recognizes_cc_paths() {
        assert_eq!(
            detect_compiler(&s(&["cc"])).map(|adapter| adapter.id()),
            Some(cc::CC_ID)
        );
        assert_eq!(
            detect_compiler(&s(&["gcc"])).map(|adapter| adapter.id()),
            Some(cc::CC_ID)
        );
        assert_eq!(
            detect_compiler(&s(&["clang++"])).map(|adapter| adapter.id()),
            Some(cc::CC_ID)
        );
        assert_eq!(
            detect_compiler(&s(&["/usr/bin/cc", "-c", "foo.c"])).map(|adapter| adapter.id()),
            Some(cc::CC_ID)
        );
        // Regression for issue #514: target-prefixed cross compilers must enter
        // wrapper mode instead of falling through to clap as unknown commands.
        assert_eq!(
            detect_compiler(&s(&[
                "/opt/cross/bin/arm-linux-gnueabihf-gcc",
                "-c",
                "foo.c",
            ]))
            .map(|adapter| adapter.id()),
            Some(cc::CC_ID)
        );
        assert!(detect_compiler(&s(&["arm-linux-gnueabihf-gcc-ar"])).is_none());
    }

    #[test]
    fn detect_compiler_returns_none_for_cc_probe_shape() {
        // The cc-crate compiler-family probe (`kache -E <file>`) is
        // intentionally NOT a compiler adapter — it's a non-compiler
        // invocation pattern handled separately in run_wrapper_mode
        // via `CcCompiler::recognizes_family_probe`. Asserting None
        // here pins that boundary: detect_compiler must not grow into
        // a grab-bag of "anything kache should passthrough".
        assert!(detect_compiler(&s(&["-E", "/tmp/probe.c"])).is_none());
        assert!(detect_compiler(&s(&["-E", "/tmp/detect_compiler_family.c"])).is_none());
    }

    #[test]
    fn detect_compiler_returns_none_for_unrelated_argv() {
        assert!(detect_compiler(&s(&["cargo", "build"])).is_none());
        assert!(detect_compiler(&s(&["make"])).is_none());
        assert!(detect_compiler(&s(&["ld"])).is_none());
        assert!(detect_compiler(&s(&["--crate-name"])).is_none());
    }

    #[test]
    fn workspace_wrapper_chain_detects_unrecognized_drivers() {
        // Issue #505: dylint-driver and any future RUSTC_WORKSPACE_WRAPPER
        // tool. Cargo passes `kache <wrapper-path> rustc <args>`.
        assert!(is_workspace_wrapper_chain(&s(&[
            "/Users/dev/.dylint_drivers/nightly/dylint-driver",
            "rustc",
            "--crate-name",
        ])));
        // Windows backslash path (host-OS-independent).
        assert!(is_workspace_wrapper_chain(&s(&[
            r"C:\tools\custom-driver.exe",
            "rustc",
        ])));
    }

    #[test]
    fn workspace_wrapper_chain_detects_bare_name_via_env() {
        // Cargo may pass a bare wrapper name (resolved via PATH) when
        // RUSTC_WORKSPACE_WRAPPER is set without a path separator.
        let args = s(&["mydriver", "rustc"]);
        assert!(is_workspace_wrapper_chain_with(
            &args,
            Some(std::ffi::OsStr::new("mydriver")),
            |_| false,
        ));
        assert!(!is_workspace_wrapper_chain_with(
            &args,
            Some(std::ffi::OsStr::new("other-driver")),
            |_| false,
        ));
    }

    #[test]
    fn workspace_wrapper_chain_detects_bare_name_via_path() {
        use std::fs::File;
        let temp_dir = tempfile::TempDir::new().unwrap();
        let wrapper_name = "custom-wrapper-test-executable";
        #[cfg(windows)]
        {
            let wrapper_path_exe = temp_dir.path().join(format!("{}.exe", wrapper_name));
            File::create(&wrapper_path_exe).unwrap();
        }
        #[cfg(not(windows))]
        {
            let wrapper_path = temp_dir.path().join(wrapper_name);
            File::create(&wrapper_path).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&wrapper_path).unwrap().permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&wrapper_path, perms).unwrap();
            }
        }

        let test_path = std::env::join_paths([temp_dir.path()]).unwrap();
        assert!(is_workspace_wrapper_chain_with(
            &s(&[wrapper_name, "rustc"]),
            None,
            |program| {
                resolve_program_on_path_with(program, Some(test_path.as_os_str()), None).is_some()
            },
        ));
    }

    #[test]
    fn workspace_wrapper_chain_rejects_non_paths() {
        // No path separator and not RUSTC_WORKSPACE_WRAPPER → CLI subcommand.
        for subcommand in ["init", "gc", "doctor", "config", "report"] {
            assert!(!is_workspace_wrapper_chain_with(
                &s(&[subcommand, "rustc"]),
                None,
                |_| true,
            ));
        }

        // Non-existent executable name
        assert!(!is_workspace_wrapper_chain_with(
            &s(&["nonexistentwrappername12345", "rustc"]),
            None,
            |_| false,
        ));

        // Inner arg not rustc.
        assert!(!is_workspace_wrapper_chain(&s(&["/usr/bin/cc", "file.c"])));
        assert!(!is_workspace_wrapper_chain(&s(&["cargo", "build"])));

        // Too few args.
        assert!(!is_workspace_wrapper_chain(&s(&["/usr/bin/rustc"])));
    }

    #[test]
    fn passthrough_compiler_accepts_configured_rustc_and_nvcc() {
        assert!(is_passthrough_compiler_invocation_with(
            &s(&["/home/user/.kani/kani-0.67.0/bin/kani-compiler", "-vV"]),
            Some(std::ffi::OsStr::new(
                "/home/user/.kani/kani-0.67.0/bin/kani-compiler",
            )),
        ));
        assert!(is_passthrough_compiler_invocation_with(
            &s(&["custom-rustc-driver", "--crate-name", "demo"]),
            Some(std::ffi::OsStr::new("custom-rustc-driver")),
        ));
        assert!(is_passthrough_compiler_invocation_with(
            &s(&[r"C:\CUDA\bin\nvcc.exe", "-c", "kernel.cu"]),
            None,
        ));
        assert!(is_passthrough_compiler_invocation_with(
            &s(&["nvcc", "-c", "kernel.cu"]),
            None,
        ));
    }

    #[test]
    fn passthrough_compiler_rejects_unrelated_programs() {
        // Exercise the environment-reading facade too. A Kache command must
        // never become a compiler invocation, even if RUSTC has the same name.
        assert!(!is_passthrough_compiler_invocation(&s(&["gc"])));
        assert!(!is_passthrough_compiler_invocation_with(&[], None));
        assert!(!is_passthrough_compiler_invocation_with(
            &s(&["stat"]),
            None,
        ));
        assert!(!is_passthrough_compiler_invocation_with(
            &s(&["/usr/bin/stat"]),
            Some(std::ffi::OsStr::new("/usr/bin/other-driver")),
        ));
        assert!(!is_passthrough_compiler_invocation_with(
            &s(&["gc"]),
            Some(std::ffi::OsStr::new("gc")),
        ));
    }

    #[test]
    fn command_basename_splits_both_separators() {
        assert_eq!(command_basename("rustc"), Some("rustc"));
        assert_eq!(command_basename("/usr/bin/rustc"), Some("rustc"));
        // Windows backslash paths resolve identically on every host OS —
        // std::path::Path::file_name would not split these off-Windows.
        assert_eq!(
            command_basename(r"G:\bin\clippy-driver.exe"),
            Some("clippy-driver.exe")
        );
        assert_eq!(command_basename(r"C:\a/b\c.exe"), Some("c.exe"));
        // A trailing separator leaves no command name.
        assert_eq!(command_basename("/usr/bin/"), None);
        assert_eq!(command_basename(r"C:\bin\"), None);
        assert_eq!(command_basename(""), None);
    }

    #[test]
    fn strip_windows_exe_suffix_is_case_insensitive_and_optional() {
        assert_eq!(strip_windows_exe_suffix("rustc.exe"), "rustc");
        assert_eq!(
            strip_windows_exe_suffix("clippy-driver.EXE"),
            "clippy-driver"
        );
        // No suffix: returned unchanged.
        assert_eq!(strip_windows_exe_suffix("rustc"), "rustc");
        // `.exe` is only stripped from the end, never mid-name.
        assert_eq!(strip_windows_exe_suffix("a.exe.b"), "a.exe.b");
        // Too short to carry a `.exe` suffix.
        assert_eq!(strip_windows_exe_suffix(".ex"), ".ex");
    }

    #[test]
    fn plan_post_restore_dep_info_expands_paths() {
        assert_eq!(
            plan_post_restore(ArtifactKind::DepInfo),
            vec![PostRestoreAction::ExpandDepInfoPaths]
        );
    }

    #[test]
    fn plan_post_restore_executable_signs_for_os_loading() {
        assert_eq!(
            plan_post_restore(ArtifactKind::Executable),
            vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)]
        );
    }

    #[test]
    fn plan_post_restore_dynamic_library_signs_for_os_loading() {
        // Same plan as Executable: dylibs are loaded by the dynamic linker
        // and need an OS-acceptable signature on macOS arm64. Encoded as a
        // single condition in `plan_post_restore` so adding a third
        // OS-loaded kind requires changing one place.
        assert_eq!(
            plan_post_restore(ArtifactKind::DynamicLibrary),
            vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)]
        );
    }

    #[test]
    fn plan_post_restore_object_is_empty() {
        // Regression guard: `.o` / `.rcgu.o` files must not pick up any
        // post-restore action — in particular not codesign (kache-fork
        // bug 572f321).
        assert!(plan_post_restore(ArtifactKind::Object).is_empty());
    }

    #[test]
    fn plan_post_restore_passive_kinds_are_empty() {
        // DebugBundle is deliberately NOT in this list: it is the one
        // non-executable kind with a post-restore action (the unpack,
        // see #319) — pinned separately below.
        for kind in [
            ArtifactKind::Library,
            ArtifactKind::Metadata,
            ArtifactKind::DebugSidecar,
            // A wasm module is never OS-loaded, so it must NOT pick up the
            // codesign action its Copy-strategy siblings get (#431).
            ArtifactKind::WasmModule,
            ArtifactKind::Other("test"),
        ] {
            assert!(
                plan_post_restore(kind).is_empty(),
                "{kind:?} should have no post-restore actions"
            );
        }
    }

    #[test]
    fn plan_post_restore_debug_bundle_unpacks_exactly() {
        // kunobi-ninja/kache#319: the bundle tar gets exactly the unpack —
        // in particular NOT codesign (it is not a loadable binary) and NOT
        // dep-info expansion.
        assert_eq!(
            plan_post_restore(ArtifactKind::DebugBundle),
            vec![PostRestoreAction::UnpackDebugBundle]
        );
    }

    // ── transform() / apply() ────────────────────────────────────
    //
    // Coverage for the action executors. ExpandDepInfoPaths is a content
    // transform: it maps store-blob bytes to final bytes in memory.
    // Sign(OsLoading) is an external mutation routed through the
    // injected Platform.

    #[test]
    fn expand_dep_info_paths_is_a_content_transform() {
        // The classification that routes an action to `transform` (in
        // memory, pre-materialization) vs `apply` (external, post-).
        assert!(PostRestoreAction::ExpandDepInfoPaths.is_content_transform());
        assert!(!PostRestoreAction::Sign(SigningPurpose::OsLoading).is_content_transform());
        // The unpack needs the tar materialized on disk first, so it is an
        // external (post-materialization) action, and its transform leg
        // passes the tar bytes through untouched.
        assert!(!PostRestoreAction::UnpackDebugBundle.is_content_transform());
        let bytes = b"tar bytes".to_vec();
        assert_eq!(
            PostRestoreAction::UnpackDebugBundle
                .transform(bytes.clone(), std::path::Path::new("/anchor")),
            bytes
        );
    }

    #[test]
    fn transform_expand_dep_info_paths_roots_relative_paths_at_anchor() {
        // The sentinel-path shape `rewrite_depinfo_content`'s Relativize
        // mode produces; Expand (the restore-side transform) reverses it.
        // The anchor is the restoring build's target dir — NOT the
        // process cwd.
        let blob = b"__kache_root__/target/debug/foo: __kache_root__/src/lib.rs".to_vec();
        let anchor = std::path::Path::new("/restored/worktree");

        let out = PostRestoreAction::ExpandDepInfoPaths.transform(blob, anchor);
        let content = String::from_utf8(out).unwrap();

        assert!(
            content.contains("/restored/worktree/target/debug/foo"),
            "expected anchor-rooted target path, got: {content}"
        );
        assert!(
            content.contains("/restored/worktree/src/lib.rs"),
            "expected anchor-rooted source path, got: {content}"
        );
        assert!(
            !content.contains("__kache_root__/"),
            "no kache dep-info markers should remain, got: {content}"
        );
    }

    #[test]
    fn transform_expand_dep_info_paths_preserves_parent_relative_deps() {
        let blob =
            b"foo.o: ../../src/foo.cc ../include/foo.h __kache_root__/generated/header.h".to_vec();
        let anchor = std::path::Path::new("/restored/worktree/obj");

        let out = PostRestoreAction::ExpandDepInfoPaths.transform(blob, anchor);
        let content = String::from_utf8(out).unwrap();

        assert!(
            content.contains("../../src/foo.cc"),
            "compiler-emitted parent-relative source paths must survive: {content}"
        );
        assert!(
            content.contains("../include/foo.h"),
            "compiler-emitted parent-relative header paths must survive: {content}"
        );
        assert!(
            content.contains("/restored/worktree/obj/generated/header.h"),
            "kache sentinel paths should still expand: {content}"
        );
    }

    #[test]
    fn transform_expand_dep_info_paths_passes_through_non_utf8() {
        // A `.d` is always UTF-8 in practice, but the transform must
        // never corrupt bytes it can't interpret — it returns them
        // unchanged rather than panicking.
        let blob = vec![0xff, 0xfe, 0x00, 0x42];
        let out = PostRestoreAction::ExpandDepInfoPaths
            .transform(blob.clone(), std::path::Path::new("/anchor"));
        assert_eq!(out, blob);
    }

    #[test]
    fn apply_sign_os_loading_routes_through_platform() {
        // The dispatch contract: Sign(OsLoading) must hand off to the
        // platform's ensure_binary_loadable, not re-implement codesign
        // logic in-line. CountingPlatform proves the call happened
        // exactly once per apply().
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-actually-a-binary");
        std::fs::write(&path, b"definitely not Mach-O").unwrap();

        let platform = CountingPlatform::new();
        PostRestoreAction::Sign(SigningPurpose::OsLoading)
            .apply(&path, &platform)
            .expect("apply must not error even when the platform impl is a no-op");
        assert_eq!(
            platform.ensure_calls(),
            1,
            "Sign(OsLoading) must dispatch to platform.ensure_binary_loadable exactly once"
        );
    }

    // ── UnpackDebugBundle (kunobi-ninja/kache#319) ───────────────

    /// Build an in-memory tar with the given `(path, content)` regular-file
    /// entries — both well-formed bundles and malicious shapes for the
    /// hardening tests.
    fn synthetic_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            header.set_entry_type(tar::EntryType::Regular);
            builder
                .append_data(&mut header, path, &content[..])
                .unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[test]
    fn apply_unpack_debug_bundle_creates_sibling_dsym_dir() {
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("foo-abc123.dsym.tar");
        std::fs::write(
            &tar_path,
            synthetic_tar(&[
                ("Contents/Info.plist", b"plist"),
                ("Contents/Resources/DWARF/foo-abc123", b"dwarf bytes"),
            ]),
        )
        .unwrap();

        PostRestoreAction::UnpackDebugBundle
            .apply(&tar_path, &CountingPlatform::new())
            .unwrap();

        // `foo-abc123.dsym.tar` → sibling `foo-abc123.dSYM` bundle dir.
        let bundle = dir.path().join("foo-abc123.dSYM");
        assert_eq!(
            std::fs::read(bundle.join("Contents/Resources/DWARF/foo-abc123")).unwrap(),
            b"dwarf bytes"
        );
        assert_eq!(
            std::fs::read(bundle.join("Contents/Info.plist")).unwrap(),
            b"plist"
        );
        // The tar stays materialized: it IS the cached artifact (a
        // hardlinked store blob) and blob accounting expects it on disk.
        assert!(tar_path.is_file(), "the restored tar must not be deleted");
    }

    #[test]
    fn apply_unpack_debug_bundle_replaces_stale_bundle() {
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("foo.dSYM");
        std::fs::create_dir_all(bundle.join("Contents")).unwrap();
        std::fs::write(bundle.join("Contents/stale"), b"old").unwrap();

        let tar_path = dir.path().join("foo.dsym.tar");
        std::fs::write(
            &tar_path,
            synthetic_tar(&[("Contents/Resources/DWARF/foo", b"new dwarf")]),
        )
        .unwrap();

        PostRestoreAction::UnpackDebugBundle
            .apply(&tar_path, &CountingPlatform::new())
            .unwrap();

        assert!(
            !bundle.join("Contents/stale").exists(),
            "a stale bundle must be replaced wholesale, not merged — lldb \
             would otherwise trust leftover files from another build"
        );
        assert_eq!(
            std::fs::read(bundle.join("Contents/Resources/DWARF/foo")).unwrap(),
            b"new dwarf"
        );
    }

    /// A tar whose single entry carries a raw (hostile) name that
    /// `tar::Builder` itself refuses to write — forged by patching the
    /// header's name field and re-checksumming, exactly what an attacker
    /// controlling a shared bucket would serve.
    fn forged_tar_with_entry_name(name: &[u8]) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.set_size(5);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        let mut builder = tar::Builder::new(Vec::new());
        builder
            .append_data(&mut header, "placeholder", &b"pwned"[..])
            .unwrap();
        let mut bytes = builder.into_inner().unwrap();
        assert!(name.len() < 100, "GNU tar name field is 100 bytes");
        bytes[..name.len()].copy_from_slice(name);
        bytes[name.len()..100].fill(0);
        // Recompute the header checksum the tar reader validates.
        let mut patched = tar::Header::new_gnu();
        patched.as_mut_bytes().copy_from_slice(&bytes[..512]);
        patched.set_cksum();
        bytes[..512].copy_from_slice(patched.as_bytes());
        bytes
    }

    #[test]
    fn apply_unpack_debug_bundle_rejects_path_traversal() {
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let outdir = dir.path().join("deps");
        std::fs::create_dir_all(&outdir).unwrap();
        let tar_path = outdir.join("evil.dsym.tar");
        std::fs::write(&tar_path, forged_tar_with_entry_name(b"../escaped-file")).unwrap();

        let err = PostRestoreAction::UnpackDebugBundle
            .apply(&tar_path, &CountingPlatform::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("path traversal"),
            "a `..` entry must be rejected, got: {err}"
        );
        assert!(
            !dir.path().join("escaped-file").exists(),
            "nothing may be written outside the temp extraction dir"
        );
        assert!(
            !outdir.join("evil.dSYM").exists(),
            "a rejected archive must not publish a bundle"
        );
    }

    #[test]
    fn apply_unpack_debug_bundle_rejects_absolute_entry() {
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("abs.dsym.tar");
        std::fs::write(
            &tar_path,
            forged_tar_with_entry_name(b"/tmp/kache-absolute-escape"),
        )
        .unwrap();

        let err = PostRestoreAction::UnpackDebugBundle
            .apply(&tar_path, &CountingPlatform::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("absolute path"),
            "an absolute entry must be rejected, got: {err}"
        );
    }

    #[test]
    fn apply_unpack_debug_bundle_rejects_non_dsym_tar_name() {
        use crate::compiler::platform::tests::CountingPlatform;
        // Structural misuse — the action planned for a file that is not a
        // `.dsym.tar` cannot derive a bundle path, and silently unpacking
        // somewhere would be worse than a clean restore failure.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("foo.tar");
        std::fs::write(&path, synthetic_tar(&[("Contents/x", b"y")])).unwrap();
        let err = PostRestoreAction::UnpackDebugBundle
            .apply(&path, &CountingPlatform::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains(".dsym.tar"), "got: {err}");
    }

    /// Symlink and hardlink entries are each rejected ON THEIR OWN — the
    /// two link kinds are independent smuggling vectors, so neither may
    /// depend on the other also being present (kunobi-ninja/kache#319).
    #[test]
    fn apply_unpack_debug_bundle_rejects_each_link_kind_alone() {
        use crate::compiler::platform::tests::CountingPlatform;
        for entry_type in [tar::EntryType::Symlink, tar::EntryType::Link] {
            let dir = tempfile::tempdir().unwrap();
            let tar_path = dir.path().join("linky.dsym.tar");
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(entry_type);
            let mut builder = tar::Builder::new(Vec::new());
            builder
                .append_link(&mut header, "Contents/evil", "/etc/passwd")
                .unwrap();
            std::fs::write(&tar_path, builder.into_inner().unwrap()).unwrap();

            let err = PostRestoreAction::UnpackDebugBundle
                .apply(&tar_path, &CountingPlatform::new())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("is a link"),
                "{entry_type:?} alone must be rejected, got: {err}"
            );
        }
    }

    /// The extraction cap rejects strictly past the limit and accepts a
    /// bundle landing exactly ON it — the boundary a corrupt-size header
    /// would probe (kunobi-ninja/kache#319).
    #[test]
    fn unpack_debug_bundle_cap_boundary_is_exact() {
        let payload = vec![b'x'; 100];
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("capped.dsym.tar");
        std::fs::write(
            &tar_path,
            synthetic_tar(&[("Contents/blob", payload.as_slice())]),
        )
        .unwrap();

        let err = unpack_debug_bundle_with_cap(&tar_path, 99)
            .unwrap_err()
            .to_string();
        assert!(err.contains("extraction cap"), "got: {err}");
        assert!(!dir.path().join("capped.dSYM").exists());

        unpack_debug_bundle_with_cap(&tar_path, 100)
            .expect("a bundle exactly at the cap is within budget");
        assert!(dir.path().join("capped.dSYM/Contents/blob").exists());
    }

    /// `ArtifactSet::push` genuinely appends — the store-time debug bundle
    /// rides on it (kunobi-ninja/kache#319).
    #[test]
    fn artifact_set_push_appends_the_artifact() {
        let mut set = ArtifactSet::empty();
        set.push(Artifact {
            path: std::path::PathBuf::from("/tmp/x.dsym.tar"),
            store_name: "x.dsym.tar".to_string(),
            kind: ArtifactKind::DebugBundle,
            required: false,
        });
        assert_eq!(set.outputs().len(), 1);
        assert_eq!(set.outputs()[0].store_name, "x.dsym.tar");
        assert_eq!(set.outputs()[0].kind, ArtifactKind::DebugBundle);
    }

    // ── classify → plan integration ──────────────────────────────
    //
    // The wrapper does `compiler.classify_output(...) → plan_post_restore(...)`
    // per cached file. These tests exercise that chain end-to-end so a
    // mistake in either side (e.g. `.rcgu.o` getting classified as
    // Executable, or a kind silently picking up the wrong actions) is
    // caught here without needing wrapper-level integration plumbing.

    #[test]
    fn rustc_classify_to_plan_chain_for_typical_lib_build() {
        use crate::compiler::rustc::RustcCompiler;
        let compiler = RustcCompiler::new();
        let lib_args = compiler
            .parse(&[
                "rustc".into(),
                "src/lib.rs".into(),
                "--crate-name".into(),
                "foo".into(),
                "--crate-type".into(),
                "lib".into(),
            ])
            .unwrap();

        let cases: &[(&str, Vec<PostRestoreAction>)] = &[
            ("libfoo-abc.rlib", vec![]),
            ("libfoo-abc.rmeta", vec![]),
            ("foo-abc.d", vec![PostRestoreAction::ExpandDepInfoPaths]),
            ("foo-abc.rcgu.o", vec![]),
            ("foo-abc.dwo", vec![]),
        ];

        for (name, expected) in cases {
            let kind = compiler.classify_output(&lib_args, name);
            assert_eq!(
                &plan_post_restore(kind),
                expected,
                "for {name}: kind = {kind:?}"
            );
        }
    }

    #[test]
    fn classify_by_filename_recognizes_known_extensions() {
        // Single source of truth — every caller in the codebase that does
        // suffix matching should delegate here. Locking the mapping in.
        assert_eq!(
            classify_by_filename("libfoo-abc.rlib"),
            ArtifactKind::Library
        );
        assert_eq!(
            classify_by_filename("libfoo-abc.rmeta"),
            ArtifactKind::Metadata
        );
        assert_eq!(classify_by_filename("foo-abc.d"), ArtifactKind::DepInfo);
        assert_eq!(
            classify_by_filename("host_pathsub.o.pp"),
            ArtifactKind::DepInfo
        );
        assert_eq!(classify_by_filename("foo.o"), ArtifactKind::Object);
        assert_eq!(
            classify_by_filename("foo-abc.123.rcgu.o"),
            ArtifactKind::Object
        );
        assert_eq!(classify_by_filename("foo.obj"), ArtifactKind::Object);
        assert_eq!(
            classify_by_filename("libfoo.dylib"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("libfoo.so"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("rococo_runtime.wasm"),
            ArtifactKind::WasmModule
        );
        assert_eq!(
            classify_by_filename("foo.dll"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("foo-abc.dwo"),
            ArtifactKind::DebugSidecar
        );
        assert_eq!(classify_by_filename("foo.pdb"), ArtifactKind::DebugSidecar);
        assert_eq!(classify_by_filename("foo.exe"), ArtifactKind::Executable);
        // kache's own store-time debug bundle tar (#319): the compound
        // `.dsym.tar` suffix must win over the bare "tar" extension, which
        // would otherwise classify Other("unknown-ext") and lose the
        // restore-side unpack dispatch.
        assert_eq!(
            classify_by_filename("foo-abc123.dsym.tar"),
            ArtifactKind::DebugBundle
        );
        assert_eq!(
            classify_by_filename("foo.tar"),
            ArtifactKind::Other("unknown-ext")
        );
        // DebugBundle restores via hardlink like every immutable kind — the
        // tar is never mutated in place (the unpack writes siblings).
        assert_eq!(
            ArtifactKind::DebugBundle.link_strategy(),
            LinkStrategy::Hardlink
        );
    }

    #[test]
    fn classify_by_filename_distinguishes_extensionless_from_unknown() {
        // Two distinct "Other" tags so callers can choose what convention
        // to apply: target/-scan callers treat extensionless as bin output;
        // others fall back to safe defaults.
        match classify_by_filename("my_bin-abc123") {
            ArtifactKind::Other("extensionless") => {}
            other => panic!("expected Other(extensionless), got {other:?}"),
        }
        match classify_by_filename("foo.lock") {
            ArtifactKind::Other("unknown-ext") => {}
            other => panic!("expected Other(unknown-ext), got {other:?}"),
        }
    }

    /// kunobi-ninja/kache#325: filename → canonical `--emit` kind, the SSOT for
    /// the emit-coverage gate. Every mapped value is in [`GATED_EMIT_KINDS`];
    /// unmapped sidecars return `None`.
    #[test]
    fn emit_kind_for_filename_maps_outputs() {
        let cases = [
            ("libfoo-abc.rlib", Some("link")),
            ("libfoo.so", Some("link")),
            ("libfoo.dylib", Some("link")),
            ("foo.dll", Some("link")),
            ("foo.exe", Some("link")),
            // A wasm32 target's link product (#431): before this, the
            // coverage gate saw a `--emit=link` wasm entry as covering
            // nothing, so every wasm module refused to store.
            ("rococo_runtime.wasm", Some("link")),
            ("my_bin-abc123", Some("link")), // extensionless bin
            ("libfoo-abc.rmeta", Some("metadata")),
            ("foo-abc.123.rcgu.o", Some("obj")),
            ("foo.obj", Some("obj")),
            ("foo-abc.d", Some("dep-info")),
            ("foo.s", Some("asm")),
            ("foo.ll", Some("llvm-ir")),
            ("foo.bc", Some("llvm-bc")),
            ("foo.mir", Some("mir")),
            ("foo.dwo", None),
            ("foo.pdb", None),
            ("foo.lock", None),
            // The store-time debug bundle (#319) satisfies no `--emit` kind —
            // it must never make the emit-coverage gate think an entry covers
            // something it doesn't.
            ("foo-abc.dsym.tar", None),
        ];
        for (name, expected) in cases {
            assert_eq!(emit_kind_for_filename(name), expected, "for {name}");
            if let Some(kind) = expected {
                assert!(
                    GATED_EMIT_KINDS.contains(&kind),
                    "{kind} (from {name}) must be in GATED_EMIT_KINDS"
                );
            }
        }
    }

    #[test]
    fn rustc_classify_to_plan_chain_for_typical_bin_build() {
        use crate::compiler::rustc::RustcCompiler;
        let compiler = RustcCompiler::new();
        let bin_args = compiler
            .parse(&[
                "rustc".into(),
                "src/main.rs".into(),
                "--crate-name".into(),
                "foo".into(),
                "--crate-type".into(),
                "bin".into(),
            ])
            .unwrap();

        let cases: &[(&str, Vec<PostRestoreAction>)] = &[
            // Extensionless binary on Unix → Executable → must sign.
            (
                "foo-abc",
                vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)],
            ),
            // Dep-info still rewrites paths even in a bin build.
            ("foo-abc.d", vec![PostRestoreAction::ExpandDepInfoPaths]),
            // Per-codegen-unit object files must NEVER pick up codesign
            // (kache-fork bug 572f321). This case is the regression guard
            // for the whole bug class.
            ("foo-abc.rcgu.o", vec![]),
            // Debug sidecars are passive too.
            ("foo-abc.dwo", vec![]),
            // The store-time macOS debug bundle (#319): classified through
            // the same chain restore uses, so a cached `.dsym.tar` picks up
            // exactly the unpack — and never codesign.
            (
                "foo-abc.dsym.tar",
                vec![PostRestoreAction::UnpackDebugBundle],
            ),
        ];

        for (name, expected) in cases {
            let kind = compiler.classify_output(&bin_args, name);
            assert_eq!(
                &plan_post_restore(kind),
                expected,
                "for {name}: kind = {kind:?}"
            );
        }
    }
}
