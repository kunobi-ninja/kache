//! Whether a Windows executable and the `.pdb` beside it belong together.
//!
//! An MSVC-linked `.exe` names its program database in a CodeView `RSDS`
//! record of its debug directory: a GUID, an age and a path. A debugger loads
//! a `.pdb` only when its GUID and executable-validation ages agree with
//! that record, and looks for it by
//! the recorded path, then by the same file name next to the executable.
//! rustc links with `/PDBALTPATH:%_PDB%`, so the record holds the file name
//! alone (`foo-0123abcd.pdb`): no target directory, user name or worktree.
//!
//! A PE executable is admitted when it was not linked incrementally (no
//! `.ilk` beside it), and its record is absent (no external debug
//! information) or names a `.pdb` by bare file name, that file is one of the
//! outputs stored with it, and its GUID and age match. A restore
//! then puts both back side by side, exactly as the link left them. Anything
//! else (an absolute `/PDBALTPATH`, a `/PDB:` elsewhere, a `.pdb` the link
//! left behind from an earlier build) keeps the executable out of the cache:
//! the compile has already run, so nothing but the cache entry is lost.
//!
//! Only the bytes needed are read: the PE headers, the debug directory and
//! its record, and from the `.pdb` (an MSF 7.0 container) its superblock,
//! stream directory, PDB information stream and DBI stream header. Cache hits
//! validate the same pair before restoring any output. The linked invocation
//! selects an explicit `-o` output even without an `.exe` suffix; an unknown
//! or ambiguous output cannot be cached. Other executable formats
//! keep their existing handling, including ELF binaries named with `.exe`.

use anyhow::{Context, Result, bail, ensure};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// `IMAGE_DEBUG_TYPE_CODEVIEW`.
const DEBUG_TYPE_CODEVIEW: u32 = 2;
/// Index of the debug directory in the optional header's data directories.
const DEBUG_DATA_DIRECTORY: u32 = 6;
/// Size of one `IMAGE_DEBUG_DIRECTORY` entry.
const DEBUG_ENTRY_SIZE: usize = 28;
/// Size of one `IMAGE_SECTION_HEADER`.
const SECTION_HEADER_SIZE: usize = 40;
/// The MSF 7.0 superblock's magic.
const MSF_MAGIC: &[u8; 32] = b"Microsoft C/C++ MSF 7.00\r\n\x1aDS\0\0\0";
/// The PDB information stream, which carries the GUID and age.
const PDB_INFO_STREAM: usize = 1;
/// The DBI stream, whose header repeats the age.
const DBI_STREAM: usize = 3;
/// The largest stream directory read; a multi-gigabyte `.pdb` stays far below.
const MAX_DIRECTORY_BYTES: u64 = 64 << 20;
/// The largest CodeView record read; its path is at most `MAX_PATH`-ish.
const MAX_CODEVIEW_BYTES: u32 = 4096;
/// A conservative admission limit; ordinary linkers write only a few entries.
const MAX_DEBUG_ENTRIES: u32 = 4096;

/// The program database an executable names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeViewRecord {
    pub(crate) guid: [u8; 16],
    pub(crate) age: u32,
    pub(crate) pdb_path: String,
}

/// A program database's identity: its GUID, and the ages a debugger may
/// compare with the executable's record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PdbIdentity {
    pub(crate) guid: [u8; 16],
    pub(crate) info_age: u32,
    pub(crate) dbi_age: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExecutableDebug {
    OtherFormat,
    NoCodeView,
    CodeView(CodeViewRecord),
}

/// What a store learns about one executable output.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pairing {
    /// A PE without a CodeView record naming an external PDB.
    SelfContained,
    /// The named `.pdb` is stored with it and matches.
    Paired { pdb_store_name: String },
}

/// Check `.exe` outputs and the invocation-selected executable against their
/// named program databases, including `-o app` and custom suffixes.
///
/// `outputs` holds each output's path and store name. `Ok` lists what was
/// found for each executable; `Err` explains why the unit must not be cached.
pub(crate) fn check_outputs<'a>(
    outputs: impl IntoIterator<Item = (&'a Path, &'a str)> + Clone,
    executable_name: Option<&str>,
) -> Result<Vec<Pairing>> {
    check_outputs_in(outputs, executable_name, true)
}

/// Check stored blobs before publishing a cache hit. Blob paths are content
/// addresses, so a sibling `.ilk` there says nothing about the original link.
pub(crate) fn check_cached_outputs<'a>(
    outputs: impl IntoIterator<Item = (&'a Path, &'a str)> + Clone,
    executable_name: Option<&str>,
) -> Result<Vec<Pairing>> {
    check_outputs_in(outputs, executable_name, false)
}

fn check_outputs_in<'a>(
    outputs: impl IntoIterator<Item = (&'a Path, &'a str)> + Clone,
    executable_name: Option<&str>,
    compiler_outputs: bool,
) -> Result<Vec<Pairing>> {
    let mut found = Vec::new();
    for (path, name) in outputs.clone() {
        if !is_windows_executable_name(name) && executable_name != Some(name) {
            continue;
        }
        if let Some(pair) = check_executable(path, name, outputs.clone(), compiler_outputs)? {
            found.push(pair);
        }
    }
    Ok(found)
}

/// Add the PDB named by each `.exe` or invocation-selected executable.
///
/// rustc's artifact notifications, which Cargo asks for and the store takes
/// as the unit's outputs, report the `.exe` but never the `.pdb` LINK writes
/// beside it. Without it a restore would leave an `.exe` with no `.pdb`, or
/// beside an older one. The `.pdb` is added when the `.exe` names it by bare
/// file name and it lies next to the `.exe`; [`check_outputs`] then decides
/// whether the pair may be stored.
pub(crate) fn add_named_pdbs(
    files: &mut Vec<(std::path::PathBuf, String)>,
    executable_name: Option<&str>,
) {
    let mut named = Vec::new();
    for (path, name) in files.iter() {
        if !is_windows_executable_name(name) && executable_name != Some(name.as_str()) {
            continue;
        }
        let Ok(ExecutableDebug::CodeView(record)) = read_codeview(path) else {
            continue;
        };
        if !is_bare_file_name(&record.pdb_path)
            || files
                .iter()
                .chain(named.iter())
                .any(|(_, name)| name.eq_ignore_ascii_case(&record.pdb_path))
        {
            continue;
        }
        let Some(pdb) = path.parent().map(|dir| dir.join(&record.pdb_path)) else {
            continue;
        };
        if pdb.is_file() {
            named.push((pdb, record.pdb_path));
        }
    }
    files.extend(named);
}

/// Select the primary output without opening unrelated artifacts. A linked
/// bin/test with an explicit `-o` keeps that filename even without `.exe`.
/// Otherwise use the same filename classification as the Rust adapter; an
/// unknown or ambiguous custom-target suffix cannot bypass the pairing gate.
pub(crate) fn executable_output_name<'a>(
    linked_executable: bool,
    output: Option<&Path>,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<Option<String>> {
    if !linked_executable {
        return Ok(None);
    }
    let mut names = names.into_iter();
    if let Some(output) = output {
        let name = output
            .file_name()
            .and_then(|name| name.to_str())
            .context("executable output has no Unicode filename")?;
        ensure!(
            names.any(|candidate| candidate == name),
            "executable output {name} is missing"
        );
        return Ok(Some(name.to_string()));
    }
    let mut candidates = names.filter(|name| {
        matches!(
            crate::compiler::classify_by_filename(name),
            crate::compiler::ArtifactKind::Executable
                | crate::compiler::ArtifactKind::Other("extensionless")
        )
    });
    let name = candidates
        .next()
        .context("cannot identify the linked executable output")?;
    ensure!(
        candidates.next().is_none(),
        "ambiguous linked executable outputs"
    );
    Ok(Some(name.to_string()))
}

/// Whether `name` is a Windows executable by its extension (`.exe`, any case).
pub(crate) fn is_windows_executable_name(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("exe"))
}

fn check_executable<'a>(
    exe: &Path,
    exe_name: &str,
    outputs: impl IntoIterator<Item = (&'a Path, &'a str)>,
    compiler_outputs: bool,
) -> Result<Option<Pairing>> {
    let debug = read_codeview(exe).with_context(|| format!("reading {exe_name}"))?;
    if debug == ExecutableDebug::OtherFormat {
        return Ok(None);
    }
    // rustc's `/OPT:REF` turns incremental linking off; `-C link-dead-code`
    // drops it, and LINK then keeps an `.ilk` that the next link patches the
    // `.exe` from. A restored `.exe` is not the one that `.ilk` describes.
    ensure!(
        !compiler_outputs || !exe.with_extension("ilk").exists(),
        "{exe_name} was linked incrementally (an .ilk lies beside it)"
    );
    let ExecutableDebug::CodeView(record) = debug else {
        return Ok(Some(Pairing::SelfContained));
    };
    ensure!(
        is_bare_file_name(&record.pdb_path),
        "{exe_name} names its .pdb by path ({}), not by file name: \
         a restore elsewhere would point the debugger at another build",
        record.pdb_path
    );
    let mut candidates = outputs
        .into_iter()
        .filter(|(_, name)| name.eq_ignore_ascii_case(&record.pdb_path));
    let Some((pdb, pdb_name)) = candidates.next() else {
        bail!(
            "{exe_name} names {}, which is not among the outputs stored with it",
            record.pdb_path
        );
    };
    ensure!(
        candidates.next().is_none(),
        "{exe_name} has ambiguous outputs for {}",
        record.pdb_path
    );
    if compiler_outputs && pdb.parent() != exe.parent() {
        let parent = |path: &Path| -> Result<_> {
            Ok(std::fs::canonicalize(
                path.parent().context("output has no parent")?,
            )?)
        };
        ensure!(
            parent(pdb)? == parent(exe)?,
            "{pdb_name} is not beside {exe_name}"
        );
    }
    let identity = read_pdb_identity(pdb).with_context(|| format!("reading {pdb_name}"))?;
    ensure!(
        identity.matches(&record),
        "{pdb_name} does not match {exe_name} (GUID or age differ): \
         the link left an older program database"
    );
    Ok(Some(Pairing::Paired {
        pdb_store_name: pdb_name.to_string(),
    }))
}

/// A file name with no directory, drive or stream part.
fn is_bare_file_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['\\', '/', ':']) && name != "." && name != ".."
}

impl PdbIdentity {
    /// Whether a debugger accepts this database for `record`: the GUID is
    /// the same, the information stream is at least as new as the image,
    /// and the DBI belongs to that image. Microsoft OpenValidate4 permits a
    /// zero DBI age for legacy databases; other mismatches are rejected.
    pub(crate) fn matches(&self, record: &CodeViewRecord) -> bool {
        self.guid == record.guid
            && self.info_age >= record.age
            && (self.dbi_age == 0 || self.dbi_age == record.age)
    }
}

/// Inspect an executable without treating an ELF named `.exe` as a PE.
/// A file starting with MZ must have valid PE/debug headers; malformed or
/// ambiguous CodeView records make the unit uncacheable.
pub(crate) fn read_codeview(path: &Path) -> Result<ExecutableDebug> {
    let mut file = Reader::open(path)?;
    if file.read_at(0, 2)?.as_slice() != b"MZ" {
        return Ok(ExecutableDebug::OtherFormat);
    }
    let dos = file.read_at(0, 64)?;
    let pe = u64::from(u32_at(&dos, 0x3c));
    let coff = file.read_at(pe, 24)?;
    ensure!(&coff[..4] == b"PE\0\0", "not a PE file (no PE signature)");
    let sections = usize::from(u16_at(&coff, 6));
    let optional_size = usize::from(u16_at(&coff, 20));
    let optional = file.read_at(pe + 24, optional_size)?;
    ensure!(optional.len() >= 2, "PE optional header is missing");
    let (count_at, dirs_at) = match u16_at(&optional, 0) {
        0x10b => (92, 96),
        0x20b => (108, 112),
        magic => bail!("unknown PE optional header magic {magic:#x}"),
    };
    ensure!(
        optional.len() >= count_at + 4,
        "PE optional header is truncated"
    );
    if u32_at(&optional, count_at) <= DEBUG_DATA_DIRECTORY {
        return Ok(ExecutableDebug::NoCodeView);
    }
    let entry = dirs_at + 8 * DEBUG_DATA_DIRECTORY as usize;
    ensure!(
        optional.len() >= entry + 8,
        "PE data directories are truncated"
    );
    let (debug_rva, debug_size) = (u32_at(&optional, entry), u32_at(&optional, entry + 4));
    if debug_rva == 0 && debug_size == 0 {
        return Ok(ExecutableDebug::NoCodeView);
    }
    ensure!(
        debug_rva != 0 && debug_size != 0,
        "incomplete PE debug directory location"
    );
    ensure!(
        debug_size.is_multiple_of(DEBUG_ENTRY_SIZE as u32)
            && debug_size / DEBUG_ENTRY_SIZE as u32 <= MAX_DEBUG_ENTRIES,
        "PE debug directory has an invalid or excessive entry count"
    );
    let table = file.read_at(
        pe + 24 + optional_size as u64,
        sections * SECTION_HEADER_SIZE,
    )?;
    let headers = u64::from(u32_at(&optional, 60));
    let debug_offset = if u64::from(debug_rva) < headers {
        ensure!(
            u64::from(debug_rva) + u64::from(debug_size) <= headers,
            "PE debug directory crosses the headers"
        );
        u64::from(debug_rva)
    } else {
        let mut locations = table
            .as_chunks::<SECTION_HEADER_SIZE>()
            .0
            .iter()
            .filter_map(|section| {
                let address = u32_at(section, 12);
                let delta = debug_rva.checked_sub(address)?;
                let raw_size = u32_at(section, 16);
                let span = u32_at(section, 8).max(raw_size);
                if delta >= span || u64::from(delta) + u64::from(debug_size) > u64::from(raw_size) {
                    return None;
                }
                Some(u64::from(u32_at(section, 20)) + u64::from(delta))
            });
        let offset = locations
            .next()
            .context("the PE debug directory has no file-backed section")?;
        ensure!(
            locations.next().is_none(),
            "PE debug directory has ambiguous sections"
        );
        offset
    };
    file.check_range(debug_offset, u64::from(debug_size))?;
    let mut found = None;
    for index in 0..debug_size / DEBUG_ENTRY_SIZE as u32 {
        let entry = file.read_at(
            debug_offset + u64::from(index) * DEBUG_ENTRY_SIZE as u64,
            DEBUG_ENTRY_SIZE,
        )?;
        if u32_at(&entry, 12) != DEBUG_TYPE_CODEVIEW {
            continue;
        }
        ensure!(found.is_none(), "PE has multiple CodeView records");
        let size = u32_at(&entry, 16);
        ensure!(
            (25..=MAX_CODEVIEW_BYTES).contains(&size),
            "CodeView record of {size} bytes"
        );
        let record = file.read_at(u64::from(u32_at(&entry, 24)), size as usize)?;
        ensure!(&record[..4] == b"RSDS", "CodeView record is not RSDS");
        let mut guid = [0; 16];
        guid.copy_from_slice(&record[4..20]);
        let name = &record[24..];
        let end = name
            .iter()
            .position(|&b| b == 0)
            .context("CodeView path is not NUL-terminated")?;
        let pdb_path = std::str::from_utf8(&name[..end])
            .context("CodeView .pdb path is not UTF-8")?
            .to_string();
        found = Some(CodeViewRecord {
            guid,
            age: u32_at(&record, 20),
            pdb_path,
        });
    }
    Ok(found.map_or(ExecutableDebug::NoCodeView, ExecutableDebug::CodeView))
}

/// The GUID and executable-validation ages of a modern MSF 7.0 database.
/// Both Info and DBI headers must exist within the declared file blocks.
pub(crate) fn read_pdb_identity(path: &Path) -> Result<PdbIdentity> {
    let mut file = Reader::open(path)?;
    let superblock = file.read_at(0, 56)?;
    ensure!(
        &superblock[..32] == MSF_MAGIC,
        "not an MSF 7.0 program database"
    );
    let block_size = u64::from(u32_at(&superblock, 32));
    ensure!(
        block_size.is_power_of_two() && (512..=65536).contains(&block_size),
        "MSF block size {block_size}"
    );
    let blocks = u64::from(u32_at(&superblock, 40));
    ensure!(blocks != 0, "MSF has no blocks");
    file.check_range(0, blocks * block_size)?;
    let directory_bytes = u64::from(u32_at(&superblock, 44));
    ensure!(
        (4..=MAX_DIRECTORY_BYTES).contains(&directory_bytes),
        "MSF stream directory of {directory_bytes} bytes"
    );
    let block_map = u64::from(u32_at(&superblock, 52));
    ensure!(
        block_map < blocks,
        "MSF block map is outside the declared blocks"
    );
    let directory_blocks = directory_bytes.div_ceil(block_size);
    ensure!(
        directory_blocks * 4 <= block_size,
        "MSF stream directory spans too many blocks"
    );
    let map = file.read_at(block_map * block_size, (directory_blocks * 4) as usize)?;
    let mut directory = Vec::with_capacity(directory_bytes as usize);
    for block in map.as_chunks::<4>().0 {
        let block = u64::from(u32::from_le_bytes(*block));
        ensure!(
            block < blocks,
            "MSF directory block is outside the declared blocks"
        );
        directory.extend(file.read_at(block * block_size, block_size as usize)?);
    }
    directory.truncate(directory_bytes as usize);
    let streams = u32_at(&directory, 0) as usize;
    ensure!(
        streams <= (directory.len() - 4) / 4,
        "MSF stream directory is truncated"
    );
    let first_block = |stream: usize| -> Result<(u64, u32)> {
        ensure!(stream < streams, "PDB stream {stream} is missing");
        let size_of = |index: usize| match u32_at(&directory, 4 + 4 * index) {
            u32::MAX => 0,
            size => size,
        };
        let blocks_before: u64 = (0..stream)
            .map(|i| u64::from(size_of(i)).div_ceil(block_size))
            .sum();
        let size = size_of(stream);
        ensure!(size != 0, "PDB stream {stream} is empty");
        let at = 4 + 4 * streams + 4 * blocks_before as usize;
        ensure!(
            directory.len() >= at + 4,
            "MSF stream directory is truncated"
        );
        let block = u64::from(u32_at(&directory, at));
        ensure!(
            block < blocks,
            "PDB stream {stream} points outside the declared blocks"
        );
        Ok((block * block_size, size))
    };
    let (info_at, info_size) = first_block(PDB_INFO_STREAM)?;
    ensure!(info_size >= 28, "the PDB information stream is truncated");
    let info = file.read_at(info_at, 28)?;
    let mut guid = [0; 16];
    guid.copy_from_slice(&info[12..28]);
    let (dbi_at, dbi_size) = first_block(DBI_STREAM)?;
    ensure!(dbi_size >= 64, "the PDB DBI header is truncated");
    let dbi = file.read_at(dbi_at, 64)?;
    ensure!(
        u32_at(&dbi, 0) == u32::MAX && matches!(u32_at(&dbi, 4), 19990903 | 20091201),
        "unsupported PDB DBI header"
    );
    Ok(PdbIdentity {
        guid,
        info_age: u32_at(&info, 8),
        dbi_age: u32_at(&dbi, 8),
    })
}

/// A bounded file reader shared by the PE and MSF headers. Bounds are checked
/// before allocation, including when an offset points into an absent block.
struct Reader {
    file: File,
    len: u64,
}

impl Reader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Self { file, len })
    }

    fn check_range(&self, offset: u64, len: u64) -> Result<()> {
        ensure!(
            offset <= self.len && len <= self.len - offset,
            "reading {len} bytes at offset {offset} exceeds file size {}",
            self.len
        );
        Ok(())
    }

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_range(offset, len as u64)?;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0; len];
        self.file
            .read_exact(&mut buffer)
            .with_context(|| format!("reading {len} bytes at offset {offset}"))?;
        Ok(buffer)
    }
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

#[cfg(test)]
pub(crate) use tests::fixture_pair;
#[cfg(test)]
mod tests;
