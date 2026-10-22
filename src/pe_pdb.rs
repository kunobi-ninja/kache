//! Whether a Windows executable and the `.pdb` beside it belong together.
//!
//! An MSVC-linked `.exe` names its program database in a CodeView `RSDS`
//! record of its debug directory: a GUID, an age and a path. A debugger loads
//! a `.pdb` only when its GUID and age match that record, and looks for it by
//! the recorded path, then by the same file name next to the executable.
//! rustc links with `/PDBALTPATH:%_PDB%`, so the record holds the file name
//! alone (`foo-0123abcd.pdb`): no target directory, user name or worktree.
//!
//! An executable is safe to cache when it was not linked incrementally (no
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
//! stream directory, PDB information stream and DBI stream header.

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
    pub(crate) dbi_age: Option<u32>,
}

/// What a store learns about one executable output.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pairing {
    /// A PE without a CodeView record: no external debug information.
    SelfContained,
    /// The named `.pdb` is stored with it and matches.
    Paired { pdb_store_name: String },
}

/// Check every `.exe` among a unit's outputs against the `.pdb` it names.
///
/// `outputs` holds each output's path and store name. `Ok` lists what was
/// found for each executable; `Err` explains why the unit must not be cached.
pub(crate) fn check_outputs<'a>(
    outputs: impl IntoIterator<Item = (&'a Path, &'a str)> + Clone,
) -> Result<Vec<Pairing>> {
    let mut found = Vec::new();
    for (path, name) in outputs.clone() {
        if !is_windows_executable_name(name) {
            continue;
        }
        found.push(check_executable(path, name, outputs.clone())?);
    }
    Ok(found)
}

/// Add to rustc's reported outputs the `.pdb` each `.exe` among them names.
///
/// rustc's artifact notifications, which Cargo asks for and the store takes
/// as the unit's outputs, report the `.exe` but never the `.pdb` LINK writes
/// beside it. Without it a restore would leave an `.exe` with no `.pdb`, or
/// beside an older one. The `.pdb` is added when the `.exe` names it by bare
/// file name and it lies next to the `.exe`; [`check_outputs`] then decides
/// whether the pair may be stored.
pub(crate) fn add_named_pdbs(files: &mut Vec<(std::path::PathBuf, String)>) {
    let mut named = Vec::new();
    for (path, name) in files.iter() {
        if !is_windows_executable_name(name) {
            continue;
        }
        let Ok(Some(record)) = read_codeview(path) else {
            continue;
        };
        if !is_bare_file_name(&record.pdb_path)
            || files
                .iter()
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
) -> Result<Pairing> {
    // rustc's `/OPT:REF` turns incremental linking off; `-C link-dead-code`
    // drops it, and LINK then keeps an `.ilk` that the next link patches the
    // `.exe` from. A restored `.exe` is not the one that `.ilk` describes.
    ensure!(
        !exe.with_extension("ilk").exists(),
        "{exe_name} was linked incrementally (an .ilk lies beside it)"
    );
    let Some(record) = read_codeview(exe).with_context(|| format!("reading {exe_name}"))? else {
        return Ok(Pairing::SelfContained);
    };
    ensure!(
        is_bare_file_name(&record.pdb_path),
        "{exe_name} names its .pdb by path ({}), not by file name: \
         a restore elsewhere would point the debugger at another build",
        record.pdb_path
    );
    let Some((pdb, pdb_name)) = outputs
        .into_iter()
        .find(|(_, name)| name.eq_ignore_ascii_case(&record.pdb_path))
    else {
        bail!(
            "{exe_name} names {}, which is not among the outputs stored with it",
            record.pdb_path
        );
    };
    let identity = read_pdb_identity(pdb).with_context(|| format!("reading {pdb_name}"))?;
    ensure!(
        identity.matches(&record),
        "{pdb_name} does not match {exe_name} (GUID or age differ): \
         the link left an older program database"
    );
    Ok(Pairing::Paired {
        pdb_store_name: pdb_name.to_string(),
    })
}

/// A file name with no directory, drive or stream part.
fn is_bare_file_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['\\', '/', ':']) && name != "." && name != ".."
}

impl PdbIdentity {
    /// Whether a debugger accepts this database for `record`: the GUID is
    /// the same and the age is the one the information stream or the DBI
    /// stream records.
    pub(crate) fn matches(&self, record: &CodeViewRecord) -> bool {
        self.guid == record.guid
            && (self.info_age == record.age || self.dbi_age == Some(record.age))
    }
}

/// The CodeView `RSDS` record of a PE file, `None` when it has none.
///
/// Fails on a file that is not a well-formed PE, and on a CodeView record in
/// an older format (`NB10`) or with an unreadable path.
pub(crate) fn read_codeview(path: &Path) -> Result<Option<CodeViewRecord>> {
    let mut file = File::open(path)?;
    let dos = read_at(&mut file, 0, 64)?;
    ensure!(&dos[..2] == b"MZ", "not a PE file (no MZ header)");
    let pe = u64::from(u32_at(&dos, 0x3c));
    let coff = read_at(&mut file, pe, 24)?;
    ensure!(&coff[..4] == b"PE\0\0", "not a PE file (no PE signature)");
    let sections = u64::from(u16_at(&coff, 6));
    let optional_size = u64::from(u16_at(&coff, 20));
    let optional = read_at(&mut file, pe + 24, optional_size as usize)?;
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
        return Ok(None);
    }
    let entry = dirs_at + 8 * DEBUG_DATA_DIRECTORY as usize;
    ensure!(
        optional.len() >= entry + 8,
        "PE data directories are truncated"
    );
    let (debug_rva, debug_size) = (u32_at(&optional, entry), u32_at(&optional, entry + 4));
    if debug_rva == 0 || debug_size == 0 {
        return Ok(None);
    }
    let table = read_at(
        &mut file,
        pe + 24 + optional_size,
        sections as usize * SECTION_HEADER_SIZE,
    )?;
    let debug_offset = table
        .as_chunks::<SECTION_HEADER_SIZE>()
        .0
        .iter()
        .find_map(|section| {
            let virtual_size = u32_at(section, 8);
            let address = u32_at(section, 12);
            let raw_size = u32_at(section, 16);
            let raw_at = u32_at(section, 20);
            let span = virtual_size.max(raw_size);
            (debug_rva >= address && debug_rva - address < span)
                .then(|| u64::from(raw_at) + u64::from(debug_rva - address))
        })
        .context("the PE debug directory lies in no section")?;
    let entries = read_at(&mut file, debug_offset, debug_size as usize)?;
    for entry in entries.as_chunks::<DEBUG_ENTRY_SIZE>().0 {
        if u32_at(entry, 12) != DEBUG_TYPE_CODEVIEW {
            continue;
        }
        let size = u32_at(entry, 16);
        ensure!(
            (24..=MAX_CODEVIEW_BYTES).contains(&size),
            "CodeView record of {size} bytes"
        );
        let record = read_at(&mut file, u64::from(u32_at(entry, 24)), size as usize)?;
        ensure!(&record[..4] == b"RSDS", "CodeView record is not RSDS");
        let mut guid = [0; 16];
        guid.copy_from_slice(&record[4..20]);
        let name = &record[24..];
        let name = &name[..name.iter().position(|&b| b == 0).unwrap_or(name.len())];
        let pdb_path = std::str::from_utf8(name)
            .context("CodeView .pdb path is not UTF-8")?
            .to_string();
        return Ok(Some(CodeViewRecord {
            guid,
            age: u32_at(&record, 20),
            pdb_path,
        }));
    }
    Ok(None)
}

/// The GUID and ages of an MSF 7.0 program database.
pub(crate) fn read_pdb_identity(path: &Path) -> Result<PdbIdentity> {
    let mut file = File::open(path)?;
    let superblock = read_at(&mut file, 0, 56)?;
    ensure!(
        &superblock[..32] == MSF_MAGIC,
        "not an MSF 7.0 program database"
    );
    let block_size = u64::from(u32_at(&superblock, 32));
    ensure!(
        block_size.is_power_of_two() && (512..=65536).contains(&block_size),
        "MSF block size {block_size}"
    );
    let directory_bytes = u64::from(u32_at(&superblock, 44));
    ensure!(
        (4..=MAX_DIRECTORY_BYTES).contains(&directory_bytes),
        "MSF stream directory of {directory_bytes} bytes"
    );
    let block_map = u64::from(u32_at(&superblock, 52));
    let directory_blocks = directory_bytes.div_ceil(block_size);
    ensure!(
        directory_blocks * 4 <= block_size,
        "MSF stream directory spans too many blocks"
    );
    let map = read_at(
        &mut file,
        block_map * block_size,
        (directory_blocks * 4) as usize,
    )?;
    let mut directory = Vec::with_capacity(directory_bytes as usize);
    for block in map.as_chunks::<4>().0 {
        let block = u64::from(u32::from_le_bytes(*block));
        directory.extend(read_at(&mut file, block * block_size, block_size as usize)?);
    }
    directory.truncate(directory_bytes as usize);

    let first_block = |stream: usize, directory: &[u8]| -> Result<Option<(u64, u32)>> {
        let streams = u32_at(directory, 0) as usize;
        if stream >= streams {
            return Ok(None);
        }
        ensure!(
            directory.len() >= 4 + 4 * streams,
            "MSF stream directory is truncated"
        );
        let size_of = |index: usize| match u32_at(directory, 4 + 4 * index) {
            u32::MAX => 0,
            size => size,
        };
        let blocks_before: u64 = (0..stream)
            .map(|index| u64::from(size_of(index)).div_ceil(block_size))
            .sum();
        let size = size_of(stream);
        if size == 0 {
            return Ok(None);
        }
        let at = 4 + 4 * streams + 4 * blocks_before as usize;
        ensure!(
            directory.len() >= at + 4,
            "MSF stream directory is truncated"
        );
        Ok(Some((u64::from(u32_at(directory, at)) * block_size, size)))
    };

    let (info_at, info_size) = first_block(PDB_INFO_STREAM, &directory)?
        .context("the PDB information stream is missing")?;
    ensure!(info_size >= 28, "the PDB information stream is truncated");
    let info = read_at(&mut file, info_at, 28)?;
    let mut guid = [0; 16];
    guid.copy_from_slice(&info[12..28]);
    let dbi_age = match first_block(DBI_STREAM, &directory)? {
        Some((at, size)) if size >= 12 => Some(u32_at(&read_at(&mut file, at, 12)?, 8)),
        _ => None,
    };
    Ok(PdbIdentity {
        guid,
        info_age: u32_at(&info, 8),
        dbi_age,
    })
}

/// Exactly `len` bytes at `offset`.
fn read_at(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = vec![0; len];
    file.read_exact(&mut buffer)
        .with_context(|| format!("reading {len} bytes at offset {offset}"))?;
    Ok(buffer)
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

#[cfg(test)]
mod tests;
