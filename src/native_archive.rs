//! Conservative structural hashing of linked `static=` native archives.
//!
//! rustc bundles a `-l static=NAME` archive INTO its output, so kache folds the
//! archive's content into the cache key to catch an in-place rebuild — same
//! name, same path, different bytes (kunobi-ninja/kache#421). Hashing the whole
//! `.a` file catches that byte change, but is NOT portable across build directories: the
//! `cc` crate names each archive member with a hash derived from the absolute
//! build path (e.g. `cafca65b3467684e-quickjs.o` in one checkout vs
//! `4af22b2a007cb61a-quickjs.o` in another), while the object member CONTENT is
//! byte-identical. The whole-file hash therefore diverges across clones / CI
//! machines even when the linked content is identical (kunobi-ninja/kache#471),
//! cross-clone-missing the lib and everything downstream of it.
//!
//! [`portable_static_archive_hash`] hashes the archive's link-relevant content
//! for the two `ar` flavors rustc links on kache's Unix targets: GNU / SysV
//! (Linux) and BSD / Darwin (macOS, #691). It deliberately retains every
//! effective member name. Linkers can observe `archive(member)` through order
//! files, map files, diagnostics, and target-specific relocation handling, and
//! rustc preserves those names when bundling a native archive into an rlib.
//! Ignoring path-derived `cc` member prefixes would therefore permit a false
//! hit. Anything not proven safe returns `None`; the caller uses a path-bound
//! fallback digest (or makes thin archives uncacheable).
//!
//! ## What is hashed (GNU archives)
//! - every object's exact header name token and DATA bytes, length-framed,
//!   **in archive order**
//!   (member order can be link-significant — duplicate symbols, `--whole-archive`
//!   — so it is never sorted);
//! - the symbol-table member (`/`, `/SYM64/`) DATA **as-is**. For GNU, member
//!   names live in the `//` long-name table (fixed-width `cc` name hashes keep
//!   that table the same SIZE across clones), so member-header offsets — and
//!   thus the symbol table's bytes — are already identical across clones. Hashing
//!   it raw is therefore portable AND keeps a stale/crafted symbol table from
//!   colliding with one that links differently (it is NOT dropped).
//!
//! The `//` long-name table is hashed byte-for-byte, and each `/N` reference is
//! validated against it. The parser also enforces the canonical GNU layout —
//! at most one symbol table as the first member, at most one `//` table
//! immediately after — and falls back on anything else (e.g. COFF `.lib`'s two
//! `/` linker members).
//! Object payloads must be structurally valid ELF relocatable objects without
//! recognized embedded LTO/offload section names or types, or pass the same
//! fail-closed Mach-O gate as BSD members. Raw/wrapped LLVM bitcode and unknown
//! payloads use path fallback.
//!
//! ## What is hashed (BSD / Darwin archives, #691)
//! macOS `ar` stores long member names INLINE: a `#1/N` header name puts the
//! (NUL-padded) name in the first N bytes of the member's data area, and the
//! header size INCLUDES those bytes. The path-derived `cc` name therefore
//! lives inside the member data itself. The exact stored name bytes (including
//! encoding/padding), parsed timestamp, and object DATA are all hashed.
//! - only structurally valid, known Mach-O `MH_OBJECT` files
//!   without debug sections, STABS, or embedded compiler bitcode/LTO. Darwin's
//!   linker
//!   records `archive(member)` (and archive-member time) in `N_OSO` debug-map
//!   entries. Unsupported, malformed, debug-bearing, bitcode, and ARM64_32
//!   objects therefore use the path-bound fallback;
//! - the ranlib member (`__.SYMDEF[ SORTED]` / `__.SYMDEF_64[ SORTED]`) DATA
//!   **as-is**, tagged with its trimmed name: `_64` reads the same bytes with
//!   a different word width and ` SORTED` changes the lookup contract, so the
//!   variants must never collide (the `/` vs `/SYM64/` reasoning). Measured on
//!   rquickjs-sys across two checkouts, the ranlib payload is byte-identical
//!   (fixed-width `cc` names keep every stored offset equal), so hashing it
//!   raw is portable AND keeps a stale/crafted ranlib from colliding with one
//!   that links differently — the GNU symtab treatment, not `//`'s.
//!
//! ## What is ignored
//! UID, GID, and mode header spellings. GNU/Apple linkers do not consume them,
//! and rustc normalizes them when rebuilding an rlib. Member names, timestamps,
//! sizes, contents, order, and symbol tables remain identity-bearing.
//!
//! ## Out of scope -> path-bound fallback (`None`)
//! Thin archives (`!<thin>`), Windows COFF `.lib`, GNU/BSD mixed layouts,
//! debug-bearing Mach-O, compiler bitcode/LTO, ARM64_32, unknown object formats,
//! and anything malformed. Thin archives are made uncacheable because their
//! external member bytes are absent from the container. Other fallbacks bind
//! both the archive bytes and lexical absolute archive path.

use crate::checked_regions::checked_file_region;

const AR_MAGIC: &[u8; 8] = b"!<arch>\n";
const AR_HEADER_LEN: usize = 60;
/// Domain tags so these schemes can never collide with the path-bound fallback,
/// with each other, or with any other key input. Bump
/// the trailing version if a hashed-content definition ever changes (also bump
/// `CACHE_KEY_VERSION`).
const GNU_DOMAIN: &[u8] = b"kache.native-ar.gnu.member-identity.v2\0";
const BSD_DOMAIN: &[u8] = b"kache.native-ar.bsd.member-identity.v2\0";

/// Structural identity hash of a GNU or BSD `ar` static archive, or `None` to
/// signal the caller should use its path-bound fallback. See the module docs.
///
/// GNU is tried first because an archive with only plain short names and no
/// reserved members can parse under both arms. A BSD-parsed archive deliberately
/// NEVER hashes equal to a
/// GNU-parsed one (distinct domain + textual prefix), even for identical
/// member contents: rustc bundles the archive BYTES into its output, so the
/// format difference IS an output difference — and the two arms frame
/// different symbol-table semantics.
pub fn portable_static_archive_hash(bytes: &[u8]) -> Option<String> {
    gnu_archive_hash(bytes).or_else(|| bsd_archive_hash(bytes))
}

/// The GNU / SysV arm. See "What is hashed (GNU archives)" in the module docs.
fn gnu_archive_hash(bytes: &[u8]) -> Option<String> {
    // `!<thin>\n` and non-archives fail this check -> fallback.
    if bytes.len() < AR_MAGIC.len() || &bytes[..AR_MAGIC.len()] != AR_MAGIC {
        return None;
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(GNU_DOMAIN);
    let mut pos = AR_MAGIC.len();
    let mut member_index: usize = 0;
    let mut seen_symtab = false;
    let mut longnames: Option<&[u8]> = None;
    let mut object_members: u64 = 0;

    while pos < bytes.len() {
        let header = bytes.get(pos..pos.checked_add(AR_HEADER_LEN)?)?;
        // Header terminator must be "`\n" — a strict gate against misalignment.
        if &header[58..60] != b"`\n" {
            return None;
        }
        let name = ar_name(&header[0..16])?;
        let mtime = parse_ar_decimal(&header[16..28])?;
        let size = parse_ar_decimal(&header[48..58])?;

        let data_start = pos + AR_HEADER_LEN;
        let data_end = data_start.checked_add(size)?;
        let data = bytes.get(data_start..data_end)?;

        // BSD/macOS/Darwin64 markers -> not a GNU archive -> hand off to the
        // BSD arm (never guess within this one). `__.SYMDEF_64 SORTED`
        // (19 chars) cannot fit the 16-byte name field — such Darwin64
        // archives use the `#1/` extended-name encoding, also handed off here
        // — so it is not listed.
        if name.starts_with("#1/")
            || name == "__.SYMDEF"
            || name == "__.SYMDEF SORTED"
            || name == "__.SYMDEF_64"
        {
            return None;
        }

        hasher.update(b"mtime\0");
        hasher.update(&(mtime as u64).to_le_bytes());

        match classify(name) {
            Member::SymbolTable => {
                // A GNU symbol table is the FIRST member, and there is exactly
                // one. A symtab anywhere else — notably COFF `.lib`'s SECOND `/`
                // linker member — means this is not a plain GNU archive; fall
                // back rather than impose GNU offset semantics on it.
                if seen_symtab || member_index != 0 {
                    return None;
                }
                seen_symtab = true;
                // `/` (32-bit) and `/SYM64/` (64-bit) armaps use DIFFERENT count/
                // offset word widths, so identical bytes mean different things to
                // the linker — tag them distinctly so they can never collide
                // (codex review #471).
                let tag: &[u8] = if name == "/SYM64/" {
                    b"symtab64\0"
                } else {
                    b"symtab32\0"
                };
                hasher.update(tag);
                hasher.update(&(data.len() as u64).to_le_bytes());
                hasher.update(data);
            }
            Member::LongNameTable => {
                // The complete table plus each object's raw `/N` token commits
                // the effective member-name mapping. It appears once, right
                // after the optional symbol table.
                let allowed = usize::from(seen_symtab);
                // `member_index == allowed` also proves this is the first
                // long-name table: after one is consumed, every later member
                // index is greater than the only allowed slot.
                if member_index != allowed {
                    return None;
                }
                longnames = Some(data);
                hasher.update(b"longnames\0");
                hasher.update(&(data.len() as u64).to_le_bytes());
                hasher.update(data);
            }
            Member::Object => {
                if let Some(offset) = name.strip_prefix('/') {
                    let table = longnames?;
                    let offset = parse_ar_decimal(offset.as_bytes())?;
                    if offset != 0 && table.get(offset - 1) != Some(&b'\n') {
                        return None;
                    }
                    let entry = table.get(offset..)?;
                    let terminator = entry.windows(2).position(|bytes| bytes == b"/\n")?;
                    if terminator == 0 {
                        return None;
                    }
                } else if !name.ends_with('/') {
                    return None;
                }
                // GNU archives can carry ELF, Mach-O, raw bitcode, or arbitrary
                // payloads. A path-independent digest is safe only after the
                // member has passed a bounded object-format gate: raw/wrapped
                // bitcode uses archive-path-derived LTO identifiers, and an
                // unknown format may have equally path-sensitive semantics.
                let known_object = if has_macho_magic(data) {
                    is_known_no_debug_macho_object(data)
                } else {
                    is_known_elf_relocatable_object(data)
                };
                if !known_object {
                    return None;
                }
                hasher.update(b"member\0");
                hasher.update(&(name.len() as u64).to_le_bytes());
                hasher.update(name.as_bytes());
                hasher.update(&(data.len() as u64).to_le_bytes());
                hasher.update(data);
                object_members += 1;
            }
        }

        // ar members are padded to an even offset with a single '\n'.
        pos = data_end.checked_add(size & 1)?;
        member_index += 1;
    }

    // Exact consumption: trailing bytes mean we misparsed -> fallback.
    if pos != bytes.len() {
        return None;
    }
    // An archive with no object members is degenerate; be conservative.
    if object_members == 0 {
        return None;
    }
    // Tagged so a portable digest can never even textually collide with the
    // plain-hex whole-file fallback (no reliance on blake3 collision resistance).
    Some(format!("gnu-ar-v2:{}", hasher.finalize().to_hex()))
}

/// The BSD / Darwin arm (#691). See "What is hashed (BSD / Darwin archives)"
/// in the module docs. Strictness mirrors [`gnu_archive_hash`]: any GNU
/// reserved name shape means a mixed/foreign layout -> `None`, never a guess;
/// the ranlib member may only be the first member and appear at most once
/// (where Darwin `ranlib` always writes it).
fn bsd_archive_hash(bytes: &[u8]) -> Option<String> {
    if !bytes.starts_with(AR_MAGIC) {
        return None;
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(BSD_DOMAIN);
    let mut pos = AR_MAGIC.len();
    let mut member_index: usize = 0;
    let mut seen_symdef = false;
    let mut object_members: u64 = 0;

    while pos < bytes.len() {
        let header = bytes.get(pos..pos.checked_add(AR_HEADER_LEN)?)?;
        // Header terminator must be "`\n" — a strict gate against misalignment.
        if &header[58..60] != b"`\n" {
            return None;
        }
        let field = ar_name(&header[0..16])?;
        let mtime = parse_ar_decimal(&header[16..28])?;
        let size = parse_ar_decimal(&header[48..58])?;

        let data_start = pos + AR_HEADER_LEN;
        let data_end = data_start.checked_add(size)?;
        let data = bytes.get(data_start..data_end)?;

        // GNU reserved shapes (`/` symtab, `/SYM64/`, `//` long names, `/N`
        // name refs, `name/` short names) -> not a BSD archive -> fallback.
        if field.starts_with('/') || field.ends_with('/') {
            return None;
        }

        // `#1/N` extended name: the first N bytes of the data area are the
        // (NUL-padded) member name, and the header size INCLUDES them. Trim
        // the padding for classification only; `N` itself is folded below.
        let (name, stored_name, inline_len, name_encoding): (&str, &[u8], usize, &[u8]) =
            match field.strip_prefix("#1/") {
                Some(digits) => {
                    let n = parse_ar_decimal(digits.as_bytes())?;
                    let raw = data.get(..n)?;
                    let end = raw.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
                    let name = std::str::from_utf8(&raw[..end]).ok()?;
                    (name, raw, n, b"inline\0")
                }
                None => (field, &header[0..16], 0, b"short\0"),
            };
        let content = &data[inline_len..];

        hasher.update(b"name\0");
        hasher.update(name_encoding);
        hasher.update(&(stored_name.len() as u64).to_le_bytes());
        hasher.update(stored_name);
        hasher.update(&(mtime as u64).to_le_bytes());

        if is_bsd_symdef(name) {
            // Darwin `ranlib` writes the symbol table as the FIRST member,
            // exactly once — anything else is not a plain Darwin archive;
            // fall back (mirror the GNU symtab strictness).
            if seen_symdef || member_index != 0 {
                return None;
            }
            seen_symdef = true;
            // Tag with the trimmed variant name: `_64` reads identical bytes
            // with a different word width and ` SORTED` changes the lookup
            // contract, so no two variants may collide (the GNU symtab32/
            // symtab64 reasoning). The payload is hashed AS-IS, like the GNU
            // symtab: its stored member offsets converge across clones (the
            // fixed-width `cc` names keep every inline-name length equal —
            // measured byte-identical on rquickjs-sys across checkouts), and
            // hashing it raw keeps a stale/crafted ranlib from colliding with
            // one that links differently (it is NOT dropped, and NOT reduced
            // to its length).
            hasher.update(b"symdef\0");
            hasher.update(&(name.len() as u64).to_le_bytes());
            hasher.update(name.as_bytes());
            hasher.update(&(inline_len as u64).to_le_bytes());
            hasher.update(&(content.len() as u64).to_le_bytes());
            hasher.update(content);
        } else {
            // ld64 writes an N_OSO debug-map entry containing
            // `archive(member)` (and the member timestamp) for debug-bearing
            // Mach-O objects. Dropping the name/header is therefore safe only
            // after a bounded, fail-closed Mach-O inspection proves this is a
            // known MH_OBJECT without debug sections, STABS, or bitcode.
            if !is_known_no_debug_macho_object(content) {
                return None;
            }
            // The exact stored name was committed above; frame the object
            // content separately so no concatenation ambiguity is possible.
            hasher.update(b"member\0");
            hasher.update(&(inline_len as u64).to_le_bytes());
            hasher.update(&(content.len() as u64).to_le_bytes());
            hasher.update(content);
            object_members += 1;
        }

        // ar members are padded to an even offset with a single '\n'.
        pos = data_end.checked_add(size & 1)?;
        member_index += 1;
    }

    // Exact consumption: trailing bytes mean we misparsed -> fallback.
    if pos != bytes.len() {
        return None;
    }
    // An archive with no object members is degenerate; be conservative.
    if object_members == 0 {
        return None;
    }
    // Tagged so a portable digest can never even textually collide with the
    // whole-file fallback or the GNU scheme.
    Some(format!("bsd-ar-v2:{}", hasher.finalize().to_hex()))
}

// Mach-O constants used by the deliberately small, fail-closed object gate.
// This is not a general Mach-O reader: it recognizes only load commands that
// occur in ordinary clang-produced relocatable objects and rejects everything
// else so the caller keeps the whole archive bytes in the cache key.
const MH_OBJECT: u32 = 0x1;
const LC_SEGMENT: u32 = 0x1;
const LC_SYMTAB: u32 = 0x2;
const LC_DYSYMTAB: u32 = 0xb;
const LC_SEGMENT_64: u32 = 0x19;
const LC_UUID: u32 = 0x1b;
const LC_VERSION_MIN_MACOSX: u32 = 0x24;
const LC_VERSION_MIN_IPHONEOS: u32 = 0x25;
const LC_DATA_IN_CODE: u32 = 0x29;
const LC_SOURCE_VERSION: u32 = 0x2a;
const LC_LINKER_OPTION: u32 = 0x2d;
const LC_LINKER_OPTIMIZATION_HINT: u32 = 0x2e;
const LC_VERSION_MIN_TVOS: u32 = 0x2f;
const LC_VERSION_MIN_WATCHOS: u32 = 0x30;
const LC_BUILD_VERSION: u32 = 0x32;

const N_STAB: u8 = 0xe0;
const N_TYPE: u8 = 0x0e;
const N_SECT: u8 = 0x0e;
const S_ZEROFILL: u32 = 0x1;
const S_GB_ZEROFILL: u32 = 0xc;
const S_THREAD_LOCAL_ZEROFILL: u32 = 0x12;
const SECTION_TYPE: u32 = 0xff;
const S_ATTR_DEBUG: u32 = 0x0200_0000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum MachEndian {
    Little,
    Big,
}

impl MachEndian {
    fn u16(self, bytes: &[u8], offset: usize) -> Option<u16> {
        let raw: [u8; 2] = bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?;
        Some(match self {
            Self::Little => u16::from_le_bytes(raw),
            Self::Big => u16::from_be_bytes(raw),
        })
    }

    fn u32(self, bytes: &[u8], offset: usize) -> Option<u32> {
        let raw: [u8; 4] = bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?;
        Some(match self {
            Self::Little => u32::from_le_bytes(raw),
            Self::Big => u32::from_be_bytes(raw),
        })
    }

    fn u64(self, bytes: &[u8], offset: usize) -> Option<u64> {
        let raw: [u8; 8] = bytes.get(offset..offset.checked_add(8)?)?.try_into().ok()?;
        Some(match self {
            Self::Little => u64::from_le_bytes(raw),
            Self::Big => u64::from_be_bytes(raw),
        })
    }
}

#[derive(Clone, Copy)]
struct MachSymtab {
    symoff: u32,
    nsyms: u32,
    stroff: u32,
    strsize: u32,
}

/// Prove that `bytes` is a supported Mach-O relocatable object whose behavior
/// is independent of the containing archive path after name/time are hashed.
/// All arithmetic and table walks are bounded by the input slice. Any doubt
/// returns `false`, selecting the caller's path-bound fallback.
fn is_known_no_debug_macho_object(bytes: &[u8]) -> bool {
    parse_known_no_debug_macho_object(bytes).is_some()
}

fn has_macho_magic(bytes: &[u8]) -> bool {
    let Some(magic) = bytes.get(..4) else {
        return false;
    };
    magic == b"\xce\xfa\xed\xfe"
        || magic == b"\xfe\xed\xfa\xce"
        || magic == b"\xcf\xfa\xed\xfe"
        || magic == b"\xfe\xed\xfa\xcf"
}

/// Prove that `bytes` is an ordinary ELF relocatable object, not raw/wrapped
/// bitcode or an ELF LTO carrier. The parser deliberately rejects extended
/// section counts and unknown machines rather than guessing: `None` at the
/// archive level is a safe path-bound fallback.
fn is_known_elf_relocatable_object(bytes: &[u8]) -> bool {
    parse_known_elf_relocatable_object(bytes).is_some()
}

const SHT_LLVM_OFFLOADING: u32 = 0x6fff_4c0b;
const SHT_LLVM_LTO: u32 = 0x6fff_4c0c;

fn elf_machine_shape_is_supported(machine: u16, class: u8, endian: MachEndian) -> bool {
    match machine {
        2 => class == 1 && endian == MachEndian::Big, // EM_SPARC
        3 => class == 1 && endian == MachEndian::Little, // EM_386
        8 => matches!(class, 1 | 2),                  // EM_MIPS
        20 | 40 | 42 => class == 1,                   // EM_PPC / EM_ARM / EM_SH
        21 | 50 | 183 | 247 => class == 2,            // 64-bit PPC/IA-64/AArch64/BPF
        22 => class == 2 && endian == MachEndian::Big, // EM_S390 (s390x)
        43 => class == 2 && endian == MachEndian::Big, // EM_SPARCV9
        62 | 258 => class == 2 && endian == MachEndian::Little, // x86-64/LoongArch
        243 => matches!(class, 1 | 2) && endian == MachEndian::Little, // EM_RISCV
        _ => false,
    }
}

fn elf_section_layout_is_supported(
    header_len: usize,
    section_offset: usize,
    section_count: usize,
    names_index: usize,
) -> bool {
    // `names_index != 0 && names_index < section_count` also implies a
    // non-zero section count and rejects SHN_XINDEX (u16::MAX): no u16 section
    // count can be greater than that sentinel.
    names_index != 0 && names_index < section_count && section_offset >= header_len
}

fn parse_known_elf_relocatable_object(bytes: &[u8]) -> Option<()> {
    if bytes.get(..4)? != b"\x7fELF" || bytes.get(6).copied()? != 1 {
        return None;
    }
    let endian = match bytes.get(5).copied()? {
        1 => MachEndian::Little,
        2 => MachEndian::Big,
        _ => return None,
    };
    let class = bytes.get(4).copied()?;
    if endian.u16(bytes, 16)? != 1 || endian.u32(bytes, 20)? != 1 {
        return None; // ET_REL and EV_CURRENT only
    }
    let machine = endian.u16(bytes, 18)?;
    if !elf_machine_shape_is_supported(machine, class, endian) {
        return None;
    }

    let (header_len, section_len, section_offset, section_count, names_index) = match class {
        1 => {
            if endian.u16(bytes, 40)? != 52 || endian.u16(bytes, 46)? != 40 {
                return None;
            }
            (
                52_usize,
                40_usize,
                usize::try_from(endian.u32(bytes, 32)?).ok()?,
                usize::from(endian.u16(bytes, 48)?),
                usize::from(endian.u16(bytes, 50)?),
            )
        }
        2 => {
            if endian.u16(bytes, 52)? != 64 || endian.u16(bytes, 58)? != 64 {
                return None;
            }
            (
                64_usize,
                64_usize,
                usize::try_from(endian.u64(bytes, 40)?).ok()?,
                usize::from(endian.u16(bytes, 60)?),
                usize::from(endian.u16(bytes, 62)?),
            )
        }
        _ => return None,
    };
    // Section count 0 and SHN_XINDEX use extended fields in section zero. They
    // are valid ELF but rare for compiler objects; reject rather than partially
    // interpret them in this safety gate.
    if !elf_section_layout_is_supported(header_len, section_offset, section_count, names_index) {
        return None;
    }
    let table_bytes = section_len.checked_mul(section_count)?;
    bytes.get(section_offset..section_offset.checked_add(table_bytes)?)?;

    let section = |index: usize| -> Option<&[u8]> {
        let start = section_offset.checked_add(section_len.checked_mul(index)?)?;
        bytes.get(start..start.checked_add(section_len)?)
    };
    let names_header = section(names_index)?;
    if endian.u32(names_header, 4)? != 3 {
        return None; // SHT_STRTAB
    }
    let (names_offset, names_len) = if class == 1 {
        (
            usize::try_from(endian.u32(names_header, 16)?).ok()?,
            usize::try_from(endian.u32(names_header, 20)?).ok()?,
        )
    } else {
        (
            usize::try_from(endian.u64(names_header, 24)?).ok()?,
            usize::try_from(endian.u64(names_header, 32)?).ok()?,
        )
    };
    let names = bytes.get(names_offset..names_offset.checked_add(names_len)?)?;
    if names.first() != Some(&0) {
        return None;
    }

    for index in 0..section_count {
        let header = section(index)?;
        let section_type = endian.u32(header, 4)?;
        if matches!(section_type, SHT_LLVM_OFFLOADING | SHT_LLVM_LTO) {
            return None;
        }
        let name_offset = usize::try_from(endian.u32(header, 0)?).ok()?;
        let raw_name = names.get(name_offset..)?;
        let name_end = raw_name.iter().position(|byte| *byte == 0)?;
        let name = &raw_name[..name_end];
        if name.starts_with(b".gnu.lto_")
            || name.starts_with(b".gnu.offload_lto_")
            || name == b".llvmbc"
            || name == b".llvmcmd"
            || name.starts_with(b".llvm.lto")
            || name.starts_with(b".llvm.offloading")
        {
            return None;
        }

        // All non-NOBITS sections occupy real file bytes and must be bounded.
        if section_type != 8 {
            let (offset, len) = if class == 1 {
                (
                    u64::from(endian.u32(header, 16)?),
                    u64::from(endian.u32(header, 20)?),
                )
            } else {
                (endian.u64(header, 24)?, endian.u64(header, 32)?)
            };
            let start = usize::try_from(offset).ok()?;
            let len = usize::try_from(len).ok()?;
            bytes.get(start..start.checked_add(len)?)?;
        }
    }
    Some(())
}

fn parse_known_no_debug_macho_object(bytes: &[u8]) -> Option<()> {
    let magic = bytes.get(..4)?;
    let (endian, is_64) = match magic {
        b"\xce\xfa\xed\xfe" => (MachEndian::Little, false),
        b"\xfe\xed\xfa\xce" => (MachEndian::Big, false),
        b"\xcf\xfa\xed\xfe" => (MachEndian::Little, true),
        b"\xfe\xed\xfa\xcf" => (MachEndian::Big, true),
        _ => return None, // fat Mach-O, LLVM bitcode, ELF, and unknown formats
    };
    let header_len = if is_64 { 32 } else { 28 };
    bytes.get(..header_len)?;

    let cpu_type = endian.u32(bytes, 4)?;
    let known_cpu = match cpu_type {
        7 | 12 | 18 => !is_64, // x86, ARM, PowerPC
        0x0100_0007 | 0x0100_000c | 0x0100_0012 => is_64,
        // ARM64_32 and every unknown CPU are intentionally path-bound. ld64
        // has ARM64_32 relocation behavior keyed by substrings of the complete
        // `archive-path(member)` name.
        _ => false,
    };
    if !known_cpu || endian.u32(bytes, 12)? != MH_OBJECT {
        return None;
    }

    let ncmds = usize::try_from(endian.u32(bytes, 16)?).ok()?;
    let sizeofcmds = usize::try_from(endian.u32(bytes, 20)?).ok()?;
    let commands_end = header_len.checked_add(sizeofcmds)?;
    bytes.get(header_len..commands_end)?;
    if !macho_command_table_is_plausible(ncmds, sizeofcmds) {
        return None;
    }

    let command_alignment = if is_64 { 8 } else { 4 };
    let mut command_offset = header_len;
    let mut section_count = 0_u32;
    let mut saw_segment = false;
    let mut symtab: Option<MachSymtab> = None;
    let mut dysymtab: Option<[u32; 18]> = None;

    for _ in 0..ncmds {
        let cmd = endian.u32(bytes, command_offset)?;
        let cmdsize = usize::try_from(endian.u32(bytes, command_offset + 4)?).ok()?;
        if !macho_command_size_is_valid(cmdsize, command_alignment) {
            return None;
        }
        let command_end = command_offset.checked_add(cmdsize)?;
        if command_end > commands_end {
            return None;
        }
        let command = &bytes[command_offset..command_end];

        match cmd {
            LC_SEGMENT | LC_SEGMENT_64 => {
                let segment_is_64 = macho_segment_width(cmd, is_64)?;
                section_count = section_count.checked_add(validate_macho_segment(
                    bytes,
                    command,
                    endian,
                    segment_is_64,
                    commands_end,
                )?)?;
                saw_segment = true;
            }
            LC_SYMTAB => {
                if symtab.is_some() {
                    return None;
                }
                let command: &[u8; 24] = command.try_into().ok()?;
                symtab = Some(MachSymtab {
                    symoff: endian.u32(command, 8)?,
                    nsyms: endian.u32(command, 12)?,
                    stroff: endian.u32(command, 16)?,
                    strsize: endian.u32(command, 20)?,
                });
            }
            LC_DYSYMTAB => {
                if dysymtab.is_some() {
                    return None;
                }
                let command: &[u8; 80] = command.try_into().ok()?;
                let mut fields = [0_u32; 18];
                for (field, raw) in fields.iter_mut().zip(command[8..].as_chunks::<4>().0) {
                    *field = endian.u32(raw, 0)?;
                }
                dysymtab = Some(fields);
            }
            LC_BUILD_VERSION => validate_build_version(command, endian)?,
            LC_VERSION_MIN_MACOSX
            | LC_VERSION_MIN_IPHONEOS
            | LC_VERSION_MIN_TVOS
            | LC_VERSION_MIN_WATCHOS => {
                let _: &[u8; 16] = command.try_into().ok()?;
            }
            LC_DATA_IN_CODE | LC_LINKER_OPTIMIZATION_HINT => {
                validate_linkedit_data(bytes, command, endian, commands_end)?;
                if cmd == LC_DATA_IN_CODE && endian.u32(command, 12)? % 8 != 0 {
                    return None;
                }
            }
            LC_LINKER_OPTION => validate_linker_option(command, endian)?,
            LC_SOURCE_VERSION => {
                let _: &[u8; 16] = command.try_into().ok()?;
            }
            LC_UUID => {
                let _: &[u8; 24] = command.try_into().ok()?;
            }
            _ => return None, // unknown semantics: preserve the whole archive
        }

        command_offset = command_end;
    }

    if !macho_commands_are_complete(command_offset, commands_end, saw_segment) {
        return None;
    }
    let symtab = symtab?;
    validate_macho_symtab(bytes, endian, is_64, commands_end, section_count, symtab)?;
    if let Some(fields) = dysymtab {
        validate_macho_dysymtab(bytes, is_64, commands_end, symtab.nsyms, &fields)?;
    }
    Some(())
}

fn macho_command_table_is_plausible(ncmds: usize, sizeofcmds: usize) -> bool {
    ncmds != 0
        && ncmds
            .checked_mul(8)
            .is_some_and(|minimum_size| minimum_size <= sizeofcmds)
}

fn macho_command_size_is_valid(command_size: usize, alignment: usize) -> bool {
    command_size >= 8 && command_size.is_multiple_of(alignment)
}

fn macho_segment_width(command: u32, object_is_64: bool) -> Option<bool> {
    match (command, object_is_64) {
        (LC_SEGMENT, false) => Some(false),
        (LC_SEGMENT_64, true) => Some(true),
        _ => None,
    }
}

fn macho_commands_are_complete(
    command_offset: usize,
    commands_end: usize,
    saw_segment: bool,
) -> bool {
    command_offset == commands_end && saw_segment
}

fn validate_macho_segment(
    bytes: &[u8],
    command: &[u8],
    endian: MachEndian,
    is_64: bool,
    commands_end: usize,
) -> Option<u32> {
    let (base_size, section_size, fileoff, filesize, nsects) = if is_64 {
        (
            72_usize,
            80_usize,
            endian.u64(command, 40)?,
            endian.u64(command, 48)?,
            endian.u32(command, 64)?,
        )
    } else {
        (
            56_usize,
            68_usize,
            u64::from(endian.u32(command, 32)?),
            u64::from(endian.u32(command, 36)?),
            endian.u32(command, 48)?,
        )
    };
    let section_bytes = usize::try_from(nsects).ok()?.checked_mul(section_size)?;
    if command.len() != base_size.checked_add(section_bytes)? {
        return None;
    }

    let segment_name = macho_fixed_name(command.get(8..24)?)?;
    if !macho_segment_is_portable(segment_name) {
        return None;
    }
    let segment_range = if filesize == 0 {
        None
    } else {
        Some(checked_file_region(bytes.len(), fileoff, filesize, 0)?)
    };

    for index in 0..usize::try_from(nsects).ok()? {
        let start = base_size.checked_add(index.checked_mul(section_size)?)?;
        let section = command.get(start..start.checked_add(section_size)?)?;
        let section_name = macho_fixed_name(section.get(0..16)?)?;
        let section_segment = macho_fixed_name(section.get(16..32)?)?;
        let (size, offset, reloff, nreloc, flags) = if is_64 {
            (
                endian.u64(section, 40)?,
                endian.u32(section, 48)?,
                endian.u32(section, 56)?,
                endian.u32(section, 60)?,
                endian.u32(section, 64)?,
            )
        } else {
            (
                u64::from(endian.u32(section, 36)?),
                endian.u32(section, 40)?,
                endian.u32(section, 48)?,
                endian.u32(section, 52)?,
                endian.u32(section, 56)?,
            )
        };

        if !macho_section_is_portable(section_segment, section_name, flags) {
            return None;
        }

        let is_zerofill = is_macho_zerofill(flags);
        if !is_zerofill && size != 0 {
            let section_range =
                checked_file_region(bytes.len(), u64::from(offset), size, commands_end)?;
            if let Some((segment_start, segment_end)) = segment_range
                && !range_is_within(section_range, (segment_start, segment_end))
            {
                return None;
            }
        }
        if nreloc != 0 {
            let relocation_bytes = u64::from(nreloc).checked_mul(8)?;
            checked_file_region(
                bytes.len(),
                u64::from(reloff),
                relocation_bytes,
                commands_end,
            )?;
        }
    }
    Some(nsects)
}

fn macho_segment_is_portable(name: &[u8]) -> bool {
    name != b"__DWARF" && !is_macho_lto_segment(name)
}

fn macho_section_is_portable(section_segment: &[u8], section_name: &[u8], flags: u32) -> bool {
    // clang marks `__LD,__compact_unwind` with S_ATTR_DEBUG so ld64 strips
    // the intermediate records after producing runtime unwind metadata. It
    // is not source-level debug data and does not cause an N_OSO entry; a
    // no-debug C/C++ object commonly contains it.
    let compact_unwind = section_segment == b"__LD" && section_name == b"__compact_unwind";
    (flags & S_ATTR_DEBUG == 0 || compact_unwind)
        && section_segment != b"__DWARF"
        && !section_name.starts_with(b"__debug_")
        && !section_name.starts_with(b"__zdebug_")
        && !is_macho_lto_segment(section_segment)
        && section_name != b"__bitcode"
        && section_name != b"__bundle"
}

fn is_macho_zerofill(flags: u32) -> bool {
    matches!(
        flags & SECTION_TYPE,
        S_ZEROFILL | S_GB_ZEROFILL | S_THREAD_LOCAL_ZEROFILL
    )
}

fn range_is_within(inner: (usize, usize), outer: (usize, usize)) -> bool {
    inner.0 >= outer.0 && inner.1 <= outer.1
}

fn is_macho_lto_segment(name: &[u8]) -> bool {
    name == b"__LLVM" || name == b"__GNU_LTO" || name == b"__GNU_OFFLD_LTO"
}

fn validate_macho_symtab(
    bytes: &[u8],
    endian: MachEndian,
    is_64: bool,
    commands_end: usize,
    section_count: u32,
    symtab: MachSymtab,
) -> Option<()> {
    let entry_size = if is_64 { 16_u64 } else { 12_u64 };
    let symbols_size = u64::from(symtab.nsyms).checked_mul(entry_size)?;
    let (symbols_start, _) = checked_file_region(
        bytes.len(),
        u64::from(symtab.symoff),
        symbols_size,
        commands_end,
    )?;
    let (strings_start, strings_end) = checked_file_region(
        bytes.len(),
        u64::from(symtab.stroff),
        u64::from(symtab.strsize),
        commands_end,
    )?;
    let strings = bytes.get(strings_start..strings_end)?;
    if strings.first() != Some(&0) {
        return None;
    }

    let entry_size = usize::try_from(entry_size).ok()?;
    for index in 0..usize::try_from(symtab.nsyms).ok()? {
        let start = symbols_start.checked_add(index.checked_mul(entry_size)?)?;
        let symbol = bytes.get(start..start.checked_add(entry_size)?)?;
        let string_index = usize::try_from(endian.u32(symbol, 0)?).ok()?;
        let symbol_type = *symbol.get(4)?;
        if symbol_type & N_STAB != 0 {
            return None;
        }
        let section = u32::from(*symbol.get(5)?);
        if !macho_symbol_section_is_valid(symbol_type, section, section_count) {
            return None;
        }
        if !macho_symbol_name_is_valid(strings, string_index) {
            return None;
        }
    }
    Some(())
}

fn macho_symbol_section_is_valid(symbol_type: u8, section: u32, section_count: u32) -> bool {
    symbol_type & N_TYPE != N_SECT || (section != 0 && section <= section_count)
}

fn macho_symbol_name_is_valid(strings: &[u8], string_index: usize) -> bool {
    string_index == 0
        || strings
            .get(string_index..)
            .is_some_and(|name| name.contains(&0))
}

fn validate_macho_dysymtab(
    bytes: &[u8],
    is_64: bool,
    commands_end: usize,
    symbol_count: u32,
    fields: &[u32; 18],
) -> Option<()> {
    for (start, count) in [
        (fields[0], fields[1]),
        (fields[2], fields[3]),
        (fields[4], fields[5]),
    ] {
        if start.checked_add(count)? > symbol_count {
            return None;
        }
    }

    validate_counted_region(bytes, fields[6], fields[7], 8, commands_end)?;
    validate_counted_region(
        bytes,
        fields[8],
        fields[9],
        if is_64 { 56 } else { 52 },
        commands_end,
    )?;
    validate_counted_region(bytes, fields[10], fields[11], 4, commands_end)?;
    validate_counted_region(bytes, fields[12], fields[13], 4, commands_end)?;
    validate_counted_region(bytes, fields[14], fields[15], 8, commands_end)?;
    validate_counted_region(bytes, fields[16], fields[17], 8, commands_end)?;
    Some(())
}

fn validate_build_version(command: &[u8], endian: MachEndian) -> Option<()> {
    let tools_size = usize::try_from(endian.u32(command, 20)?)
        .ok()?
        .checked_mul(8)?;
    if command.len() != 24_usize.checked_add(tools_size)? {
        return None;
    }
    Some(())
}

fn validate_linkedit_data(
    bytes: &[u8],
    command: &[u8],
    endian: MachEndian,
    commands_end: usize,
) -> Option<()> {
    let command: &[u8; 16] = command.try_into().ok()?;
    checked_file_region(
        bytes.len(),
        u64::from(endian.u32(command, 8)?),
        u64::from(endian.u32(command, 12)?),
        commands_end,
    )?;
    Some(())
}

fn validate_linker_option(command: &[u8], endian: MachEndian) -> Option<()> {
    let header = command.get(..12)?;
    let count = usize::try_from(endian.u32(header, 8)?).ok()?;
    let mut rest = command.get(12..)?;
    for _ in 0..count {
        let end = rest.iter().position(|&byte| byte == 0)?;
        if end == 0 {
            return None;
        }
        rest = rest.get(end + 1..)?;
    }
    if rest.iter().any(|&byte| byte != 0) {
        return None;
    }
    Some(())
}

fn validate_counted_region(
    bytes: &[u8],
    offset: u32,
    count: u32,
    entry_size: u64,
    commands_end: usize,
) -> Option<()> {
    if count == 0 {
        return Some(());
    }
    checked_file_region(
        bytes.len(),
        u64::from(offset),
        u64::from(count).checked_mul(entry_size)?,
        commands_end,
    )?;
    Some(())
}

fn macho_fixed_name(field: &[u8]) -> Option<&[u8]> {
    if field.len() != 16 {
        return None;
    }
    let end = field
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(field.len());
    if field[end..].iter().any(|&byte| byte != 0) {
        return None;
    }
    Some(&field[..end])
}

/// The Darwin ranlib (symbol-table) member names: {32, 64-bit} x {unsorted,
/// sorted}. Matched after inline-name NUL-trimming, so the `#1/N`-encoded
/// spellings land here too.
fn is_bsd_symdef(name: &str) -> bool {
    matches!(
        name,
        "__.SYMDEF" | "__.SYMDEF SORTED" | "__.SYMDEF_64" | "__.SYMDEF_64 SORTED"
    )
}

enum Member {
    SymbolTable,
    LongNameTable,
    Object,
}

/// Classify a GNU member by its (space-trimmed) name field. Only the exact
/// reserved names are special; a `/123` long-name reference or a `name/` short
/// name is an ordinary object member whose exact identity is hashed.
fn classify(name: &str) -> Member {
    match name {
        "/" | "/SYM64/" => Member::SymbolTable,
        "//" => Member::LongNameTable,
        _ => Member::Object,
    }
}

/// The raw 16-byte name field with trailing ASCII spaces removed. Invalid UTF-8
/// falls back instead of allowing two byte-distinct names to compare equally.
fn ar_name(field: &[u8]) -> Option<&str> {
    let end = field.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    std::str::from_utf8(&field[..end]).ok()
}

/// Parse a space-padded ASCII decimal `ar` header field. `None` on empty or any
/// non-digit byte (which forces a safe fallback rather than a misparse).
fn parse_ar_decimal(field: &[u8]) -> Option<usize> {
    let s = std::str::from_utf8(field).ok()?.trim_end();
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<usize>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// Build a 60-byte GNU `ar` header + data + even padding for one member.
    fn member(name: &str, data: &[u8]) -> Vec<u8> {
        assert!(name.len() <= 16);
        let mut h = Vec::new();
        h.extend_from_slice(format!("{name:<16}").as_bytes()); // name (16)
        h.extend_from_slice(b"0           "); // mtime (12)
        h.extend_from_slice(b"0     "); // uid (6)
        h.extend_from_slice(b"0     "); // gid (6)
        h.extend_from_slice(b"100644  "); // mode (8)
        h.extend_from_slice(format!("{:<10}", data.len()).as_bytes()); // size (10)
        h.extend_from_slice(b"`\n"); // terminator (2)
        assert_eq!(h.len(), AR_HEADER_LEN);
        h.extend_from_slice(data);
        if data.len() % 2 == 1 {
            h.push(b'\n'); // even-boundary padding
        }
        h
    }

    fn raw_archive(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut a = AR_MAGIC.to_vec();
        for (n, d) in members {
            a.extend_from_slice(&member(n, d));
        }
        a
    }

    fn elf_object_with_section_type(
        section_name: &[u8],
        section_type: u32,
        payload: &[u8],
    ) -> Vec<u8> {
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
        let mut names = vec![0];
        let payload_name_offset = names.len();
        names.extend_from_slice(section_name);
        names.push(0);
        let table_name_offset = names.len();
        names.extend_from_slice(b".shstrtab\0");
        object.extend_from_slice(&names);
        while !object.len().is_multiple_of(8) {
            object.push(0);
        }
        let section_offset = object.len();
        object.resize(section_offset + 3 * 64, 0);
        object[40..48].copy_from_slice(&(section_offset as u64).to_le_bytes());

        let payload_header = section_offset + 64;
        object[payload_header..payload_header + 4]
            .copy_from_slice(&(payload_name_offset as u32).to_le_bytes());
        object[payload_header + 4..payload_header + 8].copy_from_slice(&section_type.to_le_bytes());
        object[payload_header + 24..payload_header + 32]
            .copy_from_slice(&(payload_offset as u64).to_le_bytes());
        object[payload_header + 32..payload_header + 40]
            .copy_from_slice(&(payload.len() as u64).to_le_bytes());
        object[payload_header + 48..payload_header + 56].copy_from_slice(&1_u64.to_le_bytes());

        let names_header = section_offset + 2 * 64;
        object[names_header..names_header + 4]
            .copy_from_slice(&(table_name_offset as u32).to_le_bytes());
        object[names_header + 4..names_header + 8].copy_from_slice(&3_u32.to_le_bytes());
        object[names_header + 24..names_header + 32]
            .copy_from_slice(&(names_offset as u64).to_le_bytes());
        object[names_header + 32..names_header + 40]
            .copy_from_slice(&(names.len() as u64).to_le_bytes());
        object[names_header + 48..names_header + 56].copy_from_slice(&1_u64.to_le_bytes());
        object
    }

    fn elf_object_with_section(section_name: &[u8], payload: &[u8]) -> Vec<u8> {
        elf_object_with_section_type(section_name, 1, payload)
    }

    fn elf32be_object(payload: &[u8]) -> Vec<u8> {
        let mut object = vec![0_u8; 52];
        object[..4].copy_from_slice(b"\x7fELF");
        object[4] = 1; // ELFCLASS32
        object[5] = 2; // ELFDATA2MSB
        object[6] = 1; // EV_CURRENT
        object[16..18].copy_from_slice(&1_u16.to_be_bytes()); // ET_REL
        object[18..20].copy_from_slice(&2_u16.to_be_bytes()); // EM_SPARC
        object[20..24].copy_from_slice(&1_u32.to_be_bytes());
        object[40..42].copy_from_slice(&52_u16.to_be_bytes());
        object[46..48].copy_from_slice(&40_u16.to_be_bytes());
        object[48..50].copy_from_slice(&3_u16.to_be_bytes());
        object[50..52].copy_from_slice(&2_u16.to_be_bytes());

        let payload_offset = object.len();
        object.extend_from_slice(payload);
        let names_offset = object.len();
        let names = b"\0.data\0.shstrtab\0";
        object.extend_from_slice(names);
        while !object.len().is_multiple_of(4) {
            object.push(0);
        }
        let section_offset = object.len();
        object.resize(section_offset + 3 * 40, 0);
        object[32..36].copy_from_slice(&(section_offset as u32).to_be_bytes());

        let payload_header = section_offset + 40;
        object[payload_header..payload_header + 4].copy_from_slice(&1_u32.to_be_bytes());
        object[payload_header + 4..payload_header + 8].copy_from_slice(&1_u32.to_be_bytes());
        object[payload_header + 16..payload_header + 20]
            .copy_from_slice(&(payload_offset as u32).to_be_bytes());
        object[payload_header + 20..payload_header + 24]
            .copy_from_slice(&(payload.len() as u32).to_be_bytes());
        object[payload_header + 32..payload_header + 36].copy_from_slice(&1_u32.to_be_bytes());

        let names_header = section_offset + 2 * 40;
        object[names_header..names_header + 4].copy_from_slice(&7_u32.to_be_bytes());
        object[names_header + 4..names_header + 8].copy_from_slice(&3_u32.to_be_bytes());
        object[names_header + 16..names_header + 20]
            .copy_from_slice(&(names_offset as u32).to_be_bytes());
        object[names_header + 20..names_header + 24]
            .copy_from_slice(&(names.len() as u32).to_be_bytes());
        object[names_header + 32..names_header + 36].copy_from_slice(&1_u32.to_be_bytes());
        object
    }

    fn elf_object(payload: &[u8]) -> Vec<u8> {
        elf_object_with_section(b".data", payload)
    }

    /// GNU parser fixtures use structurally valid ELF objects by default.
    /// Tests for unknown/bitcode payloads call [`raw_archive`] explicitly.
    fn archive(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut a = AR_MAGIC.to_vec();
        for (name, data) in members {
            let wrapped;
            let data = if matches!(classify(name), Member::Object)
                && !has_macho_magic(data)
                && !is_known_elf_relocatable_object(data)
            {
                wrapped = elf_object(data);
                wrapped.as_slice()
            } else {
                data
            };
            a.extend_from_slice(&member(name, data));
        }
        a
    }

    // A realistic GNU symbol-table payload (its exact bytes don't matter to the
    // parser; only that it is stable across clones, which it is for GNU).
    const SYMTAB: &[u8] = b"\x00\x00\x00\x01\x00\x00\x00\x68foo\x00";

    #[test]
    fn gnu_longname_table_has_one_canonical_slot() {
        let first = archive(&[("//", b"object.o/\n"), ("/0", b"object")]);
        assert!(gnu_archive_hash(&first).is_some());

        let after_symtab = archive(&[("/", SYMTAB), ("//", b"object.o/\n"), ("/0", b"object")]);
        assert!(gnu_archive_hash(&after_symtab).is_some());

        let misplaced = archive(&[
            ("first.o/", b"first"),
            ("//", b"second.o/\n"),
            ("/0", b"second"),
        ]);
        assert!(gnu_archive_hash(&misplaced).is_none());
    }

    #[test]
    fn bsd_magic_gate_handles_short_exact_and_wrong_prefixes() {
        assert!(bsd_archive_hash(b"").is_none());
        assert!(bsd_archive_hash(AR_MAGIC).is_none());
        assert!(bsd_archive_hash(b"!<arch>?").is_none());
    }

    #[test]
    fn macho_magic_recognizes_every_supported_width_and_endian() {
        for magic in [
            &b"\xce\xfa\xed\xfe"[..],
            &b"\xfe\xed\xfa\xce"[..],
            &b"\xcf\xfa\xed\xfe"[..],
            &b"\xfe\xed\xfa\xcf"[..],
        ] {
            assert!(has_macho_magic(magic));
        }
        for other in [&b""[..], &b"\xce\xfa\xed"[..], &b"\x7fELF"[..]] {
            assert!(!has_macho_magic(other));
        }
    }

    #[test]
    fn elf_machine_shape_matrix_is_explicit() {
        let accepted = [
            (2, 1, MachEndian::Big),
            (3, 1, MachEndian::Little),
            (8, 1, MachEndian::Little),
            (8, 1, MachEndian::Big),
            (8, 2, MachEndian::Little),
            (8, 2, MachEndian::Big),
            (20, 1, MachEndian::Little),
            (40, 1, MachEndian::Little),
            (42, 1, MachEndian::Little),
            (21, 2, MachEndian::Little),
            (50, 2, MachEndian::Little),
            (183, 2, MachEndian::Little),
            (247, 2, MachEndian::Little),
            (22, 2, MachEndian::Big),
            (43, 2, MachEndian::Big),
            (62, 2, MachEndian::Little),
            (258, 2, MachEndian::Little),
            (243, 1, MachEndian::Little),
            (243, 2, MachEndian::Little),
        ];
        for (machine, class, endian) in accepted {
            assert!(
                elf_machine_shape_is_supported(machine, class, endian),
                "machine {machine}, class {class} must be accepted"
            );
        }

        let rejected = [
            (2, 1, MachEndian::Little),
            (2, 2, MachEndian::Big),
            (3, 1, MachEndian::Big),
            (3, 2, MachEndian::Little),
            (8, 3, MachEndian::Little),
            (20, 2, MachEndian::Little),
            (40, 2, MachEndian::Little),
            (42, 2, MachEndian::Little),
            (21, 1, MachEndian::Little),
            (50, 1, MachEndian::Little),
            (183, 1, MachEndian::Little),
            (247, 1, MachEndian::Little),
            (22, 2, MachEndian::Little),
            (22, 1, MachEndian::Big),
            (43, 2, MachEndian::Little),
            (43, 1, MachEndian::Big),
            (62, 2, MachEndian::Big),
            (62, 1, MachEndian::Little),
            (258, 2, MachEndian::Big),
            (243, 1, MachEndian::Big),
            (243, 3, MachEndian::Little),
            (0xffff, 2, MachEndian::Little),
        ];
        for (machine, class, endian) in rejected {
            assert!(
                !elf_machine_shape_is_supported(machine, class, endian),
                "machine {machine}, class {class} must be rejected"
            );
        }
    }

    #[test]
    fn elf_section_layout_checks_each_boundary_independently() {
        assert!(elf_section_layout_is_supported(64, 64, 3, 2));
        assert!(elf_section_layout_is_supported(64, 65, 3, 2));
        for (header_len, offset, count, names) in [
            (64, 64, 0, 1),
            (64, 64, 3, 0),
            (64, 64, 3, 3),
            (64, 63, 3, 2),
        ] {
            assert!(!elf_section_layout_is_supported(
                header_len, offset, count, names
            ));
        }
    }

    #[test]
    fn elf_header_and_section_bounds_fail_closed_one_field_at_a_time() {
        let valid64 = elf_object(b"payload");
        let mut bad_magic = valid64.clone();
        bad_magic[0] = 0;
        assert!(parse_known_elf_relocatable_object(&bad_magic).is_none());
        let mut bad_version = valid64.clone();
        bad_version[6] = 2;
        assert!(parse_known_elf_relocatable_object(&bad_version).is_none());

        let valid32 = elf32be_object(b"payload");
        for range in [40..42, 46..48] {
            let mut malformed = valid32.clone();
            malformed[range].fill(0);
            assert!(parse_known_elf_relocatable_object(&malformed).is_none());
        }
        for range in [52..54, 58..60] {
            let mut malformed = valid64.clone();
            malformed[range].fill(0);
            assert!(parse_known_elf_relocatable_object(&malformed).is_none());
        }

        let section_offset =
            usize::try_from(u64::from_le_bytes(valid64[40..48].try_into().unwrap())).unwrap();
        let payload_header = section_offset + 64;
        let mut out_of_bounds = valid64.clone();
        out_of_bounds[payload_header + 24..payload_header + 32]
            .copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(parse_known_elf_relocatable_object(&out_of_bounds).is_none());

        let mut nobits = out_of_bounds;
        nobits[payload_header + 4..payload_header + 8].copy_from_slice(&8_u32.to_le_bytes());
        assert!(
            parse_known_elf_relocatable_object(&nobits).is_some(),
            "SHT_NOBITS occupies no file bytes"
        );
    }

    #[test]
    fn identical_content_different_member_names_hash_differ() {
        // Member names remain observable after rustc bundles this archive into
        // an rlib, so even same-length cc path prefixes must re-key.
        let obj1 = b"\x7fELF-object-one-contents";
        let obj2 = b"\x7fELF-object-two-contents!"; // even len
        let clone_a = archive(&[
            ("/", SYMTAB),
            ("//", b"cafca65b3467684e-a.o/\ncafca65b3467684e-b.o/\n"),
            ("/0", obj1),
            ("/22", obj2),
        ]);
        let clone_b = archive(&[
            ("/", SYMTAB),
            ("//", b"4af22b2a007cb61a-a.o/\n4af22b2a007cb61a-b.o/\n"),
            ("/0", obj1),
            ("/22", obj2),
        ]);
        let ha = portable_static_archive_hash(&clone_a).expect("clone-a parses");
        let hb = portable_static_archive_hash(&clone_b).expect("clone-b parses");
        assert_ne!(
            ha, hb,
            "effective member names are part of archive identity"
        );
    }

    #[test]
    fn changed_object_content_changes_hash() {
        // #421 must be preserved: a real object-byte change re-keys.
        let a = archive(&[("/", SYMTAB), ("//", b"x.o/\n"), ("/0", b"object-vONE")]);
        let b = archive(&[("/", SYMTAB), ("//", b"x.o/\n"), ("/0", b"object-vTWO")]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn member_timestamp_changes_hash() {
        let a = archive(&[("object.o/", b"same object")]);
        let mut b = a.clone();
        b[AR_MAGIC.len() + 16..AR_MAGIC.len() + 28].fill(b' ');
        b[AR_MAGIC.len() + 16] = b'1';
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn longname_reference_requires_a_valid_table_entry() {
        assert!(portable_static_archive_hash(&archive(&[("/0", b"obj")])).is_none());

        let mid_entry = archive(&[("//", b"first.o/\nsecond.o/\n"), ("/2", b"obj")]);
        assert!(portable_static_archive_hash(&mid_entry).is_none());

        let unterminated = archive(&[("//", b"first.o"), ("/0", b"obj")]);
        assert!(portable_static_archive_hash(&unterminated).is_none());
    }

    #[test]
    fn changed_symbol_table_changes_hash() {
        // A stale/crafted symbol table that disagrees with the members must NOT
        // collide with one that links differently — the symtab is hashed, not
        // dropped.
        let a = archive(&[("/", SYMTAB), ("foo.o/", b"obj-data")]);
        let b = archive(&[
            ("/", b"\x00\x00\x00\x01\x00\x00\x00\x99bar\x00"),
            ("foo.o/", b"obj-data"),
        ]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn member_order_changes_hash() {
        // Order is link-significant; reordering members must re-key.
        let a = archive(&[("a.o/", b"aaaa"), ("b.o/", b"bbbb")]);
        let b = archive(&[("a.o/", b"bbbb"), ("b.o/", b"aaaa")]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn longname_table_content_changes_hash() {
        // The complete effective name mapping is identity-bearing.
        let a = archive(&[("//", b"aaaaaaaa/\n"), ("/0", b"obj")]);
        let b = archive(&[("//", b"bbbbbbbb/\n"), ("/0", b"obj")]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn longname_table_length_change_changes_hash() {
        // codex review #471: the `//` table's LENGTH shifts the absolute member
        // offsets the symbol table stores. A different-LENGTH `//` (with the same
        // raw symtab + object bytes) must re-key, or a stale/crafted symtab could
        // point at a different member yet collide. Both content and length are hashed.
        let a = archive(&[("/", SYMTAB), ("//", b"aaaaaaaa/\n"), ("/0", b"obj")]);
        let b = archive(&[
            ("/", SYMTAB),
            ("//", b"aaaaaaaaaaaaaaaa/\n"),
            ("/0", b"obj"),
        ]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn output_is_domain_tagged() {
        let a = archive(&[("obj.o/", b"obj")]);
        assert!(
            portable_static_archive_hash(&a)
                .unwrap()
                .starts_with("gnu-ar-v2:"),
            "portable digest must be textually distinct from the whole-file fallback"
        );
    }

    #[test]
    fn symbol_table_must_be_first() {
        // A `/` symtab after a regular member is not a plain GNU archive.
        let a = archive(&[("/0", b"obj"), ("/", SYMTAB)]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn second_symbol_table_falls_back_coff_like() {
        // COFF `.lib` has two leading `/` linker members -> must fall back.
        let a = archive(&[("/", SYMTAB), ("/", SYMTAB), ("/0", b"obj")]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn misplaced_longname_table_falls_back() {
        // `//` after a regular member (not in the optional first/second slot).
        let a = archive(&[("/0", b"obj"), ("//", b"x.o/\n")]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn darwin64_symdef_with_gnu_member_falls_back() {
        // A Darwin64 ranlib member followed by a GNU `/0` name ref is a mixed
        // layout: the GNU arm rejects the `__.SYMDEF_64` marker and the BSD
        // arm rejects the GNU name shape -> whole-file fallback, never a guess.
        let a = archive(&[("__.SYMDEF_64", b"symdef64data"), ("/0", b"obj")]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn sym64_and_sym32_with_identical_bytes_hash_differently() {
        // `/` (32-bit armap) and `/SYM64/` (64-bit armap) interpret the SAME
        // bytes with different word widths, so they must never collide even when
        // their member data + objects are byte-identical (codex review #471).
        let map = b"\x00\x00\x00\x00\x00\x00\x00\x58foo\x00";
        let a32 = archive(&[("/", map), ("foo.o/", b"OBJ\n")]);
        let a64 = archive(&[("/SYM64/", map), ("foo.o/", b"OBJ\n")]);
        assert_ne!(
            portable_static_archive_hash(&a32).unwrap(),
            portable_static_archive_hash(&a64).unwrap(),
        );
    }

    #[test]
    fn bsd_symdef_with_gnu_member_falls_back() {
        // Same mixed-layout contract as the Darwin64 case above, 32-bit name.
        let a = archive(&[("__.SYMDEF", b"symdefdata"), ("/0", b"obj")]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn thin_archive_falls_back() {
        let mut a = b"!<thin>\n".to_vec();
        a.extend_from_slice(&member("/0", b"obj"));
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn non_archive_falls_back() {
        assert!(portable_static_archive_hash(b"not an archive at all").is_none());
        assert!(portable_static_archive_hash(b"").is_none());
        assert!(portable_static_archive_hash(b"!<arch>\n").is_none()); // magic only, no members
    }

    #[test]
    fn malformed_falls_back() {
        // Invalid UTF-8 in an identity-bearing name must not be lossily folded.
        let mut invalid_name = archive(&[("object.o/", b"obj")]);
        invalid_name[AR_MAGIC.len()] = 0xff;
        assert!(portable_static_archive_hash(&invalid_name).is_none());

        // Bad header terminator.
        let mut bad = AR_MAGIC.to_vec();
        let mut m = member("/0", b"obj");
        m[58] = b'X';
        bad.extend_from_slice(&m);
        assert!(portable_static_archive_hash(&bad).is_none());

        // Truncated mid-data (size says more than is present).
        let mut trunc = AR_MAGIC.to_vec();
        trunc.extend_from_slice(b"/0              0           0     0     100644  9999      `\n");
        trunc.extend_from_slice(b"short");
        assert!(portable_static_archive_hash(&trunc).is_none());

        // Non-decimal size field.
        let mut nondec = AR_MAGIC.to_vec();
        nondec.extend_from_slice(b"/0              0           0     0     100644  12x4      `\n");
        nondec.extend_from_slice(b"data");
        assert!(portable_static_archive_hash(&nondec).is_none());
    }

    // ── BSD / Darwin (#691) ────────────────────────────────────────────────

    /// Build a BSD member with a `#1/pad` extended name: `name` NUL-padded to
    /// `pad` bytes inline before `data`; the header size INCLUDES those bytes.
    fn bsd_member(name: &str, pad: usize, data: &[u8]) -> Vec<u8> {
        assert!(name.len() <= pad);
        let total = pad + data.len();
        let mut h = Vec::new();
        h.extend_from_slice(format!("{:<16}", format!("#1/{pad}")).as_bytes()); // name (16)
        h.extend_from_slice(b"0           "); // mtime (12)
        h.extend_from_slice(b"0     "); // uid (6)
        h.extend_from_slice(b"0     "); // gid (6)
        h.extend_from_slice(b"100644  "); // mode (8)
        h.extend_from_slice(format!("{total:<10}").as_bytes()); // size (10)
        h.extend_from_slice(b"`\n"); // terminator (2)
        assert_eq!(h.len(), AR_HEADER_LEN);
        h.extend_from_slice(name.as_bytes());
        h.resize(AR_HEADER_LEN + pad, 0); // NUL name padding
        h.extend_from_slice(data);
        if total % 2 == 1 {
            h.push(b'\n'); // even-boundary padding
        }
        h
    }

    fn bsd_archive(members: &[(&str, usize, &[u8])]) -> Vec<u8> {
        let mut a = AR_MAGIC.to_vec();
        for (n, pad, d) in members {
            a.extend_from_slice(&bsd_member(n, *pad, d));
        }
        a
    }

    fn push_u16(out: &mut Vec<u8>, endian: MachEndian, value: u16) {
        out.extend_from_slice(&match endian {
            MachEndian::Little => value.to_le_bytes(),
            MachEndian::Big => value.to_be_bytes(),
        });
    }

    fn push_u32(out: &mut Vec<u8>, endian: MachEndian, value: u32) {
        out.extend_from_slice(&match endian {
            MachEndian::Little => value.to_le_bytes(),
            MachEndian::Big => value.to_be_bytes(),
        });
    }

    fn push_u64(out: &mut Vec<u8>, endian: MachEndian, value: u64) {
        out.extend_from_slice(&match endian {
            MachEndian::Little => value.to_le_bytes(),
            MachEndian::Big => value.to_be_bytes(),
        });
    }

    fn push_macho_name(out: &mut Vec<u8>, name: &str) {
        assert!(name.len() <= 16);
        out.extend_from_slice(name.as_bytes());
        out.resize(out.len() + 16 - name.len(), 0);
    }

    /// Structurally valid, minimal relocatable Mach-O used to exercise the
    /// fail-closed object gate without relying on host architecture fixtures.
    fn macho_object(
        endian: MachEndian,
        is_64: bool,
        segment_name: &str,
        section_name: &str,
        section_flags: u32,
        symbol_type: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let header_len = if is_64 { 32_usize } else { 28 };
        let segment_size = if is_64 { 72_usize } else { 56 };
        let section_size = if is_64 { 80_usize } else { 68 };
        let segment_command_size = segment_size + section_size;
        let sizeofcmds = segment_command_size + 24; // LC_SYMTAB
        let data_offset = header_len + sizeofcmds;
        let nlist_size = if is_64 { 16_usize } else { 12 };
        let symoff = data_offset + payload.len();
        let strings = b"\0_probe\0";
        let stroff = symoff + nlist_size;

        let mut out = Vec::new();
        out.extend_from_slice(match (endian, is_64) {
            (MachEndian::Little, false) => b"\xce\xfa\xed\xfe",
            (MachEndian::Big, false) => b"\xfe\xed\xfa\xce",
            (MachEndian::Little, true) => b"\xcf\xfa\xed\xfe",
            (MachEndian::Big, true) => b"\xfe\xed\xfa\xcf",
        });
        push_u32(
            &mut out,
            endian,
            match (endian, is_64) {
                (MachEndian::Little, false) => 7,          // x86
                (MachEndian::Little, true) => 0x0100_0007, // x86_64
                (MachEndian::Big, false) => 18,            // PowerPC
                (MachEndian::Big, true) => 0x0100_0012,    // PowerPC64
            },
        );
        push_u32(&mut out, endian, 3); // CPU subtype
        push_u32(&mut out, endian, MH_OBJECT);
        push_u32(&mut out, endian, 2); // ncmds
        push_u32(&mut out, endian, u32::try_from(sizeofcmds).unwrap());
        push_u32(&mut out, endian, 0); // flags
        if is_64 {
            push_u32(&mut out, endian, 0); // reserved
        }

        push_u32(
            &mut out,
            endian,
            if is_64 { LC_SEGMENT_64 } else { LC_SEGMENT },
        );
        push_u32(
            &mut out,
            endian,
            u32::try_from(segment_command_size).unwrap(),
        );
        push_macho_name(&mut out, segment_name);
        if is_64 {
            push_u64(&mut out, endian, 0); // vmaddr
            push_u64(&mut out, endian, u64::try_from(payload.len()).unwrap());
            push_u64(&mut out, endian, u64::try_from(data_offset).unwrap());
            push_u64(&mut out, endian, u64::try_from(payload.len()).unwrap());
        } else {
            push_u32(&mut out, endian, 0); // vmaddr
            push_u32(&mut out, endian, u32::try_from(payload.len()).unwrap());
            push_u32(&mut out, endian, u32::try_from(data_offset).unwrap());
            push_u32(&mut out, endian, u32::try_from(payload.len()).unwrap());
        }
        push_u32(&mut out, endian, 7); // maxprot
        push_u32(&mut out, endian, 7); // initprot
        push_u32(&mut out, endian, 1); // nsects
        push_u32(&mut out, endian, 0); // segment flags

        push_macho_name(&mut out, section_name);
        push_macho_name(&mut out, segment_name);
        if is_64 {
            push_u64(&mut out, endian, 0); // addr
            push_u64(&mut out, endian, u64::try_from(payload.len()).unwrap());
        } else {
            push_u32(&mut out, endian, 0); // addr
            push_u32(&mut out, endian, u32::try_from(payload.len()).unwrap());
        }
        push_u32(&mut out, endian, u32::try_from(data_offset).unwrap());
        push_u32(&mut out, endian, 0); // align
        push_u32(&mut out, endian, 0); // reloff
        push_u32(&mut out, endian, 0); // nreloc
        push_u32(&mut out, endian, section_flags);
        push_u32(&mut out, endian, 0); // reserved1
        push_u32(&mut out, endian, 0); // reserved2
        if is_64 {
            push_u32(&mut out, endian, 0); // reserved3
        }

        push_u32(&mut out, endian, LC_SYMTAB);
        push_u32(&mut out, endian, 24);
        push_u32(&mut out, endian, u32::try_from(symoff).unwrap());
        push_u32(&mut out, endian, 1); // nsyms
        push_u32(&mut out, endian, u32::try_from(stroff).unwrap());
        push_u32(&mut out, endian, u32::try_from(strings.len()).unwrap());
        assert_eq!(out.len(), data_offset);

        out.extend_from_slice(payload);
        push_u32(&mut out, endian, 1); // n_strx
        out.push(symbol_type);
        out.push(if symbol_type & N_TYPE == N_SECT { 1 } else { 0 });
        push_u16(&mut out, endian, 0); // n_desc
        if is_64 {
            push_u64(&mut out, endian, 0); // n_value
        } else {
            push_u32(&mut out, endian, 0); // n_value
        }
        out.extend_from_slice(strings);
        out
    }

    fn no_debug_macho(payload: &[u8]) -> Vec<u8> {
        macho_object(
            MachEndian::Little,
            true,
            "__TEXT",
            "__text",
            0,
            N_SECT,
            payload,
        )
    }

    fn read_le_u32(bytes: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
    }

    fn write_le_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn read_le_u64(bytes: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }

    fn write_le_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn macho_command(command: u32, size: usize) -> Vec<u8> {
        assert!(size >= 8);
        let mut bytes = vec![0_u8; size];
        write_le_u32(&mut bytes, 0, command);
        write_le_u32(&mut bytes, 4, u32::try_from(size).unwrap());
        bytes
    }

    /// Insert a command at the end of the load-command area of the canonical
    /// little-endian 64-bit fixture, shifting every file offset in the two
    /// original commands by the same amount.
    fn macho_with_extra_command(mut object: Vec<u8>, command: &[u8]) -> Vec<u8> {
        assert_eq!(&object[..4], b"\xcf\xfa\xed\xfe");
        assert!(command.len().is_multiple_of(8));
        let old_commands_size = usize::try_from(read_le_u32(&object, 20)).unwrap();
        let insertion = 32 + old_commands_size;
        let delta = u32::try_from(command.len()).unwrap();
        drop(object.splice(insertion..insertion, command.iter().copied()));

        let ncmds = read_le_u32(&object, 16) + 1;
        let sizeofcmds = read_le_u32(&object, 20) + delta;
        let fileoff = read_le_u64(&object, 72) + u64::from(delta);
        write_le_u32(&mut object, 16, ncmds);
        write_le_u32(&mut object, 20, sizeofcmds);
        write_le_u64(&mut object, 72, fileoff);
        for offset in [152, 192, 200] {
            let shifted = read_le_u32(&object, offset) + delta;
            write_le_u32(&mut object, offset, shifted);
        }
        object
    }

    #[test]
    fn macho_command_shape_helpers_cover_boundaries_and_widths() {
        for (ncmds, bytes, expected) in [
            (0, 0, false),
            (0, 8, false),
            (1, 7, false),
            (1, 8, true),
            (2, 15, false),
            (2, 16, true),
            (3, 16, false),
        ] {
            assert_eq!(macho_command_table_is_plausible(ncmds, bytes), expected);
        }
        for (size, alignment, expected) in [
            (0, 8, false),
            (7, 8, false),
            (8, 8, true),
            (12, 4, true),
            (12, 8, false),
            (16, 8, true),
        ] {
            assert_eq!(macho_command_size_is_valid(size, alignment), expected);
        }
        assert_eq!(macho_segment_width(LC_SEGMENT, false), Some(false));
        assert_eq!(macho_segment_width(LC_SEGMENT_64, true), Some(true));
        assert_eq!(macho_segment_width(LC_SEGMENT, true), None);
        assert_eq!(macho_segment_width(LC_SEGMENT_64, false), None);
        assert!(macho_commands_are_complete(16, 16, true));
        assert!(!macho_commands_are_complete(15, 16, true));
        assert!(!macho_commands_are_complete(16, 16, false));
        assert!(!macho_commands_are_complete(15, 16, false));
    }

    #[test]
    fn macho_parser_accepts_every_supported_optional_command() {
        let base = no_debug_macho(b"command payload");
        let mut linker_option = macho_command(LC_LINKER_OPTION, 16);
        write_le_u32(&mut linker_option, 8, 0);

        let commands = [
            macho_command(LC_DYSYMTAB, 80),
            macho_command(LC_BUILD_VERSION, 24),
            macho_command(LC_VERSION_MIN_MACOSX, 16),
            macho_command(LC_VERSION_MIN_IPHONEOS, 16),
            macho_command(LC_VERSION_MIN_TVOS, 16),
            macho_command(LC_VERSION_MIN_WATCHOS, 16),
            macho_command(LC_DATA_IN_CODE, 16),
            macho_command(LC_LINKER_OPTIMIZATION_HINT, 16),
            linker_option,
            macho_command(LC_SOURCE_VERSION, 16),
            macho_command(LC_UUID, 24),
        ];
        for command in commands {
            let object = macho_with_extra_command(base.clone(), &command);
            assert!(
                parse_known_no_debug_macho_object(&object).is_some(),
                "supported load command {} must parse",
                read_le_u32(&command, 0)
            );
        }
    }

    #[test]
    fn macho_parser_rejects_duplicate_and_wrong_sized_commands() {
        let base = no_debug_macho(b"command payload");
        let mut wrong_file_type = base.clone();
        write_le_u32(&mut wrong_file_type, 12, 2);
        assert!(parse_known_no_debug_macho_object(&wrong_file_type).is_none());

        let symtab_offset = 32 + usize::try_from(read_le_u32(&base, 36)).unwrap();
        let mut duplicate_symtab_command = base[symtab_offset..symtab_offset + 24].to_vec();
        for offset in [8, 16] {
            let shifted = read_le_u32(&duplicate_symtab_command, offset) + 24;
            write_le_u32(&mut duplicate_symtab_command, offset, shifted);
        }
        let duplicate_symtab = macho_with_extra_command(base.clone(), &duplicate_symtab_command);
        assert!(parse_known_no_debug_macho_object(&duplicate_symtab).is_none());

        let with_dysymtab = macho_with_extra_command(base.clone(), &macho_command(LC_DYSYMTAB, 80));
        let duplicate_dysymtab =
            macho_with_extra_command(with_dysymtab, &macho_command(LC_DYSYMTAB, 80));
        assert!(parse_known_no_debug_macho_object(&duplicate_dysymtab).is_none());

        for command in [
            macho_command(LC_SYMTAB, 16),
            macho_command(LC_DYSYMTAB, 72),
            macho_command(LC_VERSION_MIN_MACOSX, 24),
            macho_command(LC_SOURCE_VERSION, 24),
            macho_command(LC_UUID, 16),
        ] {
            let object = macho_with_extra_command(base.clone(), &command);
            assert!(parse_known_no_debug_macho_object(&object).is_none());
        }
    }

    fn macho_with_linkedit_command(command_id: u32, data_size: u32) -> Vec<u8> {
        let base = no_debug_macho(b"0123456789abcdef");
        let final_commands_end = 32 + usize::try_from(read_le_u32(&base, 20)).unwrap() + 16;
        let mut command = macho_command(command_id, 16);
        write_le_u32(&mut command, 8, u32::try_from(final_commands_end).unwrap());
        write_le_u32(&mut command, 12, data_size);
        macho_with_extra_command(base, &command)
    }

    #[test]
    fn macho_section_policy_checks_each_marker_independently() {
        assert!(macho_segment_is_portable(b"__TEXT"));
        for segment in [
            &b"__DWARF"[..],
            &b"__LLVM"[..],
            &b"__GNU_LTO"[..],
            &b"__GNU_OFFLD_LTO"[..],
        ] {
            assert!(!macho_segment_is_portable(segment));
        }

        for (segment, section, flags, expected) in [
            (&b"__TEXT"[..], &b"__text"[..], 0, true),
            (&b"__LD"[..], &b"__compact_unwind"[..], S_ATTR_DEBUG, true),
            (
                &b"__TEXT"[..],
                &b"__compact_unwind"[..],
                S_ATTR_DEBUG,
                false,
            ),
            (&b"__LD"[..], &b"__text"[..], S_ATTR_DEBUG, false),
            (&b"__TEXT"[..], &b"__text"[..], S_ATTR_DEBUG, false),
            (&b"__DWARF"[..], &b"__text"[..], 0, false),
            (&b"__TEXT"[..], &b"__debug_info"[..], 0, false),
            (&b"__TEXT"[..], &b"__zdebug_info"[..], 0, false),
            (&b"__LLVM"[..], &b"__text"[..], 0, false),
            (&b"__GNU_LTO"[..], &b"__text"[..], 0, false),
            (&b"__GNU_OFFLD_LTO"[..], &b"__text"[..], 0, false),
            (&b"__TEXT"[..], &b"__bitcode"[..], 0, false),
            (&b"__TEXT"[..], &b"__bundle"[..], 0, false),
        ] {
            assert_eq!(macho_section_is_portable(segment, section, flags), expected);
        }

        for section_type in [S_ZEROFILL, S_GB_ZEROFILL, S_THREAD_LOCAL_ZEROFILL] {
            assert!(is_macho_zerofill(section_type));
            assert!(is_macho_zerofill(section_type | S_ATTR_DEBUG));
        }
        for section_type in [0, 2, 3, SECTION_TYPE] {
            assert!(!is_macho_zerofill(section_type));
        }

        assert!(range_is_within((10, 20), (10, 20)));
        assert!(range_is_within((11, 19), (10, 20)));
        assert!(!range_is_within((9, 19), (10, 20)));
        assert!(!range_is_within((11, 21), (10, 20)));
    }

    #[test]
    fn macho_segment_ranges_relocations_and_zerofill_fail_closed() {
        const SEGMENT_FILEOFF: usize = 72;
        const SEGMENT_FILESIZE: usize = 80;
        const SECTION_SIZE: usize = 144;
        const SECTION_OFFSET: usize = 152;
        const SECTION_RELOFF: usize = 160;
        const SECTION_NRELOC: usize = 164;

        let base = no_debug_macho(b"code");
        let data_offset = read_le_u32(&base, SECTION_OFFSET);

        let mut starts_before_segment = base.clone();
        write_le_u64(
            &mut starts_before_segment,
            SEGMENT_FILEOFF,
            u64::from(data_offset + 1),
        );
        assert!(parse_known_no_debug_macho_object(&starts_before_segment).is_none());

        let mut ends_after_segment = base.clone();
        write_le_u64(
            &mut ends_after_segment,
            SEGMENT_FILEOFF,
            u64::from(data_offset - 1),
        );
        write_le_u64(&mut ends_after_segment, SEGMENT_FILESIZE, 4);
        assert!(parse_known_no_debug_macho_object(&ends_after_segment).is_none());

        let mut ordinary_out_of_bounds = base.clone();
        write_le_u32(&mut ordinary_out_of_bounds, SECTION_OFFSET, u32::MAX);
        assert!(parse_known_no_debug_macho_object(&ordinary_out_of_bounds).is_none());

        let mut zero_sized = ordinary_out_of_bounds.clone();
        write_le_u64(&mut zero_sized, SECTION_SIZE, 0);
        assert!(parse_known_no_debug_macho_object(&zero_sized).is_some());

        let mut zerofill = macho_object(
            MachEndian::Little,
            true,
            "__DATA",
            "__bss",
            S_ZEROFILL,
            N_SECT,
            b"code",
        );
        write_le_u32(&mut zerofill, SECTION_OFFSET, u32::MAX);
        assert!(parse_known_no_debug_macho_object(&zerofill).is_some());

        let mut bad_relocation = base;
        write_le_u32(&mut bad_relocation, SECTION_RELOFF, u32::MAX);
        write_le_u32(&mut bad_relocation, SECTION_NRELOC, 1);
        assert!(parse_known_no_debug_macho_object(&bad_relocation).is_none());
    }

    #[test]
    fn data_in_code_alignment_applies_only_to_that_command() {
        assert!(
            parse_known_no_debug_macho_object(&macho_with_linkedit_command(LC_DATA_IN_CODE, 0))
                .is_some()
        );
        assert!(
            parse_known_no_debug_macho_object(&macho_with_linkedit_command(LC_DATA_IN_CODE, 8))
                .is_some()
        );
        assert!(
            parse_known_no_debug_macho_object(&macho_with_linkedit_command(LC_DATA_IN_CODE, 7))
                .is_none()
        );
        assert!(
            parse_known_no_debug_macho_object(&macho_with_linkedit_command(
                LC_LINKER_OPTIMIZATION_HINT,
                7,
            ))
            .is_some()
        );
    }

    fn symtab_fixture(
        symbol_type: u8,
        section: u8,
        string_index: u32,
        strings: &[u8],
    ) -> (Vec<u8>, MachSymtab) {
        let mut bytes = vec![0_u8; 8];
        let symoff = u32::try_from(bytes.len()).unwrap();
        let mut symbol = [0_u8; 16];
        symbol[..4].copy_from_slice(&string_index.to_le_bytes());
        symbol[4] = symbol_type;
        symbol[5] = section;
        bytes.extend_from_slice(&symbol);
        let stroff = u32::try_from(bytes.len()).unwrap();
        bytes.extend_from_slice(strings);
        (
            bytes,
            MachSymtab {
                symoff,
                nsyms: 1,
                stroff,
                strsize: u32::try_from(strings.len()).unwrap(),
            },
        )
    }

    #[test]
    fn macho_symbol_section_and_name_rules_cover_each_boundary() {
        assert!(macho_symbol_section_is_valid(N_SECT, 1, 1));
        assert!(!macho_symbol_section_is_valid(N_SECT, 0, 1));
        assert!(!macho_symbol_section_is_valid(N_SECT, 2, 1));
        assert!(macho_symbol_section_is_valid(0, 0, 1));

        assert!(macho_symbol_name_is_valid(b"\0name\0", 0));
        assert!(macho_symbol_name_is_valid(b"\0name\0", 1));
        assert!(!macho_symbol_name_is_valid(b"\0name", 1));
        assert!(!macho_symbol_name_is_valid(b"\0name\0", 6));
        assert!(!macho_symbol_name_is_valid(b"\0name\0", usize::MAX));
    }

    #[test]
    fn macho_symtab_validator_rejects_each_malformed_field_independently() {
        let (valid_bytes, valid) = symtab_fixture(N_SECT, 1, 1, b"\0name\0");
        assert!(
            validate_macho_symtab(&valid_bytes, MachEndian::Little, true, 8, 1, valid).is_some()
        );

        for (symbol_type, section, string_index, strings) in [
            (N_SECT, 0, 1, &b"\0name\0"[..]),
            (N_SECT, 2, 1, &b"\0name\0"[..]),
            (0x64, 0, 1, &b"\0name\0"[..]),
            (N_SECT, 1, 0, &b"name\0"[..]),
            (N_SECT, 1, 1, &b"\0name"[..]),
            (N_SECT, 1, 6, &b"\0name\0"[..]),
        ] {
            let (bytes, symtab) = symtab_fixture(symbol_type, section, string_index, strings);
            assert!(
                validate_macho_symtab(&bytes, MachEndian::Little, true, 8, 1, symtab).is_none()
            );
        }

        let (bytes, mut empty) = symtab_fixture(0, 0, 0, b"");
        empty.strsize = 0;
        assert!(validate_macho_symtab(&bytes, MachEndian::Little, true, 8, 1, empty).is_none());
    }

    #[test]
    fn macho_leaf_validators_cover_success_failure_and_boundaries() {
        let bytes = vec![0_u8; 256];
        let fields = [0_u32; 18];
        assert!(validate_macho_dysymtab(&bytes, true, 16, 0, &fields).is_some());

        let mut invalid_symbols = fields;
        invalid_symbols[0] = 1;
        invalid_symbols[1] = 1;
        assert!(validate_macho_dysymtab(&bytes, true, 16, 1, &invalid_symbols).is_none());
        invalid_symbols[0] = u32::MAX;
        assert!(validate_macho_dysymtab(&bytes, true, 16, 1, &invalid_symbols).is_none());

        for (offset_index, count_index) in [(6, 7), (8, 9), (10, 11), (12, 13), (14, 15), (16, 17)]
        {
            let mut valid_region = fields;
            valid_region[offset_index] = 16;
            valid_region[count_index] = 1;
            assert!(validate_macho_dysymtab(&bytes, true, 16, 0, &valid_region).is_some());

            let mut overlap = valid_region;
            overlap[offset_index] = 15;
            assert!(validate_macho_dysymtab(&bytes, true, 16, 0, &overlap).is_none());
        }

        let build_zero = macho_command(LC_BUILD_VERSION, 24);
        assert!(validate_build_version(&build_zero, MachEndian::Little).is_some());
        let mut build_one = macho_command(LC_BUILD_VERSION, 32);
        write_le_u32(&mut build_one, 20, 1);
        assert!(validate_build_version(&build_one, MachEndian::Little).is_some());
        let mut missing_tool = macho_command(LC_BUILD_VERSION, 24);
        write_le_u32(&mut missing_tool, 20, 1);
        assert!(validate_build_version(&missing_tool, MachEndian::Little).is_none());
        let extra_tool = macho_command(LC_BUILD_VERSION, 32);
        assert!(validate_build_version(&extra_tool, MachEndian::Little).is_none());
        assert!(validate_build_version(&build_zero[..20], MachEndian::Little).is_none());

        let mut linkedit = macho_command(LC_DATA_IN_CODE, 16);
        write_le_u32(&mut linkedit, 8, 16);
        write_le_u32(&mut linkedit, 12, 8);
        assert!(validate_linkedit_data(&bytes, &linkedit, MachEndian::Little, 16).is_some());
        let wrong_linkedit_len = macho_command(LC_DATA_IN_CODE, 24);
        assert!(
            validate_linkedit_data(&bytes, &wrong_linkedit_len, MachEndian::Little, 16).is_none()
        );
        write_le_u32(&mut linkedit, 8, 15);
        assert!(validate_linkedit_data(&bytes, &linkedit, MachEndian::Little, 16).is_none());
        write_le_u32(&mut linkedit, 8, 256);
        write_le_u32(&mut linkedit, 12, 1);
        assert!(validate_linkedit_data(&bytes, &linkedit, MachEndian::Little, 16).is_none());

        assert!(validate_counted_region(&bytes, u32::MAX, 0, 8, 16).is_some());
        assert!(validate_counted_region(&bytes, 16, 2, 8, 16).is_some());
        assert!(validate_counted_region(&bytes, 15, 1, 8, 16).is_none());
        assert!(validate_counted_region(&bytes, 16, 2, u64::MAX, 16).is_none());

        assert_eq!(checked_file_region(32, 8, 8, 8), Some((8, 16)));
        assert_eq!(checked_file_region(32, 0, 0, 8), Some((0, 0)));
        assert_eq!(checked_file_region(32, 7, 1, 8), None);
        assert_eq!(checked_file_region(32, 24, 8, 8), Some((24, 32)));
        assert_eq!(checked_file_region(32, 25, 8, 8), None);
        assert_eq!(checked_file_region(32, u64::MAX, 1, 8), None);
    }

    #[test]
    fn linker_option_validator_consumes_exactly_the_declared_strings() {
        let count_zero = macho_command(LC_LINKER_OPTION, 12);
        assert!(validate_linker_option(&count_zero, MachEndian::Little).is_some());
        let padded_zero = macho_command(LC_LINKER_OPTION, 16);
        assert!(validate_linker_option(&padded_zero, MachEndian::Little).is_some());

        let mut one = macho_command(LC_LINKER_OPTION, 16);
        write_le_u32(&mut one, 8, 1);
        one[12..14].copy_from_slice(b"a\0");
        assert!(validate_linker_option(&one, MachEndian::Little).is_some());

        let mut two = macho_command(LC_LINKER_OPTION, 24);
        write_le_u32(&mut two, 8, 2);
        two[12..18].copy_from_slice(b"ab\0cd\0");
        assert!(validate_linker_option(&two, MachEndian::Little).is_some());

        let mut empty = macho_command(LC_LINKER_OPTION, 16);
        write_le_u32(&mut empty, 8, 1);
        assert!(validate_linker_option(&empty, MachEndian::Little).is_none());

        let mut unterminated = macho_command(LC_LINKER_OPTION, 16);
        write_le_u32(&mut unterminated, 8, 1);
        unterminated[12..].fill(b'x');
        assert!(validate_linker_option(&unterminated, MachEndian::Little).is_none());

        let mut missing_second = macho_command(LC_LINKER_OPTION, 16);
        write_le_u32(&mut missing_second, 8, 2);
        missing_second[12..14].copy_from_slice(b"a\0");
        assert!(validate_linker_option(&missing_second, MachEndian::Little).is_none());

        let mut trailing = one;
        trailing[14] = b'x';
        assert!(validate_linker_option(&trailing, MachEndian::Little).is_none());
        assert!(validate_linker_option(&count_zero[..11], MachEndian::Little).is_none());
    }

    // A realistic Darwin ranlib payload shape (its exact bytes don't matter to
    // the parser; only that it is stable across clones, which it is when the
    // inline-name lengths — and thus its stored offsets — are equal).
    const BSD_SYMDEF: &[u8] =
        b"\x10\x00\x00\x00\x00\x00\x00\x00\x88\x00\x00\x00\x08\x00\x00\x00foo\0bar\0";

    #[test]
    fn bsd_identical_content_different_member_names_hash_differ() {
        // ld64 and downstream rlibs can observe the exact inline member name.
        let obj1 = no_debug_macho(b"object-one-contents");
        let obj2 = no_debug_macho(b"object-two-contents!");
        let clone_a = bsd_archive(&[
            ("__.SYMDEF SORTED", 20, BSD_SYMDEF),
            ("cafca65b3467684e-a.o", 20, &obj1),
            ("cafca65b3467684e-b.o", 20, &obj2),
        ]);
        let clone_b = bsd_archive(&[
            ("__.SYMDEF SORTED", 20, BSD_SYMDEF),
            ("4af22b2a007cb61a-a.o", 20, &obj1),
            ("4af22b2a007cb61a-b.o", 20, &obj2),
        ]);
        let ha = portable_static_archive_hash(&clone_a).expect("clone-a parses");
        let hb = portable_static_archive_hash(&clone_b).expect("clone-b parses");
        assert_ne!(ha, hb, "inline member names are part of archive identity");
    }

    #[test]
    fn bsd_changed_object_content_changes_hash() {
        // #421 must be preserved on the BSD arm too: an in-place object-byte
        // change (same path, same names) re-keys.
        let object_a = no_debug_macho(b"object-vONE");
        let object_b = no_debug_macho(b"object-vTWO");
        let a = bsd_archive(&[("x.o", 4, &object_a)]);
        let b = bsd_archive(&[("x.o", 4, &object_b)]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn bsd_member_order_changes_hash() {
        // Order is link-significant; reordering members must re-key.
        let object_a = no_debug_macho(b"aaaa");
        let object_b = no_debug_macho(b"bbbb");
        let a = bsd_archive(&[("a.o", 4, &object_a), ("b.o", 4, &object_b)]);
        let b = bsd_archive(&[("a.o", 4, &object_b), ("b.o", 4, &object_a)]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn bsd_ranlib_content_change_changes_hash() {
        // A stale/crafted ranlib that disagrees with the members must NOT
        // collide with one that links differently — the payload is hashed
        // raw (like the GNU symtab), not dropped and not reduced to a length.
        let mut crafted = BSD_SYMDEF.to_vec();
        let last = crafted.len() - 1;
        crafted[last] ^= 0xff; // same length, different bytes
        let object = no_debug_macho(b"obj-data");
        let a = bsd_archive(&[("__.SYMDEF", 12, BSD_SYMDEF), ("x.o", 4, &object)]);
        let b = bsd_archive(&[("__.SYMDEF", 12, &crafted), ("x.o", 4, &object)]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn bsd_ranlib_variant_changes_hash() {
        // Same payload bytes, same inline padding — only the variant name
        // differs. `_64` reads the bytes with a different word width and
        // ` SORTED` changes the lookup contract, so each must re-key.
        let object = no_debug_macho(b"obj-data");
        let base = bsd_archive(&[("__.SYMDEF", 20, BSD_SYMDEF), ("x.o", 4, &object)]);
        for other in ["__.SYMDEF_64", "__.SYMDEF SORTED", "__.SYMDEF_64 SORTED"] {
            let variant = bsd_archive(&[(other, 20, BSD_SYMDEF), ("x.o", 4, &object)]);
            assert_ne!(
                portable_static_archive_hash(&base).unwrap(),
                portable_static_archive_hash(&variant).unwrap(),
                "{other} must not collide with __.SYMDEF",
            );
        }
    }

    #[test]
    fn bsd_inline_name_length_change_changes_hash() {
        // Inline-name LENGTHS shift the absolute member offsets the ranlib
        // stores (the BSD analog of the GNU `//`-length rule): same content,
        // different padded name length must re-key. Exact stored bytes and
        // length are both identity-bearing.
        let object = no_debug_macho(b"obj-data");
        let a = bsd_archive(&[("x.o", 4, &object)]);
        let b = bsd_archive(&[("x.o", 8, &object)]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn bsd_short_header_names_parse() {
        // Names ≤ 15 chars may sit directly in the header field (older ar);
        // they occupy zero inline bytes. Mixed with `#1/N` members it is
        // still one BSD archive.
        let mut a = AR_MAGIC.to_vec();
        a.extend_from_slice(&bsd_member("__.SYMDEF", 12, BSD_SYMDEF));
        a.extend_from_slice(&member("cutils.o", &no_debug_macho(b"obj-data")));
        let h = portable_static_archive_hash(&a).expect("short-name BSD archive parses");
        assert!(h.starts_with("bsd-ar-v2:"));
    }

    #[test]
    fn bsd_output_is_domain_tagged() {
        // Textually distinct from BOTH the whole-file fallback and the GNU
        // scheme — a BSD-parsed archive never hashes equal to a GNU-parsed
        // one (rustc bundles the archive BYTES, so format is content).
        let object = no_debug_macho(b"obj-data");
        let a = bsd_archive(&[("x.o", 4, &object)]);
        assert!(
            portable_static_archive_hash(&a)
                .unwrap()
                .starts_with("bsd-ar-v2:")
        );
    }

    #[test]
    fn macho_gate_accepts_32_and_64_bit_both_endians() {
        for endian in [MachEndian::Little, MachEndian::Big] {
            for is_64 in [false, true] {
                let object = macho_object(endian, is_64, "__TEXT", "__text", 0, N_SECT, b"code");
                assert!(
                    is_known_no_debug_macho_object(&object),
                    "supported MH_OBJECT must pass its endian/width gate"
                );
            }
        }

        let compact_unwind = macho_object(
            MachEndian::Little,
            true,
            "__LD",
            "__compact_unwind",
            S_ATTR_DEBUG,
            N_SECT,
            b"unwind",
        );
        assert!(is_known_no_debug_macho_object(&compact_unwind));
    }

    #[test]
    fn arm64_32_object_uses_path_bound_fallback() {
        let mut object = macho_object(
            MachEndian::Little,
            true,
            "__TEXT",
            "__text",
            0,
            N_SECT,
            b"code",
        );
        object[4..8].copy_from_slice(&0x0200_000c_u32.to_le_bytes());
        assert!(!is_known_no_debug_macho_object(&object));

        let archive = bsd_archive(&[("PerfUtils.o", 16, &object)]);
        assert!(portable_static_archive_hash(&archive).is_none());
    }

    #[test]
    fn bsd_debug_sections_and_stabs_fall_back() {
        let debug_section = macho_object(
            MachEndian::Little,
            true,
            "__DWARF",
            "__debug_info",
            S_ATTR_DEBUG,
            N_SECT,
            b"debug",
        );
        let stab = macho_object(
            MachEndian::Little,
            true,
            "__TEXT",
            "__text",
            0,
            0x64, // N_SO: any N_STAB entry is name/time-sensitive in ld64
            b"code",
        );
        for object in [&debug_section, &stab] {
            let archive = bsd_archive(&[("derived-name.o", 16, object)]);
            assert!(portable_static_archive_hash(&archive).is_none());
        }

        // A GNU-layout archive can carry Mach-O too; its `//` names are just as
        // capable of entering ld64's N_OSO entry and must use the same guard.
        let gnu_debug = archive(&[("debug.o/", &debug_section)]);
        assert!(portable_static_archive_hash(&gnu_debug).is_none());
    }

    #[test]
    fn bsd_bitcode_unknown_and_malformed_objects_fall_back() {
        let bitcode = macho_object(
            MachEndian::Little,
            true,
            "__LLVM",
            "__bitcode",
            0,
            N_SECT,
            b"bitcode",
        );
        let gcc_lto = macho_object(
            MachEndian::Little,
            true,
            "__GNU_LTO",
            "__lto",
            0,
            N_SECT,
            b"gcc lto",
        );
        let gcc_offload_lto = macho_object(
            MachEndian::Little,
            true,
            "__GNU_OFFLD_LTO",
            "__offload",
            0,
            N_SECT,
            b"gcc offload lto",
        );
        let unknown = b"BC\xc0\xde-not-a-mach-o-object";
        let mut malformed = no_debug_macho(b"code");
        malformed[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        for object in [
            &bitcode[..],
            &gcc_lto[..],
            &gcc_offload_lto[..],
            &unknown[..],
            &malformed[..],
        ] {
            let archive = bsd_archive(&[("derived-name.o", 16, object)]);
            assert!(portable_static_archive_hash(&archive).is_none());
            let gnu_archive = raw_archive(&[("derived-name.o/", object)]);
            assert!(portable_static_archive_hash(&gnu_archive).is_none());
        }
    }

    #[test]
    fn gnu_bitcode_lto_and_unknown_objects_fall_back() {
        let raw_bitcode = b"BC\xc0\xde-raw-bitcode";
        let wrapped_bitcode = b"\xde\xc0\x17\x0b-wrapped-bitcode";
        let unknown = b"not-a-known-object-format";
        let elf_bitcode = elf_object_with_section(b".llvmbc", b"bitcode");
        let llvm_command = elf_object_with_section(b".llvmcmd", b"bitcode");
        let llvm_lto = elf_object_with_section(b".llvm.lto", b"bitcode");
        let gnu_lto = elf_object_with_section(b".gnu.lto_.opts", b"bitcode");
        let gnu_offload_lto = elf_object_with_section(b".gnu.offload_lto_.opts", b"bitcode");
        let llvm_offloading = elf_object_with_section(b".llvm.offloading", b"bitcode");
        let llvm_offloading_type =
            elf_object_with_section_type(b".data", SHT_LLVM_OFFLOADING, b"bitcode");
        let llvm_lto_type = elf_object_with_section_type(b".data", SHT_LLVM_LTO, b"bitcode");

        for object in [
            &raw_bitcode[..],
            &wrapped_bitcode[..],
            &unknown[..],
            &elf_bitcode[..],
            &llvm_command[..],
            &llvm_lto[..],
            &gnu_lto[..],
            &gnu_offload_lto[..],
            &llvm_offloading[..],
            &llvm_offloading_type[..],
            &llvm_lto_type[..],
        ] {
            let archive = raw_archive(&[("derived-name.o/", object)]);
            assert!(portable_static_archive_hash(&archive).is_none());
        }
    }

    #[test]
    fn gnu_elf_gate_checks_shape_and_bounds() {
        let elf32be = elf32be_object(b"ordinary object");
        assert!(portable_static_archive_hash(&raw_archive(&[("sparc.o/", &elf32be)])).is_some());

        let mut executable = elf_object(b"ordinary object");
        executable[16..18].copy_from_slice(&2_u16.to_le_bytes()); // ET_EXEC
        let mut mismatched_machine = elf_object(b"ordinary object");
        mismatched_machine[18..20].copy_from_slice(&3_u16.to_le_bytes()); // EM_386 + ELF64
        let mut malformed_boundary = elf_object(b"ordinary object");
        malformed_boundary.pop();
        for object in [executable, mismatched_machine, malformed_boundary] {
            assert!(portable_static_archive_hash(&raw_archive(&[("bad.o/", &object)])).is_none());
        }
    }

    #[test]
    fn bsd_symdef_with_slash_terminated_member_falls_back_as_mixed() {
        let object = no_debug_macho(b"obj-data");
        let mut archive = AR_MAGIC.to_vec();
        archive.extend_from_slice(&bsd_member("__.SYMDEF", 12, BSD_SYMDEF));
        archive.extend_from_slice(&member("object.o/", &object));
        assert!(portable_static_archive_hash(&archive).is_none());
    }

    #[test]
    fn bsd_ranlib_not_first_falls_back() {
        // Darwin ranlib always writes the symbol table first; anywhere else
        // is not a plain Darwin archive.
        let object = no_debug_macho(b"obj-data");
        let a = bsd_archive(&[("x.o", 4, &object), ("__.SYMDEF", 12, BSD_SYMDEF)]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn bsd_second_ranlib_falls_back() {
        let object = no_debug_macho(b"obj-data");
        let a = bsd_archive(&[
            ("__.SYMDEF", 12, BSD_SYMDEF),
            ("__.SYMDEF", 12, BSD_SYMDEF),
            ("x.o", 4, &object),
        ]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn bsd_ranlib_only_falls_back() {
        // No object members is degenerate; be conservative (mirror GNU).
        let a = bsd_archive(&[("__.SYMDEF", 12, BSD_SYMDEF)]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn bsd_malformed_falls_back() {
        // Inline-name length exceeding the member size (which includes it).
        let mut long_name = AR_MAGIC.to_vec();
        long_name
            .extend_from_slice(b"#1/40           0           0     0     100644  10        `\n");
        long_name.extend_from_slice(b"0123456789");
        assert!(portable_static_archive_hash(&long_name).is_none());

        // Non-decimal inline-name length.
        let mut nondec = AR_MAGIC.to_vec();
        nondec.extend_from_slice(b"#1/1x           0           0     0     100644  10        `\n");
        nondec.extend_from_slice(b"0123456789");
        assert!(portable_static_archive_hash(&nondec).is_none());

        // Truncated mid-data (size says more than is present).
        let mut trunc = AR_MAGIC.to_vec();
        trunc.extend_from_slice(b"#1/4            0           0     0     100644  9999      `\n");
        trunc.extend_from_slice(b"x.o\0short");
        assert!(portable_static_archive_hash(&trunc).is_none());

        // Trailing garbage after the last member.
        let mut trailing = bsd_archive(&[("x.o", 4, b"obj-data")]);
        trailing.push(b'X');
        assert!(portable_static_archive_hash(&trailing).is_none());
    }

    #[test]
    fn bsd_odd_sized_members_parse() {
        // Odd member sizes are '\n'-padded to the next even offset; two in a
        // row proves the padding arithmetic.
        let object_a = no_debug_macho(b"odd");
        let object_b = no_debug_macho(b"data!");
        // Five bytes of inline-name storage makes both member sizes odd.
        let a = bsd_archive(&[("a.o", 5, &object_a), ("b.o", 5, &object_b)]);
        assert!(portable_static_archive_hash(&a).is_some());
    }

    /// Drive the system compiler and BSD `ar` on macOS. Real no-debug Mach-O
    /// members make `ar crs` emit both the `#1/N` names and ranlib table that
    /// #691 needs; distinct derived names must remain distinct.
    #[test]
    #[cfg(target_os = "macos")]
    fn real_darwin_ar_identical_content_different_names_hash_differ() {
        use std::process::Command;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("probe.c");
        let seed_object = dir.path().join("seed.o");
        std::fs::write(&source, b"int kache_archive_probe(void) { return 701; }\n").unwrap();
        let status = Command::new("cc")
            .arg("-c")
            .arg("-g0")
            .arg("-O0")
            .arg(&source)
            .arg("-o")
            .arg(&seed_object)
            .status()
            .expect("system C compiler runs");
        assert!(status.success(), "cc -c -g0 failed");
        assert!(is_known_no_debug_macho_object(
            &std::fs::read(&seed_object).unwrap()
        ));

        let mut digests = Vec::new();
        for prefix in ["cafca65b3467684e", "4af22b2a007cb61a"] {
            let sub = dir.path().join(prefix);
            std::fs::create_dir(&sub).unwrap();
            let lib = sub.join("libprobe.a");
            // >15 equal-width bytes force the `#1/N` inline-name encoding.
            let object = sub.join(format!("{prefix}-probe.o"));
            std::fs::copy(&seed_object, &object).unwrap();
            let status = Command::new("ar")
                .arg("crs")
                .arg(&lib)
                .arg(&object)
                .env("ZERO_AR_DATE", "1")
                .status()
                .expect("system ar runs");
            assert!(status.success(), "ar crs failed");
            let bytes = std::fs::read(&lib).unwrap();
            assert!(
                bytes
                    .windows(b"__.SYMDEF".len())
                    .any(|window| window == b"__.SYMDEF"),
                "ar crs must emit a ranlib member"
            );
            assert!(
                bytes.windows(3).any(|window| window == b"#1/"),
                "system BSD ar must use inline member names"
            );
            digests.push(bsd_archive_hash(&bytes).expect("BSD arm must claim the archive"));
        }
        assert_ne!(
            digests[0], digests[1],
            "path-derived member names remain linker-visible"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn real_darwin_debug_object_falls_back() {
        use std::process::Command;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("probe.c");
        let object = dir.path().join("cafca65b3467684e-debug.o");
        let archive = dir.path().join("libprobe.a");
        std::fs::write(
            &source,
            b"int kache_archive_debug_probe(void) { return 701; }\n",
        )
        .unwrap();
        let status = Command::new("cc")
            .arg("-c")
            .arg("-g")
            .arg("-O0")
            .arg(&source)
            .arg("-o")
            .arg(&object)
            .status()
            .expect("system C compiler runs");
        assert!(status.success(), "cc -c -g failed");
        assert!(
            !is_known_no_debug_macho_object(&std::fs::read(&object).unwrap()),
            "debug-bearing Mach-O must fail the name-normalization gate"
        );

        let status = Command::new("ar")
            .arg("crs")
            .arg(&archive)
            .arg(&object)
            .env("ZERO_AR_DATE", "1")
            .status()
            .expect("system ar runs");
        assert!(status.success(), "ar crs failed");
        let bytes = std::fs::read(&archive).unwrap();
        assert!(bsd_archive_hash(&bytes).is_none());
        assert!(portable_static_archive_hash(&bytes).is_none());
    }

    fn assert_portable_hash_shape(hash: &str) {
        let digest = hash
            .strip_prefix("gnu-ar-v2:")
            .or_else(|| hash.strip_prefix("bsd-ar-v2:"))
            .expect("portable archive hash has a known domain tag");
        assert_eq!(digest.len(), 64, "portable archive hash is BLAKE3-sized");
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "portable archive digest is lowercase hexadecimal"
        );
    }

    /// Every minimized fuzz failure is committed here and replayed by ordinary
    /// stable `cargo test`, so a regression does not depend on nightly or
    /// libFuzzer being available in pull-request CI.
    #[test]
    fn fuzz_regression_corpus_is_total_and_deterministic() {
        let corpus =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/native_archive_fuzz");
        let mut cases = std::fs::read_dir(&corpus)
            .unwrap_or_else(|error| panic!("read {}: {error}", corpus.display()))
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        cases.sort();
        assert!(
            !cases.is_empty(),
            "native archive regression corpus is empty"
        );

        for case in cases {
            let bytes = std::fs::read(&case).unwrap();
            let first = portable_static_archive_hash(&bytes);
            let second = portable_static_archive_hash(&bytes);
            assert_eq!(
                first,
                second,
                "non-deterministic result for {}",
                case.display()
            );
            if let Some(hash) = first.as_deref() {
                assert_portable_hash_shape(hash);
            }
        }
    }

    /// Export structurally deep fixtures into a temporary libFuzzer seed
    /// corpus. The generated files are intentionally not committed: their
    /// source-of-truth builders already live beside the parser tests.
    #[test]
    #[ignore = "seed generator for cargo-fuzz"]
    fn emit_fuzz_seed_corpus() {
        let output = PathBuf::from(
            std::env::var_os("KACHE_FUZZ_SEED_DIR")
                .expect("KACHE_FUZZ_SEED_DIR must name the output directory"),
        );
        std::fs::create_dir_all(&output).unwrap();

        let elf32be = elf32be_object(b"big-endian seed payload");
        let mut seeds = vec![
            (
                "gnu-short.a".to_string(),
                archive(&[("/", SYMTAB), ("seed.o/", b"seed payload")]),
                true,
            ),
            (
                "gnu-long-name.a".to_string(),
                archive(&[
                    ("/", SYMTAB),
                    ("//", b"very-long-object-name.o/\n"),
                    ("/0", b"long-name seed payload"),
                ]),
                true,
            ),
            (
                "gnu-elf32be.a".to_string(),
                raw_archive(&[("sparc.o/", elf32be.as_slice())]),
                true,
            ),
        ];

        for (endian_name, endian) in [("little", MachEndian::Little), ("big", MachEndian::Big)] {
            for width in [32_u8, 64] {
                let object = macho_object(
                    endian,
                    width == 64,
                    "__TEXT",
                    "__text",
                    0,
                    N_SECT,
                    b"Mach-O seed payload",
                );
                seeds.push((
                    format!("bsd-{endian_name}-{width}.a"),
                    bsd_archive(&[("seed.o", 16, object.as_slice())]),
                    true,
                ));
            }
        }
        seeds.push((
            "truncated-member.a".to_string(),
            b"!<arch>\nX".to_vec(),
            false,
        ));

        for (name, bytes, should_parse) in seeds {
            let parsed = portable_static_archive_hash(&bytes);
            assert_eq!(
                parsed.is_some(),
                should_parse,
                "seed {name} did not exercise the expected parser path"
            );
            if let Some(hash) = parsed.as_deref() {
                assert_portable_hash_shape(hash);
            }
            std::fs::write(output.join(name), bytes).unwrap();
        }
    }
}
