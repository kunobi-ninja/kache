//! Build-path-portable content hashing of linked `static=` native archives.
//!
//! rustc bundles a `-l static=NAME` archive INTO its output, so kache folds the
//! archive's content into the cache key to catch an in-place rebuild — same
//! name, same path, different bytes (kunobi-ninja/kache#421). Hashing the whole
//! `.a` file is correct for that, but NOT portable across build directories: the
//! `cc` crate names each archive member with a hash derived from the absolute
//! build path (e.g. `cafca65b3467684e-quickjs.o` in one checkout vs
//! `4af22b2a007cb61a-quickjs.o` in another), while the object member CONTENT is
//! byte-identical. The whole-file hash therefore diverges across clones / CI
//! machines even when the linked content is identical (kunobi-ninja/kache#471),
//! cross-clone-missing the lib and everything downstream of it.
//!
//! [`portable_static_archive_hash`] hashes the archive's link-relevant content
//! while ignoring those path-derived member names, for the GNU / SysV `ar`
//! format. It is deliberately **fail-closed**: anything it cannot parse as a
//! plain (non-thin) GNU archive returns `None` and the caller falls back to the
//! whole-file hash — so it can never produce a *wrong* (colliding) hash, only a
//! less-portable one.
//!
//! ## What is hashed (GNU archives)
//! - every object member's DATA bytes, length-framed, **in archive order**
//!   (member order can be link-significant — duplicate symbols, `--whole-archive`
//!   — so it is never sorted);
//! - the symbol-table member (`/`, `/SYM64/`) DATA **as-is**. For GNU, member
//!   names live in the `//` long-name table (fixed-width `cc` name hashes keep
//!   that table the same SIZE across clones), so member-header offsets — and
//!   thus the symbol table's bytes — are already identical across clones. Hashing
//!   it raw is therefore portable AND keeps a stale/crafted symbol table from
//!   colliding with one that links differently (it is NOT dropped).
//!
//! The `//` long-name table's CONTENT (the path-derived names) is ignored, but
//! its LENGTH is folded in: the table's size shifts the absolute member offsets
//! the symbol table stores, so a length change must re-key (otherwise a stale or
//! crafted symbol table could point at a different member yet collide). The
//! parser also enforces the canonical GNU layout — at most one symbol table as
//! the first member, at most one `//` table immediately after — and falls back
//! on anything else (e.g. COFF `.lib`'s two `/` linker members).
//!
//! ## What is ignored
//! - the `//` long-name table CONTENT (the path-derived `cc` names);
//! - every member-header field (name, mtime, uid, gid, mode) — none affect
//!   linking; the name is the `cc` path-hash we are normalizing away.
//!
//! ## Out of scope -> whole-file fallback (`None`)
//! BSD / macOS archives (`#1/N` inline names, `__.SYMDEF`), thin archives
//! (`!<thin>`), Windows COFF `.lib`, and anything malformed. These keep today's
//! whole-file behavior (correct, just not yet cross-clone-portable). The BSD
//! symbol table embeds member offsets that shift with the inline name lengths,
//! so it needs offset->ordinal normalization to be portable — deferred; see #471.

const AR_MAGIC: &[u8; 8] = b"!<arch>\n";
const AR_HEADER_LEN: usize = 60;
/// Domain tag so this scheme can never collide with a plain whole-file blake3
/// (the fallback) or any other key input. Bump the trailing version if the
/// hashed-content definition ever changes (also bump `CACHE_KEY_VERSION`).
const DOMAIN: &[u8] = b"kache.native-ar.gnu.member-content.v1\0";

/// Portable content hash of a GNU `ar` static archive, or `None` to signal the
/// caller should fall back to a whole-file hash. See the module docs.
pub fn portable_static_archive_hash(bytes: &[u8]) -> Option<String> {
    // `!<thin>\n` and non-archives fail this check -> fallback.
    if bytes.len() < AR_MAGIC.len() || &bytes[..AR_MAGIC.len()] != AR_MAGIC {
        return None;
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    let mut pos = AR_MAGIC.len();
    let mut member_index: usize = 0;
    let mut seen_symtab = false;
    let mut seen_longnames = false;
    let mut object_members: u64 = 0;

    while pos < bytes.len() {
        let header = bytes.get(pos..pos.checked_add(AR_HEADER_LEN)?)?;
        // Header terminator must be "`\n" — a strict gate against misalignment.
        if &header[58..60] != b"`\n" {
            return None;
        }
        let name = ar_name(&header[0..16]);
        let size = parse_ar_decimal(&header[48..58])?;

        let data_start = pos + AR_HEADER_LEN;
        let data_end = data_start.checked_add(size)?;
        let data = bytes.get(data_start..data_end)?;

        // BSD/macOS/Darwin64 markers -> not a GNU archive -> fallback (never
        // guess). `__.SYMDEF_64 SORTED` (19 chars) cannot fit the 16-byte name
        // field — such Darwin64 archives use the `#1/` extended-name encoding,
        // already rejected above — so it is not listed.
        if name.starts_with("#1/")
            || name == "__.SYMDEF"
            || name == "__.SYMDEF SORTED"
            || name == "__.SYMDEF_64"
        {
            return None;
        }

        match classify(&name) {
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
                // The `//` table's CONTENT is the cc path-derived names, which we
                // ignore. But its LENGTH and position shift the absolute member
                // offsets the symbol table stores, so a `//`-length change must
                // re-key (else a stale/crafted symtab could point at a different
                // member yet collide — codex review #471). Hash its length, not
                // its bytes. It appears once, right after the optional symtab.
                let allowed = usize::from(seen_symtab);
                if seen_longnames || member_index != allowed {
                    return None;
                }
                seen_longnames = true;
                hasher.update(b"longnames\0");
                hasher.update(&(data.len() as u64).to_le_bytes());
            }
            Member::Object => {
                hasher.update(b"member\0");
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
    Some(format!("gnu-ar-v1:{}", hasher.finalize().to_hex()))
}

enum Member {
    SymbolTable,
    LongNameTable,
    Object,
}

/// Classify a GNU member by its (space-trimmed) name field. Only the exact
/// reserved names are special; a `/123` long-name reference or a `name/` short
/// name is an ordinary object member (whose name we ignore anyway).
fn classify(name: &str) -> Member {
    match name {
        "/" | "/SYM64/" => Member::SymbolTable,
        "//" => Member::LongNameTable,
        _ => Member::Object,
    }
}

/// The raw 16-byte name field with trailing ASCII spaces removed. (We only ever
/// compare it against reserved markers; object names are otherwise ignored.)
fn ar_name(field: &[u8]) -> String {
    let end = field.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    String::from_utf8_lossy(&field[..end]).into_owned()
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

    fn archive(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut a = AR_MAGIC.to_vec();
        for (n, d) in members {
            a.extend_from_slice(&member(n, d));
        }
        a
    }

    // A realistic GNU symbol-table payload (its exact bytes don't matter to the
    // parser; only that it is stable across clones, which it is for GNU).
    const SYMTAB: &[u8] = b"\x00\x00\x00\x01\x00\x00\x00\x68foo\x00";

    #[test]
    fn identical_content_different_member_names_hash_equal() {
        // The #471 case: two clones, byte-identical objects + symbol table, but
        // the `//` long-name table holds different (same-length) cc path hashes,
        // and the header name refs differ. Must hash EQUAL.
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
        assert_eq!(ha, hb, "path-derived member names must not affect the hash");
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
    fn changed_symbol_table_changes_hash() {
        // A stale/crafted symbol table that disagrees with the members must NOT
        // collide with one that links differently — the symtab is hashed, not
        // dropped.
        let a = archive(&[("/", SYMTAB), ("/0", b"obj-data")]);
        let b = archive(&[
            ("/", b"\x00\x00\x00\x01\x00\x00\x00\x99bar\x00"),
            ("/0", b"obj-data"),
        ]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn member_order_changes_hash() {
        // Order is link-significant; reordering members must re-key.
        let a = archive(&[("/0", b"aaaa"), ("/4", b"bbbb")]);
        let b = archive(&[("/0", b"bbbb"), ("/4", b"aaaa")]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn longname_table_content_is_ignored() {
        // Only the `//` table differs (same size) -> identical hash.
        let a = archive(&[("//", b"aaaaaaaa/\n"), ("/0", b"obj")]);
        let b = archive(&[("//", b"bbbbbbbb/\n"), ("/0", b"obj")]);
        assert_eq!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn longname_table_length_change_changes_hash() {
        // codex review #471: the `//` table's LENGTH shifts the absolute member
        // offsets the symbol table stores. A different-LENGTH `//` (with the same
        // raw symtab + object bytes) must re-key, or a stale/crafted symtab could
        // point at a different member yet collide. Content is ignored; length is not.
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
        let a = archive(&[("/0", b"obj")]);
        assert!(
            portable_static_archive_hash(&a)
                .unwrap()
                .starts_with("gnu-ar-v1:"),
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
    fn darwin64_symdef_falls_back() {
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
    fn bsd_archive_falls_back() {
        // BSD `#1/N` inline-name member -> None (caller uses whole-file hash).
        let mut a = AR_MAGIC.to_vec();
        a.extend_from_slice(&member("#1/20", b"name________________objbytes"));
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn bsd_symdef_falls_back() {
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
}
