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
//! while ignoring those path-derived member names, for the two `ar` flavors
//! rustc links on kache's Unix targets: GNU / SysV (Linux) and BSD / Darwin
//! (macOS, #691). It is deliberately **fail-closed**: anything it cannot parse
//! as a plain (non-thin) GNU or BSD archive returns `None` and the caller falls
//! back to the whole-file hash — so it can never produce a *wrong* (colliding)
//! hash, only a less-portable one.
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
//! ## What is hashed (BSD / Darwin archives, #691)
//! macOS `ar` stores long member names INLINE: a `#1/N` header name puts the
//! (NUL-padded) name in the first N bytes of the member's data area, and the
//! header size INCLUDES those bytes. The path-derived `cc` name therefore
//! lives inside the member data itself — which is why the whole-file fallback
//! re-keyed every cc-built staticlib per checkout on macOS (#691).
//! - every object member's DATA bytes AFTER the inline name, length-framed,
//!   **in archive order** (same order reasoning as GNU);
//! - every member's inline-name LENGTH, not its bytes — the BSD analog of the
//!   GNU `//`-length fold: the ranlib member stores absolute member offsets,
//!   and those offsets shift with the inline name lengths, so a length change
//!   must re-key (else a stale/crafted ranlib could point at a different
//!   member yet collide). `cc` names are a fixed-width hash prefix + stem, so
//!   their lengths — and thus the stored offsets — converge across clones;
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
//! - the `//` long-name table CONTENT and the `#1/N` inline-name BYTES (both
//!   are the path-derived `cc` names);
//! - every member-header field (name, mtime, uid, gid, mode) — none affect
//!   linking; the name is the `cc` path-hash we are normalizing away.
//!
//! ## Out of scope -> whole-file fallback (`None`)
//! Thin archives (`!<thin>`), Windows COFF `.lib`, GNU/BSD mixed layouts, and
//! anything malformed. These keep today's whole-file behavior (correct, just
//! not cross-clone-portable).

const AR_MAGIC: &[u8; 8] = b"!<arch>\n";
const AR_HEADER_LEN: usize = 60;
/// Domain tags so these schemes can never collide with a plain whole-file
/// blake3 (the fallback), with each other, or with any other key input. Bump
/// the trailing version if a hashed-content definition ever changes (also bump
/// `CACHE_KEY_VERSION`).
const GNU_DOMAIN: &[u8] = b"kache.native-ar.gnu.member-content.v1\0";
const BSD_DOMAIN: &[u8] = b"kache.native-ar.bsd.member-content.v1\0";

/// Portable content hash of a GNU or BSD `ar` static archive, or `None` to
/// signal the caller should fall back to a whole-file hash. See the module docs.
///
/// GNU is tried first: an archive with only plain short names and no reserved
/// members parses under both arms, and GNU claiming it keeps every pre-#691
/// digest stable. A BSD-parsed archive deliberately NEVER hashes equal to a
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

/// The BSD / Darwin arm (#691). See "What is hashed (BSD / Darwin archives)"
/// in the module docs. Strictness mirrors [`gnu_archive_hash`]: any GNU
/// reserved name shape means a mixed/foreign layout -> `None`, never a guess;
/// the ranlib member may only be the first member and appear at most once
/// (where Darwin `ranlib` always writes it).
fn bsd_archive_hash(bytes: &[u8]) -> Option<String> {
    if bytes.len() < AR_MAGIC.len() || &bytes[..AR_MAGIC.len()] != AR_MAGIC {
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
        let field = ar_name(&header[0..16]);
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
        let (name, inline_len) = match field.strip_prefix("#1/") {
            Some(digits) => {
                let n = parse_ar_decimal(digits.as_bytes())?;
                let raw = data.get(..n)?;
                let end = raw.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
                (String::from_utf8_lossy(&raw[..end]).into_owned(), n)
            }
            None => (field, 0),
        };
        let content = &data[inline_len..];

        if is_bsd_symdef(&name) {
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
            // The inline name is the path-derived `cc` hash we normalize
            // away: hash the data AFTER it, but fold its LENGTH — the BSD
            // analog of the GNU `//`-length fold (the absolute offsets the
            // ranlib stores shift with it, so a length change must re-key).
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
    Some(format!("bsd-ar-v1:{}", hasher.finalize().to_hex()))
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

    // A realistic Darwin ranlib payload shape (its exact bytes don't matter to
    // the parser; only that it is stable across clones, which it is when the
    // inline-name lengths — and thus its stored offsets — are equal).
    const BSD_SYMDEF: &[u8] =
        b"\x10\x00\x00\x00\x00\x00\x00\x00\x88\x00\x00\x00\x08\x00\x00\x00foo\0bar\0";

    #[test]
    fn bsd_identical_content_different_member_names_hash_equal() {
        // The #691 case: two clones, byte-identical object contents AND ranlib
        // payload (the equal-length `cc` name prefixes keep its stored offsets
        // equal — measured on rquickjs-sys), but different path-derived inline
        // names. Must hash EQUAL.
        let obj1 = b"\xcf\xfa\xed\xfe-object-one-contents";
        let obj2 = b"\xcf\xfa\xed\xfe-object-two-contents!"; // even len
        let clone_a = bsd_archive(&[
            ("__.SYMDEF SORTED", 20, BSD_SYMDEF),
            ("cafca65b3467684e-a.o", 20, obj1),
            ("cafca65b3467684e-b.o", 20, obj2),
        ]);
        let clone_b = bsd_archive(&[
            ("__.SYMDEF SORTED", 20, BSD_SYMDEF),
            ("4af22b2a007cb61a-a.o", 20, obj1),
            ("4af22b2a007cb61a-b.o", 20, obj2),
        ]);
        let ha = portable_static_archive_hash(&clone_a).expect("clone-a parses");
        let hb = portable_static_archive_hash(&clone_b).expect("clone-b parses");
        assert_eq!(ha, hb, "path-derived inline names must not affect the hash");
    }

    #[test]
    fn bsd_changed_object_content_changes_hash() {
        // #421 must be preserved on the BSD arm too: an in-place object-byte
        // change (same path, same names) re-keys.
        let a = bsd_archive(&[("x.o", 4, b"object-vONE")]);
        let b = bsd_archive(&[("x.o", 4, b"object-vTWO")]);
        assert_ne!(
            portable_static_archive_hash(&a).unwrap(),
            portable_static_archive_hash(&b).unwrap(),
        );
    }

    #[test]
    fn bsd_member_order_changes_hash() {
        // Order is link-significant; reordering members must re-key.
        let a = bsd_archive(&[("a.o", 4, b"aaaa"), ("b.o", 4, b"bbbb")]);
        let b = bsd_archive(&[("a.o", 4, b"bbbb"), ("b.o", 4, b"aaaa")]);
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
        let a = bsd_archive(&[("__.SYMDEF", 12, BSD_SYMDEF), ("x.o", 4, b"obj-data")]);
        let b = bsd_archive(&[("__.SYMDEF", 12, &crafted), ("x.o", 4, b"obj-data")]);
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
        let base = bsd_archive(&[("__.SYMDEF", 20, BSD_SYMDEF), ("x.o", 4, b"obj-data")]);
        for other in ["__.SYMDEF_64", "__.SYMDEF SORTED", "__.SYMDEF_64 SORTED"] {
            let variant = bsd_archive(&[(other, 20, BSD_SYMDEF), ("x.o", 4, b"obj-data")]);
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
        // different padded name length must re-key. Bytes are ignored; length
        // is not.
        let a = bsd_archive(&[("x.o", 4, b"obj-data")]);
        let b = bsd_archive(&[("x.o", 8, b"obj-data")]);
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
        a.extend_from_slice(&member("cutils.o", b"obj-data"));
        let h = portable_static_archive_hash(&a).expect("short-name BSD archive parses");
        assert!(h.starts_with("bsd-ar-v1:"));
    }

    #[test]
    fn bsd_output_is_domain_tagged() {
        // Textually distinct from BOTH the whole-file fallback and the GNU
        // scheme — a BSD-parsed archive never hashes equal to a GNU-parsed
        // one (rustc bundles the archive BYTES, so format is content).
        let a = bsd_archive(&[("x.o", 4, b"obj-data")]);
        assert!(
            portable_static_archive_hash(&a)
                .unwrap()
                .starts_with("bsd-ar-v1:")
        );
    }

    #[test]
    fn bsd_ranlib_not_first_falls_back() {
        // Darwin ranlib always writes the symbol table first; anywhere else
        // is not a plain Darwin archive.
        let a = bsd_archive(&[("x.o", 4, b"obj-data"), ("__.SYMDEF", 12, BSD_SYMDEF)]);
        assert!(portable_static_archive_hash(&a).is_none());
    }

    #[test]
    fn bsd_second_ranlib_falls_back() {
        let a = bsd_archive(&[
            ("__.SYMDEF", 12, BSD_SYMDEF),
            ("__.SYMDEF", 12, BSD_SYMDEF),
            ("x.o", 4, b"obj-data"),
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
        let a = bsd_archive(&[("a.o", 4, b"odd"), ("b.o", 4, b"data!")]);
        assert!(portable_static_archive_hash(&a).is_some());
    }

    /// Drive the SYSTEM `ar` (BSD on macOS): two archives, identical member
    /// bytes, names differing only in their equal-length path-derived prefix —
    /// the exact #691 rquickjs-sys shape — must hash EQUAL through the BSD arm
    /// (not the fallback). Runs where the CI macOS arm runs `cargo test`.
    #[test]
    #[cfg(target_os = "macos")]
    fn real_darwin_ar_identical_content_different_names_hash_equal() {
        use std::process::Command;
        let dir = tempfile::tempdir().unwrap();
        let mut digests = Vec::new();
        for prefix in ["cafca65b3467684e", "4af22b2a007cb61a"] {
            let sub = dir.path().join(prefix);
            std::fs::create_dir(&sub).unwrap();
            let lib = sub.join("libprobe.a");
            let mut objects = Vec::new();
            for (stem, content) in [("one", &b"object-one-bytes"[..]), ("two", b"object-two!")] {
                // >15 chars forces the `#1/N` inline-name encoding.
                let obj = sub.join(format!("{prefix}-{stem}.o"));
                std::fs::write(&obj, content).unwrap();
                objects.push(obj);
            }
            let status = Command::new("ar")
                .arg("cqS") // S: no symbol table — the members aren't real objects
                .arg(&lib)
                .args(&objects)
                .env("ZERO_AR_DATE", "1")
                .status()
                .expect("system ar runs");
            assert!(status.success(), "ar cqS failed");
            let bytes = std::fs::read(&lib).unwrap();
            let digest = portable_static_archive_hash(&bytes)
                .expect("system-ar BSD archive parses portably");
            assert!(
                digest.starts_with("bsd-ar-v1:"),
                "BSD arm must claim it: {digest}"
            );
            digests.push(digest);
        }
        assert_eq!(
            digests[0], digests[1],
            "equal-length path-derived name prefixes must not re-key"
        );
    }
}
