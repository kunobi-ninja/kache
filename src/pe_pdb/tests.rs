use super::*;
use std::path::PathBuf;

const GUID: [u8; 16] = [
    0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
];

fn put_u16(bytes: &mut [u8], at: usize, value: u16) {
    bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

/// What a fixture executable carries in its debug directory.
enum Debug<'a> {
    None,
    Rsds {
        guid: [u8; 16],
        age: u32,
        path: &'a str,
    },
    Raw(&'a [u8]),
}

/// A minimal PE: DOS header, PE signature, COFF header, optional header of the
/// given magic with 16 data directories, one section holding the debug
/// directory and its CodeView record.
fn pe(magic: u16, debug: Debug<'_>) -> Vec<u8> {
    let pe_at = 0x80;
    let optional_size: usize = if magic == 0x20b { 240 } else { 224 };
    let section_table = pe_at + 24 + optional_size;
    let raw_at = 0x400;
    let section_rva = 0x1000u32;
    let mut bytes = vec![0u8; raw_at + 0x200];
    bytes[..2].copy_from_slice(b"MZ");
    put_u32(&mut bytes, 0x3c, pe_at as u32);
    bytes[pe_at..pe_at + 4].copy_from_slice(b"PE\0\0");
    put_u16(&mut bytes, pe_at + 4, 0x8664);
    put_u16(&mut bytes, pe_at + 6, 1); // one section
    put_u16(&mut bytes, pe_at + 20, optional_size as u16);
    let optional = pe_at + 24;
    put_u16(&mut bytes, optional, magic);
    let (count_at, dirs_at) = if magic == 0x20b { (108, 112) } else { (92, 96) };
    put_u32(&mut bytes, optional + count_at, 16);
    // .rdata: virtual 0x1000.., raw at 0x400, 0x200 bytes.
    bytes[section_table..section_table + 6].copy_from_slice(b".rdata");
    put_u32(&mut bytes, section_table + 8, 0x200);
    put_u32(&mut bytes, section_table + 12, section_rva);
    put_u32(&mut bytes, section_table + 16, 0x200);
    put_u32(&mut bytes, section_table + 20, raw_at as u32);
    let record = match debug {
        Debug::None => return bytes,
        Debug::Rsds { guid, age, path } => {
            let mut record = b"RSDS".to_vec();
            record.extend(guid);
            record.extend(age.to_le_bytes());
            record.extend(path.as_bytes());
            record.push(0);
            record
        }
        Debug::Raw(raw) => raw.to_vec(),
    };
    // Debug directory at the section's start: a POGO entry first (type 13),
    // then the CodeView one, as link.exe writes them.
    let dir = raw_at;
    let record_at = raw_at + 0x40;
    put_u32(&mut bytes, optional + dirs_at + 8 * 6, section_rva);
    put_u32(&mut bytes, optional + dirs_at + 8 * 6 + 4, 56);
    put_u32(&mut bytes, dir + 12, 13);
    put_u32(&mut bytes, dir + 28 + 12, 2);
    put_u32(&mut bytes, dir + 28 + 16, record.len() as u32);
    put_u32(&mut bytes, dir + 28 + 20, section_rva + 0x40);
    put_u32(&mut bytes, dir + 28 + 24, record_at as u32);
    bytes[record_at..record_at + record.len()].copy_from_slice(&record);
    bytes
}

/// A minimal MSF 7.0 file with 512-byte blocks: stream 0 empty, stream 1 the
/// PDB information stream, stream 2 nil, stream 3 a DBI header with
/// `dbi_age` (absent when `None`).
fn pdb(guid: [u8; 16], info_age: u32, dbi_age: Option<u32>) -> Vec<u8> {
    const BLOCK: usize = 512;
    // Blocks: 0 superblock, 1 block map, 2 directory, 3 info, 4 DBI.
    let mut bytes = vec![0u8; BLOCK * 5];
    bytes[..32].copy_from_slice(MSF_MAGIC);
    put_u32(&mut bytes, 32, BLOCK as u32);
    put_u32(&mut bytes, 40, 5);
    put_u32(&mut bytes, 52, 1);
    put_u32(&mut bytes, BLOCK, 2); // the directory lives in block 2
    let mut directory = Vec::new();
    let streams: u32 = if dbi_age.is_some() { 4 } else { 2 };
    directory.extend(streams.to_le_bytes());
    directory.extend(0u32.to_le_bytes()); // stream 0: empty
    directory.extend(28u32.to_le_bytes()); // stream 1: info
    if dbi_age.is_some() {
        directory.extend(u32::MAX.to_le_bytes()); // stream 2: nil
        directory.extend(64u32.to_le_bytes()); // stream 3: DBI
    }
    directory.extend(3u32.to_le_bytes());
    if dbi_age.is_some() {
        directory.extend(4u32.to_le_bytes());
    }
    put_u32(&mut bytes, 44, directory.len() as u32);
    bytes[BLOCK * 2..BLOCK * 2 + directory.len()].copy_from_slice(&directory);
    let info = BLOCK * 3;
    put_u32(&mut bytes, info, 20000404);
    put_u32(&mut bytes, info + 4, 0x5f00_0000);
    put_u32(&mut bytes, info + 8, info_age);
    bytes[info + 12..info + 28].copy_from_slice(&guid);
    if let Some(age) = dbi_age {
        let dbi = BLOCK * 4;
        put_u32(&mut bytes, dbi, u32::MAX);
        put_u32(&mut bytes, dbi + 4, 19990903);
        put_u32(&mut bytes, dbi + 8, age);
    }
    bytes
}

struct Unit {
    _dir: tempfile::TempDir,
    files: Vec<(PathBuf, String)>,
}

impl Unit {
    fn new(files: &[(&str, Vec<u8>)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let files = files
            .iter()
            .map(|(name, bytes)| {
                let path = dir.path().join(name);
                std::fs::write(&path, bytes).unwrap();
                (path, name.to_string())
            })
            .collect();
        Self { _dir: dir, files }
    }

    fn check(&self) -> Result<Vec<Pairing>> {
        check_outputs(
            self.files
                .iter()
                .map(|(path, name)| (path.as_path(), name.as_str())),
            None,
        )
    }
}

fn rsds(path: &str) -> Debug<'_> {
    Debug::Rsds {
        guid: GUID,
        age: 1,
        path,
    }
}

#[test]
fn reads_the_codeview_record_of_pe32_plus_and_pe32() {
    for magic in [0x20b, 0x10b] {
        let unit = Unit::new(&[("app-0a1b.exe", pe(magic, rsds("app-0a1b.pdb")))]);
        let ExecutableDebug::CodeView(record) = read_codeview(&unit.files[0].0).unwrap() else {
            panic!("expected CodeView record")
        };
        assert_eq!(
            record,
            CodeViewRecord {
                guid: GUID,
                age: 1,
                pdb_path: "app-0a1b.pdb".into()
            },
            "magic {magic:#x}"
        );
    }
}

#[test]
fn a_pe_without_a_debug_directory_has_no_record() {
    let unit = Unit::new(&[("app.exe", pe(0x20b, Debug::None))]);
    assert_eq!(
        read_codeview(&unit.files[0].0).unwrap(),
        ExecutableDebug::NoCodeView
    );
    assert_eq!(unit.check().unwrap(), vec![Pairing::SelfContained]);
}

#[test]
fn non_pe_executables_stay_eligible_and_old_codeview_records_fail() {
    let elf = Unit::new(&[("app.exe", b"\x7fELF\0\0\0\0".repeat(16))]);
    assert_eq!(
        read_codeview(&elf.files[0].0).unwrap(),
        ExecutableDebug::OtherFormat
    );
    assert!(elf.check().unwrap().is_empty());
    let mut nb10 = b"NB10".to_vec();
    nb10.extend([0u8; 20]);
    nb10.extend(b"app.pdb\0");
    let unit = Unit::new(&[("app.exe", pe(0x20b, Debug::Raw(&nb10)))]);
    let error = read_codeview(&unit.files[0].0).unwrap_err();
    assert!(error.to_string().contains("not RSDS"), "{error:#}");
}

#[test]
fn reads_the_guid_and_both_ages_of_a_pdb() {
    let unit = Unit::new(&[
        ("a.pdb", pdb(GUID, 3, Some(2))),
        ("b.pdb", pdb(GUID, 1, None)),
    ]);
    assert_eq!(
        read_pdb_identity(&unit.files[0].0).unwrap(),
        PdbIdentity {
            guid: GUID,
            info_age: 3,
            dbi_age: 2
        }
    );
    assert!(
        read_pdb_identity(&unit.files[1].0).is_err(),
        "an executable needs its DBI header"
    );
    let not_msf = Unit::new(&[("c.pdb", vec![0u8; 1024])]);
    assert!(read_pdb_identity(&not_msf.files[0].0).is_err());
}

#[test]
fn an_executable_with_its_matching_pdb_pairs() {
    let unit = Unit::new(&[
        ("app-0a1b.exe", pe(0x20b, rsds("app-0a1b.pdb"))),
        ("app-0a1b.pdb", pdb(GUID, 1, Some(1))),
        ("app-0a1b.d", b"app-0a1b.exe: src/main.rs\n".to_vec()),
    ]);
    assert_eq!(
        unit.check().unwrap(),
        vec![Pairing::Paired {
            pdb_store_name: "app-0a1b.pdb".into()
        }]
    );
}

#[test]
fn the_pdb_name_matches_without_regard_to_case() {
    let unit = Unit::new(&[
        ("app-0a1b.exe", pe(0x20b, rsds("APP-0A1B.PDB"))),
        ("app-0a1b.pdb", pdb(GUID, 1, Some(1))),
    ]);
    assert!(matches!(
        unit.check().unwrap()[..],
        [Pairing::Paired { .. }]
    ));
}

#[test]
fn executable_pairing_uses_the_info_age_bound_and_the_dbi_image_age() {
    let record = CodeViewRecord {
        guid: GUID,
        age: 2,
        pdb_path: "a.pdb".into(),
    };
    let identity = |info_age, dbi_age| PdbIdentity {
        guid: GUID,
        info_age,
        dbi_age,
    };
    assert!(identity(2, 2).matches(&record));
    assert!(
        identity(3, 2).matches(&record),
        "type updates can advance Info without DBI"
    );
    assert!(
        identity(2, 0).matches(&record),
        "Microsoft's legacy DBI-age-zero rule"
    );
    assert!(
        !identity(1, 0).matches(&record),
        "legacy DBI does not waive the Info lower bound"
    );
    assert!(
        !identity(2, 1).matches(&record),
        "Info equality cannot hide a stale DBI"
    );
    assert!(
        !identity(1, 2).matches(&record),
        "DBI equality cannot hide an older Info stream"
    );
    assert!(!identity(3, 1).matches(&record));
    let mut other = identity(2, 2);
    other.guid[15] ^= 1;
    assert!(!other.matches(&record));
}

#[test]
fn an_absolute_pdb_path_is_refused() {
    for path in [
        r"C:\w\target\debug\deps\app-0a1b.pdb",
        "/w/target/debug/deps/app-0a1b.pdb",
        "deps/app-0a1b.pdb",
        r"deps\app-0a1b.pdb",
        "C:app-0a1b.pdb",
        ".",
        "..",
        "",
    ] {
        let unit = Unit::new(&[
            ("app-0a1b.exe", pe(0x20b, rsds(path))),
            ("app-0a1b.pdb", pdb(GUID, 1, Some(1))),
        ]);
        let error = unit.check().unwrap_err();
        assert!(error.to_string().contains("by path"), "{path}: {error:#}");
    }
}

#[test]
fn a_pdb_missing_from_the_outputs_is_refused() {
    let unit = Unit::new(&[
        ("app-0a1b.exe", pe(0x20b, rsds("app-0a1b.pdb"))),
        ("other.pdb", pdb(GUID, 1, Some(1))),
    ]);
    let error = unit.check().unwrap_err();
    assert!(
        error.to_string().contains("not among the outputs"),
        "{error:#}"
    );
}

#[test]
fn a_stale_pdb_is_refused() {
    let mut stale = GUID;
    stale[0] ^= 0xff;
    for (guid, age) in [(stale, 1), (GUID, 2)] {
        let unit = Unit::new(&[
            ("app-0a1b.exe", pe(0x20b, rsds("app-0a1b.pdb"))),
            ("app-0a1b.pdb", pdb(guid, age, Some(age))),
        ]);
        let error = unit.check().unwrap_err();
        assert!(error.to_string().contains("does not match"), "{error:#}");
    }
}

#[test]
fn an_incrementally_linked_executable_is_refused() {
    let unit = Unit::new(&[
        ("app-0a1b.exe", pe(0x20b, rsds("app-0a1b.pdb"))),
        ("app-0a1b.pdb", pdb(GUID, 1, Some(1))),
    ]);
    assert!(unit.check().is_ok());
    std::fs::write(unit.files[0].0.with_extension("ilk"), b"ilk").unwrap();
    let error = unit.check().unwrap_err();
    assert!(error.to_string().contains("incrementally"), "{error:#}");
}

/// rustc reports the `.exe` alone; the `.pdb` it names by file name is added
/// when it lies beside it, once, and nothing else is.
#[test]
fn add_named_pdbs_adds_the_pdb_each_exe_names() {
    let unit = Unit::new(&[
        ("app.exe", pe(0x20b, rsds("app.pdb"))),
        ("app.pdb", pdb(GUID, 1, Some(1))),
        ("t-0a1b.exe", pe(0x20b, rsds("t-0a1b.pdb"))),
        ("t-0a1b.pdb", pdb(GUID, 1, Some(1))),
        ("far.exe", pe(0x20b, rsds(r"C:\elsewhere\far.pdb"))),
        ("far.pdb", pdb(GUID, 1, Some(1))),
        ("gone.exe", pe(0x20b, rsds("gone.pdb"))),
        ("bare.exe", pe(0x20b, Debug::None)),
        ("lib.dll", pe(0x20b, rsds("lib.pdb"))),
        ("lib.pdb", pdb(GUID, 1, Some(1))),
        ("app.d", b"app.exe: src/main.rs\n".to_vec()),
    ]);
    let reported = |names: &[&str]| -> Vec<(PathBuf, String)> {
        unit.files
            .iter()
            .filter(|(_, name)| names.contains(&name.as_str()))
            .cloned()
            .collect()
    };
    let mut files = reported(&[
        "app.exe",
        "t-0a1b.exe",
        "far.exe",
        "gone.exe",
        "bare.exe",
        "lib.dll",
        "app.d",
    ]);
    add_named_pdbs(&mut files, None);
    let added: Vec<_> = files[7..].iter().map(|(_, name)| name.as_str()).collect();
    assert_eq!(added, ["app.pdb", "t-0a1b.pdb"]);
    assert_eq!(files[7].0, unit.files[1].0);

    // Already reported (in any case): not added twice.
    let mut files = reported(&["app.exe", "app.pdb"]);
    files[1].1 = "APP.pdb".into();
    add_named_pdbs(&mut files, None);
    assert_eq!(files.len(), 2);
}

#[test]
fn a_corrupt_pdb_is_refused() {
    let unit = Unit::new(&[
        ("app-0a1b.exe", pe(0x20b, rsds("app-0a1b.pdb"))),
        (
            "app-0a1b.pdb",
            b"Microsoft C/C++ program database 2.00\r\n".to_vec(),
        ),
    ]);
    assert!(unit.check().is_err());
}

#[test]
fn only_exe_outputs_are_checked() {
    // An ELF binary, a DLL with an unpaired record, an rlib: none is an
    // `.exe`, so none is read.
    let unit = Unit::new(&[
        ("app-0a1b", b"\x7fELF".to_vec()),
        ("mac-0a1b.dll", pe(0x20b, rsds(r"C:\elsewhere\mac.pdb"))),
        ("libdep-0a1b.rlib", b"!<arch>\n".to_vec()),
    ]);
    assert_eq!(unit.check().unwrap(), vec![]);
    assert!(is_windows_executable_name("APP.EXE"));
    assert!(!is_windows_executable_name("app.exe.manifest"));
    assert!(!is_windows_executable_name("exe"));
}

#[test]
fn bare_file_names() {
    assert!(is_bare_file_name("app-0a1b.pdb"));
    for name in ["", ".", "..", "a/b.pdb", r"a\b.pdb", "c:b.pdb"] {
        assert!(!is_bare_file_name(name), "{name}");
    }
}

/// Checks a real executable and its `.pdb` (`KACHE_TEST_PE_PDB=<exe>`; the
/// `.pdb` is the one the executable names, next to it). Run by hand against a
/// link.exe or lld-link output: `cargo test -p kache pe_pdb -- --ignored`.
#[test]
#[ignore = "needs a real executable: KACHE_TEST_PE_PDB=<path to .exe>"]
fn a_real_executable_pairs_with_its_pdb() {
    let exe = PathBuf::from(std::env::var_os("KACHE_TEST_PE_PDB").expect("KACHE_TEST_PE_PDB"));
    let ExecutableDebug::CodeView(record) = read_codeview(&exe).unwrap() else {
        panic!("expected CodeView record")
    };
    let pdb = exe.with_file_name(&record.pdb_path);
    let identity = read_pdb_identity(&pdb).unwrap();
    assert!(identity.matches(&record), "{record:?} vs {identity:?}");
    let names = [
        exe.file_name().unwrap().to_string_lossy().into_owned(),
        record.pdb_path.clone(),
    ];
    let outputs = [
        (exe.as_path(), names[0].as_str()),
        (pdb.as_path(), names[1].as_str()),
    ];
    assert!(matches!(
        check_outputs(outputs.iter().copied(), None).unwrap()[..],
        [Pairing::Paired { .. }]
    ));
}

/// Shared only with wrapper restore tests; no native executable is run here.
pub(crate) fn fixture_pair() -> (Vec<u8>, Vec<u8>) {
    (pe(0x20b, rsds("app.pdb")), pdb(GUID, 1, Some(1)))
}

fn inspect(bytes: Vec<u8>) -> Result<ExecutableDebug> {
    let unit = Unit::new(&[("app.exe", bytes)]);
    read_codeview(&unit.files[0].0)
}

#[test]
fn debug_directory_extents_and_counts_are_bounded_before_reading() {
    for (rva, size) in [
        (0, 28),
        (0x1000, 0),
        (0x1000, 27),
        (0x1000, u32::MAX),
        (0x1000, 114_716),
    ] {
        let mut bytes = pe(0x20b, rsds("app.pdb"));
        put_u32(&mut bytes, 0x138, rva);
        put_u32(&mut bytes, 0x13c, size);
        let error = inspect(bytes).unwrap_err().to_string();
        assert!(
            error.contains(if rva == 0 || size == 0 {
                "incomplete PE debug directory"
            } else {
                "invalid or excessive entry count"
            }),
            "rva={rva:#x}, size={size}: {error}"
        );
    }
    // The independent literal count exercises the admitted boundary, not a
    // fixture derived from MAX_DEBUG_ENTRIES that would move with a mutant.
    let mut bytes = pe(0x20b, Debug::None);
    bytes.resize(0x400 + 4096 * 28, 0);
    put_u32(&mut bytes, 0x138, 0x1000);
    put_u32(&mut bytes, 0x13c, 4096 * 28);
    put_u32(&mut bytes, 0x188 + 8, 4096 * 28);
    put_u32(&mut bytes, 0x188 + 16, 4096 * 28);
    assert_eq!(inspect(bytes).unwrap(), ExecutableDebug::NoCodeView);

    let mut bytes = pe(0x20b, rsds("app.pdb"));
    put_u32(&mut bytes, 0x188 + 16, 16);
    assert!(
        inspect(bytes).is_err(),
        "virtual space is not file-backed debug data"
    );
    let mut bytes = pe(0x20b, rsds("app.pdb"));
    put_u32(&mut bytes, 0x188 + 20, u32::MAX);
    assert!(
        inspect(bytes).is_err(),
        "a raw section offset beyond EOF is refused"
    );
}

#[test]
fn header_debug_directories_and_section_ambiguity_are_checked() {
    let mut bytes = pe(0x20b, rsds("app.pdb"));
    let directory = bytes[0x400..0x438].to_vec();
    bytes[0x240..0x278].copy_from_slice(&directory);
    put_u32(&mut bytes, 0x98 + 60, 0x400);
    put_u32(&mut bytes, 0x138, 0x240);
    assert!(matches!(
        inspect(bytes.clone()).unwrap(),
        ExecutableDebug::CodeView(_)
    ));
    put_u32(&mut bytes, 0x98 + 60, 0x250);
    assert!(
        inspect(bytes).is_err(),
        "a directory cannot cross the header boundary"
    );

    let mut bytes = pe(0x20b, rsds("app.pdb"));
    let section = bytes[0x188..0x1b0].to_vec();
    bytes[0x1b0..0x1d8].copy_from_slice(&section);
    put_u16(&mut bytes, 0x80 + 6, 2);
    assert!(
        inspect(bytes).is_err(),
        "overlapping sections cannot choose the debug bytes"
    );
}

#[test]
fn codeview_bounds_termination_and_ambiguity_fail_closed() {
    for size in [0, 24, 4097] {
        let mut bytes = pe(0x20b, rsds("app.pdb"));
        put_u32(&mut bytes, 0x41c + 16, size);
        let error = inspect(bytes).unwrap_err().to_string();
        assert!(
            error.contains("CodeView record of"),
            "record size {size}: {error}"
        );
    }
    let mut bytes = pe(0x20b, rsds("app.pdb"));
    put_u32(&mut bytes, 0x41c + 24, u32::MAX);
    assert!(
        inspect(bytes).is_err(),
        "a record pointer beyond EOF is refused"
    );

    let mut record = b"RSDS".to_vec();
    record.extend(GUID);
    record.extend(1u32.to_le_bytes());
    record.extend(b"app.pdb");
    assert!(
        inspect(pe(0x20b, Debug::Raw(&record))).is_err(),
        "the path needs its terminator"
    );
    record.push(0);
    record[24] = 0xff;
    assert!(
        inspect(pe(0x20b, Debug::Raw(&record))).is_err(),
        "non-UTF8 names are unsupported"
    );

    let mut bytes = pe(0x20b, rsds("app.pdb"));
    let codeview = bytes[0x41c..0x438].to_vec();
    bytes[0x400..0x41c].copy_from_slice(&codeview);
    assert!(
        inspect(bytes).is_err(),
        "even duplicate records have no unique selection"
    );

    let mut bytes = pe(0x20b, rsds("app.pdb"));
    bytes.resize(0x440 + 4096, 0);
    put_u32(&mut bytes, 0x41c + 16, 4096);
    bytes[0x440 + 24..0x440 + 4096 - 1].fill(b'a');
    assert!(
        matches!(inspect(bytes).unwrap(), ExecutableDebug::CodeView(_)),
        "the supported record-size boundary is admitted"
    );
}

#[test]
fn a_bounded_reader_refuses_overflowing_or_out_of_file_reads() {
    let unit = Unit::new(&[("bytes", vec![1, 2, 3])]);
    let mut reader = Reader::open(&unit.files[0].0).unwrap();
    assert_eq!(reader.read_at(2, 1).unwrap(), [3]);
    assert!(reader.read_at(3, 0).unwrap().is_empty());
    assert!(reader.read_at(4, 0).is_err());
    assert!(reader.read_at(2, 2).is_err());
    assert!(reader.read_at(u64::MAX, usize::MAX).is_err());
    assert!(reader.check_range(0, u64::MAX).is_err());
}

#[test]
fn truncated_pe_headers_are_errors_instead_of_absent_debug_info() {
    let complete = pe(0x20b, rsds("app.pdb"));
    for end in [1, 63, 0x80 + 23, 0x98 + 1, 0x188 + 39] {
        assert!(
            inspect(complete[..end].to_vec()).is_err(),
            "truncation at {end}"
        );
    }
    let mut bytes = complete.clone();
    put_u16(&mut bytes, 0x98, 0xffff);
    assert!(inspect(bytes).is_err());
    let mut bytes = complete.clone();
    put_u16(&mut bytes, 0x80 + 20, 110);
    assert!(inspect(bytes).is_err(), "PE32+ needs the directory count");
    let mut bytes = complete;
    put_u32(&mut bytes, 0x98 + 108, 6);
    assert_eq!(
        inspect(bytes).unwrap(),
        ExecutableDebug::NoCodeView,
        "the optional header declares no debug-directory slot"
    );
}

#[test]
fn msf_block_stream_and_header_bounds_are_checked() {
    let identity = |bytes: Vec<u8>| {
        let unit = Unit::new(&[("app.pdb", bytes)]);
        read_pdb_identity(&unit.files[0].0)
    };
    for (offset, value) in [
        (32, 0),
        (32, 513),
        (32, 256),
        (32, 131072),
        (40, 0),
        (40, 6),
        (52, 5),
        (44, 3),
        (44, 67_108_865),
        (512, 5),
        (1024, u32::MAX),
        (1024 + 20, 5),
        (1024 + 24, 5),
        (1024 + 8, 27),
        (1024 + 16, 63),
        (2048, 0),
        (2048 + 4, 1),
    ] {
        let mut bytes = pdb(GUID, 1, Some(1));
        put_u32(&mut bytes, offset, value);
        let error = identity(bytes).unwrap_err().to_string();
        if offset == 32 {
            assert!(error.contains("MSF block size"), "{error}");
        } else if offset == 44 {
            assert!(error.contains("MSF stream directory of"), "{error}");
        }
    }
    for end in [55, 1024 + 3, 1536 + 27, 2048 + 63] {
        assert!(
            identity(pdb(GUID, 1, Some(1))[..end].to_vec()).is_err(),
            "truncated at {end}"
        );
    }
    let mut newer = pdb(GUID, 2, Some(1));
    put_u32(&mut newer, 2048 + 4, 20091201);
    assert_eq!(
        identity(newer).unwrap().dbi_age,
        1,
        "the newer DBI header version is supported"
    );
}

#[test]
fn reported_or_pending_pdb_names_must_be_unique_and_beside_the_executable() {
    let mut unit = Unit::new(&[
        ("app.exe", pe(0x20b, rsds("app.pdb"))),
        ("app.pdb", pdb(GUID, 1, Some(1))),
        ("other.pdb", pdb(GUID, 1, Some(1))),
    ]);
    unit.files[2].1 = "APP.PDB".into();
    assert!(
        unit.check().is_err(),
        "case-fold-equivalent outputs are ambiguous"
    );
    unit.files.pop();
    let nested = unit._dir.path().join("elsewhere");
    std::fs::create_dir(&nested).unwrap();
    let moved = nested.join("app.pdb");
    std::fs::rename(&unit.files[1].0, &moved).unwrap();
    unit.files[1].0 = moved;
    assert!(
        unit.check().is_err(),
        "a bare recorded name needs its sibling output"
    );

    let unit = Unit::new(&[
        ("one.exe", pe(0x20b, rsds("shared.pdb"))),
        ("two.exe", pe(0x20b, rsds("shared.pdb"))),
        ("shared.pdb", pdb(GUID, 1, Some(1))),
    ]);
    let mut reported = unit.files[..2].to_vec();
    add_named_pdbs(&mut reported, None);
    assert_eq!(reported.len(), 3, "pending additions are deduplicated too");
    assert_eq!(
        check_outputs(
            reported
                .iter()
                .map(|(path, name)| (path.as_path(), name.as_str())),
            None
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn a_cache_blob_has_no_compiler_incremental_sidecar() {
    let unit = Unit::new(&[
        ("app.exe", pe(0x20b, rsds("app.pdb"))),
        ("app.pdb", pdb(GUID, 1, Some(1))),
    ]);
    std::fs::write(unit.files[0].0.with_extension("ilk"), b"compiler state").unwrap();
    assert!(unit.check().is_err());
    assert!(
        check_cached_outputs(
            unit.files
                .iter()
                .map(|(path, name)| (path.as_path(), name.as_str())),
            None
        )
        .is_ok()
    );
}

#[test]
fn selected_executable_outputs_do_not_depend_on_an_exe_extension() {
    for name in ["app", "app.custom", "app.exe"] {
        let unit = Unit::new(&[
            (name, pe(0x20b, rsds("app.pdb"))),
            ("app.pdb", pdb(GUID, 1, Some(1))),
        ]);
        let selected = executable_output_name(
            true,
            Some(Path::new(name)),
            unit.files.iter().map(|(_, name)| name.as_str()),
        )
        .unwrap();
        assert_eq!(selected.as_deref(), Some(name));
        let mut files = vec![unit.files[0].clone()];
        add_named_pdbs(&mut files, selected.as_deref());
        assert_eq!(files.len(), 2, "{name}: discovery omitted the named PDB");
        assert!(matches!(
            check_outputs(
                files
                    .iter()
                    .map(|(path, name)| (path.as_path(), name.as_str())),
                selected.as_deref(),
            )
            .unwrap()[..],
            [Pairing::Paired { .. }]
        ));
        assert!(
            check_cached_outputs(
                files[..1]
                    .iter()
                    .map(|(path, name)| (path.as_path(), name.as_str())),
                selected.as_deref(),
            )
            .is_err(),
            "{name}: an EXE-only cache entry bypassed the audit"
        );
    }
    assert_eq!(
        executable_output_name(true, None, ["app", "app.d"])
            .unwrap()
            .as_deref(),
        Some("app")
    );
    assert!(executable_output_name(true, None, ["app.custom", "app.d"]).is_err());
    assert!(executable_output_name(true, None, ["app.exe", "other.exe"]).is_err());
    assert!(executable_output_name(true, Some(Path::new("missing")), ["app.exe"]).is_err());
    assert_eq!(
        executable_output_name(false, Some(Path::new("library.rlib")), ["library.rlib"]).unwrap(),
        None
    );
    let unit = Unit::new(&[("app.custom", b"\x7fELF native image".to_vec())]);
    assert!(
        check_outputs(
            unit.files
                .iter()
                .map(|(path, name)| (path.as_path(), name.as_str())),
            Some("app.custom")
        )
        .unwrap()
        .is_empty()
    );
}
