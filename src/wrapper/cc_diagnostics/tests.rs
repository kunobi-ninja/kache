use super::*;

fn roots(path: &str) -> Vec<(PathBuf, String, u8)> {
    vec![(path.into(), "checkout".into(), 5)]
}

#[test]
fn locations_and_include_stacks_move_without_rewriting_source_or_message_text() {
    let producer = roots("/work/A");
    let consumer = roots("/work/B");
    let text = concat!(
        "In file included from /work/A/include/a.h:3,\n",
        "                 from /work/A/unit.c:1:\n",
        "\x1b[1m/work/A/unit.c:2:4:\x1b[0m warning: /work/A/message stays literal\n",
        "    2 | const char *s = \"/work/A/source-literal\";\n",
        "/work/A/unit.c: In function 'f':\n",
        "/work/A/unit.c(3,4): warning C4018: signedness\n",
        "/work/AB/other.c:2:4: note: a sibling checkout\n",
        "/sdk/system.h:1:2: note: an external header\n",
        "relative.c:2:4: warning: a relative source\n"
    );
    let expected = text
        .replace("from /work/A/", "from /work/B/")
        .replace("\x1b[1m/work/A/", "\x1b[1m/work/B/")
        .replace("\n/work/A/unit.c", "\n/work/B/unit.c");
    let stored = store(text, &producer);
    assert!(stored.starts_with(PREFIX));
    assert_eq!(restore(&stored, &consumer).unwrap(), expected);
    assert_eq!(restore(&stored, &producer).unwrap(), text);
}

#[test]
fn unnumbered_source_excerpts_that_start_with_a_path_remain_literal() {
    let text = concat!(
        "/A/unit.c:2:4: warning: a multiline source excerpt\n",
        "    /A/raw-string-contents\n",
        "    from /A/more-raw-string-contents\n",
        "    /A/filename:nondiagnostic-text\n"
    );
    let stored = store(text, &roots("/A"));
    assert_eq!(
        restore(&stored, &roots("/B")).unwrap(),
        text.replacen("/A/unit.c", "/B/unit.c", 1)
    );
}

#[test]
fn location_suffixes_require_compiler_location_syntax() {
    for suffix in [
        "/unit.c:2:4: warning: comparison",
        "/unit.c: 2: comparison",
        "/unit.c: In function 'f':",
        "/unit.c: At global scope:",
        "/header.h:4,",
        "/odd:name.h:3: note: here",
        "/unit.c(2,4): warning C4018: comparison",
        "/unit.c(2): warning C4018: comparison",
        "/unit.c(2,4): comparison",
        "/unit.c(2): comparison",
    ] {
        assert!(location_suffix(suffix), "{suffix}");
    }
    for suffix in [
        "/raw-string-contents",
        "/filename:nondiagnostic-text",
        "/unit.c(foo): text",
        "/unit.c(2,4) text",
        "/unit.c(): text",
        "/unit.c(,): text",
        "/unit.c(2,a): text",
    ] {
        assert!(!location_suffix(suffix), "{suffix}");
    }
}

#[test]
fn root_priority_then_specificity_and_designated_consumer_spelling_are_preserved() {
    let producer = vec![
        ("/work/A".into(), "checkout".into(), 5),
        ("/work/A/nested".into(), "nested".into(), 5),
        ("/work/A/nested/higher".into(), "checkout".into(), 1),
    ];
    let consumer = vec![
        ("/consumer/nested/".into(), "nested".into(), 5),
        ("/canonical/nested".into(), "nested".into(), 5),
        ("/consumer/B".into(), "checkout".into(), 5),
    ];
    let stored = store(
        "/work/A/nested/higher/a.h:2: warning: nested\n/work/A/b.c:3: warning: checkout\n",
        &producer,
    );
    assert_eq!(
        restore(&stored, &consumer).unwrap(),
        "/consumer/nested/higher/a.h:2: warning: nested\n/consumer/B/b.c:3: warning: checkout\n"
    );
}

#[test]
fn json_file_locations_round_trip_escaped_windows_paths_and_keep_messages() {
    let producer = roots(r#"C:\producer\a"quoted"#);
    let consumer = roots(r#"D:\consumer\b"quoted"#);
    let value = serde_json::json!([{
        "locations": [{"caret": {"file": "C:\\producer\\a\"quoted\\unit.c", "line": 2}}],
        "message": "C:\\producer\\a\"quoted\\message remains literal"
    }]);
    let original = serde_json::to_string_pretty(&value).unwrap();
    let stored = store(&original, &producer);
    assert!(stored.starts_with(PREFIX));
    let restored = restore(&stored, &consumer).unwrap();
    let restored: serde_json::Value = serde_json::from_str(&restored).unwrap();
    assert_eq!(
        restored[0]["locations"][0]["caret"]["file"],
        "D:\\consumer\\b\"quoted\\unit.c"
    );
    assert_eq!(restored[0]["message"], value[0]["message"]);
    assert_eq!(restore(&stored, &producer).unwrap(), original);
}

#[test]
fn many_unicode_locations_keep_their_order_and_exact_text() {
    let text = (0..1000)
        .map(|line| format!("/生产/é/unit_{line}.c:{line}: warning\r\n"))
        .collect::<String>();
    let stored = store(&text, &roots("/生产/é"));
    assert_eq!(
        restore(&stored, &roots("/消费/β")).unwrap(),
        text.replace("/生产/é", "/消费/β")
    );
    assert_eq!(restore(&stored, &roots("/生产/é")).unwrap(), text);
}

#[test]
fn unchanged_diagnostics_stay_borrowed_and_literal_record_markers_are_escaped() {
    for text in ["", "warning: no path\n", " /work/AB/a.c:1: warning\n"] {
        assert!(matches!(store(text, &roots("/work/A")), Cow::Borrowed(_)));
        assert!(matches!(restore(text, &[]).unwrap(), Cow::Borrowed(_)));
    }
    let literal = format!("{PREFIX}not a record, just compiler text");
    let stored = store(&literal, &[]);
    assert_eq!(restore(&stored, &[]).unwrap(), literal);
    assert!(matches!(
        store("/a.c:1: warning\n", &roots("/")),
        Cow::Borrowed(_)
    ));
}

#[test]
fn malformed_ranges_and_missing_roots_are_rejected_without_panics() {
    assert!(restore(&format!("{PREFIX}bad json"), &[]).is_err());
    let encoded = |text: &str, paths: Vec<Location>| {
        format!(
            "{PREFIX}{}",
            serde_json::to_string(&Record {
                text: text.into(),
                paths
            })
            .unwrap()
        )
    };
    let location = |start, end, root: &str| Location {
        start,
        end,
        root: root.into(),
        json: false,
    };
    for paths in [
        vec![location(0, 0, "checkout")],
        vec![location(2, 1, "checkout")],
        vec![location(100, 101, "checkout")],
        vec![location(0, 100, "checkout")],
        vec![location(1, 3, "checkout")],
        vec![location(0, 1, "checkout")],
        vec![location(0, 2, "checkout"), location(0, 2, "checkout")],
        vec![location(0, 2, "absent")],
    ] {
        assert!(restore(&encoded("é/x", paths), &roots("/B")).is_err());
    }
    let stored = store("/A/a.c:1: warning\n", &roots("/A"));
    assert!(restore(&stored, &[]).is_err());
    assert!(restore(&stored, &roots("/")).is_err());
}

#[test]
fn json_location_prefixes_and_color_sequences_are_recognized_conservatively() {
    assert!(json_file_prefix("{\"file\": \""));
    assert!(json_file_prefix("{\"file\" :\""));
    for prefix in ["", "{\"message\": \"", "{\"file\" \"", "{\"file\": "] {
        assert!(!json_file_prefix(prefix));
    }
    assert_eq!(
        strip_sgr("In file \x1b[1mincluded\x1b[0m from "),
        "In file included from "
    );
    assert_eq!(strip_sgr("plain"), "plain");
    assert_eq!(strip_sgr("\x1b[unterminated"), "\x1b[unterminated");
    assert_eq!(strip_sgr("\x1b[1;38:2mfrom \x1b[m"), "from ");
    assert_eq!(strip_sgr("\x1b[123;"), "\x1b[123;");
}

#[cfg(unix)]
#[test]
fn non_utf8_roots_do_not_guess_a_path_spelling() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let opaque_roots = vec![(
        PathBuf::from(OsString::from_vec(b"/A/\xff".to_vec())),
        "checkout".into(),
        5,
    )];
    assert!(matches!(
        store("/A/�/a.c:1: warning\n", &opaque_roots),
        Cow::Borrowed(_)
    ));
    let stored = store("/A/a.c:1: warning\n", &roots("/A"));
    assert!(restore(&stored, &opaque_roots).is_err());
}
