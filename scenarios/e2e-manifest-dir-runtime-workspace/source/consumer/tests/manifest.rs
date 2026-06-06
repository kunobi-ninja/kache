use std::path::Path;

#[test]
fn helper_manifest_dir_matches_this_checkout() {
    let embedded = Path::new(consumer::helper_manifest_dir())
        .canonicalize()
        .unwrap();
    let expected = std::env::current_dir()
        .unwrap()
        .parent()
        .unwrap()
        .join("helper")
        .canonicalize()
        .unwrap();
    assert_eq!(embedded, expected);
}
