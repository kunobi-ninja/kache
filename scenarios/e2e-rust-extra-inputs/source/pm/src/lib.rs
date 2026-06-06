use proc_macro::TokenStream;

/// `bake!()` expands to the trimmed contents of `<using-crate>/data/value.txt`,
/// read at compile time with `std::fs` — exactly how sqlx's `query!` reads
/// `.sqlx/*.json`. The crucial property for this fixture: rustc does NOT record
/// this read in its dep-info, so editing `data/value.txt` changes `app`'s
/// compiled output while leaving every `.rs` file (and this proc-macro's rlib)
/// untouched. Without the co-located `kache.toml` declaring the file as an
/// extra input, kache would serve a stale `app` binary — the false hit the
/// feature exists to prevent.
#[proc_macro]
pub fn bake(_input: TokenStream) -> TokenStream {
    let dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let path = std::path::Path::new(&dir).join("data/value.txt");
    let value = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    // `{:?}` renders the value as an escaped Rust string literal token.
    format!("{:?}", value.trim()).parse().unwrap()
}
