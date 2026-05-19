use proc_macro::TokenStream;

/// A trivial proc-macro: `answer!()` expands to the literal `42u32`.
#[proc_macro]
pub fn answer(_input: TokenStream) -> TokenStream {
    "42u32".parse().unwrap()
}
