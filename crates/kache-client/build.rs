use heck::ToKebabCase;
use std::collections::BTreeSet;
use std::path::PathBuf;
use syn::{Expr, Item, Lit, Meta};

fn string(expr: &Expr) -> String {
    let Expr::Lit(literal) = expr else {
        panic!("command routing names must be string literals")
    };
    let Lit::Str(value) = &literal.lit else {
        panic!("command routing names must be strings")
    };
    value.value()
}

fn main() {
    println!("cargo:rerun-if-changed=src/commands.rs");
    let schema = std::fs::read_to_string("src/commands.rs").expect("read engine command schema");
    let parsed = syn::parse_file(&schema).expect("parse engine command schema");
    let commands = parsed
        .items
        .into_iter()
        .find_map(|item| match item {
            Item::Enum(item) if item.ident == "Commands" => Some(item),
            _ => None,
        })
        .expect("Commands enum in engine schema");
    let mut names = BTreeSet::from(["help".to_owned()]);
    for variant in commands.variants {
        let mut name = variant.ident.to_string().to_kebab_case();
        for attr in variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("command"))
        {
            let items = attr
                .parse_args_with(
                    syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                )
                .expect("parse command attributes");
            for item in items {
                match item {
                    Meta::NameValue(value) if value.path.is_ident("name") => {
                        name = string(&value.value)
                    }
                    Meta::NameValue(value)
                        if value.path.is_ident("alias") || value.path.is_ident("visible_alias") =>
                    {
                        names.insert(string(&value.value));
                    }
                    Meta::NameValue(value)
                        if value.path.is_ident("aliases")
                            || value.path.is_ident("visible_aliases") =>
                    {
                        let Expr::Array(array) = value.value else {
                            panic!("command aliases must be a literal array")
                        };
                        names.extend(array.elems.iter().map(string));
                    }
                    Meta::Path(path)
                        if path.is_ident("flatten") || path.is_ident("external_subcommand") =>
                    {
                        panic!("update client routing for flattened or external commands")
                    }
                    _ => {}
                }
            }
        }
        names.insert(name);
    }
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
    std::fs::write(output.join("commands.rs"), format!("#[doc(hidden)]\n#[macro_export]\nmacro_rules! define_engine_commands {{ () => {{ {schema} }}; }}\npub fn engine_command_names() -> &'static [&'static str] {{ &{:?} }}\n", names.into_iter().collect::<Vec<_>>())).expect("write shared command definition");
}
