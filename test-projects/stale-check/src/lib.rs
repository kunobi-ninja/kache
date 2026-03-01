mod helper;

pub fn value() -> &'static str {
    "v1"
}

pub use helper::helper_value;

pub fn env_value() -> &'static str {
    option_env!("KACHE_TEST_VALUE").unwrap_or("default")
}

pub fn profile_value() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

pub fn feature_tag() -> &'static str {
    if cfg!(feature = "fancy") {
        "fancy"
    } else {
        "plain"
    }
}
