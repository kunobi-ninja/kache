fn main() {
    println!(
        "{}.{}.{}.{}.{}",
        stale_check::value(),
        stale_check::helper_value(),
        stale_check::env_value(),
        stale_check::profile_value(),
        stale_check::feature_tag(),
    );
}
