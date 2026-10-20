pub(crate) use kache_config::config::*;

#[cfg(test)]
pub(crate) use kache_config::testing::config_path_lock;

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use kache_config::testing::{config_path_lock, set_env_for_test};
}
