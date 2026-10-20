//! Helpers for tests that change process state.

pub use kache_fs::process_state_test::process_state_test_lock as config_path_lock;
use std::ffi::OsString;

pub struct TestEnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        unsafe {
            match self.previous.as_ref() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

pub fn set_env_for_test(key: &'static str, value: Option<&std::ffi::OsStr>) -> TestEnvGuard {
    let previous = std::env::var_os(key);
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
    TestEnvGuard { key, previous }
}
