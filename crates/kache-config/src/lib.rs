//! Invocation inputs and configuration shared by the client and engine.

pub mod config;
pub mod invocation;
pub mod policy;
pub use config::Config;
pub use invocation::InvocationEnv;

/// Scheduler lease inherited by nested compiler invocations.
pub const TEST_LEASE_ENV: &str = "KACHE_TEST_LEASE";

#[cfg(any(test, feature = "test-support"))]
pub mod testing;
