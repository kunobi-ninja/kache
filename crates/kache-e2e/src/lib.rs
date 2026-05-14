//! End-to-end test harness for kache.
//!
//! Drives example projects through a fixed lifecycle (cold → warm → noop) and
//! asserts per-fixture contracts against kache's own JSON report. The harness
//! is the single source of truth for "kache works end-to-end" coverage; the
//! per-language bash scripts it replaces shipped per-language assertions in
//! shell, which made the contract opaque and impossible to extend without
//! growing more scripts.
//!
//! Each fixture is self-contained: a `kache-fixture.toml` declares the env,
//! commands, verification step, and per-phase assertions. The harness owns
//! the lifecycle (which phases run in what order); the toml owns *what this
//! fixture is and what it expects*.

pub mod assertions;
pub mod fixture;
pub mod report;
pub mod result;
pub mod runner;
