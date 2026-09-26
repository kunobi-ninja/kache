# kache-core

[![crates.io](https://img.shields.io/crates/v/kache-core.svg)](https://crates.io/crates/kache-core)
[![docs.rs](https://img.shields.io/docsrs/kache-core)](https://docs.rs/kache-core)

Planner data types for [Kache](https://github.com/kunobi-ninja/kache), a compiler cache for Rust, C/C++ and CUDA. The Kache client and the planner service both build on them.

## What it provides

- `BuildIntent`: the crates, `Cargo.lock` dependencies and identity key of a build, which the client sends to a planner.
- `PrefetchPlan` and `PrefetchCandidate`: the ranked entries a planner suggests downloading before rustc asks for them.
- `PlanLimits`: per-source caps that stop one low-confidence source from crowding out better candidates.
- `PlannerDataSource`: the trait a planner implements to answer a build intent.
- `timeline`: build timeline records, one per build session, with every time in Unix epoch milliseconds and no filesystem paths.

## Features

| Feature | Default | Adds |
| --- | --- | --- |
| `planning` | yes | `PlannerDataSource`, `dispatch_sort_key` and the plan composition helpers |

Without `planning`, the crate has the serializable types only and depends on nothing but Serde.

## Versioning

Released with Kache under the same version number. The minimum Rust version is 1.93.

## License

Apache-2.0
