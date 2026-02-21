// Phase 3: Smart prefetch algorithm
//
// Five signals combined into a final score:
// 1. Build history (0.95 same-branch, 0.80 base-branch)
// 2. Git diff modifier (src-only → boost deps; Cargo.toml → reduce deps)
// 3. Dep fan-out boost (high-fanout crates from dep_edges)
// 4. Cross-branch (0.75 for new branches using cargo_lock_hash matching)
// 5. Time decay (penalty after 7d, up to -50%)
//
// Score = confidence * compile_time_ms / max(estimated_download_ms, 1)
