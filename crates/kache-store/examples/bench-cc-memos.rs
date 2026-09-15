//! Compare legacy JSON and shared-input storage using an exported memo sample.
//! Run with `--features test-support --example bench-cc-memos -- SAMPLE OUTPUT`.

use anyhow::{Context, Result, ensure};
use kache_store::file_hash::{CcPreprocessMemoInput, FileHashCache};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Instant};

#[derive(Deserialize)]
struct Sample {
    memo_key: String,
    preprocessed_hash: String,
    inputs: Vec<CcPreprocessMemoInput>,
}

#[derive(Serialize)]
struct Measurement {
    backend: &'static str,
    phase: &'static str,
    sample: usize,
    elapsed_ms: f64,
}

fn elapsed(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    ensure!(
        args.len() == 3,
        "usage: bench-cc-memos SAMPLE.json OUTPUT_DIR"
    );
    let samples: Vec<Sample> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    ensure!(!samples.is_empty(), "sample must contain memos");
    let output = Path::new(&args[2]);
    std::fs::create_dir(output).context("output directory must be new")?;
    let legacy_path = output.join("legacy.db");
    let shared_path = output.join("shared.db");
    let legacy = Connection::open(&legacy_path)?;
    legacy.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
        CREATE TABLE cc_preprocess_memos(memo_key TEXT PRIMARY KEY,
        preprocessed_hash TEXT NOT NULL, inputs_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now')));",
    )?;
    let shared = FileHashCache::open(&shared_path)?;
    let mut measurements = Vec::new();
    let start = Instant::now();
    for sample in &samples {
        legacy.execute(
            "INSERT OR REPLACE INTO cc_preprocess_memos
            (memo_key, preprocessed_hash, inputs_json) VALUES (?1, ?2, ?3)",
            params![
                sample.memo_key,
                sample.preprocessed_hash,
                serde_json::to_string(&sample.inputs)?
            ],
        )?;
    }
    measurements.push(Measurement {
        backend: "legacy",
        phase: "write",
        sample: 0,
        elapsed_ms: elapsed(start),
    });
    let start = Instant::now();
    for sample in &samples {
        shared.put_cc_preprocess_memo_inputs(
            &sample.memo_key,
            &sample.preprocessed_hash,
            &sample.inputs,
        )?;
    }
    measurements.push(Measurement {
        backend: "shared",
        phase: "write",
        sample: 0,
        elapsed_ms: elapsed(start),
    });
    for trial in 0..6 {
        for shared_first in [trial % 2 == 0, trial % 2 != 0] {
            let start = Instant::now();
            for sample in &samples {
                let (hash, inputs) = if shared_first {
                    let memo = shared
                        .get_cc_preprocess_memo(&sample.memo_key)?
                        .context("missing shared memo")?;
                    (memo.preprocessed_hash, memo.inputs)
                } else {
                    let (hash, json): (String, String) = legacy.query_row(
                        "SELECT preprocessed_hash, inputs_json FROM cc_preprocess_memos WHERE memo_key = ?1",
                        [&sample.memo_key], |row| Ok((row.get(0)?, row.get(1)?)))?;
                    (
                        hash,
                        serde_json::from_str::<Vec<CcPreprocessMemoInput>>(&json)?,
                    )
                };
                ensure!(
                    hash == sample.preprocessed_hash && inputs.len() == sample.inputs.len(),
                    "memo changed"
                );
                std::hint::black_box(inputs);
            }
            measurements.push(Measurement {
                backend: if shared_first { "shared" } else { "legacy" },
                phase: "read",
                sample: trial,
                elapsed_ms: elapsed(start),
            });
        }
    }
    legacy.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    shared
        .db()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let unique: i64 = shared
        .db()
        .query_row("SELECT count(*) FROM cc_memo_inputs", [], |r| r.get(0))?;
    let report = serde_json::json!({
        "scope": "SQLite memo storage only; compiler and filesystem validation excluded",
        "memos": samples.len(), "input_references": samples.iter().map(|s| s.inputs.len()).sum::<usize>(),
        "unique_inputs": unique, "legacy_file_bytes": std::fs::metadata(legacy_path)?.len(),
        "shared_file_bytes": std::fs::metadata(shared_path)?.len(), "measurements": measurements,
    });
    let report = serde_json::to_string_pretty(&report)?;
    std::fs::write(output.join("report.json"), &report)?;
    println!("{report}");
    Ok(())
}
