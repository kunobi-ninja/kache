//! Opt-in, monotonic wrapper intervals for Perfetto diagnostics.
//! One file per wrapper avoids a shared trace lock during concurrent builds.
//! This records the calling thread; it does not infer worker-thread intervals.

use serde_json::{Value, json};
use std::cell::RefCell;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const MAX_EVENTS: usize = 4096;
thread_local! {
    static ACTIVE: RefCell<Option<Recorder>> = const { RefCell::new(None) };
}

struct Recorder {
    directory: PathBuf,
    start: Instant,
    epoch_us: u64,
    adapter: &'static str,
    unit: String,
    events: Vec<Value>,
    dropped: usize,
}

impl Recorder {
    fn push(&mut self, event: Value) {
        if self.events.len() < MAX_EVENTS {
            self.events.push(event);
        } else {
            self.dropped += 1;
        }
    }

    fn interval(&mut self, name: &str, start_us: u64, duration_us: u64) {
        self.push(json!({
            "name": name, "cat": self.adapter, "ph": "X",
            "pid": std::process::id(), "tid": 0,
            "ts": self.epoch_us.saturating_add(start_us), "dur": duration_us,
        }));
    }
}

pub(crate) struct Invocation(bool);
pub(crate) struct Phase(Option<(&'static str, u64)>);

pub(crate) fn start(adapter: &'static str, args: &[String]) -> Invocation {
    start_at(
        std::env::var_os("KACHE_PHASE_TRACE_DIR").map(PathBuf::from),
        adapter,
        args,
    )
}

fn start_at(directory: Option<PathBuf>, adapter: &'static str, args: &[String]) -> Invocation {
    let Some(directory) = directory.filter(|path| !path.as_os_str().is_empty()) else {
        return Invocation(false);
    };
    ACTIVE.with(|active| {
        let mut active = active.borrow_mut();
        if active.is_some() {
            return Invocation(false);
        }
        *active = Some(Recorder {
            directory,
            start: Instant::now(),
            epoch_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |time| time.as_micros().try_into().unwrap_or(u64::MAX)),
            adapter,
            unit: args
                .windows(2)
                .find(|pair| pair[0] == "--crate-name")
                .map_or_else(|| adapter.to_string(), |pair| pair[1].clone()),
            events: Vec::new(),
            dropped: 0,
        });
        Invocation(true)
    })
}

pub(crate) fn phase(name: &'static str) -> Phase {
    Phase(ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(|recorder| (name, micros(recorder.start.elapsed())))
    }))
}

pub(crate) fn decision(name: &'static str, outcome: &str) {
    ACTIVE.with(|active| {
        if let Some(recorder) = active.borrow_mut().as_mut() {
            let ts = recorder
                .epoch_us
                .saturating_add(micros(recorder.start.elapsed()));
            recorder.push(json!({
                "name": name, "cat": recorder.adapter, "ph": "i", "s": "t",
                "pid": std::process::id(), "tid": 0, "ts": ts,
                "args": {"outcome": outcome},
            }));
        }
    });
}

fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

impl Drop for Phase {
    fn drop(&mut self) {
        if let Some((name, start)) = self.0 {
            ACTIVE.with(|active| {
                if let Some(recorder) = active.borrow_mut().as_mut() {
                    let duration = micros(recorder.start.elapsed()).saturating_sub(start);
                    recorder.interval(name, start, duration);
                }
            });
        }
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        if !self.0 {
            return;
        }
        let recorder = ACTIVE.with(|active| active.borrow_mut().take());
        if let Some(mut recorder) = recorder {
            let elapsed = micros(recorder.start.elapsed());
            let root = json!({
                "name": recorder.unit, "cat": recorder.adapter, "ph": "X",
                "pid": std::process::id(), "tid": 0, "ts": recorder.epoch_us,
                "dur": elapsed, "args": {"dropped_events": recorder.dropped},
            });
            recorder.events.insert(0, root);
            // Diagnostic I/O must not change compiler output or its exit status.
            let _ = (|| -> std::io::Result<()> {
                std::fs::create_dir_all(&recorder.directory)?;
                let mut file = tempfile::Builder::new()
                    .prefix("wrapper-")
                    .suffix(".trace.json")
                    .tempfile_in(&recorder.directory)?;
                serde_json::to_writer(
                    &mut file,
                    &json!({
                        "traceEvents": recorder.events,
                        "displayTimeUnit": "ms",
                        "kache_trace": {"version": 1, "clock": "monotonic", "scope": "wrapper-thread"},
                    }),
                )?;
                file.keep().map_err(|error| error.error)?;
                Ok(())
            })();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_real_nested_intervals_and_decisions() {
        let directory = tempfile::tempdir().unwrap();
        {
            let _invocation = start_at(
                Some(directory.path().into()),
                "rustc",
                &["--crate-name".into(), "subject".into()],
            );
            let _key = phase("key");
            std::thread::sleep(std::time::Duration::from_millis(2));
            {
                let _child = phase("dep-info");
                decision("prediction", "not-eligible");
            }
        }
        let path = std::fs::read_dir(directory.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let data: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let events = data["traceEvents"].as_array().unwrap();
        assert_eq!(events[0]["name"], "subject");
        let named = |name| events.iter().find(|e| e["name"] == name).unwrap();
        let start = |e: &Value| e["ts"].as_u64().unwrap();
        let end = |e: &Value| start(e) + e["dur"].as_u64().unwrap();
        assert!(start(named("dep-info")) > start(named("key")));
        assert!(end(named("dep-info")) <= end(named("key")));
        assert!(end(named("key")) <= end(&events[0]));
        assert_eq!(named("prediction")["args"]["outcome"], "not-eligible");
    }

    #[test]
    fn disabled_and_nested_invocations_do_not_replace_the_recorder() {
        let directory = tempfile::tempdir().unwrap();
        let outer = start_at(Some(directory.path().into()), "rustc", &[]);
        drop(start_at(None, "cc", &[]));
        drop(start_at(Some(directory.path().into()), "cc", &[]));
        decision("prediction", "used");
        assert!(ACTIVE.with(|active| active.borrow().is_some()));
        drop(outer);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        drop(start_at(None, "cc", &[]));
        assert!(ACTIVE.with(|active| active.borrow().is_none()));
    }

    #[test]
    fn bounded_trace_reports_dropped_events_without_affecting_work() {
        let directory = tempfile::tempdir().unwrap();
        let invocation = start_at(Some(directory.path().into()), "rustc", &[]);
        for _ in 0..4100 {
            decision("prediction", "used");
        }
        drop(invocation);
        let path = std::fs::read_dir(directory.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let data: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(data["traceEvents"].as_array().unwrap().len(), 4097);
        assert_eq!(data["traceEvents"][0]["args"]["dropped_events"], 4);
        let file = tempfile::NamedTempFile::new().unwrap();
        drop(start_at(Some(file.path().into()), "rustc", &[]));
    }
}
