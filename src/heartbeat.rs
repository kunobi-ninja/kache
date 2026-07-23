//! In-flight compile heartbeats (kunobi-ninja/kache#131).
//!
//! While a cache-miss compile runs, the wrapper is otherwise silent until the
//! compiler exits — for a long crate (Firefox's gkrust runs ~8 minutes) that
//! reads as "kache hung" to users, TTY-throttling build drivers, and stderr
//! parsers alike. This module makes the wait observable from the only process
//! that can: the wrapper is the compiler child's parent AND the owner of the
//! build's stderr (the daemon's stderr goes to its own log file, so it cannot
//! speak to the user mid-build). A monitor thread ticks while the child runs
//! and, once per cadence:
//!
//! - prints `[kache] still compiling <crate> — 4m20s elapsed (typical: 7m51s,
//!   ETA 3m31s)` to stderr,
//! - appends a structured [`HeartbeatEvent`] to `events.jsonl` for non-TTY
//!   consumers (TTY throttling — mozilla's mach — can eat stderr, the event
//!   log can't),
//! - samples the child's CPU time and WARNs once if it looks stuck (<1% CPU
//!   over 60 s: swap thrashing, deadlock, store-lock contention — the cases
//!   that otherwise present as "kache is slow").
//!
//! The typical/ETA suffix comes from [`crate::events::typical_compile_ms`],
//! computed lazily on the FIRST tick — a compile that finishes inside one
//! cadence never reads the event log, so heartbeats cost a cold build with
//! hundreds of fast misses nothing.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::events::{HEARTBEAT_EVENT_TAG, HEARTBEAT_SCHEMA, HeartbeatEvent};

/// Stuck detection window: WARN when the child accrues <1% CPU over this span.
const STUCK_WINDOW: Duration = Duration::from_secs(60);
/// Stuck detection CPU-usage floor (fraction of one core).
const STUCK_CPU_FLOOR: f64 = 0.01;

/// Per-process heartbeat context, set once at wrapper entry (each compiler
/// invocation runs under its own wrapper process, so process-global is
/// per-compile — the same pattern as `link.rs`'s restore toggles).
struct HeartbeatCtx {
    /// Cadence in seconds; `set_heartbeat_ctx` is never called with 0.
    cadence_secs: u64,
    event_log: PathBuf,
    /// Daemon socket, for the fire-and-forget in-flight registry
    /// (CompileStarted/CompileFinished). The daemon may not be running;
    /// sends silently no-op then.
    socket: PathBuf,
    /// Build tree/root, same value the wrapper stamps on `BuildEvent`s.
    root: String,
}

static CTX: OnceLock<HeartbeatCtx> = OnceLock::new();

/// Install the heartbeat context (from `Config::heartbeat_secs` and the
/// derived event root). Call once per process before the compile; not calling
/// it — or a `cadence_secs` of 0 — leaves heartbeats off.
pub fn set_heartbeat_ctx(cadence_secs: u64, event_log: PathBuf, socket: PathBuf, root: String) {
    if cadence_secs == 0 {
        return;
    }
    let _ = CTX.set(HeartbeatCtx {
        cadence_secs,
        event_log,
        socket,
        root,
    });
}

/// Handle to a running monitor thread. Dropping it without calling
/// [`CompileMonitor::finish`] also stops the thread (abnormal exits must not
/// leave a detached ticker printing about a dead compile). No completion line
/// is printed on finish: the wrapper's normal event logging covers it, and a
/// "finished" line after rustc's own output would read as noise.
pub struct CompileMonitor {
    stop: std::sync::Arc<AtomicBool>,
    // Wakes the ticker immediately on finish so process exit never waits out
    // a sleep. `recv_timeout` doubles as the tick sleep.
    wake_tx: mpsc::Sender<()>,
    handle: Option<std::thread::JoinHandle<()>>,
}

/// Start monitoring a spawned compiler child. Returns `None` (a no-op) when
/// no heartbeat context is installed — callers need no gating of their own.
pub fn start_monitor(crate_name: &str, pid: u32) -> Option<CompileMonitor> {
    let ctx = CTX.get()?;
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let (wake_tx, wake_rx) = mpsc::channel::<()>();
    let thread_stop = stop.clone();
    let crate_name = crate_name.to_string();
    let handle = std::thread::Builder::new()
        .name("kache-heartbeat".into())
        .spawn(move || monitor_loop(ctx, &crate_name, pid, &thread_stop, &wake_rx))
        .ok()?;
    Some(CompileMonitor {
        stop,
        wake_tx,
        handle: Some(handle),
    })
}

impl CompileMonitor {
    /// Stop the ticker and join it. Prints a completion line only if at least
    /// one heartbeat was emitted — quiet compiles stay quiet.
    pub fn finish(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.wake_tx.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for CompileMonitor {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn monitor_loop(
    ctx: &HeartbeatCtx,
    crate_name: &str,
    pid: u32,
    stop: &AtomicBool,
    wake_rx: &mpsc::Receiver<()>,
) {
    let started_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // Register with the daemon's in-flight registry (best-effort; the daemon
    // may not be running). Registration happens on THIS thread so the compile
    // hot path never pays a socket connect.
    crate::daemon::send_compile_started(
        &ctx.socket,
        crate::daemon::CompileStartedRequest {
            crate_name: crate_name.to_string(),
            root: ctx.root.clone(),
            pid,
            started_at_ms,
            typical_ms: None,
            client_epoch: 0,
        },
    );
    run_ticks(ctx, crate_name, pid, started_at_ms, stop, wake_rx);
    crate::daemon::send_compile_finished(&ctx.socket, pid, started_at_ms);
}

fn run_ticks(
    ctx: &HeartbeatCtx,
    crate_name: &str,
    pid: u32,
    started_at_ms: u64,
    stop: &AtomicBool,
    wake_rx: &mpsc::Receiver<()>,
) {
    let started = Instant::now();
    let cadence = Duration::from_secs(ctx.cadence_secs);
    // Lazily resolved on the first tick — see module docs.
    let mut typical_s: Option<Option<u64>> = None;
    // (sampled_at, cpu_time) anchor for the stuck window; refreshed whenever
    // usage rises above the floor so the WARN needs a FULL quiet window.
    let mut stuck_anchor: Option<(Instant, Duration)> = None;
    let mut stuck_warned = false;

    loop {
        match wake_rx.recv_timeout(cadence) {
            // finish() poked us, or the sender vanished with the parent.
            Ok(()) => return,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }

        let elapsed = started.elapsed();
        let first_tick = typical_s.is_none();
        let typical = *typical_s.get_or_insert_with(|| {
            crate::events::typical_compile_ms(&ctx.event_log, crate_name, &ctx.root)
                .map(|ms| ms.div_ceil(1000))
        });
        if first_tick && typical.is_some() {
            // Refresh the registry entry now that the typical time is known
            // (upsert by pid — idempotent on the daemon side).
            crate::daemon::send_compile_started(
                &ctx.socket,
                crate::daemon::CompileStartedRequest {
                    crate_name: crate_name.to_string(),
                    root: ctx.root.clone(),
                    pid,
                    started_at_ms,
                    typical_ms: typical.map(|s| s * 1000),
                    client_epoch: 0,
                },
            );
        }
        let eta = typical.map(|t| t.saturating_sub(elapsed.as_secs()));

        let suffix = match (typical, eta) {
            (Some(t), Some(e)) => format!(
                " (typical: {}, ETA {})",
                format_secs(t),
                format_secs(e.max(1))
            ),
            _ => String::new(),
        };
        eprintln!(
            "[kache] still compiling {} — {} elapsed{}",
            crate_name,
            format_secs(elapsed.as_secs()),
            suffix
        );

        let hb = HeartbeatEvent {
            event: HEARTBEAT_EVENT_TAG.to_string(),
            ts: chrono::Utc::now(),
            crate_name: crate_name.to_string(),
            root: ctx.root.clone(),
            pid,
            elapsed_s: elapsed.as_secs(),
            typical_s: typical,
            eta_s: eta,
            schema: HEARTBEAT_SCHEMA,
        };
        if let Err(e) = crate::events::log_heartbeat(&ctx.event_log, &hb) {
            tracing::debug!("heartbeat event write failed: {e:#}");
        }

        // Stuck detection piggybacks on the same tick. A sampling failure
        // (permissions, exotic platform) silently disables it — a diagnostic
        // must never degrade the build.
        if !stuck_warned && let Some(cpu) = child_cpu_time(pid) {
            let now = Instant::now();
            match stuck_anchor {
                None => stuck_anchor = Some((now, cpu)),
                Some((anchor_at, anchor_cpu)) => {
                    let wall = now.duration_since(anchor_at);
                    let used = cpu.saturating_sub(anchor_cpu);
                    if used.as_secs_f64() > wall.as_secs_f64() * STUCK_CPU_FLOOR {
                        // Working again — restart the quiet window.
                        stuck_anchor = Some((now, cpu));
                    } else if wall >= STUCK_WINDOW {
                        tracing::warn!(
                            "[kache] rustc for {} appears stuck (<1% CPU for {}s, PID {})",
                            crate_name,
                            wall.as_secs(),
                            pid
                        );
                        eprintln!(
                            "[kache] WARN: rustc for {} appears stuck (<1% CPU for {}s, PID {})",
                            crate_name,
                            wall.as_secs(),
                            pid
                        );
                        stuck_warned = true;
                    }
                }
            }
        }
    }
}

/// `4m20s` / `51s` / `2h05m` — compact duration for heartbeat lines.
fn format_secs(total: u64) -> String {
    let (h, m, s) = (total / 3600, (total % 3600) / 60, total % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

/// Total CPU time (user+system) the child has accrued, or `None` when the
/// platform gives no cheap answer. `getrusage(RUSAGE_CHILDREN)` is useless
/// here — it only counts *reaped* children — so this reads the live process.
#[cfg(target_os = "linux")]
fn child_cpu_time(pid: u32) -> Option<Duration> {
    // /proc/<pid>/stat fields 14 (utime) + 15 (stime), in clock ticks. The
    // comm field (2) may contain spaces/parens, so parse after the LAST ')'.
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &stat[stat.rfind(')')? + 2..];
    let mut fields = rest.split_ascii_whitespace();
    let utime: u64 = fields.nth(11)?.parse().ok()?; // field 14, 0-indexed 11 after state
    let stime: u64 = fields.next()?.parse().ok()?;
    let tick = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if tick <= 0 {
        return None;
    }
    Some(Duration::from_secs_f64(
        (utime + stime) as f64 / tick as f64,
    ))
}

#[cfg(target_os = "macos")]
// libc deprecates its mach_* bindings in favor of the `mach2` crate; one
// timebase struct does not justify a new dependency, so use them anyway.
#[allow(deprecated)]
fn child_cpu_time(pid: u32) -> Option<Duration> {
    // proc_pidinfo(PROC_PIDTASKINFO) reports pti_total_user/system in Mach
    // absolute time units — NOT nanoseconds on Apple Silicon (24 MHz tick),
    // so convert via mach_timebase_info or the <1%-CPU ratio is off by ~42x.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let got = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    if got != size {
        return None;
    }
    let mut tb = libc::mach_timebase_info { numer: 0, denom: 0 };
    if unsafe { libc::mach_timebase_info(&mut tb) } != libc::KERN_SUCCESS || tb.denom == 0 {
        return None;
    }
    let mach_units = info.pti_total_user + info.pti_total_system;
    let nanos = mach_units as u128 * tb.numer as u128 / tb.denom as u128;
    Some(Duration::from_nanos(nanos.min(u64::MAX as u128) as u64))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn child_cpu_time(_pid: u32) -> Option<Duration> {
    // Windows would need GetProcessTimes on an opened handle; deferred until
    // someone needs it — returning None disables stuck detection only, the
    // heartbeat itself is platform-independent.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_secs_is_compact() {
        assert_eq!(format_secs(51), "51s");
        assert_eq!(format_secs(260), "4m20s");
        assert_eq!(format_secs(471), "7m51s");
        assert_eq!(format_secs(7500), "2h05m");
    }

    /// The sampler must report a live process's CPU time on supported
    /// platforms — sample our own PID, which is certainly alive and has
    /// certainly burned some CPU.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn child_cpu_time_reads_a_live_process() {
        let cpu = child_cpu_time(std::process::id());
        assert!(cpu.is_some(), "self-sample must succeed");
    }
}
