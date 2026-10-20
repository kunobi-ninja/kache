//! In-flight compile heartbeats (kunobi-ninja/kache#131).
//!
//! While a cache-miss compile runs, the wrapper is otherwise silent until the
//! compiler exits — for a long crate (Firefox's gkrust runs ~8 minutes) that
//! reads as "kache hung" to users, TTY-throttling build drivers, and stderr
//! parsers alike. The monitor borrows the compiler request's output channels;
//! the daemon's own stderr remains separate. A monitor thread ticks while the child runs
//! and, once per cadence:
//!
//! - with `KACHE_PROGRESS=verbose`, prints `[kache] still compiling <crate> —
//!   4m20s elapsed (typical: 7m51s, ETA 3m31s)` to stderr,
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

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::events::{HEARTBEAT_EVENT_TAG, HEARTBEAT_SCHEMA, HeartbeatEvent};

/// Stuck detection window: WARN when the child accrues <1% CPU over this span.
const STUCK_WINDOW: Duration = Duration::from_secs(60);
/// Stuck detection CPU-usage floor (fraction of one core).
const STUCK_CPU_FLOOR: f64 = 0.01;

/// Heartbeat configuration owned by an invocation and its monitor thread.
struct HeartbeatCtx {
    /// Nonzero cadence in seconds; a cadence of 0 installs no context.
    cadence_secs: u64,
    event_log: PathBuf,
    /// Daemon socket, for the fire-and-forget in-flight registry
    /// (CompileStarted/CompileFinished). The daemon may not be running;
    /// sends silently no-op then.
    socket: PathBuf,
    /// Build tree/root, same value the wrapper stamps on `BuildEvent`s.
    root: String,
    /// Whether to also show a person the heartbeat line
    /// ([`crate::notice`]). Opt-in only.
    show_lines: bool,
}

static CTX: OnceLock<Arc<HeartbeatCtx>> = OnceLock::new();

type InvocationHeartbeatCtx = Rc<RefCell<Option<Arc<HeartbeatCtx>>>>;

thread_local! {
    static INVOCATION_CTX: RefCell<Option<InvocationHeartbeatCtx>> = const { RefCell::new(None) };
}

/// Own heartbeat configuration for one synchronous compiler invocation.
///
/// Each scope starts disabled. Nested scopes restore their parent on drop.
/// The guard cannot move between threads and must not span an async suspension
/// or task switch. A spawned monitor retains its own context after scope exit.
pub(crate) struct InvocationHeartbeatScope {
    previous: Option<InvocationHeartbeatCtx>,
    _current: InvocationHeartbeatCtx,
}

impl InvocationHeartbeatScope {
    pub(crate) fn enter() -> Self {
        let current = Rc::new(RefCell::new(None));
        let previous = INVOCATION_CTX.with(|slot| slot.replace(Some(Rc::clone(&current))));
        Self {
            previous,
            _current: current,
        }
    }
}

impl Drop for InvocationHeartbeatScope {
    fn drop(&mut self) {
        INVOCATION_CTX.with(|slot| slot.replace(self.previous.take()));
    }
}

fn current_heartbeat_ctx() -> Option<Arc<HeartbeatCtx>> {
    INVOCATION_CTX.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or_else(|| CTX.get().cloned(), |current| current.borrow().clone())
    })
}

/// Install the heartbeat context (from `Config::heartbeat_secs` and the
/// derived event root). In an invocation scope, a cadence of 0 disables the
/// current context. Outside a scope, the first nonzero configuration wins.
pub fn set_heartbeat_ctx(
    cadence_secs: u64,
    event_log: PathBuf,
    socket: PathBuf,
    root: String,
    show_lines: bool,
) {
    let ctx = (cadence_secs != 0).then(|| {
        Arc::new(HeartbeatCtx {
            cadence_secs,
            event_log,
            socket,
            root,
            show_lines,
        })
    });
    INVOCATION_CTX.with(|slot| {
        if let Some(current) = slot.borrow().as_ref() {
            current.replace(ctx);
        } else if let Some(ctx) = ctx {
            let _ = CTX.set(ctx);
        }
    });
}

/// Handle to a running monitor thread. Dropping it stops the ticker, including
/// on abnormal exits. Shutdown waits at most 50 ms for the ticker: a stalled
/// diagnostic must not hold up compiler exit. No completion line is printed
/// on shutdown: the wrapper's normal event logging covers it, and a
/// "finished" line after rustc's own output would read as noise.
pub struct CompileMonitor {
    stop: std::sync::Arc<AtomicBool>,
    // Wakes the ticker immediately on drop so process exit never waits out
    // a sleep. `recv_timeout` doubles as the tick sleep.
    wake_tx: mpsc::Sender<()>,
    done: Option<mpsc::Receiver<()>>,
}

/// Start monitoring a spawned compiler child. Returns `None` (a no-op) when
/// no heartbeat context is installed — callers need no gating of their own.
pub fn start_monitor(crate_name: &str, pid: u32) -> Option<CompileMonitor> {
    let ctx = current_heartbeat_ctx()?;
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let (wake_tx, wake_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let thread_stop = stop.clone();
    let io = crate::invocation::io::current().borrow_for_monitor();
    let logging = tracing::dispatcher::get_default(Clone::clone);
    let crate_name = crate_name.to_string();
    let _ = std::thread::Builder::new()
        .name("kache-heartbeat".into())
        .spawn(move || {
            let _io = crate::invocation::io::InvocationIoScope::enter(io);
            let _logging = tracing::dispatcher::set_default(&logging);
            monitor_loop(&ctx, &crate_name, pid, &thread_stop, &wake_rx);
            let _ = done_tx.send(());
        })
        .ok()?;
    Some(CompileMonitor {
        stop,
        wake_tx,
        done: Some(done_rx),
    })
}

impl Drop for CompileMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.wake_tx.send(());
        if let Some(done) = self.done.take() {
            let _ = done.recv_timeout(Duration::from_millis(50));
        }
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
            client_version: None,
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
            // The monitor stopped us, or the sender vanished with the parent.
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
        if stop.load(Ordering::Relaxed) {
            return;
        }
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
                    client_version: None,
                },
            );
        }
        let eta = typical.map(|t| t.saturating_sub(elapsed.as_secs()));

        if let Some(line) =
            heartbeat_line(ctx.show_lines, crate_name, elapsed.as_secs(), typical, eta)
        {
            crate::notice::show_requested(&line);
        }

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
        if stop.load(Ordering::Relaxed) {
            return;
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
                        crate::notice::show(&format!(
                            "[kache] WARN: rustc for {} appears stuck (<1% CPU for {}s, PID {})",
                            crate_name,
                            wall.as_secs(),
                            pid
                        ));
                        stuck_warned = true;
                    }
                }
            }
        }
    }
}

/// The human-facing heartbeat line, when verbose progress asked for one.
/// Pure so the default-silent contract is tested without a terminal.
fn heartbeat_line(
    enabled: bool,
    crate_name: &str,
    elapsed_s: u64,
    typical_s: Option<u64>,
    eta_s: Option<u64>,
) -> Option<String> {
    if !enabled {
        return None;
    }
    let suffix = match (typical_s, eta_s) {
        (Some(t), Some(e)) => format!(
            " (typical: {}, ETA {})",
            format_secs(t),
            format_secs(e.max(1))
        ),
        _ => String::new(),
    };
    Some(format!(
        "[kache] still compiling {} — {} elapsed{}",
        crate_name,
        format_secs(elapsed_s),
        suffix
    ))
}

/// `4m20s` / `51s` / `2h05m` — compact duration for heartbeat lines.
pub(crate) fn format_secs(total: u64) -> String {
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

    fn set_test_context(cadence_secs: u64, root: &str, show_lines: bool) {
        set_heartbeat_ctx(
            cadence_secs,
            PathBuf::from(format!("{root}/events.jsonl")),
            PathBuf::from(format!("{root}/daemon.sock")),
            root.to_string(),
            show_lines,
        );
    }

    fn assert_test_context(cadence_secs: u64, root: &str, show_lines: bool) {
        let ctx = current_heartbeat_ctx().expect("this invocation enabled heartbeats");
        assert_eq!(ctx.cadence_secs, cadence_secs);
        assert_eq!(ctx.event_log, PathBuf::from(format!("{root}/events.jsonl")));
        assert_eq!(ctx.socket, PathBuf::from(format!("{root}/daemon.sock")));
        assert_eq!(ctx.root, root);
        assert_eq!(ctx.show_lines, show_lines);
    }

    #[test]
    fn concurrent_monitors_keep_request_context_after_scope_exit() {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for cadence_secs in [1, 2] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let scope = InvocationHeartbeatScope::enter();
                    let dir = tempfile::tempdir().unwrap();
                    let event_log = dir.path().join("events.jsonl");
                    let socket = dir.path().join("missing-daemon.sock");
                    let root = format!("/request-{cadence_secs}");
                    let crate_name = format!("crate_{cadence_secs}");
                    set_heartbeat_ctx(
                        cadence_secs,
                        event_log.clone(),
                        socket.clone(),
                        root.clone(),
                        false,
                    );
                    barrier.wait();
                    let ctx = current_heartbeat_ctx().unwrap();
                    assert_eq!(ctx.cadence_secs, cadence_secs);
                    assert_eq!(ctx.event_log, event_log);
                    assert_eq!(ctx.socket, socket);
                    assert_eq!(ctx.root, root);
                    assert!(!ctx.show_lines);
                    let monitor = start_monitor(&crate_name, std::process::id()).unwrap();
                    drop(scope);
                    drop(ctx);
                    let deadline = Instant::now() + Duration::from_secs(5);
                    let event = loop {
                        if let Ok(contents) = std::fs::read_to_string(&event_log)
                            && let Some(event) = contents
                                .lines()
                                .find_map(|line| serde_json::from_str::<HeartbeatEvent>(line).ok())
                        {
                            break event;
                        }
                        assert!(Instant::now() < deadline, "request monitor did not tick");
                        std::thread::sleep(Duration::from_millis(20));
                    };
                    drop(monitor);
                    assert_eq!(event.root, root);
                    assert_eq!(event.crate_name, crate_name);
                    assert_eq!(event.pid, std::process::id());
                    assert_eq!(event.schema, HEARTBEAT_SCHEMA);
                });
            }
        });
    }

    #[test]
    fn disabled_invocation_does_not_inherit_another_context() {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for enabled in [false, true] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let _scope = InvocationHeartbeatScope::enter();
                    set_test_context(3600, "/initial-root", false);
                    if !enabled {
                        set_test_context(0, "/disabled-root", true);
                    }
                    barrier.wait();
                    let monitor = start_monitor("test_crate", std::process::id());
                    assert_eq!(monitor.is_some(), enabled);
                    if enabled {
                        assert_test_context(3600, "/initial-root", false);
                    } else {
                        assert!(current_heartbeat_ctx().is_none());
                    }
                    drop(monitor);
                });
            }
        });
    }

    #[test]
    fn nested_heartbeat_scopes_restore_parent_after_panic_and_worker_reuse() {
        std::thread::spawn(|| {
            for cadence_secs in [13, 37] {
                let _scope = InvocationHeartbeatScope::enter();
                assert!(current_heartbeat_ctx().is_none());
                set_test_context(cadence_secs, "/parent-root", false);
                {
                    let _nested = InvocationHeartbeatScope::enter();
                    assert!(current_heartbeat_ctx().is_none());
                    assert!(start_monitor("disabled", std::process::id()).is_none());
                    set_test_context(71, "/child-root", true);
                    assert_test_context(71, "/child-root", true);
                }
                assert_test_context(cadence_secs, "/parent-root", false);
                let _ = std::panic::catch_unwind(|| {
                    let _nested = InvocationHeartbeatScope::enter();
                    set_test_context(97, "/panicking-root", true);
                    panic!("test panic");
                });
                assert_test_context(cadence_secs, "/parent-root", false);
            }
        })
        .join()
        .unwrap();
    }

    #[test]
    fn format_secs_is_compact() {
        assert_eq!(format_secs(51), "51s");
        assert_eq!(format_secs(260), "4m20s");
        assert_eq!(format_secs(471), "7m51s");
        assert_eq!(format_secs(7500), "2h05m");
    }

    #[test]
    fn heartbeat_line_is_opt_in_and_verbatim() {
        assert_eq!(
            heartbeat_line(false, "arrow_array", 30, Some(18), Some(0)),
            None
        );
        assert_eq!(
            heartbeat_line(true, "arrow_array", 30, Some(18), Some(0)).as_deref(),
            Some("[kache] still compiling arrow_array — 30s elapsed (typical: 18s, ETA 1s)")
        );
    }

    /// Keep this as the only test that initializes `CTX`: the process-global
    /// `OnceLock` cannot be reset safely between parallel tests.
    #[test]
    fn public_monitor_api_emits_a_structured_heartbeat() {
        let dir = tempfile::tempdir().unwrap();
        let event_log = dir.path().join("events.jsonl");
        set_heartbeat_ctx(
            1,
            event_log.clone(),
            dir.path().join("missing-daemon.sock"),
            "/test/root".to_string(),
            false,
        );

        let monitor = start_monitor("slow_crate", std::process::id())
            .expect("a configured heartbeat context must start a monitor");
        let deadline = Instant::now() + Duration::from_secs(5);
        let heartbeat = loop {
            if let Ok(contents) = std::fs::read_to_string(&event_log)
                && let Some(line) = contents
                    .lines()
                    .find(|line| line.contains("\"event\":\"heartbeat\""))
            {
                break Some(line.to_string());
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        drop(monitor);

        let event: HeartbeatEvent = serde_json::from_str(
            heartbeat
                .as_deref()
                .expect("monitor must append a heartbeat after one cadence"),
        )
        .unwrap();
        assert_eq!(event.crate_name, "slow_crate");
        assert_eq!(event.root, "/test/root");
        assert_eq!(event.schema, HEARTBEAT_SCHEMA);
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

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[test]
    fn drop_does_not_wait_for_a_stalled_ticker() {
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let (wake_tx, wake_rx) = mpsc::channel();
        // Keeping this sender alive models a ticker stuck before completion.
        let (done_tx, done_rx) = mpsc::channel();
        let monitor = CompileMonitor {
            stop: stop.clone(),
            wake_tx,
            done: Some(done_rx),
        };
        let (returned_tx, returned_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            drop(monitor);
            returned_tx.send(()).unwrap();
        });
        let returned = returned_rx.recv_timeout(Duration::from_secs(3));
        drop(done_tx);
        worker.join().unwrap();
        assert!(
            returned.is_ok(),
            "compiler exit waited for diagnostic completion"
        );
        assert!(stop.load(Ordering::Relaxed));
        assert!(wake_rx.try_recv().is_ok());
    }
}
