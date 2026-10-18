//! Shell runs: one process group per run, stdout and stderr merged into one
//! pipe, kept in a bounded in-memory ring and appended to `<handle>.log`.

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use event_listener::Event;
use futures_lite::AsyncReadExt;
use futures_lite::future::or;

use super::clock::Clock;
use super::{BodyRef, DoneReason, ShellSpec, Terminal, UnitHub, UnitId, UnitResult};
use crate::child_guard::ChildGuard;

const RING_BYTES: usize = 64 * 1024;
const READ_CHUNK: usize = 8 * 1024;
const MAX_PAGE_BYTES: usize = 32 * 1024;
const DEFAULT_LOG_CAP: u64 = 64 * 1024 * 1024;
const DEFAULT_GRACE: Duration = Duration::from_secs(5);
/// How long to wait for the pipe to close once the group is dead; a process
/// that escaped the group could hold it open forever.
const DRAIN_GRACE: Duration = Duration::from_secs(2);
const SUMMARY_LINES: usize = 5;
const SUMMARY_BYTES: usize = 1024;
const SHELL: &str = "sh";

#[derive(Debug, Clone)]
pub struct ShellOptions {
    /// Where `<handle>.log` goes; without it output lives only in the ring.
    pub dir: Option<PathBuf>,
    pub log_cap: u64,
    /// Between SIGTERM and SIGKILL.
    pub grace: Duration,
}

impl Default for ShellOptions {
    fn default() -> Self {
        Self {
            dir: None,
            log_cap: DEFAULT_LOG_CAP,
            grace: DEFAULT_GRACE,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct OutputSlice {
    pub text: String,
    pub start_offset: u64,
    pub next_offset: u64,
    /// Bytes between where the reader was and `start_offset` that are not in
    /// `text`: dropped from memory, or cut by `tail`.
    pub skipped_bytes: u64,
    pub log: Option<PathBuf>,
}

#[derive(Default)]
struct Output {
    buf: Vec<u8>,
    /// Offset in the log of `buf[0]`.
    start: u64,
    cursors: HashMap<UnitId, u64>,
}

impl Output {
    fn end(&self) -> u64 {
        self.start + self.buf.len() as u64
    }

    fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        if self.buf.len() > RING_BYTES {
            let cut = self.buf.len() - RING_BYTES;
            self.buf.drain(..cut);
            self.start += cut as u64;
        }
    }
}

/// Index where the last `n` lines of `data` begin.
fn tail_start(data: &[u8], n: usize) -> usize {
    let body = data.strip_suffix(b"\n").unwrap_or(data);
    let mut seen = 0;
    for (i, &b) in body.iter().enumerate().rev() {
        if b == b'\n' {
            seen += 1;
            if seen == n {
                return i + 1;
            }
        }
    }
    0
}

/// What the supervisor and the hub share about one shell.
pub(super) struct ShellRun {
    output: Mutex<Output>,
    log_path: Option<PathBuf>,
    pid: AtomicU32,
    stop: AtomicBool,
    /// `true` once the hub is gone. Launching and publishing the pid happen
    /// under this lock, so teardown either stops the launch or sees the pid.
    killed: Mutex<bool>,
    stop_event: Event,
}

impl ShellRun {
    pub(super) fn new(log_path: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            output: Mutex::default(),
            log_path,
            pid: AtomicU32::new(0),
            stop: AtomicBool::new(false),
            killed: Mutex::new(false),
            stop_event: Event::new(),
        })
    }

    fn out(&self) -> MutexGuard<'_, Output> {
        self.output.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(super) fn total_bytes(&self) -> u64 {
        self.out().end()
    }

    pub(super) fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.stop_event.notify(usize::MAX);
    }

    /// Blocks further launches and returns the live group, if any.
    pub(super) fn mark_killed(&self) -> Option<u32> {
        *self.killed.lock().unwrap_or_else(|e| e.into_inner()) = true;
        match self.pid.load(Ordering::SeqCst) {
            0 => None,
            pid => Some(pid),
        }
    }

    /// Hub teardown: no grace, nobody left to wait for it.
    pub(super) fn kill_now(&self) {
        *self.killed.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.request_stop();
        let pid = self.pid.load(Ordering::SeqCst);
        if pid != 0 {
            #[cfg(unix)]
            crate::child_guard::sigkill_process_group(pid);
        }
    }

    async fn stop_requested(&self) {
        loop {
            let listener = self.stop_event.listen();
            if self.stop.load(Ordering::Acquire) {
                return;
            }
            listener.await;
        }
    }

    pub(super) fn last_line(&self) -> Option<String> {
        let out = self.out();
        let body = out.buf.strip_suffix(b"\n").unwrap_or(&out.buf);
        let line = body.rsplit(|&b| b == b'\n').next()?;
        (!line.is_empty()).then(|| String::from_utf8_lossy(line).into_owned())
    }

    fn summary(&self) -> String {
        let out = self.out();
        let from = tail_start(&out.buf, SUMMARY_LINES);
        let tail = &out.buf[from..];
        let tail = &tail[tail.len().saturating_sub(SUMMARY_BYTES)..];
        String::from_utf8_lossy(tail).trim_end().to_owned()
    }

    /// Without `from` this is the output since `reader` last consumed, last
    /// `tail` lines of it. With `from` it is a byte page of at most
    /// `MAX_PAGE_BYTES`, ending on a line break when it can.
    pub(super) fn read(
        &self,
        reader: UnitId,
        from: Option<u64>,
        tail: usize,
        consume: bool,
    ) -> OutputSlice {
        let (begin, ring_start, mut start, mut data) = {
            let out = self.out();
            let begin = from.unwrap_or_else(|| out.cursors.get(&reader).copied().unwrap_or(0));
            let start = begin.clamp(out.start, out.end());
            let data = out.buf[(start - out.start) as usize..].to_vec();
            (begin, out.start, start, data)
        };
        if let Some(from) = from {
            if from < ring_start
                && let Some(page) =
                    self.read_log(from, (ring_start - from).min(MAX_PAGE_BYTES as u64))
                && !page.is_empty()
            {
                let reached_ring = from + page.len() as u64 == ring_start;
                start = from;
                let ring = std::mem::replace(&mut data, page);
                if reached_ring {
                    data.extend(ring);
                }
            }
            if data.len() >= MAX_PAGE_BYTES {
                data.truncate(MAX_PAGE_BYTES);
                if let Some(nl) = data.iter().rposition(|&b| b == b'\n') {
                    data.truncate(nl + 1);
                }
            }
        } else {
            let cut = tail_start(&data, tail);
            data.drain(..cut);
            start += cut as u64;
        }
        let next_offset = start + data.len() as u64;
        if consume {
            self.out().cursors.insert(reader, next_offset);
        }
        OutputSlice {
            text: String::from_utf8_lossy(&data).into_owned(),
            start_offset: start,
            next_offset,
            skipped_bytes: start.saturating_sub(begin),
            log: self.log_path.clone(),
        }
    }

    fn read_log(&self, from: u64, len: u64) -> Option<Vec<u8>> {
        let mut file = File::open(self.log_path.as_ref()?).ok()?;
        file.seek(SeekFrom::Start(from)).ok()?;
        let mut page = Vec::new();
        file.take(len).read_to_end(&mut page).ok()?;
        Some(page)
    }
}

pub(super) fn start(
    hub: Weak<UnitHub>,
    id: UnitId,
    run: Arc<ShellRun>,
    spec: ShellSpec,
    clock: Arc<dyn Clock>,
    opts: ShellOptions,
) {
    smol::spawn(supervise(hub, id, run, spec, clock, opts)).detach();
}

enum Cause {
    Stopped,
    TimedOut,
}

enum Ended {
    Exited(io::Result<ExitStatus>),
    Stop(Cause),
}

async fn supervise(
    hub: Weak<UnitHub>,
    id: UnitId,
    run: Arc<ShellRun>,
    spec: ShellSpec,
    clock: Arc<dyn Clock>,
    opts: ShellOptions,
) {
    let started = clock.now();
    // Before the process exists, so whoever sees its output may rely on the
    // deadline being fixed.
    let deadline = started + Duration::from_secs(spec.timeout_secs);
    let finish = |terminal: Terminal| {
        if let Some(hub) = hub.upgrade() {
            hub.settle(id, terminal, run.total_bytes());
        }
    };
    let elapsed_ms = || clock.now().saturating_duration_since(started).as_millis() as u64;
    let result = |reason, exit_code, detail| Terminal {
        reason,
        exit_code,
        detail,
        result: UnitResult {
            summary: run.summary(),
            body_ref: run.log_path.clone().map(BodyRef),
            elapsed_ms: elapsed_ms(),
            ..UnitResult::default()
        },
    };

    // Looked up before the launch: that holds the `killed` gate, and teardown
    // takes the table lock first and that gate second.
    let handle = hub
        .upgrade()
        .and_then(|h| h.handle_of(id))
        .map(|h| h.to_string());
    let record = |pid: u32| {
        if let (Some(hub), Some(handle)) = (hub.upgrade(), &handle) {
            hub.record_proc(handle, pid);
        }
    };
    let Some(launched) = launch_unless_killed(&spec, &run, &opts, &record) else {
        return;
    };
    let (mut guard, pump) = match launched {
        Ok(launched) => launched,
        Err(e) => {
            finish(result(
                DoneReason::Failed,
                None,
                Some(format!("failed to start: {e}")),
            ));
            return;
        }
    };
    let pid = guard.id();

    let ended = or(async { Ended::Exited(guard.status().await) }, async {
        Ended::Stop(
            or(
                async {
                    run.stop_requested().await;
                    Cause::Stopped
                },
                async {
                    clock.sleep_until(deadline).await;
                    Cause::TimedOut
                },
            )
            .await,
        )
    })
    .await;

    let terminal = match ended {
        Ended::Exited(Ok(status)) => {
            let (reason, code, detail) = classify(&status);
            result(reason, code, detail)
        }
        Ended::Exited(Err(e)) => {
            result(DoneReason::Failed, None, Some(format!("wait failed: {e}")))
        }
        Ended::Stop(cause) => {
            let reason = match cause {
                Cause::Stopped => DoneReason::Killed,
                Cause::TimedOut => {
                    if let Some(hub) = hub.upgrade() {
                        hub.mark_stopping(id);
                    }
                    DoneReason::TimedOut
                }
            };
            let grace_ends = clock.now() + opts.grace;
            term_group(pid);
            let exited = or(async { guard.status().await.ok() }, async {
                clock.sleep_until(grace_ends).await;
                None
            })
            .await;
            let (code, detail) = match exited {
                Some(status) => (status.code(), signal_of(&status).map(signal_name)),
                None => {
                    guard.kill_and_reap().await;
                    (None, Some(signal_name(9)))
                }
            };
            let detail = match reason {
                DoneReason::TimedOut => Some(format!(
                    "timed out after {}s{}",
                    spec.timeout_secs,
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                )),
                _ => detail,
            };
            result(reason, code, detail)
        }
    };

    // The leader is gone; anything it left behind in the group is an orphan.
    #[cfg(unix)]
    crate::child_guard::sigkill_process_group(pid);
    run.pid.store(0, Ordering::Release);
    live_groups().remove(&pid);
    or(
        async {
            let _ = pump.await;
        },
        async { clock.sleep_until(clock.now() + DRAIN_GRACE).await },
    )
    .await;
    let mut terminal = terminal;
    terminal.result.summary = run.summary();
    terminal.result.elapsed_ms = elapsed_ms();
    finish(terminal);
}

/// Process groups of shells that were launched and not yet reaped, so a
/// signal handler or panic hook can tear them down without a hub in hand.
static LIVE: Mutex<BTreeSet<u32>> = Mutex::new(BTreeSet::new());
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
/// How long `shutdown_groups` waits between SIGTERM and SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

fn live_groups() -> MutexGuard<'static, BTreeSet<u32>> {
    LIVE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Process exit: stops new launches and tears down every live shell group.
pub(super) fn shutdown_all() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
    let pids: Vec<u32> = live_groups().iter().copied().collect();
    shutdown_groups(&pids);
}

/// Synchronous and runtime-free: SIGTERM, a bounded wait, then SIGKILL.
/// Idempotent; a group that is already gone is skipped.
pub(super) fn shutdown_groups(pids: &[u32]) {
    #[cfg(unix)]
    {
        for &pid in pids {
            term_group(pid);
        }
        let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
        while std::time::Instant::now() < deadline && pids.iter().any(|&p| group_exists(p)) {
            std::thread::sleep(Duration::from_millis(10));
        }
        for &pid in pids {
            crate::child_guard::sigkill_process_group(pid);
        }
    }
    #[cfg(not(unix))]
    let _ = pids;
}

#[cfg(unix)]
pub(super) fn group_exists(pgid: u32) -> bool {
    use rustix::process::{Pid, test_kill_process_group};
    i32::try_from(pgid)
        .ok()
        .and_then(Pid::from_raw)
        .is_some_and(|g| test_kill_process_group(g).is_ok())
}

/// `None` when the hub is already gone.
fn launch_unless_killed(
    spec: &ShellSpec,
    run: &Arc<ShellRun>,
    opts: &ShellOptions,
    record: &dyn Fn(u32),
) -> Option<io::Result<(ChildGuard, smol::Task<()>)>> {
    let gate = run.killed.lock().unwrap_or_else(|e| e.into_inner());
    if *gate || SHUTTING_DOWN.load(Ordering::SeqCst) {
        return None;
    }
    let launched = launch(spec, run, opts, record);
    if let Ok((guard, _)) = &launched {
        run.pid.store(guard.id(), Ordering::SeqCst);
        live_groups().insert(guard.id());
    }
    Some(launched)
}

fn launch(
    spec: &ShellSpec,
    run: &Arc<ShellRun>,
    opts: &ShellOptions,
    record: &dyn Fn(u32),
) -> io::Result<(ChildGuard, smol::Task<()>)> {
    let log = run.log_path.as_ref().and_then(|path| {
        private_log(path)
            .inspect_err(
                |e| tracing::warn!(?path, %e, "shell log unavailable, keeping output in memory"),
            )
            .ok()
    });
    let (reader, writer) = io::pipe()?;
    let mut cmd = std::process::Command::new(SHELL);
    #[cfg(unix)]
    cmd.arg("-c").arg(GATE_SCRIPT).arg(SHELL).arg(&spec.command);
    #[cfg(not(unix))]
    cmd.arg("-c").arg(&spec.command);
    if let Some(cwd) = &spec.cwd {
        cmd.current_dir(cwd);
    }
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
    // The child waits before exec until the parent has made its pid durable,
    // so a crash of maki can never leave a shell that no journal knows.
    #[cfg(unix)]
    let gate = install_gate(&mut cmd)?;
    // Stdio is set here: converting resets whatever the std command had.
    let mut cmd = smol::process::Command::from(cmd);
    cmd.stdin(Stdio::null())
        .stdout(writer.try_clone()?)
        .stderr(writer);
    let child = cmd.spawn()?;
    #[cfg(unix)]
    {
        let (read_end, mut write_end) = gate;
        drop(read_end);
        record(child.id());
        // A failed write means the child already gave up and exited.
        let _ = io::Write::write_all(&mut write_end, b"\n");
    }
    #[cfg(not(unix))]
    record(child.id());
    // Drops the parent's write ends, so the pipe closes with the last child.
    drop(cmd);
    let pump = smol::spawn(pump(
        smol::Unblock::new(reader),
        Arc::clone(run),
        log,
        opts.log_cap,
    ));
    Ok((ChildGuard::new(child), pump))
}

/// The gate fd the wrapper shell reads before it runs the command.
#[cfg(unix)]
const GATE_FD: libc::c_int = 3;

/// Blocks on the gate until the parent has made the shell's pid durable and
/// sent a byte, then replaces itself (same pid, same start time) with the real
/// `sh -c`. EOF, which is what the parent's death looks like, ends it with 127
/// before anything ran. `$1` is the command. (`pre_exec` cannot do the waiting:
/// `spawn` blocks until exec, so that would deadlock the handshake.)
#[cfg(unix)]
const GATE_SCRIPT: &str = "read -r _ <&3 || exit 127; exec 3<&-; exec sh -c \"$1\"";

/// Wires the pipe the wrapper reads: fd 3 in the child is its read end.
#[cfg(unix)]
fn install_gate(cmd: &mut std::process::Command) -> io::Result<(io::PipeReader, io::PipeWriter)> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let (reader, writer) = io::pipe()?;
    let rfd = reader.as_raw_fd();
    // SAFETY: the closure only calls dup2 and fcntl, both async-signal-safe.
    unsafe {
        cmd.pre_exec(move || {
            // dup2 clears close-on-exec on the new fd; when the pipe already
            // sits on fd 3 only the flag has to go.
            let ok = if rfd == GATE_FD {
                libc::fcntl(rfd, libc::F_SETFD, 0) != -1
            } else {
                libc::dup2(rfd, GATE_FD) != -1
            };
            if ok {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
    Ok((reader, writer))
}

fn private_log(path: &std::path::Path) -> io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    opts.open(path)
}

async fn pump(
    mut reader: smol::Unblock<io::PipeReader>,
    run: Arc<ShellRun>,
    mut log: Option<File>,
    cap: u64,
) {
    let mut chunk = [0u8; READ_CHUNK];
    let mut logged = 0u64;
    let mut discarding = false;
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if discarding {
            continue;
        }
        let keep = n.min((cap - logged) as usize);
        let mut data = chunk[..keep].to_vec();
        if keep < n {
            data.extend_from_slice(
                format!("\n[log limit of {cap} bytes reached; further output discarded]\n")
                    .as_bytes(),
            );
            discarding = true;
        }
        logged += data.len() as u64;
        if let Some(file) = &mut log
            && let Err(e) = file.write_all(&data)
        {
            tracing::warn!(%e, "shell log write failed");
            log = None;
        }
        run.out().push(&data);
    }
}

fn classify(status: &ExitStatus) -> (DoneReason, Option<i32>, Option<String>) {
    match (status.code(), signal_of(status)) {
        (Some(0), _) => (DoneReason::Completed, Some(0), None),
        (Some(code), _) => (DoneReason::Failed, Some(code), None),
        (None, Some(sig)) => (
            DoneReason::Failed,
            None,
            Some(format!("killed by {}", signal_name(sig))),
        ),
        (None, None) => (DoneReason::Failed, None, Some("unknown exit".into())),
    }
}

#[cfg(unix)]
fn signal_of(status: &ExitStatus) -> Option<i32> {
    std::os::unix::process::ExitStatusExt::signal(status)
}

#[cfg(not(unix))]
fn signal_of(_: &ExitStatus) -> Option<i32> {
    None
}

fn signal_name(sig: i32) -> String {
    match sig {
        1 => "SIGHUP".into(),
        2 => "SIGINT".into(),
        3 => "SIGQUIT".into(),
        6 => "SIGABRT".into(),
        9 => "SIGKILL".into(),
        11 => "SIGSEGV".into(),
        13 => "SIGPIPE".into(),
        14 => "SIGALRM".into(),
        15 => "SIGTERM".into(),
        n => format!("signal {n}"),
    }
}

#[cfg(unix)]
pub(super) fn term_group(pid: u32) {
    use rustix::process::{Pid, Signal, kill_process_group};
    if let Some(group) = i32::try_from(pid).ok().and_then(Pid::from_raw) {
        let _ = kill_process_group(group, Signal::TERM);
    }
}

/// No process groups here; the KILL after the grace period does the work.
#[cfg(not(unix))]
fn term_group(_: u32) {}

const COMPOUND_WORDS: [&str; 15] = [
    "if", "then", "elif", "else", "fi", "for", "while", "until", "do", "done", "case", "esac",
    "select", "function", "time",
];

/// The permission scopes of a command line and whether to force a prompt.
/// A conservative port of the bash plugin's scope logic: plain chains split
/// at `&&`, `||`, `;`, `|` and newlines into one scope per command; anything
/// the plugin would parse as complex, plus anything this scanner is unsure
/// about, becomes one scope for the whole line and always prompts.
pub fn command_scopes(command: &str) -> (Vec<String>, bool) {
    let whole = || (vec![command.trim().to_owned()], true);
    let mut segments = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some('\''), '\'') | (Some('"'), '"') => {
                quote = None;
                cur.push(c);
            }
            (Some('\''), _) => cur.push(c),
            (Some(_), '`') => return whole(),
            (Some(_), '$') if chars.peek() == Some(&'(') => return whole(),
            (Some(_), '\\') => {
                cur.push(c);
                cur.extend(chars.next());
            }
            (Some(_), _) => cur.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                cur.push(c);
            }
            (None, '\\') => match chars.next() {
                None | Some('\n') => return whole(),
                Some(next) => {
                    cur.push(c);
                    cur.push(next);
                }
            },
            (None, '`' | '(' | ')' | '{' | '}' | '#') => return whole(),
            // `$'..'` has escape rules this scanner does not follow.
            (None, '$') if matches!(chars.peek(), Some('{' | '\'' | '"')) => return whole(),
            (None, '<') if chars.peek() == Some(&'<') => return whole(),
            (None, ';' | '\n') => segments.push(std::mem::take(&mut cur)),
            (None, '&') if chars.peek() == Some(&'&') => {
                chars.next();
                segments.push(std::mem::take(&mut cur));
            }
            (None, '&') if cur.ends_with('>') || chars.peek() == Some(&'>') => cur.push(c),
            (None, '&') => return whole(),
            (None, '|') => {
                match chars.peek() {
                    Some('|') => {
                        chars.next();
                    }
                    Some('&') => return whole(),
                    _ => {}
                }
                segments.push(std::mem::take(&mut cur));
            }
            (None, _) => cur.push(c),
        }
    }
    if quote.is_some() {
        return whole();
    }
    segments.push(cur);
    let segments: Vec<String> = segments
        .into_iter()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let compound = segments.iter().any(|s| {
        s.split_whitespace()
            .next()
            .is_some_and(|w| COMPOUND_WORDS.contains(&w))
    });
    if segments.is_empty() || compound {
        return whole();
    }
    (segments, false)
}

#[cfg(all(test, unix))]
mod tests {
    use std::future::Future;
    use std::time::Instant;

    use test_case::test_case;

    use super::*;
    use crate::Envelope;
    use crate::EventSender;
    use crate::cancel::CancelToken;
    use crate::runs::{
        ManualClock, NoticePolicy, RunKind, StopStatus, UnitLimits, UnitState, UnitView, WaitMode,
    };

    const BOUND: Duration = Duration::from_secs(60);
    /// For waits that must be woken by an event, never by time.
    const FOREVER: Duration = Duration::from_secs(10_000_000);

    struct Rig {
        hub: Arc<UnitHub>,
        clock: Arc<ManualClock>,
        dir: tempfile::TempDir,
    }

    fn rig_with(clock: Arc<ManualClock>, log_cap: u64) -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let (tx, _rx) = flume::unbounded::<Envelope>();
        let hub = UnitHub::with_shell_options(
            UnitLimits::default(),
            EventSender::new(tx, 0),
            clock.clone(),
            ShellOptions {
                dir: Some(dir.path().to_path_buf()),
                log_cap,
                grace: DEFAULT_GRACE,
            },
        );
        Rig { hub, clock, dir }
    }

    fn rig() -> Rig {
        let clock = Arc::new(ManualClock::default());
        rig_with(clock, DEFAULT_LOG_CAP)
    }

    fn start(rig: &Rig, command: &str) -> UnitId {
        start_with(rig, command, None)
    }

    fn start_with(rig: &Rig, command: &str, timeout_secs: Option<u64>) -> UnitId {
        let mut req = crate::runs::tests::request(RunKind::Shell, "job");
        req.command = Some(command.into());
        req.timeout_secs = timeout_secs;
        rig.hub.spawn(rig.hub.main(), req).unwrap().id
    }

    fn view(rig: &Rig, id: UnitId) -> UnitView {
        rig.hub.get(id).unwrap()
    }

    fn finished(rig: &Rig, id: UnitId) -> Terminal {
        let views = smol::block_on(rig.hub.wait(
            rig.hub.main(),
            &[id],
            WaitMode::All,
            FOREVER,
            &CancelToken::none(),
        ));
        match &views[0].state {
            UnitState::Done(t) => (**t).clone(),
            other => panic!("not done: {other:?}"),
        }
    }

    /// Polls until `f` holds; the process under test is on its own schedule.
    fn until(what: &str, mut f: impl FnMut() -> bool) {
        let end = Instant::now() + BOUND;
        while !f() {
            assert!(Instant::now() < end, "timed out waiting for {what}");
            smol::block_on(smol::Timer::after(Duration::from_millis(10)));
        }
    }

    fn output_has(rig: &Rig, id: UnitId, needle: &str) -> bool {
        read_all(rig, id).contains(needle)
    }

    fn read_all(rig: &Rig, id: UnitId) -> String {
        let reader = UnitId::new();
        rig.hub.read_output(reader, id, Some(0), 0, false).text
    }

    fn block_on<T>(f: impl Future<Output = T>) -> T {
        smol::block_on(f)
    }

    #[test]
    fn exit_zero_completes() {
        let rig = rig();
        let id = start(&rig, "echo hi");
        let t = finished(&rig, id);
        assert_eq!((t.reason, t.exit_code), (DoneReason::Completed, Some(0)));
        assert_eq!(t.result.summary, "hi");
    }

    #[test]
    fn nonzero_is_failed() {
        let rig = rig();
        let id = start(&rig, "echo boom >&2; exit 3");
        let t = finished(&rig, id);
        assert_eq!((t.reason, t.exit_code), (DoneReason::Failed, Some(3)));
        assert_eq!(t.result.summary, "boom");
    }

    #[test]
    fn output_ordered_in_log() {
        let rig = rig();
        let id = start(&rig, "echo a; echo b >&2; echo c");
        finished(&rig, id);
        let log = std::fs::read_to_string(
            rig.dir
                .path()
                .join(format!("{}.log", view(&rig, id).handle)),
        )
        .unwrap();
        assert_eq!(log, "a\nb\nc\n");
        let path = rig
            .dir
            .path()
            .join(format!("{}.log", view(&rig, id).handle));
        let mode = std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(path).unwrap().permissions(),
        );
        assert_eq!(mode & 0o077, 0, "log is private");
    }

    #[test]
    fn log_cap_discards_and_marks() {
        let clock = Arc::new(ManualClock::default());
        let rig = rig_with(clock, 10);
        let id = start(&rig, "printf 0123456789abcdef; printf more");
        let t = finished(&rig, id);
        assert_eq!(t.reason, DoneReason::Completed);
        let log = std::fs::read_to_string(
            rig.dir
                .path()
                .join(format!("{}.log", view(&rig, id).handle)),
        )
        .unwrap();
        assert!(
            log.starts_with("0123456789\n[log limit of 10 bytes"),
            "{log}"
        );
        assert_eq!(log.matches("[log limit").count(), 1, "{log}");
        assert_eq!(read_all(&rig, id), log);
    }

    #[test]
    fn timeout_marks_timed_out() {
        let rig = rig();
        let id = start_with(&rig, "echo ready; sleep 600", Some(30));
        until("ready", || output_has(&rig, id, "ready"));
        rig.clock.advance(Duration::from_secs(30));
        rig.clock.advance(DEFAULT_GRACE);
        let t = finished(&rig, id);
        assert_eq!(t.reason, DoneReason::TimedOut);
        assert!(t.detail.unwrap().starts_with("timed out after 30s"));
    }

    #[test]
    fn stop_sends_term_then_kill_after_grace() {
        let rig = rig();
        let id = start(
            &rig,
            "trap 'echo got-term' TERM; echo ready; while :; do sleep 0.05; done",
        );
        until("ready", || output_has(&rig, id, "ready"));
        rig.hub.stop(rig.hub.main(), &[id], Default::default());
        until("TERM delivered", || output_has(&rig, id, "got-term"));
        assert!(matches!(view(&rig, id).state, UnitState::Stopping));
        rig.clock.advance(DEFAULT_GRACE);
        let t = finished(&rig, id);
        assert_eq!(t.reason, DoneReason::Killed);
        assert_eq!(t.detail.as_deref(), Some("SIGKILL"));
    }

    #[test]
    fn orphan_group_killed() {
        let rig = rig();
        let id = start(&rig, "sleep 600 & echo $!");
        finished(&rig, id);
        let pid: u32 = read_all(&rig, id).trim().parse().unwrap();
        until("orphan to die", || !alive(pid));
    }

    #[test]
    fn stop_is_idempotent_and_truthful() {
        let rig = rig();
        let id = start(&rig, "echo $$; sleep 600");
        until("pid", || read_all(&rig, id).trim().parse::<u32>().is_ok());
        let pgid: u32 = read_all(&rig, id).trim().parse().unwrap();
        let main = rig.hub.main();
        let first = rig.hub.stop(main, &[id], Default::default());
        let again = rig.hub.stop(main, &[id], Default::default());
        assert!(matches!(first[0].status, Ok(StopStatus::Requested)));
        // The shell may already have died of the first SIGTERM on a busy host.
        assert!(
            matches!(&again[0].status, Ok(StopStatus::Requested))
                || matches!(&again[0].status, Ok(StopStatus::AlreadyFinished(s)) if s == "killed"),
            "{:?}",
            again[0].status
        );
        let t = finished(&rig, id);
        assert_eq!(t.reason, DoneReason::Killed);
        let after = rig.hub.stop(main, &[id], Default::default());
        assert!(
            matches!(&after[0].status, Ok(StopStatus::AlreadyFinished(s)) if s == "killed"),
            "{:?}",
            after[0].status
        );
        until("group gone", || !group_alive(pgid));
        assert!(rig.hub.pending_notices(main).is_empty());
    }

    #[test]
    fn stop_rejects_non_ancestors() {
        let rig = rig();
        let id = start(&rig, "sleep 600");
        let stranger = UnitId::new();
        let out = rig.hub.stop(stranger, &[id], Default::default());
        assert!(matches!(
            out[0].status,
            Err(crate::runs::UnitError::NotReachable)
        ));
        rig.hub.stop(rig.hub.main(), &[id], Default::default());
        finished(&rig, id);
    }

    #[test]
    fn wait_any_returns_first() {
        let rig = rig();
        let slow = start(&rig, "sleep 600");
        let fast = start(&rig, "true");
        let views = block_on(rig.hub.wait(
            rig.hub.main(),
            &[slow, fast],
            WaitMode::Any,
            FOREVER,
            &CancelToken::none(),
        ));
        assert!(views.iter().find(|v| v.id == fast).unwrap().state.is_done());
        assert!(matches!(
            views.iter().find(|v| v.id == slow).unwrap().state,
            UnitState::Running
        ));
        rig.hub.stop(rig.hub.main(), &[slow], Default::default());
        finished(&rig, slow);
    }

    #[test]
    fn wait_timeout_does_not_consume_result() {
        let rig = rig();
        let gate = rig.dir.path().join("gate");
        let id = start(
            &rig,
            &format!("while [ ! -e {} ]; do sleep 0.02; done", gate.display()),
        );
        let main = rig.hub.main();
        let token = CancelToken::none();
        let ids = [id];
        let waiting = rig
            .hub
            .wait(main, &ids, WaitMode::All, Duration::from_secs(5), &token);
        let views = block_on(drive(&rig.clock, waiting));
        assert!(matches!(views[0].state, UnitState::Running));
        std::fs::write(&gate, "").unwrap();
        until("done", || view(&rig, id).state.is_done());
        // The notice follows the state change by a moment.
        until("notice", || rig.hub.pending_notices(main).len() == 1);
        finished(&rig, id);
        assert_eq!(
            rig.hub.pending_notices(main).len(),
            1,
            "a bare wait on the hub claims nothing"
        );
        let claim = crate::runs::Claim {
            runs: vec![(id, view(&rig, id).settle_gen)],
            seqs: Vec::new(),
        };
        rig.hub.claim(main, None, claim);
        assert!(rig.hub.pending_notices(main).is_empty());
    }

    #[test]
    fn wait_cancel_does_not_stop_runs() {
        let rig = rig();
        let id = start(&rig, "sleep 600");
        let (trigger, token) = CancelToken::new();
        let hub = rig.hub.clone();
        let main = hub.main();
        let waiting =
            smol::spawn(async move { hub.wait(main, &[id], WaitMode::All, FOREVER, &token).await });
        trigger.cancel();
        let views = block_on(waiting);
        assert!(matches!(views[0].state, UnitState::Running));
        rig.hub.stop(main, &[id], Default::default());
        finished(&rig, id);
    }

    #[test]
    fn stopped_run_leaves_no_notice_but_finished_one_does() {
        let rig = rig();
        let main = rig.hub.main();
        let quiet = start(&rig, "true");
        finished_without_consuming(&rig, quiet);
        let notices = rig.hub.pending_notices(main);
        assert_eq!(notices.len(), 1);
        assert!(notices[0].wake);
        assert!(notices[0].text.contains("exited 0"), "{}", notices[0].text);
    }

    #[test]
    fn never_policy_enqueues_nothing() {
        let rig = rig();
        let mut req = crate::runs::tests::request(RunKind::Shell, "job");
        req.notify = Some(NoticePolicy::Never);
        let id = rig.hub.spawn(rig.hub.main(), req).unwrap().id;
        until("done", || view(&rig, id).state.is_done());
        assert!(rig.hub.pending_notices(rig.hub.main()).is_empty());
    }

    #[test]
    fn hub_drop_kills_running_shells() {
        let rig = rig();
        let id = start(&rig, "echo $$; sleep 600");
        until("pid", || read_all(&rig, id).trim().parse::<u32>().is_ok());
        let pgid: u32 = read_all(&rig, id).trim().parse().unwrap();
        let Rig { hub, .. } = rig;
        drop(hub);
        until("group gone", || !group_alive(pgid));
    }

    #[test]
    fn hub_shutdown_kills_group_idempotently_and_bounded() {
        let rig = rig();
        let id = start(&rig, "echo $$; sleep 600");
        until("pid", || read_all(&rig, id).trim().parse::<u32>().is_ok());
        let pgid: u32 = read_all(&rig, id).trim().parse().unwrap();
        let t = std::time::Instant::now();
        rig.hub.shutdown();
        rig.hub.shutdown();
        assert!(t.elapsed() < Duration::from_secs(5));
        until("group gone", || !group_alive(pgid));
    }

    const HARNESS_ENV: &str = "MAKI_SHELL_SIGNAL_HARNESS";
    const HARNESS_TEST: &str = "runs::shell::tests::signal_harness";

    /// Child side of `sigterm_and_sighup_tear_down_shell_groups`: inert unless
    /// the env var is set.
    #[test]
    fn signal_harness() {
        if std::env::var_os(HARNESS_ENV).is_none() {
            return;
        }
        super::super::install_signal_teardown();
        let rig = rig();
        let id = start(&rig, "sh -c 'sleep 401 & sleep 402; wait' & echo $$; wait");
        until("pid", || read_all(&rig, id).trim().parse::<u32>().is_ok());
        println!("\nPGID={}", read_all(&rig, id).trim());
        std::thread::sleep(Duration::from_secs(120));
    }

    fn group_members(pgid: u32) -> Vec<u32> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir("/proc").unwrap().flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, rest)) = stat.rsplit_once(") ") else {
                continue;
            };
            let mut fields = rest.split(' ');
            let state = fields.next();
            let _ppid = fields.next();
            let group = fields.next().and_then(|g| g.parse::<u32>().ok());
            if group == Some(pgid) && state != Some("Z") {
                found.push(pid);
            }
        }
        found
    }

    #[test_case(libc::SIGTERM ; "sigterm")]
    #[test_case(libc::SIGHUP ; "sighup")]
    fn sigterm_and_sighup_tear_down_shell_groups(sig: libc::c_int) {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", HARNESS_TEST, "--nocapture", "--test-threads=1"])
            .env(HARNESS_ENV, "1")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let pgid: u32 = loop {
            let line = lines.next().expect("harness exited early").unwrap();
            if let Some(n) = line.strip_prefix("PGID=") {
                break n.parse().unwrap();
            }
        };
        until("sleeps running", || group_members(pgid).len() >= 3);
        // SAFETY: signals the harness process this test spawned.
        unsafe { libc::kill(child.id() as i32, sig) };
        let status = child.wait().unwrap();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(sig)
        );
        until("group gone", || group_members(pgid).is_empty());
    }

    #[test]
    fn cursors_are_per_reader() {
        let run = ShellRun::new(None);
        let (a, b) = (UnitId::new(), UnitId::new());
        run.out().push(b"1\n2\n");
        assert_eq!(run.read(a, None, 40, true).text, "1\n2\n");
        run.out().push(b"3\n");
        assert_eq!(run.read(a, None, 40, true).text, "3\n");
        assert_eq!(run.read(a, None, 40, true).text, "");
        let b_view = run.read(b, None, 40, true);
        assert_eq!((b_view.text.as_str(), b_view.next_offset), ("1\n2\n3\n", 6));
    }

    #[test]
    fn tail_and_paging() {
        let run = ShellRun::new(None);
        let reader = UnitId::new();
        run.out().push(b"1\n2\n3\n4\n");
        let slice = run.read(reader, None, 2, false);
        assert_eq!(
            (slice.text.as_str(), slice.start_offset, slice.skipped_bytes),
            ("3\n4\n", 4, 4)
        );
        let page = run.read(reader, Some(2), 2, false);
        assert_eq!(page.text, "2\n3\n4\n");
        assert_eq!(
            run.read(reader, None, 40, false).text,
            "1\n2\n3\n4\n",
            "peek leaves the cursor"
        );
    }

    #[test]
    fn ring_overflow_reads_older_bytes_from_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.log");
        let line = "0123456789abcdef\n";
        let all = line.repeat(RING_BYTES / line.len() + 10);
        std::fs::write(&path, &all).unwrap();
        let run = ShellRun::new(Some(path));
        run.out().push(all.as_bytes());
        let reader = UnitId::new();
        let cursor = run.read(reader, None, usize::MAX, true);
        assert!(cursor.skipped_bytes > 0 && cursor.start_offset == run.out().start);
        let page = run.read(reader, Some(0), 0, false);
        assert!(page.text.starts_with(line) && page.start_offset == 0);
        assert!(page.text.len() <= MAX_PAGE_BYTES && page.text.ends_with('\n'));
    }

    #[test_case("cargo test", &["cargo test"], false ; "plain")]
    #[test_case("cd x && cargo test 2>&1", &["cd x", "cargo test 2>&1"], false ; "chain")]
    #[test_case("a | b; c || d\ne", &["a", "b", "c", "d", "e"], false ; "separators")]
    #[test_case("echo 'a && b' \"c; d\"", &["echo 'a && b' \"c; d\""], false ; "quoted")]
    #[test_case("echo $(whoami)", &["echo $(whoami)"], true ; "substitution")]
    #[test_case("echo \"`id`\"", &["echo \"`id`\""], true ; "backtick")]
    #[test_case("(cd x; ls)", &["(cd x; ls)"], true ; "subshell")]
    #[test_case("sleep 1 &", &["sleep 1 &"], true ; "background")]
    #[test_case("for i in 1 2; do echo $i; done", &["for i in 1 2; do echo $i; done"], true ; "compound")]
    #[test_case("echo 'open", &["echo 'open"], true ; "unterminated")]
    #[test_case("cat <<EOF\nx\nEOF", &["cat <<EOF\nx\nEOF"], true ; "heredoc")]
    #[test_case("echo ${HOME}", &["echo ${HOME}"], true ; "brace_expansion")]
    #[test_case("echo $'\\'' ; rm x ; echo $'\\'''", &["echo $'\\'' ; rm x ; echo $'\\'''"], true ; "ansi_c_quote")]
    fn scopes_follow_the_bash_rules(command: &str, scopes: &[&str], force: bool) {
        assert_eq!(
            command_scopes(command),
            (scopes.iter().map(|s| s.to_string()).collect(), force)
        );
    }

    fn finished_without_consuming(rig: &Rig, id: UnitId) {
        until("done", || view(rig, id).state.is_done());
        // The notice follows the state change by a moment.
        until("notice", || {
            !rig.hub.pending_notices(rig.hub.main()).is_empty()
        });
    }

    /// Advances the manual clock a second per poll until `fut` is ready.
    async fn drive<T>(clock: &ManualClock, fut: impl Future<Output = T>) -> T {
        let mut fut = std::pin::pin!(fut);
        loop {
            if let Some(v) = futures_lite::future::poll_once(&mut fut).await {
                return v;
            }
            clock.advance(Duration::from_secs(1));
            futures_lite::future::yield_now().await;
        }
    }

    fn pid_of(pid: u32) -> rustix::process::Pid {
        rustix::process::Pid::from_raw(pid as i32).unwrap()
    }

    fn alive(pid: u32) -> bool {
        rustix::process::test_kill_process(pid_of(pid)).is_ok()
    }

    fn group_alive(pgid: u32) -> bool {
        rustix::process::test_kill_process_group(pid_of(pgid)).is_ok()
    }

    #[test]
    fn gate_without_go_ahead_runs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let run = |release: bool| {
            let mut cmd = std::process::Command::new(SHELL);
            cmd.arg("-c")
                .arg(GATE_SCRIPT)
                .arg(SHELL)
                .arg(format!("touch {}", marker.display()));
            let (reader, mut writer) = install_gate(&mut cmd).unwrap();
            let mut child = cmd.spawn().unwrap();
            drop(reader);
            if release {
                std::io::Write::write_all(&mut writer, b"\n").unwrap();
            }
            // The parent "dies" here: the write end closes.
            drop(writer);
            child.wait().unwrap()
        };
        assert_eq!(run(false).code(), Some(127));
        assert!(!marker.exists());
        assert!(run(true).success());
        assert!(marker.exists());
    }
}
