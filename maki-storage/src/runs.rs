//! Durable journal of background runs (design 7.3).
//!
//! `<runs dir>/<session>/journal.ndjson` is an append-only log, one JSON object
//! per line, first line `{"v":1,"t":"hdr"}`. Message bodies live beside it in
//! `messages.ndjson`. Records use plain strings and numbers so this leaf crate
//! knows nothing of the agent crate.
//!
//! Writes never happen on the caller's thread: [`RunJournal::append`] is a
//! `try_send` into a bounded queue drained by one writer thread. When the queue
//! is full the record is dropped and counted, and the writer notes the loss as
//! a `gap` record; a producer is never blocked and no lock is held across I/O.
//! `spawn`, `term`, `proc` and `mark` are fsynced at once, the rest is flushed
//! every [`FLUSH_INTERVAL`] and on shutdown.
//!
//! The reader tolerates a torn last line, skips corrupt lines with a warning
//! and ignores record types it does not know. A header with a higher version
//! makes the journal read-only for this binary.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::StorageError;
use crate::id::SessionRef;
use crate::lock::{Lock, LockError};

pub const JOURNAL_VERSION: u32 = 1;
pub const JOURNAL_FILE: &str = "journal.ndjson";
pub const MESSAGES_FILE: &str = "messages.ndjson";
const LOCK_FILE: &str = "journal.lock";
pub const COMPACT_THRESHOLD: u64 = 4 * 1024 * 1024;
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// How long a critical record waits for queue room before it is dropped.
const CRITICAL_WAIT: Duration = Duration::from_secs(5);
pub const QUEUE_CAP: usize = 4096;
pub const MAX_BODY: usize = 16 * 1024;
/// A journal line longer than this is corrupt: records are a few hundred bytes.
pub const MAX_LINE: usize = 1024 * 1024;
/// Terminal runs older than this are dropped at compaction.
pub const RETAIN_TERMINAL_MS: u64 = 24 * 3600 * 1000;
/// Past this multiple of the threshold even after compaction, only critical
/// records are written, so the journal cannot grow without bound.
const HARD_CAP_FACTOR: u64 = 4;

pub type NowMs = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    #[serde(default)]
    pub v: u32,
    #[serde(default)]
    pub ts: u64,
    #[serde(flatten)]
    pub rec: JournalRecord,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum JournalRecord {
    Hdr(HdrRec),
    Spawn(SpawnRec),
    State(StateRec),
    Msg(MsgRec),
    Ack(AckRec),
    Notice(NoticeRec),
    Proc(ProcRec),
    Term(TermRec),
    Mark(MarkRec),
    Gap(GapRec),
}

impl JournalRecord {
    /// Records that must survive a crash a moment later.
    fn critical(&self) -> bool {
        matches!(
            self,
            Self::Hdr(_) | Self::Spawn(_) | Self::Term(_) | Self::Proc(_) | Self::Mark(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HdrRec {
    pub maki: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SpawnRec {
    pub id: String,
    pub handle: String,
    pub name: String,
    pub kind: String,
    pub owner: String,
    pub parent: String,
    pub depth: u8,
    pub desc: String,
    pub origin_tool_use_id: Option<String>,
    pub notify: String,
    /// The full spec of the run, enough to rebuild it (cold revive).
    pub spec: Value,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StateRec {
    pub handle: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MsgRec {
    pub id: String,
    pub from: String,
    pub to: String,
    pub kind: String,
    pub bytes: u64,
    pub stage: String,
    /// Goes to `messages.ndjson`, never to the journal line.
    #[serde(skip_serializing)]
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AckRec {
    pub id: String,
    pub stage: String,
    pub step: Option<u32>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct NoticeRec {
    pub run: String,
    pub kind: String,
    pub generation: u64,
    pub wake: bool,
    pub text: String,
    /// `queued`, `delivered` or `consumed`.
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcRec {
    pub handle: String,
    pub pgid: u32,
    pub start_ticks: u64,
    /// [`boot_stamp`] of the machine and pid namespace that ran it. Start
    /// ticks only mean something within one boot and one pid namespace, so a
    /// record with another or no stamp never matches a process.
    pub boot: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TermRec {
    pub handle: String,
    pub reason: String,
    pub detail: Option<String>,
    pub exit_code: Option<i32>,
    pub cost: f64,
    pub turns: u32,
    pub elapsed_ms: u64,
    pub result_len: u64,
    pub body_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MarkRec {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GapRec {
    pub dropped: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunState {
    pub spawn: SpawnRec,
    pub spawn_ts: u64,
    pub state: String,
    pub proc: Option<ProcRec>,
    pub term: Option<TermRec>,
    pub term_ts: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageState {
    pub rec: MsgRec,
    /// Last stage: `accepted`, `held`, `delivered` or `dropped`.
    pub stage: String,
}

impl MessageState {
    pub fn pending(&self) -> bool {
        !matches!(self.stage.as_str(), "delivered" | "dropped")
    }
}

/// The run table as the journal left it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Replay {
    /// Header version, 0 when there is no header.
    pub version: u32,
    /// A newer binary wrote it: list, never rewrite.
    pub read_only: bool,
    pub runs: Vec<RunState>,
    pub messages: Vec<MessageState>,
    pub notices: Vec<NoticeRec>,
    /// A `mark` is the last meaningful record.
    pub clean_mark: bool,
    /// Records applied.
    pub applied: usize,
    pub corrupt: usize,
    pub unknown: usize,
    pub dropped: u64,
}

impl Replay {
    pub fn read(dir: &Path) -> Self {
        let mut replay = match File::open(dir.join(JOURNAL_FILE)) {
            Ok(file) => Self::parse_reader(io::BufReader::new(file)),
            Err(_) => Self::default(),
        };
        let bodies = read_bodies(dir);
        for m in &mut replay.messages {
            m.rec.body = bodies.get(&m.rec.id).cloned();
        }
        replay
    }

    pub fn parse(bytes: &[u8]) -> Self {
        Self::parse_reader(bytes)
    }

    /// Streams the journal line by line. A line longer than [`MAX_LINE`] is
    /// skipped without being held, so a corrupt file cannot exhaust memory.
    pub fn parse_reader(reader: impl BufRead) -> Self {
        let mut replay = Self::default();
        let mut n = 0;
        for_each_line(reader, |line, torn_tail| {
            n += 1;
            let Some(line) = line else {
                warn!(line = n, "skipping oversized journal line");
                replay.corrupt += 1;
                return;
            };
            let line = String::from_utf8_lossy(line);
            if line.trim().is_empty() {
                return;
            }
            match serde_json::from_str::<Value>(&line) {
                Err(_) if torn_tail => {}
                Err(e) => {
                    warn!(line = n, %e, "skipping corrupt journal line");
                    replay.corrupt += 1;
                }
                Ok(value) => replay.apply_value(value),
            }
        });
        replay
    }

    fn apply_value(&mut self, value: Value) {
        let v = value.get("v").and_then(Value::as_u64).unwrap_or(0) as u32;
        if value.get("t").and_then(Value::as_str) == Some("hdr") && self.version == 0 {
            self.version = v.max(1);
            self.read_only = v > JOURNAL_VERSION;
        }
        if v > JOURNAL_VERSION {
            self.unknown += 1;
            return;
        }
        match serde_json::from_value::<Entry>(value) {
            Ok(entry) => self.apply(entry),
            Err(_) => self.unknown += 1,
        }
    }

    fn apply(&mut self, entry: Entry) {
        self.applied += 1;
        let ts = entry.ts;
        if !matches!(entry.rec, JournalRecord::Mark(_) | JournalRecord::Hdr(_)) {
            self.clean_mark = false;
        }
        match entry.rec {
            JournalRecord::Hdr(_) => {}
            JournalRecord::Spawn(spawn) => {
                if !self.runs.iter().any(|r| r.spawn.handle == spawn.handle) {
                    self.runs.push(RunState {
                        spawn,
                        spawn_ts: ts,
                        state: "running".to_owned(),
                        proc: None,
                        term: None,
                        term_ts: 0,
                    });
                }
            }
            JournalRecord::State(s) => {
                if let Some(run) = self.run_mut(&s.handle)
                    && run.term.is_none()
                {
                    run.state = s.state;
                }
            }
            JournalRecord::Proc(p) => {
                if let Some(run) = self.run_mut(&p.handle) {
                    run.proc = Some(p);
                }
            }
            JournalRecord::Term(t) => {
                if let Some(run) = self.run_mut(&t.handle)
                    && run.term.is_none()
                {
                    run.state = t.reason.clone();
                    run.term = Some(t);
                    run.term_ts = ts;
                }
            }
            JournalRecord::Msg(rec) => {
                let stage = rec.stage.clone();
                self.messages.push(MessageState { rec, stage });
            }
            JournalRecord::Ack(a) => {
                if let Some(m) = self.messages.iter_mut().find(|m| m.rec.id == a.id) {
                    m.stage = a.stage;
                }
            }
            JournalRecord::Notice(n) => {
                match self
                    .notices
                    .iter_mut()
                    .find(|k| (&k.run, &k.kind, k.generation) == (&n.run, &n.kind, n.generation))
                {
                    Some(known) => known.state = n.state,
                    None => self.notices.push(n),
                }
            }
            JournalRecord::Mark(_) => self.clean_mark = true,
            JournalRecord::Gap(g) => self.dropped += g.dropped,
        }
    }

    fn run_mut(&mut self, handle: &str) -> Option<&mut RunState> {
        self.runs.iter_mut().find(|r| r.spawn.handle == handle)
    }

    /// The records that rebuild this state and nothing else: terminal runs
    /// older than a day, delivered message bodies and settled notices go.
    pub fn compacted(&self, now_ms: u64) -> Vec<Entry> {
        let entry = |ts, rec| Entry {
            v: JOURNAL_VERSION,
            ts,
            rec,
        };
        let mut out = vec![entry(
            now_ms,
            JournalRecord::Hdr(HdrRec {
                maki: env!("CARGO_PKG_VERSION").to_owned(),
            }),
        )];
        let mut kept: Vec<&str> = Vec::new();
        for run in &self.runs {
            let old = run.term.is_some() && now_ms.saturating_sub(run.term_ts) > RETAIN_TERMINAL_MS;
            if old {
                continue;
            }
            kept.push(&run.spawn.handle);
            out.push(entry(run.spawn_ts, JournalRecord::Spawn(run.spawn.clone())));
            match &run.term {
                Some(term) => {
                    if let Some(p) = &run.proc {
                        out.push(entry(run.spawn_ts, JournalRecord::Proc(p.clone())));
                    }
                    out.push(entry(run.term_ts, JournalRecord::Term(term.clone())));
                }
                None => {
                    out.push(entry(
                        run.spawn_ts,
                        JournalRecord::State(StateRec {
                            handle: run.spawn.handle.clone(),
                            state: run.state.clone(),
                        }),
                    ));
                    if let Some(p) = &run.proc {
                        out.push(entry(run.spawn_ts, JournalRecord::Proc(p.clone())));
                    }
                }
            }
        }
        for m in self.messages.iter().filter(|m| m.pending()) {
            let mut rec = m.rec.clone();
            rec.stage = m.stage.clone();
            out.push(entry(now_ms, JournalRecord::Msg(rec)));
        }
        for n in self.notices.iter().filter(|n| n.state == "queued") {
            out.push(entry(now_ms, JournalRecord::Notice(n.clone())));
        }
        if self.dropped > 0 {
            out.push(entry(
                now_ms,
                JournalRecord::Gap(GapRec {
                    dropped: self.dropped,
                }),
            ));
        }
        out
    }
}

/// Calls `f(line, torn_tail)` for every line without its newline; `line` is
/// `None` for one longer than [`MAX_LINE`], which is consumed but not kept.
/// `torn_tail` marks a last line with no newline.
fn for_each_line(mut reader: impl BufRead, mut f: impl FnMut(Option<&[u8]>, bool)) {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let got = match reader
            .by_ref()
            .take(MAX_LINE as u64 + 1)
            .read_until(b'\n', &mut buf)
        {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let newline = buf.last() == Some(&b'\n');
        if newline {
            buf.pop();
        } else if got > MAX_LINE {
            // Skip the rest of the oversized line.
            let mut ended = false;
            loop {
                let (used, hit) = match reader.fill_buf() {
                    Ok([]) | Err(_) => break,
                    Ok(chunk) => match chunk.iter().position(|&b| b == b'\n') {
                        Some(i) => (i + 1, true),
                        None => (chunk.len(), false),
                    },
                };
                reader.consume(used);
                if hit {
                    ended = true;
                    break;
                }
            }
            f(None, !ended);
            if !ended {
                return;
            }
            continue;
        }
        f(Some(&buf), !newline);
        if !newline {
            return;
        }
    }
}

/// Message bodies by id; a torn, corrupt or oversized line is skipped.
pub fn read_bodies(dir: &Path) -> HashMap<String, String> {
    #[derive(Deserialize)]
    struct Line {
        id: String,
        body: String,
    }
    let mut out = HashMap::new();
    let Ok(file) = File::open(dir.join(MESSAGES_FILE)) else {
        return out;
    };
    for_each_line(io::BufReader::new(file), |line, _| {
        if let Some(l) = line.and_then(|l| serde_json::from_slice::<Line>(l).ok()) {
            out.insert(l.id, l.body);
        }
    });
    out
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// Another maki process owns this session's journal.
    #[error("journal of this session is in use by another maki process")]
    Locked,
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Clone)]
pub struct JournalOptions {
    pub compact_threshold: u64,
    pub now: NowMs,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            compact_threshold: COMPACT_THRESHOLD,
            now: Arc::new(system_now_ms),
        }
    }
}

enum Cmd {
    Rec(Box<Entry>),
    Flush(SyncSender<()>),
    Close,
}

/// Handle to a session's journal. Cheap to share behind an `Arc`.
pub struct RunJournal {
    dir: PathBuf,
    tx: Option<SyncSender<Cmd>>,
    dropped: Arc<AtomicU64>,
    degraded: Arc<AtomicBool>,
    now: NowMs,
    read_only: bool,
    thread: Mutex<Option<JoinHandle<()>>>,
    _lock: Lock,
}

impl RunJournal {
    pub fn open(session: &SessionRef) -> Result<Self, JournalError> {
        let dir = crate::paths::runs_dir(&session.id().to_string())?;
        Self::open_in(dir, JournalOptions::default())
    }

    /// Takes the session's journal lock, compacts when that saves anything and
    /// starts the writer. `Locked` when another process has it.
    pub fn open_in(dir: PathBuf, opts: JournalOptions) -> Result<Self, JournalError> {
        crate::paths::create_private_dir(&dir)?;
        if fs::symlink_metadata(&dir)?.file_type().is_symlink() {
            return Err(io::Error::other("run directory is a symlink").into());
        }
        let lock = Lock::acquire(&dir.join(LOCK_FILE)).map_err(|e| match e {
            LockError::Held { .. } => JournalError::Locked,
            LockError::Io { source, .. } => JournalError::Io(source),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(dir.join(LOCK_FILE), fs::Permissions::from_mode(0o600));
        }
        let replay = Replay::read(&dir);
        let dropped = Arc::new(AtomicU64::new(0));
        let degraded = Arc::new(AtomicBool::new(false));
        let mut tx = None;
        let mut thread = None;
        if !replay.read_only {
            let mut writer = Writer {
                dir: dir.clone(),
                file: None,
                size: 0,
                opts: opts.clone(),
                dirty: false,
                degraded: Arc::clone(&degraded),
                dropped: Arc::clone(&dropped),
                hard_capped: false,
                compact_at: opts.compact_threshold,
            };
            let existing = fs::metadata(dir.join(JOURNAL_FILE)).map_or(0, |m| m.len());
            if existing > 0 && replay.compacted((opts.now)()).len() < replay.applied {
                writer.compact(&replay)?;
            }
            writer.open_file()?;
            let (sender, rx) = sync_channel(QUEUE_CAP);
            thread = Some(
                std::thread::Builder::new()
                    .name("maki-journal".into())
                    .spawn(move || writer.run(rx))?,
            );
            tx = Some(sender);
        }
        Ok(Self {
            dir,
            tx,
            dropped,
            degraded,
            now: Arc::clone(&opts.now),
            read_only: replay.read_only,
            thread: Mutex::new(thread),
            _lock: lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// A write failed and the journal stopped: runs continue memory-only.
    pub fn degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    /// Never blocks. A full queue drops the record and counts it.
    pub fn append(&self, rec: JournalRecord) {
        let Some(tx) = &self.tx else { return };
        let entry = Entry {
            v: JOURNAL_VERSION,
            ts: (self.now)(),
            rec,
        };
        if !entry.rec.critical() {
            if tx.try_send(Cmd::Rec(Box::new(entry))).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        // A lost `spawn`, `term`, `proc` or `mark` would make a finished run
        // look unfinished or hide a shell, so these wait for room (the writer
        // drains in milliseconds) and give up only after a long stall.
        let mut cmd = Cmd::Rec(Box::new(entry));
        let deadline = std::time::Instant::now() + CRITICAL_WAIT;
        loop {
            match tx.try_send(cmd) {
                Ok(()) => return,
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => break,
                Err(std::sync::mpsc::TrySendError::Full(back)) => {
                    if std::time::Instant::now() >= deadline {
                        break;
                    }
                    cmd = back;
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
        warn!("run journal queue stalled, a critical record was dropped");
    }

    /// Blocks until everything appended so far is on disk (shutdown and tests).
    pub fn flush(&self) {
        let Some(tx) = &self.tx else { return };
        let (done_tx, done_rx) = sync_channel(1);
        if tx.send(Cmd::Flush(done_tx)).is_ok() {
            let _ = done_rx.recv_timeout(Duration::from_secs(5));
        }
    }

    /// The state on disk after everything appended so far.
    pub fn replay(&self) -> Replay {
        self.flush();
        Replay::read(&self.dir)
    }
}

impl Drop for RunJournal {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Cmd::Close);
        }
        let handle = self.thread.get_mut().ok().and_then(|t| t.take());
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

struct Writer {
    dir: PathBuf,
    file: Option<BufWriter<File>>,
    size: u64,
    opts: JournalOptions,
    dirty: bool,
    degraded: Arc<AtomicBool>,
    dropped: Arc<AtomicU64>,
    hard_capped: bool,
    compact_at: u64,
}

fn private_options() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    o
}

/// Where a torn last line starts, `None` when the file ends with a newline.
/// Reads the tail backwards, never the whole file.
fn torn_tail_start(path: &Path) -> io::Result<Option<u64>> {
    use std::io::{Seek, SeekFrom};
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut end = len;
    let mut chunk = vec![0u8; 64 * 1024];
    while end > 0 {
        let start = end.saturating_sub(chunk.len() as u64);
        let part = &mut chunk[..(end - start) as usize];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(part)?;
        if let Some(i) = part.iter().rposition(|&b| b == b'\n') {
            let keep = start + i as u64 + 1;
            return Ok((keep < len).then_some(keep));
        }
        end = start;
    }
    Ok((len > 0).then_some(0))
}

fn encode(entry: &Entry) -> Vec<u8> {
    let mut line = serde_json::to_vec(entry).unwrap_or_default();
    line.push(b'\n');
    line
}

impl Writer {
    fn open_file(&mut self) -> io::Result<()> {
        let path = self.dir.join(JOURNAL_FILE);
        let file = private_options().open(&path)?;
        self.size = file.metadata()?.len();
        let mut file = BufWriter::new(file);
        if self.size == 0 {
            let hdr = Entry {
                v: JOURNAL_VERSION,
                ts: (self.opts.now)(),
                rec: JournalRecord::Hdr(HdrRec {
                    maki: env!("CARGO_PKG_VERSION").to_owned(),
                }),
            };
            let line = encode(&hdr);
            file.write_all(&line)?;
            file.flush()?;
            file.get_ref().sync_data()?;
            self.size = line.len() as u64;
        } else {
            // A torn last line goes: left in place it would turn into a corrupt
            // line once the next record follows it.
            if let Some(keep) = torn_tail_start(&path)? {
                OpenOptions::new().write(true).open(&path)?.set_len(keep)?;
                self.size = keep;
            }
        }
        self.file = Some(file);
        Ok(())
    }

    fn run(&mut self, rx: Receiver<Cmd>) {
        loop {
            let cmd = match rx.recv_timeout(FLUSH_INTERVAL) {
                Ok(cmd) => cmd,
                Err(RecvTimeoutError::Timeout) => {
                    self.sync(false);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };
            if !self.handle(cmd) {
                break;
            }
            while let Ok(cmd) = rx.try_recv() {
                if !self.handle(cmd) {
                    self.sync(true);
                    return;
                }
            }
        }
        self.sync(true);
    }

    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Rec(entry) => self.write(*entry),
            Cmd::Flush(done) => {
                self.sync(true);
                let _ = done.send(());
            }
            Cmd::Close => return false,
        }
        true
    }

    fn fail(&mut self, e: &dyn std::fmt::Display) {
        if !self.degraded.swap(true, Ordering::Relaxed) {
            warn!(%e, "run journal write failed, runs continue without persistence");
        }
        self.file = None;
    }

    fn write(&mut self, mut entry: Entry) {
        if self.degraded.load(Ordering::Relaxed) {
            return;
        }
        let lost = self.dropped.swap(0, Ordering::Relaxed);
        if lost > 0 {
            self.write_line(&Entry {
                v: JOURNAL_VERSION,
                ts: entry.ts,
                rec: JournalRecord::Gap(GapRec { dropped: lost }),
            });
        }
        if self.hard_capped && !entry.rec.critical() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let JournalRecord::Msg(m) = &mut entry.rec
            && let Some(body) = m.body.take()
            && let Err(e) = self.write_body(&m.id, &body)
        {
            self.fail(&e);
            return;
        }
        self.write_line(&entry);
        if entry.rec.critical() {
            self.sync(true);
        }
        if self.size > self.compact_at && !self.degraded.load(Ordering::Relaxed) {
            self.compact_live();
        }
    }

    fn write_body(&mut self, id: &str, body: &str) -> io::Result<()> {
        let mut end = body.len().min(MAX_BODY);
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        let line = serde_json::json!({"id": id, "body": &body[..end]});
        let mut file = private_options().open(self.dir.join(MESSAGES_FILE))?;
        writeln!(file, "{line}")
    }

    fn write_line(&mut self, entry: &Entry) {
        let line = encode(entry);
        let Some(file) = &mut self.file else { return };
        match file.write_all(&line) {
            Ok(()) => {
                self.size += line.len() as u64;
                self.dirty = true;
            }
            Err(e) => self.fail(&e),
        }
    }

    fn sync(&mut self, force: bool) {
        if !self.dirty && !force {
            return;
        }
        let Some(file) = &mut self.file else { return };
        if let Err(e) = file.flush().and_then(|()| file.get_ref().sync_data()) {
            self.fail(&e);
            return;
        }
        self.dirty = false;
    }

    fn compact_live(&mut self) {
        self.sync(true);
        let replay = Replay::read(&self.dir);
        self.file = None;
        if let Err(e) = self.compact(&replay).and_then(|()| self.open_file()) {
            self.fail(&e);
            return;
        }
        // Nothing left to drop: wait for real growth before trying again.
        self.compact_at = self.opts.compact_threshold.max(self.size.saturating_mul(2));
        self.hard_capped = self.size > self.opts.compact_threshold * HARD_CAP_FACTOR;
    }

    /// Replaces the journal (and the message file) with `replay` minus what
    /// can go: write a temp file, fsync, rename.
    fn compact(&mut self, replay: &Replay) -> io::Result<()> {
        let now = (self.opts.now)();
        let mut data = Vec::new();
        for entry in replay.compacted(now) {
            data.extend(encode(&entry));
        }
        crate::atomic_write_permissions(&self.dir.join(JOURNAL_FILE), &data, 0o600)
            .map_err(storage_io)?;
        let mut bodies = Vec::new();
        for m in replay.messages.iter().filter(|m| m.pending()) {
            if let Some(body) = &m.rec.body {
                bodies.extend(
                    serde_json::json!({"id": m.rec.id, "body": body})
                        .to_string()
                        .bytes(),
                );
                bodies.push(b'\n');
            }
        }
        crate::atomic_write_permissions(&self.dir.join(MESSAGES_FILE), &bodies, 0o600)
            .map_err(storage_io)
    }
}

fn storage_io(e: StorageError) -> io::Error {
    io::Error::other(e.to_string())
}

/// What `/proc` says about a process, to tell the process a journal recorded
/// from a later one that got the same pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcInfo {
    /// Field 22 of `/proc/<pid>/stat`, in clock ticks since boot.
    pub start_ticks: u64,
    /// Process group id, field 5.
    pub pgrp: u32,
}

/// Identifies the boot and the pid namespace this process sees: the kernel's
/// boot id plus the `pid` namespace link. Empty when `/proc` cannot say, and
/// an empty stamp never matches anything.
pub fn boot_stamp() -> String {
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap_or_default();
    let ns = fs::read_link("/proc/self/ns/pid").unwrap_or_default();
    let boot = boot.trim();
    if boot.is_empty() {
        return String::new();
    }
    format!("{boot}/{}", ns.display())
}

/// Whether `stamp` is this boot and namespace.
pub fn same_boot(stamp: &str) -> bool {
    !stamp.is_empty() && stamp == boot_stamp()
}

/// `None` when the process is gone or the platform has no `/proc`.
pub fn proc_info(pid: u32) -> Option<ProcInfo> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_stat(&stat)
}

fn parse_stat(stat: &str) -> Option<ProcInfo> {
    // The command name is in parentheses and may hold spaces and parens.
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    Some(ProcInfo {
        pgrp: fields.get(2)?.parse().ok()?,
        start_ticks: fields.get(19)?.parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    fn spawn(handle: &str) -> JournalRecord {
        JournalRecord::Spawn(SpawnRec {
            id: format!("id-{handle}"),
            handle: handle.into(),
            name: handle.into(),
            kind: "agent".into(),
            spec: serde_json::json!({"prompt": "p", "one_shot": false}),
            ..SpawnRec::default()
        })
    }

    fn term(handle: &str, reason: &str) -> JournalRecord {
        JournalRecord::Term(TermRec {
            handle: handle.into(),
            reason: reason.into(),
            ..TermRec::default()
        })
    }

    fn opts(now: Arc<AtomicU64>, threshold: u64) -> JournalOptions {
        JournalOptions {
            compact_threshold: threshold,
            now: Arc::new(move || now.load(Ordering::Relaxed)),
        }
    }

    fn open(dir: &Path) -> RunJournal {
        RunJournal::open_in(dir.to_owned(), JournalOptions::default()).unwrap()
    }

    #[test]
    fn replay_ignores_torn_last_line() {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        j.append(spawn("ag-1"));
        j.append(term("ag-1", "completed"));
        let full = j.replay();
        assert_eq!(full.runs.len(), 1);
        drop(j);
        let path = dir.path().join(JOURNAL_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(br#"{"v":1,"t":"term","handle":"ag-2","rea"#);
        fs::write(&path, &bytes).unwrap();
        let torn = Replay::read(dir.path());
        assert_eq!(torn.corrupt, 0);
        assert_eq!(torn.runs, full.runs);
        // Appending after a torn tail starts a fresh line.
        let j = open(dir.path());
        j.append(spawn("ag-3"));
        let again = j.replay();
        assert_eq!(again.runs.len(), 2);
        assert_eq!(again.corrupt, 0);
    }

    #[test]
    fn corrupt_middle_line_is_skipped() {
        let text = concat!(
            r#"{"v":1,"t":"hdr"}"#,
            "\nnot json\n",
            r#"{"v":1,"t":"spawn","handle":"ag-1"}"#,
            "\n"
        );
        let r = Replay::parse(text.as_bytes());
        assert_eq!((r.corrupt, r.runs.len()), (1, 1));
    }

    #[test]
    fn unknown_record_and_future_version() {
        let text = concat!(
            r#"{"v":1,"t":"hdr"}"#,
            "\n",
            r#"{"v":1,"t":"from_the_future","x":1}"#,
            "\n",
            r#"{"v":1,"t":"spawn","handle":"ag-1","new_field":true}"#,
            "\n"
        );
        let r = Replay::parse(text.as_bytes());
        assert_eq!((r.unknown, r.runs.len(), r.read_only), (1, 1, false));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(JOURNAL_FILE);
        let future = concat!(
            r#"{"v":2,"t":"hdr"}"#,
            "\n",
            r#"{"v":2,"t":"spawn","handle":"ag-1"}"#,
            "\n"
        );
        fs::write(&path, future).unwrap();
        let j = open(dir.path());
        assert!(j.read_only());
        j.append(spawn("ag-9"));
        j.flush();
        assert_eq!(fs::read_to_string(&path).unwrap(), future);
    }

    #[test]
    fn truncation_at_every_offset_is_prefix_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        for i in 0..4 {
            let h = format!("ag-{i}");
            j.append(spawn(&h));
            j.append(JournalRecord::State(StateRec {
                handle: h.clone(),
                state: "idle".into(),
            }));
            j.append(JournalRecord::Msg(MsgRec {
                id: format!("m-{i}"),
                to: h.clone(),
                stage: "accepted".into(),
                body: Some("héllo".into()),
                ..MsgRec::default()
            }));
            j.append(term(&h, "completed"));
        }
        j.flush();
        drop(j);
        let bytes = fs::read(dir.path().join(JOURNAL_FILE)).unwrap();
        let mut last_applied = 0;
        for cut in 0..=bytes.len() {
            let part = &bytes[..cut];
            let r = Replay::parse(part);
            let complete = part.iter().filter(|&&b| b == b'\n').count();
            assert!(
                r.applied == complete || r.applied == complete + 1,
                "cut {cut}"
            );
            assert!(r.applied >= last_applied, "monotone at {cut}");
            assert_eq!(r.corrupt, 0, "cut {cut}");
            last_applied = r.applied;
            let lines: Vec<_> = part.split(|&b| b == b'\n').collect();
            let upto = lines.len() - 1;
            let whole: Vec<u8> = lines[..upto].join(&b'\n');
            let base = Replay::parse(&[whole, vec![b'\n']].concat());
            assert!(r.runs.len() >= base.runs.len());
            for (a, b) in base.runs.iter().zip(&r.runs) {
                assert_eq!(a.spawn, b.spawn);
            }
        }
    }

    #[test]
    fn message_bodies_survive() {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        j.append(spawn("ag-1"));
        let big = "x".repeat(MAX_BODY + 100);
        j.append(JournalRecord::Msg(MsgRec {
            id: "m-1".into(),
            to: "ag-1".into(),
            stage: "accepted".into(),
            body: Some(big),
            ..MsgRec::default()
        }));
        j.append(JournalRecord::Msg(MsgRec {
            id: "m-2".into(),
            to: "ag-1".into(),
            stage: "accepted".into(),
            body: Some("second".into()),
            ..MsgRec::default()
        }));
        j.append(JournalRecord::Ack(AckRec {
            id: "m-2".into(),
            stage: "delivered".into(),
            ..AckRec::default()
        }));
        let r = j.replay();
        assert_eq!(r.messages[0].rec.body.as_ref().unwrap().len(), MAX_BODY);
        assert!(r.messages[0].pending() && !r.messages[1].pending());
        let line = fs::read_to_string(dir.path().join(JOURNAL_FILE)).unwrap();
        assert!(!line.contains("second"), "bodies stay out of the journal");
        drop(j);
        // After compaction at open only the pending body is left.
        let j = open(dir.path());
        let r = j.replay();
        assert_eq!(r.messages.len(), 1);
        assert_eq!(r.messages[0].rec.body.as_ref().unwrap().len(), MAX_BODY);
        assert!(
            !fs::read_to_string(dir.path().join(MESSAGES_FILE))
                .unwrap()
                .contains("second")
        );
    }

    #[test]
    fn compaction_on_size_threshold_while_live() {
        let dir = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(1_000));
        let j = RunJournal::open_in(dir.path().to_owned(), opts(Arc::clone(&now), 4096)).unwrap();
        for i in 0..40 {
            let h = format!("ag-{i}");
            j.append(spawn(&h));
            j.append(term(&h, "completed"));
        }
        j.flush();
        let before = fs::metadata(dir.path().join(JOURNAL_FILE)).unwrap().len();
        assert!(before > 4096);
        // A day later the finished runs are droppable; growth triggers it.
        now.store(1_000 + RETAIN_TERMINAL_MS + 10, Ordering::Relaxed);
        for i in 0..200 {
            j.append(spawn(&format!("live-{i}")));
        }
        let r = j.replay();
        assert!(r.runs.iter().all(|r| r.term.is_none()), "old runs dropped");
        assert_eq!(r.runs.len(), 200);
        j.append(term("live-0", "completed"));
        assert!(j.replay().runs[0].term.is_some());
    }

    #[test]
    fn compaction_keeps_state_equal() {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        j.append(spawn("ag-1"));
        j.append(spawn("sh-1"));
        j.append(JournalRecord::Proc(ProcRec {
            handle: "sh-1".into(),
            pgid: 7,
            start_ticks: 99,
            boot: "b".into(),
        }));
        j.append(term("ag-1", "completed"));
        j.append(JournalRecord::Notice(NoticeRec {
            run: "ag-1".into(),
            kind: "settled".into(),
            text: "t".into(),
            state: "queued".into(),
            ..NoticeRec::default()
        }));
        let before = j.replay();
        drop(j);
        let j = open(dir.path());
        let after = j.replay();
        assert_eq!(before.runs, after.runs);
        assert_eq!(before.notices, after.notices);
    }

    #[test]
    fn second_open_is_refused_and_files_are_private() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("s1");
        let j = open(&sub);
        j.append(spawn("ag-1"));
        j.flush();
        assert!(matches!(
            RunJournal::open_in(sub.clone(), JournalOptions::default()),
            Err(JournalError::Locked)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&sub), 0o700);
            assert_eq!(mode(&sub.join(JOURNAL_FILE)), 0o600);
        }
    }

    #[test]
    fn overflow_drops_and_records_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        j.dropped.fetch_add(3, Ordering::Relaxed);
        j.append(spawn("ag-1"));
        assert_eq!(j.replay().dropped, 3);
    }

    #[test]
    fn stat_parser_handles_odd_names() {
        let stat = "123 (we ird) name) S 1 456 456 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 987654 0 0";
        let info = parse_stat(stat).unwrap();
        assert_eq!((info.pgrp, info.start_ticks), (456, 987654));
    }

    fn sample_journal() -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let j = open(dir.path());
        for i in 0..6 {
            j.append(spawn(&format!("ag-{i}")));
            j.append(JournalRecord::State(StateRec {
                handle: format!("ag-{i}"),
                state: "idle".into(),
            }));
            j.append(JournalRecord::Proc(ProcRec {
                handle: format!("ag-{i}"),
                pgid: 7,
                start_ticks: 9,
                boot: "b".into(),
            }));
        }
        j.append(term("ag-1", "completed"));
        j.flush();
        drop(j);
        fs::read(dir.path().join(JOURNAL_FILE)).unwrap()
    }

    #[test]
    fn random_corruption_never_panics() {
        let base = sample_journal();
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..2000 {
            let mut bytes = base.clone();
            for _ in 0..1 + next() % 8 {
                if bytes.len() < 2 {
                    break;
                }
                let at = (next() % bytes.len() as u64) as usize;
                match next() % 4 {
                    0 => bytes[at] ^= 1 << (next() % 8),
                    1 => bytes[at] = next() as u8,
                    2 => {
                        bytes.remove(at);
                    }
                    _ => bytes.truncate(at.max(1)),
                }
            }
            let r = Replay::parse(&bytes);
            // Whatever survived still compacts and round-trips.
            let _ = r.compacted(1);
        }
    }

    #[test]
    fn a_huge_line_is_skipped_without_being_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(JOURNAL_FILE);
        let mut f = File::create(&path).unwrap();
        f.write_all(b"{\"v\":1,\"t\":\"hdr\"}\n").unwrap();
        let chunk = vec![b'a'; 1 << 20];
        for _ in 0..100 {
            f.write_all(&chunk).unwrap();
        }
        f.write_all(b"\n{\"v\":1,\"t\":\"spawn\",\"handle\":\"ag-1\"}\n")
            .unwrap();
        // A second one with no newline at all: torn tail of 100 MB.
        for _ in 0..100 {
            f.write_all(&chunk).unwrap();
        }
        drop(f);
        let r = Replay::read(dir.path());
        assert_eq!(r.runs.len(), 1);
        assert!(r.corrupt >= 1);
        // The writer cuts the torn 100 MB tail instead of reading it in.
        let j = open(dir.path());
        j.append(spawn("ag-2"));
        let r = j.replay();
        assert_eq!(r.runs.len(), 2);
        assert!(fs::metadata(&path).unwrap().len() < 4096 + (101 << 20));
    }

    #[test]
    fn terminal_is_final_and_unknown_handles_are_ignored() {
        let text = [
            r#"{"v":1,"t":"state","handle":"ghost","state":"running"}"#,
            r#"{"v":1,"t":"term","handle":"ghost","reason":"completed"}"#,
            r#"{"v":1,"t":"proc","handle":"ghost","pgid":5}"#,
            r#"{"v":1,"t":"spawn","handle":"ag-1"}"#,
            r#"{"v":1,"t":"spawn","handle":"ag-1","name":"dup"}"#,
            r#"{"v":1,"t":"term","handle":"ag-1","reason":"failed"}"#,
            r#"{"v":1,"t":"state","handle":"ag-1","state":"running"}"#,
            r#"{"v":1,"t":"term","handle":"ag-1","reason":"completed"}"#,
        ]
        .join("\n")
            + "\n";
        let r = Replay::parse(text.as_bytes());
        assert_eq!(r.runs.len(), 1);
        assert_eq!(r.runs[0].state, "failed");
        assert!(r.runs[0].term.is_some());
        assert_eq!(r.runs[0].spawn.name, "");
    }

    #[test]
    fn newer_journal_is_read_only_and_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(JOURNAL_FILE);
        let text = "{\"v\":2,\"t\":\"hdr\"}\n{\"v\":2,\"t\":\"spawn\",\"handle\":\"ag-1\"}\n";
        fs::write(&path, text).unwrap();
        let j = open(dir.path());
        assert!(j.read_only());
        j.append(spawn("ag-2"));
        j.append(term("ag-2", "completed"));
        j.flush();
        drop(j);
        assert_eq!(fs::read_to_string(&path).unwrap(), text);
    }

    #[test]
    fn critical_records_survive_a_flood() {
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(open(dir.path()));
        let flood = {
            let j = Arc::clone(&j);
            std::thread::spawn(move || {
                for i in 0..30_000 {
                    j.append(JournalRecord::State(StateRec {
                        handle: format!("x-{i}"),
                        state: "idle".into(),
                    }));
                }
            })
        };
        for i in 0..50 {
            j.append(spawn(&format!("ag-{i}")));
            j.append(term(&format!("ag-{i}"), "completed"));
        }
        flood.join().unwrap();
        let r = j.replay();
        assert_eq!(r.runs.len(), 50);
        assert!(r.runs.iter().all(|r| r.term.is_some()));
    }

    #[test]
    fn a_symlinked_run_dir_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(RunJournal::open_in(link, JournalOptions::default()).is_err());
        assert!(!real.join(JOURNAL_FILE).exists());
    }

    #[test]
    fn a_symlinked_journal_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, "").unwrap();
        let run = dir.path().join("run");
        fs::create_dir(&run).unwrap();
        std::os::unix::fs::symlink(&target, run.join(JOURNAL_FILE)).unwrap();
        assert!(RunJournal::open_in(run, JournalOptions::default()).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"");
    }

    #[test]
    fn boot_stamp_is_stable_and_checked() {
        let a = boot_stamp();
        assert!(!a.is_empty());
        assert!(same_boot(&a));
        assert!(!same_boot(""));
        assert!(!same_boot("other/pid:[4026531836]"));
    }
}
