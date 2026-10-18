//! Background runs: subagents and shell jobs behind one id scheme, state
//! machine and hub. Shell runs execute (`shell.rs`); agent runs get a driver
//! task that owns their history (`agent.rs`).

pub mod ack;
mod admit;
pub mod agent;
#[cfg(test)]
mod agent_run;
pub mod clock;
mod drive;
pub mod inbox;
pub mod limits;
pub mod notice;
#[cfg(test)]
mod notice_tests;
pub mod shell;
#[cfg(unix)]
mod signals;
mod topology;

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use event_listener::Event;
use futures_lite::future::or;

use maki_providers::{Message, TokenUsage};
use maki_storage::frame::TopologyFacts;
use maki_storage::id::MakiId;
use serde::Deserialize;
use serde_json::Value;

use crate::EventSender;
use crate::cancel::{CancelToken, CancelTrigger};
use crate::tools::ToolAudience;
use crate::tools::registry::Tool;
use crate::{AgentEvent, Envelope, SubagentInfo};

use ack::Acks;
pub use ack::{AckRecord, AckState};
use admit::Pool;
use agent::AgentRun;
pub use agent::{AgentLaunch, resolve_model};
pub use clock::{Clock, ManualClock, SystemClock};
pub use drive::{Joined, WakeSink};
use inbox::{Accepted, Phase};
pub use inbox::{DeliveryKind, Inbox, InboxItem, MessageId, SendAck};
pub use limits::UnitLimits;
pub use notice::{Notice, NoticeInfo, NoticeKind};
use shell::ShellRun;
pub use shell::{OutputSlice, ShellOptions};

const AGENT_HANDLE_PREFIX: &str = "ag";
const SHELL_HANDLE_PREFIX: &str = "sh";
const HANDLE_CHARS: usize = 6;
/// Crockford base32: no `i`, `l`, `o`, `u`, so handles survive being read aloud.
const HANDLE_ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
const NAME_MAX_LEN: usize = 32;
const RESERVED_NAMES: [&str; 2] = ["main", "parent"];
const FALLBACK_NAME: &str = "run";
pub const DEFAULT_AGENT_TIMEOUT_SECS: u64 = 1800;
pub const DEFAULT_SHELL_TIMEOUT_SECS: u64 = 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnitId(MakiId);

impl UnitId {
    pub fn new() -> Self {
        Self(MakiId::generate())
    }

    pub fn id(&self) -> MakiId {
        self.0
    }
}

impl Default for UnitId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnitHandle(Arc<str>);

impl UnitHandle {
    /// Taken from the random tail of the id: the head of a v7 uuid is a
    /// timestamp, so every run started in the same minute would share it.
    fn derive(prefix: &str, id: &UnitId) -> Self {
        let bytes = id.0.as_bytes();
        let mut bits = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        let mut chars = String::with_capacity(prefix.len() + 1 + HANDLE_CHARS);
        chars.push_str(prefix);
        chars.push('-');
        for _ in 0..HANDLE_CHARS {
            chars.push(HANDLE_ALPHABET[(bits & 0x1f) as usize] as char);
            bits >>= 5;
        }
        Self(chars.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UnitHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnitName(Arc<str>);

impl UnitName {
    /// `^[a-z][a-z0-9-]{0,31}$`
    pub fn is_valid(name: &str) -> bool {
        let mut chars = name.chars();
        chars.next().is_some_and(|c| c.is_ascii_lowercase())
            && name.len() <= NAME_MAX_LEN
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    }

    /// Lower-kebab slug of a description, clipped to leave room for a `-N`
    /// suffix; never empty and never a reserved name.
    pub fn slug(description: &str, suffix: Option<u32>) -> String {
        let mut slug = String::new();
        for c in description.chars() {
            if c.is_ascii_alphanumeric() {
                slug.push(c.to_ascii_lowercase());
            } else if !slug.ends_with('-') {
                slug.push('-');
            }
        }
        let slug = slug.trim_matches(|c: char| c == '-' || c.is_ascii_digit());
        let suffix = suffix.map(|n| format!("-{n}")).unwrap_or_default();
        let room = NAME_MAX_LEN - suffix.len();
        let base = if slug.is_empty() { FALLBACK_NAME } else { slug };
        let base = base[..base.len().min(room)].trim_end_matches('-');
        let name = format!("{base}{suffix}");
        if RESERVED_NAMES.contains(&name.as_str()) {
            format!("{name}-run")
        } else {
            name
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UnitName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunKind {
    Agent,
    Shell,
}

impl RunKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Shell => "shell",
        }
    }

    fn handle_prefix(self) -> &'static str {
        match self {
            Self::Agent => AGENT_HANDLE_PREFIX,
            Self::Shell => SHELL_HANDLE_PREFIX,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentSpec {
    pub prompt: String,
    pub subagent_type: String,
    pub model_tier: Option<String>,
    pub thinking: Option<String>,
    pub output_schema: Option<Value>,
    pub fork: bool,
    pub one_shot: bool,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone)]
pub struct ShellSpec {
    pub command: String,
    pub cwd: Option<String>,
    pub timeout_secs: u64,
    pub watch: Option<String>,
}

#[derive(Debug, Clone)]
pub enum UnitKind {
    Agent(AgentSpec),
    Shell(ShellSpec),
}

impl UnitKind {
    pub fn kind(&self) -> RunKind {
        match self {
            Self::Agent(_) => RunKind::Agent,
            Self::Shell(_) => RunKind::Shell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoticePolicy {
    #[default]
    Wake,
    Quiet,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneReason {
    Completed,
    Failed,
    Killed,
    TimedOut,
    MaxTurns,
    MaxTokens,
    Refused,
    Lost,
}

impl DoneReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Killed => "killed",
            Self::TimedOut => "timed_out",
            Self::MaxTurns => "max_turns",
            Self::MaxTokens => "max_tokens",
            Self::Refused => "refused",
            Self::Lost => "lost",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BodyRef(pub PathBuf);

#[derive(Debug, Clone, Default)]
pub struct UnitResult {
    pub summary: String,
    pub truncated: bool,
    pub body_ref: Option<BodyRef>,
    pub structured: Option<Value>,
    pub usage: TokenUsage,
    pub cost: f64,
    pub turns: u32,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Terminal {
    pub reason: DoneReason,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,
    pub result: UnitResult,
}

#[derive(Debug, Clone)]
pub enum UnitState {
    Queued,
    Running,
    Idle,
    Stopping,
    Done(Box<Terminal>),
}

impl UnitState {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Idle => "idle",
            Self::Stopping => "stopping",
            Self::Done(t) => t.reason.as_str(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Unit {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub name: UnitName,
    pub owner: UnitId,
    pub parent: UnitId,
    pub depth: u8,
    pub kind: UnitKind,
    pub description: String,
    pub origin_tool_use_id: Option<Arc<str>>,
    pub state: UnitState,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub last_activity: Instant,
    pub progress: Option<String>,
    pub usage: TokenUsage,
    pub cost: f64,
    pub notice_policy: NoticePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Children,
    Descendants,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitError {
    UnknownRun,
    NotReachable,
    RecipientTerminal,
    InboxFull {
        retry_after: u64,
    },
    TooLarge {
        max: usize,
    },
    RateLimited {
        retry_after: u64,
    },
    SelfSend,
    QueueFull,
    SpawnLimit,
    ShellLimit {
        max: usize,
        per_owner: bool,
    },
    DepthLimit,
    Disabled,
    NotAgent,
    BadField {
        field: &'static str,
        kind: &'static str,
    },
}

impl fmt::Display for UnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRun => f.write_str("UNKNOWN_RUN"),
            Self::NotReachable => f.write_str("NOT_REACHABLE"),
            Self::RecipientTerminal => f.write_str("RECIPIENT_TERMINAL"),
            Self::InboxFull { retry_after } => write!(f, "INBOX_FULL retry_after={retry_after}s"),
            Self::TooLarge { max } => write!(f, "TOO_LARGE max={max}"),
            Self::RateLimited { retry_after } => {
                write!(f, "RATE_LIMITED retry_after={retry_after}s")
            }
            Self::SelfSend => f.write_str("SELF"),
            Self::QueueFull => f.write_str("QUEUE_FULL"),
            Self::SpawnLimit => f.write_str("SPAWN_LIMIT"),
            Self::ShellLimit { max, per_owner } => write!(
                f,
                "SHELL_LIMIT: {max} shells already running{}; wait for one to finish or stop one",
                if *per_owner { " for you" } else { "" }
            ),
            Self::DepthLimit => f.write_str("DEPTH_LIMIT"),
            Self::Disabled => f.write_str("DISABLED"),
            Self::NotAgent => f.write_str("NOT_AGENT"),
            Self::BadField { field, kind } => match *kind {
                "agent" => write!(
                    f,
                    "BAD_FIELD: \"{field}\" is for kind \"agent\"; a shell takes \"command\""
                ),
                "shell" => write!(
                    f,
                    "BAD_FIELD: \"{field}\" is for kind \"shell\"; an agent takes \"prompt\""
                ),
                "reserved" => f.write_str("BAD_FIELD: reserved name"),
                "pattern" => write!(
                    f,
                    "BAD_FIELD: \"{field}\" must match ^[a-z][a-z0-9-]{{0,31}}$"
                ),
                "taken" => write!(f, "BAD_FIELD: \"{field}\" is already in use"),
                "missing" => write!(f, "BAD_FIELD: \"{field}\" is required for this kind"),
                other => write!(f, "BAD_FIELD: \"{field}\" ({other})"),
            },
        }
    }
}

impl std::error::Error for UnitError {}

/// `spawn` arguments exactly as the model sends them.
#[derive(Debug, Clone, Deserialize)]
pub struct SpawnRequest {
    pub kind: RunKind,
    pub description: String,
    pub name: Option<String>,
    pub prompt: Option<String>,
    pub subagent_type: Option<String>,
    pub model_tier: Option<String>,
    pub thinking: Option<String>,
    pub output_schema: Option<Value>,
    pub context: Option<String>,
    pub one_shot: Option<bool>,
    pub command: Option<String>,
    pub cwd: Option<String>,
    pub timeout_secs: Option<u64>,
    pub watch: Option<String>,
    pub notify: Option<NoticePolicy>,
    #[serde(skip)]
    pub origin_tool_use_id: Option<Arc<str>>,
}

impl SpawnRequest {
    pub(crate) fn into_kind(self) -> Result<UnitKind, UnitError> {
        let bad = |field, kind| UnitError::BadField { field, kind };
        let agent_only = [
            ("prompt", self.prompt.is_some()),
            ("subagent_type", self.subagent_type.is_some()),
            ("model_tier", self.model_tier.is_some()),
            ("thinking", self.thinking.is_some()),
            ("output_schema", self.output_schema.is_some()),
            ("context", self.context.is_some()),
            ("one_shot", self.one_shot.is_some()),
        ];
        let shell_only = [
            ("command", self.command.is_some()),
            ("cwd", self.cwd.is_some()),
            ("watch", self.watch.is_some()),
        ];
        let kind = match self.kind {
            RunKind::Agent => {
                if let Some((field, _)) = shell_only.iter().find(|(_, given)| *given) {
                    return Err(bad(field, "shell"));
                }
                let Some(prompt) = self.prompt else {
                    return Err(bad("prompt", "missing"));
                };
                let fork = match self.context.as_deref() {
                    None | Some("fresh") => false,
                    Some("fork") => true,
                    Some(_) => return Err(bad("context", "invalid")),
                };
                UnitKind::Agent(AgentSpec {
                    prompt,
                    subagent_type: self.subagent_type.unwrap_or_else(|| "general".into()),
                    model_tier: self.model_tier,
                    thinking: self.thinking,
                    output_schema: self.output_schema,
                    fork,
                    one_shot: self.one_shot.unwrap_or(false),
                    timeout_secs: self.timeout_secs.unwrap_or(DEFAULT_AGENT_TIMEOUT_SECS),
                })
            }
            RunKind::Shell => {
                if let Some((field, _)) = agent_only.iter().find(|(_, given)| *given) {
                    return Err(bad(field, "agent"));
                }
                let Some(command) = self.command else {
                    return Err(bad("command", "missing"));
                };
                UnitKind::Shell(ShellSpec {
                    command,
                    cwd: self.cwd,
                    timeout_secs: self.timeout_secs.unwrap_or(DEFAULT_SHELL_TIMEOUT_SECS),
                    watch: self.watch,
                })
            }
        };
        Ok(kind)
    }
}

#[derive(Debug, Clone)]
pub struct SpawnAck {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub name: UnitName,
    pub kind: RunKind,
    pub state: UnitState,
    /// 1-based position and queue length while `Queued`.
    pub queue_position: Option<(usize, usize)>,
}

/// A snapshot detached from the table, safe to render without the lock.
#[derive(Debug, Clone)]
pub struct UnitView {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub name: UnitName,
    pub kind: RunKind,
    pub parent: UnitId,
    pub depth: u8,
    pub description: String,
    pub state: UnitState,
    pub age_secs: u64,
    pub cost: f64,
    pub progress: Option<String>,
    pub command: Option<String>,
    /// The final result of a `Done` run, or the last turn's result of an `Idle` agent.
    pub result: Option<UnitResult>,
    /// How many times the run settled or went idle; see [`UnitHub::claim`].
    pub settle_gen: u64,
    /// 1-based position and queue length while waiting for an agent slot.
    pub queue: Option<(usize, usize)>,
}

impl UnitView {
    /// The state as shown to a model: a waiting run says where it stands.
    pub fn state_label(&self) -> String {
        match self.queue {
            Some((at, of)) => format!("queued (position {at} of {of})"),
            None => self.state.label().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitMode {
    All,
    Any,
}

/// `soft` and `detach` only mean something to agents.
#[derive(Debug, Clone, Copy, Default)]
pub struct StopOpts {
    pub soft: bool,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopStatus {
    /// Signalled or already on its way down; it settles on its own.
    Requested,
    /// A soft stop: the agent abandons its turn and goes idle.
    Interrupted,
    AlreadyFinished(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopOutcome {
    pub id: UnitId,
    pub status: Result<StopStatus, UnitError>,
}

#[derive(Default)]
struct UnitTable {
    units: HashMap<UnitId, Unit>,
    shells: HashMap<UnitId, Arc<ShellRun>>,
    agents: HashMap<UnitId, Arc<AgentRun>>,
    /// The last turn's result of each `Idle` agent.
    idle_results: HashMap<UnitId, UnitResult>,
    /// Committed history a `context=fork` spawn starts from, by the id of the
    /// `spawn` call it belongs to.
    fork_snapshots: HashMap<String, Arc<Vec<Message>>>,
    /// Events produced under the lock, sent once it is released.
    outbox: Vec<Envelope>,
    /// Notices created under this lock, moved into the owner's inbox right
    /// after it is released (inbox lock first, hub lock second).
    pending_notices: Vec<PendingNotice>,
    /// How many times each run settled or went idle.
    settle_gen: HashMap<UnitId, u64>,
    /// Highest settle of a run whose notice a committed read consumed.
    consumed: HashMap<UnitId, u64>,
    /// Reads that returned a result and wait for it to be committed, by the
    /// `tool_use_id` of the call.
    claims: HashMap<String, Claim>,
    /// Creation order, which is the order every listing uses.
    order: Vec<UnitId>,
    handles: HashMap<UnitHandle, UnitId>,
    names: HashMap<UnitName, UnitId>,
    children: HashMap<UnitId, Vec<UnitId>>,
    /// Ping-pong guard state per pair of agent runs, see [`UnitHub::send`].
    pairs: HashMap<PairKey, PairState>,
    /// Who holds an agent slot and who waits for one.
    pool: Pool,
    /// When each owner started its recent runs, for `spawn_per_minute`.
    spawn_log: HashMap<UnitId, VecDeque<Instant>>,
}

/// Window of the per-owner spawn rate.
const SPAWN_WINDOW: Duration = Duration::from_secs(60);

fn settled_info(run: UnitId, handle: String, name: String, generation: u64) -> NoticeInfo {
    NoticeInfo {
        kind: NoticeKind::Settled,
        run,
        handle,
        name,
        generation,
        count: 1,
        created: SystemTime::now(),
    }
}

struct PendingNotice {
    owner: UnitId,
    info: NoticeInfo,
    text: String,
    wake: bool,
}

/// A read's claim on notices (design 4.8): consumption only once the tool
/// result that carries the read is committed to the reader's history.
#[derive(Debug, Default, Clone)]
pub struct Claim {
    /// Runs whose result the read returned, with the settle it saw.
    pub runs: Vec<(UnitId, u64)>,
    /// Notices the read listed, by inbox `seq`.
    pub seqs: Vec<u64>,
}

/// Consecutive notice-initiated turns of `main` before further notices are quiet.
pub const WAKE_BUDGET: u32 = 3;

#[derive(Default)]
struct WakeBudget {
    streak: u32,
    marker_sent: bool,
}

/// An unordered pair of runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PairKey(UnitId, UnitId);

impl PairKey {
    fn of(a: UnitId, b: UnitId) -> Self {
        if a.0.to_string() <= b.0.to_string() {
            Self(a, b)
        } else {
            Self(b, a)
        }
    }

    fn has(&self, id: UnitId) -> bool {
        self.0 == id || self.1 == id
    }
}

#[derive(Default)]
struct PairState {
    /// Consecutive agent-to-agent messages since the last other work.
    count: u32,
    /// Messages kept back after the burst, released when the counter resets.
    held: Vec<(UnitId, InboxItem)>,
    notified: bool,
}

impl UnitTable {
    fn unique_name(&self, description: &str) -> UnitName {
        let taken = |s: &str| self.names.keys().any(|n| n.as_str() == s);
        let mut candidate = UnitName::slug(description, None);
        let mut n = 2;
        while taken(&candidate) {
            candidate = UnitName::slug(description, Some(n));
            n += 1;
        }
        UnitName(candidate.into())
    }

    fn fresh_identity(&self, kind: RunKind) -> (UnitId, UnitHandle) {
        loop {
            let id = UnitId::new();
            let handle = UnitHandle::derive(kind.handle_prefix(), &id);
            if !self.handles.contains_key(&handle) {
                return (id, handle);
            }
        }
    }

    /// An event about `id`, stamped when it is an agent run.
    fn event_of(&self, id: UnitId, event: AgentEvent) -> Envelope {
        Envelope {
            event,
            subagent: self.agents.get(&id).map(|run| run.info.clone()),
            run_id: crate::BACKGROUND_RUN_ID,
        }
    }

    /// Every run below `root`, children before their parents.
    fn subtree_leaves_first(&self, root: UnitId) -> Vec<UnitId> {
        let mut all = topology::descendants(&self.children, root);
        all.reverse();
        all
    }

    fn live_children(&self, id: UnitId) -> usize {
        self.children
            .get(&id)
            .into_iter()
            .flatten()
            .filter(|c| !self.units[c].state.is_done())
            .count()
    }

    /// Gives free slots to the head of the line; a run that was `Queued`
    /// becomes `Running`. The caller wakes the waiters after the lock.
    fn promote(&mut self, max: usize) {
        let units = &self.units;
        self.pool.resume(|id| {
            units
                .get(&id)
                .is_none_or(|u| matches!(u.state, UnitState::Done(_) | UnitState::Idle))
        });
        for id in self.pool.grant(max) {
            let Some(unit) = self.units.get_mut(&id) else {
                continue;
            };
            if !matches!(unit.state, UnitState::Queued) {
                continue;
            }
            unit.state = UnitState::Running;
            unit.started_at.get_or_insert_with(SystemTime::now);
            let handle = unit.handle.to_string();
            let event = self.event_of(
                id,
                AgentEvent::RunStateChanged {
                    handle,
                    state: "running".to_owned(),
                },
            );
            self.outbox.push(event);
        }
    }

    fn running_shells(&self, owner: Option<UnitId>) -> usize {
        self.units
            .values()
            .filter(|u| {
                matches!(u.kind, UnitKind::Shell(_))
                    && !u.state.is_done()
                    && owner.is_none_or(|o| u.owner == o)
            })
            .count()
    }

    fn view(&self, id: UnitId) -> UnitView {
        let unit = &self.units[&id];
        UnitView {
            id,
            handle: unit.handle.clone(),
            name: unit.name.clone(),
            kind: unit.kind.kind(),
            parent: unit.parent,
            depth: unit.depth,
            description: unit.description.clone(),
            state: unit.state.clone(),
            age_secs: unit.created_at.elapsed().map_or(0, |d| d.as_secs()),
            cost: unit.cost,
            progress: unit.progress.clone().or_else(|| {
                matches!(unit.state, UnitState::Running)
                    .then(|| self.shells.get(&id)?.last_line())
                    .flatten()
            }),
            command: match &unit.kind {
                UnitKind::Shell(s) => Some(s.command.clone()),
                UnitKind::Agent(_) => None,
            },
            result: match &unit.state {
                UnitState::Done(t) => Some(t.result.clone()),
                UnitState::Idle => self.idle_results.get(&id).cloned(),
                _ => None,
            },
            settle_gen: self.settle_gen.get(&id).copied().unwrap_or(0),
            queue: self.pool.position(id),
        }
    }
}

/// One per root session.
pub struct UnitHub {
    main: UnitId,
    units: Mutex<UnitTable>,
    limits: UnitLimits,
    clock: Arc<dyn Clock>,
    shell: ShellOptions,
    /// Fired after any run settles; `wait` listens before it looks.
    settled: Event,
    events: EventSender,
    /// Session-lifetime root of every agent run's cancel token. It is not the
    /// parent turn's token and not in the per-run `CancelMap`, so ending a
    /// turn leaves background agents alone.
    cancel: CancelToken,
    cancel_root: Mutex<Option<CancelTrigger>>,
    /// `main`'s inbox. The root agent drains it like any run; an idle `main`
    /// is started by its frontend once [`UnitHub::claim_main_wake`] says so.
    main_inbox: Arc<Inbox>,
    wake: Mutex<WakeBudget>,
    /// A turn of `main` is running or about to; see `drive.rs`.
    main_busy: AtomicBool,
    /// Notices taken out of the pending list and not yet in an inbox.
    notices_in_flight: AtomicUsize,
    /// The reserved sender of text the user types into a run's tab.
    user: UnitId,
    message_seq: AtomicU64,
    /// Delivery acks per sender. A leaf lock: nothing is taken under it.
    acks: Mutex<Acks>,
}

/// Messages the ping-pong guard keeps per pair before it refuses more.
const MAX_HELD_PER_PAIR: usize = 4;

/// What `stop` decided under the lock and carries out after it.
#[derive(Default)]
struct StopPlan {
    cancel: Vec<(UnitId, Arc<AgentRun>)>,
    interrupt: Vec<Arc<AgentRun>>,
    settled: bool,
}

impl UnitHub {
    pub fn new(limits: UnitLimits, events: EventSender, clock: Arc<dyn Clock>) -> Arc<Self> {
        Self::with_shell_options(limits, events, clock, ShellOptions::default())
    }

    pub fn with_shell_options(
        limits: UnitLimits,
        events: EventSender,
        clock: Arc<dyn Clock>,
        shell: ShellOptions,
    ) -> Arc<Self> {
        let (trigger, cancel) = CancelToken::new();
        Arc::new(Self {
            main: UnitId::new(),
            units: Mutex::default(),
            limits,
            clock,
            shell,
            settled: Event::new(),
            events: events.with_run_id(crate::BACKGROUND_RUN_ID),
            cancel,
            cancel_root: Mutex::new(Some(trigger)),
            main_inbox: Arc::new(Inbox::new(Phase::Idle)),
            wake: Mutex::new(WakeBudget::default()),
            main_busy: AtomicBool::new(false),
            notices_in_flight: AtomicUsize::new(0),
            user: UnitId::new(),
            message_seq: AtomicU64::new(0),
            acks: Mutex::default(),
        })
    }

    /// The root agent's identity; it owns the top-level runs.
    pub fn main(&self) -> UnitId {
        self.main
    }

    fn table(&self) -> MutexGuard<'_, UnitTable> {
        self.units.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Drops the lock, then sends what was queued under it.
    fn release(&self, mut table: MutexGuard<'_, UnitTable>) {
        let events = std::mem::take(&mut table.outbox);
        drop(table);
        for envelope in events {
            let _ = self.events.send_envelope(envelope);
        }
    }

    fn depth_of(&self, table: &UnitTable, caller: UnitId) -> Result<u8, UnitError> {
        if caller == self.main {
            return Ok(0);
        }
        table
            .units
            .get(&caller)
            .map(|u| u.depth)
            .ok_or(UnitError::UnknownRun)
    }

    /// Shell runs start at once. An agent run starts only through
    /// [`Self::spawn_agent`], which brings what it needs to run; asked for
    /// here it stays `Queued`, a record with no driver.
    pub fn spawn(
        self: &Arc<Self>,
        caller: UnitId,
        req: SpawnRequest,
    ) -> Result<SpawnAck, UnitError> {
        self.spawn_with(caller, req, None)
    }

    /// Starts an agent run: its driver task owns the history and the cancel
    /// token is a child of the session root. `launch` comes from
    /// [`AgentLaunch::prepare`]; `req.origin_tool_use_id` selects the history
    /// snapshot for `context=fork`.
    pub fn spawn_agent(
        self: &Arc<Self>,
        caller: UnitId,
        req: SpawnRequest,
        launch: AgentLaunch,
    ) -> Result<SpawnAck, UnitError> {
        self.spawn_with(caller, req, Some(launch))
    }

    fn spawn_with(
        self: &Arc<Self>,
        caller: UnitId,
        mut req: SpawnRequest,
        mut launch: Option<AgentLaunch>,
    ) -> Result<SpawnAck, UnitError> {
        let description = req.description.clone();
        let name = req.name.take();
        let notice_policy = req.notify.unwrap_or_default();
        let origin_tool_use_id = req.origin_tool_use_id.take();
        let is_agent = req.kind == RunKind::Agent;
        let default_timeout = if is_agent {
            self.limits.default_timeout.as_secs()
        } else {
            DEFAULT_SHELL_TIMEOUT_SECS
        };
        req.timeout_secs = Some(
            req.timeout_secs
                .unwrap_or(default_timeout)
                .clamp(1, self.limits.max_timeout.as_secs().max(1)),
        );
        let kind = req.into_kind()?;
        let mut table = self.table();
        let caller_depth = self.depth_of(&table, caller)?;
        if table
            .units
            .get(&caller)
            .is_some_and(|u| matches!(u.state, UnitState::Stopping | UnitState::Done(_)))
        {
            return Err(UnitError::RecipientTerminal);
        }
        let name = match name {
            Some(n) if RESERVED_NAMES.contains(&n.as_str()) => {
                return Err(UnitError::BadField {
                    field: "name",
                    kind: "reserved",
                });
            }
            Some(n) if !UnitName::is_valid(&n) => {
                return Err(UnitError::BadField {
                    field: "name",
                    kind: "pattern",
                });
            }
            Some(n) if table.names.keys().any(|taken| taken.as_str() == n) => {
                return Err(UnitError::BadField {
                    field: "name",
                    kind: "taken",
                });
            }
            Some(n) => UnitName(n.into()),
            None => table.unique_name(&description),
        };
        if kind.kind() == RunKind::Agent && caller_depth >= self.limits.max_depth {
            return Err(UnitError::DepthLimit);
        }
        let live = table.live_children(caller);
        if live >= usize::from(self.limits.max_children_per_run)
            || table.units.len() >= usize::from(self.limits.session_max_runs)
        {
            return Err(UnitError::SpawnLimit);
        }
        if !is_agent {
            let max = usize::from(self.limits.max_shells);
            let per_owner = usize::from(self.limits.max_shells_per_owner);
            if table.running_shells(None) >= max {
                return Err(UnitError::ShellLimit {
                    max,
                    per_owner: false,
                });
            }
            if table.running_shells(Some(caller)) >= per_owner {
                return Err(UnitError::ShellLimit {
                    max: per_owner,
                    per_owner: true,
                });
            }
        }
        let max_agents = usize::from(self.limits.max_agents);
        let would_queue = is_agent && table.pool.would_queue(max_agents);
        if would_queue && table.pool.queued_len() >= usize::from(self.limits.max_queue) {
            return Err(UnitError::QueueFull);
        }
        let now = self.clock.now();
        let recent = table.spawn_log.entry(caller).or_default();
        while recent
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= SPAWN_WINDOW)
        {
            recent.pop_front();
        }
        if recent.len() >= usize::from(self.limits.spawn_per_minute) {
            let wait = recent.front().map_or(SPAWN_WINDOW, |t| {
                SPAWN_WINDOW - now.saturating_duration_since(*t)
            });
            return Err(UnitError::RateLimited {
                retry_after: wait.as_secs() + u64::from(wait.subsec_nanos() > 0),
            });
        }
        if let (UnitKind::Agent(spec), Some(launch)) = (&kind, launch.as_mut())
            && spec.fork
        {
            let snapshot = origin_tool_use_id
                .as_deref()
                .and_then(|call| table.fork_snapshots.remove(call));
            launch.fork = Some(snapshot.ok_or(UnitError::BadField {
                field: "context",
                kind: "fork needs a completed turn to copy",
            })?);
        }
        table.spawn_log.entry(caller).or_default().push_back(now);
        let (id, handle) = table.fresh_identity(kind.kind());
        let shell_spec = match &kind {
            UnitKind::Shell(spec) => Some(spec.clone()),
            UnitKind::Agent(_) => None,
        };
        let running = !would_queue;
        let state = if running {
            UnitState::Running
        } else {
            UnitState::Queued
        };
        let unit = Unit {
            id,
            handle: handle.clone(),
            name: name.clone(),
            owner: caller,
            parent: caller,
            depth: caller_depth + 1,
            kind,
            description,
            origin_tool_use_id,
            state: state.clone(),
            created_at: SystemTime::now(),
            started_at: running.then(SystemTime::now),
            finished_at: None,
            last_activity: self.clock.now(),
            progress: None,
            usage: TokenUsage::default(),
            cost: 0.0,
            notice_policy,
        };
        let kind = unit.kind.kind();
        let agent_spec = match &unit.kind {
            UnitKind::Agent(spec) => Some(spec.clone()),
            UnitKind::Shell(_) => None,
        };
        table.handles.insert(handle.clone(), id);
        table.names.insert(name.clone(), id);
        table.children.entry(caller).or_default().push(id);
        table.order.push(id);
        table.units.insert(id, unit);
        if is_agent {
            table.pool.request(id, false);
            table.pool.grant(max_agents);
        }
        let queue_position = table.pool.position(id);
        let started = shell_spec.map(|spec| {
            let log = self
                .shell
                .dir
                .as_ref()
                .map(|dir| dir.join(format!("{handle}.log")));
            let run = ShellRun::new(log);
            table.shells.insert(id, Arc::clone(&run));
            (spec, run)
        });
        let agent_started = match (agent_spec, launch) {
            (Some(spec), Some(launch)) => {
                let info = SubagentInfo {
                    parent_tool_use_id: handle.to_string(),
                    name: name.to_string(),
                    prompt: Some(spec.prompt.clone()),
                    model: Some(launch.model_spec.clone()),
                    opts: Some(launch.request_options()),
                    answer_tx: None,
                    inbox: None,
                };
                let run = AgentRun::new(&self.cancel, info);
                table.agents.insert(id, Arc::clone(&run));
                Some((spec, launch, run))
            }
            _ => None,
        };
        let started_event = table.event_of(
            id,
            AgentEvent::RunStarted {
                handle: handle.to_string(),
                name: name.to_string(),
                kind: kind.as_str().to_owned(),
            },
        );
        table.outbox.push(started_event);
        self.release(table);
        if let Some((spec, run)) = started {
            shell::start(
                Arc::downgrade(self),
                id,
                run,
                spec,
                Arc::clone(&self.clock),
                self.shell.clone(),
            );
        }
        if let Some((spec, launch, run)) = agent_started {
            agent::start(
                Arc::downgrade(self),
                id,
                run,
                spec,
                launch,
                Arc::clone(&self.clock),
            );
        }
        Ok(SpawnAck {
            id,
            handle,
            name,
            kind,
            state,
            queue_position,
        })
    }

    /// The caller's committed history for the `spawn` calls of one assistant
    /// turn that asked for `context=fork`. It ends on a complete user or tool
    /// result boundary because the turn in flight is not in it yet.
    pub fn publish_fork_snapshot(&self, tool_use_ids: &[String], messages: Vec<Message>) {
        const KEPT: usize = 64;
        let snapshot = Arc::new(messages);
        let mut table = self.table();
        if table.fork_snapshots.len() >= KEPT {
            table.fork_snapshots.clear();
        }
        for id in tool_use_ids {
            table
                .fork_snapshots
                .insert(id.clone(), Arc::clone(&snapshot));
        }
    }

    /// First transition into `Done` wins. The notice is queued in the same
    /// critical section, so a `wait` racing this settle cannot miss it.
    pub(crate) fn settle(&self, id: UnitId, terminal: Terminal, output_bytes: u64) {
        let mut table = self.table();
        self.settle_locked(&mut table, id, terminal, output_bytes);
        self.release(table);
        self.flush_notices();
        self.settled.notify(usize::MAX);
    }

    fn settle_locked(
        &self,
        table: &mut UnitTable,
        id: UnitId,
        mut terminal: Terminal,
        output_bytes: u64,
    ) {
        let Some(unit) = table.units.get_mut(&id) else {
            return;
        };
        if unit.state.is_done() {
            return;
        }
        // An agent stopped while idle keeps the result of its last turn.
        if terminal.result.summary.is_empty()
            && let Some(last) = table.idle_results.remove(&id)
        {
            terminal.result = last;
        }
        let text = (unit.notice_policy != NoticePolicy::Never).then(|| match &unit.kind {
            UnitKind::Shell(spec) => notice::shell_settled(
                &unit.handle,
                &unit.name,
                &terminal,
                spec.timeout_secs,
                output_bytes,
            ),
            UnitKind::Agent(spec) => {
                notice::agent_settled(&unit.handle, &unit.name, &terminal, spec.timeout_secs)
            }
        });
        let finished = AgentEvent::RunFinished {
            handle: unit.handle.to_string(),
            reason: terminal.reason.as_str().to_owned(),
            detail: terminal.detail.clone(),
        };
        unit.state = UnitState::Done(Box::new(terminal));
        unit.finished_at = Some(SystemTime::now());
        unit.last_activity = self.clock.now();
        unit.progress = None;
        let (owner, wake) = (unit.owner, unit.notice_policy == NoticePolicy::Wake);
        let (handle, name) = (unit.handle.to_string(), unit.name.to_string());
        // Every way a run ends passes here, so a slot cannot leak.
        table.pool.drop_run(id);
        table.promote(usize::from(self.limits.max_agents));
        let generation = table.settle_gen.entry(id).or_default();
        *generation += 1;
        let generation = *generation;
        if let Some(text) = text {
            table.pending_notices.push(PendingNotice {
                owner,
                info: settled_info(id, handle, name, generation),
                text,
                wake,
            });
        }
        let event = table.event_of(id, finished);
        table.outbox.push(event);
    }

    /// An agent finished a turn: `Running` becomes `Idle` and the result is
    /// kept in the record. False when the run is already on its way down.
    pub(crate) fn settle_idle(&self, id: UnitId, result: UnitResult, notify: bool) -> bool {
        let mut table = self.table();
        let Some(unit) = table.units.get_mut(&id) else {
            return false;
        };
        if !matches!(unit.state, UnitState::Running) {
            return false;
        }
        unit.state = UnitState::Idle;
        // An idle agent holds no slot.
        unit.last_activity = self.clock.now();
        unit.progress = None;
        let text = (notify && unit.notice_policy != NoticePolicy::Never)
            .then(|| notice::agent_idle(&unit.handle, &unit.name, &result));
        let (owner, wake, handle, name) = (
            unit.owner,
            unit.notice_policy == NoticePolicy::Wake,
            unit.handle.to_string(),
            unit.name.to_string(),
        );
        table.idle_results.insert(id, result);
        table.pool.drop_run(id);
        table.promote(usize::from(self.limits.max_agents));
        let generation = table.settle_gen.entry(id).or_default();
        *generation += 1;
        let generation = *generation;
        if let Some(text) = text {
            table.pending_notices.push(PendingNotice {
                owner,
                info: settled_info(id, handle.clone(), name, generation),
                text,
                wake,
            });
        }
        let event = table.event_of(
            id,
            AgentEvent::RunStateChanged {
                handle,
                state: "idle".to_owned(),
            },
        );
        table.outbox.push(event);
        self.release(table);
        self.flush_notices();
        self.settled.notify(usize::MAX);
        true
    }

    /// Own usage of an agent run, one turn at a time. Nothing is rolled up
    /// here: the ledger chain already adds a child's spend to its parent's.
    pub(crate) fn record_turn(&self, id: UnitId, usage: TokenUsage, cost: Option<f64>) {
        if let Some(unit) = self.table().units.get_mut(&id) {
            unit.usage += usage;
            unit.cost += cost.unwrap_or(0.0);
            unit.last_activity = self.clock.now();
        }
    }

    pub(crate) fn set_progress(&self, id: UnitId, progress: Option<String>) {
        if let Some(unit) = self.table().units.get_mut(&id) {
            unit.progress = progress;
            unit.last_activity = self.clock.now();
        }
    }

    /// Resolves once every child of `id` is `Done`.
    pub(crate) async fn children_settled(&self, id: UnitId) {
        loop {
            let listener = self.settled.listen();
            if self.table().live_children(id) == 0 {
                return;
            }
            listener.await;
        }
    }

    /// A run that ends has no owner left for its children: stop them all,
    /// without a notice.
    pub(crate) fn stop_children(self: &Arc<Self>, parent: UnitId) {
        let kids = self
            .table()
            .children
            .get(&parent)
            .cloned()
            .unwrap_or_default();
        if !kids.is_empty() {
            self.stop(parent, &kids, StopOpts::default());
        }
    }

    pub(crate) fn mark_stopping(&self, id: UnitId) {
        if let Some(unit) = self.table().units.get_mut(&id)
            && matches!(unit.state, UnitState::Running)
        {
            unit.state = UnitState::Stopping;
        }
    }

    fn is_below(&self, table: &UnitTable, caller: UnitId, id: UnitId) -> bool {
        if caller == self.main {
            return table.units.contains_key(&id);
        }
        let mut cur = id;
        while let Some(unit) = table.units.get(&cur) {
            if unit.parent == caller {
                return true;
            }
            cur = unit.parent;
        }
        false
    }

    /// Whether `id` is in `caller`'s subtree: the only runs it may wait on,
    /// read and stop.
    pub fn can_reach(&self, caller: UnitId, id: UnitId) -> bool {
        let table = self.table();
        self.is_below(&table, caller, id)
    }

    /// Starts the stop and returns without waiting. Agents are stopped
    /// together with everything below them, leaves first, and every run in
    /// the subtree goes `Stopping` in this one critical section so nothing can
    /// attach to it. A shell gets SIGTERM, then SIGKILL once the grace period
    /// is over. The owner asked, so no notice follows. Safe to repeat.
    pub fn stop(
        self: &Arc<Self>,
        caller: UnitId,
        ids: &[UnitId],
        opts: StopOpts,
    ) -> Vec<StopOutcome> {
        let mut table = self.table();
        let mut plan = StopPlan::default();
        let outcomes = ids
            .iter()
            .map(|&id| {
                let status = if self.is_below(&table, caller, id) {
                    self.stop_one(&mut table, id, opts, &mut plan)
                } else {
                    Err(UnitError::NotReachable)
                };
                StopOutcome { id, status }
            })
            .collect();
        self.release(table);
        self.flush_notices();
        if plan.settled {
            self.settled.notify(usize::MAX);
        }
        for run in &plan.interrupt {
            run.interrupt_turn();
        }
        for (id, run) in plan.cancel {
            let left = run.cancel_run();
            self.messages_dropped(&left, "recipient_stopped");
            self.watch_for_force_settle(id);
        }
        outcomes
    }

    fn stop_one(
        &self,
        table: &mut UnitTable,
        id: UnitId,
        opts: StopOpts,
        plan: &mut StopPlan,
    ) -> Result<StopStatus, UnitError> {
        let is_agent = table.units[&id].kind.kind() == RunKind::Agent;
        let state = table.units[&id].state.clone();
        if let UnitState::Done(t) = &state {
            return Ok(StopStatus::AlreadyFinished(t.reason.as_str().to_owned()));
        }
        if opts.soft {
            if !is_agent {
                return Err(UnitError::NotAgent);
            }
            return match (&state, table.agents.get(&id)) {
                (UnitState::Running, Some(run)) => {
                    plan.interrupt.push(Arc::clone(run));
                    Ok(StopStatus::Interrupted)
                }
                _ => Ok(StopStatus::AlreadyFinished(state.label().to_owned())),
            };
        }
        if opts.detach {
            self.detach_children(table, id);
        }
        let mut order = if opts.detach {
            Vec::new()
        } else {
            table.subtree_leaves_first(id)
        };
        order.push(id);
        for uid in order {
            self.stop_unit(table, uid, plan);
        }
        Ok(StopStatus::Requested)
    }

    /// The one exception to "never re-parented": children of a stopped agent
    /// that is told to leave them running move up to its own parent.
    fn detach_children(&self, table: &mut UnitTable, id: UnitId) {
        let grandparent = table.units[&id].parent;
        let kids = table.children.remove(&id).unwrap_or_default();
        for kid in &kids {
            if let Some(unit) = table.units.get_mut(kid) {
                unit.parent = grandparent;
                unit.owner = grandparent;
            }
        }
        table.children.entry(grandparent).or_default().extend(kids);
    }

    fn stop_unit(&self, table: &mut UnitTable, id: UnitId, plan: &mut StopPlan) {
        let Some(unit) = table.units.get_mut(&id) else {
            return;
        };
        match unit.state {
            UnitState::Done(_) => {}
            UnitState::Queued => {
                unit.notice_policy = NoticePolicy::Never;
                let terminal = Terminal {
                    reason: DoneReason::Killed,
                    exit_code: None,
                    detail: None,
                    result: UnitResult::default(),
                };
                self.settle_locked(table, id, terminal, 0);
                plan.settled = true;
                // A queued agent has a driver parked on its slot: end it.
                if let Some(run) = table.agents.get(&id) {
                    plan.cancel.push((id, Arc::clone(run)));
                }
            }
            UnitState::Running | UnitState::Idle | UnitState::Stopping => {
                unit.notice_policy = NoticePolicy::Never;
                if !matches!(unit.state, UnitState::Stopping) {
                    unit.state = UnitState::Stopping;
                }
                if let Some(run) = table.shells.get(&id) {
                    run.request_stop();
                } else if let Some(run) = table.agents.get(&id) {
                    plan.cancel.push((id, Arc::clone(run)));
                }
            }
        }
    }

    /// A run that ignores its cancel is settled `killed` after a deadline and
    /// its driver dropped, so a stuck runner cannot hold up a stop or the
    /// shutdown.
    fn watch_for_force_settle(self: &Arc<Self>, id: UnitId) {
        let hub = Arc::downgrade(self);
        let clock = Arc::clone(&self.clock);
        smol::spawn(async move {
            clock
                .sleep_until(clock.now() + agent::FORCE_SETTLE_AFTER)
                .await;
            if let Some(hub) = hub.upgrade() {
                hub.force_settle(id);
            }
        })
        .detach();
    }

    fn force_settle(&self, id: UnitId) {
        let mut table = self.table();
        let run = table.agents.get(&id).cloned();
        let terminal = Terminal {
            reason: DoneReason::Killed,
            exit_code: None,
            detail: Some("forced".to_owned()),
            result: UnitResult::default(),
        };
        self.settle_locked(&mut table, id, terminal, 0);
        self.release(table);
        self.flush_notices();
        self.settled.notify(usize::MAX);
        if let Some(run) = run {
            run.abort();
        }
    }

    /// Returns the current view of `ids` once they are settled (`Done` or
    /// `Idle`) per `mode`, or when `timeout` or `cancel` ends the wait; the
    /// caller can tell which from the states. Only runs in the caller's
    /// subtree count; others are left out. Results returned as settled are
    /// consumed, so they do not come again as notices; a timeout claims
    /// nothing, and a cancel leaves the runs alone.
    pub async fn wait(
        &self,
        caller: UnitId,
        ids: &[UnitId],
        mode: WaitMode,
        timeout: Duration,
        cancel: &CancelToken,
    ) -> Vec<UnitView> {
        let ids: Vec<UnitId> = {
            let table = self.table();
            ids.iter()
                .copied()
                .filter(|&id| self.is_below(&table, caller, id))
                .collect()
        };
        let deadline = self.clock.now() + timeout;
        // A run that blocks here gives its slot back (and takes one again,
        // ahead of runs that never started, once it is done blocking).
        let mut released = false;
        let on = (ids.clone(), mode == WaitMode::All);
        let views = loop {
            let listener = self.settled.listen();
            let (views, satisfied) = {
                let table = self.table();
                let views: Vec<UnitView> = ids
                    .iter()
                    .filter(|id| table.units.contains_key(id))
                    .map(|&id| table.view(id))
                    .collect();
                let settled =
                    |v: &UnitView| matches!(v.state, UnitState::Done(_) | UnitState::Idle);
                let satisfied = match mode {
                    WaitMode::All => views.iter().all(settled),
                    WaitMode::Any => views.iter().any(settled),
                };
                (views, satisfied)
            };
            if satisfied || cancel.is_cancelled() {
                break views;
            }
            if !released {
                released = self.release_slot(caller, Some(on.clone()));
            }
            let ended = or(
                or(
                    async {
                        listener.await;
                        false
                    },
                    async {
                        self.clock.sleep_until(deadline).await;
                        true
                    },
                ),
                async {
                    cancel.cancelled().await;
                    true
                },
            )
            .await;
            if ended {
                let table = self.table();
                break ids
                    .iter()
                    .filter(|id| table.units.contains_key(id))
                    .map(|&id| table.view(id))
                    .collect();
            }
        };
        if released {
            self.acquire(caller, cancel, true).await;
        }
        views
    }

    /// Waits for an agent slot and holds it; false when the run was stopped
    /// or `token` fired first. Nothing is held then. A fresh or woken run
    /// queues behind everyone; `priority` (a run that resumes after blocking)
    /// goes ahead of runs that never started. The pair of this and
    /// [`Self::release_slot`] is the hook for any wait that must not pin a
    /// slot (`wait` today, a permission prompt in 5b).
    pub(crate) async fn acquire(&self, id: UnitId, token: &CancelToken, priority: bool) -> bool {
        loop {
            let listener = self.settled.listen();
            if let Some(got) = self.try_acquire(id, priority) {
                return got;
            }
            let cancelled = or(
                async {
                    listener.await;
                    false
                },
                async {
                    token.cancelled().await;
                    true
                },
            )
            .await;
            if cancelled {
                self.table().pool.drop_run(id);
                return false;
            }
        }
    }

    /// One step of [`Self::acquire`] under the lock: `Some(true)` holding,
    /// `Some(false)` the run is no longer admissible, `None` still waiting.
    fn try_acquire(&self, id: UnitId, priority: bool) -> Option<bool> {
        let mut table = self.table();
        let state = table.units.get(&id).map(|u| u.state.clone());
        if !matches!(
            state,
            Some(UnitState::Queued | UnitState::Running | UnitState::Idle)
        ) {
            table.pool.drop_run(id);
            return Some(false);
        }
        if table.pool.holds(id) {
            return Some(true);
        }
        table.pool.request(id, priority);
        if !priority
            && matches!(state, Some(UnitState::Running))
            && let Some(unit) = table.units.get_mut(&id)
        {
            unit.state = UnitState::Queued;
            let handle = unit.handle.to_string();
            let event = table.event_of(
                id,
                AgentEvent::RunStateChanged {
                    handle,
                    state: "queued".to_owned(),
                },
            );
            table.outbox.push(event);
        }
        table.promote(usize::from(self.limits.max_agents));
        let got = table.pool.holds(id);
        self.release(table);
        got.then_some(true)
    }

    /// Gives `id`'s slot back, if it holds one, and hands it on. True when
    /// a slot was released.
    /// With `on`, the run is put back in line with priority as soon as that
    /// wait is satisfied, inside the same critical section that settles the
    /// run it waited for.
    pub(crate) fn release_slot(&self, id: UnitId, on: Option<(Vec<UnitId>, bool)>) -> bool {
        let mut table = self.table();
        let held = table.pool.block(id, on);
        if held {
            table.promote(usize::from(self.limits.max_agents));
        }
        self.release(table);
        if held {
            self.settled.notify(usize::MAX);
        }
        held
    }

    /// Slots in use and the pool size, for accounting checks and `runs`.
    pub fn slots_in_use(&self) -> usize {
        self.table().pool.held_count()
    }

    /// Runs started this session and the backstop.
    pub fn session_usage(&self) -> (usize, usize) {
        (
            self.table().units.len(),
            usize::from(self.limits.session_max_runs),
        )
    }

    /// Output of a shell run since `reader` last consumed, or a byte page
    /// when `from` is given. `consume` moves the reader's cursor; observers
    /// pass false. Agents have no output.
    pub fn read_output(
        &self,
        reader: UnitId,
        id: UnitId,
        from: Option<u64>,
        tail: usize,
        consume: bool,
    ) -> OutputSlice {
        let run = self.table().shells.get(&id).cloned();
        run.map(|run| run.read(reader, from, tail, consume))
            .unwrap_or_default()
    }

    /// Notices queued for `owner` and not yet drained, oldest first.
    pub fn pending_notices(&self, owner: UnitId) -> Vec<Notice> {
        self.inbox_of(owner)
            .map(|inbox| inbox.lock().queued_notices())
            .unwrap_or_default()
    }

    /// Moves what settling queued under the hub lock into the owners' inboxes.
    /// Called with no lock held.
    fn flush_notices(&self) {
        let batch = {
            let mut table = self.table();
            let batch = std::mem::take(&mut table.pending_notices);
            // Counted under the lock, so `join` never sees a notice in neither place.
            self.notices_in_flight
                .fetch_add(batch.len(), Ordering::SeqCst);
            batch
        };
        let any = !batch.is_empty();
        for pending in batch {
            self.enqueue_notice(pending);
            self.notices_in_flight.fetch_sub(1, Ordering::SeqCst);
        }
        if any {
            self.settled.notify(usize::MAX);
        }
    }

    /// The check of "already consumed" and the enqueue are one inbox critical
    /// section, so a read finalised in between cannot lose or duplicate it.
    fn enqueue_notice(&self, pending: PendingNotice) {
        let PendingNotice {
            owner,
            info,
            text,
            wake,
        } = pending;
        let Some(inbox) = self.inbox_of(owner) else {
            return;
        };
        let mut guard = inbox.lock();
        if info.kind == NoticeKind::Settled
            && self
                .table()
                .consumed
                .get(&info.run)
                .is_some_and(|&seen| seen >= info.generation)
        {
            return;
        }
        let item = InboxItem::notice(info, text, wake, self.clock.now());
        let Some(accepted) = guard.enqueue_notice(item, self.clock.now()) else {
            return;
        };
        if accepted == Accepted::Wake && owner != self.main {
            guard.set_phase(Phase::Running);
            self.mark_running(owner);
        }
        drop(guard);
        inbox.signal();
        if accepted == Accepted::Wake && owner == self.main {
            self.events.try_send(AgentEvent::RunNotice { wake: true });
        }
    }

    /// A read returned results. Records what it saw; the notices go only when
    /// [`Self::commit_claims`] says the tool result is in the reader's
    /// history. A call with no `tool_use_id` has no history to wait for and
    /// commits at once. Only the owner's reads count: the notice is its own.
    pub fn claim(&self, owner: UnitId, tool_use_id: Option<&str>, mut claim: Claim) {
        {
            let table = self.table();
            claim
                .runs
                .retain(|(run, _)| table.units.get(run).is_some_and(|u| u.owner == owner));
        }
        if claim.runs.is_empty() && claim.seqs.is_empty() {
            return;
        }
        match tool_use_id {
            Some(id) => {
                let mut table = self.table();
                if table.claims.len() >= 64 {
                    table.claims.clear();
                }
                // Replace, never merge: a provider may reuse a call id, and a
                // stale claim of an abandoned call must not ride on a new one.
                table.claims.insert(id.to_owned(), claim);
            }
            None => self.finalize(owner, &claim),
        }
    }

    /// The tool results for `tool_use_ids` are in the reader's history.
    pub fn commit_claims(&self, owner: UnitId, tool_use_ids: &[String]) {
        let claims: Vec<Claim> = {
            let mut table = self.table();
            if table.claims.is_empty() {
                return;
            }
            tool_use_ids
                .iter()
                .filter_map(|id| table.claims.remove(id))
                .collect()
        };
        for claim in &claims {
            self.finalize(owner, claim);
        }
    }

    /// A read was cancelled or failed before it was committed: its claim goes.
    pub fn release_claims(&self, tool_use_ids: &[String]) {
        let mut table = self.table();
        for id in tool_use_ids {
            table.claims.remove(id);
        }
    }

    /// Under the owner's inbox lock: a notice still queued is removed, one
    /// already drained is out of reach (and the read still returned the result).
    fn finalize(&self, owner: UnitId, claim: &Claim) {
        let Some(inbox) = self.inbox_of(owner) else {
            return;
        };
        let mut guard = inbox.lock();
        {
            let mut table = self.table();
            for &(run, seen) in &claim.runs {
                let at = table.consumed.entry(run).or_default();
                *at = (*at).max(seen);
            }
        }
        guard.remove_notices(|item| {
            claim.seqs.contains(&item.seq())
                || item.notice.as_ref().is_some_and(|n| {
                    n.kind == NoticeKind::Settled
                        && claim
                            .runs
                            .iter()
                            .any(|&(run, seen)| run == n.run && n.generation <= seen)
                })
        });
    }

    /// Whether the frontend may start a turn for `main` now. True when a waking
    /// item is queued; a notice-only wake spends the budget (design 4.3). When
    /// the budget is gone the waking notices turn quiet and one marker says so.
    pub fn claim_main_wake(&self) -> bool {
        let mut guard = self.main_inbox.lock();
        if !guard.has_wakeable() {
            return false;
        }
        if guard.has_non_notice_wakeable() {
            return true;
        }
        let mut budget = self.wake.lock().unwrap_or_else(|e| e.into_inner());
        if budget.streak < WAKE_BUDGET {
            budget.streak += 1;
            return true;
        }
        guard.demote_notice_wakes();
        if !std::mem::replace(&mut budget.marker_sent, true) {
            let info = NoticeInfo {
                kind: NoticeKind::Overflow,
                run: UnitId::new(),
                handle: String::new(),
                name: String::new(),
                generation: 0,
                count: 1,
                created: SystemTime::now(),
            };
            guard.push_marker(
                info,
                format!(
                    "{WAKE_BUDGET} turns in a row were started by run notices, so further notices will not wake you; they are queued and listed by runs()."
                ),
                self.clock.now(),
            );
        }
        false
    }

    /// The user spoke, or the model read run state itself: it is engaged, so
    /// the wake budget starts over.
    #[cfg(test)]
    pub(crate) fn wake_streak(&self) -> u32 {
        self.wake.lock().unwrap_or_else(|e| e.into_inner()).streak
    }

    pub fn note_engaged(&self) {
        let mut budget = self.wake.lock().unwrap_or_else(|e| e.into_inner());
        budget.streak = 0;
        budget.marker_sent = false;
    }

    pub fn list(&self, caller: UnitId, scope: Scope) -> Vec<UnitView> {
        let table = self.table();
        let ids: Vec<UnitId> = match scope {
            Scope::Children => table.children.get(&caller).cloned().unwrap_or_default(),
            Scope::Descendants => topology::descendants(&table.children, caller),
            Scope::All => table.order.clone(),
        };
        ids.into_iter().map(|id| table.view(id)).collect()
    }

    /// What `who` should know of the tree around it, for `tell_changes`.
    pub fn topology_facts(&self, who: UnitId) -> Option<TopologyFacts> {
        topology::facts(&self.table(), self.main, who, &self.limits)
    }

    pub fn get(&self, id: UnitId) -> Option<UnitView> {
        let table = self.table();
        table.units.contains_key(&id).then(|| table.view(id))
    }

    /// The sender id for text the user types into a run's tab.
    pub fn user(&self) -> UnitId {
        self.user
    }

    /// The inbox `id` drains: a native agent run or `main`.
    pub fn inbox_of(&self, id: UnitId) -> Option<Arc<Inbox>> {
        if id == self.main {
            return Some(Arc::clone(&self.main_inbox));
        }
        self.table()
            .agents
            .get(&id)
            .map(|run| Arc::clone(&run.inbox))
    }

    /// Accepts `main`, `parent`, a handle, a name or the full id.
    pub fn resolve(&self, caller: UnitId, key: &str) -> Result<UnitId, UnitError> {
        let table = self.table();
        match key {
            "main" => Ok(self.main),
            "parent" => table
                .units
                .get(&caller)
                .map(|u| u.parent)
                .ok_or(UnitError::UnknownRun),
            _ => table
                .handles
                .iter()
                .find(|(h, _)| h.as_str() == key)
                .map(|(_, id)| *id)
                .or_else(|| {
                    table
                        .names
                        .iter()
                        .find(|(n, _)| n.as_str() == key)
                        .map(|(_, id)| *id)
                })
                .or_else(|| {
                    let id = UnitId(key.parse().ok()?);
                    table.units.contains_key(&id).then_some(id)
                })
                .ok_or(UnitError::UnknownRun),
        }
    }
}

impl UnitHub {
    /// Queues a message from `from` for `to`. Reachability is the tree: a run
    /// talks to its parent, its direct children and `main`; `main` and `user`
    /// reach every agent. Siblings are reachable only with `[agent.runs]
    /// siblings = true`; the ping-pong guard below covers them like any pair.
    ///
    /// The checks run under the hub lock; the enqueue runs under the
    /// recipient's inbox lock after the hub lock is gone, and wakes an `Idle`
    /// recipient (moving it to `Running`) in the same inbox critical section,
    /// so the driver's settle cannot miss it.
    pub fn send(
        self: &Arc<Self>,
        from: UnitId,
        to: UnitId,
        text: String,
        kind: DeliveryKind,
    ) -> Result<SendAck, UnitError> {
        if from == to {
            return Err(UnitError::SelfSend);
        }
        if text.len() > inbox::MAX_MESSAGE_BYTES {
            return Err(UnitError::TooLarge {
                max: inbox::MAX_MESSAGE_BYTES,
            });
        }
        let id = MessageId(self.message_seq.fetch_add(1, Ordering::Relaxed) + 1);
        let now = self.clock.now();
        let mut table = self.table();
        let label = if from == self.user {
            "user".to_owned()
        } else if from == self.main {
            "main".to_owned()
        } else {
            let unit = table.units.get(&from).ok_or(UnitError::UnknownRun)?;
            if matches!(unit.state, UnitState::Stopping | UnitState::Done(_)) {
                return Err(UnitError::NotReachable);
            }
            unit.name.to_string()
        };
        let item = InboxItem::new(id, from, label, kind, text, now);
        let (inbox, state) = if to == self.main {
            if from == self.user {
                return Err(UnitError::NotReachable);
            }
            (Arc::clone(&self.main_inbox), "next request or turn")
        } else {
            let unit = table.units.get(&to).ok_or(UnitError::UnknownRun)?;
            if unit.kind.kind() != RunKind::Agent {
                return Err(UnitError::NotAgent);
            }
            let reachable = from == self.main
                || from == self.user
                || unit.parent == from
                || table.units.get(&from).is_some_and(|f| f.parent == to)
                || (self.limits.siblings
                    && table
                        .units
                        .get(&from)
                        .is_some_and(|f| f.parent == unit.parent));
            if !reachable {
                return Err(UnitError::NotReachable);
            }
            let state = match unit.state {
                UnitState::Running => "running",
                UnitState::Idle => "idle",
                UnitState::Queued => "queued",
                UnitState::Stopping | UnitState::Done(_) => {
                    return Err(UnitError::RecipientTerminal);
                }
            };
            let run = table.agents.get(&to).ok_or(UnitError::RecipientTerminal)?;
            (Arc::clone(&run.inbox), state)
        };
        let agents = from != self.user
            && from != self.main
            && to != self.main
            && table.units.contains_key(&from);
        let mut released = Vec::new();
        if agents {
            let key = PairKey::of(from, to);
            let pair = table.pairs.entry(key).or_default();
            if pair.count >= inbox::PAIR_BURST {
                if pair.held.len() >= MAX_HELD_PER_PAIR {
                    return Err(UnitError::RateLimited {
                        retry_after: inbox::PAIR_WINDOW.as_secs(),
                    });
                }
                pair.held.push((to, item));
                self.ack_insert(from, id, to, AckState::Held);
                let first = !std::mem::replace(&mut pair.notified, true);
                if first {
                    self.guard_notice(&mut table, from, to);
                }
                self.release(table);
                self.flush_notices();
                return Ok(SendAck {
                    id,
                    state,
                    held: true,
                });
            }
        } else {
            // A message from outside the pair resets it.
            released = self.reset_pairs(&mut table, |key| key.has(to));
        }
        self.release(table);
        self.deliver_all(released);
        // Before the enqueue: a drain right after it must find the record.
        self.ack_insert(from, id, to, AckState::Accepted);
        let delivered = self.deliver(to, &inbox, item, now, true);
        if delivered.is_ok() && agents {
            let mut table = self.table();
            table.pairs.entry(PairKey::of(from, to)).or_default().count += 1;
        }
        if let Err(e) = delivered {
            self.acks().remove(from, id);
            return Err(e);
        }
        Ok(SendAck {
            id,
            state,
            held: false,
        })
    }

    /// The pair `a`, `b` talked past the burst: the parent of the sender (or
    /// `main`) hears it once, quietly.
    fn guard_notice(&self, table: &mut UnitTable, from: UnitId, to: UnitId) {
        let owner = table.units.get(&from).map_or(self.main, |u| u.parent);
        let name = |id: UnitId| {
            table
                .units
                .get(&id)
                .map_or_else(|| "main".to_owned(), |u| u.name.to_string())
        };
        let text = format!(
            "{} and {} exchanged {} messages without doing other work; further messages are held",
            name(from),
            name(to),
            inbox::PAIR_BURST,
        );
        let (handle, run_name) = table.units.get(&from).map_or_else(Default::default, |u| {
            (u.handle.to_string(), u.name.to_string())
        });
        table.pending_notices.push(PendingNotice {
            owner,
            info: NoticeInfo {
                kind: NoticeKind::Guard,
                run: from,
                handle,
                name: run_name,
                generation: 0,
                count: 1,
                created: SystemTime::now(),
            },
            text,
            wake: false,
        });
    }

    /// Resets the counter of every pair `which` selects and takes what the
    /// guard held for them. Delivery is the caller's, after the lock.
    fn reset_pairs(
        &self,
        table: &mut UnitTable,
        which: impl Fn(&PairKey) -> bool,
    ) -> Vec<(UnitId, InboxItem)> {
        let mut released = Vec::new();
        table.pairs.retain(|key, pair| {
            if !which(key) {
                return true;
            }
            released.append(&mut pair.held);
            false
        });
        released
    }

    /// A run did other work than `send`, `runs` or `wait`: whatever the
    /// ping-pong guard held for its pairs is released.
    pub fn note_work(&self, who: UnitId) {
        let released = {
            let mut table = self.table();
            if table.pairs.is_empty() {
                return;
            }
            self.reset_pairs(&mut table, |key| key.has(who))
        };
        self.deliver_all(released);
    }

    fn deliver_all(&self, items: Vec<(UnitId, InboxItem)>) {
        let now = self.clock.now();
        for (to, item) in items {
            let failed = match self.inbox_of(to) {
                Some(inbox) => match self.deliver(to, &inbox, item.clone(), now, false) {
                    Ok(()) => {
                        self.acks().released(item.from, item.id);
                        continue;
                    }
                    Err(UnitError::InboxFull { .. }) => "inbox_full",
                    Err(_) => "recipient_stopped",
                },
                None => "recipient_stopped",
            };
            self.messages_dropped(&[item], failed);
        }
    }

    fn acks(&self) -> MutexGuard<'_, Acks> {
        self.acks.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The user types into tabs and has no `runs`, so nothing is kept for it.
    fn ack_insert(self: &Arc<Self>, from: UnitId, id: MessageId, to: UnitId, state: AckState) {
        if from == self.user {
            return;
        }
        self.acks().insert(from, AckRecord { id, to, state });
        let hub = Arc::downgrade(self);
        let clock = Arc::clone(&self.clock);
        smol::spawn(async move {
            clock.sleep_until(clock.now() + ack::SLOW_DELIVERY).await;
            if let Some(hub) = hub.upgrade() {
                hub.slow_delivery(from, id, to);
            }
        })
        .detach();
    }

    fn slow_delivery(&self, from: UnitId, id: MessageId, to: UnitId) {
        if !self.acks().is_pending(from, id) {
            return;
        }
        let text = format!(
            "message {id} to {} is not delivered after {}s; it is still waiting",
            self.run_label(to),
            ack::SLOW_DELIVERY.as_secs()
        );
        self.ack_notice(from, to, text);
    }

    fn run_label(&self, id: UnitId) -> String {
        if id == self.main {
            return "main".to_owned();
        }
        self.table()
            .units
            .get(&id)
            .map_or_else(|| "?".to_owned(), |u| u.handle.to_string())
    }

    /// A quiet notice to the sender; nothing for the user.
    fn ack_notice(&self, from: UnitId, to: UnitId, text: String) {
        if from == self.user {
            return;
        }
        self.table().pending_notices.push(PendingNotice {
            owner: from,
            info: NoticeInfo {
                kind: NoticeKind::Delivery,
                run: to,
                handle: String::new(),
                name: String::new(),
                generation: 0,
                count: 1,
                created: SystemTime::now(),
            },
            text,
            wake: false,
        });
        self.flush_notices();
    }

    /// The messages are in the recipient's history for a request that went
    /// out. Notices carry no ack. Never call with an inbox lock held.
    pub fn message_delivered(&self, items: &[InboxItem], step: Option<u32>) {
        let mut acks = self.acks();
        for item in items.iter().filter(|i| i.kind != DeliveryKind::Notice) {
            acks.advance(item.from, item.id, AckState::Delivered { step });
        }
    }

    /// The messages will never be delivered: the sender learns it through
    /// `runs` and one quiet notice each. Never call with an inbox lock held.
    pub fn messages_dropped(&self, items: &[InboxItem], reason: &'static str) {
        // One notice per sender and recipient, so a stopped agent with a full
        // inbox costs its senders one line, not one per message.
        let mut groups: Vec<(UnitId, UnitId, Vec<MessageId>)> = Vec::new();
        for item in items.iter().filter(|i| i.kind != DeliveryKind::Notice) {
            let to = self
                .acks()
                .advance(item.from, item.id, AckState::Dropped(reason));
            let Some(to) = to else { continue };
            match groups.iter_mut().find(|g| g.0 == item.from && g.1 == to) {
                Some(g) => g.2.push(item.id),
                None => groups.push((item.from, to, vec![item.id])),
            }
        }
        for (from, to, ids) in groups {
            let list = match ids.as_slice() {
                [one] => format!("message {one}"),
                many => {
                    let shown: Vec<String> =
                        many.iter().take(8).map(MessageId::to_string).collect();
                    let more = many.len().saturating_sub(8);
                    let more = if more > 0 {
                        format!(" and {more} more")
                    } else {
                        String::new()
                    };
                    format!("{} messages ({}{more})", many.len(), shown.join(", "))
                }
            };
            let text = format!(
                "{list} to {} {} dropped: {}",
                self.run_label(to),
                if ids.len() == 1 { "was" } else { "were" },
                reason.replace('_', " ")
            );
            self.ack_notice(from, to, text);
        }
    }

    /// What `from` sent to `to` that is pending or dropped, and the last one
    /// delivered.
    pub fn acks_to(&self, from: UnitId, to: UnitId) -> Vec<AckRecord> {
        self.acks().to(from, to)
    }

    /// The inbox critical section: enqueue, and when that wakes an idle
    /// agent, move it to `Running` before anyone can look.
    fn deliver(
        &self,
        to: UnitId,
        inbox: &Inbox,
        item: InboxItem,
        now: Instant,
        rated: bool,
    ) -> Result<(), UnitError> {
        let mut guard = inbox.lock();
        let accepted = guard.enqueue_with(item, now, rated)?;
        if accepted == Accepted::Wake && to != self.main {
            guard.set_phase(Phase::Running);
            self.mark_running(to);
        }
        drop(guard);
        inbox.signal();
        Ok(())
    }

    fn mark_running(&self, id: UnitId) {
        let mut table = self.table();
        let Some(unit) = table.units.get_mut(&id) else {
            return;
        };
        if !matches!(unit.state, UnitState::Idle) {
            return;
        }
        unit.state = UnitState::Running;
        unit.last_activity = self.clock.now();
        let handle = unit.handle.to_string();
        let event = table.event_of(
            id,
            AgentEvent::RunStateChanged {
                handle,
                state: "running".to_owned(),
            },
        );
        table.outbox.push(event);
        self.release(table);
    }

    /// Best-effort teardown for process exit: no new shell launches, SIGTERM to
    /// every running shell group, then SIGKILL after a short bounded grace.
    /// Idempotent, synchronous, needs no runtime; safe from a signal-handling
    /// thread. Returns once the groups are signalled, not once they are reaped.
    pub fn shutdown(&self) {
        let root = self
            .cancel_root
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(root) = root {
            root.cancel();
        }
        let pids = {
            let table = self.units.lock().unwrap_or_else(|e| e.into_inner());
            table
                .shells
                .values()
                .filter_map(|run| run.mark_killed())
                .collect::<Vec<_>>()
        };
        shell::shutdown_groups(&pids);
    }
}

/// Tears down the shells of every hub in this process. Called on SIGTERM,
/// SIGHUP, SIGINT, a panic on the main thread and normal exit.
pub fn shutdown_all() {
    shell::shutdown_all();
}

/// Makes SIGTERM, SIGHUP and SIGINT run [`shutdown_all`] and then die of that
/// signal as before. The handler only writes a byte to a pipe; a thread does
/// the work.
#[cfg(unix)]
pub fn install_signal_teardown() {
    signals::install();
}

#[cfg(not(unix))]
pub fn install_signal_teardown() {}

impl Drop for UnitHub {
    /// No grace: nobody is left to wait for it.
    fn drop(&mut self) {
        let table = self.units.get_mut().unwrap_or_else(|e| e.into_inner());
        for run in table.shells.values() {
            run.kill_now();
        }
    }
}

/// What a tool needs to act as one run: the shared hub and who is calling.
#[derive(Clone)]
pub struct RunsCtx {
    pub hub: Arc<UnitHub>,
    pub self_run: UnitId,
}

/// The five tools an audience may call: `runs`, `wait` and `send` everywhere,
/// `spawn` and `stop` only where the agent may start work.
pub fn native_run_tools(audience: ToolAudience) -> Vec<Arc<dyn Tool>> {
    crate::tools::runs::all()
        .into_iter()
        .filter(|tool| tool.audience().contains(audience))
        .collect()
}

static RUN_NAMES_RESERVED: AtomicBool = AtomicBool::new(false);

/// True while the run tools are registered, so only then do their names count
/// as built-in (an MCP server may use them when `[agent.runs]` is off).
pub fn run_names_reserved() -> bool {
    RUN_NAMES_RESERVED.load(Ordering::Relaxed)
}

/// Makes the global registry match `[agent.runs] enabled`. Called before the
/// Lua tools load so a plugin cannot shadow a native tool, and again on every
/// reload, so it starts by dropping what an earlier load put there.
pub fn sync_registration(
    registry: &crate::tools::ToolRegistry,
    config: &maki_config::RunsConfig,
) -> Result<(), crate::tools::RegistryError> {
    registry.clear_native();
    RUN_NAMES_RESERVED.store(config.enabled, Ordering::Relaxed);
    if !config.enabled {
        return Ok(());
    }
    registry.register_many(
        crate::tools::runs::all()
            .into_iter()
            .map(|tool| (tool, crate::tools::ToolSource::Native)),
    )
}

/// The hub for one root session and the root agent's handle on it, or `None`
/// when `[agent.runs]` is off.
pub fn start_session(
    config: &maki_config::RunsConfig,
    events: EventSender,
    session: MakiId,
) -> Option<RunsCtx> {
    config.enabled.then(|| {
        let dir = maki_storage::paths::runs_dir(&session.to_string())
            .inspect_err(|e| tracing::warn!(%e, "run logs unavailable, keeping output in memory"))
            .ok();
        let shell = ShellOptions {
            dir,
            ..ShellOptions::default()
        };
        let hub = UnitHub::with_shell_options(
            UnitLimits::from(config),
            events,
            Arc::new(SystemClock),
            shell,
        );
        let self_run = hub.main();
        RunsCtx { hub, self_run }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_case::test_case;

    use super::*;
    use crate::Envelope;

    fn hub_with(limits: UnitLimits) -> Arc<UnitHub> {
        let (tx, _rx) = flume::unbounded::<Envelope>();
        UnitHub::new(
            limits,
            EventSender::new(tx, 0),
            Arc::new(ManualClock::default()),
        )
    }

    fn hub() -> Arc<UnitHub> {
        hub_with(UnitLimits::default())
    }

    pub(crate) fn request(kind: RunKind, description: &str) -> SpawnRequest {
        let mut req: SpawnRequest = serde_json::from_value(serde_json::json!({
            "kind": kind.as_str(),
            "description": description,
        }))
        .unwrap();
        match kind {
            RunKind::Agent => req.prompt = Some("do it".into()),
            RunKind::Shell => req.command = Some("true".into()),
        }
        req
    }

    #[test_case(RunKind::Agent, "ag-" ; "agent")]
    #[test_case(RunKind::Shell, "sh-" ; "shell")]
    fn handle_is_prefixed_and_six_chars(kind: RunKind, prefix: &str) {
        let hub = hub();
        let ack = hub.spawn(hub.main(), request(kind, "work")).unwrap();
        let handle = ack.handle.as_str();
        assert!(handle.starts_with(prefix), "{handle}");
        let tail = &handle[prefix.len()..];
        assert_eq!(tail.len(), 6, "{handle}");
        assert!(
            tail.bytes().all(|b| HANDLE_ALPHABET.contains(&b)),
            "{handle}"
        );
    }

    #[test]
    fn name_slug_suffixes_on_clash() {
        let hub = hub();
        let names: Vec<String> = (0..3)
            .map(|_| {
                hub.spawn(hub.main(), request(RunKind::Shell, "Build the Docs!"))
                    .unwrap()
                    .name
                    .to_string()
            })
            .collect();
        assert_eq!(
            names,
            ["build-the-docs", "build-the-docs-2", "build-the-docs-3"]
        );
    }

    #[test_case("", "run" ; "empty")]
    #[test_case("123 456", "run" ; "digits_only")]
    #[test_case("main", "main-run" ; "reserved")]
    #[test_case("  --Fix   the bug--  ", "fix-the-bug" ; "trimmed")]
    fn slug_shapes(description: &str, expected: &str) {
        assert_eq!(UnitName::slug(description, None), expected);
    }

    #[test]
    fn slug_leaves_room_for_the_suffix() {
        let long = "a".repeat(100);
        let name = UnitName::slug(&long, Some(12));
        assert_eq!(name.len(), NAME_MAX_LEN);
        assert!(name.ends_with("-12"));
        assert!(UnitName::is_valid(&name));
    }

    #[test_case("main" ; "main")]
    #[test_case("parent" ; "parent")]
    fn reserved_names_rejected(name: &str) {
        let hub = hub();
        let mut req = request(RunKind::Shell, "x");
        req.name = Some(name.into());
        assert_eq!(
            hub.spawn(hub.main(), req).unwrap_err(),
            UnitError::BadField {
                field: "name",
                kind: "reserved"
            }
        );
    }

    #[test_case("Upper" ; "uppercase")]
    #[test_case("1abc" ; "leading_digit")]
    #[test_case("a_b" ; "underscore")]
    fn invalid_names_rejected(name: &str) {
        let hub = hub();
        let mut req = request(RunKind::Shell, "x");
        req.name = Some(name.into());
        assert_eq!(
            hub.spawn(hub.main(), req).unwrap_err(),
            UnitError::BadField {
                field: "name",
                kind: "pattern"
            }
        );
    }

    #[test]
    fn explicit_name_clash_is_rejected() {
        let hub = hub();
        let mut req = request(RunKind::Shell, "x");
        req.name = Some("taken".into());
        hub.spawn(hub.main(), req.clone()).unwrap();
        assert_eq!(
            hub.spawn(hub.main(), req).unwrap_err(),
            UnitError::BadField {
                field: "name",
                kind: "taken"
            }
        );
    }

    #[test]
    fn resolve_main_parent_name_handle() {
        let hub = hub();
        let main = hub.main();
        let mut req = request(RunKind::Agent, "auditor");
        req.name = Some("auditor".into());
        let parent = hub.spawn(main, req).unwrap();
        let child = hub
            .spawn(parent.id, request(RunKind::Shell, "build"))
            .unwrap();

        assert_eq!(hub.resolve(child.id, "main"), Ok(main));
        assert_eq!(hub.resolve(child.id, "parent"), Ok(parent.id));
        assert_eq!(hub.resolve(main, "auditor"), Ok(parent.id));
        assert_eq!(hub.resolve(main, child.handle.as_str()), Ok(child.id));
        assert_eq!(hub.resolve(main, &child.id.id().to_string()), Ok(child.id));
        assert_eq!(hub.resolve(main, "parent"), Err(UnitError::UnknownRun));
        assert_eq!(hub.resolve(main, "nope"), Err(UnitError::UnknownRun));
    }

    #[test]
    fn depth_limit_rejects_agent_spawn() {
        let hub = hub_with(UnitLimits {
            max_depth: 1,
            ..UnitLimits::default()
        });
        let main = hub.main();
        let child = hub.spawn(main, request(RunKind::Agent, "one")).unwrap();
        assert_eq!(
            hub.spawn(child.id, request(RunKind::Agent, "two"))
                .unwrap_err(),
            UnitError::DepthLimit
        );
        assert!(hub.spawn(child.id, request(RunKind::Shell, "sh")).is_ok());
    }

    #[test]
    fn spawn_limits_reject_the_overflow() {
        let hub = hub_with(UnitLimits {
            max_children_per_run: 1,
            session_max_runs: 2,
            ..UnitLimits::default()
        });
        let main = hub.main();
        let first = hub.spawn(main, request(RunKind::Agent, "a")).unwrap();
        assert_eq!(
            hub.spawn(main, request(RunKind::Agent, "b")).unwrap_err(),
            UnitError::SpawnLimit
        );
        hub.spawn(first.id, request(RunKind::Shell, "c")).unwrap();
        assert_eq!(
            hub.spawn(first.id, request(RunKind::Shell, "d"))
                .unwrap_err(),
            UnitError::SpawnLimit
        );
    }

    #[test]
    fn unknown_caller_is_rejected() {
        let hub = hub();
        assert_eq!(
            hub.spawn(UnitId::new(), request(RunKind::Shell, "x"))
                .unwrap_err(),
            UnitError::UnknownRun
        );
    }

    #[test]
    fn agents_queue_in_order_and_shells_start() {
        let hub = hub_with(UnitLimits {
            max_agents: 0,
            ..UnitLimits::default()
        });
        let main = hub.main();
        let a = hub.spawn(main, request(RunKind::Agent, "a")).unwrap();
        let b = hub.spawn(main, request(RunKind::Agent, "b")).unwrap();
        let sh = hub.spawn(main, request(RunKind::Shell, "c")).unwrap();
        assert_eq!(a.queue_position, Some((1, 1)));
        assert_eq!(b.queue_position, Some((2, 2)));
        assert_eq!((sh.queue_position, sh.state.label()), (None, "running"));
        let views = hub.list(main, Scope::Children);
        assert_eq!(views.len(), 3);
        assert!(
            views[..2]
                .iter()
                .all(|v| matches!(v.state, UnitState::Queued))
        );
    }

    #[test]
    fn list_scopes_follow_the_tree() {
        let hub = hub();
        let main = hub.main();
        let a = hub.spawn(main, request(RunKind::Agent, "a")).unwrap();
        let b = hub.spawn(a.id, request(RunKind::Agent, "b")).unwrap();
        hub.spawn(b.id, request(RunKind::Shell, "c")).unwrap();
        let other = hub.spawn(main, request(RunKind::Shell, "d")).unwrap();

        let names = |scope, from| -> Vec<String> {
            hub.list(from, scope)
                .iter()
                .map(|v| v.name.to_string())
                .collect()
        };
        assert_eq!(names(Scope::Children, main), ["a", "d"]);
        assert_eq!(names(Scope::Descendants, main), ["a", "b", "c", "d"]);
        assert_eq!(names(Scope::Descendants, a.id), ["b", "c"]);
        assert_eq!(names(Scope::All, other.id), ["a", "b", "c", "d"]);
    }

    #[test]
    fn manual_clock_only_moves_when_told() {
        let clock = ManualClock::default();
        let start = clock.now();
        assert_eq!(clock.now(), start);
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.now() - start, Duration::from_secs(5));
    }
}
