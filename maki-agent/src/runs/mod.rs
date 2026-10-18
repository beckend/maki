//! Background runs: subagents and shell jobs behind one id scheme, state
//! machine and hub. Shell runs execute (`shell.rs`); agent runs get a driver
//! task that owns their history (`agent.rs`).

pub mod agent;
#[cfg(test)]
mod agent_run;
pub mod clock;
pub mod limits;
pub mod notice;
pub mod shell;
#[cfg(unix)]
mod signals;
mod topology;

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use event_listener::Event;
use futures_lite::future::or;

use maki_providers::{Message, TokenUsage};
use maki_storage::id::MakiId;
use serde::Deserialize;
use serde_json::Value;

use crate::EventSender;
use crate::cancel::{CancelToken, CancelTrigger};
use crate::tools::ToolAudience;
use crate::tools::registry::Tool;
use crate::{AgentEvent, Envelope, SubagentInfo};

use agent::AgentRun;
pub use agent::{AgentLaunch, resolve_model};
pub use clock::{Clock, ManualClock, SystemClock};
pub use limits::UnitLimits;
pub use notice::{Notice, NoticeKind};
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
    /// Queued for the owner, oldest first; delivery is phase 4b.
    notices: HashMap<UnitId, Vec<Notice>>,
    notice_seq: HashMap<UnitId, u64>,
    /// Creation order, which is the order every listing uses.
    order: Vec<UnitId>,
    handles: HashMap<UnitHandle, UnitId>,
    names: HashMap<UnitName, UnitId>,
    children: HashMap<UnitId, Vec<UnitId>>,
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

    fn drop_notices_of(&mut self, run: UnitId) {
        for queue in self.notices.values_mut() {
            queue.retain(|n| n.run != run);
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

    fn queue_position(&self, id: UnitId) -> Option<(usize, usize)> {
        let queued: Vec<UnitId> = self
            .order
            .iter()
            .copied()
            .filter(|i| matches!(self.units[i].state, UnitState::Queued))
            .collect();
        let at = queued.iter().position(|i| *i == id)?;
        Some((at + 1, queued.len()))
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
}

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
        if live >= self.limits.max_children_per_run as usize
            || table.units.len() >= self.limits.session_max_runs as usize
        {
            return Err(UnitError::SpawnLimit);
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
        let (id, handle) = table.fresh_identity(kind.kind());
        let shell_spec = match &kind {
            UnitKind::Shell(spec) => Some(spec.clone()),
            UnitKind::Agent(_) => None,
        };
        let running = shell_spec.is_some() || launch.is_some();
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
        let queue_position = table.queue_position(id);
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
        if let Some(text) = text {
            let seq = table.notice_seq.entry(owner).or_default();
            *seq += 1;
            let notice = Notice {
                seq: *seq,
                run: id,
                kind: NoticeKind::Settled,
                wake,
                text,
                created: SystemTime::now(),
            };
            table.notices.entry(owner).or_default().push(notice);
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
        unit.last_activity = self.clock.now();
        unit.progress = None;
        let text = (notify && unit.notice_policy != NoticePolicy::Never)
            .then(|| notice::agent_idle(&unit.handle, &unit.name, &result));
        let (owner, wake, handle) = (
            unit.owner,
            unit.notice_policy == NoticePolicy::Wake,
            unit.handle.to_string(),
        );
        table.idle_results.insert(id, result);
        if let Some(text) = text {
            let seq = table.notice_seq.entry(owner).or_default();
            *seq += 1;
            let notice = Notice {
                seq: *seq,
                run: id,
                kind: NoticeKind::Settled,
                wake,
                text,
                created: SystemTime::now(),
            };
            table.notices.entry(owner).or_default().push(notice);
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
        if plan.settled {
            self.settled.notify(usize::MAX);
        }
        for run in &plan.interrupt {
            run.interrupt_turn();
        }
        for (id, run) in plan.cancel {
            run.cancel_run();
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
        loop {
            let listener = self.settled.listen();
            let (views, satisfied) = {
                let mut table = self.table();
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
                if satisfied {
                    for view in views.iter().filter(|v| settled(v)) {
                        table.drop_notices_of(view.id);
                    }
                }
                (views, satisfied)
            };
            if satisfied || cancel.is_cancelled() {
                return views;
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
                return ids
                    .iter()
                    .filter(|id| table.units.contains_key(id))
                    .map(|&id| table.view(id))
                    .collect();
            }
        }
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

    pub fn pending_notices(&self, owner: UnitId) -> Vec<Notice> {
        self.table()
            .notices
            .get(&owner)
            .cloned()
            .unwrap_or_default()
    }

    /// Hands out everything queued for `owner`.
    pub fn take_notices(&self, owner: UnitId) -> Vec<Notice> {
        self.table().notices.remove(&owner).unwrap_or_default()
    }

    /// The caller has the run's result in hand, so its notice is moot.
    pub fn consume_notice_of(&self, run: UnitId) {
        self.table().drop_notices_of(run);
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

    pub fn get(&self, id: UnitId) -> Option<UnitView> {
        let table = self.table();
        table.units.contains_key(&id).then(|| table.view(id))
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
        let hub = hub();
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
