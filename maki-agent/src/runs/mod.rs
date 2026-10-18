//! Background runs: subagents and shell jobs behind one id scheme, state
//! machine and hub. Phase 1 only records and lists them, nothing executes.

pub mod clock;
pub mod limits;
mod topology;

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime};

use maki_providers::TokenUsage;
use maki_storage::id::MakiId;
use serde::Deserialize;
use serde_json::Value;

use crate::EventSender;
use crate::tools::ToolAudience;
use crate::tools::registry::Tool;

pub use clock::{Clock, ManualClock, SystemClock};
pub use limits::UnitLimits;

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
    fn into_kind(self) -> Result<UnitKind, UnitError> {
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
}

#[derive(Default)]
struct UnitTable {
    units: HashMap<UnitId, Unit>,
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
            progress: unit.progress.clone(),
            command: match &unit.kind {
                UnitKind::Shell(s) => Some(s.command.clone()),
                UnitKind::Agent(_) => None,
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
    #[expect(dead_code, reason = "run events are emitted from phase 3 on")]
    events: EventSender,
}

impl UnitHub {
    pub fn new(limits: UnitLimits, events: EventSender, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            main: UnitId::new(),
            units: Mutex::default(),
            limits,
            clock,
            events,
        })
    }

    /// The root agent's identity; it owns the top-level runs.
    pub fn main(&self) -> UnitId {
        self.main
    }

    fn table(&self) -> MutexGuard<'_, UnitTable> {
        self.units.lock().unwrap_or_else(|e| e.into_inner())
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

    pub fn spawn(&self, caller: UnitId, mut req: SpawnRequest) -> Result<SpawnAck, UnitError> {
        let description = req.description.clone();
        let name = req.name.take();
        let notice_policy = req.notify.unwrap_or_default();
        let origin_tool_use_id = req.origin_tool_use_id.take();
        let kind = req.into_kind()?;
        let mut table = self.table();
        let caller_depth = self.depth_of(&table, caller)?;
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
        let (id, handle) = table.fresh_identity(kind.kind());
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
            state: UnitState::Queued,
            created_at: SystemTime::now(),
            started_at: None,
            finished_at: None,
            last_activity: self.clock.now(),
            progress: None,
            usage: TokenUsage::default(),
            cost: 0.0,
            notice_policy,
        };
        let kind = unit.kind.kind();
        table.handles.insert(handle.clone(), id);
        table.names.insert(name.clone(), id);
        table.children.entry(caller).or_default().push(id);
        table.order.push(id);
        table.units.insert(id, unit);
        Ok(SpawnAck {
            id,
            handle,
            name,
            kind,
            state: UnitState::Queued,
            queue_position: table.queue_position(id),
        })
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

/// Makes the global registry match `[agent.runs] enabled`. Called before the
/// Lua tools load so a plugin cannot shadow a native tool, and again on every
/// reload, so it starts by dropping what an earlier load put there.
pub fn sync_registration(
    registry: &crate::tools::ToolRegistry,
    config: &maki_config::RunsConfig,
) -> Result<(), crate::tools::RegistryError> {
    registry.clear_native();
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
pub fn start_session(config: &maki_config::RunsConfig, events: EventSender) -> Option<RunsCtx> {
    config.enabled.then(|| {
        let hub = UnitHub::new(UnitLimits::from(config), events, Arc::new(SystemClock));
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
    fn spawned_runs_are_queued_in_order() {
        let hub = hub();
        let main = hub.main();
        let a = hub.spawn(main, request(RunKind::Agent, "a")).unwrap();
        let b = hub.spawn(main, request(RunKind::Shell, "b")).unwrap();
        assert_eq!(a.queue_position, Some((1, 1)));
        assert_eq!(b.queue_position, Some((2, 2)));
        let views = hub.list(main, Scope::Children);
        assert_eq!(views.len(), 2);
        assert!(views.iter().all(|v| matches!(v.state, UnitState::Queued)));
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
