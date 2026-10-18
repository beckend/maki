//! The five model-facing run tools. They are stateless: the hub comes in
//! through `ToolContext::runs`, so one instance serves every session.

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::schema::{
    JsonPath, ParamSchema, ToolInputError, ToolInputErrorKind, try_from_json, validate,
};
use super::{
    BoxFuture, DescriptionContext, ExecFuture, HeaderFuture, HeaderResult, ParseError,
    PermissionScopes, Tool, ToolAudience, ToolContext, ToolExecResult, ToolInvocation, ToolReach,
    compact_duration,
};
use crate::ToolOutput;
use crate::cancel::CancelToken;
use crate::runs::shell::command_scopes;
use crate::runs::{
    AgentLaunch, OutputSlice, RunKind, RunsCtx, Scope, SpawnRequest, StopOpts, StopStatus,
    UnitError, UnitId, UnitKind, UnitState, UnitView, WaitMode,
};
use maki_config::Permission;

pub const SPAWN_TOOL_NAME: &str = "spawn";
pub const SEND_TOOL_NAME: &str = "send";
pub const RUNS_TOOL_NAME: &str = "runs";
pub const WAIT_TOOL_NAME: &str = "wait";
pub const STOP_TOOL_NAME: &str = "stop";
pub const RUN_TOOL_NAMES: [&str; 5] = [
    SPAWN_TOOL_NAME,
    SEND_TOOL_NAME,
    RUNS_TOOL_NAME,
    WAIT_TOOL_NAME,
    STOP_TOOL_NAME,
];

const NOT_IMPLEMENTED: &str = "NOT_IMPLEMENTED";
const PLAN_BLOCK: &str = "PLAN_MODE: only research agents may be spawned in plan mode";
const NO_RUNS: &str = "no runs";
const DEFAULT_WAIT_SECS: u64 = 60;
const DEFAULT_STOP_WAIT_SECS: u64 = 10;
const DEFAULT_TAIL_LINES: usize = 40;
const NO_RUNTIME: &str = "runs are not available in this session";
const SPAWNERS: ToolAudience = ToolAudience::MAIN.union(ToolAudience::GENERAL_SUB);

const SPAWN_DESCRIPTION: &str =
    "Start background work and return immediately with its id. kind \"agent\" starts a
subagent: a separate agent with its own context that works on a self-contained task and
reports back. kind \"shell\" starts a command that keeps running while you continue. You
are notified when a run finishes; do not poll or sleep. Start independent runs together
in one message. Use `wait` only when you are blocked on a result, `send` to talk to a
run, `stop` to end it, `runs` to look. Not available to agents at the maximum depth: you
get DEPTH_LIMIT and must do the work yourself.

Agent runs do not see this conversation, so the prompt must contain everything they
need. They share your workspace. An agent run stays available after it reports and can
receive follow-up messages unless one_shot is true.";

const SEND_DESCRIPTION: &str =
    "Send a message to another run in this session: your child, your parent (name \"parent\"),
or \"main\". (Siblings only if the session enables it.) The message is queued and returns a
message id at once. A running recipient reads it at its next step; an idle one starts a turn
with it. Sending does not end your turn and is not an answer: the recipient's reply, if any,
comes as a message to you. Use mode \"quiet\" for FYI notes that must not start a turn. Your
final result reaches your parent on its own: use send to a parent only for a question or
mid-run progress.";

const WAIT_DESCRIPTION: &str =
    "Block until runs have finished their work or a timeout expires. Use only when you cannot
proceed without the result; otherwise keep working, you will be notified. mode \"all\" (default)
returns when every listed run is done or idle (finished its turn), \"any\" when the first is.
A timeout is not an error: you get the current states and the runs keep going. Results you
receive here are not repeated as notices.";

const RUNS_DESCRIPTION: &str =
    "List your runs and everything below you, or inspect one. Without id: a tree of runs with id,
name, kind, state, age, last activity, cost and a one-line progress, plus unread quiet
notices. With id: the full record, the result (full=true for untruncated), and for shell runs
output since your last read (tail=N lines, from_offset=B to page; output is read from the log
file). Reading a finished run's result marks it received, so its notice is dropped.";

const STOP_DESCRIPTION: &str =
    "Stop runs. Agents are cancelled at their next safe point and their children are stopped
too (detach=true leaves children running). With soft=true an agent only abandons its current
turn: it stays idle, keeps its transcript and inbox, and can be messaged again. Shells get
SIGTERM, then SIGKILL after 5 seconds. Returns once the runs have settled or after wait_secs.
A stopped agent keeps its transcript and is read with runs; you will not get a notice for
runs you stopped.";

#[derive(Clone, Copy)]
enum Which {
    Spawn,
    Send,
    Runs,
    Wait,
    Stop,
}

const ALL: [Which; 5] = [
    Which::Spawn,
    Which::Send,
    Which::Runs,
    Which::Wait,
    Which::Stop,
];

pub struct RunTool(Which);

pub fn all() -> Vec<Arc<dyn Tool>> {
    ALL.into_iter()
        .map(|w| Arc::new(RunTool(w)) as Arc<dyn Tool>)
        .collect()
}

#[derive(Deserialize)]
struct SendArgs {
    to: String,
}

#[derive(Deserialize)]
struct RunsArgs {
    id: Option<String>,
    scope: Option<String>,
    include_finished: Option<bool>,
    tail: Option<usize>,
    from_offset: Option<u64>,
    full: Option<bool>,
}

#[derive(Deserialize)]
struct WaitArgs {
    ids: Vec<String>,
    mode: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
struct StopArgs {
    ids: Vec<String>,
    soft: Option<bool>,
    detach: Option<bool>,
    wait_secs: Option<u64>,
}

enum Call {
    Spawn(Box<SpawnRequest>),
    Send(SendArgs),
    Runs(RunsArgs),
    Wait(WaitArgs),
    Stop(StopArgs),
}

impl Which {
    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        RUN_TOOL_NAMES[self.index()]
    }

    fn json_schema(self) -> Value {
        match self {
            Self::Spawn => json!({
                "type": "object",
                "required": ["kind", "description"],
                "properties": {
                    "kind": {"type": "string", "enum": ["agent", "shell"]},
                    "description": {"type": "string", "description": "3-8 word label for display and for default naming."},
                    "name": {"type": "string", "pattern": "^[a-z][a-z0-9-]{0,31}$", "description": "Optional unique name others can address this run by. `main` and `parent` are reserved."},
                    "prompt": {"type": "string", "description": "agent only. The complete, self-contained task."},
                    "subagent_type": {"type": "string", "enum": ["research", "general"], "description": "agent only. research is read-only; general can edit. Default general."},
                    "model_tier": {"type": "string", "enum": ["weak", "medium", "strong"], "description": "agent only. Clamped to your tier."},
                    "thinking": {"type": "string"},
                    "output_schema": {"type": "object", "description": "agent only. JSON Schema the final result must satisfy."},
                    "context": {"type": "string", "enum": ["fresh", "fork"], "description": "agent only. fork seeds the run with your completed turns, excluding the turn in flight (default fresh)."},
                    "one_shot": {"type": "boolean", "description": "agent only. Complete after the first result instead of staying available. Default false."},
                    "command": {"type": "string", "description": "shell only. Command line run with the user's shell."},
                    "cwd": {"type": "string"},
                    "timeout_secs": {"type": "integer", "minimum": 1, "description": "Wall clock limit. Default: 1800 for agents, 3600 for shell. Expiry ends the run as timed_out."},
                    "watch": {"type": "string", "description": "shell only. Regex; each matching output line becomes a notice (max 1 per second, then batched)."},
                    "notify": {"type": "string", "enum": ["wake", "quiet", "never"], "description": "On finish: wake starts a turn if you are idle (default), quiet waits for your next turn, never sends nothing (read with runs)."}
                }
            }),
            Self::Send => json!({
                "type": "object",
                "required": ["to", "message"],
                "properties": {
                    "to": {"type": "string", "description": "Run id or name, or \"parent\" / \"main\"."},
                    "message": {"type": "string", "description": "Up to 16 KiB. Be self-contained."},
                    "mode": {"type": "string", "enum": ["steer", "followup", "quiet"], "description": "steer (default): next step of a running recipient, wakes an idle one. followup: after the recipient's current turn ends, as its own turn. quiet: delivered at the recipient's next model request, never wakes."}
                }
            }),
            Self::Runs => json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "scope": {"type": "string", "enum": ["children", "descendants", "all"], "description": "No id only. Default children. all includes siblings and ancestors."},
                    "full": {"type": "boolean"},
                    "tail": {"type": "integer", "minimum": 1, "maximum": 200, "description": "shell: last N lines. Default 40."},
                    "from_offset": {"type": "integer", "minimum": 0, "description": "shell: byte offset in the log; the reply gives next_offset."},
                    "include_finished": {"type": "boolean", "description": "List only. Default true, finished runs retained for the session."}
                }
            }),
            Self::Wait => json!({
                "type": "object",
                "required": ["ids"],
                "properties": {
                    "ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 16},
                    "mode": {"type": "string", "enum": ["all", "any"]},
                    "timeout_secs": {"type": "integer", "minimum": 1, "maximum": 600, "description": "Default 60."}
                }
            }),
            Self::Stop => json!({
                "type": "object",
                "required": ["ids"],
                "properties": {
                    "ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 16},
                    "reason": {"type": "string", "description": "Forwarded to the run as its last message."},
                    "detach": {"type": "boolean"},
                    "soft": {"type": "boolean", "description": "Agents only. Cancel the current turn, stay idle, keep the inbox."},
                    "wait_secs": {"type": "integer", "minimum": 0, "maximum": 30, "description": "Default 10."}
                }
            }),
        }
    }

    fn validator(self) -> &'static ParamSchema {
        static PARSED: [OnceLock<&'static ParamSchema>; 5] = [const { OnceLock::new() }; 5];
        PARSED[self.index()].get_or_init(|| {
            try_from_json(&self.json_schema()).expect("run tool schemas are well-formed")
        })
    }
}

fn typed<T: DeserializeOwned>(value: Value) -> Result<T, ParseError> {
    serde_json::from_value(value).map_err(|e| ToolInputError {
        path: JsonPath::default(),
        kind: ToolInputErrorKind::InternalBug {
            detail: e.to_string(),
        },
    })
}

impl Tool for RunTool {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn description(&self, _: &DescriptionContext) -> Cow<'_, str> {
        Cow::Borrowed(match self.0 {
            Which::Spawn => SPAWN_DESCRIPTION,
            Which::Send => SEND_DESCRIPTION,
            Which::Runs => RUNS_DESCRIPTION,
            Which::Wait => WAIT_DESCRIPTION,
            Which::Stop => STOP_DESCRIPTION,
        })
    }

    fn schema(&self) -> Value {
        self.0.json_schema()
    }

    fn audience(&self) -> ToolAudience {
        match self.0 {
            Which::Spawn | Which::Stop => SPAWNERS,
            _ => ToolAudience::MODEL,
        }
    }

    fn tool_kind(&self) -> Option<&str> {
        matches!(self.0, Which::Spawn).then_some("execute")
    }

    fn reach(&self) -> Option<ToolReach> {
        Some(match self.0 {
            Which::Spawn => ToolReach {
                permission: Permission::Run,
                prompted: true,
            },
            _ => ToolReach {
                permission: Permission::Agent,
                prompted: false,
            },
        })
    }

    fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
        let value = validate(self.0.validator(), input.clone())?;
        let call = match self.0 {
            Which::Spawn => Call::Spawn(Box::new(typed(value)?)),
            Which::Send => Call::Send(typed(value)?),
            Which::Runs => Call::Runs(typed(value)?),
            Which::Wait => Call::Wait(typed(value)?),
            Which::Stop => Call::Stop(typed(value)?),
        };
        Ok(Box::new(RunInvocation(call)))
    }
}

struct RunInvocation(Call);

impl ToolInvocation for RunInvocation {
    fn start_header(&self) -> HeaderFuture {
        let header = match &self.0 {
            Call::Spawn(req) => req.name.clone().unwrap_or_else(|| req.description.clone()),
            Call::Send(args) => args.to.clone(),
            Call::Runs(args) => args.id.clone().unwrap_or_default(),
            Call::Wait(args) => args.ids.join(", "),
            Call::Stop(args) => args.ids.join(", "),
        };
        HeaderFuture::Ready(HeaderResult::plain(header))
    }

    fn permission_scopes(&self) -> BoxFuture<'_, Option<PermissionScopes>> {
        let scopes = match &self.0 {
            Call::Spawn(req) if req.kind == RunKind::Shell => {
                req.command.as_deref().map(|command| {
                    let (scopes, force_prompt) = command_scopes(command);
                    PermissionScopes {
                        scopes,
                        force_prompt,
                    }
                })
            }
            _ => None,
        };
        Box::pin(std::future::ready(scopes))
    }

    fn execute<'a>(self: Box<Self>, ctx: &'a ToolContext) -> ExecFuture<'a> {
        Box::pin(async move {
            let result = match (self.0, ctx.runs.as_ref()) {
                (_, None) => Err(NO_RUNTIME.to_owned()),
                (Call::Spawn(mut req), Some(runs)) => {
                    req.origin_tool_use_id = ctx.tool_use_id.as_deref().map(Arc::from);
                    spawn(runs, ctx, *req).await
                }
                (Call::Runs(args), Some(runs)) => list_or_show(runs, args),
                (Call::Wait(args), Some(runs)) => wait(runs, args, &ctx.cancel).await,
                (Call::Stop(args), Some(runs)) => stop(runs, args, &ctx.cancel).await,
                (Call::Send(_), Some(_)) => Err(NOT_IMPLEMENTED.to_owned()),
            };
            ToolExecResult::from(result.map(|text| ToolOutput::Plain(text.into())))
        })
    }
}

async fn spawn(runs: &RunsCtx, ctx: &ToolContext, req: SpawnRequest) -> Result<String, String> {
    let agent_spec = if req.kind == RunKind::Agent {
        match req.clone().into_kind().map_err(|e| e.to_string())? {
            UnitKind::Agent(spec) => Some(spec),
            UnitKind::Shell(_) => None,
        }
    } else {
        None
    };
    let ack = match agent_spec {
        Some(spec) => {
            if ctx.mode.plan_path().is_some() && spec.subagent_type != "research" {
                return Err(PLAN_BLOCK.to_owned());
            }
            let launch = AgentLaunch::prepare(ctx, &spec).await?;
            runs.hub.spawn_agent(runs.self_run, req, launch)
        }
        None => {
            if ctx.mode.plan_path().is_some() {
                return Err(PLAN_BLOCK.to_owned());
            }
            runs.hub.spawn(runs.self_run, req)
        }
    }
    .map_err(|e| e.to_string())?;
    let mut state = ack.state.label().to_owned();
    if let Some((at, of)) = ack.queue_position {
        let _ = write!(state, ", position {at} of {of}");
    }
    Ok(format!(
        "started {} \"{}\" ({}, {state})",
        ack.handle,
        ack.name,
        ack.kind.as_str()
    ))
}

fn resolve_all(runs: &RunsCtx, keys: &[String]) -> Result<Vec<UnitId>, String> {
    keys.iter()
        .map(|key| {
            runs.hub
                .resolve(runs.self_run, key)
                .map_err(|e| format!("{e}: {key}"))
        })
        .collect()
}

async fn wait(runs: &RunsCtx, args: WaitArgs, cancel: &CancelToken) -> Result<String, String> {
    let ids = resolve_all(runs, &args.ids)?;
    let mode = match args.mode.as_deref() {
        Some("any") => WaitMode::Any,
        _ => WaitMode::All,
    };
    let timeout = Duration::from_secs(args.timeout_secs.unwrap_or(DEFAULT_WAIT_SECS));
    let views = runs
        .hub
        .wait(runs.self_run, &ids, mode, timeout, cancel)
        .await;
    let settled = |v: &UnitView| matches!(v.state, UnitState::Done(_) | UnitState::Idle);
    let satisfied = match mode {
        WaitMode::All => views.iter().all(settled),
        WaitMode::Any => views.iter().any(settled),
    };
    if !satisfied && cancel.is_cancelled() {
        return Err("cancelled; the runs keep running".to_owned());
    }
    let mut out = String::new();
    for view in &views {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(&record(view));
        if view.kind == RunKind::Agent {
            out.push_str(&agent_result(view, false));
        } else if let UnitState::Done(t) = &view.state
            && !t.result.summary.is_empty()
        {
            let _ = write!(out, "\nlast output:\n{}", t.result.summary);
        }
    }
    if !satisfied {
        let pending: Vec<String> = views
            .iter()
            .filter(|v| !settled(v))
            .map(|v| v.handle.to_string())
            .collect();
        let _ = write!(
            out,
            "\n\nStill running: {}. Do not wait again immediately; do other work, you will be notified when they finish.",
            pending.join(", ")
        );
    }
    Ok(out)
}

async fn stop(runs: &RunsCtx, args: StopArgs, cancel: &CancelToken) -> Result<String, String> {
    let ids = resolve_all(runs, &args.ids)?;
    let opts = StopOpts {
        soft: args.soft.unwrap_or(false),
        detach: args.detach.unwrap_or(false),
    };
    let outcomes = runs.hub.stop(runs.self_run, &ids, opts);
    let requested: Vec<UnitId> = outcomes
        .iter()
        .filter(|o| o.status == Ok(StopStatus::Requested))
        .map(|o| o.id)
        .collect();
    let timeout = Duration::from_secs(args.wait_secs.unwrap_or(DEFAULT_STOP_WAIT_SECS));
    let settled = runs
        .hub
        .wait(runs.self_run, &requested, WaitMode::All, timeout, cancel)
        .await;
    let mut lines = Vec::new();
    let mut any_ok = false;
    for (key, outcome) in args.ids.iter().zip(&outcomes) {
        let label = runs
            .hub
            .get(outcome.id)
            .map_or_else(|| key.clone(), |v| format!("{} \"{}\"", v.handle, v.name));
        let verdict = match &outcome.status {
            Err(e) => format!("{e}"),
            Ok(StopStatus::AlreadyFinished(reason)) => {
                any_ok = true;
                format!("already_finished({reason})")
            }
            Ok(StopStatus::Requested | StopStatus::Interrupted) => {
                any_ok = true;
                match settled
                    .iter()
                    .find(|v| v.id == outcome.id)
                    .map(|v| &v.state)
                {
                    Some(UnitState::Done(_)) => "stopped".to_owned(),
                    _ => "still_stopping".to_owned(),
                }
            }
        };
        lines.push(format!("{label}: {verdict}"));
    }
    let text = lines.join("\n");
    if any_ok { Ok(text) } else { Err(text) }
}

fn list_or_show(runs: &RunsCtx, args: RunsArgs) -> Result<String, String> {
    if let Some(key) = args.id {
        let id = runs
            .hub
            .resolve(runs.self_run, &key)
            .map_err(|e| format!("{e}: {key}"))?;
        if !runs.hub.can_reach(runs.self_run, id) {
            return Err(format!("{}: {key}", UnitError::NotReachable));
        }
        let view = runs
            .hub
            .get(id)
            .ok_or_else(|| format!("UNKNOWN_RUN: {key}"))?;
        let mut out = record(&view);
        if view.kind == RunKind::Agent {
            out.push_str(&agent_result(&view, args.full.unwrap_or(false)));
        }
        if view.kind == RunKind::Shell {
            let from = args.from_offset;
            let tail = args.tail.unwrap_or(DEFAULT_TAIL_LINES);
            let slice = runs
                .hub
                .read_output(runs.self_run, id, from, tail, from.is_none());
            let _ = write!(out, "\n{}", render_output(&slice));
        }
        if view.state.is_done() {
            runs.hub.consume_notice_of(id);
        }
        return Ok(out);
    }
    let scope = match args.scope.as_deref() {
        Some("descendants") => Scope::Descendants,
        Some("all") => Scope::All,
        _ => Scope::Children,
    };
    let include_finished = args.include_finished.unwrap_or(true);
    let views: Vec<UnitView> = runs
        .hub
        .list(runs.self_run, scope)
        .into_iter()
        .filter(|v| include_finished || !v.state.is_done())
        .filter(|v| scope != Scope::All || runs.hub.can_reach(runs.self_run, v.id))
        .collect();
    let notices = runs.hub.take_notices(runs.self_run);
    if views.is_empty() && notices.is_empty() {
        return Ok(NO_RUNS.to_owned());
    }
    let mut out = String::new();
    for notice in &notices {
        let _ = writeln!(out, "{}\n", notice.text);
    }
    if views.is_empty() {
        out.truncate(out.trim_end().len());
        return Ok(out);
    }
    let caller = runs.hub.get(runs.self_run);
    let base_depth = caller.as_ref().map_or(0, |v| v.depth);
    let me = caller.map_or_else(|| "main".to_owned(), |v| v.name.to_string());
    let _ = writeln!(out, "{me} (you)");
    for v in &views {
        let indent = "  ".repeat(usize::from(v.depth.saturating_sub(base_depth)));
        let detail = v
            .progress
            .as_deref()
            .or(v.command.as_deref())
            .unwrap_or(&v.description);
        let _ = writeln!(
            out,
            "{indent}{} {}  {}  {}  {}  ${:.2}  {detail}",
            v.handle,
            v.name,
            v.kind.as_str(),
            v.state.label(),
            compact_duration(v.age_secs),
            v.cost,
        );
    }
    out.truncate(out.trim_end().len());
    Ok(out)
}

fn render_output(slice: &OutputSlice) -> String {
    let mut out = format!(
        "output (offsets {}..{}, next_offset={}):",
        slice.start_offset, slice.next_offset, slice.next_offset
    );
    if slice.skipped_bytes > 0 {
        let _ = write!(
            out,
            "\n[{} earlier bytes not shown; from_offset=0 pages the log]",
            slice.skipped_bytes
        );
    }
    if slice.text.is_empty() {
        out.push_str("\n(no new output)");
    } else {
        let _ = write!(out, "\n{}", slice.text.trim_end());
    }
    if let Some(log) = &slice.log {
        let _ = write!(out, "\nlog: {}", log.display());
    }
    out
}

/// The result of an agent run that has produced one: bounded text, the
/// structured value, and with `full` the spilled body.
fn agent_result(v: &UnitView, full: bool) -> String {
    let Some(result) = &v.result else {
        return String::new();
    };
    let mut out = String::new();
    let mut text = result.summary.clone();
    if full
        && let Some(body) = &result.body_ref
        && let Ok(whole) = std::fs::read_to_string(&body.0)
    {
        text = whole;
    }
    if !text.is_empty() {
        let _ = write!(out, "\nresult:\n{text}");
    }
    if let Some(structured) = &result.structured
        && text != structured.to_string()
    {
        let _ = write!(out, "\nstructured: {structured}");
    }
    if result.truncated
        && !full
        && let Some(body) = &result.body_ref
    {
        let _ = write!(out, "\nfull text: {}", body.0.display());
    }
    out
}

fn record(v: &UnitView) -> String {
    let mut out = format!(
        "{} \"{}\" ({}, {})\ndescription: {}\nage: {}",
        v.handle,
        v.name,
        v.kind.as_str(),
        v.state.label(),
        v.description,
        compact_duration(v.age_secs),
    );
    if let Some(command) = &v.command {
        let _ = write!(out, "\ncommand: {command}");
    }
    if let UnitState::Done(t) = &v.state {
        if let Some(code) = t.exit_code {
            let _ = write!(out, "\nexit_code: {code}");
        }
        if let Some(detail) = &t.detail {
            let _ = write!(out, "\ndetail: {detail}");
        }
        let _ = write!(
            out,
            "\nelapsed: {}",
            compact_duration(t.result.elapsed_ms / 1000)
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use serde_json::Map;
    use test_case::test_case;

    use super::*;
    use crate::AgentMode;
    use crate::runs::{ManualClock, UnitHub, UnitLimits};
    use crate::tools::test_support::stub_ctx;

    fn tool(which: Which) -> RunTool {
        RunTool(which)
    }

    fn ctx() -> ToolContext {
        ctx_with_clock().0
    }

    fn ctx_with_clock() -> (ToolContext, Arc<ManualClock>) {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.provider = Arc::new(crate::test_support::StubStreamProvider::default());
        let clock = Arc::new(ManualClock::default());
        let hub = UnitHub::new(UnitLimits::default(), ctx.event_tx.clone(), clock.clone());
        let self_run = hub.main();
        ctx.runs = Some(RunsCtx { hub, self_run });
        (ctx, clock)
    }

    fn call(which: Which, ctx: &ToolContext, input: Value) -> Result<String, String> {
        let invocation = tool(which).parse(&input).map_err(|e| e.to_string())?;
        match smol::block_on(invocation.execute(ctx)).output? {
            ToolOutput::Plain(t) => Ok(t.text),
            other => panic!("unexpected output {other:?}"),
        }
    }

    #[test]
    fn schema_matches_snapshot() {
        let mut actual = Map::new();
        for which in ALL {
            actual.insert(which.name().to_owned(), which.json_schema());
        }
        let expected: Value = serde_json::from_str(include_str!("runs_schema.json")).unwrap();
        assert_eq!(Value::Object(actual), expected);
    }

    #[test_case(json!({"kind": "shell", "description": "d", "command": "ls", "prompt": "p"}), "\"prompt\" is for kind \"agent\"; a shell takes \"command\"" ; "shell_with_prompt")]
    #[test_case(json!({"kind": "shell", "description": "d", "command": "ls", "one_shot": true}), "\"one_shot\" is for kind \"agent\"" ; "shell_with_one_shot")]
    #[test_case(json!({"kind": "agent", "description": "d", "prompt": "p", "command": "ls"}), "\"command\" is for kind \"shell\"; an agent takes \"prompt\"" ; "agent_with_command")]
    #[test_case(json!({"kind": "agent", "description": "d", "prompt": "p", "watch": "x"}), "\"watch\" is for kind \"shell\"" ; "agent_with_watch")]
    #[test_case(json!({"kind": "agent", "description": "d", "prompt": "p", "name": "main"}), "BAD_FIELD: reserved name" ; "reserved_name")]
    #[test_case(json!({"kind": "agent", "description": "d"}), "\"prompt\" is required" ; "agent_without_prompt")]
    fn cross_kind_fields_rejected(input: Value, expected: &str) {
        let err = call(Which::Spawn, &ctx(), input).unwrap_err();
        assert!(err.starts_with("BAD_FIELD"), "{err}");
        assert!(err.contains(expected), "{err}");
    }

    #[test]
    fn spawn_starts_an_agent_record() {
        let ctx = ctx();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "agent", "description": "Audit auth", "prompt": "look"}),
        )
        .unwrap();
        assert!(
            started.starts_with("started ag-")
                && started.ends_with("\"audit-auth\" (agent, running)"),
            "{started}"
        );
        let shell = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "build", "command": "sleep 600"}),
        )
        .unwrap();
        assert!(shell.ends_with("(shell, running)"), "{shell}");

        let listing = call(Which::Runs, &ctx, json!({})).unwrap();
        assert!(listing.starts_with("main (you)\n"), "{listing}");
        assert!(listing.contains("audit-auth  agent  running"), "{listing}");
        assert!(listing.contains("build  shell  running"), "{listing}");

        let record = call(Which::Runs, &ctx, json!({"id": "audit-auth"})).unwrap();
        assert!(record.contains("description: Audit auth"), "{record}");
    }

    #[test]
    fn runs_without_any_says_so() {
        assert_eq!(call(Which::Runs, &ctx(), json!({})).unwrap(), NO_RUNS);
    }

    #[test]
    fn unknown_run_id_is_reported() {
        let err = call(Which::Runs, &ctx(), json!({"id": "ag-nope00"})).unwrap_err();
        assert!(err.starts_with("UNKNOWN_RUN"), "{err}");
    }

    #[test]
    fn send_is_still_a_stub() {
        let input = json!({"to": "main", "message": "hi"});
        assert_eq!(
            call(Which::Send, &ctx(), input).unwrap_err(),
            NOT_IMPLEMENTED
        );
    }

    fn handle_of(started: &str) -> &str {
        started.split_whitespace().nth(1).unwrap()
    }

    #[test]
    fn shell_lifecycle_through_the_tools() {
        let ctx = ctx();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "count", "command": "echo one; echo two"}),
        )
        .unwrap();
        let id = handle_of(&started);
        let done = call(Which::Wait, &ctx, json!({"ids": [id]})).unwrap();
        assert!(
            done.contains("(shell, completed)") && done.contains("exit_code: 0"),
            "{done}"
        );
        assert!(done.contains("last output:\none\ntwo"), "{done}");
        assert!(!done.contains("Still running"), "{done}");

        let first = call(Which::Runs, &ctx, json!({"id": id})).unwrap();
        assert!(first.contains("one\ntwo"), "{first}");
        let second = call(Which::Runs, &ctx, json!({"id": id})).unwrap();
        assert!(
            second.contains("(no new output)") && !second.contains("one\ntwo"),
            "{second}"
        );
        assert_eq!(
            call(Which::Runs, &ctx, json!({}))
                .unwrap()
                .matches("Run sh-")
                .count(),
            0
        );
    }

    #[test]
    fn wait_consumes_the_settled_notice() {
        let ctx = ctx();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "quick", "command": "true"}),
        )
        .unwrap();
        let hub = &ctx.runs.as_ref().unwrap().hub;
        let id = hub.resolve(hub.main(), handle_of(&started)).unwrap();
        smol::block_on(hub.wait(
            id_owner(&ctx),
            &[id],
            WaitMode::All,
            Duration::from_secs(10_000_000),
            &CancelToken::none(),
        ));
        assert!(
            hub.pending_notices(hub.main()).is_empty(),
            "wait consumed it"
        );
    }

    fn id_owner(ctx: &ToolContext) -> UnitId {
        ctx.runs.as_ref().unwrap().self_run
    }

    #[test]
    fn wait_timeout_tells_the_model_to_do_other_work() {
        let (ctx, clock) = ctx_with_clock();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "long", "command": "sleep 600"}),
        )
        .unwrap();
        let id = handle_of(&started).to_owned();
        let invocation = tool(Which::Wait)
            .parse(&json!({"ids": [id], "timeout_secs": 2}))
            .unwrap();
        let out = smol::block_on(async {
            let mut fut = std::pin::pin!(invocation.execute(&ctx));
            loop {
                if let Some(v) = futures_lite::future::poll_once(&mut fut).await {
                    break v;
                }
                clock.advance(Duration::from_secs(1));
                futures_lite::future::yield_now().await;
            }
        });
        let ToolOutput::Plain(t) = out.output.unwrap() else {
            panic!("plain output expected")
        };
        assert!(t.text.contains("(shell, running)"), "{}", t.text);
        assert!(
            t.text.ends_with(&format!(
                "Still running: {id}. Do not wait again immediately; do other work, you will be notified when they finish."
            )),
            "{}",
            t.text
        );
    }

    #[test]
    fn stop_reports_truthfully_and_twice_is_fine() {
        let ctx = ctx();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "long", "command": "sleep 600"}),
        )
        .unwrap();
        let id = handle_of(&started);
        let first = call(Which::Stop, &ctx, json!({"ids": [id]})).unwrap();
        assert!(first.ends_with(": stopped"), "{first}");
        let again = call(Which::Stop, &ctx, json!({"ids": [id]})).unwrap();
        assert!(again.ends_with(": already_finished(killed)"), "{again}");
        let unknown = call(Which::Stop, &ctx, json!({"ids": ["sh-nope00"]})).unwrap_err();
        assert!(unknown.starts_with("UNKNOWN_RUN"), "{unknown}");
    }

    #[test]
    fn tools_refuse_without_a_hub() {
        let mut ctx = ctx();
        ctx.runs = None;
        assert_eq!(call(Which::Runs, &ctx, json!({})).unwrap_err(), NO_RUNTIME);
    }

    #[test]
    fn shell_spawn_prompts_on_its_command() {
        let invocation = tool(Which::Spawn)
            .parse(&json!({"kind": "shell", "description": "d", "command": "make -j8"}))
            .unwrap();
        let scopes = smol::block_on(invocation.permission_scopes()).unwrap();
        assert_eq!(scopes.scopes, ["make -j8"]);
    }
}
