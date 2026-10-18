//! The five model-facing run tools. They are stateless: the hub comes in
//! through `ToolContext::runs`, so one instance serves every session.

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::{Arc, OnceLock};

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
use crate::runs::{RunKind, RunsCtx, Scope, SpawnRequest, UnitView};
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
const NO_RUNS: &str = "no runs";
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
}

#[derive(Deserialize)]
struct IdsArgs {
    ids: Vec<String>,
}

enum Call {
    Spawn(Box<SpawnRequest>),
    Send(SendArgs),
    Runs(RunsArgs),
    Wait(IdsArgs),
    Stop(IdsArgs),
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
            Call::Wait(args) | Call::Stop(args) => args.ids.join(", "),
        };
        HeaderFuture::Ready(HeaderResult::plain(header))
    }

    fn permission_scopes(&self) -> BoxFuture<'_, Option<PermissionScopes>> {
        let scopes = match &self.0 {
            Call::Spawn(req) if req.kind == RunKind::Shell => {
                req.command.clone().map(PermissionScopes::single)
            }
            _ => None,
        };
        Box::pin(std::future::ready(scopes))
    }

    fn execute<'a>(self: Box<Self>, ctx: &'a ToolContext) -> ExecFuture<'a> {
        let result = match (self.0, ctx.runs.as_ref()) {
            (_, None) => Err(NO_RUNTIME.to_owned()),
            (Call::Spawn(mut req), Some(runs)) => {
                req.origin_tool_use_id = ctx.tool_use_id.as_deref().map(Arc::from);
                spawn(runs, *req)
            }
            (Call::Runs(args), Some(runs)) => list_or_show(runs, args),
            (Call::Send(_) | Call::Wait(_) | Call::Stop(_), Some(_)) => {
                Err(NOT_IMPLEMENTED.to_owned())
            }
        };
        Box::pin(std::future::ready(ToolExecResult::from(
            result.map(|text| ToolOutput::Plain(text.into())),
        )))
    }
}

fn spawn(runs: &RunsCtx, req: SpawnRequest) -> Result<String, String> {
    let ack = runs
        .hub
        .spawn(runs.self_run, req)
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

fn list_or_show(runs: &RunsCtx, args: RunsArgs) -> Result<String, String> {
    if let Some(key) = args.id {
        let id = runs
            .hub
            .resolve(runs.self_run, &key)
            .map_err(|e| format!("{e}: {key}"))?;
        return runs
            .hub
            .get(id)
            .map(|view| record(&view))
            .ok_or_else(|| format!("UNKNOWN_RUN: {key}"));
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
        .collect();
    if views.is_empty() {
        return Ok(NO_RUNS.to_owned());
    }
    let caller = runs.hub.get(runs.self_run);
    let base_depth = caller.as_ref().map_or(0, |v| v.depth);
    let me = caller.map_or_else(|| "main".to_owned(), |v| v.name.to_string());
    let mut out = format!("{me} (you)\n");
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
        let mut ctx = stub_ctx(&AgentMode::Build);
        let hub = UnitHub::new(
            UnitLimits::default(),
            ctx.event_tx.clone(),
            Arc::new(ManualClock::default()),
        );
        let self_run = hub.main();
        ctx.runs = Some(RunsCtx { hub, self_run });
        ctx
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
    fn spawn_returns_queued_record() {
        let ctx = ctx();
        let started = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "agent", "description": "Audit auth", "prompt": "look"}),
        )
        .unwrap();
        assert!(
            started.starts_with("started ag-")
                && started.ends_with("\"audit-auth\" (agent, queued, position 1 of 1)"),
            "{started}"
        );
        let shell = call(
            Which::Spawn,
            &ctx,
            json!({"kind": "shell", "description": "build", "command": "make"}),
        )
        .unwrap();
        assert!(
            shell.ends_with("(shell, queued, position 2 of 2)"),
            "{shell}"
        );

        let listing = call(Which::Runs, &ctx, json!({})).unwrap();
        assert!(listing.starts_with("main (you)\n"), "{listing}");
        assert!(listing.contains("audit-auth  agent  queued"), "{listing}");
        assert!(listing.contains("build  shell  queued"), "{listing}");

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

    #[test_case(Which::Send, json!({"to": "main", "message": "hi"}) ; "send")]
    #[test_case(Which::Wait, json!({"ids": ["a"]}) ; "wait")]
    #[test_case(Which::Stop, json!({"ids": ["a"]}) ; "stop")]
    fn later_phase_tools_are_stubs(which: Which, input: Value) {
        assert_eq!(call(which, &ctx(), input).unwrap_err(), NOT_IMPLEMENTED);
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
