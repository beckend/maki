//! Tests the task plugin's delegation policy end-to-end: real plugin
//! source, real `maki.json` / `maki.async`, with model I/O replaced by
//! scriptable Lua stubs.

mod common;

use std::sync::Arc;
use std::thread;

use common::{
    Chat, WAIT, call_ctx, done_text, exec, next_event, next_task_done, request_move, wait_live_buf,
    wait_movable,
};
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{ToolContext, ToolRegistry};
use maki_agent::{
    AgentEvent, AgentMode, BufferSnapshot, CancelToken, Envelope, SharedBuf, ToolDoneEvent,
    ToolOutput,
};
use maki_lua::test_support::runtime;
use maki_lua::{MoveOutcome, PluginHost};
use serde_json::{Value, json};

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");
const TASK_RUNNER_SRC: &str = include_str!("../../plugins/task/runner.lua");

// Mirrors of the plugin's error contracts and policy numbers.
const STRUCTURED_OUTPUT_TOOL: &str = "structured_output";
const MAX_STRUCTURED_RETRIES: usize = 2;
const MAX_SCHEMA_ERRORS: usize = 3;
const SCHEMA_COMPILE_ERROR: &str = "invalid output_schema";
const SCHEMA_ROOT_ERROR: &str = "output_schema must have type object";
const STRUCTURED_MISSING_ERROR: &str = "subagent finished without calling structured_output";
const STRUCTURED_INVALID_ERROR: &str = "subagent result does not match output_schema";
const SUMMARY_MISSING_ERROR: &str = "subagent finished without providing a summary";
const UNKNOWN_SUBAGENT_ERR: &str = "unknown subagent type: bogus";
const SUB_AGENT_ERROR_PREFIX: &str = "sub-agent error: ";

const TASK_TOOL: &str = "task";
const PROBE_TOOL: &str = "probe";
const WAIT_TOOL: &str = "wait";
const RELEASE_TOOL: &str = "release";
const TASK_PROMPT: &str = "do the thing";
const PLAIN_TEXT: &str = "plain text result";
const RECOVERED_TEXT: &str = "summary after nudge";
/// Mirrors the task plugin's `NUDGE_SUMMARY` wording.
const SUMMARY_NUDGE_FRAGMENT: &str = "concise summary";
const PROMPT_ERR_MSG: &str = "model exploded";
const RAISE_MSG: &str = "stub prompt kaboom";
const PARTIAL_TEXT: &str = "half a transcript";
const CANCELLED_ERR: &str = "cancelled";
const SESSION_OPEN_ERR: &str = "session admission failed";
/// Mirrors the task plugin's `max_concurrent` default.
const TASK_DEFAULT_MAX_CONCURRENT: u64 = 8;

const FIRST_CALL_ID: &str = "toolu_first";
const SECOND_CALL_ID: &str = "toolu_second";
const MOVED_RECEIPT: &str = "Subagent parked";
const WAITING_FOR_SLOT: &str = "waiting for a free subagent slot";
/// Mirrors the task plugin's `FOREGROUND_NOTE` wording.
const FOREGROUND_NOTE_HEAD: &str = "Could not start in the background (";
const FOREGROUND_NOTE_TAIL: &str = "Ran in the foreground.";

const SCENARIO_PLAIN: &str = "plain";
const SCENARIO_HAPPY: &str = "happy";
const SCENARIO_INVALID_THEN_VALID: &str = "invalid_then_valid";
const SCENARIO_NEVER_STRUCTURED: &str = "never_structured";
const SCENARIO_INVALID_ONLY: &str = "invalid_only";
const SCENARIO_PROMPT_ERROR: &str = "prompt_error";
const SCENARIO_PARTIAL_ERROR: &str = "partial_error";
const SCENARIO_RAISE: &str = "raise";
const SCENARIO_NO_SUMMARY: &str = "no_summary";
const SCENARIO_NO_SUMMARY_THEN_RECOVER: &str = "no_summary_then_recover";
const SCENARIO_PARKED: &str = "parked";
const SCENARIO_PARKED_SECOND: &str = "parked_second";
const SCENARIO_SESSION_ERROR: &str = "session_error";
const SCENARIO_OPEN_PARKED: &str = "open_parked";

/// The stub tool the background path waits on. The native run tools reserve
/// the name, so the suite that loads both leaves it out.
const WAIT_STUB: &str = r#"
maki.api.register_tool({
  name = "wait",
  description = "wait until a step is parked and enough acquires and notices happened",
  schema = {
    type = "object",
    properties = {
      parked = { type = "string" },
      acquires = { type = "integer" },
      notices = { type = "integer" },
    },
  },
  audiences = { "main" },
  handler = function(input)
    local function ready()
      return (not input.parked or parked[input.parked] ~= nil)
        and recorder.acquire_attempts >= (input.acquires or 0)
        and #recorder.notices >= (input.notices or 0)
    end
    while not ready() do
      maki.async.await(1, function(done)
        waiters[#waiters + 1] = done
      end)
    end
    return "ready"
  end,
})
"#;

/// Stubs keyed by `opts.name` (the task's `description`). `maki.json` and
/// `maki.async` stay real so schema validation and semaphore behavior are tested.
const STUB_PRELUDE: &str = r#"
recorder = {
  prompts = {}, closed = 0, sessions = 0, acquired = 0, released = 0,
  notices = {}, acquire_attempts = 0,
}
local sessions = {}
local parked = {}
local waiters = {}

-- Every `wait` call rechecks its condition after any recorded step.
local function changed()
  local woken = waiters
  waiters = {}
  for _, done in ipairs(woken) do
    done()
  end
end

-- Parks the caller until the `release` tool resumes it by name.
local function park(name)
  return maki.async.await(1, function(done)
    parked[name] = done
    changed()
  end)
end

-- Spy wrapper: the real semaphore does the work, counters track that every
-- permit is explicitly released (gc would silently hide a leak).
local real_semaphore = maki.async.semaphore
maki.async.semaphore = function(n)
  recorder.sem_size = n
  local sem = real_semaphore(n)
  local function spied(permit)
    recorder.acquired = recorder.acquired + 1
    return {
      release = function(p)
        recorder.released = recorder.released + 1
        return permit:release()
      end,
    }
  end
  return {
    try_acquire = function(self)
      local permit = sem:try_acquire()
      if permit then
        recorder.acquire_attempts = recorder.acquire_attempts + 1
        changed()
        return spied(permit)
      end
    end,
    acquire = function(self)
      recorder.acquire_attempts = recorder.acquire_attempts + 1
      changed()
      return spied(sem:acquire())
    end,
  }
end

maki.agent.resolve_model = function(ctx, opts)
  recorder.resolve_opts = opts
  return { spec = "test/model" }
end

maki.agent.system_prompt = function(ctx, opts)
  return "sys"
end

maki.agent.tools = function(ctx, opts)
  return {}
end

local behaviors = {}

behaviors.plain = function(sess, msg)
  return { text = "@PLAIN_TEXT@" }
end
behaviors.open_parked = behaviors.plain

behaviors.happy = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ answer = "42" })
  return { text = "raw text ignored" }
end

behaviors.invalid_then_valid = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ answer = 42 })
  recorder.second_ack, recorder.second_err = h({ answer = "42" })
  return { text = "raw text ignored" }
end

behaviors.never_structured = function(sess, msg)
  return { text = "no structured call" }
end

behaviors.invalid_only = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ a = 1, b = 2, c = 3, d = 4 })
  return { text = "still invalid" }
end

behaviors.prompt_error = function(sess, msg)
  return nil, "@PROMPT_ERR@"
end

behaviors.partial_error = function(sess, msg)
  return { text = "@PARTIAL_TEXT@" }, "@CANCELLED_ERR@"
end

behaviors.no_summary = function(sess, msg)
  return { text = "" }
end

behaviors.no_summary_then_recover = function(sess, msg)
  if #recorder.prompts == 1 then
    return { text = "" }
  end
  return { text = "@RECOVERED_TEXT@" }
end

behaviors.raise = function(sess, msg)
  error("@RAISE_MSG@")
end

behaviors.parked = function(sess, msg)
  return park(sess.opts.name)
end
behaviors.parked_second = behaviors.parked

maki.agent.session = function(ctx, opts)
  if opts == nil then
    opts = ctx
    recorder.scope = opts.scope
    recorder.owner = opts.session
  end
  recorder.sessions = recorder.sessions + 1
  if opts.name == "@SESSION_ERROR_SCENARIO@" then
    return nil, "@SESSION_OPEN_ERR@"
  end
  recorder.has_local_tools = opts.local_tools ~= nil
  recorder.thinking = opts.thinking
  local sess = { opts = opts }
  sessions[opts.name] = sess
  function sess:id() return opts.name end
  function sess:prompt(msg)
    recorder.prompts[#recorder.prompts + 1] = msg
    return behaviors[opts.name](self, msg)
  end
  function sess:close()
    if self.closed then return end
    self.closed = true
    recorder.closed = recorder.closed + 1
  end
  if opts.name == "@OPEN_PARKED_SCENARIO@" then
    park(opts.name)
  end
  return sess
end

maki.agent.list = function(opts)
  local out = {}
  for name, session in pairs(sessions) do
    if session.opts.session == opts.session and not session.closed then
      out[#out + 1] = { id = name }
    end
  end
  return out
end

maki.session.notify = function(text, opts)
  recorder.notices[#recorder.notices + 1] = {
    text = text, session = opts.session, wake = opts.wake,
  }
  changed()
  return true
end

@WAIT_STUB@

maki.api.register_tool({
  name = "release",
  description = "resume a parked prompt or session open",
  schema = { type = "object", properties = { name = { type = "string" } } },
  audiences = { "main" },
  handler = function(input)
    local done = assert(parked[input.name], "nothing parked")
    parked[input.name] = nil
    done({ text = "@PLAIN_TEXT@" })
    return "released"
  end,
})

maki.api.register_tool({
  name = "probe",
  description = "recorder snapshot",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(input, ctx)
    local snap = {
      sessions = recorder.sessions,
      closed = recorder.closed,
      prompt_count = #recorder.prompts,
      has_local_tools = recorder.has_local_tools,
      thinking = recorder.thinking,
      first_ack = recorder.first_ack,
      first_err = recorder.first_err,
      second_ack = recorder.second_ack,
      second_err = recorder.second_err,
      acquired = recorder.acquired,
      released = recorder.released,
      sem_size = recorder.sem_size,
      notices = recorder.notices,
      notice_count = #recorder.notices,
      scope = recorder.scope,
      owner = recorder.owner,
    }
    if recorder.resolve_opts then
      snap.resolve_opts = recorder.resolve_opts
    end
    if #recorder.prompts > 0 then
      snap.prompts = recorder.prompts
    end
    return (maki.json.encode(snap))
  end,
})
"#;

fn load_task_host() -> (Arc<ToolRegistry>, PluginHost) {
    load_task_host_with_opts(serde_json::Map::new())
}

fn load_background_host() -> (Arc<ToolRegistry>, PluginHost) {
    let mut opts = serde_json::Map::new();
    opts.insert("allow_background".into(), json!(true));
    load_task_host_with_opts(opts)
}

fn load_task_host_with_opts(
    opts: serde_json::Map<String, serde_json::Value>,
) -> (Arc<ToolRegistry>, PluginHost) {
    load_task_host_in(Arc::new(ToolRegistry::new()), opts)
}

/// The same host, over a registry the caller filled: the native run tools are
/// registered before the task plugin loads, so both are there.
fn load_task_host_in(
    reg: Arc<ToolRegistry>,
    opts: serde_json::Map<String, serde_json::Value>,
) -> (Arc<ToolRegistry>, PluginHost) {
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let prelude = STUB_PRELUDE
        .replace("@WAIT_STUB@", "")
        .replace("@PLAIN_TEXT@", PLAIN_TEXT)
        .replace("@RECOVERED_TEXT@", RECOVERED_TEXT)
        .replace("@PROMPT_ERR@", PROMPT_ERR_MSG)
        .replace("@RAISE_MSG@", RAISE_MSG)
        .replace("@PARTIAL_TEXT@", PARTIAL_TEXT)
        .replace("@CANCELLED_ERR@", CANCELLED_ERR)
        .replace("@SESSION_ERROR_SCENARIO@", SCENARIO_SESSION_ERROR)
        .replace("@OPEN_PARKED_SCENARIO@", SCENARIO_OPEN_PARKED)
        .replace("@SESSION_OPEN_ERR@", SESSION_OPEN_ERR);
    let source = format!(
        r#"{prelude}
local builtin_require = require
local runner
local function require(name)
  if name == "runner" then
    if runner == nil then
      runner = (function()
{TASK_RUNNER_SRC}
      end)()
    end
    return runner
  end
  return builtin_require(name)
end
{TASK_PLUGIN_SRC}
"#
    );
    host.load_source_with_opts("task_policy", &source, opts)
        .unwrap();
    (reg, host)
}

fn exec_tool(reg: &ToolRegistry, name: &str, input: Value) -> Result<String, String> {
    exec_tool_with_ctx(reg, name, input, &stub_ctx(&AgentMode::Build))
}

fn exec_tool_with_ctx(
    reg: &ToolRegistry,
    name: &str,
    input: Value,
    ctx: &ToolContext,
) -> Result<String, String> {
    let entry = reg
        .get(name)
        .unwrap_or_else(|| panic!("tool {name} not registered"));
    let inv = entry.tool.parse(&input).expect("parse failed");
    runtime()
        .block_on(inv.execute(ctx))
        .output
        .map(|out| match out {
            ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
            other => panic!("unexpected output: {other:?}"),
        })
}

fn background_input(scenario: &str, output_schema: Option<Value>) -> Value {
    let mut input = task_input(scenario, output_schema);
    input["background"] = json!(true);
    input
}

/// Blocks until every condition holds: `parked` (a scenario name),
/// `acquires` and `notices` (minimum counts).
fn wait(reg: &ToolRegistry, conditions: Value) {
    exec_tool(reg, WAIT_TOOL, conditions).expect("wait failed");
}

fn release(reg: &ToolRegistry, scenario: &str) {
    exec_tool(reg, RELEASE_TOOL, json!({ "name": scenario })).expect("release failed");
}

fn probe(reg: &ToolRegistry) -> Value {
    let out = exec_tool(reg, PROBE_TOOL, json!({})).expect("probe failed");
    serde_json::from_str(&out).expect("probe returned invalid json")
}

fn task_input(scenario: &str, output_schema: Option<Value>) -> Value {
    let mut input = json!({ "description": scenario, "prompt": TASK_PROMPT });
    if let Some(schema) = output_schema {
        input["output_schema"] = schema;
    }
    input
}

const FULL_MODEL_SPEC: &str = "aperture/ollama/glm-5.2";

/// Advertised to the model, so the wording lives in the docs, not here: what
/// matters is that the property exists and that whatever the model writes
/// reaches the session verbatim, capping being the session's job.
#[test_case::test_case(Some(json!("high")) ; "effort")]
#[test_case::test_case(Some(json!(4096)) ; "token_budget")]
#[test_case::test_case(None ; "inherit_parent_when_omitted")]
fn thinking_forwards_to_session(thinking: Option<Value>) {
    let (reg, _host) = load_task_host();
    let entry = reg.get(TASK_TOOL).expect("task tool missing");
    assert!(entry.tool.schema()["properties"]["thinking"].is_object());
    let mut input = task_input(SCENARIO_PLAIN, None);
    if let Some(thinking) = &thinking {
        input["thinking"] = thinking.clone();
    }

    exec_tool(&reg, TASK_TOOL, input).expect("task failed");
    let snap = probe(&reg);
    assert_eq!(snap.get("thinking"), thinking.as_ref());
}

#[tokio::test]
async fn model_spec_forwards_full_spec_to_resolve_model() {
    let mut opts = serde_json::Map::new();
    opts.insert("allow_model".into(), json!(true));
    let (reg, _host) = load_task_host_with_opts(opts);
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["model"] = json!(FULL_MODEL_SPEC);
    let out = exec_tool(&reg, TASK_TOOL, input).expect("task with model spec failed");
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    let opts = snap["resolve_opts"]
        .as_object()
        .expect("resolve_opts missing");
    assert_eq!(opts["spec"], json!(FULL_MODEL_SPEC));
    assert!(
        opts.get("tier").is_none_or(Value::is_null),
        "tier should be unset when only model spec is given"
    );
}

#[tokio::test]
async fn model_spec_ignored_when_allow_model_off() {
    let (reg, _host) = load_task_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["model"] = json!(FULL_MODEL_SPEC);
    let out = exec_tool(&reg, TASK_TOOL, input).expect("task with model spec failed");
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    let opts = snap["resolve_opts"]
        .as_object()
        .expect("resolve_opts missing");
    assert!(
        opts.get("spec").is_none_or(Value::is_null),
        "spec should not be forwarded when allow_model is off"
    );
}

fn answer_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "answer": { "type": "string" } },
        "required": ["answer"],
        "additionalProperties": false,
    })
}

/// Four wrong-typed properties, one more than MAX_SCHEMA_ERRORS, so
/// truncation in `bounded_errors` is observable.
fn multi_error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "a": { "type": "string" },
            "b": { "type": "string" },
            "c": { "type": "string" },
            "d": { "type": "string" },
        },
        "required": ["a", "b", "c", "d"],
    })
}

#[test_case::test_case(json!({"subagent_type": "bogus"}), UNKNOWN_SUBAGENT_ERR ; "unknown_subagent_type")]
#[test_case::test_case(json!({"output_schema": {"type": "object", "properties": {"x": {"type": 42}}}}), SCHEMA_COMPILE_ERROR ; "invalid_output_schema")]
#[test_case::test_case(json!({"output_schema": {"type": "array"}}), SCHEMA_ROOT_ERROR ; "non_object_output_schema")]
fn bad_input_errors_before_any_session(extra: Value, expected_prefix: &str) {
    let (reg, _host) = load_task_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    for (k, v) in extra.as_object().unwrap() {
        input[k.as_str()] = v.clone();
    }
    let err = exec_tool(&reg, TASK_TOOL, input).unwrap_err();
    assert!(err.starts_with(expected_prefix), "got: {err}");
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(0));
    assert_eq!(snap["prompt_count"], json!(0));
}

fn typeless_answer_schema() -> Value {
    let mut schema = answer_schema();
    schema.as_object_mut().unwrap().remove("type");
    schema
}

#[test_case::test_case(answer_schema() ; "typed_root")]
#[test_case::test_case(typeless_answer_schema() ; "missing_root_type_defaults_to_object")]
fn structured_happy_path_returns_validated_json(schema: Value) {
    let (reg, _host) = load_task_host();
    let out = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_HAPPY, Some(schema)))
        .expect("structured task failed");
    let parsed: Value = serde_json::from_str(&out).expect("result is not json");
    assert_eq!(parsed, json!({ "answer": "42" }));

    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(1));
    assert_eq!(snap["closed"], json!(1));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["has_local_tools"], json!(true));
    assert!(snap["first_ack"].is_string(), "valid input must be acked");
    assert!(snap.get("first_err").is_none_or(Value::is_null));
    let prompt = snap["prompts"][0].as_str().expect("prompt missing");
    assert!(prompt.starts_with(TASK_PROMPT), "got: {prompt}");
    assert!(
        prompt.contains(STRUCTURED_OUTPUT_TOOL),
        "prompt must point at the structured_output tool: {prompt}"
    );
}

#[tokio::test]
async fn invalid_then_valid_recovers_within_one_prompt() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_INVALID_THEN_VALID, Some(answer_schema())),
    )
    .expect("task should succeed after inline retry");
    let parsed: Value = serde_json::from_str(&out).expect("result is not json");
    assert_eq!(parsed, json!({ "answer": "42" }));

    let snap = probe(&reg);
    assert!(snap.get("first_ack").is_none_or(Value::is_null));
    let first_err = snap["first_err"].as_str().expect("first_err missing");
    assert!(
        first_err.contains("/answer"),
        "inline error should point at the failing path: {first_err}"
    );
    assert!(snap["second_ack"].is_string(), "valid retry must be acked");
    assert!(snap.get("second_err").is_none_or(Value::is_null));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["closed"], json!(1));
}

#[tokio::test]
async fn missing_structured_output_nudges_then_errors() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_NEVER_STRUCTURED, Some(answer_schema())),
    )
    .unwrap_err();
    assert_eq!(err, STRUCTURED_MISSING_ERROR);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    for i in 1..=MAX_STRUCTURED_RETRIES {
        let nudge = snap["prompts"][i].as_str().expect("nudge prompt missing");
        assert!(nudge.contains(STRUCTURED_OUTPUT_TOOL), "got: {nudge}");
    }
    assert_eq!(snap["closed"], json!(1));
}

#[tokio::test]
async fn invalid_only_errors_with_bounded_schema_errors() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_INVALID_ONLY, Some(multi_error_schema())),
    )
    .unwrap_err();
    assert!(err.starts_with(STRUCTURED_INVALID_ERROR), "got: {err}");
    assert_eq!(err.lines().count(), 1 + MAX_SCHEMA_ERRORS, "got: {err}");

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    let first_err = snap["first_err"].as_str().expect("first_err missing");
    assert_eq!(
        first_err.lines().count(),
        1 + MAX_SCHEMA_ERRORS,
        "inline error must carry at most MAX_SCHEMA_ERRORS validation lines: {first_err}"
    );
}

#[tokio::test]
async fn prompt_error_maps_to_sub_agent_error() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PROMPT_ERROR, None)).unwrap_err();
    assert_eq!(err, format!("{SUB_AGENT_ERROR_PREFIX}{PROMPT_ERR_MSG}"));
    let snap = probe(&reg);
    assert_eq!(snap["closed"], json!(1));
}

/// Esc during a sub-agent run: the prompt hands back both an error and
/// whatever the sub-agent managed to say, and half a transcript is worth
/// more to the model than a bare "cancelled".
#[tokio::test]
async fn interrupted_prompt_reports_the_partial_transcript() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PARTIAL_ERROR, None)).unwrap_err();
    assert_eq!(
        err,
        format!("sub-agent interrupted ({CANCELLED_ERR}). Partial output:\n{PARTIAL_TEXT}")
    );
}

#[tokio::test]
async fn plain_path_returns_text_without_local_tools() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    assert_eq!(snap["has_local_tools"], json!(false));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["prompts"][0], json!(TASK_PROMPT));
    assert_eq!(snap["closed"], json!(1));
}

#[tokio::test]
async fn no_summary_nudges_then_recovers() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_NO_SUMMARY_THEN_RECOVER, None),
    )
    .unwrap();
    assert_eq!(out, RECOVERED_TEXT);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(2));
    let nudge = snap["prompts"][1].as_str().expect("nudge prompt missing");
    assert!(nudge.contains(SUMMARY_NUDGE_FRAGMENT), "got: {nudge}");
    assert_eq!(snap["closed"], json!(1));
}

#[tokio::test]
async fn no_summary_errors_after_nudges() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_NO_SUMMARY, None)).unwrap_err();
    assert_eq!(err, SUMMARY_MISSING_ERROR);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    assert_eq!(snap["closed"], json!(1));
}

/// Spy counters catch a leaked permit even when gc would silently reclaim it.
#[tokio::test]
async fn raising_prompt_does_not_leak_semaphore_permit() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_RAISE, None)).unwrap_err();
    assert!(err.contains(RAISE_MSG), "got: {err}");

    let snap = probe(&reg);
    assert_eq!(
        snap["sem_size"],
        json!(TASK_DEFAULT_MAX_CONCURRENT),
        "semaphore not sized from the default max_concurrent option"
    );
    assert_eq!(snap["acquired"], json!(1));
    assert_eq!(snap["released"], json!(1), "permit not explicitly released");

    // Pool is full again (released == acquired), so this cannot block.
    let out = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(out, PLAIN_TEXT);
}

#[tokio::test]
async fn background_input_is_on_by_default_and_can_be_turned_off() {
    let advertised = |reg: &ToolRegistry| {
        reg.get(TASK_TOOL).unwrap().tool.schema()["properties"]["background"].is_object()
    };
    let mut off = serde_json::Map::new();
    off.insert("allow_background".into(), json!(false));
    assert!(advertised(&load_task_host().0));
    assert!(!advertised(&load_task_host_with_opts(off).0));
}

#[tokio::test]
async fn background_without_a_chat_session_runs_blocking() {
    let (reg, _host) = load_background_host();
    let out = exec_tool(&reg, TASK_TOOL, background_input(SCENARIO_PLAIN, None)).unwrap();
    assert!(out.starts_with(FOREGROUND_NOTE_HEAD), "{out}");
    assert!(out.contains(FOREGROUND_NOTE_TAIL), "{out}");
    assert!(out.ends_with(PLAIN_TEXT), "{out}");
    let snap = probe(&reg);
    assert_eq!(snap["scope"], Value::Null);
    assert_eq!(snap["released"], json!(1));
    assert_eq!(snap["notice_count"], json!(0));
}

/// A blocking `task` call as dispatch makes it: one scope under the run
/// token and its own event channel, executing on its own thread.
struct ModelCall {
    events: flume::Receiver<Envelope>,
    result: flume::Receiver<ToolDoneEvent>,
}

impl ModelCall {
    fn start(
        reg: &Arc<ToolRegistry>,
        chat: &Chat,
        run: &CancelToken,
        id: &str,
        input: Value,
    ) -> Self {
        let (ctx, events) = call_ctx(reg, chat.id, run, id);
        let reg = Arc::clone(reg);
        let (result_tx, result) = flume::bounded(1);
        thread::spawn(move || {
            let _ = result_tx.send(exec(&reg, TASK_TOOL, input, &ctx));
        });
        Self { events, result }
    }

    fn finish(&self) -> ToolDoneEvent {
        self.result
            .recv_timeout(WAIT)
            .expect("task call never replied")
    }
}

fn buf_text(buf: &SharedBuf) -> String {
    BufferSnapshot::from_arc(buf.read()).text()
}

fn reply_text(done: ToolDoneEvent) -> String {
    let text = done.output.as_text();
    assert!(!done.is_error, "task call failed: {text}");
    text
}

#[tokio::test]
async fn a_moved_task_keeps_its_slot_and_the_next_task_waits_visibly() {
    let mut opts = serde_json::Map::new();
    opts.insert("max_concurrent".into(), json!(1));
    let (reg, host) = load_task_host_with_opts(opts);
    host.ui_attachment().attach();
    let ui = host.ui_action_rx();
    let chat = Chat::live();
    let (_run_trigger, run) = CancelToken::new();

    let first = ModelCall::start(
        &reg,
        &chat,
        &run,
        FIRST_CALL_ID,
        task_input(SCENARIO_PARKED, None),
    );
    let mut showed_waiting_row = false;
    next_event(&first.events, |event| {
        showed_waiting_row |= matches!(event, AgentEvent::LiveToolBuf { .. });
        matches!(&event, AgentEvent::ToolMovable { movable: true, .. }).then_some(())
    });
    assert!(!showed_waiting_row, "a free slot needs no waiting row");
    assert_eq!(
        request_move(&host.event_handle(), FIRST_CALL_ID),
        MoveOutcome::Moved
    );
    let receipt = first.finish();
    assert!(receipt.backgrounded);
    let receipt = reply_text(receipt);
    assert!(receipt.starts_with(MOVED_RECEIPT), "{receipt}");

    let second = ModelCall::start(
        &reg,
        &chat,
        &run,
        SECOND_CALL_ID,
        task_input(SCENARIO_PARKED_SECOND, None),
    );
    let waiting = wait_live_buf(&second.events, SECOND_CALL_ID);
    assert_eq!(buf_text(&waiting), WAITING_FOR_SLOT);
    wait(&reg, json!({ "acquires": 2 }));
    assert_eq!(
        request_move(&host.event_handle(), SECOND_CALL_ID),
        MoveOutcome::NoHook
    );
    assert!(second.result.is_empty());
    assert_eq!(probe(&reg)["acquired"], json!(1));

    wait(&reg, json!({ "parked": SCENARIO_PARKED }));
    release(&reg, SCENARIO_PARKED);
    assert_eq!(
        done_text(&next_task_done(&ui)),
        (PLAIN_TEXT.to_owned(), false)
    );
    assert_eq!(probe(&reg)["released"], json!(1));

    wait(&reg, json!({ "parked": SCENARIO_PARKED_SECOND }));
    assert_eq!(buf_text(&waiting), "");
    wait_movable(&second.events, SECOND_CALL_ID);
    release(&reg, SCENARIO_PARKED_SECOND);
    let result = second.finish();
    assert!(!result.backgrounded);
    assert_eq!(reply_text(result), PLAIN_TEXT);
    let final_body = next_event(&second.events, |event| match event {
        AgentEvent::ToolSnapshot { snapshot, .. } => Some(snapshot.text()),
        _ => None,
    });
    assert!(final_body.contains(PLAIN_TEXT), "{final_body}");
}

/// Answers every request with one fixed sentence, standing in for the model
/// behind a native run.
struct FixedReply;

impl maki_providers::provider::Provider for FixedReply {
    fn stream_message<'a>(
        &'a self,
        _: &'a maki_providers::Model,
        _: &'a [maki_providers::Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<maki_providers::ProviderEvent>,
        _: maki_providers::RequestOptions,
        _: Option<&'a maki_storage::id::SessionRef>,
    ) -> maki_providers::provider::BoxFuture<
        'a,
        Result<maki_providers::StreamResponse, maki_providers::AgentError>,
    > {
        Box::pin(async {
            Ok(maki_providers::StreamResponse {
                message: maki_providers::Message {
                    role: maki_providers::Role::Assistant,
                    content: vec![maki_providers::ContentBlock::Text {
                        text: "native says hi".into(),
                    }],
                    ..Default::default()
                },
                stop_reason: Some(maki_providers::StopReason::EndTurn),
                ..Default::default()
            })
        })
    }

    fn list_models(
        &self,
    ) -> maki_providers::provider::BoxFuture<
        '_,
        Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>,
    > {
        Box::pin(async { unimplemented!() })
    }
}

/// `[agent.runs] enabled` with the Lua `task` plugin loaded: both tools are
/// registered, `task` delegates to the native hub, and the two do not disturb
/// each other.
#[tokio::test]
async fn native_and_legacy_task_enabled() {
    use maki_agent::runs::{RunsCtx, SystemClock, UnitHub, UnitLimits};

    let reg = Arc::new(ToolRegistry::new());
    let config = maki_config::RunsConfig {
        enabled: true,
        ..Default::default()
    };
    maki_agent::runs::sync_registration(&reg, &config).unwrap();
    let (reg, _host) = load_task_host_in(reg, serde_json::Map::new());
    for name in [TASK_TOOL, "spawn", "wait", "runs", "stop", "send"] {
        assert!(reg.get(name).is_some(), "{name} not registered");
    }

    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.provider = Arc::new(FixedReply);
    ctx.registry = Arc::clone(&reg);
    let (tx, _rx) = flume::unbounded::<maki_agent::Envelope>();
    let hub = UnitHub::new(
        UnitLimits::default(),
        maki_agent::EventSender::new(tx, 0),
        Arc::new(SystemClock),
    );
    ctx.runs = Some(RunsCtx {
        self_run: hub.main(),
        hub: Arc::clone(&hub),
    });
    let run = |name: &'static str, input: Value| {
        let reg = Arc::clone(&reg);
        let ctx = ctx.clone();
        async move {
            let inv = reg.get(name).unwrap().tool.parse(&input).unwrap();
            inv.execute(&ctx).await.output.map(|out| match out {
                ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
                other => panic!("unexpected output: {other:?}"),
            })
        }
    };

    // With runs on, `task` delegates to a native one-shot run (the Lua
    // session stubs of the prelude are not used, so the text is the provider's).
    let delegated = run(TASK_TOOL, task_input(SCENARIO_PLAIN, None))
        .await
        .unwrap();
    assert_eq!(delegated, "native says hi");

    let started = run(
        "spawn",
        json!({"kind": "agent", "description": "reader", "prompt": "say hi", "one_shot": true}),
    )
    .await
    .unwrap();
    let handle = started.split_whitespace().nth(1).unwrap().to_owned();
    assert!(
        handle.starts_with("ag-") && !handle.starts_with("toolu_"),
        "{handle}"
    );
    // The model's wait keeps to commands, so an agent is awaited the way a
    // plugin does: on the hub.
    let id = hub.resolve(hub.main(), &handle).unwrap();
    let views = (hub.wait(
        hub.main(),
        &[id],
        maki_agent::runs::WaitMode::All,
        std::time::Duration::from_secs(30),
        &CancelToken::none(),
    ))
    .await;
    let summary = &views[0].result.as_ref().unwrap().summary;
    assert!(summary.contains("native says hi"), "{summary}");
    assert!(
        views[0].state.label().contains("completed"),
        "{}",
        views[0].state.label()
    );

    // And `task` still delegates fine after a native run existed.
    assert_eq!(
        run(TASK_TOOL, task_input(SCENARIO_PLAIN, None))
            .await
            .unwrap(),
        "native says hi"
    );
}

// With `[agent.runs] enabled` the task tool is a native one-shot run: the
// plugin spawns it on the hub and waits, instead of opening a Lua session.
mod delegated {
    use std::sync::Arc;

    use maki_agent::runs::{RunsCtx, ShellOptions, SystemClock, UnitHub, UnitLimits};
    use maki_agent::test_support::MockProvider;
    use maki_agent::tools::ToolRegistry;
    use maki_agent::tools::test_support::stub_ctx;
    use maki_agent::{AgentMode, EventSender, ToolOutput};
    use maki_lua::PluginHost;
    use maki_providers::{ContentBlock, Message, Role, StopReason, StreamResponse, TokenUsage};
    use serde_json::{Value, json};

    const ANSWER: &str = "native answer";
    const TASK_SRC: &str = include_str!("../../plugins/task/init.lua");

    fn reply(text: &str) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text { text: text.into() }],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::EndTurn),
            ..Default::default()
        }
    }

    struct Setup {
        reg: Arc<ToolRegistry>,
        _host: PluginHost,
        hub: Arc<UnitHub>,
        provider: Arc<MockProvider>,
        ctx: maki_agent::tools::ToolContext,
        _dir: tempfile::TempDir,
    }

    fn setup(responses: Vec<StreamResponse>) -> Setup {
        setup_with(MockProvider::new(responses))
    }

    fn setup_with(provider: MockProvider) -> Setup {
        setup_with_limits(provider, UnitLimits::default())
    }

    fn setup_with_limits(provider: MockProvider, limits: UnitLimits) -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&reg)).unwrap();
        // The legacy path must not be taken: it would need these stubs, and
        // fails loudly if it is.
        let guard = r#"
            maki.agent.session = function() error("legacy session path taken") end
        "#;
        host.load_source_with_opts(
            "task_delegation",
            &format!("{guard}\n{TASK_SRC}"),
            serde_json::Map::new(),
        )
        .unwrap();
        let (tx, _rx) = flume::unbounded();
        let hub = UnitHub::with_shell_options(
            limits,
            EventSender::new(tx, 0),
            Arc::new(SystemClock),
            ShellOptions {
                dir: Some(dir.path().to_path_buf()),
                ..ShellOptions::default()
            },
        );
        let provider = Arc::new(provider);
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.provider = provider.clone();
        ctx.runs = Some(RunsCtx {
            hub: Arc::clone(&hub),
            self_run: hub.main(),
        });
        Setup {
            reg,
            _host: host,
            hub,
            provider,
            ctx,
            _dir: dir,
        }
    }

    fn run_task(s: &Setup, input: Value) -> Result<String, String> {
        let entry = s.reg.get("task").expect("task registered");
        let inv = entry.tool.parse(&input).expect("parse");
        maki_lua::test_support::runtime()
            .block_on(inv.execute(&s.ctx))
            .output
            .map(|out| match out {
                ToolOutput::Plain(t) | ToolOutput::Markdown(t) => t.text,
                other => panic!("unexpected output: {other:?}"),
            })
    }

    #[tokio::test]
    async fn task_runs_as_a_native_one_shot_agent_when_runs_are_enabled() {
        let s = setup(vec![reply(ANSWER)]);
        let out = run_task(
            &s,
            json!({"description": "find it", "prompt": "look around", "subagent_type": "research"}),
        )
        .unwrap();
        assert_eq!(out, ANSWER);
        assert_eq!(s.provider.requests.lock().unwrap().len(), 1);
        // The hub owns the run: it is listed there, finished and one-shot.
        let views = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].kind.as_str(), "agent");
        assert_eq!(views[0].state_label(), "completed");
        assert_eq!(s.hub.slots_in_use(), 0);
    }

    /// `load_source` runs with every permission, so a manifest that forgets
    /// `agent` (needed by `maki.run.wait`) only shows up with a real load.
    #[tokio::test]
    async fn manifest_grants_the_permissions_delegation_needs() {
        let manifest: toml::Table = include_str!("../../plugins/task/plugin.toml")
            .parse()
            .unwrap();
        let perms = manifest["permissions"].as_table().unwrap();
        for needed in ["run", "agent"] {
            assert_eq!(
                perms.get(needed).and_then(|v| v.as_bool()),
                Some(true),
                "{needed}"
            );
        }
    }

    /// The delegated subagent must not inherit the hub's default agent
    /// timeout: the task tool asks for the session maximum, so a task the
    /// model knows to be long keeps running instead of being cut off with
    /// partial output.
    #[tokio::test]
    async fn the_delegated_task_asks_for_the_session_maximum() {
        let s = setup_with_limits(
            MockProvider::new(vec![reply(ANSWER)]),
            UnitLimits {
                default_timeout: std::time::Duration::from_secs(1800),
                max_timeout: std::time::Duration::from_secs(7200),
                ..UnitLimits::default()
            },
        );
        run_task(&s, json!({"description": "x", "prompt": "y"})).unwrap();
        let views = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All);
        assert_eq!(views.len(), 1);
        assert_eq!(
            views[0].timeout_secs, 7200,
            "the subagent must get the session maximum, not the 1800s default"
        );
    }

    /// The model can still cap a subagent it expects to overrun.
    #[tokio::test]
    async fn the_delegated_task_takes_a_timeout_from_the_caller() {
        let s = setup_with_limits(
            MockProvider::new(vec![reply(ANSWER)]),
            UnitLimits {
                default_timeout: std::time::Duration::from_secs(1800),
                max_timeout: std::time::Duration::from_secs(7200),
                ..UnitLimits::default()
            },
        );
        run_task(
            &s,
            json!({"description": "x", "prompt": "y", "timeout_secs": 600}),
        )
        .unwrap();
        let views = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].timeout_secs, 600);
    }

    #[tokio::test]
    async fn a_failing_native_run_is_a_task_error() {
        // No scripted response: the provider assert fails the run, which the
        // task reports as an error rather than success.
        let s = setup(vec![]);
        let out = run_task(&s, json!({"description": "x", "prompt": "y"}));
        assert!(out.is_err(), "{out:?}");
    }

    #[tokio::test]
    async fn cancelling_the_task_stops_the_native_run() {
        let (trigger, token) = maki_agent::cancel::CancelToken::new();
        let trigger = std::sync::Mutex::new(Some(trigger));
        let mut provider = MockProvider::new(vec![reply(ANSWER)]);
        // Cancel the task while the run's first request is in flight, and keep
        // the request busy so the run has not finished when the wait sees it.
        provider.on_request = Some(Box::new(move || {
            if let Some(t) = trigger.lock().unwrap().take() {
                t.cancel();
            }
            std::thread::sleep(std::time::Duration::from_millis(400));
        }));
        let mut s = setup_with(provider);
        s.ctx.cancel = token;
        let out = run_task(&s, json!({"description": "x", "prompt": "y"}));
        let err = out.unwrap_err();
        assert!(err.contains("cancelled"), "{err}");
        let views = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All);
        assert_eq!(views.len(), 1);
    }

    /// The user's report: a cancelled sub-agent must not stop the next one
    /// from being spawned. A cancelled run that keeps its slot makes every
    /// later task queue behind a dead run, and a cancelled run that stays
    /// live keeps the parent's child count up.
    #[tokio::test]
    async fn a_cancelled_task_leaves_the_next_task_free_to_spawn() {
        let (trigger, token) = maki_agent::cancel::CancelToken::new();
        let trigger = std::sync::Mutex::new(Some(trigger));
        let mut provider = MockProvider::new(vec![reply(ANSWER), reply(ANSWER)]);
        provider.on_request = Some(Box::new(move || {
            if let Some(t) = trigger.lock().unwrap().take() {
                t.cancel();
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }));
        let mut s = setup_with(provider);
        s.ctx.cancel = token;
        let out = run_task(&s, json!({"description": "x", "prompt": "y"}));
        assert!(out.unwrap_err().contains("cancelled"));
        let views = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All);
        assert_eq!(views.len(), 1);
        // The stop is asynchronous: the run settles when its driver sees the
        // cancel. Give it the time it needs, then look.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let state = s.hub.list(s.hub.main(), maki_agent::runs::Scope::All)[0]
                .state_label()
                .to_owned();
            if state == "killed" || state == "failed" || state == "completed" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the cancelled run never settled, it stayed {state}"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(
            s.hub.slots_in_use(),
            0,
            "a stopped run must not hold an agent slot"
        );
        // The next task, in a fresh turn with a fresh token, must run.
        s.ctx.cancel = maki_agent::cancel::CancelToken::none();
        let out = run_task(&s, json!({"description": "y", "prompt": "z"}));
        assert_eq!(out.unwrap(), ANSWER);
        assert_eq!(s.hub.slots_in_use(), 0);
    }
}
