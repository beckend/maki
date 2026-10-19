-- This plugin owns the /tasks picker over the subagents spawned here:
-- picker.lua registers the command and the keymap when this file is loaded, so
-- the two cannot be enabled apart and left pointing at each other's absence.
--
-- With `[agent.runs] enabled` the subagent is a native one-shot run: the hub
-- owns its slot (`max_agents`), queue, stop and journal, and `/runs` shows it.
-- This plugin then only translates the `task` call into `spawn` + `wait` and
-- the result back. The Lua session path stays for when runs are off, and for
-- `allow_model`, whose exact model spec the native `spawn` has no field for.

local ToolView = require("maki.tool_view")
local output_limits = require("maki.output_limits")
require("picker")

local Runner = require("runner")
local Background = Runner.allow_background and require("background")

local DEFAULT_OUTPUT_LINES = 5

local description = [[Launch an autonomous subagent to perform tasks independently. Best combined with batch.

Subagent types (set via `subagent_type`):
- `research` (default): Read-only tools. For codebase exploration or gathering context.
- `general`: Full tool access. For delegating implementation work.

Notes:
1. Launch multiple tasks concurrently when possible.
2. The agent's result is not visible to the user. Summarize it in your response.
3. Each invocation starts fresh - inline any needed context into the prompt.
4. Tell it to return concise summaries with file:line refs, not full file contents.
]]

local examples = {
  {
    description = "Find auth middleware",
    prompt = "Search the codebase for authentication middleware. Return file paths and a summary of how auth is implemented.",
    model_tier = "weak",
  },
}

-- The native path: the hub runs the subagent, so the model sees one run and
-- the transcript lives with it. `Runner` owns the session path; this only
-- speaks the hub's language.
local function delegate(input, ctx, subagent_type)
  local output_schema = input.output_schema
  if output_schema then
    if (output_schema.type or "object") ~= "object" then
      return { llm_output = Runner.errors.schema_root, is_error = true }
    end
    output_schema.type = "object"
  end
  local thinking = input.thinking
  if thinking ~= nil and type(thinking) ~= "string" then
    thinking = tostring(thinking)
  end
  local background = input.background == true
  local run, err = maki.run.spawn(ctx, {
    kind = "agent",
    description = input.description,
    prompt = input.prompt,
    subagent_type = subagent_type,
    model_tier = input.model_tier,
    thinking = thinking,
    output_schema = output_schema,
    one_shot = true,
    notify = background and "wake" or "never",
  })
  if err then
    return { llm_output = "sub-agent error: " .. err, is_error = true }
  end
  if background then
    -- The hub wakes the parent with the result, so the turn can end here.
    return { llm_output = "started " .. run.handle .. " (" .. run.state .. ")", format = "markdown" }
  end
  local view
  while true do
    local views, wait_err = maki.run.wait(ctx, { ids = { run.handle }, timeout_secs = 600 })
    if wait_err then
      -- The task was cancelled (Esc): the native run would otherwise go on.
      maki.run.stop(ctx, { ids = { run.handle }, wait_secs = 0 })
      return { llm_output = "sub-agent error: " .. wait_err, is_error = true }
    end
    view = views[1]
    if view.settled then
      break
    end
  end
  local text = view.summary or ""
  if view.done and view.reason ~= "completed" then
    local detail = view.detail and (": " .. view.detail) or ""
    if text ~= "" then
      return {
        llm_output = "sub-agent interrupted (" .. view.reason .. detail .. "). Partial output:\n" .. text,
        is_error = true,
      }
    end
    return { llm_output = "sub-agent error: " .. view.reason .. detail, is_error = true }
  end
  if output_schema then
    if view.structured == nil then
      return { llm_output = Runner.errors.structured_missing, is_error = true }
    end
    return { llm_output = maki.json.encode(view.structured), format = "markdown" }
  end
  if text == "" then
    return { llm_output = Runner.errors.summary_missing, is_error = true }
  end
  return { llm_output = text, format = "markdown" }
end

local function handler(input, ctx)
  -- The native run has no field for an exact model spec, so a call that names
  -- one keeps the session path.
  if maki.run.enabled(ctx) and not (Runner.allow_model and input.model) then
    return delegate(input, ctx, input.subagent_type or "research")
  end

  local prepared, prepare_err = Runner.prepare(input, ctx)
  if prepare_err then
    return { llm_output = prepare_err, is_error = true }
  end

  if Background and input.background then
    local result = Background.start(prepared, ctx)
    if result then
      return result
    end
  end

  local permit = Runner.semaphore:acquire()
  -- Declared out here so the epilogue closes it on every path: left to the
  -- garbage collector it keeps the subagent's event relay alive on an idle VM.
  local sess

  -- pcall so a raised error cannot leak the permit or the session.
  local ok, out = pcall(function()
    local sess_err
    sess, sess_err = maki.agent.session(ctx, prepared.options)
    if sess_err then
      return { llm_output = sess_err, is_error = true }
    end
    return Runner.run(sess, prepared)
  end)

  if sess then
    sess:close()
  end
  permit:release()
  if not ok then
    error(out, 0)
  end
  return out
end

local function header(input)
  return input.description
end

-- Standalone runs render markdown on the Rust side (format = "markdown");
-- this mirrors that for restore and batch children, which build the body here.
local function restore(_input, output, is_error, ctx)
  local tol = ctx:tool_output_lines()
  return ToolView.restore_markdown(output, is_error, {
    max_lines = (tol and tol.task) or DEFAULT_OUTPUT_LINES,
    keep = "head",
    max_line_bytes = output_limits.DEFAULT_MAX_LINE_BYTES,
  })
end

maki.api.register_tool({
  name = "task",
  description = description,
  kind = "execute",
  audiences = { "main", "workflow" },
  examples = examples,
  schema = Runner.schema,
  handler = handler,
  header = header,
  restore = restore,
})
