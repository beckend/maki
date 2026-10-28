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
require("stop")

local Runner = require("runner")
-- Steering is for parents that chose background delegation, the rest should
-- not pay for the tool's definition.
local Message = Runner.allow_background and require("message")

local DEFAULT_OUTPUT_LINES = 5
local WAITING_FOR_SLOT = "waiting for a free subagent slot"
local MOVED_RECEIPT = "Subagent %s"
local FOREGROUND_NOTE = "Could not start in the background (%s). Ran in the foreground."

local description = [[Launch an autonomous subagent to perform tasks independently. Best combined with batch.

Subagent types (set via `subagent_type`):
- `research` (default): Read-only tools. For codebase exploration or gathering context.
- `general`: Full tool access. For delegating implementation work.

Notes:
1. Launch multiple tasks concurrently when possible.
2. The agent's result is not visible to the user. Summarize it in your response.
3. Each invocation starts fresh - inline any needed context into the prompt.
4. Tell it to return concise summaries with file:line refs, not full file contents.
5. A subagent runs until it is done; it is stopped at its time limit and then reports
   only partial output. The limit is the session maximum unless you set timeout_secs.
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
  -- 0 asks the hub for the session maximum. The default agent timeout is
  -- 30 minutes, and a subagent that hits it stops with only partial output,
  -- so a task the model knows to be long must not inherit that cap silently.
  local timeout_secs = input.timeout_secs
  if timeout_secs ~= nil then
    timeout_secs = math.floor(tonumber(timeout_secs) or 0)
    if timeout_secs < 0 then
      timeout_secs = 0
    end
  else
    timeout_secs = 0
  end
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
    timeout_secs = timeout_secs,
    -- The blocking path owns the sub-agent: when the user cancels the turn
    -- the run is stopped with it, so it leaves no slot behind for the next
    -- sub-agent. A background task must outlive the turn, so it is not
    -- bound.
    bind_to_caller = not background,
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

  local call_id, chat = ctx:tool_use_id(), ctx:session_id()
  -- Declared out here so the epilogue closes it on every path: left to the
  -- garbage collector it keeps the subagent's event relay alive on an idle VM.
  local sess
  local moved
  local function on_background()
    moved = Message and Message.track(call_id, chat, prepared.options.name)
    if moved and sess then
      Message.attach(moved, sess)
    end
    return string.format(MOVED_RECEIPT, prepared.options.name)
  end

  -- A background task moves before it waits for a slot, so a full house never
  -- holds the model's turn. Once moved it still queues for a slot like any
  -- other task. A blocking task only gets its hook once it holds a slot.
  -- Moving fails where background work is off (a zero --background-timeout),
  -- and then the task simply runs blocking.
  local foreground_note
  local run_in_background = Runner.allow_background and input.background == true
  if run_in_background then
    ctx:on_background(on_background)
    local moved, move_err = ctx:background()
    if not moved then
      foreground_note = string.format(FOREGROUND_NOTE, tostring(move_err))
    end
  end

  -- Shown only while every slot is taken, moved subagents included. A live
  -- body outlives the wait as a snapshot, so the final reply carries its own.
  local waiting
  local permit = Runner.acquire_slot(function()
    waiting = maki.ui.buf()
    waiting:line({ { WAITING_FOR_SLOT, "dim" } })
    ctx:live_buf(waiting)
  end)
  if waiting then
    waiting:set_lines({})
  end

  -- pcall so a raised error cannot leak the permit or the session.
  local ok, out = pcall(function()
    local sess_err
    sess, sess_err = maki.agent.session(ctx, prepared.options)
    if sess_err then
      return { llm_output = sess_err, is_error = true }
    end
    if moved then
      Message.attach(moved, sess)
    end
    if not run_in_background then
      ctx:on_background(on_background)
    end
    return Runner.run(sess, prepared)
  end)

  if moved then
    Message.untrack(call_id)
  end
  if sess then
    sess:close()
  end
  permit:release()
  if not ok then
    error(out, 0)
  end
  if waiting then
    out.body = result_body(out.llm_output, out.is_error, ctx)
  end
  if foreground_note then
    out.llm_output = foreground_note .. "\n\n" .. out.llm_output
  end
  return out
end

local function header(input)
  return input.description
end

local function restore(_input, output, is_error, ctx)
  return result_body(output, is_error, ctx)
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
