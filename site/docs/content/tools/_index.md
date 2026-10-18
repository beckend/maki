+++
title = "Tools"
weight = 4
[extra]
group = "Reference"
+++

# Tools

Maki ships with 26 built-in tools in this reference (20 on by default, 6 opt-in via plugin options or `agent.runs.enabled`). Tools marked **opt-in** are off until you enable them under `plugins` or `agent.runs` in [Configuration](/docs/configuration/).

## File Operations

### `bash` {#bash}

Execute a bash command.
Commands run in the session's working directory (see Environment) by default.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `command` | string | yes |  | The bash command to execute |
| `description` | string | no |  | Short description (3-5 words) of what the command does |
| `tail` | integer | no |  | Return only the last N lines |
| `timeout` | integer | no | 120 | Timeout in seconds |
| `workdir` | string | no | cwd | Working directory |

### `list` {#list}

List directory contents. Returns entry names sorted alphabetically, directories first with a trailing /.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Absolute path to the directory |

### `read` {#read}

Read a file. Returns contents with line numbers (1-indexed).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `limit` | integer | yes | Max number of lines to read. Use 0 to read until end of file (capped at 2000 lines). |
| `offset` | integer | yes | Line number to start from (1-indexed). Use 1 for the first line. |
| `path` | string | yes | Absolute path to the file |

### `write` {#write}

Write content to a file, replacing existing content.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `append` | boolean | no | Add content to the end of the file instead of replacing it |
| `content` | string | yes | The complete file content to write |
| `path` | string | yes | Absolute path to the file |

### `edit` {#edit}

Replace an exact string match in a file.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `new_string` | string | yes |  | Replacement string |
| `old_string` | string | yes |  | Exact string to find (must match uniquely unless replace_all is true) |
| `path` | string | yes |  | Absolute path to the file |
| `replace_all` | boolean | no | false | Replace all occurrences |

### `multiedit` {#multiedit}

Make multiple find-and-replace edits to a single file atomically.
Prefer this over edit when making multiple changes to the same file.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `edits` | array | yes | Array of edit operations to apply sequentially |
| `path` | string | yes | Absolute path to the file |

### `edit_lines` {#edit_lines}

Edit lines by number. Replaces lines from `start` to `end` (inclusive) with `new_string`. Use empty `new_string` to delete a range. Do not use with the batch tool.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `end` | integer | yes | Last line, inclusive |
| `new_string` | string | yes | Replacement text |
| `path` | string | yes | Absolute path to the file |
| `start` | integer | yes | First line (1-indexed) |

### `insert_lines` <span class="badge badge-optin">opt-in</span> {#insert_lines}

Insert `new_string` after line `line`, or at the top with 0. Only include new lines, never lines already in the file. Do not use with the batch tool.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `line` | integer | yes | Line number to insert after (1-indexed). Use 0 to insert at the top. |
| `new_string` | string | yes | Text to insert |
| `path` | string | yes | Absolute path to the file |

### `glob` {#glob}

Find files by glob pattern.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | string | no | cwd | Directory to search in |
| `pattern` | string | yes |  | Glob pattern (e.g. **/*.rs, src/**/*.ts) |

### `grep` {#grep}

Search file contents using regex.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `context_after` | integer | no |  | Context lines after match |
| `context_before` | integer | no |  | Context lines before match |
| `include` | string | no |  | File glob filter (e.g. *.c) |
| `limit` | integer | no |  | Max match groups to return |
| `path` | string | no | cwd | Directory to search in |
| `pattern` | string | yes |  | Regex pattern |

### `index` {#index}

Return a compact overview of a source file: imports, type definitions, function signatures, and structure with their line numbers surrounded by []. ~70-90% more efficient than reading the full file.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Absolute path to the file |

### `view_image` {#view_image}

View an image file (png, jpeg, gif, webp) so you can actually see it; it is returned as vision input alongside the tool result. Use instead of `read` for images.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Path to the image file |

## Execution & Control

### `batch` {#batch}

Executes multiple independent tool calls concurrently to reduce round-trips.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tool_calls` | array | yes | Array of tool calls to execute in parallel |

### `code_execution` {#code_execution}

Execute Python in a sandbox where every tool is an async function.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `code` | string | yes |  | Python code. Tools return strings, not objects, and you MUST await every call: `result = await read(path='/file', offset=1, limit=0)`. |
| `timeout` | integer | no | 30 | Script execution timeout in seconds |

### `question` {#question}

Use this tool when you need to ask the user questions during execution. This allows you to:
- Gather user preferences or requirements
- Clarify ambiguous instructions
- Get decisions on implementation choices as you work
- Offer choices to the user about what direction to take

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `questions` | array | yes | List of questions to ask the user |

## Agent & Knowledge

### `task` {#task}

Launch an autonomous subagent to perform tasks independently. Best combined with batch.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `description` | string | yes | Short (3-5 words) description of the task |
| `model_tier` | string | no | Model tier (optional, omit to use current model, capped at current tier):<br>- "strong" (e.g. Opus): Deep reasoning, complex architecture, subtle bugs, most critical sections. ~5x cost of medium.<br>- "medium" (e.g. Sonnet): Balanced. Refactors, features, multi-file changes.<br>- "weak" (e.g. Haiku): Fast/cheap. Search, summarize, boilerplate, simple edits. |
| `output_schema` | object | no | JSON Schema (object) the subagent's final result must match. When set, the result is returned as a validated JSON string. |
| `prompt` | string | yes | Detailed task prompt for the agent |
| `subagent_type` | string | no | Subagent type: "research" (read-only, default) or "general" (can modify files) |
| `thinking` | any | no | Thinking: off\|adaptive\|minimal\|low\|medium\|high\|xhigh\|max\|int budget. Omit to inherit parent; capped at parent. |

### `todo_write` {#todo_write}

Create or update a structured todo list to track tasks.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `todos` | array | yes | The updated todo list |

### `memory` {#memory}

Persistent, project-scoped scratchpad for learnings, patterns, decisions, and gotchas across sessions.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `command` | string | yes | - `list [tags]`: tag-grouped index, no bodies.<br>- `read path\|tags`: one body (path) or collated bodies (tags).<br>- `write path tags content`: create or overwrite a note.<br>- `delete path` |
| `content` | string | no | Body for write (frontmatter added automatically). |
| `path` | string | no | Relative path, e.g. 'architecture.md'. |
| `tags` | array | no | snake_case tags. Filter for list/read; assigned on write (defaults to filename stem). |

### `skill` {#skill}

Load a skill that provides instructions and workflows for specific tasks.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | yes | Name of the skill to load |

## Background Runs

### `spawn` <span class="badge badge-optin">opt-in</span> {#spawn}

Start background work and return immediately with its id. kind "agent" starts a
subagent: a separate agent with its own context that works on a self-contained task and
reports back. kind "shell" starts a command that keeps running while you continue. You
are notified when a run finishes; do not poll or sleep. Start independent runs together
in one message. Use `wait` only when you are blocked on a result, `send` to talk to a
run, `stop` to end it, `runs` to look. Not available to agents at the maximum depth: you
get DEPTH_LIMIT and must do the work yourself.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `command` | string | no |  | shell only. Command line run with the user's shell. |
| `context` | string | no | fresh | agent only. fork seeds the run with your completed turns, excluding the turn in flight. |
| `cwd` | string | no |  |  |
| `description` | string | yes |  | 3-8 word label for display and for default naming. |
| `kind` | string | yes |  |  |
| `model_tier` | string | no |  | agent only. Clamped to your tier. |
| `name` | string | no |  | Optional unique name others can address this run by. `main` and `parent` are reserved. |
| `notify` | string | no |  | On finish: wake starts a turn if you are idle (default), quiet waits for your next turn, never sends nothing (read with runs). |
| `one_shot` | boolean | no |  | agent only. Complete after the first result instead of staying available. Default false. |
| `output_schema` | object | no |  | agent only. JSON Schema the final result must satisfy. |
| `prompt` | string | no |  | agent only. The complete, self-contained task. |
| `subagent_type` | string | no |  | agent only. research is read-only; general can edit. Default general. |
| `thinking` | string | no |  |  |
| `timeout_secs` | integer | no |  | Wall clock limit. Default: 1800 for agents, 3600 for shell. Expiry ends the run as timed_out. |
| `watch` | string | no |  | shell only. Regex; each matching output line becomes a notice (max 1 per second, then batched). |

### `send` <span class="badge badge-optin">opt-in</span> {#send}

Send a message to another run in this session: your child, your parent (name "parent"),
or "main". (Siblings only if the session enables it.) The message is queued and returns a
message id at once. A running recipient reads it at its next step; an idle one starts a turn
with it. Sending does not end your turn and is not an answer: the recipient's reply, if any,
comes as a message to you. Use mode "quiet" for FYI notes that must not start a turn. Your
final result reaches your parent on its own: use send to a parent only for a question or
mid-run progress.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `message` | string | yes | Up to 16 KiB. Be self-contained. |
| `mode` | string | no | steer (default): next step of a running recipient, wakes an idle one. followup: after the recipient's current turn ends, as its own turn. quiet: delivered at the recipient's next model request, never wakes. |
| `to` | string | yes | Run id or name, or "parent" / "main". |

### `runs` <span class="badge badge-optin">opt-in</span> {#runs}

List your runs and everything below you, or inspect one. Without id: a tree of runs with id,
name, kind, state, age, last activity, cost and a one-line progress, plus unread quiet
notices. With id: the full record, the result (full=true for untruncated), and for shell runs
output since your last read (tail=N lines, from_offset=B to page; output is read from the log
file). Reading a finished run's result marks it received, so its notice is dropped.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `from_offset` | integer | no | shell: byte offset in the log; the reply gives next_offset. |
| `full` | boolean | no |  |
| `id` | string | no |  |
| `include_finished` | boolean | no | List only. Default true, finished runs retained for the session. |
| `scope` | string | no | No id only. Default children. all includes siblings and ancestors. |
| `tail` | integer | no | shell: last N lines. Default 40. |

### `wait` <span class="badge badge-optin">opt-in</span> {#wait}

Block until runs have finished their work or a timeout expires. Use only when you cannot
proceed without the result; otherwise keep working, you will be notified. mode "all" (default)
returns when every listed run is done or idle (finished its turn), "any" when the first is.
A timeout is not an error: you get the current states and the runs keep going. Results you
receive here are not repeated as notices.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ids` | array | yes |  |
| `mode` | string | no |  |
| `timeout_secs` | integer | no | Default 60. |

### `stop` <span class="badge badge-optin">opt-in</span> {#stop}

Stop runs. Agents are cancelled at their next safe point and their children are stopped
too (detach=true leaves children running). With soft=true an agent only abandons its current
turn: it stays idle, keeps its transcript and inbox, and can be messaged again. Shells get
SIGTERM, then SIGKILL after 5 seconds. Returns once the runs have settled or after wait_secs.
A stopped agent keeps its transcript and is read with runs; you will not get a notice for
runs you stopped.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `detach` | boolean | no |  |
| `ids` | array | yes |  |
| `reason` | string | no | Forwarded to the run as its last message. |
| `soft` | boolean | no | Agents only. Cancel the current turn, stay idle, keep the inbox. |
| `wait_secs` | integer | no | Default 10. |

## Web

### `webfetch` {#webfetch}

Fetch a URL and return its contents.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `format` | string | no |  | Output format: markdown (default), text, or html |
| `timeout` | integer | no | 30, max 120 | Timeout in seconds |
| `url` | string | yes |  | URL to fetch (http:// or https://) |

### `websearch` {#websearch}

Search the web for real-time information using Exa AI.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `num_results` | integer | no | 8 | Number of results to return |
| `query` | string | yes |  | Search query |