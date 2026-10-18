# Native background agents and shell jobs (design)

Status: proposal, branch `native-agents`. Part A covers goals, data model, tool surface,
delivery and the native-tool mechanism. Part B covers limits, persistence and phases.
Line references are to maki 0.6.4 on this branch. Revised after two independent reviews (code
facts re-verified, open questions decided, phases re-split).

## 1. Goals and non-goals

Goals:
1. Subagents and background shell jobs are core Rust in `maki-agent`, so TUI, `maki -p`,
   ACP and SDK all get them (headless has no wake consumer today, see
   `maki-agent/src/live_session.rs:111` `claim_wake`, used only by
   `maki-ui/src/event_loop.rs:1189` `start_mailbox_runs`).
2. Free two-way messaging: parent to child, child to parent, sibling to sibling, under a
   known topology the model can see and the runtime enforces.
3. One concept (a run, `Unit` in Rust), one id scheme, one state machine, one notice path, one small tool
   family, for agents and shell jobs alike.
4. Bounded everything: notices, inboxes, outputs, wake storms. Full bodies are pulled by id.
5. Cache-safe: topology reaches the model through `tell_changes`/`LiveFacts`, never by
   editing the system prompt.
6. Offer a native replacement for the external Lua
   `maki-monitor` plugin (phase 2b ships the shell replacement; the Lua `task` tool, the
   Lua APIs and `maki-monitor` are left untouched in v1, see appendix B).

Non-goals: remote or cross-process agents, external-harness children (ACP/Codex as
children), a workflow DSL, re-adoption of live agent runs or shell jobs after a crash,
changing the UI tab model beyond consuming new events.

### 1.1 Comparison

| Aspect | dsh | maki today | This design |
|---|---|---|---|
| Subsystems | Two (subagent manager, jobs registry) with different ids, states, notices | Three: Lua `task`, Lua `maki.fn.job*` (`maki-lua/src/api/fn.rs:820`), user `maki-monitor` plugin | One `UnitHub`, one `Unit` |
| Ids | session uuid vs `bash-<16hex>` | task_id (tool_use_id), session_id, mailbox MakiId all differ | One `MakiId` per run, short handle `ag-k3x9ab`/`sh-4m2q7z` plus unique name |
| States | agents `running\|inactive`, jobs 5 states | ad hoc per UI tab | `Queued Running Idle Stopping Done(reason)`, reason one of completed failed killed timed_out lost max_turns max_tokens refused |
| Tools | 7+ (`subagent`, `send_message`, `interrupt_agent`, `list_agents`, `job_output/list/kill`) | `task`, bash, plugin tools | 5: `spawn send runs wait stop` |
| Wait on a child | none in core | none (blocking `task` call) | `wait(ids, any/all, timeout)` |
| Notice size | unbounded closing message | whole answer is tool result | 4 KiB cap, spill to disk, pull full body |
| Notice dedupe | wait/kill only | none | any consuming read (wait, runs peek, stop) |
| Wake budget | opt-in, jobs only | none | default 3 consecutive notice wakes on main, agents and shells |
| Delivery acks | accepted only | none | accepted, delivered |
| Inbox bounds | unbounded | mailbox 100 (`live_session.rs:18`) | 32 pending, 16 KiB/message, per-pair rate limit |
| Siblings | not in core | no | parent/child and `main` by default; siblings via config only |
| Crash | notice lost, ids dangle | stranded tabs dropped | journal, `lost` state, notice on next wake |
| Stop | request, no escalation, no cascade | `cancel_all` per run | cascade by default, TERM, grace 5 s, KILL, returns after settle |
| Cost | none per child | `RunLedger::child` chain (`types.rs:1084`) | same chain, usage in every notice |
| Headless | `waitForChildren` for agents, jobs not joined | children die with run | hub `join()` joins both, `-p` waits |
| Topology to model | depth hint at limit | none | per-turn tree fact via `LiveFacts` |

## 2. Data model

### 2.1 Crate placement

New module tree `maki-agent/src/runs/` (no dependency on lua or ui, honoring the dependency
direction storage, config, providers, agent, lua, ui):

```
runs/mod.rs        UnitHub, UnitId, UnitHandle, UnitName, Unit, UnitKind, UnitState, DoneReason
runs/agent.rs      AgentRunner: driver loop owning History for a native agent
runs/shell.rs      ShellRunner: process spawn, log file, pump, TERM/KILL
runs/inbox.rs      Inbox, InboxItem, DeliveryKind, rate limiter, ping-pong guard
runs/notice.rs     Notice, NoticeKind, rendering, spill, consume claims
runs/topology.rs   Topology snapshot, router, facts rendering
runs/clock.rs      Clock trait, SystemClock, ManualClock (tests)
runs/limits.rs     UnitLimits and its config conversion
runs/journal.rs    adapter over the storage RunJournal (Part B)
tools/runs.rs      the five native tools (impl Tool, see section 5)
```

Also `maki-agent/src/test_support.rs` (shared `MockProvider`, `StubStreamProvider`, phase 1),
`maki-storage/src/runs.rs` (journal) and mirror config types in `maki-config`. UI-facing
information goes out as new `AgentEvent` variants (`types.rs:642`) over the existing
`EventSender`, never `UiAction` (which lives in `maki-lua`). `MakiId` stays in
`maki-storage/src/id.rs:37`.

Naming: this doc says "run" (the model-facing word, tool and config name); the Rust types are
prefixed `Unit` because `Run*` collides with the per-turn `run_id` (`Envelope.run_id`,
`types.rs:1195`), `RunContext` (`agent/frame.rs`) and `RunLedger` (`types.rs:1084`).

### 2.2 Ids and names

One id per run: the existing `MakiId` (16 byte uuid). It replaces three ids that mean the same
thing in different maps today: subagent `task_id: Option<Arc<str>>` (the spawning `tool_use_id`,
`AgentParams` at `run.rs:102`), the Lua session `MakiId` mailbox key
(`maki-lua/src/api/agent.rs` ~885) and the UI chat key. For native runs the UI chat key is the
handle (phase 3); `tool_use_id` is kept as `origin_tool_use_id` for rendering the spawn call.

```rust
pub struct UnitId(MakiId);                 // internal, persisted, mailbox key
pub struct UnitHandle(Arc<str>);           // "ag-k3x9ab" | "sh-4m2q7z": kind prefix + 6 base32 chars
pub struct UnitName(Arc<str>);             // lower-kebab, 1..=32 chars, unique per session tree
```

The model-facing id is the handle; the name works anywhere an id is accepted. `main` (root
agent) and `parent` (the caller's parent) are reserved and cannot be used as `name`. A handle
is derived from the last 4 bytes (a v7 uuid starts with a timestamp) of the `MakiId`; collisions inside one session tree are
retried at spawn. Names default to a slug of `description`, suffixed `-2`, `-3` on clash.
Display: name first, handle in parentheses.

`session_id` (`ToolContext.session_id`, provider affinity and otel) stays the root session's id
for every descendant; it is not an identity and nothing may key run maps by it (comment at
`maki-lua/src/api/agent.rs` ~879 warns of the collision).

### 2.3 The `Unit` (a "run")

```rust
pub enum UnitKind {
    Agent(AgentSpec),   // model, audience, tool filter, output_schema, thinking, one_shot, timeout
    Shell(ShellSpec),   // command, cwd, timeout, watch regex
}

pub struct Unit {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub name: UnitName,
    pub owner: UnitId,           // who gets notices; main agent id for top level
    pub parent: UnitId,          // who spawned it; equals owner unless re-parented (stop detach only)
    pub depth: u8,               // main = 0
    pub kind: UnitKind,
    pub description: String,     // 3-8 words, display
    pub origin_tool_use_id: Option<Arc<str>>,
    pub state: UnitState,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub last_activity: Instant,
    pub progress: Option<String>,// agent: current tool header; shell: last output line
    pub usage: TokenUsage,       // own, cumulative; subtree rolled via RunLedger
    pub cost: f64,
    pub notice_policy: NoticePolicy, // on_exit | on_match | never, wake or quiet
}

/// One field, no separate reason: the reason is part of Done.
pub enum UnitState { Queued, Running, Idle, Stopping, Done(Box<Terminal>) }

pub enum DoneReason {
    Completed,          // agent EndTurn of a one_shot agent, or idle expiry; shell exit 0
    Failed,             // agent error; shell nonzero exit
    Killed,             // stop requested (model, user, cascade, shutdown)
    TimedOut,           // wall clock hit
    MaxTurns, MaxTokens,
    Refused,
    Lost,               // process or session died, outcome unknown
}

pub struct Terminal {
    pub reason: DoneReason,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,      // short error or signal text
    pub result: UnitResult,
}

pub struct UnitResult {
    pub summary: String,             // <= 4096 bytes, model-facing
    pub truncated: bool,
    pub body_ref: Option<BodyRef>,   // full text on disk when truncated
    pub structured: Option<serde_json::Value>,
    pub usage: TokenUsage, pub cost: f64, pub turns: u32, pub elapsed_ms: u64,
}
```

`Idle` exists only for agents: the run finished a turn, has no pending wakeable inbox and may be
woken by `send`; its last turn result is held in the record. A dsh agent's `inactive`
conflates idle, finished, crashed and unloaded; here each is distinct. An agent is spawned
continuable by default; `one_shot: true` makes it `Done(Completed)` instead of `Idle`.

### 2.4 Hub

```rust
pub struct UnitHub {                       // one per root session, Arc
    units: Mutex<UnitTable>,               // id -> entry, name index, children index
    cancel: CancelToken,                   // session-lifetime root; runs hold child tokens
    ledger: Arc<RunLedger>,                // root ledger; each run holds RunLedger::child
    events: EventSender,                   // run_id 0, stamped with SubagentInfo for agents
    limits: UnitLimits,
    notices: NoticeRouter,
    waiters: WaiterSet,                    // wait() registrations and consume claims
}
```

Creation (5.3): in the TUI next to `publish_seed` (`maki-ui/src/agent/agent_loop.rs:85`) and
stored in `AgentSeed` (`live_session.rs:53`, registered in `LIVE_SESSIONS` at `:22`); in
headless beside `SessionMailbox::register` (`headless.rs:190`, `:339`) with no seed, passed
straight to `AgentParams`. Lock discipline is in section 4.

### 2.5 State machines

Agent run:

```
spawn ──> Queued ──slot free──> Running <──────────────┐
            │                     │   │ turn ends,     │ send(wake) / notice
            │ stop                │   │ inbox settled, │ addressed to it
            v                     │   │ continuable    │
        Done(Killed)              │   v                │
                                  │  Idle ─────────────┘
                                  │   │ one_shot, or stop, or idle expiry (silent)
   stop ─> Stopping ──────┐       │   v
     (children first)     │       └─> Done(Completed | Failed | TimedOut | MaxTurns ...)
                          v
                    Done(Killed)
   any non-Done ──(process exit/crash; journal replay finds it running)──> Done(Lost)
```

Shell run:

```
spawn ──> Queued ──> Running ──exit 0──> Done(Completed)
                       │  └──exit != 0──> Done(Failed)
                       │  └──deadline───> Stopping ──> Done(TimedOut)
   stop ──> Stopping ──TERM, 5 s grace, KILL──> Done(Killed)
   any non-Done ──(hub died; pid dead or unknown on replay)──> Done(Lost)
```

Rules: the first transition into `Done` wins (settle is idempotent); `Stopping` is entered once
and left only into `Done`; `Idle` is not terminal. A turn-done (`Idle`) produces a result
notice (kind `Settled`, state idle) so a continuable agent's result reaches its owner; `Idle`
expiry and `max_idle` eviction are silent (7.4). `Done(Lost)` is produced only by
reconciliation. `Queued` exists because capacity is a queue, not an error (6.1). Dead runs
past `max_retained` leave a tombstone (7.4).

### 2.6 Notices

All run-originated text for the model goes through one type and one path.

```rust
pub struct Notice {
    pub seq: u64,               // per-owner monotonic, for ordering and idempotency
    pub run: UnitId,
    pub kind: NoticeKind,
    pub wake: bool,
    pub text: String,           // rendered, <= 4096 bytes
    pub created: SystemTime,
}

pub enum NoticeKind {
    Settled,       // run reached Done, or an agent finished a turn and went Idle
    Lost,          // reconciliation found it dead; delivered at next owner wake
    Message,       // run-to-run message body (see inbox)
    Match,         // shell watch rule fired (replaces maki-monitor lines)
    Overflow,      // "N notices dropped" coalesced marker
}
```

Notices are rendered as `Message::observation` (the existing mailbox message type,
`live_session.rs`), user role, wrapped in `<run-notice run="ag-k3x9ab" name="auditor"
seq="12" kind="settled">...</run-notice>`, so the model and the UI can recognise them and
the UI collapses them into one row.

Structured results: the settlement notice carries `UnitResult` fields; the full record is
fetchable with `runs(id)` (section 3.4). An agent with `output_schema` registers the
existing `structured_output` local tool (`LocalTool`, `tools/mod.rs:304`) and its JSON
lands in `UnitResult.structured`, shown in the notice as `structured: {...}` if it fits in
the 4 KiB budget, else only as a `body_ref`.

Bounding and spill: `summary` is the final assistant text. If longer than 4096 bytes the
notice keeps the first 3072 and last 768 bytes joined by
`\n[... N bytes omitted; full text: runs(id="ag-k3x9ab", full=true) ...]\n` and the whole
text is written to `<state>/runs/<session>/<handle>.result.txt` (`BodyRef`). Shell output
is never inlined in a notice; it lives in `<handle>.log` (append-only, offset cursor)
and the notice carries the last 5 lines (max 1 KiB) plus byte counts.

## 3. Native tool surface

Five tools, all native. Rationale for consolidation instead of `agent_*` plus `job_*`:
the model needs one mental model (a run has an id, a state, a result, can be waited on,
messaged, stopped); the only kind-specific moment is `spawn`. dsh needs 7+ tools
because its two systems diverged; maki had `task`, a monitor plugin and `fn.job*`.

Old surface (all unchanged in v1; see 8.3 and appendix B):
* `task` (bundled Lua, `plugins/task`) stays and coexists with native `spawn`; its fields
  (`subagent_type`, `model_tier`, `output_schema`, `thinking`) map one to one onto `spawn`.
  Users can hide either with `disabled_tools`. Replacing it is a separate decision.
* `monitor_*` (external Lua plugin, not in this repo) is not touched; `spawn(kind="shell",
  watch=...)` (phase 2b) covers it and the docs carry a migration table.
* `maki.fn.jobstart` family stays for plugin-private processes, not the model's path. bash
  stays the synchronous tool; its description gains one sentence: "For anything that runs
  longer than a minute or must keep running, use `spawn` with kind shell." On timeout bash
  promotes the command to a background run (phase 2c).

### 3.1 `spawn`

Description (model-facing):

```
Start background work and return immediately with its id. kind "agent" starts a
subagent: a separate agent with its own context that works on a self-contained task and
reports back. kind "shell" starts a command that keeps running while you continue. You
are notified when a run finishes; do not poll or sleep. Start independent runs together
in one message. Use `wait` only when you are blocked on a result, `send` to talk to a
run, `stop` to end it, `runs` to look. Not available to agents at the maximum depth.

Agent runs do not see this conversation, so the prompt must contain everything they
need. They share your workspace. An agent run stays available after it reports and can
receive follow-up messages unless one_shot is true.
```

Schema:

```json
{
  "type": "object",
  "required": ["kind", "description"],
  "properties": {
    "kind": {"enum": ["agent", "shell"]},
    "description": {"type": "string", "description": "3-8 word label for display and for default naming."},
    "name": {"type": "string", "pattern": "^[a-z][a-z0-9-]{0,31}$", "description": "Optional unique name others can address this run by. `main` and `parent` are reserved."},
    "prompt": {"type": "string", "description": "agent only. The complete, self-contained task."},
    "subagent_type": {"enum": ["research", "general"], "description": "agent only. research is read-only; general can edit. Default general."},
    "model_tier": {"enum": ["weak", "medium", "strong"], "description": "agent only. Clamped to your tier."},
    "thinking": {"type": "string"},
    "output_schema": {"type": "object", "description": "agent only. JSON Schema the final result must satisfy."},
    "context": {"enum": ["fresh", "fork"], "description": "agent only. fork seeds the run with your completed turns, excluding the turn in flight (default fresh)."},
    "one_shot": {"type": "boolean", "description": "agent only. Complete after the first result instead of staying available. Default false."},
    "command": {"type": "string", "description": "shell only. Command line run with the user's shell."},
    "cwd": {"type": "string"},
    "timeout_secs": {"type": "integer", "minimum": 1, "description": "Wall clock limit. Default: 1800 for agents, 3600 for shell. Expiry ends the run as timed_out."},
    "watch": {"type": "string", "description": "shell only (phase 2b). Regex; each matching output line becomes a notice (max 1 per second, then batched)."},
    "notify": {"enum": ["wake", "quiet", "never"], "description": "On finish: wake starts a turn if you are idle (default), quiet waits for your next turn, never sends nothing (read with runs)."}
  }
}
```

Cross-kind fields are rejected with explicit text, never ignored: `kind=shell` given `prompt`,
`subagent_type`, `model_tier`, `thinking`, `output_schema`, `context` or `one_shot` returns
`BAD_FIELD: "prompt" is for kind "agent"; a shell takes "command"`; `kind=agent` given
`command`, `cwd` or `watch` returns `BAD_FIELD: "command" is for kind "shell"; an agent takes
"prompt"`. `name` equal to `main` or `parent` returns `BAD_FIELD: reserved name`.

`context=fork` semantics: the child starts from a copy of the caller's committed history
snapshot taken at spawn, which excludes the in-flight assistant turn (the pending `spawn`
`tool_use` and its siblings), so the copy ends on a complete user/tool_result boundary; the
copy is then followed by `prompt` as a user message. If a snapshot ending on a clean boundary
cannot be produced, `fork` is rejected with `BAD_FIELD`. Phase 3 may cut `fork` entirely if
that proves fragile.

Result: `started ag-k3x9ab "auditor" (agent, running)`; queued runs show `(queued, position 2
of 2)`. The child is registered in the parent's children before `spawn` returns (4.4).

Permission: `spawn` is `tool_kind` execute; prompting follows the bash rule for `kind=shell`
(scope = the command, so bash allow rules and `permission_scopes` apply) and no prompt for
`kind=agent` (the child's tools are gated individually). Plan mode blocks `kind=shell` and
agent `general` (`background_write_block`).

### 3.2 `send`

```
Send a message to another run in this session: your child, your parent (name "parent"),
or "main". (Siblings only if the session enables it.) The message is queued and returns a
message id at once. A running recipient reads it at its next step; an idle one starts a turn
with it. Sending does not end your turn and is not an answer: the recipient's reply, if any,
comes as a message to you. Use mode "quiet" for FYI notes that must not start a turn. Your
final result reaches your parent on its own: use send to a parent only for a question or
mid-run progress.
```

```json
{"type":"object","required":["to","message"],"properties":{
  "to": {"type":"string","description":"Run id or name, or \"parent\" / \"main\"."},
  "message": {"type":"string","description":"Up to 16 KiB. Be self-contained."},
  "mode": {"enum":["steer","followup","quiet"],"description":"steer (default): next step of a running recipient, wakes an idle one. followup: after the recipient's current turn ends, as its own turn. quiet: delivered at the recipient's next model request, never wakes."}
}}
```

Result: `message m-17 queued for ag-k3x9ab (running); use runs(id) to see delivery`. Stable error
codes: `UNKNOWN_RUN`, `NOT_REACHABLE` (outside tree or policy, including siblings when off),
`RECIPIENT_TERMINAL` (shells, one_shot completed, unrepairable), `NOT_AGENT` (shells take no
messages in v1), `INBOX_FULL retry_after=Ns`, `TOO_LARGE max=16384`, `RATE_LIMITED
retry_after=Ns`, `HELD` (ping-pong guard, 4.7), `SELF`. A `send` to a terminal revivable agent
revives it (7.4).

### 3.3 `wait`

```
Block until runs have finished their work or a timeout expires. Use only when you cannot
proceed without the result; otherwise keep working, you will be notified. mode "all" (default)
returns when every listed run is done or idle (finished its turn), "any" when the first is.
A timeout is not an error: you get the current states and the runs keep going. Results you
receive here are not repeated as notices.
```

```json
{"type":"object","required":["ids"],"properties":{
  "ids":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":16},
  "mode":{"enum":["all","any"]},
  "timeout_secs":{"type":"integer","minimum":1,"maximum":600,"description":"Default 60."}
}}
```

Precise return rule: a run is "settled for this wait" when its state is `Done(_)` or `Idle`
(an agent that finished its turn is `Idle`, which is non-terminal, so waiting on terminal
states alone would never return for a default continuable agent). The turn-done result of an
`Idle` agent is the same result record a notice carries, and consuming it follows 4.8. Returns,
per run, the `runs(id)` record. On timeout the reply ends with guidance for the model: `Still
running: <ids>. Do not wait again immediately; do other work, you will be notified when they
finish.` It is cancellable by the caller's own cancel token; a cancel returns without stopping
the runs. `wait` on a `Queued` or `Running` run blocks; the caller releases its capacity slot
while blocked (6.1).

### 3.4 `runs`

```
List your runs and everything below you, or inspect one. Without id: a tree of runs with id,
name, kind, state, age, last activity, cost and a one-line progress, plus unread quiet
notices. With id: the full record, the result (full=true for untruncated), and for shell runs
output since your last read (tail=N lines, from_offset=B to page; output is read from the log
file). Reading a finished run's result marks it received, so its notice is dropped.
```

```json
{"type":"object","properties":{
  "id":{"type":"string"},
  "scope":{"enum":["children","descendants","all"],"description":"No id only. Default children. all includes siblings and ancestors."},
  "full":{"type":"boolean"},
  "tail":{"type":"integer","minimum":1,"maximum":200,"description":"shell: last N lines. Default 40."},
  "from_offset":{"type":"integer","minimum":0,"description":"shell: byte offset in the log; the reply gives next_offset."},
  "include_finished":{"type":"boolean","description":"List only. Default true, finished runs retained for the session."}
}}
```

Output cursors are per reader: the log cursor is keyed by the reading run's id, so two agents
tailing one shell each get their own "since your last read". Observers such as the UI log
viewer use non-consuming reads (explicit `from_offset`, no cursor advance, no notice
consumption). Every `runs` call lists unread quiet notices at the top, and committing the call
consumes them (4.8).

List output example:

```
main (you)
├─ ag-k3x9ab auditor      agent  running  2m10s  $0.12  Grep for token handling in src/auth
│  └─ ag-p8d2ne scanner   agent  idle     1m02s  $0.03  replied 40s ago
├─ sh-4m2q7z build        shell  running  5m01s  cargo build --release (last: Compiling maki-ui)
└─ ag-9wq0lc docs-check   agent  done     3m     $0.08  result received
```

### 3.5 `stop`

```
Stop runs. Agents are cancelled at their next safe point and their children are stopped
too (detach=true leaves children running). With soft=true an agent only abandons its current
turn: it stays idle, keeps its transcript and inbox, and can be messaged again. Shells get
SIGTERM, then SIGKILL after 5 seconds. Returns once the runs have settled or after wait_secs.
A stopped agent keeps its transcript and is read with runs; you will not get a notice for
runs you stopped.
```

```json
{"type":"object","required":["ids"],"properties":{
  "ids":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":16},
  "reason":{"type":"string","description":"Forwarded to the run as its last message."},
  "detach":{"type":"boolean"},
  "soft":{"type":"boolean","description":"Agents only. Cancel the current turn, stay idle, keep the inbox."},
  "wait_secs":{"type":"integer","minimum":0,"maximum":30,"description":"Default 10."}
}}
```

Outcome per id: `stopped` / `interrupted` (soft) / `already_finished(state)` /
`still_stopping`. Unlike dsh `interrupt_agent` (always `accepted:true`, no cascade) the answer
is truthful.

Reason about not adding more tools: peek is `runs(id)`, message status is `runs(id)` (shows
`inbox: 2 pending, last delivered m-17 at step 4`), progress reporting by an agent is
`send(to="parent", mode="quiet")`.

### 3.6 Per-turn identity and topology snippet

Delivered as a context update through `tell_changes` (`run.rs:460`), diffed against what the
history `told()` (`agent/history.rs:85`), so it is emitted only when it changes and persists
across resume. The fact lives in `LiveFacts` (`maki-storage/src/frame.rs:29`, today `plan` and
`prompt`). Layering: `maki-storage` cannot name maki-agent types, so the fact uses plain
`String`/`u8` (or the id newtypes move to storage next to `MakiId`):

```rust
pub struct LiveFacts {
    pub plan: Option<PathBuf>,
    pub prompt: Option<PromptFacts>,
    pub topology: Option<TopologyFacts>,   // None when the hub has no runs and caller is main
}
pub struct TopologyFacts {                 // sorted, small, no timestamps, costs or states
    pub me: (String /*handle*/, String /*name*/, u8 /*depth*/),
    pub parent: Option<(String, String)>,
    pub children: Vec<(String, String, u8 /*kind tag*/)>,
    pub limits: (u8 /*max depth*/, bool /*siblings enabled*/),
}
```

Parent and children only: siblings are not in the fact (they would make every spawn re-tell
all n siblings, O(n^2) tokens); a model finds siblings on demand with `runs scope=all`, and
they are addressable only when `siblings = true`. Free slots are not in the fact either (they
change constantly); `runs` shows them. States are not in the fact: they come from notices and
`runs`. A child being created or ending changes the child list, a legitimate re-tell, and
re-tells are coalesced: at most one topology update per drain (4.2), however many runs changed
since the last one.

Touch points for a new fact (all must change together): `LiveFacts`, `FactsUpdate` (`frame.rs:48`),
`LiveFacts::apply` (`:71`) and `changes_to` (`:100`), `facts_at_start` (`:149`, which hard-codes
`plan: None`), `Message::context_update` / `FactsUpdate` in
`maki-providers/src/types.rs:407` and the storage-format test `context_update_keeps_its_stored_format`
(`:1383`), and `context_update` in `maki-agent/src/agent/frame.rs:253` (the renderer; it is in
maki-agent, not maki-storage). `maki-providers` is therefore in the file list of phase 4d.

The real work for subagents: they are built with `RunContext::fixed(system, tools)`
(`agent/frame.rs:83`, used at `maki-lua/src/api/agent.rs:969-970`), which has `facts: None`.
`tell_changes` already runs for them (`run.rs:400`, `:533`), but `History::told()` returns `None`
without a frame, so no update is ever pushed. Native agents must get a facts-bearing frame
instead of `fixed()`: a frame constructor taking the topology as initial facts, so `told()`
returns `Some` and `changes_to` yields topology updates. Cache note: the tool descriptions
are static frame text, so adding the five tools busts the cache once per upgrade, never per
spawn.

Rendered text for a subagent:

```
Run topology changed. You are agent "auditor" (ag-k3x9ab), depth 1 of 2. Your parent is
"main". You have no children. Use send(to="parent") for a question or progress while you
work; your result is delivered to your parent automatically when you finish, so do not send
it. Sending does not end your turn. Messages to siblings are not enabled.
```

For the main agent:

```
Run topology changed. You are "main". Runs you started: "auditor" (ag-k3x9ab, agent),
"build" (sh-4m2q7z, shell). Runs finish with a notice; do not poll. Maximum depth 2.
```

At the depth cap the text adds: `You are at the maximum depth: spawn(kind="agent") will fail
with DEPTH_LIMIT; do the work directly.` The tool stays in the list with a static note
(5.2). A child's first prompt also carries the identity paragraph, so the contract exists even
if the first `tell_changes` is skipped.

### 3.7 Settlement notice wording

Rendered in the owner's `<run-notice>` wrapper. Variable part by state and reason:

```
Run ag-k3x9ab "auditor" (agent) finished its turn and is idle; send it a message to
continue. 2m10s, 14 turns, $0.12.                                  (Idle, turn done)
Run ag-k3x9ab "auditor" (agent) finished and will do no further work.   (Done Completed, one_shot)
Run ag-k3x9ab "auditor" was stopped before it finished.           (Killed)
Run ag-k3x9ab "auditor" ran out of room before it finished.       (MaxTokens / MaxTurns)
Run ag-k3x9ab "auditor" timed out after 600s.                     (TimedOut)
Run ag-k3x9ab "auditor" failed: <detail, 200 bytes>.              (Failed)
Run ag-k3x9ab "auditor" declined the task.                        (Refused)
Run ag-k3x9ab "auditor" was lost: the session ended while it was running; its
transcript up to the last saved turn is available with runs(id).  (Lost)
Run sh-4m2q7z "build" exited 0 after 5m01s.  /  exited 2 ...  /  was killed by SIGTERM.
```

followed by `Result:` and the bounded summary (agents) or `Last output:` (shells, 5 lines),
then optional `Structured: {...}` and `Full result: runs(id="ag-k3x9ab", full=true)` when
truncated or when a log exists. Match notice: `sh-4m2q7z "build" output matched /error/:
<line, 300 bytes> (+3 more since, see runs).` Overflow: `12 more notices from your runs were
dropped to protect your context; call runs() to see state.`

## 4. Delivery semantics

Lock discipline (all of section 4 and 6): the hub lock and each inbox lock are short
`std`-style critical sections. Nothing inside them awaits, performs I/O, calls a provider, or
sends on a bounded channel that can block; journal records, events and wakes are queued to
unbounded channels or a writer task and emitted after the lock is dropped.

### 4.1 Inbox

One `Inbox` per agent run and for `main` (shells have no inbox in v1: stdin `send` is cut,
`NOT_AGENT`, appendix B). Items:

```rust
pub struct InboxItem {
    pub id: MessageId,         // "m-17", per-hub counter
    pub from: UnitId,
    pub kind: DeliveryKind,
    pub text: String,          // <= 16384 bytes
    pub enqueued: Instant,
}
pub enum DeliveryKind {
    Steer,     // next step boundary of a running recipient, wakes an idle one
    Followup,  // after current turn completes; one per turn; wakes an idle one
    Quiet,     // next model request of the recipient (the mid-run drain), never wakes
    Notice,    // a Notice (section 2.6); wake flag decides
}
```

`send` modes map 1:1 onto `Steer|Followup|Quiet`; `Notice` is produced only by the hub. It
replaces the three channels a subagent has today: `SubagentInbox` as an `InterruptSource`
(`inbox.rs`), `maki.session.notify` (`live_session.rs:87`) and the per-prompt interrupt in
`prompt()`. Text typed into a subagent tab becomes `Steer` from the reserved sender `user`.
The existing `SessionMailbox` stays the physical store for `main` (TUI wake path unchanged).

### 4.2 Drain points

An agent drains at exactly these points, in `Agent::turn()` (`run.rs:538`):

1. `land_input` (`run.rs:388`) at run start: `Quiet` + `Steer` + at most one `Followup`.
2. `prepare_request` (`run.rs:514`), before each model call, after the previous tool results
   were pushed: all `Steer`, `Quiet`, `Notice`. This is the mid-run drain: a `quiet` message
   to a running agent lands here, i.e. at the recipient's next model request, not earlier.
   Nothing is inserted between an assistant `tool_use` and its `tool_result` blocks
   (Anthropic and OpenAI reject it); parallel calls (`tool_dispatch.rs:1031`) are joined first.
3. `handle_queued_command` (`run.rs:1087`) after tool results: pending `Steer` continues the
   loop instead of ending the turn.
4. Turn end (`keep_going`, `run.rs:742`): pending `Steer` or `Followup` continues the run; one
   `Followup` starts the next turn. A turn never ends with pending Steer.

Drained items become ONE user-role message ordered by `(kind rank Steer < Notice < Quiet, then
seq)`, each framed `<run-message from="auditor" id="m-17" delivered_step="4">text</run-message>`;
notices keep their `<run-notice>` frame. FIFO per (sender, recipient) pair; arrival order
across senders. A message always lands after a tool result issued in the same step.

Drain cutoff: an item is "queued" until a drain removes it from the inbox under the inbox
lock; from then on it is "drained" (in the outgoing request). The distinction matters for
notice consumption (4.8).

### 4.3 Wake rules

| Recipient state | Steer | Followup | Quiet | Notice(wake) | Notice(quiet) |
|---|---|---|---|---|---|
| running | next step | next turn | next model request | next step | next model request |
| Idle (agent) | start turn | start turn | wait | start turn | wait |
| main, idle, TUI | mailbox wake (`start_mailbox_runs`) | same | wait | same | wait |
| main, idle, headless | `drive_main` starts a run (4.4) | same | wait | same | wait |
| Stopping | dropped, sender told `RECIPIENT_TERMINAL` | dropped | dropped | kept | kept |
| Done | rejected at `send` (revivable runs excepted, 7.4) | rejected | rejected | n/a | n/a |

A waking item to a recipient whose current run is already cancelled is demoted to `Followup`.

Wake budget (default on, `main` only): at most 3 consecutive notice-initiated turns; the 4th
and later notices are delivered quiet and one `Overflow` marker says so. The counter resets
when a user message lands and when the model itself reads state (`wait` or `runs` called by
the main agent), because that proves it is engaged. Quiet notices are never stranded: every
`runs` call lists unread quiet notices; the next turn's drain (4.2 point 1) carries them; in
headless `join()` drains all remaining quiet notices as a closing turn (4.4). There is no
peer-wake budget: agent-to-agent loops are bounded by the per-pair rate limit and the
ping-pong guard (4.7).

### 4.4 Driver loops (subagents and headless main)

Subagent mailboxes never wake today (`land_notes`, `run.rs:409`, drains only inside a turn). A
native agent owns a driver:

```rust
// runs/agent.rs
async fn drive(self) {
    loop {
        self.inbox.wait_wakeable().await;           // Steer|Followup|wake-Notice, or stop
        if stop { break }
        let (input, seq) = self.inbox.take_start(); // 4.2 point 1; seq = inbox counter now
        let outcome = Agent::new(self.params()).run(input, &mut *self.history.lock().await, ..).await;
        let mut g = self.inbox.lock();              // settle under the inbox lock
        if g.seq != seq || g.has_wakeable() { continue }   // arrived while run returned: not lost
        self.settle_or_idle(&mut g, outcome);       // state set in the same critical section
    }
}
```

Race closed: `send` enqueues, bumps `seq` and decides "wake or not" under the same inbox lock
that `settle_or_idle` holds while it moves the state to `Idle`/`Done`. A message arriving
between `agent.run` returning and the settle either bumps `seq` first (the driver loops) or
sees `Idle` and wakes it; no interleaving drops it. The `History` async mutex serialises a
prompt against an inbound message.

`drive_main` is the headless wake consumer (phase 4c): started where the headless frontend
registers its mailbox (`headless.rs:190`, `:339`), it starts a main run when a waking notice
arrives while `main` is idle. `hub.join(timeout)` runs after the final turn: it waits for
non-terminal runs, ignores `Idle` agents with empty inboxes, drains all remaining quiet
notices as one closing turn, and at the timeout stops what is left (`Done(Killed)`, not
silently dropped). Parent registration: `spawn` inserts the child into the parent's children
index in the same critical section that allocates it, before returning, so an idle parent
cannot settle between "child created" and "child visible".

### 4.5 Cancel domains

`AgentLoop::process_run` ends every run with `subagent_cancels.cancel_all()`
(`maki-ui/src/agent/agent_loop.rs:141`) because a `tool_use_id` is only meaningful within its
run. Native runs are not in that `CancelMap`; they hold a child of `UnitHub.cancel`, whose
lifetime is the session. A parent's `stop` and the cascade call the child's trigger. Esc on the
main run cancels the main turn only. Esc in a run's tab, `/stop <name>` and quit call
`hub.stop`. Legacy Lua `task` runs keep the old per-run map.

### 4.6 Acks

Two stages, visible through `runs(id)`:
1. `accepted`: in the recipient's inbox (the `send` result; body journaled, 7.3).
2. `delivered`: drained into the recipient's history at step N (`AgentEvent::RunMessageDelivered`).

There is no `acted` stage: it proved nothing a runtime can attest. The sender gets a `Quiet`
notice only when delivery takes longer than 60 s or the message is dropped (`message m-17 to
ag-k3x9ab was dropped: recipient stopped`); otherwise the stages are queryable and cost no
context.

### 4.7 Caps and rate limits (defaults)

| Limit | Default |
|---|---|
| Message size | 16384 bytes, hard reject `TOO_LARGE` |
| Pending items per inbox | 32, `INBOX_FULL retry_after` |
| Pending Followups per inbox | 8 |
| Notice text | 4096 bytes (3072 head, 768 tail) |
| Match notices per shell | 1 per second, then batched; 20 per minute, then `Overflow` |
| Messages per sender to one recipient | 6 per 10 s, token bucket, burst 6 |
| Drained bytes per step into one request | 32 KiB, rest stays queued (oldest first) |
| Main notice wake budget | 3 consecutive (4.3) |
| Ping-pong guard | 6 consecutive agent-to-agent messages per pair without other tool work |
| `wait` ids | 16; default timeout 60 s; max 600 s |
| `stop` grace | TERM, 5 s, KILL |
| Notice duplicate collapse | identical `(kind, run, text)` within 30 s become `(xN)`; notices only, never messages |

The only rate limits are the per-pair bucket, the inbox cap and the main wake budget; there is
no per-sender total limit and no peer-wake budget. `session_max_runs` is the only spawn
backstop.

Ping-pong guard: per pair `{a,b}` the hub counts consecutive agent-to-agent messages with no
intervening user message, no `main` message to either, and no tool call by either side other
than `send`, `runs`, `wait`. After 6, further sends between them are held (up to 4 per pair,
else `RATE_LIMITED`), the parent of the sender (or `main`) gets one `Quiet` notice `a and b
exchanged 6 messages without doing other work; further messages are held`, and the held
messages release when the counter resets (any other tool call by either, or a message from the
parent). The guard is mandatory when `siblings = true`; it also covers parent/child pairs.

### 4.8 Notice consumption and suppression

A settlement or match notice is suppressed when: the owner called `stop` on the run (truthful
reply already delivered); a read consumed it (below); `notify=never`; the owner is gone or the
hub is shutting down. Idle expiry and `max_idle` eviction are silent and never create one.

Consumption is by commit, not by call: a `wait`, `runs` or `stop` read takes a `ConsumeClaim`
on the notice when it returns the result; the claim becomes consumption only when the tool
result carrying it is committed to the reader's history. If the call is cancelled or errors
before commit, the claim is released and the notice stays. A timed-out `wait` claims nothing.
Cutoff against drains, all under the owner's inbox lock: a claim finalised while the notice is
still queued removes it from the inbox; a notice already drained cannot be cancelled, and the
read still returns the result (the model may see it twice, which is harmless and never loses
it). The check-and-enqueue of a new notice and of a claim is one critical section, closing the
race where a `wait` timing out in the same tick as settlement loses the notice (dsh weakness
12).

## 5. Native-tool mechanism and wiring

### 5.1 New `ToolSource::Native`

`ToolSource` is `{Mcp, Lua}` (`maki-agent/src/tools/registry.rs:81`); every built-in today is
a bundled Lua plugin, and the only `Tool` impls are `LuaTool` (`maki-lua/src/api/tool.rs:217`)
and test mocks. Decision: add

```rust
pub enum ToolSource { Mcp { server: Arc<str> }, Lua { plugin: Arc<str> }, Native }
```

and implement `Tool` (`registry.rs:235`) and `ToolInvocation` (`registry.rs:212`) directly, no
new trait. The dispatcher needs no new route: `Route::Native(entry)` (`tool_dispatch.rs:393`
and `:603`) already routes any registered tool, so the only compile-forced touch point in the
dispatcher path is `ToolSource::as_log_field` (`registry.rs:87`, arm `native`). Other
touch points: docgen `gen_tools.rs`, `is_builtin_tool` (`tools/mod.rs` ~519) and
`ToolFilter::from_config`, `DEFAULT_BUILTINS` in `maki-config`, UI rendering by name/`tool_kind`
in `maki-ui/src/components/tool_display.rs`. Permissions: `ToolKey::Native` already exists
(`permissions.rs`), so `spawn = "allow"` rules work with no new key kind.

Authority: `Tool::reach()` (`registry.rs:249`) defaults to `None`, which every consumer prices
as unbounded authority. Native tools must declare it: `spawn` `ToolReach{permission: Run,
prompted: true}`; `send`, `runs`, `wait`, `stop` `ToolReach{permission: Agent, prompted:
false}`. A test asserts no native tool leaves `reach()` undeclared.

Rejected alternative: expose the hub as a Lua API and keep the tools in Lua. That re-adds the
single Lua thread (5 s hold limit), cannot work headless without the Lua host, and keeps the
plugin `run` permission bypass for shell spawning.

### 5.2 Registration

```rust
pub fn native_run_tools(audience: ToolAudience) -> Vec<Arc<dyn Tool>>;  // spawn, send, runs, wait, stop
```

Tools are stateless `Tool` objects; state comes through `ToolContext`, so one instance serves
all sessions. They are added where the frame's tool list is built, before Lua tools, so a
plugin cannot shadow them. Audiences: `MAIN` and `GENERAL_SUB` get all five; `RESEARCH_SUB`
gets `runs`, `wait`, `send` (no `spawn`, `stop`). Tool defs are part of the frame fingerprint
(`frame.rs:131`), so the set per audience is fixed for a frame's life.

Depth cap (DECIDED): `spawn` stays in the tool list of an agent at `max_depth`; its static
description says "Not available to agents at the maximum depth: you get DEPTH_LIMIT and must
do the work yourself", the call fails in `start` with `DEPTH_LIMIT`, and the topology fact
states the cap (3.6). Why: removing the tool at the cap would give depth-2 agents a different
tool list than depth-1 agents of the same audience, i.e. a second frame fingerprint and a
second prompt-cache prefix per audience, for a rare event; static text keeps one frame per
audience and the cost is a few tokens.

`Tool::parse` returns the invocation; `start_header` renders `spawn auditor`;
`permission_scopes` for `spawn(kind=shell)` returns the command scope computed by a Rust port
of the scope logic in `plugins/bash/init.lua:296` (bash itself is unchanged); everything else
returns no scopes. `execute` receives `&ToolContext` and calls the hub.

### 5.3 ToolContext and AgentParams changes

`ToolContext` (`tools/mod.rs:322`) has no hub or identity. Add one field, `runs:
Option<RunsCtx>`, with `RunsCtx { hub: Arc<UnitHub>, self_run: UnitId }` (`None` for `maki index`
and tests; `MAIN_RUN` for the root agent). About 30 struct literals construct a
`ToolContext` (`run.rs` 2, `tool_dispatch.rs` 15, `live_session.rs` 2, `tools/mod.rs` 7,
`maki-lua/src/api/util/ctx.rs` 3, `maki-lua/tests/code_execution_policy.rs` 1), each gets
`runs: None`; one mechanical step in phase 1.

`AgentParams` (`run.rs:102`) gains `runs: Option<RunsCtx>`; `task_id` stays for legacy Lua
sessions. `Agent::tool_context()` (`run.rs:935`) copies it, and so do nested `batch` and
`code_execution` children (they clone the context). Child agents get `self_run = child id`
and the same hub; router authority checks use `self_run`.

Where the hub is created. TUI: next to `publish_seed` (`maki-ui/src/agent/agent_loop.rs:85`),
stored in `AgentSeed` (`live_session.rs:53`) so `do_agent_run` (`agent_loop.rs:270`) and
`AgentSeed::tool_context` pass it on. Headless builds no `AgentSeed`: `headless.rs:190` and
`:339` only call `SessionMailbox::register`. There the hub is created beside those calls and
handed straight to `AgentParams.runs`, with `drive_main` spawned from the same site. A hub
per root session in both frontends; `AgentSeed` merely carries it for Lua nested calls.

### 5.4 Hooks into AgentLoop and frontends

* Cancel: `process_run`'s `cancel_all` (`agent_loop.rs:141`) is untouched; hub runs are
  outside that map (4.5). Session close and process exit call `hub.shutdown()`: cascade stop
  with a 5 s hard deadline, then force-settle; shutdown never waits for a runner whose cancel
  did not settle.
* Ledger: each agent run gets `RunLedger::child(&parent_ledger)` (`types.rs:1084`), parent =
  the spawning run's ledger; the notice's cost is the child's own total. Relays do not re-add.
* Events: new `AgentEvent` variants `RunStarted`, `RunStateChanged`, `RunNotice`,
  `RunMessageDelivered`, `RunFinished`, carried in `Envelope` (`types.rs:1190`) with
  `SubagentInfo` (`:1121`) stamped for agent runs, `parent_tool_use_id` = the run handle (phase
  3), so tab routing (`maki-ui/src/app/mod.rs:1636`) works unchanged. `SubagentInfo.inbox`
  becomes a handle that `send`s `Steer` from sender `user`. Background envelopes use run_id 0
  and rely on the stamp.
* Permissions: see 6.2; a background agent's `PermissionRequest` carries the stamp and an
  answer channel registered with the hub; headless without a client auto-denies
  (`permissions.rs:755`).
* Routing: `topology.rs` answers `can_send(from, to)`. Default: parent, child and `main`;
  siblings only with `siblings = true`; `stop` and `runs scope=descendants` are limited to the
  caller's subtree; only an ancestor may `stop`. Rejections return the stable codes of 3.2.
* Tests: `MockProvider` and `StubStreamProvider` from the shared `test_support` module
  (phase 1; they are private to `run.rs` today); shell runs with `sh -c`; an injected clock
  for rate limits and wake budgets, so no sleeps.

## 6. Concurrency, limits and permissions

### 6.1 Capacity pool and queue

One `UnitLimits` per hub, loaded from `[agent.runs]` in `maki-config` (mirror types there,
e.g. `UnattendedMode`, because maki-config cannot name maki-agent types; maki-agent converts),
so docgen emits it. Defaults:

| Limit | Default | Notes |
|---|---|---|
| `max_agents` | 4 | agents in state `Running` per session tree (idle agents hold no slot) |
| `max_shells` | 8 | running shells per session tree |
| `max_shells_per_owner` | 4 | one agent cannot take all shell slots |
| `max_depth` | 2 | main = 0, so main spawns depth 1, which may spawn depth 2, which cannot spawn agents |
| `max_children_per_run` | 8 | direct, non-terminal |
| `max_queue` | 16 | queued runs across the tree; the 17th `spawn` returns `QUEUE_FULL` |
| `max_retained` | 64 | terminal runs kept in the table; older ones become tombstones (7.4) |
| `max_idle` | 16 | idle agents in memory; the least recently active leaves memory silently |
| `idle_timeout` | 30 min | idle agent leaves memory silently (no notice, 7.4) |
| `default_timeout` | 30 min | wall clock for every agent unless `spawn.timeout_secs` says otherwise |
| `session_max_runs` | 64 | the only spawn backstop (6.3) |
| `siblings` | false | sibling `send` (4.7) |

Only `Running` agents consume an agent slot. Capacity gates execution, not existence
(dsh's `ACTIVATION_LIMIT_REACHED` is an error; here excess spawns queue). Admission is FIFO.
Deadlock avoidance has no special rule: a run blocked in `wait`, or awaiting a permission
prompt, releases its slot and re-acquires it on resume with queue priority (ahead of
never-started queued runs). So eight parents each waiting on a queued child cannot starve
each other. Slot release, promotion and re-acquire are one critical section under the hub
lock; nothing in it awaits or does I/O (journal and events are sent over channels after the
lock is dropped). Queue position is shown in the `spawn` result and `runs`. A queued run
that is stopped settles `Done(Killed)` without starting.

Provider concurrency is separate: the existing per-provider retry handling stays, and a user
who hits provider 429s lowers `max_agents`.

### 6.2 Permissions for background agents

Problem: `PermissionManager::enforce` (`permissions.rs:755`) emits
`AgentEvent::PermissionRequest` and waits on `user_response_rx`; with `None` it
auto-denies with `UNATTENDED_REASON` (`permissions.rs:830-841`). A background agent that
silently auto-denies every edit or bash call is useless, and one that blocks forever
on an unseen prompt is worse. Decision: route, with a policy fallback.

Routing (attended frontends: TUI, ACP with a client):
1. A native agent always has a response channel: the hub gives each agent run a
   `PermissionBroker` handle in place of `user_response_rx`. `enforce` is unchanged; the
   broker is the receiver end.
2. The `PermissionRequest` event is stamped with `SubagentInfo` and the run name. The TUI
   queues it in the existing permission prompt queue (one prompt visible at a time,
   FIFO across runs) with a header `auditor (ag-k3x9ab) wants to run: <scope>`.
   Answering is by request id (already the key). Allow-always writes the same rule
   files as today; it applies to every run in the session.
3. ACP: the same request goes to the client through `maki-acp/src/permissions.rs`
   (`permission_options`), with the run name in the title.
4. The tool call of that run (only that run) waits; siblings and main continue, and the run
   releases its capacity slot while it waits and re-acquires it on resume (6.1). Wait is
   bounded by `permission_timeout` (default 120 s). On timeout the call is denied with
   `permission prompt for <scope> timed out after 120s; ask your parent via send if
   you need it`, and the run continues, so it can report instead of stalling.
5. Notice side effect: when a prompt has been pending 60 s while the user's main
   run is idle, the owner gets one `Quiet` notice `ag-k3x9ab is waiting for permission:
   <scope>` (never wake). So a main agent can `send` guidance, and the status line shows
   a badge (7.2).

Policy fallback (unattended: `maki -p`, SDK without a handler, or a prompt timing out):
`[agent.runs] unattended_permissions = "deny" | "inherit_yolo"`. Default `deny` with
the existing text, but the deny text for agents is extended with "report this to your
parent with send". `maki -p --yolo` already means allow; it is inherited by runs
because they share `PermissionManager` (the seed carries it). Rules in `permissions.toml`
apply identically to every run; no separate per-agent policy in v1.

Why not auto-deny only: it makes `general` subagents unable to do the job they exist
for the moment a rule is missing, and the main agent cannot see why. Why not inherit
silently-allow: a subagent has no human context and the `spawn` call (which the user
approved) cannot know its future commands. Why not block with no timeout: a forgotten
prompt would pin a slot forever.

Special cases:
* Plan mode: `background_write_block` stays; spawns of `general` agents and shells are
  refused in plan mode (already in 3.1).
* Shell runs: prompt happens at `spawn` time in the spawning turn (3.1, 5.4). Because a
  prompt there blocks the spawning tool call only, nothing new is needed. A shell
  started by a subagent prompts through the broker above.
* `send` to `main` and `runs` never prompt.
* An agent cannot widen permissions: child `PermissionManager` is the parent's, plus
  an optional `tools` filter intersect (never union).

### 6.3 Budgets

The model may set only `timeout_secs` in `spawn`; `max_cost_usd`, `max_turns`, `max_tokens`
are not in the schema (the model cannot raise its own limits). Everything else comes from
config:

| Budget | Default | Effect |
|---|---|---|
| `agent_max_turns` | 100 (existing `MaxTurns`) | `Done(MaxTurns)` |
| agent timeout | 1800 s for every agent (`default_timeout`, or `spawn.timeout_secs`) | `Done(TimedOut)` |
| `session_max_runs` | 64 spawns per session | `SPAWN_LIMIT`; the only backstop (no per-sender total send limit), resets on a new session |
| shell `timeout_secs` | 3600 default, max 86400 | TERM then KILL, `Done(TimedOut)` |
| shell log size | 64 MiB per run | beyond it output is discarded, one marker line; process keeps running |

Subtree cost budgets are post-v1 (appendix B); local models report cost 0 anyway. Each
usage event still rolls up through `RunLedger::child` for display.

### 6.4 Rate limits

`spawn`: at most 10 per rolling 60 s per owner (`spawn_per_minute`), then `RATE_LIMITED retry_after`
(prevents a loop from filling the queue). `send` and wake limits are in 4.7. Rate
limiters take an injected `Clock` trait (`runs/clock.rs`: `fn now(&self) -> Instant`;
tests use a manual clock), so no test sleeps.

## 7. Lifecycle, persistence and reconciliation

### 7.1 Cancellation tree

The tree is the ownership tree: `main -> children -> grandchildren`. Each run holds a
`CancelToken` that is a `child()` of its parent's token (`cancel.rs`), the root being
`UnitHub.cancel`. Cancelling a node cancels all descendants automatically, but the hub
does not rely on token propagation alone: `stop` walks the tree explicitly so state
transitions (`Stopping` then terminal) and notices are produced per run.

Cascade order is child-first (leaves, then up):
1. Mark every run in the subtree `Stopping` (one lock, so no new spawn can attach to a
   stopping parent: `spawn` under a `Stopping` run returns `RECIPIENT_TERMINAL`).
2. Queued runs settle `killed` immediately.
3. Agents: fire the cancel token. The run settles at its next safe point (cancel
   check at `turn()` start, between tool calls, inside streaming); tools in flight
   receive the same token. `finish()` (`run.rs:368`) sanitizes history
   (`sanitize_cancelled_history`), so the transcript is valid for cold resume.
4. Shells: `killpg(SIGTERM)` to the process group (spawn with `setsid`), 5 s grace,
   then `killpg(SIGKILL)`. The pump task drains remaining output to the log, then the
   exit is recorded. Non-unix: `TerminateProcess` with no grace (job objects are post-v1).
5. Hard deadline: `stop` returns after `wait_secs` (default 10, max 30) with
   `still_stopping` per unsettled run. The hub's per-run settle watchdog runs
   independently: 5 s after the cancel token fires (15 s for an agent mid-tool), a
   run that has not settled is force-settled `killed` with detail `forced`, its runner
   task is dropped (smol task drop cancels it), and its shell tree is KILLed. This
   fixes the dsh stall where a runner ignoring cancel blocked shutdown.
6. Parent settles after its children (child-first). A parent's settlement notice
   (suppressed if it was stopped by its owner) is produced only when all descendants
   are terminal, so a notice never reports a finished parent with live children.

`stop(detach=true)` removes the child link only for agents: the children get their
`parent` set to the stopper's `owner` ("re-parent to grandparent"), which is the one
exception to "never re-parented in v1" in 2.3, documented there as a stop-only rule.

Soft interrupt: `stop(soft=true)` on an agent cancels only its current turn (the cancel
token of that turn, not the run's), the agent settles to `Idle` with its transcript
sanitized and its inbox kept, and it is not a terminal state. Use it to redirect a runaway
turn without losing the agent; a following `send` starts the next turn. Soft stop on a
shell is rejected (`NOT_AGENT`). Hard `stop` keeps the cascade semantics above.

### 7.2 What user and process events do

| Event | Effect on runs |
|---|---|
| Esc on the main run | cancels the main turn only. Background runs continue. They notify when done and the main agent wakes per the wake budget. |
| Esc in a run's tab | `hub.stop(run, cascade)`; agent tab only; Esc again = hard `forced` settle immediately |
| `/stop <name\|all>` (new command, 8.1) | `hub.stop`; `all` stops every run owned by the session root |
| `/new` | the old session's hub does `shutdown()` (cascade stop, 5 s deadline), then a fresh hub with a fresh root token. Runs do not migrate sessions. |
| session switch (`/resume`, session picker) | same as `/new` for the session being left; its runs settle `Done(Killed)` (detail `session_closed`) so the journal is clean and a resume shows them stopped, not lost |
| `/reload` (Lua plugins reload) | no effect on native runs; they are not owned by the Lua host. This is a main reason shells are native (today `JobStore` Drop kills them on reload). Lua-owned `maki.fn.job*` still die as before. |
| `/compact` | no effect on runs; the main agent compacts its own history |
| config reload | new limits apply to future admissions only |
| quit (`Ctrl-C` twice, `/quit`) | `shutdown()` with 5 s deadline. Agents `killed`, shells TERM/KILL. |
| `maki -p` finish | `hub.join()` waits for non-terminal runs, ignoring Idle agents with empty inboxes, up to the join timeout (600 s, `--join-timeout` in 7b); then `shutdown()`. `-p` exit code is unchanged by child failure. |
| crash / SIGKILL / power loss | nothing runs; journal replay at next start (7.3) |
| SIGTERM / SIGHUP to maki | handled as quit; SIGKILL cannot be handled |

Shell orphans: `spawn(kind="shell")` has no `detach` field (runs do not survive the process).
Shells run in their own process group; the `<handle>.pid` file records pgid and process start
time so that after a crash the next start kills the orphan only if the start time still
matches (7.3). There is no re-adoption (decided); output logs are kept.

### 7.3 Journal and `lost` reconciliation

Location: `<state_dir>/runs/<session_id>/` (`maki-storage/src/paths.rs:284` `state_dir()`),
created `0700`:

```
journal.ndjson          append-only event log (durable source for the run table)
messages.ndjson         message bodies (<= 16 KiB each), so messages to a crashed child survive
<handle>.log            shell merged output, append-only
<handle>.pid            pgid and process start time of a live shell (orphan cleanup)
<handle>.result.txt     spill of a truncated agent result (2.6)
```

The `RunJournal` lives in `maki-storage/src/runs.rs` (leaf crate; `SessionRef` is a storage
type, `maki-storage/src/id.rs:109`, so `RunJournal::open(&SessionRef)` is layering-safe) so the
session delete path can remove it. Records are one JSON object per line, forward compatible
(`#[serde(default)]`, unknown `t` ignored). Because it lives in storage, records use plain
`String`/`u8` ids, never maki-agent types:

```json
{"v":1,"t":"spawn","ts":1760000000123,"id":"<MakiId>","handle":"ag-k3x9ab","name":"auditor","kind":"agent","owner":"<id>","parent":"<id>","depth":1,"desc":"...","origin_tool_use_id":"toolu_01","notify":"wake","spec":{"model":"...","tools":["..."],"audience":"general_sub","thinking":"...","output_schema":null,"one_shot":false,"timeout_secs":1800}}
{"v":1,"t":"state","ts":...,"id":"...","state":"running"}
{"v":1,"t":"msg","ts":...,"id":"m-17","from":"...","to":"...","kind":"steer","bytes":420,"stage":"accepted"}
{"v":1,"t":"ack","ts":...,"id":"m-17","stage":"delivered","step":4}
{"v":1,"t":"notice","ts":...,"seq":12,"run":"...","kind":"settled","delivered":false,"consumed":false}
{"v":1,"t":"term","ts":...,"id":"...","reason":"completed","exit_code":0,"cost":0.12,"turns":14,"elapsed_ms":130000,"result_len":812,"body_ref":null}
{"v":1,"t":"mark","ts":...,"reason":"clean_shutdown"}
```

Rules:
* Versioning: first line `{"v":1,"t":"hdr","maki":"0.6.4"}`. Readers accept `v <= 1`; a higher
  `v` makes the file read-only for this binary (runs listed `lost`, nothing rewritten).
* Lock discipline: journal writes happen off the hub lock, through a channel to a writer task;
  `fsync` on `spawn`, `term` and `mark`, other records flushed every 250 ms and at shutdown.
  A torn last line is ignored on replay. Journal write failure degrades to memory-only with
  one warning notice and never fails a spawn.
* Message bodies go to `messages.ndjson` (separate file, same torn-line rules), up to 16 KiB
  each, written before `send` returns `accepted`; bodies of delivered messages are dead weight
  and dropped at compaction. Pending bodies are redelivered after a crash (cold revive,
  7.4).
* Spec: `spawn` persists the full `AgentSpec` (model, tool filter, schema, audience,
  thinking, timeout) so cold revive rebuilds the same agent.
* Growth cap while live: when `journal.ndjson` exceeds 4 MiB the writer task compacts it
  (write temp, rename, drop terminal runs older than 24 h and delivered message bodies),
  and also at session start. Compaction runs in the writer task, not under the hub lock.
* Shell log retention: each log is capped at 64 MiB (2.6, 6.3); at session start, logs of
  terminal runs older than 7 days, and the oldest logs beyond 512 MiB total per session, are
  deleted; session delete removes the directory.
* Transcripts are written incrementally per turn (7.4).

Reconciliation (`UnitHub::reconcile`, constructor of a resumed session, before any spawn):
1. Replay the journal. Runs whose last state is non-terminal and not followed by a clean mark
   are suspect; a non-terminal run despite a clean mark is a bug and treated the same.
2. Suspect agent: `Done(Lost)`, transcript untouched.
3. Suspect shell with a `.pid` file: if the process exists and its start time matches
   (`/proc/<pid>/stat` field 22; elsewhere pid only and treated as gone), it is an orphan:
   `killpg(SIGTERM)`, 5 s, `SIGKILL`, then `Done(Lost)` (detail `orphan killed`); else
   `Done(Lost)` (`process gone`). No re-adoption.
4. Append `term` records so replay is idempotent.
5. Undelivered, unconsumed `notice` records plus one `Lost` notice per newly lost run queue
   for `main`, wake flag per original policy, subject to the wake budget, delivered on the
   first `claim_wake` after load (TUI) or at hub start (headless). The lost wording (3.7)
   says what to do: `runs(id)`, `send` to cold-revive, or spawn a new one.
6. Pending messages (`accepted`, never `delivered`) keep their bodies in `messages.ndjson`;
   they are redelivered on revive and the lost notice says "N unread messages".

A session without a journal (written before this feature) reconciles as a no-op and
`StoredSubagent` keeps restoring read-only tabs.

### 7.4 Persistence of agents, cold revive

Transcripts reuse `Session.subagent_messages` (`maki-storage/src/sessions.rs:423`) and
`subagents: Vec<StoredSubagent>` (`:640`). The key becomes the run handle for native agents;
`StoredSubagent` gains `run: Option<StoredRun>` (handle, name, parent handle, kind tag, last
state, depth, one_shot), `#[serde(default, skip_serializing_if)]`; old binaries ignore it and
old sessions load with `None`. Writes use the incremental path (`saved_sub_msg_counts`,
`sessions.rs:1088`): the hub emits `AgentEvent::SubagentHistory` at each turn end, appending
only the new messages, so a crash loses at most the current turn. Not persisted in the
session: inboxes, buckets, wake counters, waiters, live processes (the journal covers
notices and delivery).

Revivability. `send` to a `Done` agent (`Completed`, `Lost`, `Killed`, `Failed`, `TimedOut`
from an earlier process) or one evicted from memory is accepted when its transcript exists.
`UnitHub::revive(id)` loads `subagent_messages[handle]`, rebuilds the runner from the persisted
`AgentSpec` (same `UnitId`, handle, name, owner), sets `Running` (`Queued` if no slot), and
delivers the message as its first `Steer` together with any pending journaled messages. Not
revivable (`RECIPIENT_TERMINAL`): shells, `one_shot` agents that completed, a `Failed` agent
whose transcript cannot be repaired by sanitize.

Eviction. Idle agents beyond `max_idle` and `Idle` expiry (`idle_timeout`) leave memory
silently: no settlement notice, because Idle never produced one; the agent becomes
`Done(Completed)` with its transcript kept. Terminal runs beyond `max_retained` are evicted
from the table but keep a tombstone (handle, name, owner, kind, last reason, transcript
pointer, spec) in the journal and a one-line entry in `runs include_finished`, so revive by
handle or name still works; tombstones are dropped at session delete or compaction after 24 h
only when the transcript no longer exists. Topology facts are re-told on revive; the system
prompt and tool frame are rebuilt for the audience (one possible cache miss, accepted).

## 8. UI and observability

### 8.1 TUI

* Tabs: unchanged model. `RunStarted` with `SubagentInfo` creates a chat tab through
  `deliver`/`resolve_or_create_chat` (`maki-ui/src/app/mod.rs:1636`), keyed by the run
  handle carried in `SubagentInfo.parent_tool_use_id` (5.4, phase 3). The tab title is the run name. Shell runs get no
  chat tab; they appear in `/runs` and the status line only, with a log viewer
  (below).
* `/runs` (picker, named `/runs` so it does not clash with the Lua `/tasks`): implemented in Rust in `maki-ui` over a `RunsView` kept in
  `App`, updated from `RunStateChanged`. Rows: tree indent, name, handle, kind, state
  glyph, age, cost, progress. Keys: Enter focus the tab (agents) or open the shell log
  viewer, `s` stop, `x` stop and forget, `r` resume prompt for finished agents. Sits beside
  the legacy Lua `task` picker (unchanged in v1); the outcome-transition logic of `diff_task_states`
  (`maki-ui/src/app/tasks.rs:184`) is ported onto `UnitState`.
* Status line: a segment `runs 3 running 1 queued` hidden at zero, plus a `!` badge
  when a run waits for permission, and `lost` count after a resume.
* Notices render as one collapsed row in the main transcript: `<run-notice>` frames are
  detected (the UI recognises the wrapper, 2.6) and shown as `auditor finished (2m10s,
  $0.12)`, expandable to the bounded text. `RunMessageDelivered` shows a dim
  `delivered` mark on the send tool row. A `send` from `user` (typing in a tab) is
  `Steer`.
* Shell log viewer: a read-only transient panel tailing `<handle>.log` by offset; no
  model tokens involved.
* Mailbox wake (`start_mailbox_runs`, `event_loop.rs:1189`): unchanged for `main`;
  `RunNotice(wake)` calls `SessionMailbox::notify(main, text, wake)`, honouring the wake
  budget in the hub before the call.
* Stale-run drop (`event_loop.rs:246`): hub events use run_id 0 plus the stamp, as today.

### 8.2 Headless, ACP, SDK

* `maki -p`: `headless.rs:163 spawn` and `:307 spawn_interactive` start `drive_main`
  (4.4, phase 4c). Run transitions print on stderr as one line each
  (`[run ag-k3x9ab auditor completed 2m10s]`) unless `--quiet-runs`; JSON output (`print.rs`)
  adds `run_*` objects; `join` per 4.4; unattended permissions follow 6.2 (phase 7b).
* ACP (`maki-acp`): permission requests carry the run name (phase 5b). Mapping run state to
  `tool_call_update` is post-v1 (appendix B); ACP exposes no run tree in v1.
* SDK (`sdk_mode.rs`): events serialised with the existing envelope plus the new variants;
  additive to the wire format.
* Logging: every transition is a `tracing` event with `run`, `handle`, `owner`, `state`;
  otel spans per agent run are children of the spawning tool span.

### 8.3 Lua API relation

Lua is left as is in v1; the native tools coexist with the Lua `task` tool until a separate
decision (appendix B).

| API | v1 decision |
|---|---|
| `maki.agent.session`, `maki.task.*`, `maki.session.notify`, `maki.fn.job*`, bundled `plugins/task` | Unchanged. The legacy `task` tool and native `spawn` coexist; users may hide one with `disabled_tools`. `plugins.task.*` config keeps working. |
| `maki.run.*` | Post-v1 (appendix B). |
| `maki-monitor` | External, not in this repo; not touched. Docs carry a migration table `monitor_start -> spawn(kind=shell, watch=...)` once phase 2b lands. |

### 8.4 Docs and docgen

`maki-docgen/src/main.rs` PAGES (8 pages, content root `site/docs/content`, `:17`; `docs/`
holds only `docs/design`). Each phase regenerates with `cargo run -p maki-docgen` and commits
the output in the same commit, because `--check` is a CI gate: phase 1 (tools via
`gen_tools.rs` showing `ToolSource::Native`, `[agent.runs]` on the configuration page),
5a (full `[agent.runs]`), 7a (commands `/stop`, keybindings). The hand-written agents page
under `site/docs/content/` lands in 7b.

## 9. Phased implementation plan

Common rules for every phase: strict sequence (a phase assumes only the earlier ones
are merged); no sleeps in tests (injected `Clock`, flume channels, `smol::block_on`);
AGENTS.md style (no trivial comments, constants at top, `test_case`, deps only through
the workspace `Cargo.toml`); one commit per phase, conventional prefix (`feat:`, 2-5
sentences, plain prose, no em-dashes), no push. Regression gates after each phase (see
Appendix A for the toolchain):

```
cargo fmt --all -- --check
cargo clippy --all --tests -- -D warnings
cargo nextest run --workspace
cargo run -p maki-docgen -- --check
```

Live e2e: isolated XDG dirs under `/tmp/e2e` (`XDG_CONFIG_HOME/maki/providers.toml`
with `[linken] protocol="openai" base_url="http://ai-models-1.linken.lan/v1"
api_key="none"`), model `linken/ornith-1.5-35b-a3b-cybertiel-mtpv2-ice-21g`, driven
through the PTY runner `/tmp/e2e/run.py`. Never kill processes by name; kill by the PID
the runner prints.

### Kill switches and naming

Each phase is sized for one agent context, ends in a green gate, a commit and a live e2e, and
is guarded by its own key under `[agent.runs]` (all default true once `enabled = true`;
setting one false removes that capability without touching the others):

| Phase | Switch | Effect when false |
|---|---|---|
| 1 | `enabled` | tools not registered (default false until phase 8) |
| 2, 2b, 2c | `shell` | `spawn(kind=shell)` rejected `DISABLED`; no watch; no promotion |
| 3 | `agents` | `spawn(kind=agent)` rejected `DISABLED` |
| 4a | `messaging` | `send` rejected `DISABLED`; no inbox drains |
| 4b | `notices` | settlement visible only through `wait`/`runs`; no wake |
| 4c | `headless_drive` | `-p` exits at the final turn as today |
| 4d | `topology` | no topology fact; `siblings` irrelevant |
| 4e | `acks` | no delivery tracking |
| 5a | `queue` | over-cap spawn rejected `QUEUE_FULL` instead of queued |
| 5b | `route_permissions` | background prompts auto-deny |
| 6a | `journal` | memory-only table, no `lost` reconciliation |
| 6b | `revive` | terminal runs never revive |
| 7a, 7b | `ui` | no `/runs`, status segment, log viewer, run stderr lines |

Naming: the new concept is `Unit` in Rust (`UnitHub`, `UnitId`, `UnitHandle`, `UnitName`,
`UnitState`, `UnitLimits`...), because `Run*` names collide with the per-turn `run_id`
(`Envelope.run_id`, `types.rs:1195`), `RunContext` and `RunLedger`. Model-facing text, the
tool names, `[agent.runs]` and the `runs/` module keep the word "run"; this doc says run.

### Phase 1: native tool path, UnitHub skeleton, ids

Goal: `ToolSource::Native` exists, the five tools are registered and callable, the hub
creates, names and lists runs with no execution (`spawn` records state `Queued` for agent
and shell, `runs` lists it). Behind `enabled` (default false).

Files: `maki-agent/src/runs/{mod,topology,clock,limits}.rs` (new, `limits.rs` holds a
minimal `UnitLimits` with only `max_depth`, `max_children_per_run`, `session_max_runs`
so spawn validation exists; the rest lands in 5a); `maki-agent/src/tools/runs.rs` (new);
`tools/registry.rs:81` (variant) and `:87` `as_log_field` only (`Route::Native` at
`tool_dispatch.rs:393` already routes any registered tool, no `Route` change);
`tools/mod.rs` (`ToolContext` `:322`, `is_builtin_tool`); `agent/run.rs` (`AgentParams`
`:102`, `tool_context` `:935`); `live_session.rs` (`AgentSeed` `:53` field); `maki-config`
(`[agent.runs]` mirror types, `DEFAULT_BUILTINS`); `maki-docgen/src/gen_tools.rs`;
`maki-ui/src/components/tool_display.rs`; headless wiring (5.3).

ToolContext churn: about 30 struct literals (`run.rs` 2, `tool_dispatch.rs` 15,
`live_session.rs` 2, `tools/mod.rs` 7, `maki-lua/src/api/util/ctx.rs` 3,
`maki-lua/tests/code_execution_policy.rs` 1; `plugin_host.rs` only implements `Tool`). The
two new fields go in one `runs: Option<RunsCtx>` so each literal gets one `runs: None`
line; do it as the first mechanical step, compile, then continue.

Test support: `MockProvider` (`run.rs:1239`) and `StubStreamProvider` (`:1293`) are private
under `#[cfg(test)]` in `run.rs`. Step 0: extract them into `maki-agent/src/test_support.rs`
(`#[cfg(any(test, feature = "test-support"))]`, `pub`), used by `runs/` tests and
`maki-agent/tests/runs_soak.rs` (a new dir; integration tests need the feature, enabled
in dev-dependencies). `maki-providers` already has a `test-support` feature for its
recorded server; reuse that flag name.

Interfaces:
```rust
pub struct UnitHub; impl UnitHub {
  pub fn new(limits: UnitLimits, events: EventSender, clock: Arc<dyn Clock>) -> Arc<Self>;
  pub fn spawn(&self, caller: UnitId, req: SpawnRequest) -> Result<SpawnAck, UnitError>;
  pub fn list(&self, caller: UnitId, scope: Scope) -> Vec<UnitView>;
  pub fn resolve(&self, caller: UnitId, key: &str) -> Result<UnitId, UnitError>; // id|handle|name|"main"|"parent"
}
pub fn native_run_tools(audience: ToolAudience) -> Vec<Arc<dyn Tool>>;
pub trait Clock: Send + Sync { fn now(&self) -> Instant; }
pub enum UnitError { UnknownRun, NotReachable, RecipientTerminal, InboxFull{retry_after:u64}, TooLarge{max:usize}, RateLimited{retry_after:u64}, SelfSend, QueueFull, SpawnLimit, DepthLimit, Disabled, BadField{field:&'static str, kind:&'static str} }
```
Steps: (1) the churn and test-support steps above; (2) enum variant, fix compile errors,
`as_log_field` arm `native`; (3) ids, handle collision retry, name slug, reserved
names `main` and `parent`; (4) `UnitTable` with name and children indexes; (5) tool structs
(`parse`, `start_header`, schema and descriptions from section 3, cross-kind field
rejection 3.1) each declaring `reach()` (`registry.rs:249`; `None` means unbounded
authority, so declare explicitly: `spawn` `Permission::Run` prompted, the rest
`Permission::Agent` not prompted); `wait/send/stop` return `NOT_IMPLEMENTED`; (6) config
flag and registration before Lua tools; (7) hub creation and `tool_context` plumbing
(5.3, including headless, which builds no `AgentSeed`); (8) docgen: `gen_tools` shows the
native tools and `[agent.runs]` appears on the configuration page, regenerated in this
commit so `--check` stays green.

Tests: `runs::handle_is_prefixed_and_six_chars`, `runs::name_slug_suffixes_on_clash`,
`runs::reserved_names_rejected`, `runs::resolve_main_parent_name_handle`,
`runs::depth_limit_rejects_agent_spawn`, `tools::runs::schema_matches_snapshot`,
`tools::runs::cross_kind_fields_rejected`, `tools::runs::spawn_returns_queued_record`,
`registry::native_tool_logs_source`, `registry::native_tools_declare_reach`.
E2E: start, ask "call runs with no arguments", expect `no runs`.
Notes from the build: the root id is `hub.main()` (not in the table, depth 0); native tools are registered by `runs::sync_registration` from `load_plugins` (`src/cmd/mod.rs`) before the Lua builtins; the run names count as built-in (MCP server name clash) only while `enabled` is true.
Commit: `feat: add native run tool surface and hub skeleton`.

### Phase 2: native shell jobs (core)

Goal: `spawn(kind=shell)` runs, logs to disk, settles with exit codes, TERM/KILL stop,
`runs(id)` reads output by cursor, `wait` and `stop` truthful. No stdin send in v1, no
`watch` (2b). Settlement notices exist in a hub-local queue; delivery is phase 4b (until
then `wait` and `runs` return them).

Files: `runs/shell.rs`, `runs/notice.rs` (new), `runs/mod.rs`, `tools/runs.rs`,
`maki-storage/src/paths.rs` (runs dir helper, `0700`), `maki-agent/Cargo.toml` (`smol::process`
is in tree; process groups via `std::os::unix::process::CommandExt::process_group`; add no
new dependency without checking the workspace `Cargo.toml`).

Interfaces:
```rust
impl UnitHub {
  pub fn stop(&self, caller: UnitId, ids: &[UnitId], opts: StopOpts) -> Vec<StopOutcome>;
  pub async fn wait(&self, caller: UnitId, ids: &[UnitId], mode: WaitMode, timeout: Duration, cancel: &CancelToken) -> Vec<UnitView>;
  pub fn read_output(&self, reader: UnitId, id: UnitId, from: Option<u64>, tail: usize, consume: bool) -> OutputSlice;
}
pub struct ShellSpec { pub command: String, pub cwd: PathBuf, pub timeout: Duration }
```
Steps: (1) spawn `sh -c` with `process_group(0)`, stdout and stderr into one pipe, pump to
`<handle>.log` capped at 64 MiB; (2) settle on exit with code or signal text; (3) timeout
task on the injected clock; (4) `stop` = TERM, 5 s grace, KILL, `Stopping` then `Done(Killed)`;
(5) `wait` per 3.3 (terminal or Idle) with `WaiterSet` and the consumption rules of 4.8;
(6) per-reader output cursors keyed by reader id (3.4); (7) last 5 lines in the result;
(8) permission scope: `permission_scopes` for bash lives in `plugins/bash/init.lua:296`
(`generalize_bash_segment`, `permissions.rs:1047`, only generalises an allow-always rule
and is not the scope logic); port the minimal scope computation to Rust, do not change
bash; (9) log retention (7.3): size cap per log, age cleanup at session start.

Tests: `shell::exit_zero_completes`, `shell::nonzero_is_failed`, `shell::output_ordered_in_log`,
`shell::stop_sends_term_then_kill_after_grace` (traps TERM, manual clock),
`shell::timeout_marks_timed_out`, `shell::orphan_group_killed`, `shell::log_cap_discards_and_marks`,
`shell::cursors_are_per_reader`, `hub::wait_any_returns_first`,
`hub::wait_timeout_does_not_consume_result`, `hub::wait_cancel_does_not_stop_runs`,
`hub::stop_is_idempotent_and_truthful`.
E2E: "spawn a shell `for i in 1 2 3; do echo $i; sleep 1; done`, wait for it, tell me the
last line" (calls spawn, wait; answer has 3). "spawn `sleep 600`, then stop it": `stopped`,
no `sleep 600` left by the PID the runner prints.
Notes from the build: shells start `Running` at once (capacity is the phase 1 spawn limits; `max_shells` and the queue arrive in 5a), agents stay `Queued` until phase 3. The log dir is `<state>/runs/<session>/` (`maki_storage::paths::runs_dir`, which also prunes: stale logs everywhere, 512 MiB budget for the own session). `Clock` gained `sleep_until` so timeouts and the TERM grace run on the injected clock. `stop` only starts the stop; the tool waits up to `wait_secs` and then reports `stopped` or `still_stopping`. Reads consume notices immediately (the commit-claim of 4.8 comes with delivery in 4b). Anything still in the process group after the shell exits is killed, and dropping the hub kills running shells without grace (a launch in flight is covered by a lock shared with the teardown). A graceful quit (Ctrl-C/Ctrl-D) leaves no shell behind, and so do SIGTERM, SIGHUP and SIGINT outside raw mode and a panic on the main thread: `runs::install_signal_teardown()` (called first in `main`) turns them into a pipe write handled by a `maki-signals` thread that calls `runs::shutdown_all()` (SIGTERM to every live shell group, at most 500 ms, then SIGKILL) and re-raises the signal with the default action; normal and error exit call `shutdown_all()` too. `UnitHub::shutdown()` does the same for one hub and both are synchronous, idempotent and need no runtime; a process-wide set of live groups lets the signal thread work without a hub, and new launches are refused once shutdown began. SIGKILL and abort cannot run code, so shells still outlive maki there until the pid-file reconciliation of phase 6a. `PR_SET_PDEATHSIG` was tested and rejected: it fires when the thread that forked the child exits (verified: a child spawned from a thread that then ended was killed at once), and shells are spawned from smol executor and blocking-pool threads that come and go; it would also reach only the leader, not the rest of the group. Shell logs are `0600` in a `0700` dir. The `spawn` prompt is keyed `spawn`, so `spawn` allow rules apply, not bash rules; scopes come from `runs::shell::command_scopes`, a quote-aware splitter that is stricter than the bash plugin (anything it is unsure about is one forced-prompt scope).
Commit: `feat: native background shell runs`.

### Phase 2b: shell watch and Match notices (maki-monitor replacement)

Goal: `spawn(kind=shell, watch=regex)` turns matching lines into `Match` notices, shipped
right after core shells so `maki-monitor` users can migrate.
Files: `runs/shell.rs`, `runs/notice.rs`, `tools/runs.rs` (`watch` field), docs migration
table. Steps: regex compile errors returned at `spawn`; 1 match notice per second then
batched, 20 per minute then `Overflow`; matches coalesce per drain; notices are quiet
unless `notify=wake`. Tests: `shell::match_notices_rate_limited`, `shell::bad_regex_rejected`,
`shell::match_batches_after_first_per_second`. E2E: spawn a loop echoing `error` lines with
`watch=error`; `runs` shows the batched matches.
Commit: `feat: shell watch notices`.

### Phase 2c: bash foreground-to-background promotion

Goal: a long `cargo build` that hits the bash timeout keeps running as a shell run instead
of being killed. First read how `plugins/bash/init.lua` spawns and times out (bash is a Lua
plugin); if the child can be handed to the hub through a core hook
(`ToolContext.runs.adopt(child, log)`, called from a native `bash_promote` seam in the
tool runner), adopt it: the timeout result says `still running as sh-xxxxxx, use wait or
runs`. If the plugin cannot hand over the process, fall back to a result text hint
("rerun with spawn kind shell") and record promotion as post-v1 (appendix B). Tests:
`shell::adopted_process_gets_handle_and_log`, `bash::timeout_result_names_handle`.
E2E: `bash` with a 3 s timeout and `sleep 20`; the result names the handle and `wait`
finishes it.
Commit: `feat: promote timed-out bash commands to background runs`.

### Phase 3: background agent driver loop

Goal: `spawn(kind=agent)` runs a real subagent that survives the parent turn, reports,
becomes `Idle`; events stream to today's subagent tabs.

Files: `runs/agent.rs` (new), `runs/mod.rs`, `tools/runs.rs`, `agent/run.rs`
(`AgentParams` `self_run`, `runs`; `task_id` untouched), `agent/frame.rs:83` and
`maki-lua/src/api/agent.rs:969-970` (the existing `RunContext::fixed` sites to learn from),
`types.rs` (`AgentEvent::{RunStarted, RunStateChanged, RunFinished}`),
`maki-ui/src/app/mod.rs:1636` and `maki-ui/src/agent/mod.rs` (hub creation), `headless.rs`.

UI spec: `SubagentInfo.parent_tool_use_id` (`types.rs:1122`) keys the tab today. For native
runs the stamp's `parent_tool_use_id` carries the run handle (`ag-k3x9ab`), `name` the run
name; `resolve_or_create_chat` is keyed by that string, so tabs, `/runs` and persistence need
no new key type. The origin `tool_use_id` is rendering metadata (`origin_tool_use_id`).
Legacy Lua `task` keeps real tool_use_ids; the two key spaces cannot collide (`ag-`/`sh-`
prefix vs `toolu_`/`call_`).

Audience tool sets: native agents use `ToolAudience::GENERAL_SUB` or `RESEARCH_SUB`
(`registry.rs:26`); phase 3 registers the five tools for `MAIN` and `GENERAL_SUB` only
(`RESEARCH_SUB` gets `runs`, `wait`, `send`; no `spawn`, `stop`). `send` is a no-op error
until 4a. Tool sets are fixed per frame (5.2).

Interfaces:
```rust
pub(crate) struct AgentRunner { /* history, gauge, inbox, cancel, ledger */ }
impl UnitHub { pub(crate) fn spawn_agent(&self, caller: UnitId, spec: AgentSpec) -> Result<SpawnAck, UnitError>; }
pub struct AgentSpec { pub prompt: String, pub audience: ToolAudience, pub model: ModelChoice, pub tools: Option<ToolFilter>, pub thinking: Option<Thinking>, pub output_schema: Option<Value>, pub one_shot: bool, pub context: ContextMode, pub timeout: Duration }
```
Steps: (1) port the `AgentParams` construction from `maki-lua/src/api/agent.rs:731`
into `AgentRunner::params` (move `resolve_model` into maki-agent, Lua calls it); (2)
child cancel token from `hub.cancel`, `RunLedger::child`; (3) drive loop 4.4 with only the
initial prompt; (4) `structured_output` local tool; (5) result capture with 4 KiB bound and
spill; (6) `Idle` after the first result unless `one_shot`; (7) port
`relay_session_events` (`maki-lua/src/api/agent.rs:270`) stamping `SubagentInfo`; (8) UI tab
creation on `RunStarted`; (9) permission broker with auto-deny only (5b adds routing);
(10) `wait` Idle semantics (3.3), default `timeout_secs` 1800 applied; (11) stop cascade child-first with
notice suppression (7.1, 4.8; test `agent_run::stop_cascades_children_first`); (12) `context=fork`
per 3.1 or cut it if the history snapshot cannot exclude the in-flight turn.

Tests: `agent_run::completes_with_result_and_goes_idle`, `agent_run::one_shot_completes`,
`agent_run::survives_parent_turn_end` (`cancel_all` called, child continues),
`agent_run::cost_rolls_up_once`, `agent_run::structured_output_lands_in_result`,
`agent_run::long_result_spills_to_disk`, `agent_run::stop_settles_killed_and_history_sanitized`,
`agent_run::wait_returns_on_idle`, `agent_run::fork_excludes_inflight_turn`,
`app::native_run_creates_tab_keyed_by_handle`, and the matrix `native_and_legacy_task_enabled`
(both the Lua `task` tool and native `spawn` registered; both complete; tabs do not clash).
E2E: "spawn an agent named reader to list the top-level files and report in one sentence,
then wait for it". Expect a tab; `wait` returns when the agent is Idle; the child tab keeps
progressing after the parent's final text when not waited.
Commit: `feat: native background agent runs`.

Notes from the build: `AgentSpec` is the request as parsed (prompt, subagent type, tier, thinking, schema, `fork`, `one_shot`, timeout) and everything the driver needs from the caller's `ToolContext` is resolved by `AgentLaunch::prepare` in the `spawn` tool before the hub is touched, so a bad schema or tier fails with no run created; the hub's `spawn_agent` takes the prepared launch. The driver (`runs/agent.rs`) owns its `History`, its cancel token is a child of a session-lifetime root on the hub (not in the per-run `CancelMap`, so `cancel_all` after a turn leaves it alone), and its `RunLedger` is a child of the caller's, so cost is added once up the chain. Events of background runs carry `BACKGROUND_RUN_ID` (`types.rs`); without it the UI's stale-turn gate dropped every event once the next turn began. The tab is keyed by the handle through `SubagentInfo.parent_tool_use_id`, opened by the stamped `RunStarted`, and double Esc on it stops the run through the hub (cascade included); `RunFinished` closes it. Run events are not stamped for shells, so they stay on the main chat. `context=fork` is kept: `tool_dispatch` publishes the history as it stood before the in-flight assistant message for every `spawn` call that asks for it, and `spawn_agent` consumes that snapshot by `tool_use_id` (`BAD_FIELD` when none exists). Results are bounded at 4 KiB (3072 head, 768 tail) with the whole text in `<handle>.result.txt` in the run dir, and `runs(id, full=true)` reads it back; `wait` and `runs` print an agent's `result:` and `structured:` lines. A soft stop is an interrupt of the turn that lands in `Idle`; a hard stop cancels the run token, and `finish` stops children first and settles the parent only once they have settled (a 5 s watcher force-settles a run that does not react). An idle agent has no wake source before 4a, so it waits for its stop. Plan mode lets only research agents be spawned and blocks shell runs. `wait`, `runs(id)`, `runs(scope=all)` and `stop` are scoped to the caller's subtree (`can_reach`). `UnitView.result` carries the last result of an `Idle` agent. The hub is shut down with its agent loop (`cancel_all`), which stops every run. Permissions are auto-deny only, as planned: a background agent has no `user_response_rx`, so `PermissionManager::enforce` denies any call that would need a prompt while allow rules still apply; routing a prompt to the owner is 5b.

Verified live (PTY, rendered screen): the status bar shows only the active chat as `[name]` once a second chat exists, so a native tab is seen by switching chats (`next_chat`), not in a strip. Esc in the parent ends only the parent turn; native tabs are exempt from the Esc sweep (`is_native_run` in `maki-ui/src/app/mod.rs`) so they stay open and are not marked cancelled. Quitting while an agent is mid-request exits at once and sends no further provider requests.

### Phase 4a: inbox, drain points, send parent <-> child

Goal: `send` between a parent and its child (and to `main`) with caps; no wake of main yet.
Files: `runs/inbox.rs`, `runs/agent.rs`, `agent/run.rs` (`land_input` `:388`, `prepare_request`
`:514`, `handle_queued_command` `:1087`, `keep_going` `:742`), `inbox.rs` (`SubagentInbox`
adapter), `live_session.rs` (per-run mailbox).
Interfaces: `UnitHub::send(from, to, text, kind) -> Result<MessageId, UnitError>`.
Steps: (1) `Inbox` with caps (4.7), per-pair bucket and duplicate-free messages; (2) the four
drain points (4.2), coalesced `<run-message>` frames, never between `tool_use` and
`tool_result`; (3) the seq-recheck drive loop (4.4); (4) router `can_send` for parent,
child, `main`, `user`; (5) anti-ping-pong guard (4.7); (6) steer/followup/quiet semantics.
Tests: `inbox::caps_reject_toolarge_and_full`, `inbox::pair_rate_limit_manual_clock`,
`inbox::steer_never_between_tool_use_and_result`, `inbox::steer_extends_open_turn`,
`inbox::idle_agent_woken_by_steer`, `inbox::quiet_drains_at_next_model_request`,
`inbox::message_arriving_during_settle_not_lost`, `inbox::messages_never_collapsed`,
`hub::pingpong_guard_holds_and_notifies_parent`, `hub::send_to_sibling_rejected_by_default`.
E2E: spawn `a`; `a` is told to ask its parent one question mid-run via `send`; the parent
answers with `send`; `a` finishes with a result that uses the answer.
Commit: `feat: run inbox and parent child messaging`.

Notes from the build: every inbox lives in `runs/inbox.rs`; `main` has one on the hub and the root agent drains it through the same code, so `main` as a recipient is capped and rate limited like any run (waking an idle `main` is 4b). `Agent::new` takes its inbox from `params.runs` (`hub.inbox_of(self_run)`), so legacy Lua subagents, which carry no `RunsCtx`, are untouched. Lock order is inbox lock first, hub lock second; `send` checks reachability and state under the hub lock, drops it, then enqueues under the inbox lock, where a waking item flips the inbox phase and the unit state to `Running` in the same critical section. A run's inbox is closed when it is hard-stopped or finishes, so a `send` that raced the state change still gets `RECIPIENT_TERMINAL`. The drive loop settles in one inbox critical section: if a wakeable item is queued the next turn starts at once, otherwise phase and unit state go `Idle` together; this makes the doc's `seq` compare unnecessary (a drained item has no business restarting the run), so the counter is not compared; it stays only as the per-item ordering key inside a drain. Drain point 1 runs after the prompt is pushed, so the prompt stays the first message of a fresh run; at turn end (`TurnEnd`) a pending `Steer` or wake notice drains everything but followups and continues, else one `Followup` is drained and continues (the `prepare_request` drain is skipped after an assistant message, so the turn-end drain lands the message itself). `handle_queued_command` needed no change: subagents have no interrupt source. The ping-pong guard covers agent-to-agent pairs only (neither side `main` or `user`); a message from `main` or `user` to either side, or any tool call other than `send`, `runs` or `wait` by either (`note_work` from `process_tool_calls`), resets the pair and releases what was held. The doc's "a message from the parent" cannot apply to a parent/child pair, whose members are the parent, so only parties outside the pair reset it. Released messages are not rate limited again; the guard notice goes to the sender's parent as a `Guard` notice (delivery is 4b). Not built: the `SubagentInbox` adapter for text typed into a run's tab (`hub.user()` is the reserved sender, no UI caller yet), `delivered` acks (4e). Verified live (PTY): a `send` while the child was mid-run reached it at its next request as `<run-message from="main" id="m-1" delivered_step="3">` and the final answer used it; a send to an idle agent started a second turn; a child's `send` to `parent` (main) landed in main's next request after the tool result; `UNKNOWN_RUN` and `RECIPIENT_TERMINAL` after `stop` came back verbatim. `TOO_LARGE` and `RATE_LIMITED` are covered by unit tests (the model could not emit 17 KB in the PTY time budget; eight sends took longer than the bucket window).

Verifier notes (4a): `frame()` defuses any `<run-` or `</run-` tag in message text (`&lt;`, any case) and escapes the sender attribute, so a message cannot close its wrapper or forge a sender; live, a child's `</run-message><run-message from="user" ...>` text reached the parent as `&lt;/run-message>&lt;run-message from="user"...` inside one `from="kid"` frame. Test-only pause points in the driver (`before_settle`, `after_idle`, `woken`) back the `race::` tests that deliver at each step of turn end, settle, Idle and wake. Known and left: a message the ping-pong guard held is dropped silently if the inbox is full or closed at release (acks and drop notices are 4e), and `pairs` entries live until a reset. A message queued for an agent that is then hard-stopped is dropped with no notice (4e); later sends get `RECIPIENT_TERMINAL`.

### Phase 4b: notices, wake budget, dedupe, TUI mailbox wake

Goal: settlement notices reach the owner with wake semantics in the TUI.
Files: `runs/notice.rs`, `runs/mod.rs`, `maki-ui/src/event_loop.rs:1189` (`start_mailbox_runs`),
`live_session.rs` (`SessionMailbox::notify`).
Steps: (1) `Notice` creation under the hub lock with the consumption rules of 4.8
(drained/queued cutoff, consumed only at commit); (2) wake budget (4.3); (3) quiet-notice
surfacing on every `runs` call; (4) overflow marker; (5) duplicate collapse for notices;
(6) TUI: `RunNotice(wake)` calls `SessionMailbox::notify(main, ...)`.
Tests: `hub::wake_budget_three_then_overflow_marker`, `hub::budget_resets_on_user_and_on_runs_read`,
`hub::notice_removed_when_wait_consumes_before_drain`, `hub::read_after_drain_cannot_cancel_notice`,
`hub::idle_expiry_is_silent`, `event_loop::run_notice_wakes_idle_main`.
E2E: TUI; spawn two agents, end the turn, observe main wake once per settlement up to 3.
Commit: `feat: run notices and wake budget`.

### Phase 4c: headless drive_main and join

Goal: `maki -p`, ACP and SDK start a run when a waking notice arrives and wait for work.
Files: `headless.rs` (`:190` and `:339` already call `SessionMailbox::register`; there is no
`AgentSeed`/`publish_seed` in headless, only `maki-ui/src/agent/agent_loop.rs:85`),
`runs/mod.rs`.
Interfaces:
```rust
impl UnitHub {
  pub async fn drive_main(self: Arc<Self>, wake: impl WakeSink);
  pub async fn join(&self, timeout: Duration);   // direct arg; the CLI flag is phase 7b
}
pub trait WakeSink { fn start_main_run(&self, preamble: Vec<Message>); }
```
Steps: (1) `drive_main` over the main mailbox; (2) `join` per 4.4 (ignores Idle agents with
empty inboxes, drains quiet notices as a closing turn); (3) on timeout `stop` everything,
runs become `Done(Killed)`.
Tests: `headless::main_wakes_on_notice_and_join_waits`, `headless::join_timeout_stops_runs`
(calls `join(timeout)` directly with a manual clock), `headless::join_ignores_idle_agents`,
`headless::join_delivers_quiet_notices_as_closing_turn`.
E2E: `maki -p` with a prompt that spawns a 5 s shell and an agent; process stays until both
finish and prints their results.
Commit: `feat: headless run driver and join`.

### Phase 4d: topology facts and sibling routing

Goal: the model sees parent and children; sibling `send` works only with `siblings = true`.
Files: `maki-storage/src/frame.rs` (`LiveFacts` `:29`, `facts_at_start` `:149`, `FactsUpdate`
`:48`, `apply` `:71`, `changes_to` `:100`), `maki-providers/src/types.rs` (`Message::context_update`
`:407`, the storage-format test near `:1383`), `maki-agent/src/agent/frame.rs:253`
`context_update`, `agent/history.rs:85` `told()`, `agent/run.rs:400,:533` (`tell_changes`),
`runs/topology.rs`.
Steps: (1) `TopologyFacts` with plain `String`/`u8` fields (3.6); (2) replace `RunContext::fixed`
with a facts-bearing frame for native agents (3.6); (3) coalesced re-tell, one per drain;
(4) depth-cap behaviour (3.6); (5) `can_send` gains the sibling rule; (6) `runs scope=all`.
Tests: `topology::fact_changes_only_when_children_change`, `topology::retell_coalesced_per_drain`,
`topology::rendering_snapshot_main_and_sub`, `topology::no_siblings_in_fact`,
`providers::context_update_topology_keeps_stored_format`, `storage::facts_at_start_plan_none`,
`hub::sibling_send_only_with_config`, `hub::pingpong_guard_mandatory_with_siblings`.
E2E: with `siblings = true`, `a` messages `b` by name and `b` reports both.
Commit: `feat: run topology facts and sibling routing`.

### Phase 4e: acks

Goal: delivery visible. Files: `runs/inbox.rs`, `types.rs` (`RunMessageDelivered`),
`tools/runs.rs`. Steps: `accepted` in the send result, `delivered` at drain with step,
`runs(id)` shows pending and last delivered; a quiet notice only when delivery exceeds 60 s
or the message is dropped. Tests: `ack::accepted_then_delivered_order`,
`ack::dropped_when_recipient_stopped_notifies_sender`. E2E: `runs(id)` after a send shows
`delivered`.
Commit: `feat: run message acks`.

### Phase 5a: capacity pool, queue, limits, budgets

Goal: section 6 caps, FIFO queue, slot release while blocked, budgets.
Files: `runs/limits.rs` (full `UnitLimits`), `runs/mod.rs` (`admit`), `maki-config/src/lib.rs`
(`[agent.runs]` full section with mirror `UnattendedMode` type, since maki-config cannot name
maki-agent types; maki-agent converts), `maki-docgen` output.
Interfaces: `UnitLimits { max_agents:u8, max_shells:u8, max_shells_per_owner:u8, max_depth:u8, max_children_per_run:u8, max_queue:u16, max_retained:u16, max_idle:u8, idle_timeout:Duration, default_timeout:Duration, spawn_per_minute:u8, session_max_runs:u16, siblings:bool }`.
Steps: (1) defaults equal to 6.1, zero values rejected; (2) FIFO `admit`; a run in `wait` or a
permission prompt releases its slot and re-acquires at queue priority; (3) queue positions;
(4) per-owner shell cap, children cap, spawn rate; (5) timeouts and `session_max_runs`;
(6) docgen regen in this commit.
Tests: `limits::defaults_match_design`, `limits::zero_rejected`, `admit::fifo_and_queue_position`,
`admit::wait_releases_slot_and_reacquires_first`, `admit::queue_full_error`,
`admit::stop_queued_never_starts`, `rate::spawn_limit_manual_clock`, `budget::default_timeout_30m`.
E2E: `max_agents = 1`; spawn three agents; see `queued position 1 of 2` and sequential
execution; an agent that `wait`s on a queued child does not deadlock.
Commit: `feat: run limits and queue`.

### Phase 5b: background permission routing

Goal: a background agent's permission prompts reach the user (or are denied when unattended)
without pinning a slot.
Interfaces: `PermissionBroker` (receiver end for `enforce`), `unattended_permissions` config.
Files: `maki-agent/src/permissions.rs` (`PermissionBroker`; `enforce` `:755`, `:830` unchanged),
`types.rs` (request attribution), `maki-ui` prompt component, `maki-acp/src/permissions.rs`.
Steps: (1) broker per agent run; (2) attributed prompt in the TUI queue; (3) ACP title;
(4) 120 s timeout then deny and the slot released while waiting; (5) headless and unattended =
deny with the "report via send" text; `[agent.runs] unattended_permissions` mirror enum;
(6) quiet `waiting for permission` notice after 60 s with main idle.
Tests: `perm::background_prompt_reaches_ui_with_run_name`, `perm::timeout_denies_and_run_continues`,
`perm::slot_released_while_prompt_pending`, `perm::unattended_deny_text_mentions_send`,
`perm::headless_inherit_yolo_allows`.
E2E: agent asked to `touch /tmp/e2e/x` with no rule: prompt shows `name (handle)`; denying
lets the agent report.
Commit: `feat: background permission routing`.

### Phase 6a: journal and lost reconciliation

Files: `maki-storage/src/runs.rs` (new `RunJournal`; `SessionRef` is a storage type,
`maki-storage/src/id.rs:109`), `maki-storage/src/lib.rs`, `maki-storage/src/sessions.rs` (`StoredRun`
`:640`, per-turn writes, delete path), `runs/journal.rs`, `runs/mod.rs` (`reconcile`),
`maki-ui/src/app/session.rs:166,253-301`.
Interfaces: `RunJournal::{open(&SessionRef), append, replay}`, `JournalRecord`, `UnitHub::reconcile`.
Steps: (1) journal with header, torn-line tolerance, off-lock writes, size-threshold
compaction (7.3); (2) message body file; (3) pid file with start time; (4) reconcile per 7.3;
(5) `StoredRun`, per-turn incremental transcript; (6) clean-shutdown `mark`; (7) session delete
removes the dir; (8) shell log retention; (9) UI restore of native tabs read-only.
Tests: `journal::replay_ignores_torn_last_line`, `journal::unknown_record_and_future_version`,
`journal::compaction_on_size_threshold_while_live`, `journal::message_bodies_survive`,
`reconcile::running_agent_becomes_lost`, `reconcile::orphan_killed_only_when_start_time_matches`,
`reconcile::lost_notice_queued_once_idempotent`, `storage::stored_run_roundtrip_and_old_session_loads`,
`session::delete_removes_runs_dir`, `logs::retention_by_size_and_age`.
E2E: SIGKILL the printed maki PID with an agent running; `--continue` shows a `lost` notice.
Commit: `feat: run journal and lost reconciliation`.

### Phase 6b: cold revive

Goal: `send` to a finished, lost or evicted agent revives it from its transcript.
Files: `runs/mod.rs` (`revive`), `runs/journal.rs`, `maki-storage/src/sessions.rs`.
Interfaces: `UnitHub::revive(id, first: InboxItem) -> Result<(), UnitError>`.
Steps: persist full `AgentSpec` in the `spawn` record; `revive(id, first)` rebuilds the runner
(7.4); tombstones for evicted runs; unread messages from the message file are redelivered.
Tests: `revive::completed_agent_cold_resumes_from_transcript`, `revive::one_shot_not_revivable`,
`revive::spec_restored_model_tools_schema`, `revive::evicted_tombstone_still_revivable`,
`revive::pending_messages_redelivered`.
E2E: after the 6a crash, the model `send`s to the lost agent and gets a reply.
Commit: `feat: cold revive of runs`.

### Phase 7a: /runs and status line

Goal: the user sees and stops runs from the TUI.
Interfaces: `RunsView` in `App`, `/stop <name|all>` command.
Files: `maki-ui/src/app/{tasks.rs,mod.rs,session.rs}` (`RunsView`, `diff_task_states`
`tasks.rs:184` logic ported), `components/runs_picker.rs`, `components/status_bar.rs`,
`commands.rs` (`/stop`), `chat.rs` (notice row collapse), keybindings, docgen outputs
(commands, keybindings) in this commit. The Lua `task` picker stays for legacy runs.
Tests: `ui::runs_picker_rows_tree_and_states`, `ui::status_segment_hidden_at_zero`,
`ui::notice_row_collapsed_and_expands`, `ui::esc_in_tab_stops_run_cascade`,
`ui::stop_command_all`, `ui::permission_badge_shown`.
E2E: PTY with three agents and a shell; assert the segment and `/runs` rows in
`/tmp/e2e/tui.log`; `/stop all` leaves all terminal.
Commit: `feat: runs view and status line`.

### Phase 7b: log viewer, headless output, docs page

Goal: shell logs viewable, headless run output, user docs.
Interfaces: CLI flags `--join-timeout`, `--quiet-runs`.
Files: transient log viewer panel, `src/print.rs` (`--join-timeout`, `--quiet-runs`, stderr
transition lines, JSON `run_*` events), `maki-agent/src/headless.rs`, `site/docs/content/`
page for agents (content root `maki-docgen/src/main.rs:17`; the page is hand-written).
Tests: `ui::log_viewer_tails_by_offset`, `print::run_lines_on_stderr`,
`print::join_timeout_flag_parsed`, `docgen::check_clean`. E2E: `maki -p --join-timeout 30`.
Commit: `feat: run log viewer and headless run output`.

### Phase 8: hardening and e2e soak

Goal: remove shortcuts, prove robustness, enable by default.
Files: `maki-agent/tests/runs_soak.rs` (uses `test_support`), docs.
Steps: (1) property test of random spawn/send/stop/wait sequences with `MockProvider` and
`ManualClock` asserting: no live run under a terminal ancestor, one terminal transition,
slots never exceed limits, no notice delivered after consume, no await under the hub lock;
(2) fault injection: runner ignoring cancel, journal write errors, provider errors; (3) hub
drop joins all tasks; (4) 20-agent live fan-out with `max_agents` 4; (5) notice context
growth bounded; (6) `enabled = true` by default (the switch stays); (7) 100 idle runs cost no
CPU (event-driven waits).
Tests: `soak::invariants_hold_over_random_ops`, `soak::runner_ignoring_cancel_force_settled`,
`soak::journal_write_failure_degrades`, `soak::hub_drop_has_no_live_tasks`,
`soak::notice_bytes_bounded`, `hub::no_timers_when_idle`.
E2E: the fan-out plus mid-run Esc, `/new`, quit; no orphan PIDs, clean journal `mark`.
Commit: `feat: harden native runs and enable by default`.

### Appendix B: post-v1 (out of the phase list)

Not in v1; each needs its own decision and design pass:
* Lua `maki.run.*`; rewriting `maki.agent.session` over the hub (`maki-lua/src/api/agent.rs:731`).
* Removing `plugins/task` and `maki.task.*` rewiring. When done it must handle
  `maki-lua/src/api/task.rs`, `BackgroundSessions` (`maki-lua/src/background.rs:44`), the
  bundled plugin registry entry (`maki-lua/src/loader.rs:145`, `include_dir!` of
  `plugins/task`), `SubagentInbox` and legacy `task_id` paths, and `plugins.task.*` mapping.
  Until then native `spawn` coexists with the Lua `task` tool.
* Non-unix job objects; ACP `tool_call_update` mapping; subtree cost budgets; idle unload to
  disk; true bash promotion if 2c fell back to a hint; stdin `send` to shells.

## 10. Risks, open questions, test strategy

### 10.1 Risks

1. Frame cache bust: adding five tools changes the frame fingerprint once. Mitigation:
   fixed tool set per audience (5.2), single release note.
2. History ownership: `Agent<'h>` borrows `&mut History` per run. Mitigation: one runner
   per agent with an async mutex; never two runs concurrently.
3. Deadlock through slot exhaustion (parents waiting on queued children): a run blocked in
   `wait` or a permission prompt releases its slot and re-acquires on resume (6.1);
   tested by `admit::wait_releases_slot_and_reacquires_first`.
4. Notice injection correctness against provider rules (tool_use/tool_result pairing):
   hard invariant with a dedicated test over both Anthropic and OpenAI shapes.
5. Two registries (legacy Lua `task` plus hub) for the whole v1. Mitigation: the flag
   defaults off, both coexist, the phase 3 matrix runs with both enabled.
6. Process groups on non-unix: no grace; documented, tested on unix only.
7. UI regressions in tab keys (handle vs `tool_use_id`): `app::tests.rs` is large;
   legacy `task` keeps its tool_use_id keys, native runs use handles (no collision).
8. Local models report cost 0; cost display is cosmetic there (6.3).
9. Cold resume cache miss and token cost: accepted (7.4).
10. Journal growth: compaction, cap on retained runs.

### 10.2 Open questions (all DECIDED after review)

1. DECIDED: `[agent.runs] enabled` stays a permanent kill switch, default off until phase 8 (hardening). Why: a feature that adds tools and background processes needs an escape hatch forever.
2. DECIDED: wake budget 3 consecutive notice wakes, reset on user messages and on model-initiated `wait`/`runs` reads (4.3). Why: bounds interruptions without stranding results.
3. DECIDED: background permission prompts go to the user, 120 s timeout then deny, the slot is released while waiting; headless and unattended = deny (6.2). Why: no unattended widening of permissions, no pinned slot.
4. DECIDED: shells are killed on quit, orphans killed after a crash via pid file (start time must match), no re-adoption (7.2, 7.3). Why: re-adoption needs log tailing and pid-reuse handling for little gain.
5. DECIDED: `max_agents` 4, depth 2, shells per owner 4, queue 16, default 30 min timeout for every agent (6.1). Why: local single-GPU models.
6. DECIDED: sibling messaging OFF by default (parent/child plus `main`); `[agent.runs] siblings = true` enables it, and the ping-pong guard (4.7) is mandatory when on. Why: O(n^2) topology and loop risk.
7. DECIDED: the model may set only `timeout_secs` (6.3). Why: budgets are operator policy.
8. DECIDED: `maki-monitor` is not touched; the migration table is documented and phase 2b ships the replacement (8.3). Why: it is the user's plugin, not in this repo.
9. DECIDED: `plugins.task.*` config keeps working; the Lua `task` tool stays in v1 and coexists with native `spawn` (appendix B). Why: no breaking change before a separate decision.
10. DECIDED: no run renaming in v1. Why: names are addressing keys, stable for the model.

### 10.3 Test strategy

* Unit: every state transition, inbox rule and limit has a table-driven test with an
  injected `Clock`; no `sleep`, no wall-clock assertions.
* Hub integration: `MockProvider` and `StubStreamProvider` from the shared `test_support` module (private in `run.rs:1239`,`:1293` today) scripted turns; shell stubs via `sh -c`;
  child process groups in a temp dir.
* Persistence: fixture journals (golden files) for every version and corruption case.
* UI: `maki-ui/src/app/tests.rs` helpers (`app_with_two_subagents`) extended; ratatui
  buffer snapshots.
* Headless and ACP: in-process `spawn`/`spawn_interactive` with a stub provider,
  assert event streams and exit behaviour.
* Docs: `docgen --check` in every phase.
* Live e2e: the PTY runner, per phase, against the local model; flaky model output is
  asserted by tool-call names and states, never by exact prose.
* Property soak in phase 8 with deterministic seeds printed on failure.

## Appendix A: toolchain recipe

`cargo` is not on PATH and maki needs rustc 1.99. Per memory note
`maki-fork-toolchain-and-ci.md`:

```bash
export PATH="/nix/store/$(ls /nix/store | grep -E '^[a-z0-9]+-rust-minimal-1\.99\.1$' | head -1)/bin:$PATH"
RF=$(ls -d /nix/store/*rustfmt-preview-1.99* | grep -v '\.drv' | head -1)
CL=/nix/store/5d1g1pdk5z3sqv3kmvrh2jb1r033rnqc-clippy-preview-1.99.1-x86_64-unknown-linux-gnu
NX=/nix/store/q0anzqrazmdr8n1049ca28098q6lv2y8-cargo-nextest-0.9.146
export PATH="$RF/bin:$CL/bin:$NX/bin:$PATH"
export SYSROOT=$(rustc --print sysroot)   # required, or clippy cannot find std
```

CI runs nextest, not libtest: fmt, clippy `-D warnings`, `cargo nextest run --workspace`,
`cargo run -p maki-docgen -- --check`.
