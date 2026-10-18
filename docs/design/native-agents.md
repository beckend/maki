# Native background agents and shell jobs (design)

Status: proposal, branch `native-agents`. Part A covers goals, data model, tool surface,
delivery and the native-tool mechanism. Part B covers limits, persistence and phases.
Line references are to maki 0.6.4 on this branch.

## 1. Goals and non-goals

Goals:
1. Subagents and background shell jobs are core Rust in `maki-agent`, so TUI, `maki -p`,
   ACP and SDK all get them (headless has no wake consumer today, see
   `maki-agent/src/live_session.rs:111` `claim_wake`, used only by
   `maki-ui/src/event_loop.rs:1189` `start_mailbox_runs`).
2. Free two-way messaging: parent to child, child to parent, sibling to sibling, under a
   known topology the model can see and the runtime enforces.
3. One concept (`Run`), one id scheme, one state machine, one notice path, one small tool
   family, for agents and shell jobs alike.
4. Bounded everything: notices, inboxes, outputs, wake storms. Full bodies are pulled by id.
5. Cache-safe: topology reaches the model through `tell_changes`/`LiveFacts`, never by
   editing the system prompt.
6. Replace the Lua `task` plugin (`plugins/task/init.lua`) and the external Lua
   `maki-monitor` plugin. The Lua `maki.agent.session` API stays and becomes a thin
   wrapper over the same hub.

Non-goals: remote or cross-process agents, external-harness children (ACP/Codex as
children), a workflow DSL, re-adoption of live agent runs after a crash (shell jobs may
be re-adopted, Part B), changing the UI tab model beyond consuming new events.

### 1.1 Comparison

| Aspect | dsh | maki today | This design |
|---|---|---|---|
| Subsystems | Two (subagent manager, jobs registry) with different ids, states, notices | Three: Lua `task`, Lua `maki.fn.job*` (`maki-lua/src/api/fn.rs:820`), user `maki-monitor` plugin | One `RunHub`, one `Run` |
| Ids | session uuid vs `bash-<16hex>` | task_id (tool_use_id), session_id, mailbox MakiId all differ | One `MakiId` per run, short handle `ag-k3x9ab`/`sh-4m2q7z` plus unique name |
| States | agents `running\|inactive`, jobs 5 states | ad hoc per UI tab | `queued running idle stopping completed failed killed timed_out lost` |
| Tools | 7+ (`subagent`, `send_message`, `interrupt_agent`, `list_agents`, `job_output/list/kill`) | `task`, bash, plugin tools | 5: `spawn send runs wait stop` |
| Wait on a child | none in core | none (blocking `task` call) | `wait(ids, any/all, timeout)` |
| Notice size | unbounded closing message | whole answer is tool result | 4 KiB cap, spill to disk, pull full body |
| Notice dedupe | wait/kill only | none | any consuming read (wait, runs peek, stop) |
| Wake budget | opt-in, jobs only | none | default 3 consecutive notice wakes, agents and shells |
| Delivery acks | accepted only | none | accepted, delivered, acted |
| Inbox bounds | unbounded | mailbox 100 (`live_session.rs:18`) | 32 pending, 16 KiB/message, per-pair rate limit |
| Siblings | not in core | no | yes, within a session tree, router-checked |
| Crash | notice lost, ids dangle | stranded tabs dropped | journal, `lost` state, notice on next wake |
| Stop | request, no escalation, no cascade | `cancel_all` per run | cascade by default, TERM, grace 5 s, KILL, returns after settle |
| Cost | none per child | `RunLedger::child` chain (`types.rs:1084`) | same chain, usage in every notice |
| Headless | `waitForChildren` for agents, jobs not joined | children die with run | hub `join()` joins both, `-p` waits |
| Topology to model | depth hint at limit | none | per-turn tree fact via `LiveFacts` |

## 2. Data model

### 2.1 Crate placement

New module tree `maki-agent/src/runs/` (no dependency on lua or ui, honoring the
dependency direction storage, config, providers, agent, lua, ui):

```
runs/mod.rs        RunHub, RunId, RunName, Run, RunKind, RunState, TerminalReason
runs/agent.rs      AgentRunner: driver loop owning History for a native agent
runs/shell.rs      ShellRunner: process spawn, log file, pump, TERM/KILL
runs/inbox.rs      Inbox, InboxItem, DeliveryKind, Ack, rate limiter
runs/notice.rs     Notice, NoticeKind, rendering, spill
runs/topology.rs   Topology snapshot, router, facts rendering
runs/journal.rs    event journal (Part B)
tools/runs.rs      the five native tools (impl Tool, see section 5)
```

UI-facing information goes out as new `AgentEvent` variants (`types.rs:642`) over the
existing `EventSender`, never `UiAction` (which lives in `maki-lua`,
`maki-lua/src/api/util/command.rs`). `MakiId` stays in `maki-storage/src/id.rs:37`.

### 2.2 Ids and names

One id per run: the existing `MakiId` (16 byte uuid). It replaces three ids that today
mean the same thing in different maps: subagent `task_id: Option<Arc<str>>` (the
spawning `tool_use_id`, `AgentParams` at `run.rs:102`), the Lua session `MakiId` mailbox
key (`maki-lua/src/api/agent.rs` ~885) and the UI chat key. `tool_use_id` is kept only as
`origin_tool_use_id` for rendering the spawn call.

```rust
// maki-agent/src/runs/mod.rs
pub struct RunId(MakiId);                 // internal, persisted, mailbox key
pub struct RunHandle(Arc<str>);           // "ag-k3x9ab" | "sh-4m2q7z": kind prefix + 6 base32 chars
pub struct RunName(Arc<str>);             // lower-kebab, 1..=32 chars, unique per session tree
```

Model-facing id is the handle; the model may also use the name anywhere an id is
accepted. `main` is the reserved name of the root agent, `parent` resolves to the caller's
parent. A handle is derived from the first 4 bytes of the `MakiId`; collision inside one
session tree is retried at spawn. Names default to a slug of `description`, suffixed `-2`,
`-3` on clash. Human display uses name first, handle in parentheses.

`session_id` (`ToolContext.session_id`, provider affinity and otel) stays the root
session's id for every descendant; it is not an identity and nothing may key maps by it
for runs (comment at `maki-lua/src/api/agent.rs` ~879 warns of the collision).

### 2.3 The `Run`

```rust
pub enum RunKind {
    Agent(AgentSpec),   // model, audience, prompt id, tools filter, output_schema, thinking
    Shell(ShellSpec),   // argv or script, cwd, env, timeout, watch rules
}

pub struct Run {
    pub id: RunId,
    pub handle: RunHandle,
    pub name: RunName,
    pub owner: RunId,            // who gets notices; main agent id for top level
    pub parent: RunId,           // who spawned it; equals owner unless re-parented (never in v1)
    pub depth: u8,               // main = 0
    pub kind: RunKind,
    pub description: String,     // 3-8 words, display
    pub origin_tool_use_id: Option<Arc<str>>,
    pub state: RunState,
    pub terminal: Option<Terminal>,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub last_activity: Instant,  // last tool call or output line
    pub progress: Option<String>,// agent: current tool header; shell: last output line
    pub usage: TokenUsage,       // own, cumulative; subtree rolled via RunLedger
    pub cost: f64,
    pub notice_policy: NoticePolicy, // on_exit | on_match | never, wake or quiet
}

pub enum RunState { Queued, Running, Idle, Stopping, Completed, Failed, Killed, TimedOut, Lost }

pub enum TerminalReason {
    Completed,          // agent EndTurn with no pending work; shell exit 0
    Failed,             // agent error; shell nonzero exit
    Killed,             // stop requested (by model, user, cascade, shutdown)
    TimedOut,           // wall clock or turn budget hit
    MaxTokens, MaxTurns,
    Refused,
    Lost,               // process or session died, outcome unknown
}

pub struct Terminal {
    pub reason: TerminalReason,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,      // short error or signal text
    pub result: RunResult,
}

pub struct RunResult {
    pub summary: String,             // <= 4096 bytes, model-facing
    pub truncated: bool,
    pub body_ref: Option<BodyRef>,   // full text on disk when truncated
    pub structured: Option<serde_json::Value>, // agent output_schema result
    pub usage: TokenUsage, pub cost: f64, pub turns: u32, pub elapsed_ms: u64,
}
```

`Idle` exists only for agents: the run finished a turn, has no pending inbox, and may
be woken by `send`. A dsh agent's `inactive` conflates idle, finished, crashed and
unloaded; here each is distinct. An agent is spawned `continuable` by default;
`one_shot: true` makes it complete instead of going idle.

`Run` is not the same as the existing per-turn `run_id: u64` (`Envelope.run_id`,
`types.rs:642` area; UI drops stale ones at `event_loop.rs:246`). In code the new type is
`BgRun` in the Rust module to avoid the clash; this doc says `Run`.

### 2.4 Hub

```rust
pub struct RunHub {                       // one per root session, Arc, in AgentSeed
    runs: Mutex<RunTable>,               // id -> RunEntry, name index, children index
    cancel: CancelToken,                 // session-lifetime root; runs hold child tokens
    ledger: Arc<RunLedger>,              // root ledger; each Run holds RunLedger::child
    events: EventSender,                 // run_id 0, stamped with SubagentInfo for agents
    limits: RunLimits,                   // Part B
    notices: NoticeRouter,
    waiters: WaiterSet,                  // wait() registrations, for dedupe
}
```

The hub is published through `AgentSeed`/`LIVE_SESSIONS` (`live_session.rs:48`) so every
frontend that builds an Agent can reach it. It is created where the seed is: TUI
`maki-ui/src/agent/mod.rs` ~249, headless `headless.rs:163` and `:307`.

### 2.5 State machines

Agent run:

```
spawn ──> queued ──slot free──> running <────────────┐
            │                     │   │ turn ends,   │ send(wake) / notice
            │ stop                │   │ inbox empty, │ addressed to it
            v                     │   │ continuable  │
         killed                   │   v              │
                                  │  idle ───────────┘
                                  │   │ one_shot, or stop, or idle > 30 min
   stop ─> stopping ─────┐        │   v
     (children first)    │        └─> completed | failed | timed_out
                         v
                      killed
   any non-terminal ──(process exit/crash, or journal replay finds it running)──> lost
```

Shell run:

```
spawn ──> queued ──> running ──exit 0──> completed
                       │  └──exit != 0──> failed
                       │  └──deadline───> stopping ──> timed_out
   stop ──> stopping ──TERM, 5 s grace, KILL──> killed
   any non-terminal ──(hub died; pid dead or unknown on replay)──> lost
   (detached shell whose pid is still alive on replay is re-adopted as running)
```

Rules: the first transition into a terminal state wins (settle is idempotent); `Stopping`
is entered once and never left except into a terminal state; `Idle` is not terminal and
never produces a settlement notice (only a `turn_done` quiet event, section 4.6);
`Lost` is produced only by reconciliation, never by a live runner. `Queued` exists
because capacity is a queue, not an error (FIFO, Part B for caps).

### 2.6 Notices

All run-originated text for the model goes through one type and one path.

```rust
pub struct Notice {
    pub seq: u64,               // per-owner monotonic, for ordering and idempotency
    pub run: RunId,
    pub kind: NoticeKind,
    pub wake: bool,
    pub text: String,           // rendered, <= 4096 bytes
    pub created: SystemTime,
}

pub enum NoticeKind {
    Settled,       // run reached a terminal state
    Lost,          // reconciliation found it dead; delivered at next owner wake
    Message,       // run-to-run message body (see inbox)
    Match,         // shell watch rule fired (replaces maki-monitor lines)
    Progress,      // agent asked to report mid-run; never wakes
    Overflow,      // "N notices dropped" coalesced marker
}
```

Notices are rendered as `Message::observation` (the existing mailbox message type,
`live_session.rs`), user role, wrapped in `<run-notice run="ag-k3x9ab" name="auditor"
seq="12" kind="settled">...</run-notice>`, so the model and the UI can recognise them and
the UI collapses them into one row.

Structured results: the settlement notice carries `RunResult` fields; the full record is
fetchable with `runs(id)` (section 3.4). An agent with `output_schema` registers the
existing `structured_output` local tool (`LocalTool`, `tools/mod.rs:295`) and its JSON
lands in `RunResult.structured`, shown in the notice as `structured: {...}` if it fits in
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

Old surface:
* `task` (bundled Lua, `plugins/task`) is removed in phase 8 (Part B). Until then it is
  hidden when native tools are on, via `disabled_tools`. Its `subagent_type`,
  `model_tier`, `output_schema`, `thinking` fields map one to one onto `spawn`.
  `/tasks` picker and `maki.task.*` are re-implemented over hub events.
* `monitor_*` (external Lua plugin) is deleted; `spawn(kind="shell", watch=...)` plus
  `Match` notices covers it.
* `maki.fn.jobstart` family stays for plugin-private processes but is not the model's
  path. bash stays as the synchronous tool; its description gains one sentence:
  "For anything that runs longer than a minute or must keep running, use `spawn`
  with kind shell."

### 3.1 `spawn`

Description (model-facing):

```
Start background work and return immediately with its id. kind "agent" starts a
subagent: a separate agent with its own context that works on a self-contained task and
reports back. kind "shell" starts a command that keeps running while you continue. You
are notified when a run finishes; do not poll or sleep. Start independent runs together
in one message. Use `wait` only when you are blocked on a result, `send` to talk to a
run, `stop` to end it, `runs` to look.

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
    "name": {"type": "string", "pattern": "^[a-z][a-z0-9-]{0,31}$", "description": "Optional unique name others can address this run by."},
    "prompt": {"type": "string", "description": "agent only. The complete, self-contained task."},
    "subagent_type": {"enum": ["research", "general"], "description": "agent only. research is read-only; general can edit. Default general."},
    "model_tier": {"enum": ["weak", "medium", "strong"], "description": "agent only. Clamped to your tier."},
    "thinking": {"type": "string"},
    "output_schema": {"type": "object", "description": "agent only. JSON Schema the final result must satisfy."},
    "context": {"enum": ["fresh", "fork"], "description": "agent only. fork seeds the run with your completed turns (default fresh)."},
    "one_shot": {"type": "boolean", "description": "agent only. Complete after the first result instead of staying available. Default false."},
    "command": {"type": "string", "description": "shell only. Command line run with the user's shell."},
    "cwd": {"type": "string"},
    "timeout_secs": {"type": "integer", "minimum": 1, "description": "Wall clock limit. Default: none for agents, 3600 for shell. Expiry ends the run as timed_out."},
    "watch": {"type": "string", "description": "shell only. Regex; each matching output line becomes a notice (max 1 per second, then batched). Default none."},
    "notify": {"enum": ["wake", "quiet", "never"], "description": "On finish: wake starts a turn if you are idle (default), quiet waits for your next turn, never sends nothing (read with runs)."}
  }
}
```

Result: `started ag-k3x9ab "auditor" (agent, running). Queued runs show: started ... (queued, position 2 of 2).`

Permission: `spawn` is `tool_kind` execute; prompting follows the existing bash rule for
`kind=shell` (scope = the command, so existing bash allow rules and `permission_scopes`
apply) and no prompt for `kind=agent` (the child's own tools are gated individually).
Plan mode blocks `kind=shell` and agent `general` (`background_write_block`).

### 3.2 `send`

```
Send a message to another run or agent in this session: your child, your parent
(name "parent"), a sibling, or "main". The message is queued and returns a message id at
once, with the recipient's state. A running recipient reads it at its next step; an idle
one starts a turn with it; a shell run receives it on stdin. Sending does not end your
turn and is not an answer: the recipient's reply, if any, comes as a message to you.
Use mode "quiet" for FYI notes that must not start a turn.
```

```json
{"type":"object","required":["to","message"],"properties":{
  "to": {"type":"string","description":"Run id or name, or \"parent\" / \"main\"."},
  "message": {"type":"string","description":"Up to 16 KiB. Be self-contained."},
  "mode": {"enum":["steer","followup","quiet"],"description":"steer (default): next step of a running recipient, wakes an idle one. followup: after the recipient's current turn ends, as its own turn. quiet: delivered at the next natural drain, never wakes."}
}}
```

Result: `message m-17 queued for ag-k3x9ab (running); use runs(id) to see delivery`. Error
codes (stable, in the text): `UNKNOWN_RUN`, `NOT_REACHABLE` (outside tree or policy),
`RECIPIENT_TERMINAL` (completed/killed runs cannot be messaged; spawn a new one),
`INBOX_FULL retry_after=Ns`, `TOO_LARGE max=16384`, `RATE_LIMITED retry_after=Ns`,
`SELF`.

### 3.3 `wait`

```
Block until runs finish or a timeout expires. Use only when you cannot proceed without
the result; otherwise keep working, you will be notified. mode "all" (default) returns
when every listed run is terminal, "any" when the first is. A timeout is not an error:
you get the current states and the runs keep going. Results you receive here are not
repeated as notices.
```

```json
{"type":"object","required":["ids"],"properties":{
  "ids":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":16},
  "mode":{"enum":["all","any"]},
  "timeout_secs":{"type":"integer","minimum":1,"maximum":600,"description":"Default 60."}
}}
```

Returns, per run, the same record as `runs(id)` (state, terminal reason, summary, usage).
It is cancellable by the caller's own cancel token; a cancel returns without stopping the
runs.

### 3.4 `runs`

```
List your runs and everything below you, or inspect one. Without id: a tree of runs
with id, name, kind, state, age, last activity, cost and a one-line progress. With id:
the full record, the result (full=true for untruncated), and for shell runs output
since your last read (tail=N lines, from_offset=B to page; output is read from the
log file). Reading a finished run's result marks it received, so its notice is dropped.
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

List output example:

```
main (you)
├─ ag-k3x9ab auditor      agent  running  2m10s  $0.12  Grep for token handling in src/auth
│  └─ ag-p8d2ne scanner   agent  idle     1m02s  $0.03  replied 40s ago
├─ sh-4m2q7z build        shell  running  5m01s  cargo build --release (last: Compiling maki-ui)
└─ ag-9wq0lc docs-check   agent  completed 3m  $0.08  result received
```

### 3.5 `stop`

```
Stop runs. Agents are cancelled at their next safe point and their children are stopped
too (detach=true leaves children running). Shells get SIGTERM, then SIGKILL after 5
seconds. Returns once the runs have settled or after wait_secs. A stopped agent keeps
its transcript and is read with runs; you will not get a notice for runs you stopped.
```

```json
{"type":"object","required":["ids"],"properties":{
  "ids":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":16},
  "reason":{"type":"string","description":"Forwarded to the run as its last message."},
  "detach":{"type":"boolean"},
  "wait_secs":{"type":"integer","minimum":0,"maximum":30,"description":"Default 10."}
}}
```

Outcome per id: `stopped` / `already_finished(state)` / `still_stopping`. Unlike dsh
`interrupt_agent` (always `accepted:true`, no cascade) the answer is truthful.

Reason about not adding more tools: peek is `runs(id)`, message status is
`runs(id)` (shows `inbox: 2 pending, last delivered m-17 at step 4`), progress reporting
by an agent is `send(to="parent", mode="quiet")`.

### 3.6 Per-turn identity and topology snippet

Delivered as a context update through `tell_changes` (`run.rs:460`), diffed against what
the history `told()`, so it is emitted only when it changes and persists across resume.
New fact fields in `LiveFacts` (`maki-storage/src/frame.rs:29`, today `plan` and `prompt`):

```rust
pub struct LiveFacts {
    pub plan: Option<PathBuf>,
    pub prompt: Option<PromptFacts>,
    pub topology: Option<TopologyFacts>,   // None when the hub has no runs and caller is main
}
pub struct TopologyFacts {                 // sorted, small, no timestamps or costs
    pub me: (RunHandle, RunName, u8 /*depth*/),
    pub parent: Option<(RunHandle, RunName)>,
    pub children: Vec<(RunHandle, RunName, KindTag)>,   // not states
    pub siblings: Vec<(RunHandle, RunName)>,
    pub limits: (u8 /*max depth*/, u8 /*slots free*/),
}
```

States are deliberately NOT in the fact (they change every few seconds and would re-tell
every turn); states come from notices and `runs`. A run being created or ending changes
the child list, which is a legitimate re-tell. Rendered text for a subagent:

```
Run topology changed. You are agent "auditor" (ag-k3x9ab), depth 1 of 2. Your parent is
"main" (main). Siblings: "scanner" (ag-p8d2ne), "docs-check" (ag-9wq0lc). You have no
children. Use send(to=...) to message any of them by name. Your parent shares your
workspace but not your transcript or tool output: send your result before you finish,
and send earlier findings when they change what your parent should do. Sending does not
end your turn. Capacity: 5 of 8 run slots free in this session.
```

For the main agent:

```
Run topology changed. You are "main". Runs you started: "auditor" (ag-k3x9ab, agent),
"build" (sh-4m2q7z, shell). Runs finish with a notice; do not poll. Capacity: 5 of 8
run slots free, maximum depth 2.
```

At the depth limit the text adds: `You are at the maximum depth: do not call spawn with
kind "agent"; do the work directly.` (Also enforced in the tool.) A child's first
prompt additionally gets the one-time paragraph from the template above, so the
contract exists even if the first `tell_changes` is skipped.

Subagent frames use `RunContext::fixed(system, tools)` (`agent.rs` ~990, no live
updates), so for them the identity fact is told on the first `land_input` and re-told
through the same `tell_changes` path, which this design makes run for subagents too.
`context_update` (`maki-storage/src/frame.rs:253`) gains a topology arm. The tool
descriptions above are static text in the frame, so adding the five tools busts the
cache once per upgrade and never per spawn.

### 3.7 Settlement notice wording

Rendered in the owner's `<run-notice>` wrapper. Variable part by terminal reason:

```
Run ag-k3x9ab "auditor" (agent) finished and will do no further work unless you send
it more. 2m10s, 14 turns, $0.12.
Run ag-k3x9ab "auditor" was stopped before it finished.           (killed)
Run ag-k3x9ab "auditor" ran out of room before it finished.       (max_tokens / max_turns)
Run ag-k3x9ab "auditor" timed out after 600s.                     (timed_out)
Run ag-k3x9ab "auditor" failed: <detail, 200 bytes>.              (failed)
Run ag-k3x9ab "auditor" declined the task.                        (refused)
Run ag-k3x9ab "auditor" was lost: the session ended while it was running; its
transcript up to the last saved turn is available with runs(id).  (lost)
Run sh-4m2q7z "build" exited 0 after 5m01s.  /  exited 2 ...  /  was killed by SIGTERM.
```

followed by `Result:` and the bounded summary (agents) or `Last output:` (shells, 5
lines), then optional `Structured: {...}` and `Full result: runs(id="ag-k3x9ab",
full=true)` when truncated or when a log exists. Match notice:
`sh-4m2q7z "build" output matched /error/: <line, 300 bytes> (+3 more since, see runs).`
Coalesced overflow: `12 more notices from your runs were dropped to protect your
context; call runs() to see state.`

## 4. Delivery semantics

### 4.1 Inbox

One `Inbox` per run (agents and `main`; shells use a stdin pipe queue with the same
caps). Items:

```rust
pub struct InboxItem {
    pub id: MessageId,         // "m-17", per-hub counter
    pub from: RunId,
    pub kind: DeliveryKind,
    pub text: String,          // <= 16384 bytes
    pub enqueued: Instant,
    pub acks: AckSink,         // reports Delivered and Acted back to the sender's hub view
}
pub enum DeliveryKind {
    Steer,     // next step boundary of a running recipient, wakes an idle one
    Followup,  // after current turn completes; one per turn; wakes an idle one
    Quiet,     // next drain point of any kind, never wakes (host inject equivalent)
    Notice,    // a Notice (section 2.6); wake flag decides
}
```

`send` modes map 1:1 onto `Steer|Followup|Quiet`; `Notice` is produced only by the hub.
It replaces the three channels that exist today for a subagent: `SubagentInbox` as an
`InterruptSource` fed by the UI (`inbox.rs`), `maki.session.notify` on the mailbox
(`live_session.rs:87`), and the per-prompt interrupt in `prompt()`. UI-typed text into a
subagent tab becomes `Steer` from the reserved sender `user`. The existing
`SessionMailbox` remains the physical store for `main` (so the TUI wake path keeps
working) and is generalised so each run has one, keyed by its `RunId`.

### 4.2 Drain points

An agent drains at exactly these points, in order, in `Agent::turn()` (`run.rs:538`):

1. `land_input` (`run.rs:388`) at run start: `Quiet` + `Steer` + at most one `Followup`
   (a wake that started the run supplies that followup as the user message).
2. `prepare_request` (`run.rs:514`), before each model call, after the previous
   `tool_result` message was pushed: all `Steer`, `Quiet` and `Notice` items. The existing
   guard "unless last msg is assistant" is kept and made stricter: nothing is ever
   inserted between an assistant `tool_use` and its `tool_result` block (Anthropic and
   OpenAI reject it). Parallel tool calls (`tool_dispatch.rs:1031`) are all joined first.
3. `handle_queued_command` (`run.rs:1087`) after tool results: if `Steer` is pending the
   loop continues instead of ending the turn (steers extend an open turn, as dsh does).
4. At turn end (`keep_going`, `run.rs:742`): if `Steer` or `Followup` is pending the run
   continues; one `Followup` starts the next turn. A turn never ends with pending Steer.

Items coalesce on drain: all drained items become ONE user-role message, ordered by
`(kind rank Steer < Notice < Quiet, then seq)`, each item framed with sender and id:

```
<run-message from="auditor" id="m-17" delivered_step="4">text</run-message>
```

Notices keep their `<run-notice>` frame. Ordering guarantee: FIFO per (sender, recipient)
pair; across senders, arrival order within a kind. No ordering is promised between a
message and a tool result issued in the same step; the message always lands after it.

### 4.3 Wake rules

| Recipient state | Steer | Followup | Quiet | Notice(wake) | Notice(quiet) |
|---|---|---|---|---|---|
| running | next step | next turn | next drain | next step | next drain |
| idle (agent) | start turn | start turn | wait | start turn | wait |
| main, idle, TUI | mailbox wake via `start_mailbox_runs` | same | wait | same | wait |
| main, idle, headless | hub driver starts a run (4.4) | same | wait | same | wait |
| stopping | dropped, sender told `RECIPIENT_TERMINAL` | dropped | dropped | kept | kept |
| terminal | rejected at `send` | rejected | rejected | n/a | n/a |

A waking item to a recipient whose current run is already cancelled is demoted to
`Followup` (dsh does the same, `agent.ts:154`).

Wake budget (default on, unlike dsh where it is opt-in and jobs-only): at most 3
consecutive notice-initiated turns on `main`; the 4th and later notices are delivered
quiet and a single `Overflow` marker says so. The counter resets when a real user
message lands. For subagents, consecutive peer-wake turns are capped at 8 per 60 s
window; beyond it items degrade to `Quiet` with a sender-visible `RATE_LIMITED`.

### 4.4 Driver loops (subagents and headless main)

Subagent mailboxes never wake today (`land_notes`, `run.rs:409`, drains only inside a
turn). A native agent therefore owns a driver:

```rust
// runs/agent.rs
struct AgentRunner {
    history: tokio-or-smol Mutex<History>,   // owned across runs; Agent<'h> borrows per run
    gauge: ContextGauge,
    inbox: Arc<Inbox>,
    state: watch::Sender<RunState>,
}
async fn drive(self) {
    loop {
        self.inbox.wait_wakeable().await;        // Steer|Followup|wake-Notice, or stop
        if stop { break }
        let input = self.inbox.take_start();     // 4.2 point 1
        let mut agent = Agent::new(self.params()...);   // existing builders
        let outcome = agent.run(input, &mut *self.history.lock().await, ...).await;
        self.settle_or_idle(outcome);            // continuable -> Idle, else terminal
    }
}
```

Only one run executes at a time per agent (the async Mutex serialises a prompt vs an
inbound message, the hazard in the core map). The same pattern, `drive_main`, is the
headless wake consumer: `headless.rs` spawn paths start `hub.drive_main(...)` so `-p`,
ACP and SDK start a run when a waking notice arrives while `main` is idle; and
`hub.join()` is called after the final turn so `maki -p` waits for running agents and
shells (bounded by `--join-timeout`, default 600 s; then stops them, they become
`killed`, not silently dropped).

### 4.5 Cancel domains

`AgentLoop::process_run` ends every run with `subagent_cancels.cancel_all()`
(`maki-ui/src/agent/agent_loop.rs:141`) because a `tool_use_id` is only meaningful within
its run. Native runs are not registered in that `CancelMap`. They hold a child of
`RunHub.cancel`, whose lifetime is the session (dropped on session close or
`hub.shutdown()`); a parent's `stop` and the cascade call the child's trigger. User Esc
on the main run cancels the main turn only, as today. A separate explicit user action
(Esc in a run's tab, `/stop <name>`, quit) calls `hub.stop`. Legacy Lua `task` runs keep
the old per-run map until removal.

### 4.6 Acks

Three stages, reported back to the sender and visible through `runs(id)`:

1. `accepted`: in the recipient's inbox (the `send` result, synchronous, durable in the
   journal in Part B).
2. `delivered`: drained into the recipient's history at step N (`Steer`/`Quiet`) or turn
   N (`Followup`). Event `AgentEvent::RunMessageDelivered{msg, step}`.
3. `acted`: the recipient completed a model response after delivery (first assistant
   message following the drain finished streaming). This proves the text was in a
   request that the model answered; it is not semantic comprehension, which no runtime
   can attest.

The sender sees stage 1 in the tool result and later stages as a `Quiet` notice only
when `delivered` takes longer than 60 s or the message is dropped
(`message m-17 to ag-k3x9ab was dropped: recipient stopped`). Otherwise the stages are
queryable, so no extra context is spent. Improvement over dsh: `messageId` there means
accepted only, and its `agent/inbox/claimed` event is never routed to the sender.

### 4.7 Caps and rate limits (defaults)

| Limit | Default |
|---|---|
| Message size | 16384 bytes, hard reject `TOO_LARGE` |
| Pending items per inbox | 32 (Steer + Followup + Quiet + Notice), `INBOX_FULL retry_after` |
| Pending Followups per inbox | 8 |
| Notice text | 4096 bytes (3072 head, 768 tail) |
| Match notices per shell | 1 per second, then batched; 20 per minute, then `Overflow` |
| Messages per sender to one recipient | 6 per 10 s, token bucket, burst 6 |
| Messages per sender total | 30 per minute |
| Drained bytes per step into one request | 32 KiB, rest stays queued (oldest first) |
| Main notice wake budget | 3 consecutive, reset on user input |
| Subagent peer-wake budget | 8 turns per 60 s |
| `wait` ids | 16; default timeout 60 s; max 600 s |
| `stop` grace | TERM, 5 s, KILL |
| Duplicate collapse | identical `(kind, run, text)` within 30 s collapses into `(xN)` |

Concrete improvements over dsh these give: bounded context growth (dsh closing message
and inbox are unbounded), no steer spam (per-pair bucket), no wake storms (default wake
budget, dsh default is unlimited), truthful acks, collapsed duplicates, and queueing with
visible position instead of a hard `ACTIVATION_LIMIT_REACHED` error.

### 4.8 Notice dedupe and suppression

A settlement or match notice is suppressed when: the owner called `stop` on the run
(truthful reply already delivered); the owner's `wait` released on it (`waiters`); the
owner read its terminal result via `runs(id)`; `notify=never`; the owner is no longer
registered or the cause is hub shutdown. Suppression is by `(run, seq)` under the hub
lock, and the check and the enqueue are one critical section, which closes dsh's race
where a `wait` timing out in the same tick as settlement loses the notice (dsh weakness
12): a timed-out wait does NOT mark the result received, only a delivered result does.
A notice already in the owner's inbox but not yet drained is removed (not just
suppressed at creation) when a consuming read happens first.

## 5. Native-tool mechanism and wiring

### 5.1 New `ToolSource::Native`

`ToolSource` is `{Mcp, Lua}` (`maki-agent/src/tools/registry.rs:81`), and every built-in
today is a bundled Lua plugin; the only `Tool` impls are `LuaTool`
(`maki-lua/src/api/tool.rs:217`) and test mocks. Decision: add

```rust
pub enum ToolSource {
    Mcp { server: Arc<str> },
    Lua { plugin: Arc<str> },
    Native,                       // implemented in maki-agent itself
}
```

and implement the `Tool` trait (`registry.rs:235`) and `ToolInvocation` (`registry.rs:209`)
directly, no new trait. Touch points to update, all enumerated by compile errors on the
new variant: `as_log_field` (`registry.rs:87`), `Route::source/authority`
(`tool_dispatch.rs` ~390-416), docgen `gen_tools.rs`, `is_builtin_tool`
(`tools/mod.rs` ~519) and `ToolFilter::from_config`, `DEFAULT_BUILTINS` in `maki-config`,
UI rendering by name/`tool_kind` in `maki-ui/src/components/tool_display.rs`, and the
permissions `ToolKey::Native` which already exists (`permissions.rs`), so rules like
`spawn = "allow"` work with no new key kind.

Alternative considered and rejected: expose the hub as a Lua API and keep the tools in
Lua (as `task` is). That re-adds the single Lua thread (5 s hold limit), cannot work
headless without the Lua host, and keeps the plugin `run` permission bypass problem for
shell spawning.

### 5.2 Registration

```rust
// maki-agent/src/tools/runs.rs
pub fn native_run_tools(audience: ToolAudience) -> Vec<Arc<dyn Tool>>;  // spawn, send, runs, wait, stop
```

Tools are `Tool` objects with no state; state is reached through `ToolContext`, so one
instance serves all sessions (same as `LuaTool`). They are added to the registry at the
same site that builds the tool list for a frame, before Lua tools, so a plugin cannot
shadow them. `ToolAudience`: all five for `MAIN` and for sub audiences that are allowed
to spawn (depth below max); at the depth limit `spawn(kind=agent)` is rejected in
`start` and `send/runs/wait/stop` remain. The research audience keeps `send`, `runs`,
`wait` and lacks `spawn(shell)` and `stop` of non-children. Because tool defs are part
of the frame fingerprint (`frame.rs:131`), the tool set per audience must be fixed for
the frame's life: depth-based removal is done by the in-tool error, not by changing
the tool list.

`Tool::parse` returns the invocation; `start_header` renders `spawn auditor` etc.;
`permission_scopes` for `spawn(kind=shell)` returns the command scope (reusing bash's
scoping and `force_prompt` for denied-by-default patterns); everything else returns no
scopes. `execute` receives `&ToolContext` and calls the hub.

### 5.3 ToolContext and AgentParams changes

`ToolContext` (`tools/mod.rs:322`) has no hub or identity. Add:

```rust
pub struct ToolContext {
    // ...existing...
    pub runs: Option<Arc<RunHub>>,      // None for `maki index` one-shot and tests
    pub self_run: RunId,                // the calling run; MAIN_RUN for the root agent
}
```

`AgentParams` (`run.rs:102`) gains `runs: Option<Arc<RunHub>>` and `self_run: RunId`
(replacing `task_id` as identity; `task_id` stays for legacy Lua sessions until phase 8).
`Agent::tool_context()` (`run.rs:935`) copies them; `AgentSeed::tool_context`
(`live_session.rs`) does too, so Lua and nested tools calling `batch`/`code_execution`
children carry them (they clone the context). Child agents get `self_run = child id`
and the same hub; authority checks in the router use `self_run`, not the Lua-era
identity-of-live-object test dsh uses.

### 5.4 Hooks into AgentLoop and frontends

* Construction: `AgentSeed` carries `Arc<RunHub>` (created once per root session).
  `maki-ui/src/agent/agent_loop.rs` ~290 (`do_agent_run`) passes it into `AgentParams`.
  `headless.rs:163/:307` does likewise and also spawns `drive_main` (4.4).
* Cancel: `process_run`'s `cancel_all` (`agent_loop.rs:141`) is untouched; hub runs are
  outside that map (4.5). Session close and process exit call `hub.shutdown()`: cascade
  stop with a 5 s hard deadline, then force-settle `killed`/`lost`; shutdown never
  waits for a runner whose cancel did not settle (dsh stall, weakness 11).
* Ledger: each agent run gets `RunLedger::child(&parent_ledger)` (`types.rs:1084`),
  parent = the spawning run's ledger, so cost rolls up one chain and the notice's cost
  is the child's own total. Relays do not re-add ("hand turns up, not bill").
* Events: new `AgentEvent` variants `RunStarted{run}`, `RunStateChanged{id,state,
  progress}`, `RunNotice{notice}`, `RunMessageDelivered{..}`, `RunFinished{..}`, carried
  in `Envelope` with `SubagentInfo` (`types.rs:1121`) stamped for agent runs so the
  existing tab routing (`maki-ui/src/app/mod.rs:1636`, `deliver`) works unchanged.
  The `SubagentInfo.inbox: Arc<SubagentInbox>` field is replaced by a handle that
  `send`s `Steer` from sender `user`. Background envelopes use run_id 0 and rely on the
  stamp (core-map hazard).
* Permissions: a background agent's `PermissionRequest` carries the `SubagentInfo` and
  `answer_tx`; the hub registers it so the UI tab can answer. If no frontend can answer
  (headless without an ACP client) it keeps the existing auto-deny
  (`permissions.rs:755`, no answer channel) and the run gets the deny text, which it can
  report to its parent via `send` instead of stalling. Shell runs reuse the bash
  permission scope at `spawn` time, so prompts happen in the spawning turn, where a
  human is attached, not later in the background.
* Lua: `maki.agent.session` (`maki-lua/src/api/agent.rs:731`) is reimplemented as a thin
  call into `RunHub::spawn_agent` with `Upstream::Chat` mapped to a UI-owned run; its
  logic (`relay_session_events`, history handoff via `SubagentHistory`) is ported into
  `runs/agent.rs`, not duplicated. `maki.fn.job*` keeps its own `JobStore` for plugin
  processes; plugin-visible `maki.run.*` wrappers over the hub are added in phase 8.
* Sibling and ancestor routing: `topology.rs` answers `can_send(from, to)`. Default
  policy: any two runs in the same session tree may message each other; `stop` and
  `runs scope=descendants` are restricted to the caller's own subtree; only an ancestor
  may `stop` a run. Each rejection returns a stable code (section 3.2).
* Tests: the hub is testable with the existing `MockProvider` (`run.rs:1239`) and a
  `StubStreamProvider`; shell runs with `sh -c` stubs; time through an injected clock so
  rate-limit and wake-budget tests have no sleeps (AGENTS.md rule).

## 6. Concurrency, limits and permissions

### 6.1 Capacity pool and queue

One `RunLimits` per hub, loaded from `[agent.runs]` in config (new section in
`maki-config`, so docgen emits it). Defaults:

| Limit | Default | Notes |
|---|---|---|
| `max_agents` | 8 | agents in state `running` per session tree (idle agents hold no slot) |
| `max_shells` | 8 | running shells per session tree |
| `max_shells_per_owner` | 4 | one agent cannot take all shell slots |
| `max_depth` | 2 | main = 0, so main spawns depth 1, which may spawn depth 2, which cannot spawn agents |
| `max_children_per_run` | 8 | direct, non-terminal |
| `max_queue` | 16 | queued runs across the tree; the 17th `spawn` returns `QUEUE_FULL` |
| `max_retained` | 64 | terminal runs kept in the table; oldest evicted (logs and results stay on disk) |
| `idle_timeout` | 30 min | idle agent becomes `completed` |

Only `Running` agents consume an agent slot. `Idle` agents are cheap (a history in
memory, no task executing); they do not count, but are bounded by
a separate `max_idle = 16`, beyond which the least recently active idle agent is
unloaded to disk and marked `completed` (a later `send` cold-resumes it, 7.4). This
resolves the dsh `ACTIVATION_LIMIT_REACHED` problem: capacity gates execution, not
existence. Admission is FIFO with one exception: a child of a running agent is admitted
before a queued sibling of that agent's parent, so a deep tree cannot starve itself
(otherwise 8 running parents each waiting for a queued child deadlock). Rule enforced in
`RunHub::admit`: if every slot is held by agents that are all blocked in `wait` on queued
descendants, the oldest queued descendant is admitted over the cap by one (`max_agents + 1`
overdraft, once). Queue position is shown in the `spawn` result and `runs`. A queued run
that is stopped settles `killed` without ever starting. Slot release and promotion are
one critical section under the hub lock.

Provider concurrency is separate: the existing per-provider retry/rate handling stays,
and 8 agents streaming at once is accepted; a user who hits provider 429s lowers
`max_agents`.

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
4. The tool call of that run (only that run) waits; siblings and main continue. Wait is
   bounded by `permission_timeout` (default 300 s). On timeout the call is denied with
   `permission prompt for <scope> timed out after 300s; ask your parent via send if
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

Per run, optional, set in `spawn` (new optional fields `max_cost_usd`, `max_turns`,
`max_tokens` are NOT added to the schema in v1; the model cannot raise its own limits).
They come from config and apply to every agent run:

| Budget | Default | Effect |
|---|---|---|
| `agent_max_turns` | 100 (existing `MaxTurns`) | terminal `max_turns` |
| `agent_timeout_secs` | none | `timed_out` (from `spawn.timeout_secs`, or config) |
| `subtree_max_cost_usd` | none | when the `RunLedger` root of a spawn subtree exceeds it, the hub `stop`s the subtree with reason `budget`, terminal `killed` detail `cost budget`; owner notice states it |
| `session_max_runs` | 64 spawns per session | `SPAWN_LIMIT`, resets only on a new session |
| shell `timeout_secs` | 3600 default, max 86400 | TERM then KILL, `timed_out` |
| shell log size | 64 MiB per run | beyond it output is discarded, one marker line; process keeps running |

Cost is checked at each `Usage` event (a ledger observer registered at spawn), not by
polling. Default `subtree_max_cost_usd` is unset because pricing is unknown for local
models (cost 0). When set and a model has unknown pricing, the budget is a no-op and
the hub logs once.

### 6.4 Rate limits

`spawn`: at most 10 per rolling 60 s per owner, then `RATE_LIMITED retry_after`
(prevents a loop from filling the queue). `send` and wake limits are in 4.7. Rate
limiters take an injected `Clock` trait (`runs/clock.rs`: `fn now(&self) -> Instant`;
tests use a manual clock), so no test sleeps.

## 7. Lifecycle, persistence and reconciliation

### 7.1 Cancellation tree

The tree is the ownership tree: `main -> children -> grandchildren`. Each run holds a
`CancelToken` that is a `child()` of its parent's token (`cancel.rs`), the root being
`RunHub.cancel`. Cancelling a node cancels all descendants automatically, but the hub
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
   exit is recorded. Non-unix: terminate the job object / `TerminateProcess` with no grace.
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

### 7.2 What user and process events do

| Event | Effect on runs |
|---|---|
| Esc on the main run | cancels the main turn only. Background runs continue. They notify when done and the main agent wakes per the wake budget. |
| Esc in a run's tab | `hub.stop(run, cascade)`; agent tab only; Esc again = hard `forced` settle immediately |
| `/stop <name\|all>` (new command, 8.1) | `hub.stop`; `all` stops every run owned by the session root |
| `/new` | the old session's hub does `shutdown()` (cascade stop, 5 s deadline), then a fresh hub with a fresh root token. Runs do not migrate sessions. |
| session switch (`/resume`, session picker) | same as `/new` for the session being left; its runs settle `killed` (reason `session_closed`) so the journal is clean and a resume shows them stopped, not lost |
| `/reload` (Lua plugins reload) | no effect on native runs; they are not owned by the Lua host. This is a main reason shells are native (today `JobStore` Drop kills them on reload). Lua-owned `maki.fn.job*` still die as before. |
| `/compact` | no effect on runs; the main agent compacts its own history |
| config reload | new limits apply to future admissions only |
| quit (`Ctrl-C` twice, `/quit`) | `shutdown()` with 5 s deadline. Agents `killed`, shells TERM/KILL. Detached shells (below) are the one exception when opted in. |
| `maki -p` finish | `hub.join()` waits for non-terminal runs up to `--join-timeout` (600 s); then `shutdown()`. `-p` exit code is unchanged by child failure. |
| crash / SIGKILL / power loss | nothing runs; journal replay at next start (7.3) |
| SIGTERM / SIGHUP to maki | handled as quit; SIGKILL cannot be handled |

Detached shells: `spawn(kind="shell")` has no `detach` field in v1 (non-goal: runs
survive the process). Shell processes are put in their own process group and the
journal records `pid` and `pgid` plus `boot_id` and process start time, only so a
crash leaves enough to kill orphans on replay (7.3). They are NOT re-adopted by default
(Part A 2.5 permitted re-adoption; see open question 4). Default: orphans are killed,
output is kept.

### 7.3 Journal and `lost` reconciliation

Location: `<state_dir>/runs/<session_id>/` (`maki-storage/src/paths.rs:284`
`state_dir()`), created `0700`:

```
journal.ndjson          append-only event log (the durable source for the run table)
<handle>.log            shell merged stdout+stderr, append-only
<handle>.result.txt     spill of a truncated agent result (2.6)
<handle>.hist.json      agent transcript, written by the existing session storage, see 7.4
```

A `RunJournal` lives in `maki-storage` (leaf crate) as `maki-storage/src/runs.rs`, so
`maki-agent` and the UI can both read it and the session delete path can remove it.
Schema, one JSON object per line, forward compatible by `#[serde(default)]` and
ignoring unknown `t`:

```json
{"v":1,"t":"spawn","ts":1760000000123,"id":"<MakiId>","handle":"ag-k3x9ab","name":"auditor",
 "kind":"agent","owner":"<id>","parent":"<id>","depth":1,"desc":"...","one_shot":false,
 "model":"...","origin_tool_use_id":"toolu_01","notify":"wake","spec":{ ... }}
{"v":1,"t":"state","ts":...,"id":"...","state":"running"}
{"v":1,"t":"msg","ts":...,"id":"m-17","from":"...","to":"...","kind":"steer","bytes":420,"stage":"accepted"}
{"v":1,"t":"ack","ts":...,"id":"m-17","stage":"delivered","step":4}
{"v":1,"t":"notice","ts":...,"seq":12,"run":"...","kind":"settled","delivered":false,"consumed":false}
{"v":1,"t":"term","ts":...,"id":"...","reason":"completed","exit_code":0,"cost":0.12,"turns":14,"elapsed_ms":130000,"result_len":812,"body_ref":null}
{"v":1,"t":"pid","ts":...,"id":"...","pid":4242,"pgid":4242,"start_ticks":123456}
{"v":1,"t":"mark","ts":...,"reason":"clean_shutdown"}
```

Rules:
* Versioning: first line of each file is `{"v":1,"t":"hdr","maki":"0.6.4"}`. Readers accept
  `v <= 1`, ignore unknown record types and unknown fields; a higher `v` makes the file
  read-only for this binary (runs listed `lost`, nothing rewritten).
* Writes: one `write_all` of a line < PIPE_BUF-ish 4 KiB (message text is NOT journaled,
  only size and ids; the message body lives in the recipient's transcript once
  delivered, and pending message bodies are not recoverable by design). `fsync` on
  `spawn`, `term` and `mark`; other records are buffered (flush every 250 ms and on
  shutdown). A torn last line is ignored on replay.
* Compaction: at session start, if the journal exceeds 4 MiB, terminal runs older than
  24 h are dropped from a rewritten file (write temp, rename). Never during a live run.
* The journal records facts, the in-memory `RunTable` is the working state, and a
  `RunTable::from_journal` replay builds the table. Journal write failure degrades to
  memory-only with a one-time warning notice; it never fails a spawn.

Reconciliation (`RunHub::reconcile`, called by the hub constructor for a resumed session,
before any new spawn):
1. Replay the journal into a table. Runs whose last state is non-terminal and the journal
   has no `mark clean_shutdown` after them are suspect. If a clean mark exists, any
   non-terminal run was left by a bug; treated the same.
2. For each suspect agent: state `Lost`, terminal `Lost`, transcript left as is.
3. For each suspect shell with a `pid` record: if the process still exists and its start
   ticks match (guards pid reuse) it is an orphan from a crash: `killpg(SIGTERM)`, 5 s,
   `SIGKILL`; then `Lost` (detail `orphan killed`). Else `Lost` (detail `process gone`).
   (If open question 4 is decided for re-adoption, this branch re-attaches a log tail
   instead.)
4. Append `term` records with reason `lost`, so replay is idempotent.
5. Undelivered, unconsumed `notice` records (settled before the crash but the owner
   never saw them) plus one `Lost` notice per newly lost run are queued for `main`, wake
   flag per original policy, subject to the wake budget. They are delivered on the first
   `claim_wake` after the session is loaded (TUI) or at hub start (headless), as
   `Notice` items rendered with the `lost` wording from 3.7. Because the notice for
   a `Lost` run says what to do (`runs(id)` for the transcript, `send` to cold-resume,
   spawn a new one), the main agent can continue the work.
6. Pending messages (`msg` stage `accepted`, never `delivered`) are listed in the lost
   notice as "N unread messages to X were lost" (bodies are gone, by design).

A resume must also work for sessions written before this feature (no journal): reconciliation
is a no-op, and `subagents` from `StoredSubagent` keep restoring as read-only tabs.

### 7.4 Persistence of agents, cold resume

Transcripts: reuse `Session.subagent_messages` (`maki-storage/src/sessions.rs:423`,
keyed by string) and `subagents: Vec<StoredSubagent>` (`sessions.rs:640`). The key
changes from `tool_use_id` to the run handle for native agents; `StoredSubagent`
gains optional fields, all `#[serde(default, skip_serializing_if)]`:

```rust
pub run: Option<StoredRun>,   // handle, name, parent handle, kind tag, last state, depth, one_shot
```

Old binaries ignore the new field (serde unknown); old sessions load with `run = None`.
Writes use the existing incremental path (`saved_sub_msg_counts`, `sessions.rs:1088`),
and the hub emits the same `AgentEvent::SubagentHistory` at each turn end (not only at
the end of the run) so the transcript is durable per turn and a crash loses at most the
current turn. What is NOT persisted in the session: inboxes, rate-limit buckets, wake
counters, waiters, notices (the journal covers delivery status), and live
processes.

Cold resume: `send` to a run that is `completed`, `lost`, or terminal `killed` from a
previous process, or an idle agent unloaded under `max_idle`, is accepted when its
transcript exists. `RunHub::revive(id)` loads `subagent_messages[handle]` into a new
`AgentRunner` (same `RunId`, handle, name, owner), sets state `Running` (queued if no
slot), writes `state` to the journal, and delivers the message as its first `Steer`.
Constraints: a run that was `completed` with `one_shot=true` is not revivable (stable
code `RECIPIENT_TERMINAL`, matching 3.2); a `failed` agent is revivable only when the
transcript ends on a user or tool_result message that sanitize can repair. Topology
facts are re-told on revive because the child list may have changed (diffed through
`tell_changes`). The system prompt and tool frame are rebuilt for the audience, which
may bust that agent's cache once; acceptable. Shell runs are never revived: spawn
anew. Rule 3.2 `RECIPIENT_TERMINAL` text is updated accordingly: only shells and
one_shot agents return it.

## 8. UI and observability

### 8.1 TUI

* Tabs: unchanged model. `RunStarted` with `SubagentInfo` creates a chat tab through
  `deliver`/`resolve_or_create_chat` (`maki-ui/src/app/mod.rs:1636`), keyed by the run
  handle rather than `tool_use_id`. The tab title is the run name. Shell runs get no
  chat tab; they appear in `/tasks` and the status line only, with a log viewer
  (below).
* `/tasks` (picker): re-implemented in Rust in `maki-ui` over a `RunsView` kept in
  `App`, updated from `RunStateChanged`. Rows: tree indent, name, handle, kind, state
  glyph, age, cost, progress. Keys: Enter focus the tab (agents) or open the shell log
  viewer, `s` stop, `x` stop and forget, `r` resume prompt for finished agents. Replaces
  `plugins/task/picker.lua` and `picker_rows.lua` and `diff_task_states`
  (`maki-ui/src/app/tasks.rs:184`) whose logic (outcome transitions for tab status) is
  ported onto `RunState`.
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
  (4.4). Output: run events are printed on stderr as one line per transition
  (`[run ag-k3x9ab auditor completed 2m10s]`) unless `--quiet`; JSON output mode
  (`print.rs`) adds `run_*` event objects. `hub.join()` waits per 4.4. Unattended
  permissions follow 6.2.
* ACP (`maki-acp/src/server.rs`, `translate.rs`): `RunStateChanged` become ACP
  `tool_call_update` status on the originating `spawn` call, so clients without
  native run support still see progress; permission requests go through
  `permissions.rs` with name attribution. ACP does not expose a run tree in v1.
* SDK (`sdk_mode.rs`): events serialised with the existing envelope plus the new
  variants; additive to the wire format, documented in the SDK schema.
* Logging: every transition is a `tracing` event with `run`, `handle`, `owner`,
  `state`; otel spans per agent run are children of the spawning tool span.

### 8.3 Lua API relation

| API | Decision |
|---|---|
| `maki.agent.session` / `prompt` / `close` (`maki-lua/src/api/agent.rs:731`) | Kept with the same Lua signatures, reimplemented as thin calls to `RunHub::spawn_agent`, `send`, `stop`. `BackgroundSessions` (`maki-lua/src/background.rs:44`) becomes a view over hub runs owned by a plugin. Plugin-owned runs have owner = the plugin's run slot (counts against caps). |
| `maki.task.list/focus/remove` (`maki-lua/src/api/task.rs`) | Kept as wrappers: list from the hub table, focus and remove still round-trip `UiAction::Task` (UI-only concern). |
| `maki.session.notify` (`maki-lua/src/api/session.rs:376`) | Unchanged: it targets the physical `SessionMailbox` which remains the store for `main`. Targeting a native run id is routed to `hub.send(from=plugin, kind=Quiet/Steer by wake flag)`. |
| `maki.fn.jobstart` family (`maki-lua/src/api/fn.rs:259 JobStore`) | Left as is: plugin-private processes with callbacks on the Lua thread, subject to plugin `run` permission and killed on reload/Drop (`fn.rs:693`). Not model-visible. Documented as "use `maki.run.spawn` for model-visible background shells". |
| New `maki.run.*` | `spawn{kind, ...}`, `send`, `stop`, `list`, `wait`, `on(event, fn)`: thin, permission `agent` (existing `Permission::Agent`, `maki-config/src/lib.rs:199`) for agents, `run` for shells. |
| bundled `plugins/task` | Deleted in phase 8 (tool, picker, keymap, `spec.lua`). Config key `plugins.task.*` (`max_concurrent`, `allow_model`) maps to `[agent.runs] max_agents` and `spawn_allow_model` with a one-release deprecation warning. |
| `maki-monitor` | External (not in the repo; the core map confirms nothing in `plugins/` matches). Nothing to delete in-tree; the design replaces its function, and the docs carry a migration table `monitor_start -> spawn(kind=shell, watch=...)`. The only in-tree reference is a comment in `plugins/task/tests`. |

### 8.4 Docs and docgen

`maki-docgen/src/main.rs` PAGES (8 pages) change: tools (`gen_tools.rs`) lists the five
native tools with `ToolSource::Native` and drops `task`; plugins page drops `task`;
configuration page gains `[agent.runs]`; commands gains `/stop`; keybindings gains the
`/tasks` keys; lua-api and `maki.d.luau` gain `maki.run.*`. Every phase that changes
one of these regenerates with `cargo run -p maki-docgen` and commits the output, because
`--check` is a CI gate. A hand-written site page `docs/content/agents.md` (what runs
are, topology, limits, permissions) lands in phase 7.

## 9. Phased implementation plan

Common rules for every phase: strict sequence (a phase assumes only the earlier ones
are merged); no sleeps in tests (injected `Clock`, flume channels, `smol::block_on`);
AGENTS.md style (no trivial comments, constants at top, `test_case`, deps only through
the workspace `Cargo.toml`); one commit per phase, conventional prefix (`feat:`, 2-5
sentences, plain prose, no em-dashes), no push. Regression gates after each phase (see
appendix for the toolchain):

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

### Phase 1: native tool path, RunHub skeleton, ids

Goal: `ToolSource::Native` exists, the five tools are registered and callable, the hub
creates, names and lists runs with no execution yet (`spawn` creates a record in state
`queued` for kind agent/shell and `runs` lists it). No behaviour change for users
because tools are behind `[agent.runs] enabled` (default false in this phase).

Files: `maki-agent/src/runs/{mod,topology,clock}.rs` (new); `maki-agent/src/tools/runs.rs`
(new); `maki-agent/src/tools/registry.rs:81` (variant, `as_log_field` `:87`);
`maki-agent/src/agent/tool_dispatch.rs` (~390-416 `Route::source/authority`);
`maki-agent/src/tools/mod.rs` (`ToolContext` `runs`, `self_run`; `is_builtin_tool`
~519); `maki-agent/src/agent/run.rs` (`AgentParams` `:102`, `tool_context` `:935`);
`maki-agent/src/live_session.rs` (`AgentSeed` hub field); `maki-config/src/lib.rs`
(`[agent.runs]`, `DEFAULT_BUILTINS`); `maki-docgen/src/gen_tools.rs`;
`maki-ui/src/components/tool_display.rs`; `maki-agent/src/lib.rs`.

Interfaces:
```rust
pub struct RunHub; impl RunHub {
  pub fn new(limits: RunLimits, events: EventSender, clock: Arc<dyn Clock>) -> Arc<Self>;
  pub fn spawn(&self, caller: RunId, req: SpawnRequest) -> Result<SpawnAck, RunError>;
  pub fn list(&self, caller: RunId, scope: Scope) -> Vec<RunView>;
  pub fn resolve(&self, caller: RunId, key: &str) -> Result<RunId, RunError>; // id|handle|name|"main"|"parent"
}
pub fn native_run_tools(audience: ToolAudience) -> Vec<Arc<dyn Tool>>;
pub trait Clock: Send + Sync { fn now(&self) -> Instant; }
pub enum RunError { UnknownRun, NotReachable, RecipientTerminal, InboxFull{retry_after:u64}, TooLarge{max:usize}, RateLimited{retry_after:u64}, SelfSend, QueueFull, SpawnLimit, DepthLimit }
```
Steps: (1) add enum variant and fix every compile error; (2) `RunId/RunHandle/RunName`
with handle generation and collision retry, name slug and uniqueness; (3) `RunTable`
with name index and children index; (4) tool structs (`parse`, `start_header`, schema,
description from Part A section 3) wired to hub calls, `wait/send/stop` return a stub
`NOT_IMPLEMENTED` error text; (5) config flag and registration before Lua tools; (6)
`ToolContext.runs/self_run` plumbed from `AgentSeed` and headless; (7) docgen regen.

Tests: `runs::handle_is_prefixed_and_six_chars`, `runs::name_slug_suffixes_on_clash`,
`runs::resolve_main_parent_name_handle`, `runs::depth_limit_rejects_agent_spawn`,
`tools::runs::schema_matches_snapshot` (insta or plain string), `tools::runs::spawn_returns_queued_record`,
`registry::native_tool_logs_source`, `frame::tool_set_stable_across_depth`.
E2E: none (no execution). Verify with the model that the tools appear: start, ask
"call runs with no arguments", expect the tool result `no runs`.
Commit: `feat: add native run tool surface and hub skeleton`.

### Phase 2: native shell jobs

Goal: `spawn(kind=shell)` really runs, logs to disk, settles with exit codes, TERM/KILL
stop, `runs(id)` reads output by offset, `wait` works, `stop` truthful. Notices
(settled) are enqueued in a hub-local queue but delivered in phase 4 (for now `wait` and
`runs` return them).

Files: `maki-agent/src/runs/shell.rs` (new), `runs/notice.rs` (new), `runs/mod.rs`,
`tools/runs.rs`, `maki-storage/src/paths.rs` (runs dir helper, `0700`),
`maki-agent/Cargo.toml` (use `smol::process` already in tree; `libc`/`rustix` only if
already a workspace dependency; check first, else `nix` is not added, use
`std::os::unix::process::CommandExt::process_group`).

Interfaces:
```rust
impl RunHub {
  pub fn stop(&self, caller: RunId, ids: &[RunId], opts: StopOpts) -> Vec<StopOutcome>;
  pub async fn wait(&self, caller: RunId, ids: &[RunId], mode: WaitMode, timeout: Duration, cancel: &CancelToken) -> Vec<RunView>;
  pub fn read_output(&self, id: RunId, from: Option<u64>, tail: usize) -> OutputSlice; // bytes, next_offset
}
pub struct ShellSpec { pub command: String, pub cwd: PathBuf, pub timeout: Duration, pub watch: Option<regex::Regex> }
```
Steps: (1) spawn `sh -c` with `process_group(0)`, merged stdout/stderr into one pipe
(single reader preserves order), pump to `<handle>.log` capped at 64 MiB; (2) settle on
exit with code or signal text; (3) timeout task (injected clock, `timed_out`); (4)
`stop` = TERM, 5 s grace via clock, KILL, `Stopping` then `Killed`; (5) `wait` with
`WaiterSet` registration and the dedupe rules of 4.8 (`consumed` marks); (6) last-5-lines
tail for the result; (7) `watch` regex lines produce `Match` notices with the 1/s rate
limiter; (8) shell stdin pipe for `send` (queue only, wired in phase 4); (9) permission:
`permission_scopes` for shell kind reuse bash's scoping (read `plugins/bash/init.lua`
for the scope logic, port the minimal part, do not change bash).

Tests: `shell::exit_zero_completes`, `shell::nonzero_is_failed`, `shell::output_ordered_in_log`,
`shell::stop_sends_term_then_kill_after_grace` (child that traps TERM, manual clock),
`shell::timeout_marks_timed_out`, `shell::orphan_group_killed` (child spawns grandchild),
`shell::log_cap_discards_and_marks`, `hub::wait_any_returns_first`,
`hub::wait_timeout_does_not_consume_result`, `hub::wait_cancel_does_not_stop_runs`,
`hub::stop_is_idempotent_and_truthful`, `shell::match_notices_rate_limited`.
E2E: ask the model "spawn a shell `for i in 1 2 3; do echo $i; sleep 1; done`, then
wait for it and tell me the last line". Expect tool calls spawn, wait; answer contains 3.
Second: "spawn `sleep 600`, then stop it" expect `stopped`; check no `sleep 600` process
remains by the PID printed by the runner.
Commit: `feat: native background shell runs`.

### Phase 3: background agent driver loop

Goal: `spawn(kind=agent)` runs a real subagent that survives the parent turn, reports
a result, and becomes `idle`; its events stream to the UI as today's subagent tabs.

Files: `maki-agent/src/runs/agent.rs` (new), `runs/mod.rs`, `tools/runs.rs`,
`maki-agent/src/agent/run.rs` (`AgentParams` `self_run`, `runs`; do not touch
`task_id`), `maki-agent/src/types.rs` (`AgentEvent::{RunStarted, RunStateChanged,
RunFinished}`), `maki-ui/src/app/mod.rs:1636` (tab creation keyed by handle),
`maki-ui/src/agent/agent_loop.rs` (no change expected; assert that `cancel_all` `:141`
does not reach hub runs), `maki-ui/src/agent/mod.rs` ~249 (hub creation), `headless.rs`
(hub creation only).

Interfaces:
```rust
pub(crate) struct AgentRunner { /* history, gauge, inbox, cancel, ledger */ }
impl RunHub {
  pub(crate) fn spawn_agent(&self, caller: RunId, spec: AgentSpec) -> Result<SpawnAck, RunError>;
}
pub struct AgentSpec { pub prompt: String, pub audience: ToolAudience, pub model: ModelChoice, pub thinking: Option<Thinking>, pub output_schema: Option<Value>, pub one_shot: bool, pub context: ContextMode }
```
Steps: (1) port the parameter construction from `maki-lua/src/api/agent.rs:731`
(`session()` builds `AgentParams`) into `AgentRunner::params`, reusing `resolve_model`
logic by moving it to maki-agent if needed (Lua then calls the moved fn); (2) child
cancel token from `hub.cancel`; ledger `RunLedger::child`; (3) drive loop of 4.4 with
only the initial prompt (no inbox yet); (4) `structured_output` local tool for
`output_schema`; (5) result capture: last assistant text -> `RunResult` with the 4 KiB
bound and spill; (6) `Idle` after first result unless `one_shot`; (7) event relay: stamp
`SubagentInfo`, port `relay_session_events` (`agent.rs` ~250); (8) UI creates the tab
for `RunStarted`, closes status on `RunFinished`; (9) permission broker with the
auto-deny behaviour only (routing is phase 5).

Tests: `agent_run::completes_with_result_and_goes_idle` (MockProvider), `agent_run::one_shot_completes`,
`agent_run::survives_parent_turn_end` (parent run finishes, `cancel_all` called, child
continues), `agent_run::cost_rolls_up_once`, `agent_run::structured_output_lands_in_result`,
`agent_run::long_result_spills_to_disk`, `agent_run::stop_settles_killed_and_history_sanitized`,
`agent_run::depth_limit_enforced_in_tool`, UI: `app::native_run_creates_tab`,
`app::native_run_tab_keyed_by_handle`.
E2E: "spawn an agent named reader to list the top-level files of this repo and report in
one sentence; then continue and tell me when it returns". Expect a tab, a settled notice
only after phase 4 wake; for now call `wait`. Check parent turn ends without killing the
child (child tab still progressing after parent's final text).
Commit: `feat: native background agent runs`.

### Phase 4: messaging, topology, acks, notices and wake

Goal: `send` works in all directions; notices are delivered with wake semantics in TUI
and headless; topology facts reach the model; acks queryable.

Files: `runs/inbox.rs`, `runs/notice.rs`, `runs/topology.rs`, `runs/agent.rs`,
`maki-agent/src/agent/run.rs` (`land_input` `:388`, `prepare_request` `:514`,
`handle_queued_command` `:1087`, `keep_going` `:742`, `tell_changes` `:460`),
`maki-agent/src/inbox.rs` (`SubagentInbox` becomes an adapter), `maki-agent/src/live_session.rs`
(per-run mailbox), `maki-storage/src/frame.rs` (`LiveFacts.topology`, `context_update`
`:253`), `maki-agent/src/headless.rs` (`drive_main`, `join`), `maki-ui/src/event_loop.rs:1189`.

Interfaces:
```rust
impl RunHub {
  pub fn send(&self, from: RunId, to: RunId, text: String, kind: DeliveryKind) -> Result<MessageId, RunError>;
  pub fn message_status(&self, id: MessageId) -> Option<MessageStatus>;
  pub async fn drive_main(self: Arc<Self>, wake: impl WakeSink);
  pub async fn join(&self, timeout: Duration);
}
pub trait WakeSink { fn start_main_run(&self, preamble: Vec<Message>); }
pub struct TopologyFacts { /* 3.6 */ }
```
Steps: (1) `Inbox` with caps and `Clock`-based buckets (4.7) and dedupe collapse; (2)
drain points 1-4 of 4.2, coalesced `<run-message>` frames, never between tool_use and
tool_result; (3) `AgentRunner` waits on `Inbox::wait_wakeable`, `drive` handles revive of
`Idle`; (4) router `can_send` per 7 policy; (5) `RunNotice` creation under the hub
lock with dedupe/removal rules (4.8), wake budget (4.3); (6) `TopologyFacts` into
`LiveFacts`, rendered via `context_update`; subagent frames call `tell_changes`; (7)
ack stages emit `RunMessageDelivered`, `acted` marker after next assistant message
end; (8) TUI: notices notify the main `SessionMailbox` with the wake flag; headless
`drive_main` starts a run on a waking notice, `join` for `-p`; (9) `stop` cascade
child-first and notice suppression complete.

Tests: `inbox::caps_reject_toolarge_and_full`, `inbox::pair_rate_limit_manual_clock`,
`inbox::dedupe_collapses_identical_within_window`, `inbox::steer_never_between_tool_use_and_result`,
`inbox::steer_extends_open_turn`, `inbox::idle_agent_woken_by_steer`, `inbox::quiet_never_wakes`,
`hub::wake_budget_three_then_overflow_marker`, `hub::notice_removed_when_wait_consumes`,
`hub::sibling_send_routed_and_parent_send_routed`, `hub::stop_only_by_ancestor`,
`topology::fact_changes_only_when_children_change`, `topology::rendering_snapshot_main_and_sub`,
`ack::accepted_delivered_acted_order`, `headless::main_wakes_on_notice_and_join_waits`,
`headless::join_timeout_stops_runs`, UI: `event_loop::run_notice_wakes_idle_main`.
E2E: spawn two agents `a` and `b` where `a` must send its finding to `b` by name and
`b` must report both; confirm in `runs` the acks `delivered`; confirm with `maki -p`
(headless) the process stays alive until both settle and prints the result.
Commit: `feat: run messaging, topology facts and notices`.

### Phase 5: limits, queue, permission routing, budgets

Goal: caps and queue enforced with the defaults of section 6; permission prompts from
background agents reach the user with attribution; budgets and rate limits.

Files: `runs/mod.rs` (`RunLimits`, `admit`), `runs/limits.rs` (new), `maki-config/src/lib.rs`
(`[agent.runs]` full section), `maki-agent/src/permissions.rs` (`PermissionBroker`; do
not change `enforce`, `:755`, `:830`), `maki-agent/src/types.rs` (request carries
run attribution), `maki-ui` permission prompt component (header name), `maki-acp/src/permissions.rs`
(title), `runs/agent.rs`.

Interfaces:
```rust
pub struct RunLimits { pub max_agents:u8, pub max_shells:u8, pub max_shells_per_owner:u8, pub max_depth:u8, pub max_children_per_run:u8, pub max_queue:u16, pub max_retained:u16, pub max_idle:u8, pub idle_timeout:Duration, pub permission_timeout:Duration, pub unattended_permissions:Unattended, pub subtree_max_cost_usd:Option<f64>, pub spawn_per_minute:u8, pub session_max_runs:u16 }
pub enum Unattended { Deny, InheritYolo }
pub struct PermissionBroker; // yields the receiver handed to enforce, plus a registry of pending prompts
```
Steps: (1) `RunLimits::default()` equal to section 6 tables, config deserialisation with
validation (zero values rejected); (2) FIFO `admit` with the child-priority and single
overdraft rule; (3) queue positions in `spawn`/`runs`, promotion on slot release in the
same critical section; (4) per-owner shell cap, children cap, spawn rate; (5) broker:
pending prompt registry, timeout via clock, deny text; the TUI shows the attributed
prompt; ACP maps it; headless applies `Unattended`; (6) `Quiet` waiting-for-permission
notice after 60 s; (7) ledger observer for `subtree_max_cost_usd` and per-run budget
settle; (8) shell log cap already phase 2: add idle unload (`max_idle`, journal-free:
history kept to disk in phase 6, so in this phase unload is deferred and `max_idle`
only evicts oldest to `completed` when `one_shot`-like; document in code).

Tests: `limits::defaults_match_design`, `limits::zero_rejected`, `admit::fifo_and_queue_position`,
`admit::child_admitted_before_unrelated_queued`, `admit::overdraft_once_when_all_blocked_in_wait`,
`admit::queue_full_error`, `admit::stop_queued_never_starts`, `perm::background_prompt_reaches_ui_with_run_name`,
`perm::timeout_denies_and_run_continues`, `perm::unattended_deny_text_mentions_send`,
`perm::headless_inherit_yolo_allows`, `budget::cost_exceeded_stops_subtree`,
`budget::unknown_pricing_is_noop`, `rate::spawn_limit_manual_clock`.
E2E: set `max_agents = 1` in the isolated config; spawn three agents; confirm positions
(`queued position 1 of 2`) and sequential execution; then an agent asked to run
`touch /tmp/e2e/x` with no allow rule: confirm the prompt shows `name (handle)` and
that denying it lets the agent report.
Commit: `feat: run limits, queue and background permission routing`.

### Phase 6: persistence and reconciliation

Goal: journal, per-turn transcript persistence, cold resume, `lost` reconciliation.

Files: `maki-storage/src/runs.rs` (new `RunJournal`), `maki-storage/src/lib.rs`,
`maki-storage/src/sessions.rs` (`StoredRun`, `:640`; per-turn writes; delete path),
`maki-agent/src/runs/journal.rs` (adapter + replay), `runs/mod.rs` (`reconcile`,
`revive`), `maki-ui/src/app/session.rs:166,253-301` (restore uses `StoredRun`, no longer
drops native runs lacking a live process).

Interfaces:
```rust
// maki-storage
pub struct RunJournal; impl RunJournal {
  pub fn open(session: &SessionRef) -> io::Result<Self>;
  pub fn append(&self, rec: &JournalRecord) -> io::Result<()>;
  pub fn replay(path: &Path) -> io::Result<Vec<JournalRecord>>;
}
pub enum JournalRecord { Hdr{..}, Spawn{..}, State{..}, Msg{..}, Ack{..}, Notice{..}, Term{..}, Pid{..}, Mark{..}, Unknown }
impl RunHub { pub fn reconcile(&self) -> Vec<Notice>; pub async fn revive(&self, id: RunId, first: String) -> Result<(), RunError>; }
```
Steps: (1) journal with versioned header, torn-line tolerance, fsync points, compaction
at 4 MiB; (2) hub writes records at every transition; (3) `pid` records for shells
with start ticks (`/proc/<pid>/stat` field 22 on linux; on other platforms record pid
only and treat unknown as gone); (4) `reconcile` per 7.3 steps 1-6 including orphan
kill; (5) `StoredRun` fields and per-turn `SubagentHistory`; (6) `revive`: load
transcript, new runner, same ids; (7) clean-shutdown `mark`; (8) session delete removes
the runs dir; (9) UI restore of native tabs as idle/stopped.

Tests: `journal::replay_ignores_torn_last_line`, `journal::unknown_record_and_future_version`,
`journal::compaction_drops_old_terminal`, `reconcile::running_agent_becomes_lost`,
`reconcile::shell_orphan_killed_when_start_ticks_match`, `reconcile::pid_reuse_not_killed`,
`reconcile::lost_notice_queued_once_idempotent`, `revive::completed_agent_cold_resumes_from_transcript`,
`revive::one_shot_not_revivable`, `storage::stored_run_roundtrip_and_old_session_loads`,
`session::delete_removes_runs_dir`, `ui::restore_native_tab_read_only`.
Crash tests use a fixture journal file, not a real crash (no sleeps, no kills).
E2E: start maki through the PTY runner, spawn an agent that sleeps via shell, kill the
maki PID printed by the runner with SIGKILL (the PID, never by name), restart with
`--continue`, expect a `lost` notice and that the model can `send` to the agent and gets
a reply from the cold-resumed run.
Commit: `feat: run journal, reconciliation and cold resume`.

### Phase 7: TUI and headless observability, docs page

Goal: user-facing surfaces on top of the hub.

Files: `maki-ui/src/app/{tasks.rs,mod.rs,session.rs}` (`RunsView`), new
`maki-ui/src/components/runs_picker.rs`, `maki-ui/src/components/status_bar.rs`,
`maki-ui/src/commands.rs` (`/stop`), keybindings, `maki-ui/src/chat.rs` (notice row
collapse), `src/print.rs`, `maki-acp/src/{translate,server}.rs`, `maki-agent/src/headless.rs`,
`docs/content/agents.md`, docgen outputs.

Interfaces: `App::runs: RunsView`; `Command::Stop(StopTarget)`; ACP `translate`
function `run_event_to_update(&AgentEvent) -> Option<SessionUpdate>`; print flags
`--join-timeout <secs>` and `--quiet-runs`.
Steps: (1) `RunsView` fed by events, `diff_task_states` logic ported (`tasks.rs:184`);
(2) `/tasks` picker in Rust per 8.1; (3) status segment and permission badge;
(4) notice collapse rendering and `delivered` mark; (5) shell log viewer; (6) `/stop`;
(7) Esc in tab = stop with second Esc forced; (8) `-p` stderr lines and JSON events,
`--join-timeout`; (9) ACP mapping; (10) docs page and docgen regen.

Tests: `ui::runs_picker_rows_tree_and_states`, `ui::status_segment_hidden_at_zero`,
`ui::notice_row_collapsed_and_expands`, `ui::esc_in_tab_stops_run_cascade`,
`ui::stop_command_all`, `ui::permission_badge_shown`, `print::run_lines_on_stderr`,
`print::join_timeout_flag_parsed`, `acp::run_state_maps_to_tool_call_update`,
`docgen::check_clean`. Plus insta-style snapshots where the repo already uses them.
E2E: PTY run with three agents and a shell; screenshot-style assertions on
`/tmp/e2e/tui.log` for the status segment and `/tasks` rows; `/stop all` leaves the
table all terminal.
Commit: `feat: runs view, status line and headless run output`.

### Phase 8: Lua re-wiring and removal of the task plugin

Goal: Lua APIs become thin wrappers; `plugins/task` removed; `maki.run.*` added.

Files: `maki-lua/src/api/agent.rs` (session/prompt/close), `maki-lua/src/background.rs`,
`maki-lua/src/api/task.rs`, `maki-lua/src/api/session.rs:376`, new `maki-lua/src/api/run.rs`,
`plugins/task/` (delete: `init.lua`, `picker*.lua`, `plugin.toml`, `tests/spec.lua`),
`maki-config` (deprecation mapping), `maki-docgen`, `plugins/maki.d.luau`, bundled
plugin registry list, `maki-agent` removal of `SubagentInbox` and the legacy `task_id`
paths (`run.rs:102` `task_id`, `subagent_cancels` if unused).

Interfaces (Lua): `maki.run.spawn(opts) -> handle`, `maki.run.send(to, text, mode)`,
`maki.run.stop(id)`, `maki.run.list()`, `maki.run.wait(ids, opts)`, `maki.run.on(name, fn)`.
Steps: (1) reimplement `session()` and `prompt()` over `RunHub`; delete the duplicate
relay; (2) `BackgroundSessions` as a view; (3) `maki.task.*` wrappers; (4) `maki.run.*`
with permissions; (5) remove `plugins/task` and the bundling entries; (6) config
mapping and deprecation warning; (7) remove dead code the compiler reports (never
`#[allow(dead_code)]`); (8) docgen regen; (9) migration paragraph in docs for
`maki-monitor`.

Tests: `lua::agent_session_is_hub_run`, `lua::session_notify_to_native_run_routes_to_hub`,
`lua::task_list_matches_hub`, `lua::run_spawn_requires_permission`, `lua::fn_job_unchanged`,
`lua::reload_does_not_kill_native_shell`, `config::task_options_map_with_warning`,
plugin spec tests for any bundled plugin that used `maki.agent.session`, `docgen::check_clean`.
E2E: full TUI session using the old workflow prompt ("research X with a subagent"); the
model now uses `spawn`; `/tasks` opens; `/reload` mid-run leaves the shell running.
Commit: `feat: lua run api over the native hub and remove task plugin`.

### Phase 9: hardening and e2e soak

Goal: remove shortcuts, prove robustness, final docs.

Files: whatever the soak finds; `maki-agent/tests/runs_soak.rs` (new), `/tmp/e2e`
scenario scripts (not committed), docs.
Steps: (1) property-style test generating random spawn/send/stop sequences against a
`MockProvider` and `ManualClock`, asserting invariants: no non-terminal run with a
terminal ancestor, at most one terminal transition, slot count never exceeds limits
(plus overdraft), no notice delivered after consume; (2) fault injection: runner that
ignores cancel (watchdog forces settle), journal write errors, provider errors mid-turn;
(3) leak check: hub drop joins all tasks; (4) 20-agent fan-out live soak against the
local model with `max_agents` 4; (5) context-growth check: tokens added by notices
bounded per 4.7; (6) default `enabled = true` (flag from phase 1 removed); (7) final
docs sweep; (8) performance: 100 idle runs add no CPU (no polling timers; all waits are
event driven).

Tests: `soak::invariants_hold_over_random_ops`, `soak::runner_ignoring_cancel_force_settled`,
`soak::journal_write_failure_degrades`, `soak::hub_drop_has_no_live_tasks`,
`soak::notice_bytes_bounded`, `hub::no_timers_when_idle`.
E2E: the 20-agent fan-out plus a mid-run Esc, `/new`, quit; verify no orphan processes
from the printed PIDs, clean journal `mark`.
Commit: `feat: harden native runs and enable by default`.

## 10. Risks, open questions, test strategy

### 10.1 Risks

1. Frame cache bust: adding five tools changes the frame fingerprint once. Mitigation:
   fixed tool set per audience (5.2), single release note.
2. History ownership: `Agent<'h>` borrows `&mut History` per run. Mitigation: one runner
   per agent with an async mutex; never two runs concurrently.
3. Deadlock through slot exhaustion (parents waiting on queued children): the
   overdraft rule in 6.1; tested by `admit::overdraft_once_when_all_blocked_in_wait`.
4. Notice injection correctness against provider rules (tool_use/tool_result pairing):
   hard invariant with a dedicated test over both Anthropic and OpenAI shapes.
5. Two registries during migration (phases 3-7 legacy `task` plus hub). Mitigation: the
   flag defaults off until phase 8 completes; the old task plugin is hidden when
   `enabled`.
6. Process groups on non-unix: no grace; documented, tested on unix only.
7. UI regressions in tab keys (handle vs `tool_use_id`): `app::tests.rs` is large;
   keep the old key path alive until phase 8.
8. Cost of local models is 0: cost budgets are inert for them (6.3).
9. Cold resume cache miss and token cost: accepted (7.4).
10. Journal growth: compaction, cap on retained runs.

### 10.2 Open questions for the user

1. Default state of the feature. Options: (a) off until phase 9, then on; (b) on from
   phase 1 for `maki -p` only; (c) on immediately. Recommended: (a), flag
   `[agent.runs] enabled`.
2. Wake budget default. Options: 3 consecutive notice wakes (recommended), unlimited
   like dsh, or 1. Affects how often a background finish interrupts idle time.
3. Background permission policy. Options: (a) route to user with 300 s timeout then deny
   (recommended); (b) auto-deny like today; (c) inherit parent's yolo only. Also the
   headless default: `deny` (recommended) vs `inherit_yolo`.
4. Shell survival across process exit. Options: (a) kill on quit, kill orphans after a
   crash (recommended, safest); (b) allow `detach` and re-adopt on restart (dsh
   does). (b) is a larger scope (log tailing, pid reuse) and a security trade.
5. Default caps: `max_agents` 8, `max_depth` 2, shells per owner 4, queue 16. Accept or
   change? Local-model users may want `max_agents` 2 to 4 (single GPU); a per-provider
   override is an option (recommended: keep global, document).
6. Sibling messaging scope. Options: any run in the session tree (recommended, matches
   the goal), only parent/child, or only via main as relay.
7. Should the model be able to set its own budgets (`max_cost_usd`, `timeout_secs` for
   agents) in `spawn`? Recommended: only `timeout_secs`, budgets from config.
8. `maki-monitor` is not in this repo: the replacement is documented, not deleted. Is
   deleting the user's own plugin copy in scope for this effort? Recommended: no.
9. Config compatibility for `plugins.task.*`. Options: warn and map for one release
   (recommended) vs hard break.
10. Run names visible to users: should `/tasks` allow renaming? Recommended: no in v1.

### 10.3 Test strategy

* Unit: every state transition, inbox rule and limit has a table-driven test with an
  injected `Clock`; no `sleep`, no wall-clock assertions.
* Hub integration: `MockProvider` (`run.rs:1239`) scripted turns; shell stubs via `sh -c`;
  child process groups in a temp dir.
* Persistence: fixture journals (golden files) for every version and corruption case.
* UI: `maki-ui/src/app/tests.rs` helpers (`app_with_two_subagents`) extended; ratatui
  buffer snapshots.
* Headless and ACP: in-process `spawn`/`spawn_interactive` with a stub provider,
  assert event streams and exit behaviour.
* Docs: `docgen --check` in every phase.
* Live e2e: the PTY runner, per phase, against the local model; flaky model output is
  asserted by tool-call names and states, never by exact prose.
* Property soak in phase 9 with deterministic seeds printed on failure.

## Appendix: toolchain recipe

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
