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
* `task` (bundled Lua, `plugins/task`) is removed in phase 4 (Part B). Until then it is
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
(replacing `task_id` as identity; `task_id` stays for legacy Lua sessions until phase 4).
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
  processes; plugin-visible `maki.run.*` wrappers over the hub are added in phase 4.
* Sibling and ancestor routing: `topology.rs` answers `can_send(from, to)`. Default
  policy: any two runs in the same session tree may message each other; `stop` and
  `runs scope=descendants` are restricted to the caller's own subtree; only an ancestor
  may `stop` a run. Each rejection returns a stable code (section 3.2).
* Tests: the hub is testable with the existing `MockProvider` (`run.rs:1239`) and a
  `StubStreamProvider`; shell runs with `sh -c` stubs; time through an injected clock so
  rate-limit and wake-budget tests have no sleeps (AGENTS.md rule).

## PART B (to be written)
