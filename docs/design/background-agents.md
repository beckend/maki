# Background agents: the parent never waits

Status: planned. The implementation plan is Phase 12 of `native-agents.md`; this
document is the model that phase implements, and it stands on its own: a reader
who has not read the reference implementation can build from it.

The reference is `deepseek-harness` (`packages/subagent/`), whose delegation tool
is background-only by construction. The wording quoted here is the harness's, and
it is the contract the model reads.

## 1. What is wrong today

Two defects, both seen in a live session (the run journal of the session that
wrote this plan):

- **A background child parks.** A child spawned with the native `spawn` tool
  ends its turn, goes `Idle` (`maki-agent/src/runs/agent.rs:672-697`) and waits
  for a message that never comes (`agent.rs:713-746`). The notice it leaves says
  *"finished its turn and is idle; send it a message to continue"*
  (`maki-agent/src/runs/notice.rs:154-165`), so the parent has to spend another
  round trip on a result it already has, and the child lingers as a zombie until
  the idle sweep retires it (`maki-agent/src/runs/revive.rs:364-433`).
- **A background child is cut off.** The limit of an agent run that names none
  is the hard-coded `DEFAULT_AGENT_TIMEOUT_SECS` (1800 s,
  `maki-agent/src/runs/mod.rs:77`), filled in by `into_kind`
  (`maki-agent/src/runs/mod.rs:502`). A child that was still working died at
  exactly 1800 s with 177 turns in it. The P9 fix ("a delegated task sets its
  own time limit") only reached the `task` plugin's delegated path. The
  operator's knob `[agent.runs] default_timeout_secs` does not reach an agent
  run at all: the spec always carries a number, so the hub never sees an
  unnamed limit.

Three places also tell the child the opposite of what it must do now: the
topology context update (`maki-agent/src/agent/frame.rs:341`, *"your result is
delivered to your parent automatically when you finish, so do not send it"*), the
`send` tool description (`maki-agent/src/tools/runs.rs:168-175`, *"Your final
result reaches your parent on its own"*) and the sub-agent tone templates
(`maki-agent/src/prompts/research_tone.md`, `general_tone.md`, *"Your entire
response is injected into the parent agent's context"*).

## 2. The model in one paragraph

The model has one way to delegate: it starts a sub-agent and continues. The
sub-agent works in its own context, and it is told, in its own task, that it is
a sub-agent and that it must send its result to the parent before it finishes.
The parent is woken when the child ends, and the notice that wakes it carries
the child's closing text. Nothing blocks: there is no tool that starts a
sub-agent and waits for it.

## 3. What the model sees

One delegation tool, the native `spawn` with `kind: "agent"`. It starts the
child and returns immediately with its handle. The description says so, and
says what the child does with its result:

> `Start background work and return immediately with its id. kind "agent" starts a
> subagent: a separate agent with its own context that works on a self-contained
> task. The child is told to send you its result with `send` before it finishes,
> and you are woken when it ends. kind "shell" starts a command that keeps
> running while you continue. You are notified when a run finishes; do not poll
> or sleep. Start independent runs together in one message. Use `wait` only when
> you are blocked on a result and have nothing else to do, `send` to talk to a
> run, `stop` to end it, `runs` to look. Only a limited number of agents work at
> once; the rest show as queued with a position and start in order. A full queue
> returns QUEUE_FULL: wait for runs to finish.`

The harness's own wording, for comparison:

> `Delegate a self-contained task to a subagent (a separate agent that works in
> its own context) to offload focused, independent work ... This tool starts an
> independently managed subagent and immediately returns its id. The runtime
> notifies you when it finishes. The child reports results with `send_message`;
> use `send_message` to steer it while running or continue its conversation
> after it finishes.`

The parent's prompt carries one line about it, the harness's tool-guidance
section, so a model that spawns several children does the right thing with the
turn that follows:

> `Start independent delegations with `spawn` together in one assistant message
> and continue useful work while they run.`

and the rule the harness states in the same place: **end the turn instead of
sleeping or polling** when only waiting for the results is left. A model that
wants to block anyway may call `wait`; that is its own choice, not the way to
delegate.

## 4. What the child sees

Two things, both in the child's own context, both assembled by the hub when the
run starts (`maki-agent/src/runs/agent.rs:561-576`).

**The return guidance**, appended to the child's first user message, after the
task the parent wrote:

> `You are a sub-agent. Your parent is {parent}. It shares your workspace, but it
> does not see your transcript, your tool output or your reasoning, and it does
> not get your final answer on its own. Before you finish, send your result to
> it with send({ to = "parent", message = "<self-contained result>" }). Send
> another message when a finding changes what the parent should do next.
> Sending does not end your turn.`

`{parent}` is `main` when the parent is the main agent, otherwise the parent's
handle.

**The delegation context**, the line that says what the child may not do:

> `You are a delegated sub-agent. Your permission scope is the one you were
> started with and cannot be widened from inside this session. A call that
> would need the user's approval is denied when nobody can answer it: do not
> retry such a call, and say in your result what you could not do.`

The harness's version of the same line:

> `You are a delegated subagent: your permission scope was fixed when you were
> started and cannot be widened from inside this session — operations that
> require approval are rejected automatically. When the task needs access beyond
> that scope, do not retry the denied operation; state the limitation in your
> reply so the delegating agent can handle it.`

The child's tone templates change with the model: *"Your entire response is
injected into the parent agent's context"* is no longer true. The child writes
its answer into the message it sends, and the parent sees that message.

The topology context update (`maki-agent/src/agent/frame.rs:341`) says the same
thing in one line, because it is what the child reads when the topology changes:

> `Use send(to="parent") to send your result to your parent before you finish.`

## 5. How the child reports

The child calls `send` with `to: "parent"`. The mechanism is already there: `send`
is `ToolAudience::MODEL`, so a research child and a general child both have it
(`maki-agent/src/tools/runs.rs:375-380`), and `UnitHub::send`
(`maki-agent/src/runs/mod.rs:2187-2316`) routes a child-to-parent message into
the parent's inbox with `wake = true`. A running parent reads it at its next
model request; an idle parent starts a turn for it. It reaches the model as

```
<run-message from="reader" id="m-1" delivered_step="3">…</run-message>
```

(`maki-agent/src/runs/inbox.rs:561-571`).

The harness frames the same message as `Agent {id} sent a message: {content}`
(`packages/subagent/subagent/src/continuation-messages.ts:61-72`) and always
delivers it as a *steer*, whatever the parent is doing.

## 6. How the parent is triggered when a child ends

The child ends, the run settles, and the settlement notice wakes the parent.
That path exists (`maki-agent/src/runs/mod.rs:1312-1373` → `:1913-1931` →
`:1935-1970` → `AgentEvent::RunNotice` → `claim_main_wake` at `:2061-2094` →
`maki-ui/src/event_loop.rs:1223-1246` → `maki-ui/src/app/queue.rs:308-318`), and
the notice reaches the model as one observation in the parent's next turn:

```
<run-notice run="ag-xxxxxx" name="reader" seq="3" kind="settled">…</run-notice>
```

(`maki-agent/src/runs/inbox.rs:536-558`).

What changes:

- **A background child ends.** An agent spawn is one-shot by default, so the
  child's first turn is its last and the run settles as `Done`. The idle state,
  and the notice that goes with it (*"finished its turn and is idle; send it a
  message to continue"*), is what a continuable child (`one_shot: false`)
  produces, not what a delegated child produces.
- **A settlement notice always wakes.** The wake budget of three consecutive
  notice-woken turns (`maki-agent/src/runs/mod.rs:688`) exists to stop a notice
  storm from interrupting the user; a settlement carries a result the parent
  asked for, so it is not budgeted. The budget still bounds the other notices.
- **The result arrives twice, on purpose.** The child's own message carries its
  answer, and the settlement notice carries its closing text. The harness does
  the same: the settlement notice is unconditional, it fires even when the child
  never sent anything, and it may duplicate what the child sent.

The harness's settlement wording, which the maki notice should match in shape:

> `Background subagent {id} finished and will do no further work unless you send
> it more.`
> `Its closing message:` + the child's last non-empty assistant text

maki's notice says the same in its own words: `Run {handle} "{name}" (agent)
finished and will do no further work.` plus `Result:` and `Structured:`
(`maki-agent/src/runs/notice.rs:168-201`).

## 7. The limits

| limit | value | where |
|---|---|---|
| agent slots | 4 at a time, the rest queue in order | `max_agents` |
| nesting depth | 2 (a child may spawn a child, that one may not) | `max_depth` |
| children per run | 8 in flight | `max_children_per_run` |
| queue | 16 waiting, then `QUEUE_FULL` | `max_queue` |
| spawns per minute | 10 per owner | `spawn_per_minute` |
| runs per session | 64 | `session_max_runs` |
| wall clock | the session maximum (24 h by default) | `max_timeout_secs` |

The harness's equivalent: 8 active children per root parent, and the message
`subagent limit reached (active child limit: N); wait for an existing child to
finish or complete this work with the current agents`. maki's limits are
enforced by the hub and reported as `SPAWN_LIMIT`, `QUEUE_FULL` and
`RATE_LIMITED`.

## 8. What the model must never do

- Start a sub-agent and wait for it. There is no tool that does both.
- Poll a child with `runs` or `wait` because it was told to.
- Expect the child's transcript. It gets the child's message and its closing
  text, nothing else.
- Assume a child that ended reported anything. A child that was stopped before
  it finished says so in the notice.

## 9. What the fork already has

Most of the mechanism is in place and must merely be kept:

- the child can message its parent (`send` is in every sub-agent's tool list);
- a child's message wakes the parent (`hub.send` with `wake = true`);
- a child's end wakes the parent (the settlement notice path);
- the notice carries the child's result and its structured result;
- the run is journalled, so `/runs` and the journal show what happened.

What is missing is the instruction, the tool surface and the one-shot default.

## 10. Differences from deepseek-harness, and why

- **The harness has no `wait` and no `idle`; maki keeps both.** A maki child
  can be continued (`send` to a live child, or a revived one from its
  transcript), and the model may block on a result with `wait`. The harness
  dropped the foreground variant; maki keeps the capability but removes it from
  the *delegation* path, so the default is the same even though the surface is
  bigger.
- **The harness delivers the child's result as a user message; maki delivers it
  as a run notice.** maki's inbox frames a run notice as one observation in the
  next turn, and the child's own message as a relayed message. The shape is the
  same, the framing is maki's.
- **The harness has no wall clock on a child; maki has one.** A maki child that
  is still working after its limit is stopped and says so in the notice
  (*"ran out of room before it finished"* in the harness, `timed_out` in maki).
  The limit is the session maximum by default so that it does not land in the
  middle of real work.
- **The harness's child may be continuable or one-shot; maki's background child
  is one-shot by default.** A continuable child is still available with
  `one_shot: false`.
- **The harness's parent system prompt carries the delegation line; maki's
  carries it too**, as a slot contribution, so a plugin can replace it.

## 11. The test plan

- the child's first user message carries the return guidance and names the
  parent;
- a background child ends after its turn and the settlement notice wakes the
  parent;
- the topology line and the `send` description tell the child to report, not the
  opposite;
- the tone templates no longer promise the parent an injected response;
- the model-facing tool list has no `task` tool while native runs are enabled,
  and still has it when they are not;
- an agent spawn without a limit gets the session maximum;
- a live run: the model spawns a background child, the child sends its result,
  the parent is woken and answers with it.
