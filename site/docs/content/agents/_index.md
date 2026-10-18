+++
title = "Background Agents"
weight = 24
[extra]
group = "Guides"
+++

# Background Agents

With `[agent.runs]` enabled, the model can start background agents and shell jobs, keep working while they run, and hear back when they finish. Each of them is a run with a handle such as `ag-1a2b3c` or `sh-4d5e6f`. See [Configuration](/docs/configuration/) for the switch and the limits.

```lua
maki.config.agent.runs.enabled = true
```

## What the model gets

| Tool | Purpose |
|------|---------|
| `spawn` | Start an agent or a shell job, in the background |
| `send` | Message a run. A finished agent is revived from its transcript |
| `runs` | List runs and their state, or read a shell run's output by offset |
| `wait` | Block until runs settle or a timeout passes |
| `stop` | Stop a run and everything it started |

A run that finishes leaves a notice for `main`. A notice can wake `main` (when the agent is idle) or wait quietly for its next turn.

## In the terminal

- `/runs` lists every run as a tree with its state and age. Enter on an agent focuses its tab. Enter on a shell run opens a log viewer.
- The log viewer follows the shell's log file as it grows and keeps the last 2000 lines. Arrow keys and page keys scroll, `Esc` or `q` closes it. A shell that has no log file (the log directory could not be written) says so.
- The status bar shows `[runs 3 running 1 queued]`, with `!` when a run waits for a permission answer.
- `/stop <name|handle|all>` stops runs.
- Typing in an agent's tab sends it a message; a finished agent is revived.

## Headless mode

With runs enabled, `maki -p` waits for background runs to finish before it exits. The agent gets extra turns to report on what finished, and stdout carries the last turn's text.

| Flag | Meaning |
|------|---------|
| `--join-timeout <SECS>` | How long to wait for runs before stopping them. Default 600. `MAKI_JOIN_TIMEOUT_SECS` is used when the flag is absent |
| `--quiet-runs` | Do not print run transition lines on stderr |

When the timeout passes, every run is stopped, a line `maki: join timeout of Ns reached, stopped N run(s): handles` goes to stderr, and the exit code is unchanged.

Run transitions go to stderr, so a pipe reading the answer is unaffected:

```
maki: run sh-4d5e6f build started [shell] (from ag-1a2b3c)
maki: run sh-4d5e6f build finished, completed
```

With `--output-format stream-json` or `--verbose`, the output also holds one object per transition:

```json
{"type":"run_started","handle":"sh-4d5e6f","session_id":"...","name":"build","kind":"shell","parent":"ag-1a2b3c","depth":1}
{"type":"run_state","handle":"ag-1a2b3c","session_id":"...","state":"idle"}
{"type":"run_finished","handle":"sh-4d5e6f","session_id":"...","reason":"completed"}
```

`--quiet-runs` silences the stderr lines only; the JSON objects stay. ACP does not print run lines: its notices arrive with the next prompt.
