//! Native agent runs end to end: a hub, a scripted provider, the real driver.
//! Nothing here sleeps; the provider hands out its answers through gates and
//! the tests wait on hub state.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ProviderEvent, RequestOptions, Role, StopReason,
    StreamResponse, TokenUsage,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::agent::RESULT_LIMIT;
use super::*;
use crate::AgentMode;
use crate::cancel::CancelToken;
use crate::tools::ToolContext;
use crate::tools::test_support::stub_ctx;

const PARENT: &str = "PARENT-SCRIPT";
const CHILD: &str = "CHILD-TASK";

enum Step {
    Reply(StreamResponse),
    Gate(flume::Receiver<()>, StreamResponse),
    Hang,
}

/// Answers by the first user message: each route has its own queue, so two
/// agents sharing the provider never steal each other's answers.
struct Script {
    routes: Mutex<Vec<(&'static str, VecDeque<Step>)>>,
    seen: Mutex<Vec<(String, Vec<Value>)>>,
}

impl Script {
    fn new(routes: Vec<(&'static str, Vec<Step>)>) -> Arc<Self> {
        Arc::new(Self {
            routes: Mutex::new(routes.into_iter().map(|(k, v)| (k, v.into())).collect()),
            seen: Mutex::default(),
        })
    }

    fn requests(&self, route: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| r == route)
            .count()
    }

    fn messages_of(&self, route: &str, n: usize) -> Vec<Value> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| r == route)
            .nth(n)
            .map(|(_, m)| m.clone())
            .unwrap()
    }
}

impl Provider for Script {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let values: Vec<Value> = messages
                .iter()
                .map(|m| serde_json::to_value(m).unwrap())
                .collect();
            // The prompt, not a context update the host put ahead of it.
            let first = values
                .iter()
                .find(|m| {
                    m.get("kind")
                        .and_then(|k| k.get("context_update"))
                        .is_none()
                })
                .map(Value::to_string)
                .unwrap_or_default();
            let step = {
                let mut routes = self.routes.lock().unwrap();
                let route = routes
                    .iter_mut()
                    .find(|(needle, _)| first.contains(needle))
                    .unwrap_or_else(|| panic!("no route for {first}"));
                self.seen.lock().unwrap().push((route.0.to_owned(), values));
                route.1.pop_front().unwrap_or(Step::Hang)
            };
            match step {
                Step::Reply(r) => Ok(r),
                Step::Gate(gate, r) => {
                    let _ = gate.recv_async().await;
                    Ok(r)
                }
                Step::Hang => futures_lite::future::pending().await,
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

fn reply(text: &str) -> StreamResponse {
    response(
        vec![ContentBlock::Text { text: text.into() }],
        StopReason::EndTurn,
        TokenUsage::default(),
    )
}

fn tool_use(id: &str, name: &str, input: Value) -> StreamResponse {
    response(
        vec![ContentBlock::tool_use(id, name, input)],
        StopReason::ToolUse,
        TokenUsage::default(),
    )
}

fn response(content: Vec<ContentBlock>, stop: StopReason, usage: TokenUsage) -> StreamResponse {
    StreamResponse {
        message: Message {
            role: Role::Assistant,
            content,
            ..Default::default()
        },
        usage,
        stop_reason: Some(stop),
        ..Default::default()
    }
}

struct Rig {
    ctx: ToolContext,
    hub: Arc<UnitHub>,
    events: flume::Receiver<Envelope>,
    dir: TempDir,
    clock: Arc<ManualClock>,
}

fn rig(script: &Arc<Script>) -> Rig {
    rig_with(script, UnitLimits::default())
}

fn rig_with(script: &Arc<Script>, limits: UnitLimits) -> Rig {
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.provider = script.clone();
    let registry = crate::tools::ToolRegistry::new();
    registry
        .register_many(
            crate::tools::runs::all()
                .into_iter()
                .map(|tool| (tool, crate::tools::ToolSource::Native)),
        )
        .unwrap();
    ctx.registry = Arc::new(registry);
    let (tx, events) = flume::unbounded::<Envelope>();
    let dir = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::default());
    let hub = UnitHub::with_shell_options(
        limits,
        EventSender::new(tx, 0),
        clock.clone(),
        ShellOptions {
            dir: Some(dir.path().to_owned()),
            ..ShellOptions::default()
        },
    );
    ctx.runs = Some(RunsCtx {
        hub: Arc::clone(&hub),
        self_run: hub.main(),
    });
    Rig {
        ctx,
        hub,
        events,
        dir,
        clock,
    }
}

fn agent_req(prompt: &str, extra: Value) -> SpawnRequest {
    let mut input = json!({"kind": "agent", "description": "work", "prompt": prompt});
    input
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(input).unwrap()
}

fn spawn_as(rig: &Rig, caller: UnitId, req: SpawnRequest) -> SpawnAck {
    let spec = match req.clone().into_kind().unwrap() {
        UnitKind::Agent(spec) => spec,
        UnitKind::Shell(_) => unreachable!(),
    };
    let launch = smol::block_on(AgentLaunch::prepare(&rig.ctx, &spec)).unwrap();
    rig.hub.spawn_agent(caller, req, launch).unwrap()
}

fn spawn(rig: &Rig, prompt: &str, extra: Value) -> SpawnAck {
    spawn_as(rig, rig.hub.main(), agent_req(prompt, extra))
}

/// Settled means `Idle` or `Done`; the manual clock never fires the timeout.
fn settle(rig: &Rig, id: UnitId) -> UnitView {
    let views = smol::block_on(rig.hub.wait(
        rig.hub.main(),
        &[id],
        WaitMode::All,
        Duration::from_secs(10_000_000),
        &CancelToken::none(),
    ));
    views.into_iter().next().unwrap()
}

/// Until the scripted provider has been asked `n` times; bounded, no sleeps.
fn await_requests(script: &Script, route: &str, n: usize) {
    for _ in 0..50_000_000u64 {
        if script.requests(route) >= n {
            return;
        }
        std::thread::yield_now();
    }
    panic!("provider never saw {n} requests on {route}");
}

fn done_reason(view: &UnitView) -> DoneReason {
    match &view.state {
        UnitState::Done(t) => {
            assert!(t.reason != DoneReason::Failed, "failed: {:?}", t.detail);
            t.reason
        }
        other => panic!("not done: {}", other.label()),
    }
}

fn finished_order(rig: &Rig) -> Vec<String> {
    rig.events
        .try_iter()
        .filter_map(|e| match e.event {
            AgentEvent::RunFinished { handle, .. } => Some(handle),
            _ => None,
        })
        .collect()
}

#[test]
fn completes_with_result_and_goes_idle() {
    let script = Script::new(vec![(CHILD, vec![Step::Reply(reply("found it"))])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    let view = settle(&rig, ack.id);
    assert_eq!(view.state.label(), "idle");
    let result = view.result.unwrap();
    assert_eq!(result.summary, "found it");
    assert_eq!(result.turns, 1);
}

#[test]
fn one_shot_completes() {
    let script = Script::new(vec![(CHILD, vec![Step::Reply(reply("all done"))])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({"one_shot": true}));
    let view = settle(&rig, ack.id);
    assert_eq!(done_reason(&view), DoneReason::Completed);
    match view.state {
        UnitState::Done(t) => assert_eq!(t.result.summary, "all done"),
        _ => unreachable!(),
    }
}

#[test]
fn survives_parent_turn_end() {
    let (release, gate) = flume::bounded::<()>(1);
    let script = Script::new(vec![(CHILD, vec![Step::Gate(gate, reply("still here"))])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    // What the frontend does when the parent's turn ends.
    rig.ctx.subagent_cancels.cancel_all();
    assert_eq!(rig.hub.get(ack.id).unwrap().state.label(), "running");
    release.send(()).unwrap();
    let view = settle(&rig, ack.id);
    assert_eq!(view.state.label(), "idle");
    assert_eq!(view.result.unwrap().summary, "still here");
}

#[test]
fn cost_rolls_up_once() {
    let usage = TokenUsage {
        input: 1000,
        output: 100,
        ..Default::default()
    };
    let script = Script::new(vec![(
        CHILD,
        vec![Step::Reply(response(
            vec![ContentBlock::Text { text: "ok".into() }],
            StopReason::EndTurn,
            usage,
        ))],
    )]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    let result = settle(&rig, ack.id).result.unwrap();
    assert_eq!(result.usage.input, 1000);
    let parent = rig.ctx.ledger.totals();
    assert_eq!(parent.usage.input, 1000, "the chain adds a child once");
    assert_eq!(parent.cost, Some(result.cost));
}

#[test]
fn structured_output_lands_in_result() {
    let script = Script::new(vec![(
        CHILD,
        vec![
            Step::Reply(tool_use("t1", "structured_output", json!({"answer": 42}))),
            Step::Reply(reply("done")),
        ],
    )]);
    let rig = rig(&script);
    let schema = json!({"type": "object", "properties": {"answer": {"type": "integer"}}, "required": ["answer"]});
    let ack = spawn(&rig, CHILD, json!({"output_schema": schema}));
    let result = settle(&rig, ack.id).result.unwrap();
    assert_eq!(result.structured, Some(json!({"answer": 42})));
}

#[test]
fn long_result_spills_to_disk() {
    let long = "x".repeat(RESULT_LIMIT * 3);
    let script = Script::new(vec![(CHILD, vec![Step::Reply(reply(&long))])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    let result = settle(&rig, ack.id).result.unwrap();
    assert!(result.truncated);
    assert!(result.summary.len() < long.len());
    let body = result.body_ref.expect("spilled");
    assert!(body.0.starts_with(rig.dir.path()));
    assert_eq!(std::fs::read_to_string(&body.0).unwrap(), long);
}

#[test]
fn wait_returns_on_idle() {
    let (release, gate) = flume::bounded::<()>(1);
    let script = Script::new(vec![(CHILD, vec![Step::Gate(gate, reply("hi"))])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    let waited = smol::block_on(async {
        release.send_async(()).await.unwrap();
        rig.hub
            .wait(
                rig.hub.main(),
                &[ack.id],
                WaitMode::Any,
                Duration::from_secs(10_000_000),
                &CancelToken::none(),
            )
            .await
    });
    assert_eq!(waited[0].state.label(), "idle");
}

#[test]
fn stop_settles_killed_and_history_sanitized() {
    let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    await_requests(&script, CHILD, 1);
    let outcomes = rig.hub.stop(rig.hub.main(), &[ack.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Ok(StopStatus::Requested));
    let view = settle(&rig, ack.id);
    assert_eq!(done_reason(&view), DoneReason::Killed);
    let history: Vec<Message> = rig
        .events
        .try_iter()
        .find_map(|e| match e.event {
            AgentEvent::SubagentHistory { messages, .. } => Some(messages),
            _ => None,
        })
        .expect("the transcript is sent before the state change");
    let calls = history
        .iter()
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolUse { .. }))
        .count();
    let results = history
        .iter()
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolResult { .. }))
        .count();
    assert_eq!(calls, results, "no dangling tool call");
    assert_eq!(script.requests(CHILD), 1);
}

#[test]
fn stop_cascades_children_first() {
    let spawn_child = tool_use(
        "t1",
        "spawn",
        json!({"kind": "agent", "description": "kid", "prompt": CHILD}),
    );
    let script = Script::new(vec![
        (PARENT, vec![Step::Reply(spawn_child), Step::Hang]),
        (CHILD, vec![Step::Hang]),
    ]);
    let rig = rig(&script);
    let parent = spawn(&rig, PARENT, json!({}));
    // The parent's second request means its spawn call has returned.
    await_requests(&script, PARENT, 2);
    let kids = rig.hub.list(parent.id, Scope::Children);
    assert_eq!(kids.len(), 1);
    let kid = kids[0].id;
    let outcomes = rig
        .hub
        .stop(rig.hub.main(), &[parent.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Ok(StopStatus::Requested));
    let view = settle(&rig, parent.id);
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(done_reason(&rig.hub.get(kid).unwrap()), DoneReason::Killed);
    let order = finished_order(&rig);
    let handles = (
        rig.hub.get(kid).unwrap().handle.to_string(),
        view.handle.to_string(),
    );
    assert_eq!(order, [handles.0, handles.1], "child settles before parent");
}

#[test]
fn stopped_agent_makes_no_more_requests() {
    let script = Script::new(vec![(CHILD, vec![Step::Reply(reply("one")), Step::Hang])]);
    let rig = rig(&script);
    let ack = spawn(&rig, CHILD, json!({}));
    settle(&rig, ack.id);
    rig.hub.stop(rig.hub.main(), &[ack.id], StopOpts::default());
    let view = settle(&rig, ack.id);
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(script.requests(CHILD), 1);
}

#[test]
fn fork_excludes_inflight_turn() {
    let script = Script::new(vec![(
        "EARLIER-QUESTION",
        vec![Step::Reply(reply("forked"))],
    )]);
    let rig = rig(&script);
    let earlier = vec![
        Message::user("EARLIER-QUESTION".into()),
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "EARLIER-ANSWER".into(),
            }],
            ..Default::default()
        },
    ];
    rig.hub
        .publish_fork_snapshot(&["toolu_spawn".to_owned()], earlier);
    let mut req = agent_req("FORK-TASK", json!({"context": "fork"}));
    req.origin_tool_use_id = Some(Arc::from("toolu_spawn"));
    let ack = spawn_as(&rig, rig.hub.main(), req);
    settle(&rig, ack.id);
    let sent = script.messages_of("EARLIER-QUESTION", 0);
    let text = serde_json::to_string(&sent).unwrap();
    // The fork prefix, the topology the child is told, then its task.
    assert_eq!(sent.len(), 4, "{text}");
    assert!(text.contains("EARLIER-ANSWER") && text.contains("FORK-TASK"));
    assert!(
        !text.contains("toolu_spawn"),
        "the in-flight call is not copied"
    );
}

#[test]
fn fork_without_a_snapshot_is_rejected() {
    let script = Script::new(vec![(CHILD, vec![])]);
    let rig = rig(&script);
    let req = agent_req(CHILD, json!({"context": "fork"}));
    let spec = match req.clone().into_kind().unwrap() {
        UnitKind::Agent(spec) => spec,
        UnitKind::Shell(_) => unreachable!(),
    };
    let launch = smol::block_on(AgentLaunch::prepare(&rig.ctx, &spec)).unwrap();
    let err = rig
        .hub
        .spawn_agent(rig.hub.main(), req, launch)
        .unwrap_err();
    assert!(err.to_string().starts_with("BAD_FIELD"), "{err}");
}

#[test]
fn wait_and_stop_are_caller_scoped() {
    let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
    let rig = rig(&script);
    let a = spawn(&rig, CHILD, json!({}));
    let b = spawn(&rig, CHILD, json!({"description": "other"}));
    let waited = smol::block_on(rig.hub.wait(
        a.id,
        &[b.id],
        WaitMode::All,
        Duration::from_secs(10_000_000),
        &CancelToken::none(),
    ));
    assert!(waited.is_empty(), "a sibling is out of scope");
    assert!(!rig.hub.can_reach(a.id, b.id));
    let outcomes = rig.hub.stop(a.id, &[b.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Err(UnitError::NotReachable));
    assert_eq!(rig.hub.get(b.id).unwrap().state.label(), "running");
    rig.hub
        .stop(rig.hub.main(), &[a.id, b.id], StopOpts::default());
}

fn say(rig: &Rig, from: UnitId, to: UnitId, text: &str, kind: DeliveryKind) -> SendAck {
    rig.hub.send(from, to, text.to_owned(), kind).unwrap()
}

/// Every request the route saw, as flat strings.
fn seen_text(script: &Script, route: &str) -> Vec<String> {
    script
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter(|(r, _)| r == route)
        .map(|(_, m)| Value::Array(m.clone()).to_string())
        .collect()
}

mod inbox {
    use super::*;

    #[test]
    fn steer_extends_open_turn() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![
                Step::Gate(gate, reply("first")),
                Step::Reply(reply("second")),
            ],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let sent = say(
            &rig,
            rig.hub.main(),
            ack.id,
            "the secret code is X",
            DeliveryKind::Steer,
        );
        assert_eq!(sent.state, "running");
        release.send(()).unwrap();
        let view = settle(&rig, ack.id);
        assert_eq!(view.result.unwrap().summary, "second");
        let second = &seen_text(&script, CHILD)[1];
        assert!(
            second.contains("<run-message from=\\\"main\\\""),
            "{second}"
        );
        assert!(second.contains("the secret code is X"));
        assert!(second.contains("delivered_step"));
    }

    #[test]
    fn steer_never_between_tool_use_and_result() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![
                Step::Gate(gate, tool_use("t1", "runs", json!({}))),
                Step::Reply(reply("done")),
            ],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        say(
            &rig,
            rig.hub.main(),
            ack.id,
            "late note",
            DeliveryKind::Steer,
        );
        release.send(()).unwrap();
        settle(&rig, ack.id);
        let messages = script.messages_of(CHILD, 1);
        let at = messages
            .iter()
            .position(|m| m.to_string().contains("\"tool_use\""))
            .expect("the tool call is in the request");
        assert!(
            messages[at + 1].to_string().contains("tool_result"),
            "the result follows its call directly"
        );
        assert!(
            messages.last().unwrap().to_string().contains("late note"),
            "the note lands after the result"
        );
    }

    #[test]
    fn idle_agent_woken_by_steer() {
        let script = Script::new(vec![(
            CHILD,
            vec![Step::Reply(reply("first")), Step::Reply(reply("second"))],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        assert_eq!(settle(&rig, ack.id).state.label(), "idle");
        let sent = say(
            &rig,
            rig.hub.main(),
            ack.id,
            "again please",
            DeliveryKind::Steer,
        );
        assert_eq!(sent.state, "idle");
        // The woken turn may already be over here, so the state is not asserted.
        let view = settle(&rig, ack.id);
        assert_eq!(view.result.unwrap().summary, "second");
        assert!(seen_text(&script, CHILD)[1].contains("again please"));
    }

    #[test]
    fn idle_agent_woken_by_followup() {
        let script = Script::new(vec![(
            CHILD,
            vec![Step::Reply(reply("first")), Step::Reply(reply("second"))],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        settle(&rig, ack.id);
        say(
            &rig,
            rig.hub.main(),
            ack.id,
            "next job",
            DeliveryKind::Followup,
        );
        assert_eq!(settle(&rig, ack.id).result.unwrap().summary, "second");
    }

    #[test]
    fn quiet_drains_at_next_model_request() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![
                Step::Gate(gate, tool_use("t1", "runs", json!({}))),
                Step::Reply(reply("done")),
            ],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        say(&rig, rig.hub.main(), ack.id, "fyi", DeliveryKind::Quiet);
        assert!(!seen_text(&script, CHILD)[0].contains("fyi"));
        release.send(()).unwrap();
        settle(&rig, ack.id);
        assert!(seen_text(&script, CHILD)[1].contains("fyi"));

        // An idle agent is not woken by a quiet note.
        say(&rig, rig.hub.main(), ack.id, "later", DeliveryKind::Quiet);
        let state = rig.hub.get(ack.id).unwrap().state.label();
        assert_eq!(state, "idle");
        assert_eq!(rig.hub.inbox_of(ack.id).unwrap().len(), 1);
        assert_eq!(script.requests(CHILD), 2);
    }

    /// Whatever the interleaving of sends and the settle, every message
    /// reaches the model and the agent ends up idle with an empty inbox.
    #[test]
    fn message_arriving_during_settle_not_lost() {
        let script = Script::new(vec![(
            CHILD,
            (0..16)
                .map(|n| Step::Reply(reply(&format!("r{n}"))))
                .collect(),
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        let hub = Arc::clone(&rig.hub);
        let id = ack.id;
        let sender = std::thread::spawn(move || {
            for n in 0..6 {
                hub.send(hub.main(), id, format!("note-{n}"), DeliveryKind::Steer)
                    .unwrap();
                std::thread::yield_now();
            }
        });
        sender.join().unwrap();
        let inbox = rig.hub.inbox_of(id).unwrap();
        let mut idle = false;
        for _ in 0..50_000_000u64 {
            let seen = seen_text(&script, CHILD).join("");
            let all = (0..6).all(|n| seen.contains(&format!("note-{n}")));
            idle = rig.hub.get(id).unwrap().state.label() == "idle";
            if all && idle && inbox.is_empty() {
                return;
            }
            std::thread::yield_now();
        }
        panic!(
            "lost a message: idle={idle} state={} requests={} pending={} seen={:?}",
            rig.hub.get(id).unwrap().state.label(),
            script.requests(CHILD),
            inbox.len(),
            seen_text(&script, CHILD)
                .iter()
                .map(|s| s.len())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn child_to_parent_main_lands_in_main_inbox() {
        let script = Script::new(vec![(CHILD, vec![Step::Reply(reply("ok"))])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        settle(&rig, ack.id);
        let to = rig.hub.resolve(ack.id, "parent").unwrap();
        assert_eq!(to, rig.hub.main());
        say(&rig, ack.id, to, "may I?", DeliveryKind::Steer);
        let items: Vec<_> = rig
            .hub
            .inbox_of(to)
            .unwrap()
            .drain_step()
            .into_iter()
            .filter(|i| i.notice.is_none())
            .collect();
        assert_eq!(items.len(), 1);
        let text = crate::runs::inbox::frame(&items, None);
        assert!(text.starts_with("<run-message from=\"work\""), "{text}");
    }
}

mod hub {
    use super::*;

    pub(super) fn pair(script: &Arc<Script>) -> (Rig, UnitId, UnitId) {
        let rig = rig(script);
        let a = spawn(&rig, "PARENT-A", json!({}));
        let b = spawn_as(&rig, a.id, agent_req("CHILD-B", json!({"name": "kid"})));
        (rig, a.id, b.id)
    }

    #[test]
    fn pingpong_guard_holds_and_notifies_parent() {
        let script = Script::new(vec![("PARENT-A", vec![]), ("CHILD-B", vec![])]);
        let (rig, a, b) = pair(&script);
        for n in 0..crate::runs::inbox::PAIR_BURST {
            let sent = say(&rig, b, a, &format!("q{n}"), DeliveryKind::Quiet);
            assert!(!sent.held);
        }
        let held = say(&rig, a, b, "held one", DeliveryKind::Quiet);
        assert!(held.held);
        assert_eq!(rig.hub.inbox_of(b).unwrap().len(), 0);
        let notices = rig.hub.pending_notices(rig.hub.main());
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].text.contains("exchanged 6 messages"));
        // Held again: one notice per burst, not per message.
        say(&rig, a, b, "held two", DeliveryKind::Quiet);
        assert_eq!(rig.hub.pending_notices(rig.hub.main()).len(), 1);
        // Other work by either side releases them.
        rig.hub.note_work(b);
        assert_eq!(rig.hub.inbox_of(b).unwrap().len(), 2);
    }

    #[test]
    fn pingpong_held_messages_are_bounded() {
        let script = Script::new(vec![("PARENT-A", vec![]), ("CHILD-B", vec![])]);
        let (rig, a, b) = pair(&script);
        for n in 0..crate::runs::inbox::PAIR_BURST {
            say(&rig, b, a, &format!("q{n}"), DeliveryKind::Quiet);
        }
        for _ in 0..4 {
            assert!(say(&rig, a, b, "h", DeliveryKind::Quiet).held);
        }
        let err = rig
            .hub
            .send(a, b, "h".into(), DeliveryKind::Quiet)
            .unwrap_err();
        assert!(matches!(err, UnitError::RateLimited { .. }), "{err}");
    }

    #[test]
    fn send_to_sibling_rejected_by_default() {
        let script = Script::new(vec![("PARENT-A", vec![]), ("CHILD-B", vec![])]);
        let rig = rig(&script);
        let a = spawn(&rig, "PARENT-A", json!({}));
        let b = spawn(&rig, "CHILD-B", json!({}));
        let err = rig
            .hub
            .send(a.id, b.id, "hi".into(), DeliveryKind::Steer)
            .unwrap_err();
        assert_eq!(err, UnitError::NotReachable);
    }

    fn siblings(on: bool) -> (Rig, UnitId, UnitId) {
        // Hung in their first request: a running child would drain its inbox
        // under the test.
        let script = Script::new(vec![
            ("SIB-A", vec![Step::Hang]),
            ("SIB-B", vec![Step::Hang]),
        ]);
        let rig = rig_with(
            &script,
            UnitLimits {
                siblings: on,
                ..UnitLimits::default()
            },
        );
        let a = spawn(&rig, "SIB-A", json!({"name": "alpha"}));
        let b = spawn(&rig, "SIB-B", json!({"name": "beta"}));
        await_requests(&script, "SIB-A", 1);
        await_requests(&script, "SIB-B", 1);
        (rig, a.id, b.id)
    }

    #[test]
    fn sibling_send_only_with_config() {
        let (rig, a, b) = siblings(false);
        let err = rig.hub.send(a, b, "hi".into(), DeliveryKind::Steer);
        assert_eq!(err.unwrap_err(), UnitError::NotReachable);
        let (rig, a, b) = siblings(true);
        assert!(!say(&rig, a, b, "hi", DeliveryKind::Steer).held);
        let items = rig.hub.inbox_of(b).unwrap().take_start();
        assert_eq!(items[0].from, a);
        assert_eq!(items[0].from_label, "alpha");
        // Unrelated runs (a cousin under another parent) stay out of reach.
        let cousin = spawn_as(&rig, a, agent_req("SIB-B", json!({"name": "cousin"})));
        let err = rig.hub.send(b, cousin.id, "x".into(), DeliveryKind::Steer);
        assert_eq!(err.unwrap_err(), UnitError::NotReachable);
    }

    #[test]
    fn pingpong_guard_mandatory_with_siblings() {
        let (rig, a, b) = siblings(true);
        for n in 0..crate::runs::inbox::PAIR_BURST {
            let from_to = if n % 2 == 0 { (a, b) } else { (b, a) };
            assert!(!say(&rig, from_to.0, from_to.1, "x", DeliveryKind::Quiet).held);
        }
        // The children run and drain their inboxes, so the length of an inbox
        // proves nothing; the ack record says where the message is.
        let held_state = |rig: &Rig| rig.hub.acks_to(a, b).last().unwrap().state;
        assert!(say(&rig, a, b, "again", DeliveryKind::Quiet).held);
        assert_eq!(held_state(&rig), crate::runs::AckState::Held);
        // Held messages release when either side works, and main hears once.
        assert_eq!(rig.hub.pending_notices(rig.hub.main()).len(), 1);
        rig.hub.note_work(b);
        assert_ne!(held_state(&rig), crate::runs::AckState::Held);
    }

    #[test]
    fn send_errors_are_the_documented_codes() {
        let script = Script::new(vec![(CHILD, vec![])]);
        let rig = rig(&script);
        let a = spawn(&rig, CHILD, json!({}));
        let main = rig.hub.main();
        let send = |to, text: String| rig.hub.send(main, to, text, DeliveryKind::Steer);
        assert_eq!(send(main, "x".into()).unwrap_err(), UnitError::SelfSend);
        let big = "x".repeat(crate::runs::inbox::MAX_MESSAGE_BYTES + 1);
        assert_eq!(
            send(a.id, big).unwrap_err().to_string(),
            "TOO_LARGE max=16384"
        );
        assert_eq!(
            send(UnitId::new(), "x".into()).unwrap_err(),
            UnitError::UnknownRun
        );
        // The flood: 6 per pair, then the bucket says wait.
        for _ in 0..crate::runs::inbox::PAIR_BURST {
            send(a.id, "x".into()).unwrap();
        }
        assert_eq!(
            send(a.id, "x".into()).unwrap_err().to_string(),
            "RATE_LIMITED retry_after=2s"
        );
        rig.hub.stop(main, &[a.id], StopOpts::default());
        assert_eq!(
            send(a.id, "x".into()).unwrap_err(),
            UnitError::RecipientTerminal
        );
    }
}

/// The driver's turn end -> settle -> Idle -> wake sequence, entered at every
/// point by parking the driver (see `Driver::pause`); no sleeps.
mod race {
    use super::*;

    type Held = (flume::Receiver<()>, flume::Sender<()>);

    fn park(rig: &Rig, id: UnitId, at: &'static str) -> Held {
        let (reached_tx, reached_rx) = flume::unbounded();
        let (go_tx, go_rx) = flume::unbounded();
        rig.hub
            .table()
            .agents
            .get(&id)
            .unwrap()
            .pauses
            .lock()
            .unwrap()
            .insert(at, (reached_tx, go_rx));
        (reached_rx, go_tx)
    }

    fn reached(held: &Held) {
        held.0
            .recv_timeout(Duration::from_secs(20))
            .expect("the driver never reached the pause point");
    }

    fn quiesce(rig: &Rig, script: &Script, id: UnitId, requests: usize) {
        for _ in 0..50_000_000u64 {
            let view = rig.hub.get(id).unwrap();
            if script.requests(CHILD) >= requests
                && view.state.label() == "idle"
                && rig.hub.inbox_of(id).unwrap().is_empty()
            {
                return;
            }
            std::thread::yield_now();
        }
        panic!("never quiesced: requests={}", script.requests(CHILD));
    }

    fn count(script: &Script, n: usize, needle: &str) -> usize {
        script
            .messages_of(CHILD, n)
            .iter()
            .map(Value::to_string)
            .collect::<String>()
            .matches(needle)
            .count()
    }

    #[test]
    fn message_at_every_point_of_settle_is_delivered_once() {
        for (point, expect_state) in [("before_settle", "running"), ("after_idle", "idle")] {
            let (release, gate) = flume::bounded::<()>(1);
            let script = Script::new(vec![(
                CHILD,
                vec![Step::Gate(gate, reply("r1")), Step::Reply(reply("r2"))],
            )]);
            let rig = rig(&script);
            let ack = spawn(&rig, CHILD, json!({}));
            await_requests(&script, CHILD, 1);
            let held = park(&rig, ack.id, point);
            release.send(()).unwrap();
            reached(&held);
            let marker = format!("ZZ-{point}");
            let sent = say(&rig, rig.hub.main(), ack.id, &marker, DeliveryKind::Steer);
            assert_eq!(sent.state, expect_state, "{point}");
            held.1.send(()).unwrap();
            quiesce(&rig, &script, ack.id, 2);
            assert_eq!(script.requests(CHILD), 2, "{point}: no extra turn");
            assert_eq!(count(&script, 0, &marker), 0);
            assert_eq!(count(&script, 1, &marker), 1, "{point}: exactly once");
            assert_eq!(rig.hub.get(ack.id).unwrap().state.label(), "idle");
        }
    }

    #[test]
    fn two_messages_across_the_wake_coalesce_into_one_turn() {
        let script = Script::new(vec![(
            CHILD,
            vec![Step::Reply(reply("r1")), Step::Reply(reply("r2"))],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        settle(&rig, ack.id);
        let held = park(&rig, ack.id, "woken");
        let first = say(&rig, rig.hub.main(), ack.id, "ZZ-one", DeliveryKind::Steer);
        assert_eq!(first.state, "idle");
        reached(&held);
        let second = say(
            &rig,
            rig.hub.main(),
            ack.id,
            "ZZ-two",
            DeliveryKind::Followup,
        );
        assert_eq!(second.state, "running", "already woken: no second wake");
        held.1.send(()).unwrap();
        quiesce(&rig, &script, ack.id, 2);
        assert_eq!(script.requests(CHILD), 2);
        assert_eq!(count(&script, 1, "ZZ-one"), 1);
        assert_eq!(count(&script, 1, "ZZ-two"), 1);
    }

    #[test]
    fn quiet_at_settle_never_wakes_and_waits_for_the_next_turn() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![Step::Gate(gate, reply("r1")), Step::Reply(reply("r2"))],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let held = park(&rig, ack.id, "before_settle");
        release.send(()).unwrap();
        reached(&held);
        say(
            &rig,
            rig.hub.main(),
            ack.id,
            "ZZ-quiet",
            DeliveryKind::Quiet,
        );
        held.1.send(()).unwrap();
        settle(&rig, ack.id);
        assert_eq!(rig.hub.inbox_of(ack.id).unwrap().len(), 1);
        assert_eq!(script.requests(CHILD), 1);
        say(&rig, rig.hub.main(), ack.id, "ZZ-wake", DeliveryKind::Steer);
        quiesce(&rig, &script, ack.id, 2);
        assert_eq!(count(&script, 1, "ZZ-quiet"), 1);
        assert_eq!(count(&script, 1, "ZZ-wake"), 1);
    }

    #[test]
    fn stopping_and_done_refuse_and_lose_nothing_silently() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(CHILD, vec![Step::Gate(gate, reply("r1"))])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let held = park(&rig, ack.id, "before_settle");
        release.send(()).unwrap();
        reached(&held);
        let main = rig.hub.main();
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        let err = rig
            .hub
            .send(main, ack.id, "late".into(), DeliveryKind::Steer)
            .unwrap_err();
        assert_eq!(err, UnitError::RecipientTerminal);
        held.1.send(()).unwrap();
        settle(&rig, ack.id);
        let err = rig
            .hub
            .send(main, ack.id, "later".into(), DeliveryKind::Steer)
            .unwrap_err();
        assert_eq!(err, UnitError::RecipientTerminal);
        assert!(rig.hub.inbox_of(ack.id).unwrap().is_empty());
        assert_eq!(script.requests(CHILD), 1);
    }
}

mod topology {
    use super::*;

    fn flat(script: &Script, route: &str, n: usize) -> String {
        Value::Array(script.messages_of(route, n)).to_string()
    }

    #[test]
    fn child_hears_its_place_and_main_hears_its_children() {
        let script = Script::new(vec![
            ("KID-ONE", vec![Step::Reply(reply("one"))]),
            ("KID-TWO", vec![Step::Reply(reply("two"))]),
            (PARENT, vec![]),
        ]);
        let rig = rig(&script);
        let main = rig.hub.main();
        assert_eq!(
            rig.hub.topology_facts(main),
            None,
            "no runs, nothing to tell"
        );
        let one = spawn(&rig, "KID-ONE", json!({"name": "first"}));
        let two = spawn(&rig, "KID-TWO", json!({"name": "second"}));
        settle(&rig, one.id);
        settle(&rig, two.id);

        let text = flat(&script, "KID-ONE", 0);
        assert!(text.contains("You are agent \\\"first\\\""), "{text}");
        assert!(text.contains("Your parent is \\\"main\\\""), "{text}");
        assert!(text.contains("You have no children."), "{text}");
        assert!(
            !text.contains("second"),
            "siblings are not in the fact: {text}"
        );

        let facts = rig.hub.topology_facts(main).unwrap();
        let names: Vec<_> = facts.children.iter().map(|c| c.1.as_str()).collect();
        assert_eq!(names, ["first", "second"]);
    }

    #[test]
    fn no_siblings_in_fact() {
        let script = Script::new(vec![
            ("KID-A", vec![]),
            ("KID-B", vec![]),
            ("KID-C", vec![]),
        ]);
        let rig = rig(&script);
        let a = spawn(&rig, "KID-A", json!({"name": "aa"}));
        let b = spawn(&rig, "KID-B", json!({"name": "bb"}));
        let c = spawn_as(&rig, a.id, agent_req("KID-C", json!({"name": "cc"})));
        let facts = rig.hub.topology_facts(a.id).unwrap();
        assert_eq!(facts.parent.as_ref().unwrap().1, "main");
        assert_eq!(facts.children.len(), 1);
        assert_eq!(facts.children[0].1, "cc");
        let facts = rig.hub.topology_facts(b.id).unwrap();
        assert!(facts.children.is_empty());
        assert_eq!(facts.me.2, 1);
        assert_eq!(rig.hub.topology_facts(c.id).unwrap().me.2, 2);
        let json = serde_json::to_string(&facts).unwrap();
        assert!(!json.contains("aa"), "{json}");
    }

    #[test]
    fn finished_children_leave_the_fact() {
        let script = Script::new(vec![("KID-A", vec![Step::Reply(reply("ok"))])]);
        let rig = rig(&script);
        let a = spawn(&rig, "KID-A", json!({"name": "aa"}));
        settle(&rig, a.id);
        assert!(rig.hub.topology_facts(rig.hub.main()).is_some());
        rig.hub.stop(rig.hub.main(), &[a.id], StopOpts::default());
        settle(&rig, a.id);
        assert_eq!(rig.hub.topology_facts(rig.hub.main()), None);
    }
}

mod ack {
    use super::*;
    use crate::runs::AckState;

    fn state_of(rig: &Rig, from: UnitId, to: UnitId) -> Vec<AckState> {
        rig.hub
            .acks_to(from, to)
            .into_iter()
            .map(|r| r.state)
            .collect()
    }

    #[test]
    fn accepted_then_delivered_order() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![
                Step::Gate(gate, reply("first")),
                Step::Reply(reply("second")),
            ],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let main = rig.hub.main();
        say(&rig, main, ack.id, "note", DeliveryKind::Steer);
        assert_eq!(state_of(&rig, main, ack.id), [AckState::Accepted]);
        release.send(()).unwrap();
        settle(&rig, ack.id);
        let states = state_of(&rig, main, ack.id);
        assert!(
            matches!(states[..], [AckState::Delivered { step: Some(_) }]),
            "{states:?}"
        );
        assert!(seen_text(&script, CHILD)[1].contains("note"));
    }

    #[test]
    fn dropped_when_recipient_stopped_notifies_sender() {
        let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let main = rig.hub.main();
        let sent = say(&rig, main, ack.id, "too late", DeliveryKind::Steer);
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        assert_eq!(
            state_of(&rig, main, ack.id),
            [AckState::Dropped("recipient_stopped")]
        );
        let notices = rig.hub.pending_notices(main);
        let text = notices.iter().map(|n| n.text.as_str()).collect::<String>();
        assert!(
            text.contains(&format!("message {} to ", sent.id))
                && text.contains("was dropped: recipient stopped"),
            "{text}"
        );
        settle(&rig, ack.id);
    }

    #[test]
    fn guard_release_into_full_inbox_is_dropped_and_reported() {
        let script = Script::new(vec![
            ("PARENT-A", vec![Step::Hang]),
            ("CHILD-B", vec![Step::Hang]),
        ]);
        let (rig, a, b) = super::hub::pair(&script);
        await_requests(&script, "PARENT-A", 1);
        await_requests(&script, "CHILD-B", 1);
        for n in 0..crate::runs::inbox::PAIR_BURST {
            say(&rig, b, a, &format!("q{n}"), DeliveryKind::Quiet);
        }
        let held = say(&rig, a, b, "held", DeliveryKind::Quiet);
        assert!(held.held);
        assert_eq!(state_of(&rig, a, b), [AckState::Held]);
        let inbox = rig.hub.inbox_of(b).unwrap();
        for n in 0..crate::runs::inbox::MAX_PENDING {
            let item = InboxItem::new(
                MessageId(900 + n as u64),
                rig.hub.main(),
                "main".into(),
                DeliveryKind::Quiet,
                "x".into(),
                std::time::Instant::now(),
            );
            inbox
                .lock()
                .enqueue_with(item, std::time::Instant::now(), false)
                .unwrap();
        }
        rig.hub.note_work(b);
        assert_eq!(state_of(&rig, a, b), [AckState::Dropped("inbox_full")]);
        let text = rig
            .hub
            .pending_notices(a)
            .iter()
            .map(|n| n.text.clone())
            .collect::<String>();
        assert!(text.contains("was dropped: inbox full"), "{text}");
    }

    #[test]
    fn records_are_bounded_per_sender_and_never_regress() {
        let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        let main = rig.hub.main();
        for n in 0..(crate::runs::ack::MAX_ACKS_PER_SENDER + 5) {
            rig.hub
                .send(main, ack.id, format!("m{n}"), DeliveryKind::Quiet)
                .ok();
        }
        assert!(rig.hub.acks_to(main, ack.id).len() <= crate::runs::ack::MAX_ACKS_PER_SENDER);
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        settle(&rig, ack.id);
    }

    #[test]
    fn slow_delivery_notice_after_sixty_seconds() {
        let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let main = rig.hub.main();
        let sent = say(&rig, main, ack.id, "slow", DeliveryKind::Quiet);
        rig.hub.slow_delivery(main, sent.id, ack.id);
        let text = rig
            .hub
            .pending_notices(main)
            .iter()
            .map(|n| n.text.clone())
            .collect::<String>();
        assert!(text.contains("is not delivered after 60s"), "{text}");
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        settle(&rig, ack.id);
    }

    fn dropped_notices(rig: &Rig, owner: UnitId) -> Vec<String> {
        rig.hub
            .pending_notices(owner)
            .into_iter()
            .map(|n| n.text)
            .filter(|t| t.contains("dropped"))
            .collect()
    }

    fn wait_idle_after(rig: &Rig, script: &Script, id: UnitId, requests: usize) {
        for _ in 0..50_000_000u64 {
            if script.requests(CHILD) >= requests
                && rig.hub.get(id).unwrap().state.label() == "idle"
            {
                return;
            }
            std::thread::yield_now();
        }
        panic!("never idle after {requests} requests");
    }

    #[test]
    fn a_request_cancelled_before_it_returned_never_confirms_the_message() {
        let script = Script::new(vec![(
            CHILD,
            vec![
                Step::Reply(reply("r1")),
                Step::Hang,
                Step::Reply(reply("r3")),
            ],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        settle(&rig, ack.id);
        let main = rig.hub.main();
        // The idle child restarts on it: the message becomes the user message
        // of a request that hangs, then is interrupted.
        say(&rig, main, ack.id, "ZZ-first", DeliveryKind::Steer);
        await_requests(&script, CHILD, 2);
        assert_eq!(state_of(&rig, main, ack.id), [AckState::Accepted]);
        rig.hub.stop(
            main,
            &[ack.id],
            StopOpts {
                soft: true,
                ..StopOpts::default()
            },
        );
        wait_idle_after(&rig, &script, ack.id, 2);
        assert_eq!(
            state_of(&rig, main, ack.id),
            [AckState::Accepted],
            "interrupted, not delivered, not lost"
        );
        // The history kept it; the next request that returns confirms it once.
        say(&rig, main, ack.id, "ZZ-second", DeliveryKind::Steer);
        wait_idle_after(&rig, &script, ack.id, 3);
        let states = state_of(&rig, main, ack.id);
        assert!(
            matches!(states[..], [AckState::Delivered { .. }]),
            "{states:?}"
        );
        assert_eq!(count_in(&script, 2, "ZZ-first"), 1, "carried, not repeated");
        assert_eq!(count_in(&script, 2, "ZZ-second"), 1);
    }

    fn count_in(script: &Script, n: usize, needle: &str) -> usize {
        script
            .messages_of(CHILD, n)
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<String>()
            .matches(needle)
            .count()
    }

    fn pause_at(
        rig: &Rig,
        id: UnitId,
        at: &'static str,
    ) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (reached_tx, reached_rx) = flume::unbounded();
        let (go_tx, go_rx) = flume::unbounded();
        rig.hub
            .table()
            .agents
            .get(&id)
            .unwrap()
            .pauses
            .lock()
            .unwrap()
            .insert(at, (reached_tx, go_rx));
        (reached_rx, go_tx)
    }

    #[test]
    fn stop_before_the_settle_drops_exactly_once() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(CHILD, vec![Step::Gate(gate, reply("r1"))])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let held = pause_at(&rig, ack.id, "before_settle");
        release.send(()).unwrap();
        held.0.recv_timeout(Duration::from_secs(20)).unwrap();
        let main = rig.hub.main();
        say(&rig, main, ack.id, "late", DeliveryKind::Steer);
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        held.1.send(()).unwrap();
        settle(&rig, ack.id);
        assert_eq!(
            state_of(&rig, main, ack.id),
            [AckState::Dropped("recipient_stopped")]
        );
        assert_eq!(dropped_notices(&rig, main).len(), 1);
    }

    #[test]
    fn stop_after_the_restart_keeps_the_delivery() {
        let (release, gate) = flume::bounded::<()>(1);
        let script = Script::new(vec![(
            CHILD,
            vec![Step::Gate(gate, reply("r1")), Step::Reply(reply("r2"))],
        )]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let held = pause_at(&rig, ack.id, "before_settle");
        release.send(()).unwrap();
        held.0.recv_timeout(Duration::from_secs(20)).unwrap();
        let main = rig.hub.main();
        say(&rig, main, ack.id, "early", DeliveryKind::Steer);
        held.1.send(()).unwrap();
        wait_idle_after(&rig, &script, ack.id, 2);
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        settle(&rig, ack.id);
        let states = state_of(&rig, main, ack.id);
        assert!(
            matches!(states[..], [AckState::Delivered { .. }]),
            "{states:?}"
        );
        assert!(dropped_notices(&rig, main).is_empty());
    }

    #[test]
    fn many_drops_make_one_notice() {
        let script = Script::new(vec![(CHILD, vec![Step::Hang])]);
        let rig = rig(&script);
        let ack = spawn(&rig, CHILD, json!({}));
        await_requests(&script, CHILD, 1);
        let main = rig.hub.main();
        for n in 0..5 {
            say(&rig, main, ack.id, &format!("q{n}"), DeliveryKind::Quiet);
        }
        rig.hub.stop(main, &[ack.id], StopOpts::default());
        settle(&rig, ack.id);
        let notices = dropped_notices(&rig, main);
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("5 messages"), "{notices:?}");
        assert_eq!(rig.hub.acks_to(main, ack.id).len(), 5);
    }
}

mod admit {
    use super::*;

    fn one_slot() -> UnitLimits {
        UnitLimits {
            max_agents: 1,
            ..UnitLimits::default()
        }
    }

    #[test]
    fn queued_agents_start_in_order_as_slots_free() {
        let (open_a, gate_a) = flume::bounded::<()>(1);
        let script = Script::new(vec![
            ("AAA", vec![Step::Gate(gate_a, reply("a"))]),
            ("BBB", vec![Step::Reply(reply("b"))]),
            ("CCC", vec![Step::Reply(reply("c"))]),
        ]);
        let rig = rig_with(&script, one_slot());
        let a = spawn(&rig, "AAA", json!({"one_shot": true}));
        let b = spawn(&rig, "BBB", json!({"one_shot": true}));
        let c = spawn(&rig, "CCC", json!({"one_shot": true}));
        assert_eq!((a.queue_position, b.queue_position), (None, Some((1, 1))));
        assert_eq!(c.queue_position, Some((2, 2)));
        await_requests(&script, "AAA", 1);
        assert_eq!(script.requests("BBB") + script.requests("CCC"), 0);
        assert_eq!(
            rig.hub.get(c.id).unwrap().state_label(),
            "queued (position 2 of 2)"
        );
        open_a.send(()).unwrap();
        for id in [a.id, b.id, c.id] {
            assert_eq!(done_reason(&settle(&rig, id)), DoneReason::Completed);
        }
        let order = finished_order(&rig);
        let handles: Vec<String> = [&a, &b, &c].map(|x| x.handle.to_string()).into();
        assert_eq!(order, handles, "FIFO");
        assert_eq!(rig.hub.slots_in_use(), 0);
    }

    #[test]
    fn stop_queued_agent_never_requests() {
        let script = Script::new(vec![
            ("AAA", vec![Step::Hang]),
            ("BBB", vec![Step::Reply(reply("b"))]),
        ]);
        let rig = rig_with(&script, one_slot());
        let a = spawn(&rig, "AAA", json!({}));
        let b = spawn(&rig, "BBB", json!({}));
        await_requests(&script, "AAA", 1);
        rig.hub.stop(rig.hub.main(), &[b.id], StopOpts::default());
        assert_eq!(done_reason(&settle(&rig, b.id)), DoneReason::Killed);
        rig.hub.stop(rig.hub.main(), &[a.id], StopOpts::default());
        assert_eq!(done_reason(&settle(&rig, a.id)), DoneReason::Killed);
        assert_eq!(script.requests("BBB"), 0);
        assert_eq!(rig.hub.slots_in_use(), 0);
    }

    #[test]
    fn timeout_counts_from_start_not_from_queueing() {
        let (open_a, gate_a) = flume::bounded::<()>(1);
        let script = Script::new(vec![
            ("AAA", vec![Step::Gate(gate_a, reply("a"))]),
            ("BBB", vec![Step::Hang]),
        ]);
        let rig = rig_with(&script, one_slot());
        let a = spawn(&rig, "AAA", json!({"one_shot": true}));
        let b = spawn(&rig, "BBB", json!({"timeout_secs": 10}));
        await_requests(&script, "AAA", 1);
        rig.clock.advance(Duration::from_secs(100));
        assert_eq!(rig.hub.get(b.id).unwrap().state.label(), "queued");
        open_a.send(()).unwrap();
        settle(&rig, a.id);
        await_requests(&script, "BBB", 1);
        rig.clock.advance(Duration::from_secs(11));
        let view = settle(&rig, b.id);
        assert_eq!(done_reason(&view), DoneReason::TimedOut);
        assert_eq!(rig.hub.slots_in_use(), 0);
    }

    #[test]
    fn parent_waiting_on_queued_child_does_not_deadlock() {
        let spawn_kid = tool_use(
            "t1",
            "spawn",
            json!({"kind": "agent", "description": "kid", "name": "kid", "prompt": CHILD}),
        );
        let wait_kid = tool_use("t2", "wait", json!({"ids": ["kid"]}));
        let script = Script::new(vec![
            (
                PARENT,
                vec![
                    Step::Reply(spawn_kid),
                    Step::Reply(wait_kid),
                    Step::Reply(reply("parent done")),
                ],
            ),
            (CHILD, vec![Step::Reply(reply("kid done"))]),
        ]);
        let rig = rig_with(&script, one_slot());
        let parent = spawn(&rig, PARENT, json!({}));
        // With one slot the child can only run once the parent's wait gave it up.
        await_requests(&script, CHILD, 1);
        let view = settle(&rig, parent.id);
        assert_eq!(view.result.unwrap().summary, "parent done");
        assert_eq!(script.requests(PARENT), 3);
        rig.hub
            .stop(rig.hub.main(), &[parent.id], StopOpts::default());
        settle(&rig, parent.id);
        assert_eq!(rig.hub.slots_in_use(), 0);
    }
}
