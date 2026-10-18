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
            let first = values.first().map(Value::to_string).unwrap_or_default();
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
}

fn rig(script: &Arc<Script>) -> Rig {
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
    let hub = UnitHub::with_shell_options(
        UnitLimits::default(),
        EventSender::new(tx, 0),
        Arc::new(ManualClock::default()),
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
    assert_eq!(sent.len(), 3, "{text}");
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
        assert_eq!(rig.hub.get(ack.id).unwrap().state.label(), "running");
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
        let items = rig.hub.inbox_of(to).unwrap().drain_step();
        assert_eq!(items.len(), 1);
        let text = crate::runs::inbox::frame(&items, None);
        assert!(text.starts_with("<run-message from=\"work\""), "{text}");
    }
}

mod hub {
    use super::*;

    fn pair(script: &Arc<Script>) -> (Rig, UnitId, UnitId) {
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
