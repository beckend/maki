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
