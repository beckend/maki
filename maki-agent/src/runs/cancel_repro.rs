//! Reproductions for "whenever a subagent is canceled, no more sub agents
//! can be spawned".
//!
//! The user path is the `task` plugin: it spawns a native agent run, waits
//! for it, and on Esc the tool call's cancel token fires. The plugin then
//! stops the run with `wait_secs = 0` and the user asks for another
//! subagent. Every test here drives that shape through the hub and asks the
//! only question that matters: does the next spawn actually work?

use std::time::{Duration, Instant};

use serde_json::json;

use super::agent_run::{
    Rig, Script, Step, await_requests, done_reason, reply, rig, rig_with, spawn,
};
use super::*;
use crate::cancel::CancelToken;

/// The hub's own timeout never fires on the manual clock, so a run that
/// never settles would hang the test. This waits with a wall-clock bound.
fn settle_bounded(rig: &Rig, id: UnitId, within: Duration) -> UnitView {
    let deadline = Instant::now() + within;
    loop {
        let view = rig.hub.get(id).unwrap();
        if matches!(view.state, UnitState::Done(_) | UnitState::Idle) {
            return view;
        }
        assert!(
            Instant::now() < deadline,
            "run {} never settled, still {}",
            view.handle,
            view.state.label()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// One agent at a time, so a lost slot shows up as a stuck queue.
fn one_agent() -> UnitLimits {
    UnitLimits {
        max_agents: 1,
        spawn_per_minute: u8::MAX,
        max_children_per_run: u8::MAX,
        max_depth: 4,
        session_max_runs: 1000,
        ..UnitLimits::default()
    }
}

fn state_of(rig: &Rig, id: UnitId) -> String {
    rig.hub.get(id).unwrap().state.label().to_owned()
}

/// A: stop a running agent, then spawn another one.
#[test]
fn stop_a_running_agent_then_spawn_another() {
    let script = Script::new(vec![
        ("FIRST-TASK", vec![Step::Hang]),
        ("SECOND-TASK", vec![Step::Reply(reply("two"))]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();
    let before = rig.hub.slots_in_use();

    let first = spawn(&rig, "FIRST-TASK", json!({}));
    await_requests(&script, "FIRST-TASK", 1);
    assert_eq!(rig.hub.slots_in_use(), before + 1, "the run holds a slot");

    let outcomes = rig.hub.stop(main, &[first.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Ok(StopStatus::Requested));
    let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(
        rig.hub.slots_in_use(),
        before,
        "the killed run gave its slot back"
    );

    let second = spawn(&rig, "SECOND-TASK", json!({}));
    assert!(
        !matches!(second.state, UnitState::Queued),
        "the new run must not queue behind a dead one"
    );
    await_requests(&script, "SECOND-TASK", 1);
    let view = settle_bounded(&rig, second.id, Duration::from_secs(10));
    assert!(
        matches!(view.state, UnitState::Done(_) | UnitState::Idle),
        "the new run must settle, got {}",
        view.state.label()
    );
}

/// B: stop a queued agent, then spawn another one. The freed slot must go
/// to the live run, not to the dead one.
#[test]
fn stop_a_queued_agent_then_spawn_another() {
    let script = Script::new(vec![
        ("FIRST-TASK", vec![Step::Hang]),
        ("SECOND-TASK", vec![Step::Hang]),
        ("THIRD-TASK", vec![Step::Reply(reply("three"))]),
    ]);
    let rig = rig_with(&script, one_agent());
    let main = rig.hub.main();

    let first = spawn(&rig, "FIRST-TASK", json!({}));
    await_requests(&script, "FIRST-TASK", 1);
    let second = spawn(&rig, "SECOND-TASK", json!({}));
    assert!(
        matches!(second.state, UnitState::Queued),
        "the pool is full"
    );

    let outcomes = rig.hub.stop(main, &[second.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Ok(StopStatus::Requested));
    let view = rig.hub.get(second.id).unwrap();
    assert!(
        matches!(&view.state, UnitState::Done(t) if t.reason == DoneReason::Killed),
        "{:?}",
        view.state
    );
    assert!(view.queue.is_none(), "the dead run left the queue");
    assert!(
        !rig.hub.table().pool.holds(second.id),
        "the dead run must not hold a slot"
    );

    let third = spawn(&rig, "THIRD-TASK", json!({}));
    assert!(
        matches!(third.state, UnitState::Queued),
        "the first run still holds the only slot"
    );
    assert!(rig.hub.table().pool.holds(first.id));

    // Freeing the slot must hand it to the live queued run.
    rig.hub.stop(main, &[first.id], StopOpts::default());
    settle_bounded(&rig, first.id, Duration::from_secs(10));
    await_requests(&script, "THIRD-TASK", 1);
    let view = settle_bounded(&rig, third.id, Duration::from_secs(10));
    assert!(
        matches!(view.state, UnitState::Done(_) | UnitState::Idle),
        "the queued run must run once a slot frees, got {}",
        view.state.label()
    );
}

/// C: the real user path. Esc fires the tool call's cancel token, the
/// plugin stops the run with `wait_secs = 0`, and the user asks for
/// another subagent.
#[test]
fn cancel_the_wait_then_stop_then_spawn_another() {
    let script = Script::new(vec![
        ("FIRST-TASK", vec![Step::Hang]),
        ("SECOND-TASK", vec![Step::Reply(reply("two"))]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();

    let first = spawn(&rig, "FIRST-TASK", json!({}));
    await_requests(&script, "FIRST-TASK", 1);

    // Esc during the `task` call: the tool's cancel token fires.
    let (trigger, token) = CancelToken::new();
    trigger.cancel();
    let views = smol::block_on(rig.hub.wait(
        main,
        &[first.id],
        WaitMode::All,
        Duration::from_secs(600),
        &token,
    ));
    assert_eq!(views.len(), 1);
    assert!(
        !matches!(views[0].state, UnitState::Done(_) | UnitState::Idle),
        "the cancelled wait leaves the run alone"
    );

    // The plugin's `maki.run.stop(..., wait_secs = 0)`.
    let outcomes = rig.hub.stop(main, &[first.id], StopOpts::default());
    assert_eq!(outcomes[0].status, Ok(StopStatus::Requested));
    let settled =
        smol::block_on(
            rig.hub
                .wait(main, &[first.id], WaitMode::All, Duration::ZERO, &token),
        );
    assert_eq!(settled.len(), 1);

    let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(rig.hub.slots_in_use(), 0, "the slot came back");

    let second = spawn(&rig, "SECOND-TASK", json!({}));
    assert!(
        !matches!(second.state, UnitState::Queued),
        "the new run must not queue behind the cancelled one"
    );
    await_requests(&script, "SECOND-TASK", 1);
    let view = settle_bounded(&rig, second.id, Duration::from_secs(10));
    assert!(
        matches!(view.state, UnitState::Done(_) | UnitState::Idle),
        "the new run must settle, got {}",
        view.state.label()
    );
}

/// D: the same cancel five times in a row. Capacity must not creep away.
#[test]
fn five_cancels_in_a_row_keep_the_capacity() {
    let script = Script::new(vec![
        ("ALPHA-TASK", vec![Step::Hang]),
        ("BRAVO-TASK", vec![Step::Reply(reply("b"))]),
        ("CHARLIE-TASK", vec![Step::Reply(reply("c"))]),
        ("DELTA-TASK", vec![Step::Reply(reply("d"))]),
        ("ECHO-TASK", vec![Step::Reply(reply("e"))]),
        ("FOXTROT-TASK", vec![Step::Reply(reply("f"))]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();
    let (trigger, token) = CancelToken::new();
    trigger.cancel();

    let routes = [
        "ALPHA-TASK",
        "BRAVO-TASK",
        "CHARLIE-TASK",
        "DELTA-TASK",
        "ECHO-TASK",
        "FOXTROT-TASK",
    ];
    for round in 0..5 {
        let first = spawn(&rig, routes[round], json!({}));
        await_requests(&script, routes[round], 1);
        let _ = smol::block_on(rig.hub.wait(
            main,
            &[first.id],
            WaitMode::All,
            Duration::from_secs(600),
            &token,
        ));
        rig.hub.stop(main, &[first.id], StopOpts::default());
        let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
        assert_eq!(done_reason(&view), DoneReason::Killed, "round {round}");
        assert_eq!(
            rig.hub.slots_in_use(),
            0,
            "round {round}: the slot came back"
        );

        // The next subagent must start.
        let next = spawn(&rig, routes[round + 1], json!({}));
        assert!(
            !matches!(next.state, UnitState::Queued),
            "round {round}: the new run must not queue"
        );
        await_requests(&script, routes[round + 1], 1);
        let view = settle_bounded(&rig, next.id, Duration::from_secs(10));
        assert!(
            matches!(view.state, UnitState::Done(_) | UnitState::Idle),
            "round {round}: the new run must settle, got {}",
            view.state.label()
        );
    }
}

/// The same cancel five times with a single agent slot: the pool must not
/// fill up with ghosts.
#[test]
fn five_cancels_with_one_slot_keep_the_pool_free() {
    let script = Script::new(vec![
        ("ALPHA-TASK", vec![Step::Hang]),
        ("BRAVO-TASK", vec![Step::Reply(reply("b"))]),
        ("CHARLIE-TASK", vec![Step::Reply(reply("c"))]),
        ("DELTA-TASK", vec![Step::Reply(reply("d"))]),
        ("ECHO-TASK", vec![Step::Reply(reply("e"))]),
        ("FOXTROT-TASK", vec![Step::Reply(reply("f"))]),
    ]);
    let rig = rig_with(&script, one_agent());
    let main = rig.hub.main();
    let (trigger, token) = CancelToken::new();
    trigger.cancel();

    let routes = [
        "ALPHA-TASK",
        "BRAVO-TASK",
        "CHARLIE-TASK",
        "DELTA-TASK",
        "ECHO-TASK",
        "FOXTROT-TASK",
    ];
    for round in 0..5 {
        let first = spawn(&rig, routes[round], json!({}));
        await_requests(&script, routes[round], 1);
        let _ = smol::block_on(rig.hub.wait(
            main,
            &[first.id],
            WaitMode::All,
            Duration::from_secs(600),
            &token,
        ));
        rig.hub.stop(main, &[first.id], StopOpts::default());
        let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
        assert_eq!(done_reason(&view), DoneReason::Killed, "round {round}");
        assert_eq!(
            rig.hub.slots_in_use(),
            0,
            "round {round}: the slot came back"
        );
        assert!(
            !rig.hub.table().pool.holds(first.id),
            "round {round}: the dead run must not hold a slot"
        );

        let next = spawn(&rig, routes[round + 1], json!({}));
        assert!(
            !matches!(next.state, UnitState::Queued),
            "round {round}: the new run must not queue"
        );
        await_requests(&script, routes[round + 1], 1);
        let view = settle_bounded(&rig, next.id, Duration::from_secs(10));
        assert!(
            matches!(view.state, UnitState::Done(_) | UnitState::Idle),
            "round {round}: the new run must settle, got {}",
            view.state.label()
        );
    }
}

/// A cancelled run must not count as a live child of its parent: eight
/// cancels would otherwise hit `max_children_per_run` and no subagent could
/// be spawned at all.
#[test]
fn a_cancelled_run_is_not_a_live_child() {
    let script = Script::new(vec![("ALPHA-TASK", vec![Step::Hang])]);
    let rig = rig(&script);
    let main = rig.hub.main();
    let (trigger, token) = CancelToken::new();
    trigger.cancel();

    let first = spawn(&rig, "ALPHA-TASK", json!({}));
    await_requests(&script, "ALPHA-TASK", 1);
    let _ = smol::block_on(rig.hub.wait(
        main,
        &[first.id],
        WaitMode::All,
        Duration::from_secs(600),
        &token,
    ));
    rig.hub.stop(main, &[first.id], StopOpts::default());
    let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    let live = rig
        .hub
        .list(main, Scope::Children)
        .into_iter()
        .filter(|v| !matches!(v.state, UnitState::Done(_)))
        .count();
    assert_eq!(live, 0, "a killed run is not a live child");
}

/// The pool hands the freed slot to a run that is still alive.
#[test]
fn the_freed_slot_goes_to_the_live_run_not_the_dead_one() {
    let script = Script::new(vec![
        ("FIRST-TASK", vec![Step::Hang]),
        ("SECOND-TASK", vec![Step::Hang]),
        ("THIRD-TASK", vec![Step::Reply(reply("three"))]),
    ]);
    let rig = rig_with(&script, one_agent());
    let main = rig.hub.main();

    let first = spawn(&rig, "FIRST-TASK", json!({}));
    await_requests(&script, "FIRST-TASK", 1);
    let second = spawn(&rig, "SECOND-TASK", json!({}));
    assert!(matches!(second.state, UnitState::Queued));

    // Kill the queued one: it must leave the queue for good.
    rig.hub.stop(main, &[second.id], StopOpts::default());
    let view = settle_bounded(&rig, second.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert!(
        rig.hub.get(second.id).unwrap().queue.is_none(),
        "the dead run is still in the queue"
    );

    let third = spawn(&rig, "THIRD-TASK", json!({}));
    assert!(matches!(third.state, UnitState::Queued));
    // Kill the running one: the slot must go to the live queued run.
    rig.hub.stop(main, &[first.id], StopOpts::default());
    settle_bounded(&rig, first.id, Duration::from_secs(10));
    await_requests(&script, "THIRD-TASK", 1);
    assert!(
        rig.hub.table().pool.holds(third.id),
        "the live run holds the freed slot"
    );
    assert!(
        !rig.hub.table().pool.holds(first.id),
        "the dead run still holds a slot"
    );
    assert!(
        matches!(state_of(&rig, third.id).as_str(), "running" | "idle"),
        "the live run must be working, got {}",
        state_of(&rig, third.id)
    );
}

/// The plugin's own path, through the tool layer: `maki.run.spawn`,
/// `maki.run.wait` (cancelled by Esc) and `maki.run.stop(wait_secs = 0)`.
/// This is what `plugins/task/init.lua` does.
#[test]
fn the_plugin_path_after_a_cancel_still_spawns() {
    use crate::tools::runs as native;
    let script = Script::new(vec![
        ("FIRST-TASK", vec![Step::Hang]),
        ("SECOND-TASK", vec![Step::Reply(reply("two"))]),
    ]);
    let mut rig = rig(&script);
    let (trigger, token) = CancelToken::new();
    trigger.cancel();
    // The tool call's cancel token: Esc fires it.
    rig.ctx.cancel = token.clone();

    // maki.run.spawn
    let first = smol::block_on(native::host_spawn(
        &rig.ctx,
        json!({
            "kind": "agent",
            "description": "first",
            "prompt": "FIRST-TASK",
            "subagent_type": "research",
            "one_shot": true,
            "notify": "never",
            "timeout_secs": 600,
        }),
    ))
    .expect("the first spawn works");
    await_requests(&script, "FIRST-TASK", 1);
    let first_id = rig
        .hub
        .resolve(rig.hub.main(), &first.handle)
        .expect("the handle resolves");

    // maki.run.wait, cancelled by Esc.
    let waited = smol::block_on(native::host_wait(
        &rig.ctx,
        json!({"ids": [first.handle], "timeout_secs": 600}),
    ));
    assert!(waited.is_err(), "the cancelled wait reports the cancel");

    // maki.run.stop(..., wait_secs = 0)
    let stopped = smol::block_on(native::host_call(
        &rig.ctx,
        "stop",
        json!({"ids": [first.handle], "wait_secs": 0}),
    ));
    eprintln!("stop said: {stopped:?}");
    let view = settle_bounded(&rig, first_id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(rig.hub.slots_in_use(), 0, "the slot came back");

    // The next subagent.
    let second = smol::block_on(native::host_spawn(
        &rig.ctx,
        json!({
            "kind": "agent",
            "description": "second",
            "prompt": "SECOND-TASK",
            "subagent_type": "research",
            "one_shot": true,
            "notify": "never",
            "timeout_secs": 600,
        }),
    ));
    assert!(
        second.is_ok(),
        "the second spawn failed: {:?}",
        second.as_ref().err()
    );
    let second = second.unwrap();
    await_requests(&script, "SECOND-TASK", 1);
    let second_id = rig
        .hub
        .resolve(rig.hub.main(), &second.handle)
        .expect("the handle resolves");
    let view = settle_bounded(&rig, second_id, Duration::from_secs(10));
    assert!(
        matches!(view.state, UnitState::Done(_) | UnitState::Idle),
        "the new run must settle, got {}",
        view.state.label()
    );
}

/// What the pool must look like after any sequence: only live runs hold a
/// slot, only live runs wait for one, and nothing dead is left in a queue.
fn check_pool(rig: &Rig, what: &str) {
    let table = rig.hub.table();
    let pool = &table.pool;
    for id in pool.holders() {
        let unit = table
            .units
            .get(&id)
            .unwrap_or_else(|| panic!("{what}: a holder is gone from the table"));
        assert!(
            !unit.state.is_done(),
            "{what}: a finished run ({}) still holds a slot",
            unit.handle
        );
    }
    for id in pool.waiting() {
        let unit = table
            .units
            .get(&id)
            .unwrap_or_else(|| panic!("{what}: a waiting run is gone from the table"));
        assert!(
            !unit.state.is_done(),
            "{what}: a finished run ({}) still waits for a slot",
            unit.handle
        );
    }
    for id in pool.blocked_runs() {
        let unit = table
            .units
            .get(&id)
            .unwrap_or_else(|| panic!("{what}: a blocked run is gone from the table"));
        assert!(
            !unit.state.is_done(),
            "{what}: a finished run ({}) is still blocked",
            unit.handle
        );
    }
    assert_eq!(
        pool.held_count(),
        pool.holders().len(),
        "{what}: the holder count does not match"
    );
}

/// A cancel must leave the pool exactly as it was: no holder, no waiter and
/// no blocked run left over.
#[test]
fn a_cancel_leaves_no_pool_ghost() {
    let script = Script::new(vec![
        ("ALPHA-TASK", vec![Step::Hang]),
        ("BRAVO-TASK", vec![Step::Reply(reply("b"))]),
        ("CHARLIE-TASK", vec![Step::Reply(reply("c"))]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();
    let (trigger, token) = CancelToken::new();
    trigger.cancel();

    let first = spawn(&rig, "ALPHA-TASK", json!({}));
    await_requests(&script, "ALPHA-TASK", 1);
    check_pool(&rig, "before the cancel");
    let _ = smol::block_on(rig.hub.wait(
        main,
        &[first.id],
        WaitMode::All,
        Duration::from_secs(600),
        &token,
    ));
    rig.hub.stop(main, &[first.id], StopOpts::default());
    let view = settle_bounded(&rig, first.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    check_pool(&rig, "after the cancel");
    assert_eq!(rig.hub.slots_in_use(), 0);
    assert!(
        rig.hub.table().pool.waiting().is_empty(),
        "a ghost waits for a slot: {:?}",
        rig.hub.table().pool.waiting()
    );
    assert!(
        rig.hub.table().pool.blocked_runs().is_empty(),
        "a ghost is blocked: {:?}",
        rig.hub.table().pool.blocked_runs()
    );
}

/// The cascade: a cancelled parent stops its children, and every slot comes
/// back.
#[test]
fn a_cancelled_parent_cascade_frees_every_slot() {
    let spawn_child = super::agent_run::tool_use(
        "t1",
        "spawn",
        json!({"kind": "agent", "description": "kid", "prompt": "KID-TASK"}),
    );
    let script = Script::new(vec![
        ("PARENT-TASK", vec![Step::Reply(spawn_child), Step::Hang]),
        ("KID-TASK", vec![Step::Hang]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();

    let parent = spawn(&rig, "PARENT-TASK", json!({}));
    await_requests(&script, "PARENT-TASK", 2);
    let kids = rig.hub.list(parent.id, Scope::Children);
    assert_eq!(kids.len(), 1, "the parent spawned a child");
    let kid = kids[0].id;
    assert_eq!(rig.hub.slots_in_use(), 2, "parent and child hold a slot");

    rig.hub.stop(main, &[parent.id], StopOpts::default());
    let view = settle_bounded(&rig, parent.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    let view = settle_bounded(&rig, kid, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(rig.hub.slots_in_use(), 0, "every slot came back");
    check_pool(&rig, "after the cascade");

    // And a new subagent still runs.
    let script2 = Script::new(vec![("NEXT-TASK", vec![Step::Reply(reply("next"))])]);
    let rig2 = rig_with(&script2, UnitLimits::default());
    let next = spawn(&rig2, "NEXT-TASK", json!({}));
    await_requests(&script2, "NEXT-TASK", 1);
    let view = settle_bounded(&rig2, next.id, Duration::from_secs(10));
    assert!(
        matches!(view.state, UnitState::Done(_) | UnitState::Idle),
        "the new run must settle, got {}",
        view.state.label()
    );
}

/// The nested case: a parent that holds a slot waits for its own child. The
/// wait is cancelled (Esc), so the parent gives its slot up and must get it
/// back or the pool must not keep its ghost.
#[test]
fn a_cancelled_wait_by_a_slot_holding_parent_keeps_the_pool_sane() {
    let spawn_child = super::agent_run::tool_use(
        "t1",
        "spawn",
        json!({"kind": "agent", "description": "kid", "prompt": "KID-TASK"}),
    );
    let wait_child =
        super::agent_run::tool_use("t2", "wait", json!({"ids": ["kid"], "timeout_secs": 600}));
    let script = Script::new(vec![
        (
            "PARENT-TASK",
            vec![
                Step::Reply(spawn_child),
                Step::Reply(wait_child),
                Step::Hang,
            ],
        ),
        ("KID-TASK", vec![Step::Hang]),
    ]);
    let rig = rig(&script);
    let main = rig.hub.main();

    let parent = spawn(&rig, "PARENT-TASK", json!({}));
    await_requests(&script, "PARENT-TASK", 2);
    let kids = rig.hub.list(parent.id, Scope::Children);
    assert_eq!(kids.len(), 1);
    let kid = kids[0].id;
    // The parent is waiting for the child: it gave its slot up.
    let deadline = Instant::now() + Duration::from_secs(10);
    while rig.hub.slots_in_use() != 1 {
        assert!(
            Instant::now() < deadline,
            "the parent never gave its slot up"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    check_pool(&rig, "while the parent waits");

    // The parent's turn is cancelled: the wait returns with the cancel.
    rig.hub.stop(main, &[parent.id], StopOpts::default());
    let view = settle_bounded(&rig, parent.id, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    let view = settle_bounded(&rig, kid, Duration::from_secs(10));
    assert_eq!(done_reason(&view), DoneReason::Killed);
    assert_eq!(rig.hub.slots_in_use(), 0, "every slot came back");
    check_pool(&rig, "after the cancelled wait");
    assert!(
        rig.hub.table().pool.waiting().is_empty(),
        "a ghost waits for a slot: {:?}",
        rig.hub.table().pool.waiting()
    );
    assert!(
        rig.hub.table().pool.blocked_runs().is_empty(),
        "a ghost is blocked: {:?}",
        rig.hub.table().pool.blocked_runs()
    );
}

/// The child's budget must survive cancels: `max_children_per_run` counts
/// direct, non-terminal children, so a cancelled run that stays non-terminal
/// would use up the budget and the ninth spawn would fail with SPAWN_LIMIT.
#[test]
fn eight_cancels_do_not_use_up_the_child_budget() {
    let mut routes = Vec::new();
    for route in TASK_ROUTE {
        routes.push((route, vec![Step::Hang]));
    }
    let script = Script::new(routes);
    let rig = rig(&script);
    let main = rig.hub.main();
    let (trigger, token) = CancelToken::new();
    trigger.cancel();

    for (n, route) in TASK_ROUTE.iter().enumerate().take(8) {
        let id = spawn(&rig, route, json!({})).id;
        await_requests(&script, route, 1);
        let _ = smol::block_on(rig.hub.wait(
            main,
            &[id],
            WaitMode::All,
            Duration::from_secs(600),
            &token,
        ));
        rig.hub.stop(main, &[id], StopOpts::default());
        let view = settle_bounded(&rig, id, Duration::from_secs(10));
        assert_eq!(done_reason(&view), DoneReason::Killed, "round {n}");
    }
    let live = rig
        .hub
        .list(main, Scope::Children)
        .into_iter()
        .filter(|v| !v.state.is_done())
        .count();
    assert_eq!(live, 0, "a cancelled run must not stay a live child");
    // The ninth spawn must be admitted.
    let ninth = spawn(&rig, TASK_ROUTE[8], json!({}));
    assert!(
        !matches!(ninth.state, UnitState::Queued),
        "the ninth spawn must not queue"
    );
    await_requests(&script, TASK_ROUTE[8], 1);
}

/// The route names the rounds above use. Distinct, and no name is a prefix
/// of another, so the scripted provider routes each run to its own queue.
const TASK_ROUTE: [&str; 9] = [
    "ALPHA-ONE",
    "BRAVO-TWO",
    "CHARLIE-THREE",
    "DELTA-FOUR",
    "ECHO-FIVE",
    "FOXTROT-SIX",
    "GOLF-SEVEN",
    "HOTEL-EIGHT",
    "INDIA-NINE",
];
