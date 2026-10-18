//! Notice delivery, consumption by commit, dedupe and the wake budget
//! (design 4.3, 4.8). Runs are agent records with no driver, settled by hand,
//! so nothing here depends on a process or on time.

use std::sync::Arc;
use std::time::Duration;

use super::inbox::{self, MAX_PENDING};
use super::*;
use crate::Envelope;

struct Rig {
    hub: Arc<UnitHub>,
    clock: Arc<ManualClock>,
    events: flume::Receiver<Envelope>,
}

fn rig() -> Rig {
    let (tx, events) = flume::unbounded::<Envelope>();
    let clock = Arc::new(ManualClock::default());
    let hub = UnitHub::new(
        UnitLimits::default(),
        EventSender::new(tx, 0),
        clock.clone(),
    );
    Rig { hub, clock, events }
}

fn run(rig: &Rig, policy: NoticePolicy) -> UnitId {
    let mut req = tests::request(RunKind::Agent, "work");
    req.notify = Some(policy);
    rig.hub.spawn(rig.hub.main(), req).unwrap().id
}

fn terminal(summary: &str) -> Terminal {
    Terminal {
        reason: DoneReason::Completed,
        exit_code: None,
        detail: None,
        result: UnitResult {
            summary: summary.to_owned(),
            ..UnitResult::default()
        },
    }
}

fn finish(rig: &Rig, id: UnitId) {
    rig.hub.settle(id, terminal("ok"), 0);
}

fn main_inbox(rig: &Rig) -> Arc<inbox::Inbox> {
    rig.hub.inbox_of(rig.hub.main()).unwrap()
}

fn claim_of(rig: &Rig, id: UnitId) -> Claim {
    let view = rig.hub.get(id).unwrap();
    Claim {
        runs: vec![(id, view.settle_gen)],
        seqs: Vec::new(),
    }
}

fn set_state(rig: &Rig, id: UnitId, state: UnitState) {
    rig.hub.table().units.get_mut(&id).unwrap().state = state;
}

#[test]
fn settlement_queues_one_notice_in_the_owner_inbox() {
    let rig = rig();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    let notices = rig.hub.pending_notices(rig.hub.main());
    assert_eq!(notices.len(), 1);
    assert!(notices[0].wake);
    assert!(notices[0].text.contains("finished"), "{}", notices[0].text);
    let framed = inbox::frame(&main_inbox(&rig).drain_step(), None);
    assert!(framed.starts_with("<run-notice run=\"ag-"), "{framed}");
    assert!(framed.contains("kind=\"settled\""), "{framed}");
    assert!(rig.hub.pending_notices(rig.hub.main()).is_empty());
}

#[test]
fn wake_budget_three_then_overflow_marker() {
    let rig = rig();
    let main = rig.hub.main();
    for _ in 0..3 {
        let id = run(&rig, NoticePolicy::Wake);
        finish(&rig, id);
        assert!(rig.hub.claim_main_wake());
        // The turn that was started drains the notice.
        assert_eq!(main_inbox(&rig).drain_step().len(), 1);
    }
    let fourth = run(&rig, NoticePolicy::Wake);
    finish(&rig, fourth);
    assert!(!rig.hub.claim_main_wake(), "the fourth wake is suppressed");
    let queued = rig.hub.pending_notices(main);
    assert_eq!(queued.len(), 2, "{queued:?}");
    assert!(queued.iter().all(|n| !n.wake));
    let markers = queued
        .iter()
        .filter(|n| n.kind == NoticeKind::Overflow)
        .count();
    assert_eq!(markers, 1);
    // More notices stay quiet and add no second marker.
    let fifth = run(&rig, NoticePolicy::Wake);
    finish(&rig, fifth);
    assert!(!rig.hub.claim_main_wake());
    let queued = rig.hub.pending_notices(main);
    assert_eq!(queued.len(), 3);
    assert_eq!(
        queued
            .iter()
            .filter(|n| n.kind == NoticeKind::Overflow)
            .count(),
        1
    );
    // The next drain still carries all of them.
    assert_eq!(main_inbox(&rig).drain_step().len(), 3);
}

#[test]
fn budget_resets_on_user_and_on_runs_read() {
    let rig = rig();
    for _ in 0..3 {
        let id = run(&rig, NoticePolicy::Wake);
        finish(&rig, id);
        assert!(rig.hub.claim_main_wake());
        main_inbox(&rig).drain_step();
    }
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    assert!(!rig.hub.claim_main_wake());
    // The user speaks (or the model reads state): the counter starts over.
    rig.hub.note_engaged();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    assert!(rig.hub.claim_main_wake());
}

#[test]
fn message_wake_does_not_spend_the_budget() {
    let rig = rig();
    for _ in 0..3 {
        let id = run(&rig, NoticePolicy::Wake);
        finish(&rig, id);
        assert!(rig.hub.claim_main_wake());
        main_inbox(&rig).drain_step();
    }
    let id = run(&rig, NoticePolicy::Wake);
    rig.hub
        .send(id, rig.hub.main(), "hi".into(), DeliveryKind::Steer)
        .unwrap();
    assert!(rig.hub.claim_main_wake(), "a message to main always wakes");
}

#[test]
fn notice_removed_when_wait_consumes_before_drain() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    rig.hub.claim(main, Some("t1"), claim_of(&rig, id));
    assert_eq!(
        rig.hub.pending_notices(main).len(),
        1,
        "a claim is not consumption"
    );
    rig.hub.commit_claims(main, &["t1".to_owned()]);
    assert!(rig.hub.pending_notices(main).is_empty());
    assert!(!rig.hub.claim_main_wake(), "nothing left to wake for");
}

#[test]
fn released_claim_keeps_the_notice() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    rig.hub.claim(main, Some("t1"), claim_of(&rig, id));
    rig.hub.release_claims(&["t1".to_owned()]);
    rig.hub.commit_claims(main, &["t1".to_owned()]);
    assert_eq!(rig.hub.pending_notices(main).len(), 1);
}

#[test]
fn read_after_drain_cannot_cancel_notice() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    let drained = main_inbox(&rig).drain_step();
    assert_eq!(drained.len(), 1, "the request already carries it");
    rig.hub.claim(main, None, claim_of(&rig, id));
    assert!(rig.hub.pending_notices(main).is_empty());
    assert_eq!(drained.len(), 1);
}

#[test]
fn consumed_before_the_notice_is_queued_suppresses_it() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    // A committed read that saw the settle lands between the settle and the enqueue.
    rig.hub.claim(
        main,
        None,
        Claim {
            runs: vec![(id, 1)],
            seqs: Vec::new(),
        },
    );
    finish(&rig, id);
    assert!(rig.hub.pending_notices(main).is_empty());
}

#[test]
fn an_older_read_cannot_consume_a_later_idle_notice() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    set_state(&rig, id, UnitState::Running);
    assert!(rig.hub.settle_idle(id, UnitResult::default(), true));
    let seen = claim_of(&rig, id);
    set_state(&rig, id, UnitState::Running);
    assert!(rig.hub.settle_idle(id, UnitResult::default(), true));
    rig.hub.claim(main, None, seen);
    let left = rig.hub.pending_notices(main);
    assert_eq!(left.len(), 1, "the second idle is news: {left:?}");
}

#[test]
fn stopped_run_produces_no_notice() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    rig.hub.stop(main, &[id], StopOpts::default());
    assert!(rig.hub.pending_notices(main).is_empty());
}

#[test]
fn idle_expiry_is_silent() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Wake);
    set_state(&rig, id, UnitState::Running);
    assert!(rig.hub.settle_idle(id, UnitResult::default(), true));
    main_inbox(&rig).drain_step();
    rig.clock.advance(Duration::from_secs(24 * 3600));
    rig.hub.stop(main, &[id], StopOpts::default());
    assert!(rig.hub.pending_notices(main).is_empty());
    assert!(!rig.hub.claim_main_wake());
}

#[test]
fn identical_notices_collapse_with_a_count() {
    let rig = rig();
    let main = rig.hub.main();
    let id = run(&rig, NoticePolicy::Quiet);
    let info = NoticeInfo {
        kind: NoticeKind::Settled,
        run: id,
        handle: "ag-aaaaaa".into(),
        name: "w".into(),
        generation: 1,
        count: 1,
        created: std::time::SystemTime::now(),
    };
    let inbox = main_inbox(&rig);
    for _ in 0..3 {
        let item = inbox::InboxItem::notice(info.clone(), "same".into(), false, rig.clock.now());
        inbox.lock().enqueue_notice(item, rig.clock.now()).unwrap();
    }
    let queued = rig.hub.pending_notices(main);
    assert_eq!(queued.len(), 1);
    assert!(inbox::frame(&inbox.drain_step(), None).contains("same (x3)"));
}

#[test]
fn a_storm_is_bounded_and_reported_once() {
    let rig = rig();
    for _ in 0..(MAX_PENDING + 10) {
        let id = run(&rig, NoticePolicy::Quiet);
        finish(&rig, id);
    }
    let inbox = main_inbox(&rig);
    assert_eq!(inbox.len(), MAX_PENDING);
    let drained = inbox.drain_step();
    let framed = inbox::frame(&drained, None);
    assert!(
        framed.contains("10 more notices from your runs were dropped"),
        "{framed}"
    );
    assert!(!framed.contains(
        "dropped to protect your context; call runs() to see state.</run-notice>\n<run-notice"
    ));
}

#[test]
fn a_waking_notice_to_main_sends_one_event() {
    let rig = rig();
    let id = run(&rig, NoticePolicy::Wake);
    finish(&rig, id);
    let seen = rig
        .events
        .try_iter()
        .filter(|e| matches!(e.event, AgentEvent::RunNotice { wake: true }))
        .count();
    assert_eq!(seen, 1);
}

#[test]
fn quiet_notice_never_wakes_and_never_sends_an_event() {
    let rig = rig();
    let id = run(&rig, NoticePolicy::Quiet);
    finish(&rig, id);
    assert!(!rig.hub.claim_main_wake());
    assert!(
        rig.events
            .try_iter()
            .all(|e| !matches!(e.event, AgentEvent::RunNotice { .. }))
    );
    assert_eq!(rig.hub.pending_notices(rig.hub.main()).len(), 1);
}
