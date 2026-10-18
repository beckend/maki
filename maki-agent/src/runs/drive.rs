//! Headless frontends: waking `main` when a notice arrives after its turn
//! ended, and waiting for background work before the process exits
//! (design 4.4, phase 4c).
//!
//! `drive_main` is the wake consumer, `join_until` is the closing wait. Both
//! park on the hub's `settled` event and on `main`'s inbox signal, never poll.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_lite::future::or;
use maki_providers::Message;

use super::{StopOpts, UnitHub, UnitState};

/// How long a join that timed out waits for the stopped runs to settle.
const STOP_WAIT: Duration = Duration::from_secs(10);

/// Where `drive_main` hands a wake. The sink must start a turn of `main` and
/// call [`UnitHub::main_turn_ends`] when it is over.
pub trait WakeSink {
    fn start_main_run(&self, preamble: Vec<Message>);
}

/// What [`UnitHub::join`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Joined {
    /// No run is active and `main` has nothing unread.
    Quiet,
    /// No run is active, but `main`'s inbox holds quiet items (notices past
    /// the wake budget, quiet messages). The caller runs one closing turn so
    /// nothing is stranded, then joins again.
    Pending,
    /// The bound passed. Everything still alive was stopped; these are the
    /// handles of the runs that were not finished.
    TimedOut { stopped: Vec<String> },
}

impl UnitHub {
    /// A turn of `main` has started or is about to. `drive_main` starts none
    /// while one is in flight (that turn drains the inbox itself).
    pub fn main_turn_begins(&self) {
        self.main_busy.store(true, Ordering::SeqCst);
    }

    /// The turn of `main` is over: `drive_main` may start the next and `join`
    /// may look at the state again.
    pub fn main_turn_ends(&self) {
        self.main_busy.store(false, Ordering::SeqCst);
        self.settled.notify(usize::MAX);
    }

    /// `main`'s inbox holds something no turn has read yet.
    pub fn main_has_unread(&self) -> bool {
        !self.main_inbox.is_empty()
    }

    /// Wakes `main` through `wake` when a waking notice or message arrives
    /// while no turn of `main` is running, within the wake budget. Returns
    /// when the hub is shut down.
    pub async fn drive_main(self: Arc<Self>, wake: impl WakeSink) {
        loop {
            // Listeners first, so a change between the check and the park is
            // not lost.
            let progress = self.settled.listen();
            let arrived = self.main_inbox.woken();
            if self.cancel.is_cancelled() {
                return;
            }
            if !self.main_busy.load(Ordering::SeqCst) {
                let waking = self.main_inbox.lock().has_wakeable();
                if self.claim_main_wake() {
                    self.main_turn_begins();
                    wake.start_main_run(Vec::new());
                } else if waking {
                    // The budget turned the wakes quiet: a `join` that saw
                    // them as pending work has to look again.
                    self.settled.notify(usize::MAX);
                }
            }
            or(
                or(progress, async {
                    arrived.await;
                }),
                self.cancel.cancelled(),
            )
            .await;
        }
    }

    /// The instant `timeout` from now on the hub's clock, for [`Self::join_until`].
    pub fn deadline_after(&self, timeout: Duration) -> Instant {
        self.clock.now() + timeout
    }

    /// [`Self::join_until`] with the bound counted from now.
    pub async fn join(self: &Arc<Self>, timeout: Duration) -> Joined {
        self.join_until(self.clock.now() + timeout).await
    }

    /// Waits for the work that can still produce a turn of `main`: runs that
    /// are queued, running or stopping, idle agents with a waking item, a
    /// turn of `main` in flight and wakes not yet claimed. Idle agents with
    /// nothing to do are ignored. At `deadline` every top-level run is
    /// stopped (child-first, so descendants go with it) and the join returns.
    pub async fn join_until(self: &Arc<Self>, deadline: Instant) -> Joined {
        loop {
            let listener = self.settled.listen();
            if !self.work_left() {
                return if self.main_inbox.is_empty() {
                    Joined::Quiet
                } else {
                    Joined::Pending
                };
            }
            if self.cancel.is_cancelled() {
                return Joined::TimedOut {
                    stopped: Vec::new(),
                };
            }
            let timed_out = or(
                async {
                    listener.await;
                    false
                },
                async {
                    self.clock.sleep_until(deadline).await;
                    true
                },
            )
            .await;
            if timed_out && self.work_left() {
                return self.stop_for_join().await;
            }
        }
    }

    /// Whether anything can still make `main` run or notice something.
    fn work_left(&self) -> bool {
        let idle_inboxes = {
            let table = self.table();
            if !table.pending_notices.is_empty()
                || self.notices_in_flight.load(Ordering::SeqCst) > 0
            {
                return true;
            }
            let mut idle = Vec::new();
            for (id, unit) in &table.units {
                match &unit.state {
                    UnitState::Queued | UnitState::Running | UnitState::Stopping => return true,
                    UnitState::Idle => {
                        if let Some(run) = table.agents.get(id) {
                            idle.push(Arc::clone(&run.inbox));
                        }
                    }
                    UnitState::Done(_) => {}
                }
            }
            idle
        };
        if self.main_busy.load(Ordering::SeqCst) || self.main_inbox.lock().has_wakeable() {
            return true;
        }
        idle_inboxes.iter().any(|inbox| inbox.lock().has_wakeable())
    }

    async fn stop_for_join(self: &Arc<Self>) -> Joined {
        let (ids, handles) = {
            let table = self.table();
            let alive = |id: &super::UnitId| {
                table
                    .units
                    .get(id)
                    .is_some_and(|unit| !unit.state.is_done())
            };
            let top: Vec<_> = table
                .children
                .get(&self.main)
                .map(|ids| ids.iter().copied().filter(alive).collect())
                .unwrap_or_default();
            let handles: Vec<String> = table
                .order
                .iter()
                .filter(|id| alive(id))
                .map(|id| table.units[id].handle.to_string())
                .collect();
            (top, handles)
        };
        // `stop` is by ids below the caller; `main` owns the top level.
        self.stop(self.main, &ids, StopOpts::default());
        let deadline = self.clock.now() + STOP_WAIT;
        loop {
            let listener = self.settled.listen();
            let live = {
                let table = self.table();
                table.units.values().any(|u| !u.state.is_done())
            };
            if !live {
                break;
            }
            let expired = or(
                async {
                    listener.await;
                    false
                },
                async {
                    self.clock.sleep_until(deadline).await;
                    true
                },
            )
            .await;
            if expired {
                break;
            }
        }
        // Whatever ignored the stop (a shell in its TERM grace) dies now.
        self.shutdown();
        Joined::TimedOut { stopped: handles }
    }
}

#[cfg(test)]
mod headless {
    use std::pin::pin;
    use std::time::Duration;

    use futures_lite::future::poll_once;

    use super::super::inbox;
    use super::super::*;
    use super::*;
    use crate::Envelope;

    const BOUND: Duration = Duration::from_secs(30);

    struct Rig {
        hub: Arc<UnitHub>,
        clock: Arc<ManualClock>,
        wakes: flume::Receiver<()>,
        _events: flume::Receiver<Envelope>,
    }

    struct Sink(flume::Sender<()>);

    impl WakeSink for Sink {
        fn start_main_run(&self, _preamble: Vec<Message>) {
            self.0.send(()).unwrap();
        }
    }

    fn rig(driven: bool) -> Rig {
        let (tx, events) = flume::unbounded::<Envelope>();
        let clock = Arc::new(ManualClock::default());
        let hub = UnitHub::new(
            UnitLimits::default(),
            EventSender::new(tx, 0),
            clock.clone(),
        );
        let (wake_tx, wakes) = flume::unbounded();
        if driven {
            smol::spawn(Arc::clone(&hub).drive_main(Sink(wake_tx))).detach();
        }
        Rig {
            hub,
            clock,
            wakes,
            _events: events,
        }
    }

    fn agent(rig: &Rig) -> UnitId {
        let mut req = tests::request(RunKind::Agent, "work");
        req.notify = Some(NoticePolicy::Wake);
        rig.hub.spawn(rig.hub.main(), req).unwrap().id
    }

    fn finish(rig: &Rig, id: UnitId) {
        let terminal = Terminal {
            reason: DoneReason::Completed,
            exit_code: None,
            detail: None,
            result: UnitResult {
                summary: "ok".to_owned(),
                ..UnitResult::default()
            },
        };
        rig.hub.settle(id, terminal, 0);
    }

    /// The turn a frontend would run for a wake: it drains the inbox.
    fn run_turn(rig: &Rig) -> String {
        let text = inbox::frame(&rig.hub.main_inbox.drain_step(), None);
        rig.hub.main_turn_ends();
        text
    }

    async fn wake(rig: &Rig) {
        rig.wakes.recv_async().await.unwrap();
    }

    #[test]
    fn main_wakes_on_notice_and_join_waits() {
        smol::block_on(async {
            let rig = rig(true);
            let id = agent(&rig);
            let hub = Arc::clone(&rig.hub);
            let deadline = hub.deadline_after(BOUND);
            let mut join = pin!(hub.join_until(deadline));
            assert!(poll_once(&mut join).await.is_none(), "a running agent");
            finish(&rig, id);
            wake(&rig).await;
            assert!(poll_once(&mut join).await.is_none(), "main's turn is open");
            let text = run_turn(&rig);
            assert!(text.contains("<run-notice"), "{text}");
            assert_eq!(join.await, Joined::Quiet);
            assert!(rig.wakes.try_recv().is_err(), "one wake for one notice");
        });
    }

    #[test]
    fn join_timeout_stops_runs() {
        smol::block_on(async {
            let rig = rig(false);
            let parent = agent(&rig);
            let child = rig
                .hub
                .spawn(parent, tests::request(RunKind::Agent, "child"))
                .unwrap()
                .id;
            let timeout = Duration::from_secs(60);
            let (tx, done) = flume::bounded(1);
            let hub = Arc::clone(&rig.hub);
            smol::spawn(async move {
                let _ = tx.send(hub.join(timeout).await);
            })
            .detach();
            let mut joined = None;
            for _ in 0..4000 {
                if let Ok(outcome) = done.try_recv() {
                    joined = Some(outcome);
                    break;
                }
                rig.clock.advance(Duration::from_secs(1));
                smol::Timer::after(Duration::from_millis(1)).await;
            }
            let Some(Joined::TimedOut { stopped }) = joined else {
                panic!("expected a timeout, got {joined:?}");
            };
            assert_eq!(stopped.len(), 2, "{stopped:?}");
            for id in [parent, child] {
                let view = rig.hub.get(id).unwrap();
                assert!(
                    matches!(&view.state, UnitState::Done(t) if t.reason == DoneReason::Killed),
                    "{:?}",
                    view.state
                );
            }
        });
    }

    #[test]
    fn join_ignores_idle_agents() {
        smol::block_on(async {
            let rig = rig(false);
            let id = agent(&rig);
            rig.hub.table().units.get_mut(&id).unwrap().state = UnitState::Idle;
            let deadline = rig.hub.deadline_after(BOUND);
            let joined = poll_once(rig.hub.join_until(deadline)).await;
            assert_eq!(joined, Some(Joined::Quiet));
        });
    }

    #[test]
    fn join_delivers_quiet_notices_as_closing_turn() {
        smol::block_on(async {
            let rig = rig(true);
            let ids: Vec<_> = (0..5).map(|_| agent(&rig)).collect();
            for &id in &ids[..WAKE_BUDGET as usize] {
                finish(&rig, id);
                wake(&rig).await;
                let text = run_turn(&rig);
                assert!(text.contains("<run-notice"), "{text}");
            }
            for &id in &ids[WAKE_BUDGET as usize..] {
                finish(&rig, id);
            }
            let deadline = rig.hub.deadline_after(BOUND);
            let hub = Arc::clone(&rig.hub);
            assert_eq!(hub.join_until(deadline).await, Joined::Pending);
            assert!(
                rig.wakes.try_recv().is_err(),
                "past the budget nothing wakes"
            );
            let text = run_turn(&rig);
            for &id in &ids[WAKE_BUDGET as usize..] {
                let handle = rig.hub.get(id).unwrap().handle.to_string();
                assert!(text.contains(&handle), "{handle} missing from {text}");
            }
            assert_eq!(hub.join_until(deadline).await, Joined::Quiet);
        });
    }
}
