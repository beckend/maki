//! The agent capacity pool: who holds a slot and who waits for one.
//!
//! Only a run that is working holds a slot. A run blocked in `wait` (or, in
//! phase 5b, a permission prompt) gives its slot back and takes one again
//! with priority over runs that never started, so parents waiting on queued
//! children cannot starve them. Everything here is plain data: the hub calls
//! it under its lock and wakes the waiters after dropping it.

use std::collections::{HashMap, HashSet, VecDeque};

use super::UnitId;

#[derive(Default)]
pub(super) struct Pool {
    held: HashSet<UnitId>,
    /// Runs that resumed from a block, served before `fifo`.
    priority: VecDeque<UnitId>,
    /// Runs that wait for their first (or next) slot, oldest first.
    fifo: VecDeque<UnitId>,
    /// Runs that gave their slot up to wait on other runs: the runs and
    /// whether all of them (not any) must be settled. They join `priority`
    /// the moment that holds, so the slot freed by the run they waited for
    /// does not go to a run that never started.
    blocked: HashMap<UnitId, (Vec<UnitId>, bool)>,
}

impl Pool {
    pub fn holds(&self, id: UnitId) -> bool {
        self.held.contains(&id)
    }

    pub fn is_waiting(&self, id: UnitId) -> bool {
        self.fifo.contains(&id) || self.priority.contains(&id)
    }

    pub fn held_count(&self) -> usize {
        self.held.len()
    }

    /// Whether a new run would have to wait for a slot right now.
    pub fn would_queue(&self, max: usize) -> bool {
        self.held.len() >= max || !self.priority.is_empty() || !self.fifo.is_empty()
    }

    pub fn queued_len(&self) -> usize {
        self.fifo.len()
    }

    /// Joins the line unless already holding or waiting.
    pub fn request(&mut self, id: UnitId, priority: bool) {
        self.blocked.remove(&id);
        if self.holds(id) || self.is_waiting(id) {
            return;
        }
        if priority {
            self.priority.push_back(id);
        } else {
            self.fifo.push_back(id);
        }
    }

    /// Hands free slots to the head of the line, priority first. Returns who
    /// got one.
    pub fn grant(&mut self, max: usize) -> Vec<UnitId> {
        let mut granted = Vec::new();
        while self.held.len() < max {
            let Some(next) = self.priority.pop_front().or_else(|| self.fifo.pop_front()) else {
                break;
            };
            self.held.insert(next);
            granted.push(next);
        }
        granted
    }

    /// Forgets `id` everywhere. True when it held a slot.
    pub fn drop_run(&mut self, id: UnitId) -> bool {
        self.blocked.remove(&id);
        self.priority.retain(|i| *i != id);
        self.fifo.retain(|i| *i != id);
        self.held.remove(&id)
    }

    /// Gives `id`'s slot up while it waits. True when it held one.
    pub fn block(&mut self, id: UnitId, on: Option<(Vec<UnitId>, bool)>) -> bool {
        let held = self.held.remove(&id);
        if held && let Some(on) = on {
            self.blocked.insert(id, on);
        }
        held
    }

    /// Blocked runs whose wait is over, for `is_settled`, now in line with
    /// priority.
    pub fn resume(&mut self, is_settled: impl Fn(UnitId) -> bool) {
        let ready: Vec<UnitId> = self
            .blocked
            .iter()
            .filter(|(_, (ids, all))| {
                if *all {
                    ids.iter().all(|i| is_settled(*i))
                } else {
                    ids.iter().any(|i| is_settled(*i))
                }
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            self.request(id, true);
        }
    }

    /// 1-based place in the FIFO and its length.
    pub fn position(&self, id: UnitId) -> Option<(usize, usize)> {
        let at = self.fifo.iter().position(|i| *i == id)?;
        Some((at + 1, self.fifo.len()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::pin::pin;
    use std::sync::Arc;
    use std::time::Duration;

    use futures_lite::future::poll_once;

    use super::super::tests::request;
    use super::super::{
        DoneReason, Scope, Terminal, UnitError, UnitHub, UnitLimits, UnitResult, UnitState,
        WaitMode,
    };
    use crate::cancel::CancelToken;
    use crate::runs::{ManualClock, RunKind};
    use crate::{Envelope, EventSender};

    fn hub_with(limits: UnitLimits) -> Arc<UnitHub> {
        let (tx, rx) = flume::unbounded::<Envelope>();
        std::mem::forget(rx);
        UnitHub::new(
            limits,
            EventSender::new(tx, 0),
            Arc::new(ManualClock::default()),
        )
    }

    fn agents(n: u8) -> UnitLimits {
        UnitLimits {
            max_agents: n,
            spawn_per_minute: u8::MAX,
            max_children_per_run: u8::MAX,
            max_depth: 4,
            session_max_runs: 1000,
            ..UnitLimits::default()
        }
    }

    fn agent(hub: &Arc<UnitHub>, owner: super::UnitId, name: &str) -> super::UnitId {
        hub.spawn(owner, request(RunKind::Agent, name)).unwrap().id
    }

    fn state(hub: &UnitHub, id: super::UnitId) -> String {
        hub.get(id).unwrap().state_label()
    }

    fn finish(hub: &Arc<UnitHub>, id: super::UnitId) {
        hub.settle(
            id,
            Terminal {
                reason: DoneReason::Completed,
                exit_code: None,
                detail: None,
                result: UnitResult::default(),
            },
            0,
        );
    }

    #[test]
    fn fifo_and_queue_position() {
        let hub = hub_with(agents(1));
        let main = hub.main();
        let a = agent(&hub, main, "a");
        let b = hub.spawn(main, request(RunKind::Agent, "b")).unwrap();
        let c = hub.spawn(main, request(RunKind::Agent, "c")).unwrap();
        let d = hub.spawn(main, request(RunKind::Agent, "d")).unwrap();
        assert_eq!(state(&hub, a), "running");
        assert_eq!(
            [b.queue_position, c.queue_position, d.queue_position],
            [Some((1, 1)), Some((2, 2)), Some((3, 3))]
        );
        assert_eq!(state(&hub, b.id), "queued (position 1 of 3)");
        finish(&hub, a);
        assert_eq!(state(&hub, b.id), "running");
        assert_eq!(state(&hub, c.id), "queued (position 1 of 2)");
        assert_eq!(state(&hub, d.id), "queued (position 2 of 2)");
        finish(&hub, b.id);
        assert_eq!(state(&hub, c.id), "running");
        finish(&hub, c.id);
        assert_eq!(state(&hub, d.id), "running");
        assert_eq!(hub.slots_in_use(), 1);
        finish(&hub, d.id);
        assert_eq!(hub.slots_in_use(), 0);
    }

    #[test]
    fn queue_full_error() {
        let hub = hub_with(UnitLimits {
            max_queue: 2,
            ..agents(1)
        });
        let main = hub.main();
        let a = agent(&hub, main, "a");
        agent(&hub, main, "b");
        agent(&hub, main, "c");
        let err = hub.spawn(main, request(RunKind::Agent, "d")).unwrap_err();
        assert_eq!(err, UnitError::QueueFull);
        assert_eq!(err.to_string(), "QUEUE_FULL");
        // Shells do not use the queue.
        assert!(hub.spawn(main, request(RunKind::Shell, "sh")).is_ok());
        finish(&hub, a);
        assert!(hub.spawn(main, request(RunKind::Agent, "e")).is_ok());
    }

    #[test]
    fn stop_queued_never_starts() {
        let hub = hub_with(agents(1));
        let main = hub.main();
        let a = agent(&hub, main, "a");
        let b = agent(&hub, main, "b");
        let c = hub.spawn(main, request(RunKind::Agent, "c")).unwrap();
        let out = hub.stop(main, &[b], Default::default());
        assert!(out[0].status.is_ok());
        let view = hub.get(b).unwrap();
        assert!(
            matches!(&view.state, UnitState::Done(t) if t.reason == DoneReason::Killed),
            "{:?}",
            view.state
        );
        assert!(view.queue.is_none());
        assert_eq!(state(&hub, c.id), "queued (position 1 of 1)");
        finish(&hub, a);
        assert_eq!(state(&hub, c.id), "running");
        assert!(matches!(hub.get(b).unwrap().state, UnitState::Done(_)));
        assert!(hub.get(b).unwrap().queue.is_none());
        assert_eq!(hub.slots_in_use(), 1);
    }

    #[test]
    fn wait_releases_slot_and_reacquires_first() {
        smol::block_on(async {
            let hub = hub_with(agents(1));
            let main = hub.main();
            let parent = agent(&hub, main, "parent");
            let child = agent(&hub, parent, "child");
            let other = agent(&hub, main, "other");
            assert_eq!(state(&hub, child), "queued (position 1 of 2)");
            let token = CancelToken::none();
            let one = [child];
            let mut wait = pin!(hub.wait(
                parent,
                &one,
                WaitMode::All,
                Duration::from_secs(1_000_000),
                &token
            ));
            assert!(poll_once(wait.as_mut()).await.is_none());
            // The slot went to the child, not to the never-started `other`.
            assert_eq!(state(&hub, child), "running");
            assert_eq!(state(&hub, other), "queued (position 1 of 1)");
            finish(&hub, child);
            // The parent is ahead of `other` before it is even polled.
            assert_eq!(state(&hub, other), "queued (position 1 of 1)");
            assert_eq!(hub.slots_in_use(), 1);
            let views = poll_once(wait.as_mut()).await.expect("wait returns");
            assert!(matches!(views[0].state, UnitState::Done(_)));
            assert!(hub.acquire(parent, &token, false).await);
            assert_eq!(hub.slots_in_use(), 1);
            finish(&hub, parent);
            assert_eq!(state(&hub, other), "running");
        });
    }

    /// max_agents 2: `q` and `p` hold both slots, `p` has three queued
    /// children (one of which has a queued child of its own) and every
    /// holder ends up in `wait`. Nothing may stall.
    #[test]
    fn waiting_slot_holders_do_not_deadlock() {
        smol::block_on(async {
            let hub = hub_with(agents(2));
            let main = hub.main();
            let token = CancelToken::none();
            let q = agent(&hub, main, "q");
            let p = agent(&hub, main, "p");
            let kids = [
                agent(&hub, p, "c1"),
                agent(&hub, p, "c2"),
                agent(&hub, p, "c3"),
            ];
            assert_eq!(hub.slots_in_use(), 2);
            let long = Duration::from_secs(1_000_000);
            let mut p_wait = pin!(hub.wait(p, &kids, WaitMode::All, long, &token));
            assert!(poll_once(p_wait.as_mut()).await.is_none());
            // p gave its slot up: c1 runs, c2 and c3 wait.
            assert_eq!(state(&hub, kids[0]), "running");
            assert_eq!(state(&hub, kids[1]), "queued (position 1 of 2)");
            // c1 spawns a grandchild and waits for it too; q is still busy.
            let g = agent(&hub, kids[0], "g");
            assert_eq!(state(&hub, g), "queued (position 3 of 3)");
            let gs = [g];
            let mut c1_wait = pin!(hub.wait(kids[0], &gs, WaitMode::All, long, &token));
            assert!(poll_once(c1_wait.as_mut()).await.is_none());
            assert_eq!(state(&hub, kids[1]), "running");
            // q finishes: c3 runs (g is behind it), then everything drains.
            finish(&hub, q);
            assert_eq!(state(&hub, kids[2]), "running");
            assert_eq!(state(&hub, g), "queued (position 1 of 1)");
            finish(&hub, kids[1]);
            assert_eq!(state(&hub, g), "running");
            finish(&hub, g);
            // c1 resumes ahead of the finished-run bookkeeping, c3 still holds one.
            assert!(poll_once(c1_wait.as_mut()).await.is_some());
            assert_eq!(hub.slots_in_use(), 2);
            finish(&hub, kids[0]);
            finish(&hub, kids[2]);
            let views = poll_once(p_wait.as_mut()).await.expect("p resumes");
            assert!(views.iter().all(|v| matches!(v.state, UnitState::Done(_))));
            finish(&hub, p);
            assert_eq!(hub.slots_in_use(), 0);
            let all = hub.list(main, Scope::Descendants);
            assert!(all.iter().all(|v| v.state.is_done()), "{all:?}");
        });
    }

    /// Slot accounting after a seeded random mix of spawn, stop, wait and
    /// finish: never above the cap, back to zero once everything ended.
    #[test]
    fn slots_return_to_zero_over_random_ops() {
        const CAP: usize = 3;
        for seed in 1..=40u64 {
            smol::block_on(async {
                let hub = hub_with(UnitLimits {
                    max_queue: 200,
                    ..agents(CAP as u8)
                });
                let main = hub.main();
                let token = CancelToken::none();
                let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut next = move |n: usize| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    (rng % n as u64) as usize
                };
                let mut ids: Vec<super::UnitId> = Vec::new();
                let mut waits = Vec::new();
                let long = Duration::from_secs(1_000_000);
                for _ in 0..120 {
                    match next(5) {
                        0 | 1 => {
                            let owner = if ids.is_empty() || next(2) == 0 {
                                main
                            } else {
                                ids[next(ids.len())]
                            };
                            if let Ok(ack) = hub.spawn(owner, request(RunKind::Agent, "r")) {
                                ids.push(ack.id);
                            }
                        }
                        2 if !ids.is_empty() => {
                            finish(&hub, ids[next(ids.len())]);
                        }
                        3 if !ids.is_empty() => {
                            let id = ids[next(ids.len())];
                            hub.stop(main, &[id], Default::default());
                            finish(&hub, id);
                        }
                        4 if !ids.is_empty() => {
                            let owner = ids[next(ids.len())];
                            let below: Vec<_> = hub
                                .list(owner, Scope::Descendants)
                                .into_iter()
                                .map(|v| v.id)
                                .collect();
                            if !below.is_empty() {
                                let hub = Arc::clone(&hub);
                                let token = token.clone();
                                let mut fut = Box::pin(async move {
                                    hub.wait(owner, &below, WaitMode::All, long, &token).await
                                });
                                if poll_once(fut.as_mut()).await.is_none() {
                                    waits.push(fut);
                                }
                            }
                        }
                        _ => {}
                    }
                    assert!(hub.slots_in_use() <= CAP, "seed {seed}");
                }
                for id in &ids {
                    finish(&hub, *id);
                }
                for fut in &mut waits {
                    let _ = poll_once(fut.as_mut()).await;
                }
                assert_eq!(hub.slots_in_use(), 0, "seed {seed}");
            });
        }
    }

    fn timeout_of(hub: &UnitHub, id: super::UnitId) -> u64 {
        match &hub.table().units[&id].kind {
            crate::runs::UnitKind::Agent(spec) => spec.timeout_secs,
            crate::runs::UnitKind::Shell(spec) => spec.timeout_secs,
        }
    }

    #[test]
    fn default_timeout_30m() {
        let hub = hub_with(agents(8));
        let main = hub.main();
        let plain = agent(&hub, main, "a");
        assert_eq!(timeout_of(&hub, plain), 1800);
        let mut req = request(RunKind::Agent, "b");
        req.timeout_secs = Some(90);
        assert_eq!(timeout_of(&hub, hub.spawn(main, req).unwrap().id), 90);
        let mut req = request(RunKind::Agent, "c");
        req.timeout_secs = Some(10_000_000);
        assert_eq!(timeout_of(&hub, hub.spawn(main, req).unwrap().id), 86_400);
        let shell = hub.spawn(main, request(RunKind::Shell, "sh")).unwrap();
        assert_eq!(timeout_of(&hub, shell.id), 3600);
    }

    #[test]
    fn timeout_is_clamped_to_the_configured_max() {
        let hub = hub_with(UnitLimits {
            max_timeout: Duration::from_secs(600),
            ..agents(8)
        });
        let mut req = request(RunKind::Agent, "a");
        req.timeout_secs = Some(5000);
        assert_eq!(
            timeout_of(&hub, hub.spawn(hub.main(), req).unwrap().id),
            600
        );
        // The default is clamped too.
        let plain = agent(&hub, hub.main(), "b");
        assert_eq!(timeout_of(&hub, plain), 600);
    }

    #[test]
    fn spawn_limit_manual_clock() {
        let (tx, rx) = flume::unbounded::<Envelope>();
        std::mem::forget(rx);
        let clock = Arc::new(ManualClock::default());
        let hub = UnitHub::new(
            UnitLimits {
                spawn_per_minute: 2,
                ..agents(8)
            },
            EventSender::new(tx, 0),
            clock.clone(),
        );
        let main = hub.main();
        let first = agent(&hub, main, "a");
        clock.advance(Duration::from_secs(10));
        agent(&hub, main, "b");
        let err = hub.spawn(main, request(RunKind::Agent, "c")).unwrap_err();
        assert_eq!(err, UnitError::RateLimited { retry_after: 50 });
        // The rate is per owner.
        assert!(hub.spawn(first, request(RunKind::Agent, "d")).is_ok());
        clock.advance(Duration::from_secs(49));
        assert_eq!(
            hub.spawn(main, request(RunKind::Agent, "c")).unwrap_err(),
            UnitError::RateLimited { retry_after: 1 }
        );
        clock.advance(Duration::from_secs(1));
        assert!(hub.spawn(main, request(RunKind::Agent, "c")).is_ok());
    }

    #[test]
    fn shell_caps_total_and_per_owner() {
        let hub = hub_with(UnitLimits {
            max_shells: 3,
            max_shells_per_owner: 2,
            ..agents(8)
        });
        let main = hub.main();
        let other = agent(&hub, main, "other");
        let sh = |h: &Arc<UnitHub>, owner| h.spawn(owner, request(RunKind::Shell, "sh"));
        let first = sh(&hub, main).unwrap();
        sh(&hub, main).unwrap();
        assert_eq!(
            sh(&hub, main).unwrap_err(),
            UnitError::ShellLimit {
                max: 2,
                per_owner: true
            }
        );
        sh(&hub, other).unwrap();
        assert_eq!(
            sh(&hub, other).unwrap_err(),
            UnitError::ShellLimit {
                max: 3,
                per_owner: false
            }
        );
        // A finished shell frees its place.
        finish(&hub, first.id);
        assert!(sh(&hub, main).is_ok());
    }

    type Fut = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

    /// Polls every future once and drops the ones that finished.
    async fn poll_all(futs: &mut Vec<Fut>) {
        let mut i = 0;
        while i < futs.len() {
            if poll_once(futs[i].as_mut()).await.is_some() {
                drop(futs.swap_remove(i));
            } else {
                i += 1;
            }
        }
    }

    fn check_pool(hub: &UnitHub, cap: usize, ctx: &str) {
        let table = hub.table();
        let pool = &table.pool;
        assert!(
            pool.held.len() <= cap,
            "{ctx}: held {} > cap",
            pool.held.len()
        );
        let mut seen = HashSet::new();
        for id in pool.priority.iter().chain(pool.fifo.iter()) {
            assert!(seen.insert(*id), "{ctx}: {id:?} queued twice");
            assert!(!pool.held.contains(id), "{ctx}: {id:?} queued and held");
            assert!(
                !table.units[id].state.is_done(),
                "{ctx}: terminal run in line"
            );
            assert!(!pool.blocked.contains_key(id), "{ctx}: blocked and queued");
        }
        for id in &pool.held {
            assert!(!table.units[id].state.is_done(), "{ctx}: terminal run held");
        }
        for (at, id) in pool.fifo.iter().enumerate() {
            assert_eq!(pool.position(*id), Some((at + 1, pool.fifo.len())), "{ctx}");
        }
        // Work conserving: a free slot never sits next to a waiting run.
        if pool.held.len() < cap {
            assert!(
                pool.priority.is_empty() && pool.fifo.is_empty(),
                "{ctx}: free slot, line not empty"
            );
        }
        for (id, unit) in &table.units {
            if matches!(unit.state, UnitState::Queued) {
                assert!(pool.is_waiting(*id), "{ctx}: Queued run {id:?} not in line");
            }
        }
    }

    /// Seeded model run with waits, soft and hard stops, timeouts on the
    /// manual clock and idle wakes. Checks the pool after every operation.
    #[test]
    fn pool_invariants_over_many_seeds() {
        const CAP: usize = 3;
        for seed in 1..=1000u64 {
            smol::block_on(async {
                let (tx, rx) = flume::unbounded::<Envelope>();
                std::mem::forget(rx);
                let clock = Arc::new(ManualClock::default());
                let hub = UnitHub::new(
                    UnitLimits {
                        max_queue: 500,
                        ..agents(CAP as u8)
                    },
                    EventSender::new(tx, 0),
                    clock.clone(),
                );
                let main = hub.main();
                let token = CancelToken::none();
                let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut next = move |n: usize| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    (rng % n as u64) as usize
                };
                let mut ids: Vec<super::UnitId> = Vec::new();
                let mut waits: Vec<Fut> = Vec::new();
                let mut acquires: Vec<Fut> = Vec::new();
                for step in 0..150 {
                    let ctx = format!("seed {seed} step {step}");
                    match next(9) {
                        0 | 1 => {
                            let owner = if ids.is_empty() || next(2) == 0 {
                                main
                            } else {
                                ids[next(ids.len())]
                            };
                            if let Ok(ack) = hub.spawn(owner, request(RunKind::Agent, "r")) {
                                ids.push(ack.id);
                            }
                        }
                        2 if !ids.is_empty() => finish(&hub, ids[next(ids.len())]),
                        3 if !ids.is_empty() => {
                            let id = ids[next(ids.len())];
                            hub.stop(main, &[id], Default::default());
                            if next(2) == 0 {
                                finish(&hub, id);
                            }
                        }
                        4 if !ids.is_empty() => {
                            let owner = ids[next(ids.len())];
                            let below: Vec<_> = hub
                                .list(owner, Scope::Descendants)
                                .into_iter()
                                .map(|v| v.id)
                                .collect();
                            if !below.is_empty() {
                                let hub = Arc::clone(&hub);
                                let token = token.clone();
                                let mode = if next(2) == 0 {
                                    WaitMode::All
                                } else {
                                    WaitMode::Any
                                };
                                let secs = 5 + next(60) as u64;
                                let mut fut: Fut = Box::pin(async move {
                                    let _ = hub
                                        .wait(
                                            owner,
                                            &below,
                                            mode,
                                            Duration::from_secs(secs),
                                            &token,
                                        )
                                        .await;
                                });
                                if poll_once(fut.as_mut()).await.is_none() {
                                    waits.push(fut);
                                }
                            }
                        }
                        5 => {
                            clock.advance(Duration::from_secs(1 + next(30) as u64));
                            poll_all(&mut waits).await;
                        }
                        6 if !ids.is_empty() => {
                            // A running agent goes idle...
                            hub.settle_idle(ids[next(ids.len())], UnitResult::default(), false);
                        }
                        7 if !ids.is_empty() => {
                            // ...and an idle one is woken: it asks for a slot again.
                            let id = ids[next(ids.len())];
                            let idle = matches!(hub.get(id).unwrap().state, UnitState::Idle);
                            if idle {
                                hub.table().units.get_mut(&id).unwrap().state = UnitState::Running;
                                let hub = Arc::clone(&hub);
                                let token = token.clone();
                                let mut fut: Fut = Box::pin(async move {
                                    let _ = hub.acquire(id, &token, false).await;
                                });
                                if poll_once(fut.as_mut()).await.is_none() {
                                    acquires.push(fut);
                                }
                            }
                        }
                        _ => {}
                    }
                    poll_all(&mut acquires).await;
                    check_pool(&hub, CAP, &ctx);
                }
                for id in &ids {
                    finish(&hub, *id);
                }
                poll_all(&mut waits).await;
                poll_all(&mut acquires).await;
                check_pool(&hub, CAP, &format!("seed {seed} end"));
                let table = hub.table();
                assert!(table.pool.held.is_empty(), "seed {seed}: slots leaked");
                assert!(table.pool.blocked.is_empty(), "seed {seed}: blocked left");
                assert!(table.pool.fifo.is_empty() && table.pool.priority.is_empty());
            });
        }
    }

    /// A woken idle agent queues behind runs that never started (it is not a
    /// resumed waiter), and a resumed waiter goes ahead of it.
    #[test]
    fn woken_idle_agent_queues_in_fifo_order() {
        smol::block_on(async {
            let hub = hub_with(agents(1));
            let main = hub.main();
            let token = CancelToken::none();
            let idle = agent(&hub, main, "idle");
            assert!(hub.settle_idle(idle, UnitResult::default(), false));
            let holder = agent(&hub, main, "holder");
            let queued = agent(&hub, main, "queued");
            assert_eq!(state(&hub, queued), "queued (position 1 of 1)");
            hub.table().units.get_mut(&idle).unwrap().state = UnitState::Running;
            let mut woken = pin!(hub.acquire(idle, &token, false));
            assert!(poll_once(woken.as_mut()).await.is_none());
            assert_eq!(state(&hub, idle), "queued (position 2 of 2)");
            finish(&hub, holder);
            assert_eq!(state(&hub, queued), "running");
            assert!(poll_once(woken.as_mut()).await.is_none());
            finish(&hub, queued);
            assert_eq!(poll_once(woken.as_mut()).await, Some(true));
            assert_eq!(hub.slots_in_use(), 1);
        });
    }
}
