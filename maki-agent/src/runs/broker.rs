//! Routes a background agent's permission prompts to the user (design 6.2).
//!
//! `PermissionManager::enforce` stays as it is: it emits a `PermissionRequest`
//! and waits on an answer channel. For a native run that channel is the
//! broker's. The run's driver calls [`PermissionBroker::begin`] when it sees
//! the request go by; the broker then gives the run's slot up, starts the
//! timeout, and either lets the request reach the frontend or answers it at
//! once (unattended, too many prompts). The frontend answers through
//! `SubagentInfo::answer_tx`, tagged with the request id, so an answer can
//! only ever reach the run whose prompt it names. When the call returns, the
//! tool dispatcher calls [`UnitHub::prompt_resolved`], which takes the slot
//! back ahead of runs that never started.
//!
//! A run asks one question at a time: `enforce` holds the receiver while it
//! waits, so a second parallel call queues behind the first and asks only
//! when it is its turn. Over all runs at most [`MAX_PENDING_PROMPTS`] prompts
//! wait; one more is denied with a text that sends the run to its parent.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use maki_config::UnattendedMode;

use super::notice::{NoticeInfo, NoticeKind};
use super::{Clock, PendingNotice, UnitHub, UnitId};
use crate::cancel::CancelToken;
use crate::permissions::{
    PermissionAnswer, TaggedAnswer, prompt_timeout_reason, unattended_reason,
};
use crate::{AgentEvent, BACKGROUND_RUN_ID, Envelope, EventSender, SubagentInfo};

/// Prompts that may wait at once in one session, over all runs.
pub const MAX_PENDING_PROMPTS: usize = 8;

/// A prompt that has waited this long while `main` is idle is worth a quiet
/// note to the owner.
const QUIET_AFTER: Duration = Duration::from_secs(60);

pub type AnswerRx = Arc<smol::lock::Mutex<flume::Receiver<String>>>;

type PendingMap = Arc<Mutex<HashMap<String, Pending>>>;

/// Every answer a frontend or the timer gives comes in on `tx` and goes through
/// a pump that only passes on one that names a prompt still on screen, by the
/// id this prompt was shown under. That id is made fresh for each prompt, so
/// an answer that arrives after its prompt is over cannot be taken for the
/// answer to a later prompt of a call that reuses the tool call id.
pub(super) struct PermissionBroker {
    tx: flume::Sender<String>,
    /// What `enforce` reads: answers that passed the pump, and the broker's own.
    out: flume::Sender<String>,
    rx: AnswerRx,
    pending: PendingMap,
    total: Arc<AtomicUsize>,
    seq: AtomicUsize,
    _pump: smol::Task<()>,
}

/// One prompt on screen. Dropping it stops its timer and gives its count back.
struct Pending {
    /// The id the prompt was shown and is answered under.
    wire: String,
    /// This prompt released the run's slot, so resolving it takes one again.
    released: bool,
    total: Arc<AtomicUsize>,
    _timer: smol::Task<()>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.total.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct Begin<'a> {
    pub hub: &'a Arc<UnitHub>,
    pub run: UnitId,
    pub info: &'a SubagentInfo,
    pub clock: &'a Arc<dyn Clock>,
    pub request: &'a str,
    pub scope: String,
    pub yolo: bool,
}

impl PermissionBroker {
    pub(super) fn new(total: Arc<AtomicUsize>) -> Arc<Self> {
        let (tx, raw_rx) = flume::unbounded::<String>();
        let (out, rx) = flume::unbounded();
        let pending = PendingMap::default();
        let pump = smol::spawn({
            let (pending, out) = (Arc::clone(&pending), out.clone());
            async move {
                while let Ok(raw) = raw_rx.recv_async().await {
                    let Some(tagged) = TaggedAnswer::decode(&raw) else {
                        continue;
                    };
                    // Checked and passed on under the lock `resolve` takes, so
                    // nothing gets through once the prompt is over.
                    let pending = pending.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some((original, _)) =
                        pending.iter().find(|(_, p)| p.wire == tagged.request_id)
                    {
                        let _ = out.send(TaggedAnswer::new(original, tagged.answer).encode());
                    }
                }
            }
        });
        Arc::new(Self {
            tx,
            out,
            rx: Arc::new(smol::lock::Mutex::new(rx)),
            pending,
            total,
            seq: AtomicUsize::new(0),
            _pump: pump,
        })
    }

    /// The frontend's way to answer; stamped into the run's `SubagentInfo`.
    pub(super) fn sender(&self) -> flume::Sender<String> {
        self.tx.clone()
    }

    pub(super) fn receiver(&self) -> AnswerRx {
        Arc::clone(&self.rx)
    }

    fn answer(&self, request: &str, answer: PermissionAnswer) {
        let _ = self.out.send(TaggedAnswer::new(request, answer).encode());
    }

    /// Decides what happens to a prompt the run just raised. True when the
    /// request is to be shown to the user, with the id to show it under; None
    /// when it was already answered here and must not reach the frontend.
    pub(super) fn begin(&self, b: Begin<'_>) -> Option<String> {
        let hub = b.hub;
        if !hub.attended.load(Ordering::Relaxed) {
            let allow = hub.limits.unattended == UnattendedMode::InheritYolo && b.yolo;
            self.answer(
                b.request,
                if allow {
                    PermissionAnswer::AllowOnce
                } else {
                    PermissionAnswer::Unavailable(unattended_reason())
                },
            );
            return None;
        }
        if self.total.fetch_add(1, Ordering::AcqRel) >= MAX_PENDING_PROMPTS {
            self.total.fetch_sub(1, Ordering::AcqRel);
            self.answer(
                b.request,
                PermissionAnswer::Unavailable(format!(
                    "too many permission prompts are waiting ({MAX_PENDING_PROMPTS}); \
                     ask your parent via send if you need {}",
                    b.scope
                )),
            );
            return None;
        }
        let wire = format!("{}~{}", b.request, self.seq.fetch_add(1, Ordering::Relaxed));
        let released = hub.release_slot(b.run, None);
        let timeout = hub.limits.permission_timeout;
        let start = b.clock.now();
        let timer = smol::spawn({
            let (tx, clock) = (self.tx.clone(), Arc::clone(b.clock));
            let (events, info) = (hub.events.clone(), b.info.clone());
            let (hub, run) = (Arc::downgrade(hub), b.run);
            let (wire_id, scope) = (wire.clone(), b.scope);
            async move {
                if timeout > QUIET_AFTER {
                    clock.sleep_until(start + QUIET_AFTER).await;
                    if let Some(hub) = hub.upgrade() {
                        hub.permission_notice(run, &scope);
                    }
                }
                clock.sleep_until(start + timeout).await;
                let reason = prompt_timeout_reason(&scope, timeout.as_secs());
                let answer = PermissionAnswer::Unavailable(reason);
                let _ = tx.send(TaggedAnswer::new(&wire_id, answer).encode());
                withdraw(&events, &info, &wire_id);
            }
        });
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                b.request.to_owned(),
                Pending {
                    wire: wire.clone(),
                    released,
                    total: Arc::clone(&self.total),
                    _timer: timer,
                },
            );
        Some(wire)
    }

    /// The call that asked has returned. `Some(released)` when a prompt of
    /// this request was waiting, with whether it gave the slot up. Whatever
    /// answer is still queued belongs to a prompt that is over, so it goes
    /// before a later prompt can read it.
    fn resolve(&self, request: &str) -> Option<(bool, String)> {
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request)?;
        if let Some(rx) = self.rx.try_lock() {
            while rx.try_recv().is_ok() {}
        }
        Some((pending.released, pending.wire.clone()))
    }

    /// The turn is over, so no prompt of it can still be waited on.
    pub(super) fn clear(&self, events: &EventSender, info: &SubagentInfo) {
        let gone: Vec<String> = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.drain().map(|(_, p)| p.wire.clone()).collect()
        };
        for id in gone {
            withdraw(events, info, &id);
        }
    }

    #[cfg(test)]
    pub(super) fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

fn withdraw(events: &EventSender, info: &SubagentInfo, request: &str) {
    let _ = events.send_envelope(Envelope {
        event: AgentEvent::PermissionWithdrawn {
            id: request.to_owned(),
        },
        subagent: Some(info.clone()),
        run_id: BACKGROUND_RUN_ID,
    });
}

impl UnitHub {
    /// Called when a tool call of `run` that may have prompted returns, for
    /// whatever reason. Takes the slot back if the prompt gave it up, with
    /// priority, as `wait` does. Only a call on the run's own answer channel
    /// counts, so a legacy subagent's prompt with the same id is not it.
    pub(crate) async fn prompt_resolved(
        &self,
        run: UnitId,
        request: &str,
        answers: Option<&AnswerRx>,
        cancel: &CancelToken,
    ) {
        let agent = self.table().agents.get(&run).cloned();
        let Some(agent) = agent else { return };
        if !answers.is_some_and(|rx| Arc::ptr_eq(rx, &agent.broker.receiver())) {
            return;
        }
        let Some((released, wire)) = agent.broker.resolve(request) else {
            return;
        };
        withdraw(&self.events, &agent.info, &wire);
        // A cancelled call is on its way out; the run's next turn queues for
        // a slot itself, and a stop settles the run, which frees it.
        if released && !cancel.is_cancelled() {
            self.acquire(run, cancel, true).await;
        }
    }

    /// One quiet note to the run's owner, when the user's main run is idle and
    /// so unlikely to look at the prompt soon. Never wakes.
    fn permission_notice(&self, run: UnitId, scope: &str) {
        if self.main_busy.load(Ordering::SeqCst) {
            return;
        }
        let mut table = self.table();
        let Some(unit) = table.units.get(&run) else {
            return;
        };
        let (owner, handle, name) = (unit.owner, unit.handle.to_string(), unit.name.to_string());
        let text = format!("{handle} is waiting for permission: {scope}");
        table.pending_notices.push(PendingNotice {
            owner,
            info: NoticeInfo {
                kind: NoticeKind::Permission,
                run,
                handle,
                name,
                generation: 0,
                count: 1,
                created: SystemTime::now(),
            },
            text,
            wake: false,
        });
        drop(table);
        self.flush_notices();
    }
}
