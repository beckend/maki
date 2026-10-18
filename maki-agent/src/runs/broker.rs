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

pub(super) struct PermissionBroker {
    tx: flume::Sender<String>,
    rx: AnswerRx,
    pending: Mutex<HashMap<String, Pending>>,
    total: Arc<AtomicUsize>,
}

/// One prompt on screen. Dropping it stops its timer and gives its count back.
struct Pending {
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
        let (tx, rx) = flume::unbounded();
        Arc::new(Self {
            tx,
            rx: Arc::new(smol::lock::Mutex::new(rx)),
            pending: Mutex::default(),
            total,
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
        let _ = self.tx.send(TaggedAnswer::new(request, answer).encode());
    }

    /// Decides what happens to a prompt the run just raised. True when the
    /// request is to be shown to the user; false when it was already answered
    /// here and must not reach the frontend.
    pub(super) fn begin(&self, b: Begin<'_>) -> bool {
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
            return false;
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
            return false;
        }
        let released = hub.release_slot(b.run, None);
        let timeout = hub.limits.permission_timeout;
        let start = b.clock.now();
        let timer = smol::spawn({
            let (tx, clock) = (self.tx.clone(), Arc::clone(b.clock));
            let (events, info) = (hub.events.clone(), b.info.clone());
            let (hub, run) = (Arc::downgrade(hub), b.run);
            let (request, scope) = (b.request.to_owned(), b.scope);
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
                let _ = tx.send(TaggedAnswer::new(&request, answer).encode());
                withdraw(&events, &info, &request);
            }
        });
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                b.request.to_owned(),
                Pending {
                    released,
                    total: Arc::clone(&self.total),
                    _timer: timer,
                },
            );
        true
    }

    /// The call that asked has returned. `Some(released)` when a prompt of
    /// this request was waiting, with whether it gave the slot up. Whatever
    /// answer is still queued belongs to a prompt that is over, so it goes
    /// before a later prompt can read it.
    fn resolve(&self, request: &str) -> Option<bool> {
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request)?;
        if let Some(rx) = self.rx.try_lock() {
            while rx.try_recv().is_ok() {}
        }
        Some(pending.released)
    }

    /// The turn is over, so no prompt of it can still be waited on.
    pub(super) fn clear(&self, events: &EventSender, info: &SubagentInfo) {
        let gone: Vec<String> = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.drain().map(|(id, _)| id).collect()
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
        let Some(released) = agent.broker.resolve(request) else {
            return;
        };
        withdraw(&self.events, &agent.info, request);
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
