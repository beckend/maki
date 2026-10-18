//! Cold revive and eviction of agent runs (design 7.4).
//!
//! A finished, lost or evicted agent that has a transcript comes back when it
//! is messaged: same id, handle, name and owner, history from the transcript,
//! the message as its first steer. Eviction is the other half: idle agents
//! beyond `max_idle` or past `idle_timeout` and finished runs beyond
//! `max_retained` leave memory, the latter as tombstones.

use std::path::PathBuf;

use maki_storage::runs::{AckRec, JournalRecord, Replay, STATE_REVIVED, StateRec};

use super::agent::AgentRun;
use super::broker::PermissionBroker;
use super::inbox::MessageId;
use super::*;

/// How many finished runs of an earlier process are kept as tombstones.
const MAX_ADOPTED_TOMBSTONES: usize = 256;

/// What a caller needs to start a revival: the spec to rebuild the agent from.
#[derive(Debug, Clone)]
pub struct RevivePlan {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub spec: AgentSpec,
}

/// One line of `runs` for a run that left memory.
#[derive(Debug, Clone)]
pub struct TombstoneView {
    pub id: UnitId,
    pub handle: UnitHandle,
    pub name: UnitName,
    pub kind: RunKind,
    pub reason: String,
    pub revivable: bool,
}

impl UnitHub {
    /// Where an agent's transcript is written, when there is a writable
    /// journal to put it beside.
    pub(super) fn transcript_path(&self, handle: &str) -> Option<PathBuf> {
        let journal = self.journal.get()?;
        (!journal.read_only()).then(|| maki_storage::runs::transcript_path(journal.dir(), handle))
    }

    fn transcript_file(&self, handle: &str) -> Option<PathBuf> {
        let journal = self.journal.get()?;
        Some(maki_storage::runs::transcript_path(journal.dir(), handle))
    }

    /// The saved history of an agent, `None` when it has none.
    pub fn load_transcript(&self, handle: &str) -> Option<Vec<Message>> {
        transcript::load(&self.transcript_file(handle)?)
    }

    /// Whether `run` is still the agent run of `id` (a revive replaces it).
    pub(super) fn is_current_run(&self, id: UnitId, run: &Arc<AgentRun>) -> bool {
        self.table()
            .agents
            .get(&id)
            .is_none_or(|cur| Arc::ptr_eq(cur, run))
    }

    fn can_message(&self, table: &UnitTable, from: UnitId, unit: &Unit) -> bool {
        from == self.main
            || from == self.user
            || unit.parent == from
            || table.units.get(&from).is_some_and(|f| {
                f.parent == unit.id || (self.limits.siblings && f.parent == unit.parent)
            })
    }

    /// Whether a `send` from `from` to `to` has to bring the agent back
    /// first. `Ok(None)` means `to` is live (or not an agent run at all) and
    /// [`UnitHub::send`] handles it.
    pub fn revive_plan(&self, from: UnitId, to: UnitId) -> Result<Option<RevivePlan>, UnitError> {
        if to == self.main || to == self.user {
            return Ok(None);
        }
        let table = self.table();
        let Some(unit) = table.units.get(&to).or_else(|| table.tombstones.get(&to)) else {
            return Err(UnitError::UnknownRun);
        };
        let UnitKind::Agent(spec) = &unit.kind else {
            return Err(UnitError::NotAgent);
        };
        let UnitState::Done(terminal) = &unit.state else {
            return Ok(None);
        };
        if from != self.user
            && from != self.main
            && table
                .units
                .get(&from)
                .is_none_or(|f| matches!(f.state, UnitState::Stopping | UnitState::Done(_)))
        {
            return Err(UnitError::NotReachable);
        }
        if !self.can_message(&table, from, unit) {
            return Err(UnitError::NotReachable);
        }
        if spec.one_shot && terminal.reason == DoneReason::Completed {
            return Err(UnitError::RecipientTerminal);
        }
        let saved = self
            .transcript_file(unit.handle.as_str())
            .is_some_and(|p| p.exists());
        if !saved {
            return Err(UnitError::RecipientTerminal);
        }
        Ok(Some(RevivePlan {
            id: to,
            handle: unit.handle.clone(),
            spec: spec.clone(),
        }))
    }

    /// Messages the journal still holds for `handle` that nothing delivered
    /// (the process died before the run read them).
    fn pending_messages(&self, handle: &str) -> Vec<(String, InboxItem)> {
        let Some(journal) = self.journal.get() else {
            return Vec::new();
        };
        let replay: Replay = journal.replay();
        replay
            .messages
            .iter()
            .filter(|m| m.pending() && m.rec.to == handle)
            .filter_map(|m| {
                let body = m.rec.body.clone()?;
                let kind = DeliveryKind::parse(&m.rec.kind)?;
                let id = MessageId(self.message_seq.fetch_add(1, Ordering::Relaxed) + 1);
                let item = InboxItem::new(
                    id,
                    self.main,
                    m.rec.from.clone(),
                    kind,
                    body,
                    self.clock.now(),
                );
                Some((m.rec.id.clone(), item))
            })
            .collect()
    }

    /// Brings the agent of `plan` back to work and delivers `text` to it as
    /// its first message, after any message the journal still held for it.
    /// `launch` is [`AgentLaunch::prepare`] of `plan.spec`; `transcript` is
    /// [`UnitHub::load_transcript`].
    pub fn revive(
        self: &Arc<Self>,
        from: UnitId,
        plan: RevivePlan,
        mut launch: AgentLaunch,
        transcript: Vec<Message>,
        text: String,
        kind: DeliveryKind,
    ) -> Result<SendAck, UnitError> {
        if text.len() > inbox::MAX_MESSAGE_BYTES {
            return Err(UnitError::TooLarge {
                max: inbox::MAX_MESSAGE_BYTES,
            });
        }
        let id = plan.id;
        let pending = self.pending_messages(plan.handle.as_str());
        let max_agents = usize::from(self.limits.max_agents);
        let msg_id = MessageId(self.message_seq.fetch_add(1, Ordering::Relaxed) + 1);
        let now = self.clock.now();
        let mut table = self.table();
        let label = if from == self.user {
            "user".to_owned()
        } else if from == self.main {
            "main".to_owned()
        } else {
            let unit = table.units.get(&from).ok_or(UnitError::UnknownRun)?;
            if matches!(unit.state, UnitState::Stopping | UnitState::Done(_)) {
                return Err(UnitError::NotReachable);
            }
            unit.name.to_string()
        };
        if let Some(mut unit) = table.tombstones.remove(&id) {
            if unit.parent != self.main && !table.units.contains_key(&unit.parent) {
                unit.parent = self.main;
            }
            table.children.entry(unit.parent).or_default().push(id);
            table.order.push(id);
            table.units.insert(id, unit);
        }
        let would_queue = table.pool.would_queue(max_agents);
        let Some(unit) = table.units.get_mut(&id) else {
            return Err(UnitError::UnknownRun);
        };
        if !matches!(unit.state, UnitState::Done(_)) {
            // Somebody else revived it first: it is a live agent now.
            drop(table);
            return self.send(from, id, text, kind);
        }
        let (handle, name) = (unit.handle.clone(), unit.name.clone());
        unit.state = if would_queue {
            UnitState::Queued
        } else {
            UnitState::Running
        };
        let state = unit.state.clone();
        unit.started_at = Some(SystemTime::now());
        unit.finished_at = None;
        unit.progress = None;
        unit.last_activity = now;
        let spec = plan.spec;
        launch.fork = Some(Arc::new(transcript));
        launch.revived = true;
        let broker = PermissionBroker::new(Arc::clone(&self.prompts));
        let info = SubagentInfo {
            parent_tool_use_id: handle.to_string(),
            name: name.to_string(),
            prompt: Some(spec.prompt.clone()),
            model: Some(launch.model_spec.clone()),
            opts: Some(launch.request_options()),
            answer_tx: Some(broker.sender()),
            inbox: None,
        };
        let run = AgentRun::new(&self.cancel, info, broker);
        table.agents.insert(id, Arc::clone(&run));
        table.pool.request(id, false);
        table.pool.grant(max_agents);
        let event = table.event_of(
            id,
            AgentEvent::RunStateChanged {
                handle: handle.to_string(),
                state: state.label().to_owned(),
            },
        );
        table.outbox.push(event);
        self.jot(JournalRecord::State(StateRec {
            handle: handle.to_string(),
            state: STATE_REVIVED.to_owned(),
        }));
        let item = InboxItem::new(msg_id, from, label, kind, text, now);
        self.jot(JournalRecord::Msg(journal::message_record(
            &item,
            handle.as_str(),
            "accepted",
        )));
        for (old, redone) in &pending {
            // The old record is settled; the fresh copy carries the text.
            self.jot(JournalRecord::Ack(AckRec {
                id: old.clone(),
                stage: "dropped".to_owned(),
                step: None,
                reason: Some("revived".to_owned()),
            }));
            self.jot(JournalRecord::Msg(journal::message_record(
                redone,
                handle.as_str(),
                "accepted",
            )));
        }
        self.ack_insert(from, msg_id, id, AckState::Accepted);
        self.release(table);
        for (_, redone) in pending {
            let _ = self.deliver(id, &run.inbox, redone, now, false);
        }
        if let Err(e) = self.deliver(id, &run.inbox, item, now, false) {
            self.acks().remove(from, msg_id);
            return Err(e);
        }
        agent::start(
            Arc::downgrade(self),
            id,
            run,
            spec,
            launch,
            Arc::clone(&self.clock),
        );
        Ok(SendAck {
            id: msg_id,
            state: "revived",
            held: false,
        })
    }

    /// Finished runs of an earlier process whose transcript is still there
    /// become tombstones, so a `send` after a restart revives them too.
    pub(super) fn adopt_tombstones(&self, replay: &Replay) {
        let Some(dir) = self.journal.get().map(|j| j.dir().to_owned()) else {
            return;
        };
        let mut table = self.table();
        let mut adopted = 0;
        for run in replay.runs.iter().rev() {
            if adopted >= MAX_ADOPTED_TOMBSTONES {
                break;
            }
            if run.spawn.kind != "agent"
                || !maki_storage::runs::transcript_path(&dir, &run.spawn.handle).exists()
            {
                continue;
            }
            let Some(mut unit) = journal::restore_finished(run) else {
                continue;
            };
            if table.handles.contains_key(&unit.handle) || table.names.contains_key(&unit.name) {
                continue;
            }
            for link in [&mut unit.owner, &mut unit.parent] {
                let known = replay
                    .runs
                    .iter()
                    .any(|r| r.spawn.id.parse().ok().map(UnitId) == Some(*link));
                if !known {
                    *link = self.main;
                }
            }
            table.handles.insert(unit.handle.clone(), unit.id);
            table.names.insert(unit.name.clone(), unit.id);
            table.tombstones.insert(unit.id, unit);
            adopted += 1;
        }
    }

    /// Runs that left memory and are visible to `caller`: all for `main` and
    /// the user, the ones it owns for an agent.
    pub fn tombstones(&self, caller: UnitId) -> Vec<TombstoneView> {
        let table = self.table();
        let mut out: Vec<TombstoneView> = table
            .tombstones
            .values()
            .filter(|u| caller == self.main || caller == self.user || u.owner == caller)
            .map(|u| TombstoneView {
                id: u.id,
                handle: u.handle.clone(),
                name: u.name.clone(),
                kind: u.kind.kind(),
                reason: u.state.label().to_owned(),
                revivable: u.kind.kind() == RunKind::Agent
                    && self
                        .transcript_file(u.handle.as_str())
                        .is_some_and(|p| p.exists()),
            })
            .collect();
        out.sort_by(|a, b| a.handle.as_str().cmp(b.handle.as_str()));
        out
    }

    /// Lets idle agents and old finished runs go (design 7.4). Cheap enough to
    /// call from every state change; there is no timer, so an expired idle
    /// agent leaves at the next one.
    pub fn sweep(&self) {
        let mut table = self.table();
        let retired = self.sweep_locked(&mut table);
        self.release(table);
        if retired.is_empty() {
            return;
        }
        for run in retired {
            let left = run.cancel_run();
            self.messages_dropped(&left, "recipient_stopped");
        }
        self.settled.notify(usize::MAX);
    }

    fn sweep_locked(&self, table: &mut UnitTable) -> Vec<Arc<AgentRun>> {
        let now = self.clock.now();
        let mut retired = Vec::new();
        let mut idle: Vec<(Instant, UnitId)> = table
            .units
            .values()
            .filter(|u| matches!(u.kind, UnitKind::Agent(_)) && matches!(u.state, UnitState::Idle))
            .map(|u| (u.last_activity, u.id))
            .collect();
        idle.sort_by_key(|(at, _)| *at);
        let mut over = idle.len().saturating_sub(usize::from(self.limits.max_idle));
        for (at, id) in idle {
            let expired = now.saturating_duration_since(at) >= self.limits.idle_timeout;
            if !(expired || over > 0) || table.live_children(id) > 0 {
                continue;
            }
            over = over.saturating_sub(1);
            if let Some(run) = self.retire_idle(table, id) {
                retired.push(run);
            }
        }
        let all_done = table.units.values().filter(|u| u.state.is_done()).count();
        let excess = all_done.saturating_sub(usize::from(self.limits.max_retained));
        if excess > 0 {
            let mut done: Vec<(Option<SystemTime>, UnitId)> = table
                .units
                .values()
                .filter(|u| u.state.is_done() && table.live_children(u.id) == 0)
                .map(|u| (u.finished_at, u.id))
                .collect();
            done.sort_by_key(|(at, _)| *at);
            for (_, id) in done.into_iter().take(excess) {
                self.tombstone(table, id);
            }
        }
        retired
    }

    /// An idle agent leaves memory with no notice: it becomes `Done(Completed)`
    /// and its transcript stays.
    fn retire_idle(&self, table: &mut UnitTable, id: UnitId) -> Option<Arc<AgentRun>> {
        let result = table.idle_results.remove(&id).unwrap_or_default();
        let unit = table.units.get_mut(&id)?;
        let terminal = Terminal {
            reason: DoneReason::Completed,
            exit_code: None,
            detail: Some("left memory while idle".to_owned()),
            result,
        };
        self.jot(JournalRecord::Term(journal::term_record(
            unit.handle.as_str(),
            &terminal,
            unit.cost,
        )));
        let handle = unit.handle.to_string();
        unit.state = UnitState::Done(Box::new(terminal));
        unit.finished_at = Some(SystemTime::now());
        unit.last_activity = self.clock.now();
        table.pool.drop_run(id);
        let event = table.event_of(
            id,
            AgentEvent::RunFinished {
                handle,
                reason: DoneReason::Completed.as_str().to_owned(),
                detail: None,
            },
        );
        table.outbox.push(event);
        table.agents.get(&id).cloned()
    }

    /// Moves a finished run out of the live table. Its handle and name keep
    /// pointing at it, so `send` by either still reaches it.
    fn tombstone(&self, table: &mut UnitTable, id: UnitId) {
        let Some(unit) = table.units.remove(&id) else {
            return;
        };
        table.order.retain(|o| *o != id);
        if let Some(siblings) = table.children.get_mut(&unit.parent) {
            siblings.retain(|c| *c != id);
        }
        table.agents.remove(&id);
        table.shells.remove(&id);
        table.idle_results.remove(&id);
        table.pool.drop_run(id);
        table.tombstones.insert(id, unit);
    }
}
