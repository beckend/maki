//! The hub's side of the run journal (design 7.3): what gets recorded, and
//! `reconcile`, which settles what a dead process left behind.
//!
//! Records are queued to the storage writer thread with a non-blocking send,
//! so they are safe to emit under the hub lock. Nothing here awaits.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use maki_storage::id::MakiId;
use maki_storage::runs::{
    AckRec, MarkRec, MsgRec, NoticeRec, ProcRec, RunJournal, RunState, SpawnRec, StateRec, TermRec,
    proc_info,
};
use maki_storage::runs::{JournalRecord, Replay};
use serde_json::{Value, json};

use super::inbox::InboxItem;
use super::notice::{NoticeInfo, NoticeKind};
use super::*;

/// What [`UnitHub::reconcile`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Handles of the runs that were still going when the process died.
    pub lost: Vec<String>,
    /// Process groups that belonged to a recorded shell and were signalled.
    pub orphans_killed: Vec<u32>,
    /// Notices from before the crash that were never delivered, queued again.
    pub requeued_notices: usize,
    /// The journal is from a newer binary: runs are listed, nothing is written.
    pub read_only: bool,
}

pub(super) fn spawn_record(unit: &Unit, model: Option<&str>) -> SpawnRec {
    let spec = match &unit.kind {
        UnitKind::Agent(s) => json!({
            "prompt": s.prompt,
            "subagent_type": s.subagent_type,
            "audience": s.subagent_type,
            "model": model,
            "model_tier": s.model_tier,
            "thinking": s.thinking,
            "output_schema": s.output_schema,
            "fork": s.fork,
            "one_shot": s.one_shot,
            "timeout_secs": s.timeout_secs,
        }),
        UnitKind::Shell(s) => json!({
            "command": s.command,
            "cwd": s.cwd,
            "timeout_secs": s.timeout_secs,
            "watch": s.watch,
        }),
    };
    SpawnRec {
        id: unit.id.0.to_string(),
        handle: unit.handle.to_string(),
        name: unit.name.to_string(),
        kind: unit.kind.kind().as_str().to_owned(),
        owner: unit.owner.0.to_string(),
        parent: unit.parent.0.to_string(),
        depth: unit.depth,
        desc: unit.description.clone(),
        origin_tool_use_id: unit.origin_tool_use_id.as_deref().map(str::to_owned),
        notify: format!("{:?}", unit.notice_policy).to_lowercase(),
        spec,
    }
}

pub(super) fn term_record(handle: &str, terminal: &Terminal, cost: f64) -> TermRec {
    TermRec {
        handle: handle.to_owned(),
        reason: terminal.reason.as_str().to_owned(),
        detail: terminal.detail.clone(),
        exit_code: terminal.exit_code,
        cost,
        turns: terminal.result.turns,
        elapsed_ms: terminal.result.elapsed_ms,
        result_len: terminal.result.summary.len() as u64,
        body_ref: terminal
            .result
            .body_ref
            .as_ref()
            .map(|b| b.0.display().to_string()),
    }
}

pub(super) fn message_record(item: &InboxItem, to: &str, stage: &str) -> MsgRec {
    MsgRec {
        id: item.id.to_string(),
        from: item.from_label.clone(),
        to: to.to_owned(),
        kind: format!("{:?}", item.kind).to_lowercase(),
        bytes: item.text.len() as u64,
        stage: stage.to_owned(),
        body: Some(item.text.clone()),
    }
}

pub(super) fn notice_record(info: &NoticeInfo, wake: bool, text: &str, state: &str) -> NoticeRec {
    NoticeRec {
        run: info.handle.clone(),
        kind: info.kind.as_str().to_owned(),
        generation: info.generation,
        wake,
        text: text.to_owned(),
        state: state.to_owned(),
    }
}

fn time_of(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

fn ms_now() -> u64 {
    maki_storage::runs::system_now_ms()
}

impl UnitHub {
    /// Starts journaling to `journal`. Done once, before the first spawn;
    /// without it the hub is memory-only.
    pub fn attach_journal(&self, journal: Arc<RunJournal>) {
        let _ = self.journal.set(journal);
    }

    pub(super) fn jot(&self, rec: JournalRecord) {
        if let Some(journal) = self.journal.get() {
            journal.append(rec);
        }
    }

    pub(super) fn jot_state(&self, handle: &str, state: &str) {
        self.jot(JournalRecord::State(StateRec {
            handle: handle.to_owned(),
            state: state.to_owned(),
        }));
    }

    /// A shell's group is running: enough to recognise and kill it after a
    /// crash. Written as soon as the process exists.
    pub(super) fn record_proc(&self, id: UnitId, pgid: u32) {
        let Some(info) = proc_info(pgid) else { return };
        let Some(handle) = self.table().units.get(&id).map(|u| u.handle.to_string()) else {
            return;
        };
        self.jot(JournalRecord::Proc(ProcRec {
            handle,
            pgid,
            start_ticks: info.start_ticks,
        }));
    }

    pub(super) fn jot_ack(
        &self,
        item: &InboxItem,
        stage: &str,
        step: Option<u32>,
        why: Option<&str>,
    ) {
        self.jot(JournalRecord::Ack(AckRec {
            id: item.id.to_string(),
            stage: stage.to_owned(),
            step,
            reason: why.map(str::to_owned),
        }));
    }

    /// Whether the journal gave up after a write error.
    pub fn journal_degraded(&self) -> bool {
        self.journal.get().is_some_and(|j| j.degraded())
    }

    /// Writes the clean-shutdown mark and waits for the file to catch up.
    pub(super) fn journal_mark(&self, reason: &str) {
        if let Some(journal) = self.journal.get() {
            journal.append(JournalRecord::Mark(MarkRec {
                reason: reason.to_owned(),
            }));
            journal.flush();
        }
    }

    /// Settles what a process that died left in the journal (design 7.3):
    /// runs that were still going become `Done(Lost)`, shells whose recorded
    /// process is still the same process are killed, and `main` gets one
    /// `Lost` notice. Idempotent, and a no-op without a journal. Call before
    /// any spawn. Blocks only to read the journal.
    pub fn reconcile(self: &Arc<Self>) -> ReconcileReport {
        let Some(journal) = self.journal.get().cloned() else {
            return ReconcileReport::default();
        };
        if self.reconciled.swap(true, Ordering::SeqCst) {
            return ReconcileReport::default();
        }
        let replay = journal.replay();
        let mut report = ReconcileReport {
            read_only: journal.read_only(),
            ..ReconcileReport::default()
        };
        let suspects: Vec<&RunState> = replay.runs.iter().filter(|r| r.term.is_none()).collect();
        let mut lost = Vec::new();
        let mut terms = Vec::new();
        for run in suspects {
            let (detail, killed) = if report.read_only {
                ("session ended", None)
            } else {
                self.settle_orphan(run)
            };
            if let Some(pgid) = killed {
                report.orphans_killed.push(pgid);
            }
            if let Some(unit) = restore_lost(run, detail) {
                if let UnitState::Done(t) = &unit.state {
                    terms.push(term_record(unit.handle.as_str(), t, 0.0));
                }
                lost.push(unit);
            }
        }
        let pending = replay
            .messages
            .iter()
            .filter(|m| m.pending() && lost.iter().any(|u| u.handle.as_str() == m.rec.to))
            .count();
        let notice = self.adopt_lost(lost, pending, &mut report);
        if !report.read_only {
            // The journal learns the outcome, so a second start finds nothing to do.
            for term in terms {
                self.jot(JournalRecord::Term(term));
            }
            report.requeued_notices = self.requeue_notices(&replay);
            if let Some((info, wake, text)) = &notice {
                self.jot(JournalRecord::Notice(notice_record(
                    info, *wake, text, "queued",
                )));
            }
        }
        if let Some((info, wake, text)) = notice {
            self.push_notice(self.main, info, text, wake);
        }
        self.flush_notices();
        report
    }

    fn push_notice(&self, owner: UnitId, info: NoticeInfo, text: String, wake: bool) {
        self.table().pending_notices.push(PendingNotice {
            owner,
            info,
            text,
            wake,
        });
    }

    /// Kills the recorded group of a shell when, and only when, the process
    /// at that pid is the one the journal saw: same start time, still its own
    /// group leader. A reused pid is left alone.
    fn settle_orphan(&self, run: &RunState) -> (&'static str, Option<u32>) {
        let Some(proc) = &run.proc else {
            return if run.spawn.kind == "shell" {
                ("process gone", None)
            } else {
                ("session ended", None)
            };
        };
        match proc_info(proc.pgid) {
            Some(info) if info.start_ticks == proc.start_ticks && info.pgrp == proc.pgid => {
                kill_orphan(proc.pgid, proc.start_ticks, self.shell.grace);
                ("orphan killed", Some(proc.pgid))
            }
            _ => ("process gone", None),
        }
    }

    /// Puts the lost runs in the table, below `main` or below each other as
    /// they were, and builds the one notice that tells `main`.
    fn adopt_lost(
        &self,
        lost: Vec<Unit>,
        unread: usize,
        report: &mut ReconcileReport,
    ) -> Option<(NoticeInfo, bool, String)> {
        if lost.is_empty() {
            return None;
        }
        let mut table = self.table();
        let known: Vec<UnitId> = lost.iter().map(|u| u.id).collect();
        let mut adopted = Vec::new();
        for mut unit in lost {
            if table.handles.contains_key(&unit.handle) || table.names.contains_key(&unit.name) {
                continue;
            }
            let id = unit.id;
            for link in [&mut unit.owner, &mut unit.parent] {
                if !known.contains(link) {
                    *link = self.main;
                }
            }
            table.handles.insert(unit.handle.clone(), id);
            table.names.insert(unit.name.clone(), id);
            table.children.entry(unit.parent).or_default().push(id);
            table.order.push(id);
            table.settle_gen.insert(id, 1);
            report.lost.push(unit.handle.to_string());
            adopted.push((
                unit.handle.clone(),
                unit.name.clone(),
                unit.kind.kind(),
                unit.notice_policy,
                unit.state.clone(),
            ));
            table.units.insert(id, unit);
        }
        drop(table);
        let loud: Vec<_> = adopted
            .iter()
            .filter(|a| a.3 != NoticePolicy::Never)
            .collect();
        let first = loud.first()?;
        let wake = loud.iter().any(|a| a.3 == NoticePolicy::Wake);
        let describe = |a: &&(UnitHandle, UnitName, RunKind, NoticePolicy, UnitState)| {
            let why = match &a.4 {
                UnitState::Done(t) => t.detail.as_deref().unwrap_or("session ended"),
                _ => "session ended",
            };
            format!("{} \"{}\" ({}, {why})", a.0, a.1, a.2.as_str())
        };
        let mut text = if let [only] = loud.as_slice() {
            format!(
                "Run {} \"{}\" ({}) was lost: the session ended while it was running ({}). Inspect it with runs(id) or start a new run.",
                only.0,
                only.1,
                only.2.as_str(),
                match &only.4 {
                    UnitState::Done(t) => t.detail.as_deref().unwrap_or("session ended"),
                    _ => "session ended",
                }
            )
        } else {
            let shown: Vec<String> = loud.iter().take(8).map(describe).collect();
            let more = loud.len().saturating_sub(8);
            format!(
                "{} runs were lost: the session ended while they were running: {}{}. Inspect them with runs() or start new runs.",
                loud.len(),
                shown.join(", "),
                if more > 0 {
                    format!(" and {more} more")
                } else {
                    String::new()
                }
            )
        };
        if unread > 0 {
            text.push_str(&format!(
                " {unread} unread messages to them were not delivered."
            ));
        }
        let info = NoticeInfo {
            kind: NoticeKind::Lost,
            run: self.table().handles.get(&first.0).copied()?,
            handle: first.0.to_string(),
            name: first.1.to_string(),
            generation: ms_now(),
            count: 1,
            created: SystemTime::now(),
        };
        Some((info, wake, text))
    }

    /// Notices queued before the crash and never delivered or read.
    fn requeue_notices(&self, replay: &Replay) -> usize {
        let mut n = 0;
        for rec in replay.notices.iter().filter(|n| n.state == "queued") {
            let Some(kind) = NoticeKind::parse(&rec.kind) else {
                continue;
            };
            let run = replay
                .runs
                .iter()
                .find(|r| r.spawn.handle == rec.run)
                .and_then(|r| r.spawn.id.parse::<MakiId>().ok())
                .map_or_else(UnitId::new, UnitId);
            let info = NoticeInfo {
                kind,
                run,
                handle: rec.run.clone(),
                name: replay
                    .runs
                    .iter()
                    .find(|r| r.spawn.handle == rec.run)
                    .map_or_else(String::new, |r| r.spawn.name.clone()),
                generation: rec.generation,
                count: 1,
                created: SystemTime::now(),
            };
            self.push_notice(self.main, info, rec.text.clone(), rec.wake);
            n += 1;
        }
        n
    }
}

/// The table entry for a run the journal shows as unfinished.
fn restore_lost(run: &RunState, detail: &str) -> Option<Unit> {
    let spawn = &run.spawn;
    let spec = &spawn.spec;
    let text = |key: &str| spec.get(key).and_then(Value::as_str).map(str::to_owned);
    let flag = |key: &str| spec.get(key).and_then(Value::as_bool).unwrap_or(false);
    let secs = spec
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_AGENT_TIMEOUT_SECS);
    let kind = if spawn.kind == "shell" {
        UnitKind::Shell(ShellSpec {
            command: text("command").unwrap_or_default(),
            cwd: text("cwd"),
            timeout_secs: secs,
            watch: text("watch"),
        })
    } else {
        UnitKind::Agent(AgentSpec {
            prompt: text("prompt").unwrap_or_default(),
            subagent_type: text("subagent_type").unwrap_or_else(|| "general".to_owned()),
            model_tier: text("model_tier"),
            thinking: text("thinking"),
            output_schema: spec.get("output_schema").filter(|v| !v.is_null()).cloned(),
            fork: flag("fork"),
            one_shot: flag("one_shot"),
            timeout_secs: secs,
        })
    };
    let id = UnitId(spawn.id.parse::<MakiId>().ok()?);
    let owner = spawn.owner.parse::<MakiId>().map_or_else(|_| id, UnitId);
    let parent = spawn
        .parent
        .parse::<MakiId>()
        .map_or_else(|_| owner, UnitId);
    let notice_policy = match spawn.notify.as_str() {
        "quiet" => NoticePolicy::Quiet,
        "never" => NoticePolicy::Never,
        _ => NoticePolicy::Wake,
    };
    let now = Instant::now();
    Some(Unit {
        id,
        handle: UnitHandle(spawn.handle.as_str().into()),
        name: UnitName(spawn.name.as_str().into()),
        owner,
        parent,
        depth: spawn.depth,
        kind,
        description: spawn.desc.clone(),
        origin_tool_use_id: spawn.origin_tool_use_id.as_deref().map(Arc::from),
        state: UnitState::Done(Box::new(Terminal {
            reason: DoneReason::Lost,
            exit_code: None,
            detail: Some(detail.to_owned()),
            result: UnitResult::default(),
        })),
        created_at: time_of(run.spawn_ts),
        started_at: Some(time_of(run.spawn_ts)),
        finished_at: Some(SystemTime::now()),
        last_activity: now,
        progress: None,
        usage: Default::default(),
        cost: 0.0,
        notice_policy,
    })
}

/// SIGTERM the group, then SIGKILL after `grace` if the same process is still
/// there. Runs on its own thread so starting a session never waits on it.
#[cfg(unix)]
fn kill_orphan(pgid: u32, start_ticks: u64, grace: Duration) {
    shell::term_group(pgid);
    let _ = std::thread::Builder::new()
        .name("maki-orphan".into())
        .spawn(move || {
            let deadline = Instant::now() + grace;
            while Instant::now() < deadline && shell::group_exists(pgid) {
                std::thread::sleep(Duration::from_millis(50));
            }
            // The pid may have been reused in the meantime: check again.
            if proc_info(pgid).is_some_and(|i| i.start_ticks == start_ticks) {
                crate::child_guard::sigkill_process_group(pgid);
            }
        });
}

#[cfg(not(unix))]
fn kill_orphan(_: u32, _: u64, _: Duration) {}

#[cfg(test)]
mod tests {
    use std::process::{Child, Command};

    use maki_storage::runs::JournalOptions;

    use super::*;
    use crate::runs::inbox::{DeliveryKind, MessageId};

    fn hub_with(dir: &std::path::Path) -> Arc<UnitHub> {
        let (tx, _rx) = flume::unbounded::<Envelope>();
        let hub = UnitHub::with_shell_options(
            UnitLimits::default(),
            EventSender::new(tx, 0),
            Arc::new(ManualClock::default()),
            ShellOptions {
                grace: Duration::from_millis(200),
                ..ShellOptions::default()
            },
        );
        let journal = RunJournal::open_in(dir.to_owned(), JournalOptions::default()).unwrap();
        hub.attach_journal(Arc::new(journal));
        hub
    }

    /// What a dead process left: spawn records with no `term`.
    fn crashed_journal(dir: &std::path::Path, records: Vec<JournalRecord>) {
        let journal = RunJournal::open_in(dir.to_owned(), JournalOptions::default()).unwrap();
        for rec in records {
            journal.append(rec);
        }
        journal.flush();
    }

    fn spawn_rec(handle: &str, kind: &str, notify: &str) -> JournalRecord {
        JournalRecord::Spawn(SpawnRec {
            id: MakiId::generate().to_string(),
            handle: handle.into(),
            name: format!("n-{handle}"),
            kind: kind.into(),
            owner: MakiId::generate().to_string(),
            parent: MakiId::generate().to_string(),
            depth: 1,
            notify: notify.into(),
            spec: json!({"prompt": "p", "command": "sleep 311", "timeout_secs": 60}),
            ..SpawnRec::default()
        })
    }

    fn sleeper() -> Child {
        let mut cmd = Command::new("sleep");
        cmd.arg("311");
        std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
        cmd.spawn().unwrap()
    }

    fn until(what: &str, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !f() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn lost_view(hub: &UnitHub, handle: &str) -> UnitView {
        let id = hub.resolve(hub.main(), handle).unwrap();
        hub.get(id).unwrap()
    }

    #[test]
    fn running_agent_becomes_lost() {
        let dir = tempfile::tempdir().unwrap();
        crashed_journal(
            dir.path(),
            vec![
                spawn_rec("ag-aaaaaa", "agent", "wake"),
                JournalRecord::State(StateRec {
                    handle: "ag-aaaaaa".into(),
                    state: "running".into(),
                }),
                spawn_rec("ag-bbbbbb", "agent", "wake"),
                JournalRecord::Term(TermRec {
                    handle: "ag-bbbbbb".into(),
                    reason: "completed".into(),
                    ..TermRec::default()
                }),
            ],
        );
        let hub = hub_with(dir.path());
        let report = hub.reconcile();
        assert_eq!(report.lost, vec!["ag-aaaaaa".to_owned()]);
        let view = lost_view(&hub, "ag-aaaaaa");
        assert_eq!(view.state.label(), "lost");
        // The completed one is history, not a lost run.
        assert!(hub.resolve(hub.main(), "ag-bbbbbb").is_err());
        // The journal now says so, so a restart has nothing left to settle.
        hub.journal.get().unwrap().flush();
        drop(hub);
        let again = hub_with(dir.path());
        assert!(again.reconcile().lost.is_empty());
        assert!(again.pending_notices(again.main()).len() <= 1);
    }

    #[test]
    fn reconcile_without_journal_is_a_noop() {
        let (tx, _rx) = flume::unbounded::<Envelope>();
        let hub = UnitHub::new(
            UnitLimits::default(),
            EventSender::new(tx, 0),
            Arc::new(ManualClock::default()),
        );
        assert_eq!(hub.reconcile(), ReconcileReport::default());
    }

    #[test]
    fn lost_notice_queued_once_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        crashed_journal(
            dir.path(),
            vec![
                spawn_rec("ag-aaaaaa", "agent", "wake"),
                spawn_rec("sh-cccccc", "shell", "wake"),
            ],
        );
        let hub = hub_with(dir.path());
        hub.reconcile();
        hub.reconcile();
        let notices = hub.pending_notices(hub.main());
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert_eq!(notices[0].kind, NoticeKind::Lost);
        assert!(
            notices[0].text.starts_with("2 runs were lost"),
            "{}",
            notices[0].text
        );
        assert!(notices[0].wake);
        // Quit before the model saw it: the next start tells it, once.
        drop(hub);
        let hub = hub_with(dir.path());
        let report = hub.reconcile();
        assert_eq!(report.requeued_notices, 1);
        let items = hub.main_inbox.drain_step();
        assert_eq!(items.len(), 1);
        hub.message_delivered(&items, Some(1));
        hub.journal.get().unwrap().flush();
        drop(hub);
        // Delivered: a third start says nothing.
        let hub = hub_with(dir.path());
        let report = hub.reconcile();
        assert_eq!(report.requeued_notices, 0);
        assert!(hub.pending_notices(hub.main()).is_empty());
    }

    #[test]
    fn single_lost_run_uses_the_per_run_wording() {
        let dir = tempfile::tempdir().unwrap();
        crashed_journal(dir.path(), vec![spawn_rec("ag-aaaaaa", "agent", "quiet")]);
        let hub = hub_with(dir.path());
        hub.reconcile();
        let notices = hub.pending_notices(hub.main());
        assert!(
            notices[0]
                .text
                .starts_with("Run ag-aaaaaa \"n-ag-aaaaaa\" (agent) was lost")
        );
        assert!(!notices[0].wake);
    }

    #[test]
    fn orphan_killed_only_when_start_time_matches() {
        let dir = tempfile::tempdir().unwrap();
        let mut right = sleeper();
        let mut recycled = sleeper();
        let ticks = |c: &Child| proc_info(c.id()).unwrap().start_ticks;
        let proc = |handle: &str, c: &Child, skew: u64| {
            JournalRecord::Proc(ProcRec {
                handle: handle.into(),
                pgid: c.id(),
                start_ticks: ticks(c) + skew,
            })
        };
        crashed_journal(
            dir.path(),
            vec![
                spawn_rec("sh-aaaaaa", "shell", "wake"),
                proc("sh-aaaaaa", &right, 0),
                // The recorded process is gone and its pid now belongs to
                // another one: the start time differs.
                spawn_rec("sh-bbbbbb", "shell", "wake"),
                proc("sh-bbbbbb", &recycled, 1),
            ],
        );
        let hub = hub_with(dir.path());
        let report = hub.reconcile();
        assert_eq!(report.orphans_killed, vec![right.id()]);
        until("orphan to die", || right.try_wait().unwrap().is_some());
        assert!(
            recycled.try_wait().unwrap().is_none(),
            "recycled pid was killed"
        );
        assert!(proc_info(recycled.id()).is_some());
        let state = |h: &str| match lost_view(&hub, h).state {
            UnitState::Done(t) => t.detail.unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(state("sh-aaaaaa"), "orphan killed");
        assert_eq!(state("sh-bbbbbb"), "process gone");
        let _ = recycled.kill();
        let _ = recycled.wait();
    }

    #[test]
    fn a_non_leader_at_the_recorded_pid_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        // Same process, same start time, but not the leader of its own group.
        let mut child = Command::new("sleep").arg("311").spawn().unwrap();
        crashed_journal(
            dir.path(),
            vec![
                spawn_rec("sh-aaaaaa", "shell", "wake"),
                JournalRecord::Proc(ProcRec {
                    handle: "sh-aaaaaa".into(),
                    pgid: child.id(),
                    start_ticks: proc_info(child.id()).unwrap().start_ticks,
                }),
            ],
        );
        let hub = hub_with(dir.path());
        assert!(hub.reconcile().orphans_killed.is_empty());
        assert!(child.try_wait().unwrap().is_none());
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn live_spawn_is_journaled_and_settles() {
        let dir = tempfile::tempdir().unwrap();
        let hub = hub_with(dir.path());
        let req: SpawnRequest = serde_json::from_value(json!({
            "kind": "shell", "description": "echo", "command": "echo hi"
        }))
        .unwrap();
        let ack = hub.spawn(hub.main(), req).unwrap();
        let journal = Arc::clone(hub.journal.get().unwrap());
        until("term record", || {
            journal
                .replay()
                .runs
                .iter()
                .any(|r| r.spawn.handle == ack.handle.to_string() && r.term.is_some())
        });
        let run = &journal.replay().runs[0];
        assert!(run.proc.is_some());
        assert_eq!(run.spawn.spec["command"], "echo hi");
        assert_eq!(run.term.as_ref().unwrap().exit_code, Some(0));
    }

    #[test]
    fn messages_and_acks_are_journaled() {
        let dir = tempfile::tempdir().unwrap();
        crashed_journal(dir.path(), vec![]);
        let hub = hub_with(dir.path());
        let item = InboxItem::new(
            MessageId(7),
            hub.main(),
            "main".into(),
            DeliveryKind::Steer,
            "do the thing".into(),
            Instant::now(),
        );
        hub.jot(JournalRecord::Msg(message_record(
            &item, "ag-x", "accepted",
        )));
        hub.jot_ack(&item, "delivered", Some(2), None);
        let replay = hub.journal.get().unwrap().replay();
        assert_eq!(replay.messages.len(), 1);
        assert!(!replay.messages[0].pending());
        assert_eq!(replay.messages[0].rec.body.as_deref(), Some("do the thing"));
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;

    fn events() -> EventSender {
        let (tx, rx) = flume::unbounded::<Envelope>();
        std::mem::forget(rx);
        EventSender::new(tx, 0)
    }

    #[test]
    fn disabled_creates_no_files() {
        let root = tempfile::tempdir().unwrap();
        let config = maki_config::RunsConfig {
            enabled: false,
            ..maki_config::RunsConfig::default()
        };
        let ctx = start_session_in(&config, events(), MakiId::generate(), true, |_| {
            panic!("a disabled session must not touch the runs dir")
        });
        assert!(ctx.is_none());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn enabled_journals_a_new_session_in_a_private_dir() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("s1");
        let config = maki_config::RunsConfig {
            enabled: true,
            ..maki_config::RunsConfig::default()
        };
        let target = dir.clone();
        let ctx = start_session_in(&config, events(), MakiId::generate(), true, move |_| {
            std::fs::create_dir_all(&target).map(|()| target)
        })
        .unwrap();
        assert!(ctx.hub.journal.get().is_some());
        assert!(dir.join(maki_storage::runs::JOURNAL_FILE).exists());
    }
}
