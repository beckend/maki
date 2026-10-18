//! What the user sees of native runs outside their tabs: the `/runs` rows and
//! the status line segment. Fed by `RunStarted`, `RunStateChanged` and
//! `RunFinished`, so shell runs (which have no tab) are covered too.

use std::time::Instant;

use maki_storage::sessions::StoredRun;

use crate::components::runs_picker::RunRow;

/// States a run can still leave. Everything else is a `DoneReason` spelling.
const LIVE_STATES: [&str; 4] = ["queued", "running", "idle", "stopping"];
pub(crate) const LOST: &str = "lost";

pub(crate) fn is_live(state: &str) -> bool {
    LIVE_STATES.contains(&state)
}

pub(crate) struct RunEntry {
    pub(crate) handle: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) parent: Option<String>,
    pub(crate) state: String,
    pub(crate) started: Instant,
}

#[derive(Default)]
pub(crate) struct RunsView {
    entries: Vec<RunEntry>,
}

impl RunsView {
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn started(
        &mut self,
        handle: &str,
        name: &str,
        kind: &str,
        parent: Option<&str>,
        state: &str,
    ) {
        let entry = RunEntry {
            handle: handle.to_owned(),
            name: name.to_owned(),
            kind: kind.to_owned(),
            parent: parent.map(str::to_owned),
            state: state.to_owned(),
            started: Instant::now(),
        };
        match self.entries.iter_mut().find(|e| e.handle == handle) {
            Some(known) => *known = entry,
            None => self.entries.push(entry),
        }
    }

    /// A run of an earlier process. Whatever it was doing, nothing runs it now,
    /// so a state it could still leave reads as `lost`.
    pub(crate) fn restored(&mut self, run: &StoredRun) {
        let state = if is_live(&run.state) {
            LOST
        } else {
            run.state.as_str()
        };
        self.started(
            &run.handle,
            &run.name,
            &run.kind,
            run.parent.as_deref(),
            state,
        );
    }

    pub(crate) fn set_state(&mut self, handle: &str, state: &str) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.handle == handle) {
            state.clone_into(&mut entry.state);
        }
    }

    #[cfg(test)]
    pub(crate) fn state_of(&self, handle: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.handle == handle)
            .map(|e| e.state.as_str())
    }

    pub(crate) fn live_handles(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|e| is_live(&e.state))
            .map(|e| e.handle.as_str())
            .collect()
    }

    /// Handle of the run `key` names: a handle, or a name (a live run wins
    /// over a finished one of the same name).
    pub(crate) fn find(&self, key: &str) -> Option<&RunEntry> {
        self.entries
            .iter()
            .find(|e| e.handle == key)
            .or_else(|| {
                self.entries
                    .iter()
                    .rev()
                    .find(|e| e.name == key && is_live(&e.state))
            })
            .or_else(|| self.entries.iter().rev().find(|e| e.name == key))
    }

    /// `runs 3 running 1 queued 2 lost !`, nothing while there is nothing to say.
    pub(crate) fn segment(&self, waiting: usize) -> Option<String> {
        let count = |state: &str| self.entries.iter().filter(|e| e.state == state).count();
        let (running, queued, lost) = (count("running"), count("queued"), count(LOST));
        if running + queued + lost + waiting == 0 {
            return None;
        }
        let mut out = String::from("runs");
        for (n, word) in [(running, "running"), (queued, "queued"), (lost, LOST)] {
            if n > 0 {
                out.push_str(&format!(" {n} {word}"));
            }
        }
        if waiting > 0 {
            out.push_str(" !");
        }
        Some(out)
    }

    /// Tree order: a run follows the run that started it.
    pub(crate) fn rows(&self, waiting: &[&str]) -> Vec<RunRow> {
        let mut out = Vec::with_capacity(self.entries.len());
        let known = |h: &str| self.entries.iter().any(|e| e.handle == h);
        let roots = self
            .entries
            .iter()
            .filter(|e| e.parent.as_deref().is_none_or(|p| !known(p)));
        for root in roots {
            self.push_tree(root, 0, waiting, &mut out);
        }
        out
    }

    fn push_tree(&self, entry: &RunEntry, depth: usize, waiting: &[&str], out: &mut Vec<RunRow>) {
        // Depth is bounded by the hub, but a cycle in restored data must not hang.
        if depth > 16 {
            return;
        }
        out.push(RunRow {
            handle: entry.handle.clone(),
            label: format!("{}{}", "  ".repeat(depth), entry.name),
            kind: entry.kind.clone(),
            state: entry.state.clone(),
            age_secs: entry.started.elapsed().as_secs(),
            waiting: waiting.contains(&entry.handle.as_str()),
        });
        for child in self
            .entries
            .iter()
            .filter(|e| e.parent.as_deref() == Some(entry.handle.as_str()))
        {
            self.push_tree(child, depth + 1, waiting, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view() -> RunsView {
        let mut v = RunsView::default();
        v.started("ag-1", "lead", "agent", None, "running");
        v.started("ag-2", "child", "agent", Some("ag-1"), "queued");
        v.started("sh-1", "build", "shell", None, "running");
        v
    }

    #[test]
    fn segment_hidden_at_zero() {
        let mut v = view();
        assert_eq!(v.segment(0).as_deref(), Some("runs 2 running 1 queued"));
        v.set_state("ag-1", "completed");
        v.set_state("ag-2", "idle");
        v.set_state("sh-1", "killed");
        assert_eq!(v.segment(0), None);
        assert_eq!(v.segment(1).as_deref(), Some("runs !"));
    }

    #[test]
    fn restored_live_runs_read_as_lost() {
        let mut v = RunsView::default();
        let run = |state: &str| StoredRun {
            handle: format!("ag-{state}"),
            name: state.to_owned(),
            parent: None,
            kind: "agent".to_owned(),
            state: state.to_owned(),
            depth: 0,
            one_shot: false,
        };
        v.restored(&run("running"));
        v.restored(&run("completed"));
        assert_eq!(v.state_of("ag-running"), Some("lost"));
        assert_eq!(v.state_of("ag-completed"), Some("completed"));
        assert_eq!(v.segment(0).as_deref(), Some("runs 1 lost"));
    }

    #[test]
    fn rows_follow_the_tree() {
        let mut v = view();
        v.started("ag-3", "grandchild", "agent", Some("ag-2"), "running");
        let rows = v.rows(&["ag-3"]);
        let labels: Vec<_> = rows.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["lead", "  child", "    grandchild", "build"]);
        assert!(rows[2].waiting && !rows[1].waiting);
    }

    #[test]
    fn find_prefers_handle_then_live_name() {
        let mut v = view();
        v.started("ag-9", "child", "agent", None, "completed");
        assert_eq!(v.find("child").unwrap().handle, "ag-2");
        assert_eq!(v.find("ag-9").unwrap().handle, "ag-9");
        assert!(v.find("nope").is_none());
    }
}
