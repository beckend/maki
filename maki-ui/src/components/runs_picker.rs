//! `/runs`: one row per native run, children under the run that started them.
//! Enter focuses the run's tab; stopping is `/stop`.

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::Rect;

use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::repaint::Cadence;

const TITLE: &str = " Runs ";

pub(crate) struct RunRow {
    pub(crate) handle: String,
    /// The name, indented by depth.
    pub(crate) label: String,
    pub(crate) kind: String,
    pub(crate) state: String,
    pub(crate) age_secs: u64,
    pub(crate) waiting: bool,
}

impl RunRow {
    fn glyph(&self) -> &'static str {
        match self.state.as_str() {
            "running" => "\u{25b6}",
            "queued" => "\u{2026}",
            "idle" => "\u{25cb}",
            "stopping" => "\u{25a0}",
            "completed" => "\u{2713}",
            _ => "\u{2717}",
        }
    }

    pub(crate) fn state_text(&self) -> String {
        let bang = if self.waiting { " !" } else { "" };
        format!("{} {}{bang}", self.glyph(), self.state)
    }

    pub(crate) fn detail_text(&self) -> String {
        format!(
            "{} \u{00b7} {} \u{00b7} {}",
            self.handle,
            self.kind,
            age(self.age_secs)
        )
    }
}

fn age(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        _ => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
    }
}

struct Item {
    row: RunRow,
    state: String,
    detail: String,
}

impl PickerItem for Item {
    fn label(&self) -> &str {
        &self.row.label
    }

    fn suffix(&self) -> Option<&str> {
        Some(&self.state)
    }

    fn detail(&self) -> Option<&str> {
        Some(&self.detail)
    }
}

impl From<RunRow> for Item {
    fn from(row: RunRow) -> Self {
        Self {
            state: row.state_text(),
            detail: row.detail_text(),
            row,
        }
    }
}

pub(crate) enum RunsPickerAction {
    Consumed,
    /// Enter on a row: bring this run's tab (or log) forward.
    Focus {
        handle: String,
        kind: String,
    },
    Close,
}

pub(crate) struct RunsPicker {
    picker: ListPicker<Item>,
}

impl RunsPicker {
    pub(crate) fn new() -> Self {
        Self {
            picker: ListPicker::new(),
        }
    }

    pub(crate) fn open(&mut self, rows: Vec<RunRow>) {
        self.picker
            .open(rows.into_iter().map(Item::from).collect(), TITLE);
    }

    pub(crate) fn refresh(&mut self, rows: Vec<RunRow>) {
        self.picker
            .replace_items(rows.into_iter().map(Item::from).collect());
    }

    pub(crate) fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub(crate) fn handle_paste(&mut self, text: &str) -> bool {
        self.picker.handle_paste(text)
    }

    pub(crate) fn handle_key(&mut self, key: KeyEvent) -> RunsPickerAction {
        match self.picker.handle_key(key) {
            PickerAction::Consumed | PickerAction::Toggle(..) => RunsPickerAction::Consumed,
            PickerAction::Select(item) => RunsPickerAction::Focus {
                handle: item.row.handle,
                kind: item.row.kind,
            },
            PickerAction::Close => RunsPickerAction::Close,
        }
    }

    pub(crate) fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.picker.view(frame, area)
    }
}

impl Overlay for RunsPicker {
    fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    fn close(&mut self) {
        self.picker.close()
    }

    fn cadence(&self) -> Cadence {
        self.picker.cadence()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::key;
    use crossterm::event::KeyCode;

    fn row(handle: &str, label: &str, state: &str, waiting: bool) -> RunRow {
        RunRow {
            handle: handle.into(),
            label: label.into(),
            kind: "agent".into(),
            state: state.into(),
            age_secs: 75,
            waiting,
        }
    }

    #[test]
    fn rows_show_tree_state_and_badge() {
        let mut p = RunsPicker::new();
        p.open(vec![
            row("ag-1", "lead", "running", true),
            row("ag-2", "  child", "completed", false),
        ]);
        let backend = ratatui::backend::TestBackend::new(100, 20);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|f| _ = p.view(f, f.area())).unwrap();
        let text = crate::components::buffer_text(terminal.backend().buffer());
        assert!(text.contains("lead"), "{text}");
        assert!(text.contains("  child"), "{text}");
        assert!(text.contains("running !"), "{text}");
        assert!(text.contains("completed"), "{text}");
        assert!(
            text.contains("ag-1 \u{00b7} agent \u{00b7} 1m15s"),
            "{text}"
        );
    }

    #[test]
    fn enter_focuses_the_selected_run() {
        let mut p = RunsPicker::new();
        p.open(vec![
            row("ag-1", "lead", "running", false),
            row("ag-2", "child", "idle", false),
        ]);
        p.handle_key(key(KeyCode::Down));
        assert!(matches!(
            p.handle_key(key(KeyCode::Enter)),
            RunsPickerAction::Focus { ref handle, .. } if handle == "ag-2"
        ));
        assert!(!p.is_open());
    }

    #[test]
    fn age_formats() {
        assert_eq!(age(5), "5s");
        assert_eq!(age(130), "2m10s");
        assert_eq!(age(3720), "1h02m");
    }
}
