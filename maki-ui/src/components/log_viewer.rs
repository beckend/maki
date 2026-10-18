//! A transient viewer for a shell run's log file. It follows the file by byte
//! offset: each poll reads only what was appended since the last one, so a
//! long log is never re-read. Only the last [`MAX_LINES`] lines are kept.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::components::keybindings::key;
use crate::components::modal::Modal;
use crate::components::scrollbar::render_vertical_scrollbar;
use crate::components::{ModalScroll, Overlay};
use crate::repaint::{Cadence, Dirty};
use crate::theme;

/// What the first read takes from the end of a long log.
const INITIAL_TAIL_BYTES: u64 = 64 * 1024;
/// The most one poll reads, so a flood cannot stall a frame.
const POLL_BYTES: u64 = 256 * 1024;
const MAX_LINES: usize = 2000;
const NO_LOG: &str = "no log for this run (kept in memory only)";

pub(crate) struct LogViewer {
    open: bool,
    title: String,
    path: Option<PathBuf>,
    /// Bytes of the file consumed so far.
    offset: u64,
    lines: Vec<String>,
    /// The last line has no newline yet.
    partial: bool,
    /// Raw bytes of that unfinished line, so a character split across two
    /// polls is decoded whole.
    tail: Vec<u8>,
    scroll: ModalScroll,
}

impl LogViewer {
    pub(crate) fn new() -> Self {
        Self {
            open: false,
            title: String::new(),
            path: None,
            offset: 0,
            lines: Vec::new(),
            partial: false,
            tail: Vec::new(),
            scroll: ModalScroll::new(),
        }
    }

    pub(crate) fn open(&mut self, handle: &str, path: Option<PathBuf>) {
        self.open = true;
        self.title = format!(" Log {handle} ");
        self.offset = path.as_deref().map_or(0, initial_offset);
        self.path = path;
        self.lines.clear();
        self.partial = false;
        self.tail.clear();
        self.scroll = ModalScroll::new();
        _ = self.poll();
    }

    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    #[cfg(test)]
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    #[cfg(test)]
    pub(crate) fn lines(&self) -> &[String] {
        &self.lines
    }

    /// Reads what was appended since the last poll.
    pub(crate) fn poll(&mut self) -> Dirty {
        if !self.open {
            return Dirty::NO;
        }
        let Some(path) = &self.path else {
            return Dirty::NO;
        };
        let Some((chunk, next, restarted)) = read_from(path, self.offset) else {
            return Dirty::NO;
        };
        if next == self.offset && !restarted {
            return Dirty::NO;
        }
        if restarted {
            self.lines.clear();
            self.tail.clear();
            self.partial = false;
        }
        self.offset = next;
        self.append(&chunk);
        Dirty::YES
    }

    fn append(&mut self, chunk: &[u8]) {
        if self.partial {
            self.lines.pop();
        }
        self.tail.extend_from_slice(chunk);
        let mut start = 0;
        while let Some(nl) = self.tail[start..].iter().position(|&b| b == b'\n') {
            let end = start + nl;
            self.lines
                .push(String::from_utf8_lossy(&self.tail[start..end]).into_owned());
            start = end + 1;
        }
        self.tail.drain(..start);
        self.partial = !self.tail.is_empty();
        if self.partial {
            self.lines
                .push(String::from_utf8_lossy(&self.tail).into_owned());
        }
        let over = self.lines.len().saturating_sub(MAX_LINES);
        if over > 0 {
            self.lines.drain(..over);
        }
    }

    pub(crate) fn scroll(&mut self, delta: i32) {
        self.scroll.scroll(delta);
    }

    pub(crate) fn handle_key(&mut self, key_event: KeyEvent) {
        if key_event.code == KeyCode::Esc
            || key_event.code == KeyCode::Char('q')
            || key::QUIT.matches(key_event)
        {
            self.close();
            return;
        }
        self.scroll.handle_key(key_event);
    }

    pub(crate) fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        if !self.open {
            return Rect::default();
        }
        let theme = theme::current();
        let lines: Vec<Line<'static>> = if self.lines.is_empty() {
            let msg = if self.path.is_some() {
                "(no output yet)"
            } else {
                NO_LOG
            };
            vec![Line::from(Span::styled(msg, theme.tool_dim))]
        } else {
            self.lines.iter().map(|l| Line::from(l.clone())).collect()
        };
        let total = u16::try_from(lines.len()).unwrap_or(u16::MAX);
        let modal = Modal {
            title: &self.title,
            width_percent: 80,
            min_width: 0,
            max_height_percent: 80,
        };
        let (popup, inner) = modal.render(frame, area, total);
        self.scroll.update_dimensions(total, inner.height);
        let scroll = self.scroll.offset();
        frame.render_widget(Paragraph::new(lines).scroll((scroll, 0)), inner);
        if total > inner.height {
            render_vertical_scrollbar(frame, inner, u32::from(total), u32::from(scroll));
        }
        popup
    }
}

/// Where the first read starts: the end of the file minus the tail window,
/// moved forward to a line start unless the window reaches the beginning.
fn initial_offset(path: &Path) -> u64 {
    let Ok(len) = std::fs::metadata(path).map(|m| m.len()) else {
        return 0;
    };
    if len <= INITIAL_TAIL_BYTES {
        return 0;
    }
    let start = len - INITIAL_TAIL_BYTES;
    let Ok(mut file) = File::open(path) else {
        return start;
    };
    let mut buf = Vec::new();
    if file.seek(SeekFrom::Start(start)).is_err()
        || file.take(INITIAL_TAIL_BYTES).read_to_end(&mut buf).is_err()
    {
        return start;
    }
    buf.iter()
        .position(|&b| b == b'\n')
        .map_or(start, |nl| start + nl as u64 + 1)
}

/// Up to [`POLL_BYTES`] from `offset`, with the offset after them. A file
/// that shrank is read again from the start.
fn read_from(path: &Path, offset: u64) -> Option<(Vec<u8>, u64, bool)> {
    let mut file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let restarted = len < offset;
    let from = if restarted { 0 } else { offset };
    file.seek(SeekFrom::Start(from)).ok()?;
    let mut chunk = Vec::new();
    file.take(POLL_BYTES).read_to_end(&mut chunk).ok()?;
    let next = from + chunk.len() as u64;
    Some((chunk, next, restarted))
}

impl Overlay for LogViewer {
    fn is_open(&self) -> bool {
        self.open
    }

    fn close(&mut self) {
        self.open = false;
        self.lines.clear();
        self.path = None;
    }

    /// The file grows without telling anyone, so an open viewer looks again.
    fn cadence(&self) -> Cadence {
        Cadence::when(self.open && self.path.is_some(), Cadence::PENDING)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::key;
    use std::io::Write;

    fn append_to(path: &Path, text: &str) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        f.write_all(text.as_bytes()).unwrap();
    }

    #[test]
    fn log_viewer_tails_by_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sh-1.log");
        append_to(&path, "one\ntwo\npar");
        let mut v = LogViewer::new();
        v.open("sh-1", Some(path.clone()));
        assert_eq!(v.lines(), ["one", "two", "par"]);
        assert_eq!(v.offset(), 11);
        assert!(!v.poll().take());

        append_to(&path, "tial\nthree\n");
        assert!(v.poll().take());
        assert_eq!(v.lines(), ["one", "two", "partial", "three"]);
        assert_eq!(v.offset(), 22);
        assert!(!v.poll().take());
    }

    #[test]
    fn long_log_starts_at_a_line_boundary_near_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sh-2.log");
        for i in 0..20_000 {
            append_to(&path, &format!("line {i}\n"));
        }
        let mut v = LogViewer::new();
        v.open("sh-2", Some(path));
        assert!(v.offset() > 0);
        assert_eq!(v.lines().last().unwrap(), "line 19999");
        assert!(v.lines().first().unwrap().starts_with("line "));
        assert!(v.lines().len() <= MAX_LINES);
    }

    #[test]
    fn missing_log_says_so_and_esc_closes() {
        let mut v = LogViewer::new();
        v.open("sh-3", None);
        let backend = ratatui::backend::TestBackend::new(80, 20);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|f| _ = v.view(f, f.area())).unwrap();
        let text = crate::components::buffer_text(terminal.backend().buffer());
        assert!(text.contains("no log for this run"), "{text}");
        assert_eq!(v.cadence(), Cadence::IDLE);
        v.handle_key(key(KeyCode::Esc));
        assert!(!v.is_open());
    }

    #[test]
    fn truncated_file_is_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sh-4.log");
        append_to(&path, "aaaaaaaa\n");
        let mut v = LogViewer::new();
        v.open("sh-4", Some(path.clone()));
        std::fs::write(&path, "b\n").unwrap();
        assert!(v.poll().take());
        assert_eq!(v.lines(), ["b"]);
    }

    #[test]
    fn multibyte_char_split_across_polls_is_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sh-5.log");
        let bytes = "é\n".as_bytes();
        std::fs::write(&path, &bytes[..1]).unwrap();
        let mut v = LogViewer::new();
        v.open("sh-5", Some(path.clone()));
        std::fs::write(&path, bytes).unwrap();
        assert!(v.poll().take());
        assert_eq!(v.lines(), ["é"]);
    }

    #[test]
    fn non_utf8_bytes_are_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sh-6.log");
        std::fs::write(&path, b"a\xffb\n").unwrap();
        let mut v = LogViewer::new();
        v.open("sh-6", Some(path));
        assert_eq!(v.lines(), ["a\u{fffd}b"]);
    }
}
