//! Shell `watch`: output lines that match a regex become `Match` notices
//! (design 2b). Lines are split from the merged stdout/stderr stream, then a
//! pure limiter decides when they go out: one notice per second with the
//! lines in between batched, at most `MAX_PER_WINDOW` notices a minute, then
//! one `Overflow` notice and silence until the window rolls over.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use regex::Regex;

use super::clock::Clock;
use super::{UnitHub, UnitId};

/// Notices closer together than this are batched into one.
pub(super) const MIN_GAP: Duration = Duration::from_secs(1);
pub(super) const WINDOW: Duration = Duration::from_secs(60);
pub(super) const MAX_PER_WINDOW: u32 = 20;
/// Lines held between two notices; the rest are only counted.
const MAX_HELD: usize = 50;
/// A line longer than this is matched in pieces.
const MAX_LINE_BYTES: usize = 8 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Out {
    Match {
        lines: Vec<String>,
        extra: usize,
    },
    /// The per-window cap was hit; matches are dropped until it rolls over.
    Overflow,
}

#[derive(Default)]
pub(super) struct Limiter {
    next_at: Option<Instant>,
    window_start: Option<Instant>,
    sent: u32,
    capped: bool,
    held: Vec<String>,
    extra: usize,
}

impl Limiter {
    pub(super) fn offer(&mut self, line: String, now: Instant) -> Option<Out> {
        if self.held.len() < MAX_HELD {
            self.held.push(line);
        } else {
            self.extra += 1;
        }
        self.tick(now)
    }

    /// When held lines may go out; `None` with nothing held.
    pub(super) fn deadline(&self) -> Option<Instant> {
        (!self.held.is_empty()).then(|| self.next_at.unwrap_or_else(Instant::now))
    }

    /// Sends what is held if the gap since the last notice has passed.
    pub(super) fn tick(&mut self, now: Instant) -> Option<Out> {
        if self.held.is_empty() || self.next_at.is_some_and(|at| now < at) {
            return None;
        }
        self.emit(now)
    }

    /// The stream ended: whatever is held goes out, gap or not.
    pub(super) fn finish(&mut self, now: Instant) -> Option<Out> {
        if self.held.is_empty() {
            return None;
        }
        self.emit(now)
    }

    fn emit(&mut self, now: Instant) -> Option<Out> {
        if self
            .window_start
            .is_none_or(|start| now.saturating_duration_since(start) >= WINDOW)
        {
            self.window_start = Some(now);
            self.sent = 0;
            self.capped = false;
        }
        let lines = std::mem::take(&mut self.held);
        let extra = std::mem::take(&mut self.extra);
        if self.sent >= MAX_PER_WINDOW {
            return (!std::mem::replace(&mut self.capped, true)).then_some(Out::Overflow);
        }
        self.sent += 1;
        self.next_at = Some(now + MIN_GAP);
        Some(Out::Match { lines, extra })
    }
}

/// Splits a byte stream into lines; a partial line waits for its newline.
#[derive(Default)]
pub(super) struct Lines {
    partial: Vec<u8>,
}

impl Lines {
    pub(super) fn feed(&mut self, data: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        for &b in data {
            if b == b'\n' {
                out.push(self.take());
                continue;
            }
            if self.partial.len() >= MAX_LINE_BYTES {
                out.push(self.take());
            }
            self.partial.push(b);
        }
        out
    }

    pub(super) fn rest(&mut self) -> Option<String> {
        (!self.partial.is_empty()).then(|| self.take())
    }

    fn take(&mut self) -> String {
        let line = String::from_utf8_lossy(&self.partial)
            .trim_end_matches('\r')
            .to_owned();
        self.partial.clear();
        line
    }
}

pub(super) struct Watch {
    re: Regex,
    hub: Weak<UnitHub>,
    id: UnitId,
    pub(super) clock: Arc<dyn Clock>,
    lines: Lines,
    limiter: Limiter,
}

impl Watch {
    pub(super) fn new(
        pattern: &str,
        hub: Weak<UnitHub>,
        id: UnitId,
        clock: Arc<dyn Clock>,
    ) -> Option<Self> {
        let re = Regex::new(pattern)
            .inspect_err(|e| tracing::warn!(%e, "shell watch regex rejected"))
            .ok()?;
        Some(Self {
            re,
            hub,
            id,
            clock,
            lines: Lines::default(),
            limiter: Limiter::default(),
        })
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.limiter.deadline()
    }

    pub(super) fn feed(&mut self, data: &[u8]) {
        for line in self.lines.feed(data) {
            self.line(line);
        }
    }

    fn line(&mut self, line: String) {
        if self.re.is_match(&line) {
            let out = self.limiter.offer(line, self.clock.now());
            self.send(out);
        }
    }

    pub(super) fn tick(&mut self) {
        let out = self.limiter.tick(self.clock.now());
        self.send(out);
    }

    pub(super) fn finish(&mut self) {
        if let Some(rest) = self.lines.rest() {
            self.line(rest);
        }
        let out = self.limiter.finish(self.clock.now());
        self.send(out);
    }

    fn send(&self, out: Option<Out>) {
        if let (Some(out), Some(hub)) = (out, self.hub.upgrade()) {
            hub.watch_notice(self.id, self.re.as_str(), out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_cap_line_has_no_empty_tail() {
        let mut l = Lines::default();
        let mut data = vec![b'a'; MAX_LINE_BYTES];
        data.push(b'\n');
        assert_eq!(l.feed(&data).len(), 1);
    }

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn lines(out: Option<Out>) -> Vec<String> {
        match out {
            Some(Out::Match { lines, .. }) => lines,
            other => panic!("expected a match, got {other:?}"),
        }
    }

    #[test]
    fn first_goes_out_at_once_then_batches_per_second() {
        let t0 = Instant::now();
        let mut l = Limiter::default();
        assert_eq!(lines(l.offer("a".into(), t0)), ["a"]);
        assert_eq!(l.offer("b".into(), t0 + Duration::from_millis(100)), None);
        assert_eq!(l.offer("c".into(), t0 + Duration::from_millis(900)), None);
        assert_eq!(l.deadline(), Some(t0 + s(1)));
        assert_eq!(lines(l.tick(t0 + s(1))), ["b", "c"]);
        assert_eq!(l.deadline(), None);
    }

    #[test]
    fn match_notices_rate_limited() {
        let t0 = Instant::now();
        let mut l = Limiter::default();
        for i in 0..u64::from(MAX_PER_WINDOW) {
            assert!(matches!(
                l.offer("x".into(), t0 + s(i)),
                Some(Out::Match { .. })
            ));
        }
        let at = t0 + s(u64::from(MAX_PER_WINDOW));
        assert_eq!(l.offer("x".into(), at), Some(Out::Overflow));
        assert_eq!(l.offer("x".into(), at + s(2)), None);
        assert_eq!(l.offer("x".into(), at + s(4)), None);
        assert!(matches!(
            l.offer("x".into(), t0 + WINDOW + s(1)),
            Some(Out::Match { .. })
        ));
    }

    #[test]
    fn finish_flushes_held_and_overlong_batches_are_counted() {
        let t0 = Instant::now();
        let mut l = Limiter::default();
        l.offer("first".into(), t0);
        for i in 0..MAX_HELD + 3 {
            assert_eq!(l.offer(format!("{i}"), t0), None);
        }
        match l.finish(t0) {
            Some(Out::Match { lines, extra }) => {
                assert_eq!(lines.len(), MAX_HELD);
                assert_eq!(extra, 3);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(l.finish(t0), None);
    }

    #[test]
    fn lines_split_across_chunks_and_flush_the_tail() {
        let mut l = Lines::default();
        assert!(l.feed(b"ab").is_empty());
        assert_eq!(l.feed(b"c\r\nde\nf"), ["abc", "de"]);
        assert_eq!(l.rest().as_deref(), Some("f"));
        assert_eq!(l.rest(), None);
    }

    #[test]
    fn case_insensitive_regex_compiles() {
        assert!(Regex::new("(?i)error").is_ok());
    }
}
