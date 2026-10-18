//! Run-originated text for the model. Phase 2 only queues notices in the
//! hub; `wait` and `runs` hand them out until delivery lands in phase 4b.

use std::fmt::Write;
use std::time::SystemTime;

use super::{DoneReason, Terminal, UnitHandle, UnitId, UnitName};
use crate::tools::compact_duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeKind {
    Settled,
}

#[derive(Debug, Clone)]
pub struct Notice {
    /// Per-owner monotonic.
    pub seq: u64,
    pub run: UnitId,
    pub kind: NoticeKind,
    pub wake: bool,
    pub text: String,
    pub created: SystemTime,
}

/// 3.7 wording for a shell that reached `Done`.
pub(super) fn shell_settled(
    handle: &UnitHandle,
    name: &UnitName,
    terminal: &Terminal,
    timeout_secs: u64,
    output_bytes: u64,
) -> String {
    let elapsed = compact_duration(terminal.result.elapsed_ms / 1000);
    let mut text = format!("Run {handle} \"{name}\" (shell) ");
    match (
        terminal.reason,
        terminal.exit_code,
        terminal.detail.as_deref(),
    ) {
        (DoneReason::Killed, ..) => text.push_str("was stopped before it finished."),
        (DoneReason::TimedOut, ..) => {
            let _ = write!(text, "timed out after {timeout_secs}s.");
        }
        (_, Some(code), _) => {
            let _ = write!(text, "exited {code} after {elapsed}.");
        }
        (_, None, Some(detail)) if detail.starts_with("killed by ") => {
            let _ = write!(text, "was {detail}.");
        }
        (_, None, detail) => {
            let _ = write!(text, "failed: {}.", detail.unwrap_or("unknown error"));
        }
    }
    if !terminal.result.summary.is_empty() {
        let _ = write!(text, "\nLast output:\n{}", terminal.result.summary);
    }
    let _ = write!(
        text,
        "\n{output_bytes} bytes of output; read more with runs(id=\"{handle}\")"
    );
    text
}
