//! Run-originated text for the model. A notice is an inbox item of kind
//! `Notice`; the hub creates it when a run settles and the owner's drains
//! deliver it (design 2.6, 4.8).

use std::fmt::Write;
use std::time::SystemTime;

use super::{DoneReason, Terminal, UnitHandle, UnitId, UnitName};
use crate::tools::compact_duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeKind {
    Settled,
    /// The ping-pong guard held messages between two runs.
    Guard,
    /// Notices were dropped or demoted to protect the owner's context.
    Overflow,
    /// A message the run sent was dropped or is slow to arrive.
    Delivery,
    /// A run's permission prompt has waited a while for the user.
    Permission,
    /// Runs that were still going when the previous process died.
    Lost,
}

impl NoticeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Settled => "settled",
            Self::Guard => "guard",
            Self::Overflow => "overflow",
            Self::Delivery => "delivery",
            Self::Permission => "permission",
            Self::Lost => "lost",
        }
    }

    pub fn parse(kind: &str) -> Option<Self> {
        [
            Self::Settled,
            Self::Guard,
            Self::Overflow,
            Self::Delivery,
            Self::Permission,
            Self::Lost,
        ]
        .into_iter()
        .find(|k| k.as_str() == kind)
    }
}

/// What an inbox item of kind `Notice` knows about itself beyond its text.
#[derive(Debug, Clone)]
pub struct NoticeInfo {
    pub kind: NoticeKind,
    /// The run the notice is about.
    pub run: UnitId,
    pub handle: String,
    pub name: String,
    /// Which settle of the run it reports; a read that saw settle N consumes
    /// the notices up to N and never a later one.
    pub generation: u64,
    /// Identical notices collapse into one with a count.
    pub count: u32,
    pub created: SystemTime,
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

/// Suffix of the file that holds an agent's untruncated result.
pub(super) const BODY_FILE_SUFFIX: &str = ".result.txt";

fn result_tail(text: &mut String, handle: &UnitHandle, result: &super::UnitResult) {
    if !result.summary.is_empty() {
        let _ = write!(text, "\nResult:\n{}", result.summary);
    }
    if let Some(structured) = &result.structured {
        let rendered = structured.to_string();
        if result.summary != rendered {
            let _ = write!(text, "\nStructured: {rendered}");
        }
    }
    if result.truncated {
        let _ = write!(text, "\nFull result: runs(id=\"{handle}\", full=true)");
    }
}

fn stats(result: &super::UnitResult) -> String {
    format!(
        "{}, {} turns, ${:.2}",
        compact_duration(result.elapsed_ms / 1000),
        result.turns,
        result.cost
    )
}

/// 3.7 wording for an agent that finished a turn and went idle.
pub(super) fn agent_idle(
    handle: &UnitHandle,
    name: &UnitName,
    result: &super::UnitResult,
) -> String {
    let mut text = format!(
        "Run {handle} \"{name}\" (agent) finished its turn and is idle; send it a message to continue. {}.",
        stats(result)
    );
    result_tail(&mut text, handle, result);
    text
}

/// 3.7 wording for an agent that reached `Done`.
pub(super) fn agent_settled(
    handle: &UnitHandle,
    name: &UnitName,
    terminal: &Terminal,
    timeout_secs: u64,
) -> String {
    let mut text = format!("Run {handle} \"{name}\" (agent) ");
    match terminal.reason {
        DoneReason::Completed => text.push_str("finished and will do no further work."),
        DoneReason::Killed => text.push_str("was stopped before it finished."),
        DoneReason::MaxTokens | DoneReason::MaxTurns => {
            text.push_str("ran out of room before it finished.");
        }
        DoneReason::TimedOut => {
            let _ = write!(text, "timed out after {timeout_secs}s.");
        }
        DoneReason::Failed => {
            let _ = write!(
                text,
                "failed: {}.",
                terminal.detail.as_deref().unwrap_or("unknown error")
            );
        }
        DoneReason::Refused => text.push_str("declined the task."),
        DoneReason::Lost => text.push_str(
            "was lost: the session ended while it was running; its transcript up to the last saved turn is available with runs(id).",
        ),
    }
    if terminal.reason == DoneReason::Completed {
        let _ = write!(text, " {}.", stats(&terminal.result));
    }
    result_tail(&mut text, handle, &terminal.result);
    text
}
