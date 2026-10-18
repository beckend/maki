//! Delivery acks (design 4.6): what became of each message a run sent.
//! Two stages, `accepted` and `delivered`, plus `dropped` with a reason.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::time::Duration;

use super::{MessageId, UnitId};

/// Records kept per sender; the oldest are evicted first, so memory is
/// bounded by `senders x MAX_ACKS_PER_SENDER` small records.
pub const MAX_ACKS_PER_SENDER: usize = 64;

/// A message still undelivered after this long gets a quiet notice.
pub const SLOW_DELIVERY: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckState {
    /// In the recipient's inbox.
    Accepted,
    /// Kept back by the ping-pong guard, not yet in the inbox.
    Held,
    /// In the recipient's history for a request that was sent. `step` is the
    /// request count when the hub knows it.
    Delivered {
        step: Option<u32>,
    },
    Dropped(&'static str),
}

impl AckState {
    fn pending(self) -> bool {
        matches!(self, Self::Accepted | Self::Held)
    }
}

impl fmt::Display for AckState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted => f.write_str("accepted"),
            Self::Held => f.write_str("accepted (held by the ping-pong guard)"),
            Self::Delivered { step: Some(n) } => write!(f, "delivered (step {n})"),
            Self::Delivered { step: None } => f.write_str("delivered"),
            Self::Dropped(reason) => write!(f, "dropped ({reason})"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AckRecord {
    pub id: MessageId,
    pub to: UnitId,
    pub state: AckState,
}

#[derive(Default)]
pub(super) struct Acks {
    by_sender: HashMap<UnitId, VecDeque<AckRecord>>,
}

impl Acks {
    pub(super) fn insert(&mut self, from: UnitId, record: AckRecord) {
        let list = self.by_sender.entry(from).or_default();
        if list.len() >= MAX_ACKS_PER_SENDER {
            list.pop_front();
        }
        list.push_back(record);
    }

    pub(super) fn remove(&mut self, from: UnitId, id: MessageId) {
        if let Some(list) = self.by_sender.get_mut(&from) {
            list.retain(|r| r.id != id);
        }
    }

    /// Moves a pending record on. A final state is never overwritten, so a
    /// late drop cannot undo a delivery. Returns the recipient when it moved.
    pub(super) fn advance(
        &mut self,
        from: UnitId,
        id: MessageId,
        state: AckState,
    ) -> Option<UnitId> {
        let record = self
            .by_sender
            .get_mut(&from)?
            .iter_mut()
            .find(|r| r.id == id && r.state.pending())?;
        record.state = state;
        Some(record.to)
    }

    pub(super) fn is_pending(&self, from: UnitId, id: MessageId) -> bool {
        self.by_sender
            .get(&from)
            .is_some_and(|l| l.iter().any(|r| r.id == id && r.state.pending()))
    }

    /// What `runs(id)` shows: everything pending or dropped, and the last
    /// delivered message, oldest first.
    pub(super) fn to(&self, from: UnitId, to: UnitId) -> Vec<AckRecord> {
        let Some(list) = self.by_sender.get(&from) else {
            return Vec::new();
        };
        let mine: Vec<&AckRecord> = list.iter().filter(|r| r.to == to).collect();
        let last_delivered = mine
            .iter()
            .rposition(|r| matches!(r.state, AckState::Delivered { .. }));
        mine.into_iter()
            .enumerate()
            .filter(|(n, r)| {
                !matches!(r.state, AckState::Delivered { .. }) || Some(*n) == last_delivered
            })
            .map(|(_, r)| *r)
            .collect()
    }
}
