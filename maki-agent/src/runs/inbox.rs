//! The per-run inbox (design section 4): a bounded queue of messages for one
//! agent run or for `main`, the caps and the rate limit that guard it, and
//! the one place a drained message is turned into text for the model.
//!
//! Lock discipline: the inbox lock is a short `std` critical section. Nothing
//! inside it awaits or does I/O; wakes go through an unbounded channel. When
//! both locks are needed the inbox lock comes first, the hub lock second, and
//! the hub never takes an inbox lock while holding its own.

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Write};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::UnitError;
use super::UnitId;

/// Hard limit of one message, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 16384;
/// Pending items per inbox.
pub const MAX_PENDING: usize = 32;
/// Pending `Followup` items per inbox.
pub const MAX_FOLLOWUPS: usize = 8;
/// Bytes one drain moves into a single request; the rest stays queued.
pub const DRAIN_BYTES: usize = 32 * 1024;
/// Per sender and recipient: this many messages per window, burst included.
pub const PAIR_BURST: u32 = 6;
pub const PAIR_WINDOW: Duration = Duration::from_secs(10);
/// What `INBOX_FULL` tells the sender to wait.
const INBOX_RETRY_SECS: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub u64);

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "m-{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryKind {
    /// Next step boundary of a running recipient; wakes an idle one.
    Steer,
    /// After the current turn; one per turn; wakes an idle one.
    Followup,
    /// The recipient's next model request; never wakes.
    Quiet,
    /// Produced by the hub only; the item's `wake` decides.
    Notice,
}

impl DeliveryKind {
    pub fn parse(mode: &str) -> Option<Self> {
        match mode {
            "steer" => Some(Self::Steer),
            "followup" => Some(Self::Followup),
            "quiet" => Some(Self::Quiet),
            _ => None,
        }
    }

    /// Drain order within one request: Steer, then Notice, then Quiet.
    fn rank(self) -> u8 {
        match self {
            Self::Steer | Self::Followup => 0,
            Self::Notice => 1,
            Self::Quiet => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InboxItem {
    pub id: MessageId,
    pub from: UnitId,
    /// Name the recipient sees: a run name, `main` or `user`.
    pub from_label: String,
    pub kind: DeliveryKind,
    pub wake: bool,
    pub text: String,
    pub enqueued: Instant,
    seq: u64,
}

impl InboxItem {
    pub fn new(
        id: MessageId,
        from: UnitId,
        from_label: String,
        kind: DeliveryKind,
        text: String,
        enqueued: Instant,
    ) -> Self {
        let wake = !matches!(kind, DeliveryKind::Quiet | DeliveryKind::Notice);
        Self {
            id,
            from,
            from_label,
            kind,
            wake,
            text,
            enqueued,
            seq: 0,
        }
    }
}

/// Where the recipient is, as far as waking it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    /// Between turns: a waking item must start one.
    Idle,
    /// The run is over; nothing is accepted any more.
    Closed,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn refill(&mut self, now: Instant) {
        let rate = f64::from(PAIR_BURST) / PAIR_WINDOW.as_secs_f64();
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(f64::from(PAIR_BURST));
        self.last = now;
    }
}

struct State {
    items: VecDeque<InboxItem>,
    phase: Phase,
    /// Bumped by every accepted item.
    seq: u64,
    buckets: HashMap<UnitId, Bucket>,
}

pub struct Inbox {
    state: Mutex<State>,
    wake_tx: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
}

/// What was learned by enqueueing: whether the recipient has to be woken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Accepted {
    Queued,
    /// The recipient is `Idle` and the item wakes it.
    Wake,
}

pub(crate) struct Guard<'a> {
    state: MutexGuard<'a, State>,
}

impl Default for Inbox {
    fn default() -> Self {
        Self::new(Phase::Running)
    }
}

impl Inbox {
    pub(crate) fn new(phase: Phase) -> Self {
        let (wake_tx, wake_rx) = flume::unbounded();
        Self {
            state: Mutex::new(State {
                items: VecDeque::new(),
                phase,
                seq: 0,
                buckets: HashMap::new(),
            }),
            wake_tx,
            wake_rx,
        }
    }

    pub(crate) fn lock(&self) -> Guard<'_> {
        Guard {
            state: self.state.lock().unwrap_or_else(|e| e.into_inner()),
        }
    }

    /// Wakes a driver parked in [`Inbox::woken`]. Never blocks.
    pub(crate) fn signal(&self) {
        let _ = self.wake_tx.send(());
    }

    /// Resolves after a `signal`. Tokens may be stale; the caller re-checks.
    pub(crate) async fn woken(&self) {
        let _ = self.wake_rx.recv_async().await;
    }

    /// The run is over: later sends fail with `RECIPIENT_TERMINAL`.
    pub(crate) fn close(&self) {
        self.lock().set_phase(Phase::Closed);
        self.signal();
    }

    /// Items queued and not yet drained.
    pub fn len(&self) -> usize {
        self.lock().state.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn seq(&self) -> u64 {
        self.lock().state.seq
    }

    /// Drain point 2: before each model request. Everything but `Followup`.
    pub fn drain_step(&self) -> Vec<InboxItem> {
        self.lock().drain(|i| i.kind != DeliveryKind::Followup, 0)
    }

    /// Drain point 1: at the start of a run. `Quiet`, `Steer` and at most one
    /// `Followup`.
    pub fn take_start(&self) -> Vec<InboxItem> {
        self.lock().take_start()
    }

    /// Turn end: a pending `Steer` continues the run (the next request drains
    /// it) and so does one `Followup`, returned here for the caller to land.
    pub fn continue_at_turn_end(&self) -> TurnEnd {
        let mut g = self.lock();
        if g.state
            .items
            .iter()
            .any(|i| i.kind == DeliveryKind::Steer || (i.kind == DeliveryKind::Notice && i.wake))
        {
            return TurnEnd::Steer(g.drain(|i| i.kind != DeliveryKind::Followup, 0));
        }
        let followup = g.drain(|i| i.kind == DeliveryKind::Followup, 1);
        if followup.is_empty() {
            TurnEnd::Done
        } else {
            TurnEnd::Followup(followup)
        }
    }
}

/// What `send` tells its caller.
#[derive(Debug, Clone)]
pub struct SendAck {
    pub id: MessageId,
    /// The recipient's state when the message was accepted.
    pub state: &'static str,
    /// The ping-pong guard kept it back (design 4.7).
    pub held: bool,
}

#[derive(Debug)]
pub enum TurnEnd {
    Done,
    /// A pending `Steer` kept the run going; this is everything drained for it.
    Steer(Vec<InboxItem>),
    Followup(Vec<InboxItem>),
}

impl Guard<'_> {
    pub(crate) fn phase(&self) -> Phase {
        self.state.phase
    }

    pub(crate) fn set_phase(&mut self, phase: Phase) {
        self.state.phase = phase;
    }

    /// Whether something here should start or continue a turn.
    pub(crate) fn has_wakeable(&self) -> bool {
        self.state.items.iter().any(|i| i.wake)
    }

    /// Checks the caps and the pair bucket, then queues. Nothing changes on
    /// an error, so a rejected send costs no rate token.
    #[cfg(test)]
    pub(crate) fn enqueue(&mut self, item: InboxItem, now: Instant) -> Result<Accepted, UnitError> {
        self.enqueue_with(item, now, true)
    }

    /// `rated` is false for a message the ping-pong guard held: it was
    /// admitted when it was sent and must not be rate limited again.
    pub(crate) fn enqueue_with(
        &mut self,
        mut item: InboxItem,
        now: Instant,
        rated: bool,
    ) -> Result<Accepted, UnitError> {
        if self.state.phase == Phase::Closed {
            return Err(UnitError::RecipientTerminal);
        }
        if item.text.len() > MAX_MESSAGE_BYTES {
            return Err(UnitError::TooLarge {
                max: MAX_MESSAGE_BYTES,
            });
        }
        let state = &mut *self.state;
        if state.items.len() >= MAX_PENDING
            || (item.kind == DeliveryKind::Followup
                && state
                    .items
                    .iter()
                    .filter(|i| i.kind == DeliveryKind::Followup)
                    .count()
                    >= MAX_FOLLOWUPS)
        {
            return Err(UnitError::InboxFull {
                retry_after: INBOX_RETRY_SECS,
            });
        }
        if rated {
            let bucket = state.buckets.entry(item.from).or_insert(Bucket {
                tokens: f64::from(PAIR_BURST),
                last: now,
            });
            bucket.refill(now);
            if bucket.tokens < 1.0 {
                let rate = f64::from(PAIR_BURST) / PAIR_WINDOW.as_secs_f64();
                let wait = ((1.0 - bucket.tokens) / rate).ceil().max(1.0);
                return Err(UnitError::RateLimited {
                    retry_after: wait as u64,
                });
            }
            bucket.tokens -= 1.0;
        }
        state.seq += 1;
        item.seq = state.seq;
        let wakes = item.wake;
        state.items.push_back(item);
        Ok(if wakes && state.phase == Phase::Idle {
            Accepted::Wake
        } else {
            Accepted::Queued
        })
    }

    pub(crate) fn take_start(&mut self) -> Vec<InboxItem> {
        let mut taken = self.drain(|i| i.kind != DeliveryKind::Followup, 0);
        taken.extend(self.drain(|i| i.kind == DeliveryKind::Followup, 1));
        taken
    }

    /// Removes matching items, ordered by (kind rank, seq), until the byte
    /// budget is spent; the oldest go first and the rest stay queued. `max`
    /// of 0 means no count limit.
    fn drain(&mut self, take: impl Fn(&InboxItem) -> bool, max: usize) -> Vec<InboxItem> {
        let mut chosen: Vec<(u8, u64)> = self
            .state
            .items
            .iter()
            .filter(|i| take(i))
            .map(|i| (i.kind.rank(), i.seq))
            .collect();
        chosen.sort_unstable();
        let mut bytes = 0usize;
        let mut keep = Vec::new();
        for (n, key) in chosen.into_iter().enumerate() {
            if max != 0 && n >= max {
                break;
            }
            let size = self
                .state
                .items
                .iter()
                .find(|i| i.seq == key.1)
                .map_or(0, |i| i.text.len());
            if !keep.is_empty() && bytes + size > DRAIN_BYTES {
                break;
            }
            bytes += size;
            keep.push(key);
        }
        let mut out = Vec::with_capacity(keep.len());
        for (_, seq) in &keep {
            if let Some(at) = self.state.items.iter().position(|i| i.seq == *seq) {
                out.extend(self.state.items.remove(at));
            }
        }
        out
    }
}

/// The one frame every drained item gets, whatever the drain point. `step`
/// is the recipient's model-request count when known.
pub fn frame(items: &[InboxItem], step: Option<u32>) -> String {
    let mut out = String::new();
    for (n, item) in items.iter().enumerate() {
        if n > 0 {
            out.push('\n');
        }
        let tag = if item.kind == DeliveryKind::Notice {
            "run-notice"
        } else {
            "run-message"
        };
        let _ = write!(
            out,
            "<{tag} from=\"{}\" id=\"{}\"",
            item.from_label, item.id
        );
        if let Some(step) = step {
            let _ = write!(out, " delivered_step=\"{step}\"");
        }
        let _ = write!(out, ">{}</{tag}>", item.text);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runs::{Clock, ManualClock};

    fn item(n: u64, from: UnitId, kind: DeliveryKind, text: &str, at: Instant) -> InboxItem {
        InboxItem::new(MessageId(n), from, "a".into(), kind, text.into(), at)
    }

    fn put(inbox: &Inbox, n: u64, kind: DeliveryKind, text: &str) -> Result<Accepted, UnitError> {
        let now = Instant::now();
        inbox
            .lock()
            .enqueue(item(n, UnitId::new(), kind, text, now), now)
    }

    #[test]
    fn caps_reject_toolarge_and_full() {
        let inbox = Inbox::default();
        let big = "x".repeat(MAX_MESSAGE_BYTES + 1);
        let err = put(&inbox, 1, DeliveryKind::Steer, &big).unwrap_err();
        assert_eq!(err.to_string(), "TOO_LARGE max=16384");
        assert!(inbox.is_empty());
        put(
            &inbox,
            2,
            DeliveryKind::Steer,
            &"x".repeat(MAX_MESSAGE_BYTES),
        )
        .unwrap();

        // One sender per item, so only the inbox caps are in play.
        for n in 0..(MAX_FOLLOWUPS as u64) {
            put(&inbox, 10 + n, DeliveryKind::Followup, "f").unwrap();
        }
        let err = put(&inbox, 30, DeliveryKind::Followup, "f").unwrap_err();
        assert_eq!(err.to_string(), "INBOX_FULL retry_after=5s");
        for n in 0..(MAX_PENDING - 1 - MAX_FOLLOWUPS) as u64 {
            put(&inbox, 40 + n, DeliveryKind::Quiet, "q").unwrap();
        }
        assert_eq!(inbox.len(), MAX_PENDING);
        assert!(matches!(
            put(&inbox, 99, DeliveryKind::Quiet, "q"),
            Err(UnitError::InboxFull { .. })
        ));
    }

    #[test]
    fn pair_rate_limit_manual_clock() {
        let clock = ManualClock::default();
        let inbox = Inbox::default();
        let from = UnitId::new();
        let send = |n: u64| {
            let now = clock.now();
            inbox
                .lock()
                .enqueue(item(n, from, DeliveryKind::Quiet, "m", now), now)
        };
        for n in 0..u64::from(PAIR_BURST) {
            send(n).unwrap();
        }
        let err = send(100).unwrap_err();
        assert!(matches!(err, UnitError::RateLimited { retry_after: 2 }));
        // Another sender has its own bucket.
        let now = clock.now();
        inbox
            .lock()
            .enqueue(item(101, UnitId::new(), DeliveryKind::Quiet, "m", now), now)
            .unwrap();
        clock.advance(Duration::from_secs(2));
        send(102).unwrap();
        assert!(send(103).is_err());
        clock.advance(PAIR_WINDOW);
        for n in 0..u64::from(PAIR_BURST) {
            send(200 + n).unwrap_or_else(|_| panic!("token {n} should have refilled"));
        }
    }

    #[test]
    fn messages_never_collapsed() {
        let inbox = Inbox::default();
        for n in 0..3 {
            put(&inbox, n, DeliveryKind::Steer, "same text").unwrap();
        }
        let items = inbox.drain_step();
        assert_eq!(items.len(), 3);
        let text = frame(&items, Some(4));
        assert_eq!(text.matches("same text").count(), 3);
        assert!(text.contains(
            "<run-message from=\"a\" id=\"m-0\" delivered_step=\"4\">same text</run-message>"
        ));
    }

    #[test]
    fn drain_orders_steer_notice_quiet_then_seq() {
        let inbox = Inbox::default();
        put(&inbox, 1, DeliveryKind::Quiet, "q").unwrap();
        put(&inbox, 2, DeliveryKind::Notice, "n").unwrap();
        put(&inbox, 3, DeliveryKind::Steer, "s").unwrap();
        put(&inbox, 4, DeliveryKind::Followup, "f").unwrap();
        let ids: Vec<u64> = inbox.drain_step().iter().map(|i| i.id.0).collect();
        assert_eq!(ids, [3, 2, 1]);
        assert_eq!(inbox.len(), 1, "the followup waits for a turn end");
    }

    #[test]
    fn drain_budget_leaves_the_rest_queued_oldest_first() {
        let inbox = Inbox::default();
        let now = Instant::now();
        for n in 0..4 {
            let from = UnitId::new();
            let text = "x".repeat(MAX_MESSAGE_BYTES);
            inbox
                .lock()
                .enqueue(item(n, from, DeliveryKind::Quiet, &text, now), now)
                .unwrap();
        }
        let first = inbox.drain_step();
        assert_eq!(first.iter().map(|i| i.id.0).collect::<Vec<_>>(), [0, 1]);
        let second = inbox.drain_step();
        assert_eq!(second.iter().map(|i| i.id.0).collect::<Vec<_>>(), [2, 3]);
    }

    #[test]
    fn turn_end_continues_on_steer_and_takes_one_followup() {
        let inbox = Inbox::default();
        assert!(matches!(inbox.continue_at_turn_end(), TurnEnd::Done));
        put(&inbox, 1, DeliveryKind::Followup, "a").unwrap();
        put(&inbox, 2, DeliveryKind::Followup, "b").unwrap();
        let TurnEnd::Followup(items) = inbox.continue_at_turn_end() else {
            panic!("a followup starts the next turn");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id.0, 1);
        put(&inbox, 3, DeliveryKind::Steer, "s").unwrap();
        put(&inbox, 4, DeliveryKind::Quiet, "q").unwrap();
        let TurnEnd::Steer(items) = inbox.continue_at_turn_end() else {
            panic!("a steer keeps the run going");
        };
        assert_eq!(items.len(), 2, "the steer and the quiet note land together");
        assert_eq!(inbox.len(), 1, "the other followup stays for its own turn");
    }

    #[test]
    fn settle_sees_what_arrived_before_it_and_idle_wakes_what_comes_after() {
        let inbox = Inbox::default();
        // Arrives while the run is returning: queued, and the settle check
        // finds it under the same lock.
        assert_eq!(
            put(&inbox, 1, DeliveryKind::Steer, "early").unwrap(),
            Accepted::Queued
        );
        assert!(inbox.lock().has_wakeable());
        inbox.take_start();
        inbox.lock().set_phase(Phase::Idle);
        // Arrives after the move to Idle: it asks for a wake.
        assert_eq!(
            put(&inbox, 2, DeliveryKind::Steer, "late").unwrap(),
            Accepted::Wake
        );
        // A quiet note never does.
        assert_eq!(
            put(&inbox, 3, DeliveryKind::Quiet, "q").unwrap(),
            Accepted::Queued
        );
    }

    #[test]
    fn closed_inbox_refuses() {
        let inbox = Inbox::default();
        inbox.close();
        let err = put(&inbox, 1, DeliveryKind::Steer, "x").unwrap_err();
        assert_eq!(err, UnitError::RecipientTerminal);
    }
}
