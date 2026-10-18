use std::sync::Mutex;
use std::time::{Duration, Instant};

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Moves only when told to, so time-dependent tests never sleep.
pub struct ManualClock(Mutex<Instant>);

impl Default for ManualClock {
    fn default() -> Self {
        Self(Mutex::new(Instant::now()))
    }
}

impl ManualClock {
    pub fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }
}
