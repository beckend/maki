use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use event_listener::Event;

pub type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    /// Resolves once `now()` reaches `deadline`.
    fn sleep_until(&self, deadline: Instant) -> Sleep<'_>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Box::pin(async move {
            smol::Timer::at(deadline).await;
        })
    }
}

/// Moves only when told to, so time-dependent tests never sleep.
pub struct ManualClock {
    now: Mutex<Instant>,
    advanced: Event,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self {
            now: Mutex::new(Instant::now()),
            advanced: Event::new(),
        }
    }
}

impl ManualClock {
    pub fn advance(&self, by: Duration) {
        *self.now.lock().unwrap() += by;
        self.advanced.notify(usize::MAX);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Box::pin(async move {
            loop {
                let listener = self.advanced.listen();
                if self.now() >= deadline {
                    return;
                }
                listener.await;
            }
        })
    }
}
