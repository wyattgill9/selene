//! Background priority, rebuilt above the runtime instead of inside it.
//!
//! Helio's scheduler gives `BACKGROUND` fibers a warrant over total observed
//! runtime and sleeps them when foreground work is active. Compio schedules
//! FIFO with no fairness, but Rust's `.await` points are explicit, so the same
//! policy fits in a guard that background work awaits:
//!
//! ```no_run
//! # async fn example(policy: selene::budget::Policy) {
//! # fn compact_a_chunk() {}
//! let mut budget = selene::budget::Budget::start(policy);
//! loop {
//!     compact_a_chunk();
//!     budget.tick().await;
//! }
//! # }
//! ```
//!
//! The yield points are visible in the source rather than implicit at every
//! fiber suspension, and there is no runtime to fork.

/// Background scheduling policy for one shard.
///
/// Helio hard-codes a 10% warrant tuned for one workload. Selene takes it as
/// configuration because the right share is a property of the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Share of the shard's total observed runtime background work may consume.
    pub warrant_percent: std::num::NonZeroU8,
    /// Foreground work seen more recently than this means the shard sleeps the
    /// background task rather than merely yielding to it.
    pub foreground_idle: std::time::Duration,
    /// Upper bound on one background sleep.
    pub sleep_max: std::time::Duration,
}

/// Per-shard, thread-local background accounting. One per background task.
#[derive(Debug)]
pub struct Budget {
    policy: Policy,
    started: std::time::Instant,
    chunk_started: std::time::Instant,
    background: std::time::Duration,
}

/// Record that foreground work just ran on this shard.
///
/// The listener marks each connection at accept and at close. A service holding
/// long-lived connections should mark each request it serves, otherwise the
/// budget sees a busy shard as idle and stops sleeping background work.
pub fn mark_foreground() {
    FOREGROUND_AT.set(Some(std::time::Instant::now()));
}

impl Budget {
    #[must_use]
    pub fn start(policy: Policy) -> Self {
        let now = std::time::Instant::now();
        Self {
            policy,
            started: now,
            chunk_started: now,
            background: std::time::Duration::ZERO,
        }
    }

    /// Close out the chunk of background work that ran since the last tick and
    /// apply the warrant.
    ///
    /// Under warrant this returns without yielding at all, which is the same
    /// optimisation helio makes in `Preempt`'s `BACKGROUND` branch.
    ///
    /// ponytail: timed with `Instant` (two `clock_gettime` calls per tick).
    /// Swap in `quanta` if a tick-per-microsecond workload shows the cost.
    pub async fn tick(&mut self) {
        let now = std::time::Instant::now();
        let chunk = now.duration_since(self.chunk_started);
        self.background += chunk;

        let observed = Observed {
            background: self.background,
            total: now.duration_since(self.started),
            chunk,
            foreground_idle: FOREGROUND_AT.get().map(|at| now.duration_since(at)),
        };

        match decide(&self.policy, &observed) {
            Decision::Continue => {}
            Decision::Yield => yield_now().await,
            Decision::Sleep(nap) => compio::runtime::time::sleep(nap).await,
        }

        self.chunk_started = std::time::Instant::now();
    }

    /// Background time this budget has accounted for so far.
    #[must_use]
    pub fn background(&self) -> std::time::Duration {
        self.background
    }
}

thread_local! {
    static FOREGROUND_AT: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// What the shard has seen at one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observed {
    background: std::time::Duration,
    total: std::time::Duration,
    chunk: std::time::Duration,
    foreground_idle: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Continue,
    Yield,
    Sleep(std::time::Duration),
}

fn decide(policy: &Policy, observed: &Observed) -> Decision {
    let spent = observed.background.as_nanos() * 100;
    let warranted = observed.total.as_nanos() * u128::from(policy.warrant_percent.get());
    if spent <= warranted {
        return Decision::Continue;
    } else {
        // Over warrant: the shard owes the foreground.
    }

    let foreground_recent = match observed.foreground_idle {
        Some(idle) => idle <= policy.foreground_idle,
        None => false,
    };
    if foreground_recent {
        let nap = observed.chunk.min(policy.sleep_max);
        if nap.is_zero() {
            Decision::Yield
        } else {
            Decision::Sleep(nap)
        }
    } else {
        Decision::Yield
    }
}

/// Hand the executor one turn. Compio has no `yield_now` of its own.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    fn policy() -> super::Policy {
        super::Policy {
            warrant_percent: std::num::NonZeroU8::new(10).expect("literal"),
            foreground_idle: std::time::Duration::from_millis(5),
            sleep_max: std::time::Duration::from_micros(1500),
        }
    }

    fn observed(background_ms: u64, total_ms: u64) -> super::Observed {
        super::Observed {
            background: std::time::Duration::from_millis(background_ms),
            total: std::time::Duration::from_millis(total_ms),
            chunk: std::time::Duration::from_micros(50),
            foreground_idle: None,
        }
    }

    #[test]
    fn under_warrant_never_yields() {
        let decision = super::decide(&policy(), &observed(9, 100));
        pretty_assertions::assert_eq!(decision, super::Decision::Continue);
    }

    #[test]
    fn exactly_at_warrant_still_runs() {
        let decision = super::decide(&policy(), &observed(10, 100));
        pretty_assertions::assert_eq!(decision, super::Decision::Continue);
    }

    #[test]
    fn over_warrant_with_idle_foreground_yields() {
        let decision = super::decide(&policy(), &observed(11, 100));
        pretty_assertions::assert_eq!(decision, super::Decision::Yield);
    }

    #[test]
    fn over_warrant_with_active_foreground_sleeps_the_chunk() {
        let mut over = observed(50, 100);
        over.foreground_idle = Some(std::time::Duration::from_millis(1));
        let decision = super::decide(&policy(), &over);
        pretty_assertions::assert_eq!(
            decision,
            super::Decision::Sleep(std::time::Duration::from_micros(50))
        );
    }

    #[test]
    fn a_long_chunk_sleeps_no_longer_than_sleep_max() {
        let mut over = observed(50, 100);
        over.chunk = std::time::Duration::from_millis(40);
        over.foreground_idle = Some(std::time::Duration::ZERO);
        let decision = super::decide(&policy(), &over);
        pretty_assertions::assert_eq!(
            decision,
            super::Decision::Sleep(std::time::Duration::from_micros(1500))
        );
    }

    #[test]
    fn stale_foreground_yields_instead_of_sleeping() {
        let mut over = observed(50, 100);
        over.foreground_idle = Some(std::time::Duration::from_millis(50));
        let decision = super::decide(&policy(), &over);
        pretty_assertions::assert_eq!(decision, super::Decision::Yield);
    }
}
