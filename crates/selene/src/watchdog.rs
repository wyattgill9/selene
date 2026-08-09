//! Stall detection.
//!
//! Head-of-line blocking is the primary failure mode of shard-per-core, and
//! compio has no detector. Each shard ticks a counter from a timer task; a
//! watchdog thread samples those counters and reports a shard whose loop has
//! stopped turning. There are no fiber stacks to decode, so this replaces both
//! helio's in-process dumper and the 535-line gdb frame decoder.

/// Whether shards are watched, and how closely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Off,
    On {
        /// A shard silent for this long is reported as stalled.
        stall_after: std::time::Duration,
        /// How often the shard ticks and the watchdog samples.
        sample_every: std::time::Duration,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sample_every ({sample_every:?}) must be shorter than stall_after ({stall_after:?})")]
    SampleTooCoarse {
        stall_after: std::time::Duration,
        sample_every: std::time::Duration,
    },
    #[error("spawning the watchdog thread")]
    SpawnThread(#[source] std::io::Error),
}

/// A shard's liveness counter. Monotonic; only its movement is meaningful, so
/// there is no clock to convert and nothing to overflow.
#[derive(Debug)]
pub(crate) struct Beat(std::sync::atomic::AtomicU64);

impl Beat {
    pub(crate) fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(0))
    }

    fn tick(&self) {
        // `fetch_add` wraps, and only movement is meaningful, so wrapping is
        // not a lost value.
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn read(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One shard as seen by the watchdog thread.
#[derive(Debug)]
pub(crate) struct Monitored {
    pub(crate) shard: crate::shard::ShardId,
    pub(crate) beat: std::sync::Arc<Beat>,
}

/// The shard-side half: tick until the task is cancelled. A shard stuck in a
/// long-running task never reaches this, which is exactly the signal.
pub(crate) async fn beat(beat: std::sync::Arc<Beat>, every: std::time::Duration) {
    loop {
        compio::runtime::time::sleep(every).await;
        beat.tick();
    }
}

#[derive(Debug)]
pub(crate) struct Watchdog {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

impl Watchdog {
    /// Start watching, or return `None` when the policy is [`Policy::Off`].
    pub(crate) fn start(
        monitored: Box<[Monitored]>,
        policy: &Policy,
    ) -> Result<Option<Self>, Error> {
        let Policy::On {
            stall_after,
            sample_every,
        } = *policy
        else {
            return Ok(None);
        };

        let samples_max = stall_samples(stall_after, sample_every)?;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watching = std::sync::Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("selene-watchdog".to_owned())
            .spawn(move || watch(&monitored, sample_every, samples_max, &watching))
            .map_err(Error::SpawnThread)?;

        Ok(Some(Self { stop, thread }))
    }

    /// Stop watching and join. Takes up to one `sample_every`.
    pub(crate) fn stop(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if self.thread.join().is_err() {
            tracing::error!("the watchdog thread panicked");
        } else {
            tracing::debug!("watchdog stopped");
        }
    }
}

fn stall_samples(
    stall_after: std::time::Duration,
    sample_every: std::time::Duration,
) -> Result<u32, Error> {
    let too_coarse = Error::SampleTooCoarse {
        stall_after,
        sample_every,
    };
    if sample_every.is_zero() || sample_every >= stall_after {
        return Err(too_coarse);
    } else {
        // At least two samples fit inside the stall window.
    }

    let samples = stall_after.as_nanos() / sample_every.as_nanos();
    u32::try_from(samples).map_err(|_source| too_coarse)
}

/// One shard's sampling state, held by the watchdog thread alone.
struct Seen {
    beats_count: u64,
    missed_count: u32,
    reported: bool,
}

fn watch(
    monitored: &[Monitored],
    sample_every: std::time::Duration,
    samples_max: u32,
    stop: &std::sync::atomic::AtomicBool,
) {
    let mut seen: Box<[Seen]> = monitored
        .iter()
        .map(|shard| Seen {
            beats_count: shard.beat.read(),
            missed_count: 0,
            reported: false,
        })
        .collect();

    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(sample_every);
        for (shard, seen) in monitored.iter().zip(seen.iter_mut()) {
            sample(shard, seen, samples_max);
        }
    }
}

/// ponytail: reports the stall, does not capture the stalled thread's stack.
/// Doing that needs a signal handler, and this crate denies unsafe. Add a
/// feature-gated `pprof` dump when a stall report actually needs a culprit.
fn sample(monitored: &Monitored, seen: &mut Seen, samples_max: u32) {
    let beats_count = monitored.beat.read();
    if beats_count != seen.beats_count {
        if seen.reported {
            tracing::info!(shard = %monitored.shard, "shard recovered");
        } else {
            // Was never reported stalled; nothing to say.
        }
        seen.beats_count = beats_count;
        seen.missed_count = 0;
        seen.reported = false;
        return;
    } else {
        // The shard has not ticked since the last sample.
    }

    seen.missed_count = seen.missed_count.saturating_add(1);
    if seen.missed_count >= samples_max && !seen.reported {
        seen.reported = true;
        tracing::error!(
            shard = %monitored.shard,
            missed_count = seen.missed_count,
            "shard stalled: its event loop has not turned",
        );
    } else {
        // Still inside the stall window, or already reported.
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_stall_window_holding_four_samples_reports_after_four() {
        let samples = super::stall_samples(
            std::time::Duration::from_millis(200),
            std::time::Duration::from_millis(50),
        );
        pretty_assertions::assert_eq!(samples.expect("valid policy"), 4);
    }

    #[test]
    fn sampling_slower_than_the_stall_window_is_rejected() {
        let samples = super::stall_samples(
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(50),
        );
        assert!(samples.is_err());
    }

    #[test]
    fn a_zero_sample_interval_is_rejected() {
        let samples = super::stall_samples(
            std::time::Duration::from_millis(50),
            std::time::Duration::ZERO,
        );
        assert!(samples.is_err());
    }
}
