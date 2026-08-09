//! The shard pool: one OS thread per shard, each owning a `compio` runtime, a
//! listener, and its connection state.
//!
//! Helio's `ProactorPool` exposes a four-way fan-out matrix gated on whether a
//! callback may block a fiber. That distinction does not exist here: every
//! `async fn` is a task and a task that awaits does not stall the loop, so the
//! four methods collapse to [`Shards::spawn_on_all`] and [`Shards::broadcast`].

/// 0-based shard index. `index + 1 = count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
#[display("shard {_0}")]
pub struct ShardId(u16);

impl ShardId {
    #[must_use]
    pub fn index(self) -> u16 {
        self.0
    }
}

/// How many shards to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Count {
    /// One per CPU the process is permitted to run on, so `taskset` and cpuset
    /// limits are respected without a flag.
    PerCore,
    Exactly(std::num::NonZeroU16),
}

/// Whether shard threads are pinned to the CPUs they are placed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    /// Pin shard `i` to the `i`th permitted CPU, wrapping if there are more
    /// shards than CPUs.
    Auto,
    /// Leave placement to the scheduler. [`Shards::affinity`] then reports an
    /// empty set, because the pool claims nothing.
    Off,
}

/// The CPUs a pool has claimed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CpuSet(std::collections::BTreeSet<usize>);

impl CpuSet {
    #[must_use]
    pub fn ids(&self) -> &std::collections::BTreeSet<usize> {
        &self.0
    }

    #[must_use]
    pub fn contains(&self, cpu: usize) -> bool {
        self.0.contains(&cpu)
    }
}

/// Pool configuration. Every value is explicit: there is no `Default`, because
/// each of these changes how the process behaves under load.
#[derive(Debug, Clone)]
pub struct Config {
    pub count: Count,
    pub pin: Affinity,
    pub watchdog: crate::watchdog::Policy,
    /// Upper bound on waiting for open connections to close during shutdown.
    pub drain: std::time::Duration,
}

/// One shard's counters, read on that shard's own thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub shard: ShardId,
    pub connections: crate::listener::Counts,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no CPUs available to place shards on")]
    NoCpus,
    #[error("{count} CPUs is more than a shard index can address")]
    TooManyCpus {
        count: usize,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("spawning the thread for {shard}")]
    SpawnThread {
        shard: ShardId,
        #[source]
        source: std::io::Error,
    },
    #[error("building the compio runtime for {shard}")]
    BuildRuntime {
        shard: ShardId,
        #[source]
        source: std::io::Error,
    },
    #[error("opening the listener for {shard}")]
    Bind {
        shard: ShardId,
        #[source]
        source: crate::listener::Error,
    },
    #[error("{shard} is no longer running")]
    ShardGone { shard: ShardId },
    #[error("{shard} panicked")]
    Panicked { shard: ShardId },
    #[error("collecting results from the shards")]
    Gather(#[from] flume::RecvError),
    #[error("starting the watchdog")]
    Watchdog(#[from] crate::watchdog::Error),
}

/// Work handed to a shard, run on the shard's thread inside its runtime.
type Job = Box<dyn FnOnce(ShardId) + Send + 'static>;

enum Message {
    Run(Job),
    Stop,
}

/// A running shard pool. Dropping it stops and joins every shard; call
/// [`Shards::shutdown`] instead to see whether they stopped cleanly.
#[derive(Debug)]
pub struct Shards {
    shards: Box<[Handle]>,
    cpus: CpuSet,
    watchdog: Option<crate::watchdog::Watchdog>,
}

#[derive(Debug)]
struct Handle {
    shard: ShardId,
    jobs: flume::Sender<Message>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Shards {
    /// Start the pool: bind one `SO_REUSEPORT` listener per shard and begin
    /// accepting. Returns once every shard is listening, or with the first
    /// shard's failure after stopping the ones that did start.
    ///
    /// `make_service` runs on the shard's own thread, so a service may hold
    /// `!Send` state.
    pub fn start<M, S>(
        config: Config,
        listen: crate::listener::Config,
        make_service: M,
    ) -> Result<Self, Error>
    where
        M: Fn(ShardId) -> S + Clone + Send + 'static,
        S: crate::listener::Service,
    {
        let placement = Placement::plan(&config)?;
        let (started, starting) = flume::bounded(placement.shards_count());

        let mut shards: Vec<Handle> = Vec::with_capacity(placement.shards_count());
        let mut monitored: Vec<crate::watchdog::Monitored> =
            Vec::with_capacity(placement.shards_count());

        for index in 0..placement.shards_count() {
            let shard = placement.shard(index)?;
            let beat = std::sync::Arc::new(crate::watchdog::Beat::new());
            let (jobs, taking) = flume::unbounded();
            let boot = Boot {
                shard,
                cpu: placement.cpu(index),
                listen: listen.clone(),
                make_service: make_service.clone(),
                jobs: taking,
                started: started.clone(),
                beat: std::sync::Arc::clone(&beat),
                beat_every: sample_every(&config.watchdog),
                drain: config.drain,
            };
            let thread = std::thread::Builder::new()
                .name(format!("selene-shard-{}", shard.index()))
                .spawn(move || run(boot))
                .map_err(|source| Error::SpawnThread { shard, source })?;

            shards.push(Handle {
                shard,
                jobs,
                thread: Some(thread),
            });
            monitored.push(crate::watchdog::Monitored { shard, beat });
        }
        drop(started);

        let mut pool = Self {
            shards: shards.into_boxed_slice(),
            cpus: placement.claimed,
            watchdog: None,
        };
        pool.await_listening(&starting)?;
        pool.watchdog =
            crate::watchdog::Watchdog::start(monitored.into_boxed_slice(), &config.watchdog)?;
        tracing::info!(shards_count = pool.shards.len(), "shards listening");
        Ok(pool)
    }

    /// The CPUs the shards claimed. The application's own runtime is a
    /// scheduling peer of the shards, so it should either keep its threads off
    /// these or run them at a lower priority.
    #[must_use]
    pub fn affinity(&self) -> &CpuSet {
        &self.cpus
    }

    #[must_use]
    pub fn shards_count(&self) -> usize {
        self.shards.len()
    }

    /// Start a task on every shard and return without waiting for it.
    pub fn spawn_on_all<F, Fut>(&self, task: F) -> Result<(), Error>
    where
        F: Fn(ShardId) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        for handle in &self.shards {
            let task = task.clone();
            let job: Job = Box::new(move |shard| {
                compio::runtime::spawn(task(shard)).detach();
            });
            handle.send(Message::Run(job))?;
        }
        Ok(())
    }

    /// Run a task on every shard and wait for all of them to finish.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn broadcast<F, Fut>(&self, task: F) -> Result<(), Error>
    where
        F: Fn(ShardId) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let done: Vec<()> = self
            .gather(|reply| {
                let task = task.clone();
                Box::new(move |shard| {
                    compio::runtime::spawn(async move {
                        task(shard).await;
                        report(&reply, ());
                    })
                    .detach();
                })
            })
            .await?;
        debug_assert_eq!(done.len(), self.shards.len());
        Ok(())
    }

    /// Gather per-shard counters. Callable from any thread and any runtime:
    /// each shard reads its own thread-locals on its own thread.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn stats(&self) -> Result<Vec<Stats>, Error> {
        self.gather(|reply| {
            Box::new(move |shard| {
                report(
                    &reply,
                    Stats {
                        shard,
                        connections: crate::listener::counts(),
                    },
                );
            })
        })
        .await
    }

    /// Walk every shard's connection registry. This is helio's `Migrate`
    /// traversal without the split atomic counter: each shard walks its own
    /// slab on its own thread.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn clients(&self) -> Result<Vec<crate::listener::Client>, Error> {
        let per_shard: Vec<Vec<crate::listener::Client>> = self
            .gather(|reply| Box::new(move |shard| report(&reply, crate::listener::clients(shard))))
            .await?;
        Ok(per_shard.into_iter().flatten().collect())
    }

    /// Stop accepting, drain open connections, and join every shard thread.
    pub fn shutdown(mut self) -> Result<(), Error> {
        match self.stop() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn await_listening(&self, starting: &flume::Receiver<Result<(), Error>>) -> Result<(), Error> {
        for _ in 0..self.shards.len() {
            match starting.recv() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_disconnected) => {
                    return Err(Error::ShardGone { shard: ShardId(0) });
                }
            }
        }
        Ok(())
    }

    /// One message per shard, one reply per shard. The only fan-out primitive.
    async fn gather<T, F>(&self, make_job: F) -> Result<Vec<T>, Error>
    where
        T: Send + 'static,
        F: Fn(flume::Sender<T>) -> Job,
    {
        let (reply, replies) = flume::bounded(self.shards.len());
        for handle in &self.shards {
            handle.send(Message::Run(make_job(reply.clone())))?;
        }
        drop(reply);

        let mut gathered = Vec::with_capacity(self.shards.len());
        for _ in 0..self.shards.len() {
            gathered.push(replies.recv_async().await?);
        }
        Ok(gathered)
    }

    /// Idempotent: the second call finds every thread handle already taken.
    fn stop(&mut self) -> Option<Error> {
        let mut failure: Option<Error> = None;
        for handle in &mut self.shards {
            let Some(thread) = handle.thread.take() else {
                continue;
            };
            if let Err(error) = handle.send(Message::Stop) {
                tracing::debug!(error = ?error, shard = %handle.shard, "already stopped");
            } else {
                // Told to stop; it will fall out of its job loop.
            }
            if thread.join().is_err() {
                failure.get_or_insert(Error::Panicked {
                    shard: handle.shard,
                });
            } else {
                tracing::debug!(shard = %handle.shard, "shard stopped");
            }
        }
        if let Some(watchdog) = self.watchdog.take() {
            watchdog.stop();
        } else {
            // Never started, or already stopped.
        }
        failure
    }
}

impl Drop for Shards {
    fn drop(&mut self) {
        if let Some(error) = self.stop() {
            tracing::error!(error = ?error, "a shard did not stop cleanly");
        } else {
            // Clean, or already shut down explicitly.
        }
    }
}

impl Handle {
    fn send(&self, message: Message) -> Result<(), Error> {
        match self.jobs.send(message) {
            Ok(()) => Ok(()),
            // `flume::SendError` carries back only the message we just built,
            // so there is no upstream failure to keep as a source.
            Err(flume::SendError(_message)) => Err(Error::ShardGone { shard: self.shard }),
        }
    }
}

/// Send one shard's reply. The channel is sized for exactly one reply per
/// shard, so this never blocks; it only fails if the caller gave up first.
fn report<T>(reply: &flume::Sender<T>, item: T) {
    if let Err(flume::SendError(_item)) = reply.send(item) {
        tracing::debug!("the caller stopped gathering before this shard replied");
    } else {
        // Delivered.
    }
}

/// Which CPU each shard is placed on, decided once before any thread starts.
struct Placement {
    cpus: Box<[usize]>,
    shards_count: usize,
    claimed: CpuSet,
}

impl Placement {
    fn plan(config: &Config) -> Result<Self, Error> {
        let cpus: Box<[usize]> = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|core| core.id)
            .collect();
        if cpus.is_empty() {
            return Err(Error::NoCpus);
        } else {
            // At least one CPU to place shards on.
        }
        u16::try_from(cpus.len()).map_err(|source| Error::TooManyCpus {
            count: cpus.len(),
            source,
        })?;

        let shards_count = match config.count {
            Count::PerCore => cpus.len(),
            Count::Exactly(count) => usize::from(count.get()),
        };
        let claimed = match config.pin {
            Affinity::Auto => CpuSet(cpus.iter().take(shards_count).copied().collect()),
            Affinity::Off => CpuSet::default(),
        };

        Ok(Self {
            cpus,
            shards_count,
            claimed,
        })
    }

    fn shards_count(&self) -> usize {
        self.shards_count
    }

    fn shard(&self, index: usize) -> Result<ShardId, Error> {
        let index = u16::try_from(index).map_err(|source| Error::TooManyCpus {
            count: self.shards_count,
            source,
        })?;
        Ok(ShardId(index))
    }

    /// `None` when pinning is off. Wraps when there are more shards than CPUs.
    fn cpu(&self, index: usize) -> Option<usize> {
        if self.claimed.ids().is_empty() {
            None
        } else {
            self.cpus.get(index % self.cpus.len()).copied()
        }
    }
}

fn sample_every(policy: &crate::watchdog::Policy) -> Option<std::time::Duration> {
    match policy {
        crate::watchdog::Policy::Off => None,
        crate::watchdog::Policy::On { sample_every, .. } => Some(*sample_every),
    }
}

/// Everything one shard thread needs to come up, in one value.
struct Boot<M> {
    shard: ShardId,
    cpu: Option<usize>,
    listen: crate::listener::Config,
    make_service: M,
    jobs: flume::Receiver<Message>,
    started: flume::Sender<Result<(), Error>>,
    beat: std::sync::Arc<crate::watchdog::Beat>,
    beat_every: Option<std::time::Duration>,
    drain: std::time::Duration,
}

fn run<M, S>(boot: Boot<M>)
where
    M: FnOnce(ShardId) -> S,
    S: crate::listener::Service,
{
    let Boot {
        shard,
        cpu,
        listen,
        make_service,
        jobs,
        started,
        beat,
        beat_every,
        drain,
    } = boot;

    let runtime = match build_runtime(cpu) {
        Ok(runtime) => runtime,
        Err(source) => {
            report(&started, Err(Error::BuildRuntime { shard, source }));
            return;
        }
    };

    runtime.block_on(async move {
        let listener = match crate::listener::bind(&listen) {
            Ok(listener) => listener,
            Err(source) => {
                report(&started, Err(Error::Bind { shard, source }));
                return;
            }
        };
        report(&started, Ok(()));
        drop(started);

        let service = std::rc::Rc::new(make_service(shard));
        let accepting = compio::runtime::spawn(crate::listener::accept(
            listener,
            std::rc::Rc::clone(&service),
            listen,
        ));
        let beating =
            beat_every.map(|every| compio::runtime::spawn(crate::watchdog::beat(beat, every)));

        while let Ok(Message::Run(job)) = jobs.recv_async().await {
            job(shard);
        }

        // Cancelling issues a real cancellation to the driver. Nothing here
        // drops an in-flight operation, which on io_uring would be a
        // use-after-free in kernel space.
        if let Some(Err(error)) = accepting.cancel().await {
            tracing::debug!(error = ?error, %shard, "accept loop ended with an error");
        } else {
            // Cancelled before it failed, which is the normal path.
        }
        if let Some(beating) = beating {
            beating.cancel().await;
        } else {
            // The watchdog was off.
        }

        service.on_shutdown();
        let remaining_count = crate::listener::drain(drain).await;
        if remaining_count > 0 {
            tracing::warn!(%shard, remaining_count, "drain deadline passed with connections open");
        } else {
            tracing::debug!(%shard, "drained");
        }
    });
}

fn build_runtime(cpu: Option<usize>) -> Result<compio::runtime::Runtime, std::io::Error> {
    let mut builder = compio::runtime::Runtime::builder();
    if let Some(cpu) = cpu {
        builder.thread_affinity(std::collections::HashSet::from([cpu]));
    } else {
        // Placement left to the scheduler.
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    fn config(count: super::Count, pin: super::Affinity) -> super::Config {
        super::Config {
            count,
            pin,
            watchdog: crate::watchdog::Policy::Off,
            drain: std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn per_core_claims_one_cpu_per_shard() {
        let placement =
            super::Placement::plan(&config(super::Count::PerCore, super::Affinity::Auto))
                .expect("this machine has CPUs");
        pretty_assertions::assert_eq!(placement.claimed.ids().len(), placement.shards_count());
    }

    #[test]
    fn pinning_off_claims_nothing() {
        let placement =
            super::Placement::plan(&config(super::Count::PerCore, super::Affinity::Off))
                .expect("this machine has CPUs");
        assert!(placement.claimed.ids().is_empty());
        pretty_assertions::assert_eq!(placement.cpu(0), None);
    }

    #[test]
    fn more_shards_than_cpus_wrap_around_the_cpu_list() {
        let many = std::num::NonZeroU16::new(512).expect("literal");
        let placement =
            super::Placement::plan(&config(super::Count::Exactly(many), super::Affinity::Auto))
                .expect("this machine has CPUs");
        pretty_assertions::assert_eq!(placement.shards_count(), 512);
        pretty_assertions::assert_eq!(placement.cpu(0), placement.cpu(placement.cpus.len()));
    }

    #[test]
    fn shard_ids_are_zero_based() {
        let placement =
            super::Placement::plan(&config(super::Count::PerCore, super::Affinity::Auto))
                .expect("this machine has CPUs");
        pretty_assertions::assert_eq!(placement.shard(0).expect("in range").index(), 0);
    }
}
