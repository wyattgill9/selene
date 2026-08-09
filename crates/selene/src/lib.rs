//! Selene is a shard-per-core backend framework: one OS thread per shard, each
//! owning a `compio` runtime, its own `SO_REUSEPORT` listener, and its own
//! `!Send` connection state.
//!
//! Selene owns the data plane only. Anything that can run late without a client
//! noticing (metrics scrapes, snapshot upload, DNS) belongs on a runtime the
//! application builds and owns; [`shard::Shards::stats`] and
//! [`shard::Shards::affinity`] are the whole seam across that boundary.
//!
//! ```no_run
//! let listen = selene::listener::Config {
//!     addr: "0.0.0.0:6379".parse().expect("literal address"),
//!     backlog: std::num::NonZeroU32::new(1024).expect("literal"),
//!     nodelay: true,
//!     max_connections: selene::listener::MaxConnections::Unlimited,
//! };
//! let config = selene::shard::Config {
//!     count: selene::shard::Count::PerCore,
//!     pin: selene::shard::Affinity::Auto,
//!     watchdog: selene::watchdog::Policy::Off,
//!     drain: std::time::Duration::from_secs(5),
//! };
//! # struct Echo;
//! # impl selene::listener::Service for Echo {
//! #     fn serve(&self, _c: compio::net::TcpStream, _p: std::net::SocketAddr)
//! #         -> impl std::future::Future<Output = std::io::Result<()>> { async { Ok(()) } }
//! # }
//! let shards = selene::shard::Shards::start(config, listen, |_shard| Echo)?;
//! shards.shutdown()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

// Futures are stackless state machines, so none of helio's placement-new fiber
// stack machinery has an analogue here. There is nothing left to write unsafe.
#![deny(unsafe_code)]

pub mod shard;

pub mod budget;
pub mod listener;
pub mod watchdog;
