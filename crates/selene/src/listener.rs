//! The per-shard listener: every shard opens its own `SO_REUSEPORT` socket, so
//! the kernel distributes connections and each one is born on the shard that
//! will serve it. Nothing migrates, so there is no migration protocol.
//!
//! The connection registry is a thread-local `Slab` each shard walks on its own
//! thread. Traversal (`clients`) and counters (`counts`) are reached by sending
//! a job to the shard, never by sharing atomics.

use compio::io::AsyncWriteExt as _;

/// What a shard does with an accepted connection.
///
/// One instance per shard, built on the shard thread, so implementations are
/// free to hold `!Send` state (`RefCell`, `Rc`) with no locking.
pub trait Service: 'static {
    /// Serve one connection to completion. Returning ends the connection.
    fn serve(
        &self,
        conn: compio::net::TcpStream,
        peer: std::net::SocketAddr,
    ) -> impl std::future::Future<Output = std::io::Result<()>>;

    /// Runs on the shard thread once the shard has stopped accepting and before
    /// open connections are drained.
    fn on_shutdown(&self) {}
}

/// Listening socket configuration. Every shard binds this same address.
#[derive(Debug, Clone)]
pub struct Config {
    /// The port must be concrete: under `SO_REUSEPORT` a zero port would hand
    /// every shard a *different* kernel-assigned port instead of one listener
    /// group, so [`bind`] rejects it.
    pub addr: std::net::SocketAddr,
    pub backlog: std::num::NonZeroU32,
    /// Applied to each accepted connection rather than the listener, because
    /// listener inheritance of `TCP_NODELAY` is not portable.
    pub nodelay: bool,
    pub max_connections: MaxConnections,
}

/// What a shard does once it is already holding its share of connections.
///
/// The limit is per shard, not per process: with `n` shards the process admits
/// `n` times this count.
#[derive(Debug, Clone, Copy)]
pub enum MaxConnections {
    Unlimited,
    /// Close the socket without writing anything.
    Close(std::num::NonZeroUsize),
    /// Write `message`, then close.
    Reply {
        count: std::num::NonZeroUsize,
        message: &'static [u8],
    },
}

/// Per-shard connection counters, read on the shard's own thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub open_count: usize,
    pub accepted_count: u64,
    pub rejected_count: u64,
}

/// One entry of a shard's connection registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Client {
    pub shard: crate::shard::ShardId,
    pub peer: std::net::SocketAddr,
    pub connected_for: std::time::Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("listening address {addr} must name a concrete port under SO_REUSEPORT")]
    EphemeralPort { addr: std::net::SocketAddr },
    #[error("creating the listening socket")]
    CreateSocket(#[source] std::io::Error),
    #[error("enabling SO_REUSEADDR")]
    SetReuseAddress(#[source] std::io::Error),
    #[error("enabling SO_REUSEPORT")]
    SetReusePort(#[source] std::io::Error),
    #[error("this platform has no SO_REUSEPORT, so shards cannot share a port")]
    ReusePortUnsupported,
    #[error("backlog {backlog} does not fit the platform's listen(2) argument")]
    Backlog {
        backlog: std::num::NonZeroU32,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("binding {addr}")]
    Bind {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("listening on {addr}")]
    Listen {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("registering the listener with the shard runtime")]
    Register(#[source] std::io::Error),
}

/// Consecutive `accept` failures a shard tolerates before giving the listener
/// up. A listener that cannot accept is a broken invariant, not a transient
/// peer fault, so the shard surfaces it instead of spinning.
const ACCEPT_ERRORS_MAX: u32 = 16;

/// How often [`drain`] rechecks the registry while waiting for connections to
/// close.
const DRAIN_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// Open one shard's listener. Must be called on the shard thread, inside its
/// runtime, because the listener registers with that thread's proactor.
pub(crate) fn bind(config: &Config) -> Result<compio::net::TcpListener, Error> {
    if config.addr.port() == 0 {
        return Err(Error::EphemeralPort { addr: config.addr });
    } else {
        // Fall through: the port is concrete.
    }

    let backlog = i32::try_from(config.backlog.get()).map_err(|source| Error::Backlog {
        backlog: config.backlog,
        source,
    })?;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(config.addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .map_err(Error::CreateSocket)?;

    socket
        .set_reuse_address(true)
        .map_err(Error::SetReuseAddress)?;
    set_reuse_port(&socket)?;
    socket
        .bind(&config.addr.into())
        .map_err(|source| Error::Bind {
            addr: config.addr,
            source,
        })?;
    socket.listen(backlog).map_err(|source| Error::Listen {
        addr: config.addr,
        source,
    })?;

    compio::net::TcpListener::from_std(socket.into()).map_err(Error::Register)
}

#[cfg(unix)]
fn set_reuse_port(socket: &socket2::Socket) -> Result<(), Error> {
    socket.set_reuse_port(true).map_err(Error::SetReusePort)
}

#[cfg(not(unix))]
fn set_reuse_port(_socket: &socket2::Socket) -> Result<(), Error> {
    Err(Error::ReusePortUnsupported)
}

/// Accept connections until the listener breaks or the task is cancelled.
///
/// The timeout rule from the design applies here: nothing races this future
/// with `select!`. The shard stops it with `JoinHandle::cancel`, which issues a
/// real cancellation to the driver rather than dropping an in-flight operation.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn accept<S: Service>(
    listener: compio::net::TcpListener,
    service: std::rc::Rc<S>,
    config: Config,
) -> std::io::Result<()> {
    let mut errors_count: u32 = 0;
    loop {
        match listener.accept().await {
            Ok((conn, peer)) => {
                errors_count = 0;
                admit(Accepted { conn, peer }, &service, &config);
            }
            Err(error) => {
                errors_count += 1;
                tracing::warn!(error = ?error, errors_count, "accept failed");
                if errors_count >= ACCEPT_ERRORS_MAX {
                    return Err(error);
                } else {
                    // Under the bound: treat it as a transient peer fault.
                }
            }
        }
    }
}

/// A connection the kernel has handed to this shard, before any policy applies.
struct Accepted {
    conn: compio::net::TcpStream,
    peer: std::net::SocketAddr,
}

/// What the shard does with one [`Accepted`] connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Serve,
    Close,
    Reply(&'static [u8]),
}

fn decide(max_connections: &MaxConnections, open_count: usize) -> Admission {
    match max_connections {
        MaxConnections::Unlimited => Admission::Serve,
        MaxConnections::Close(count) => {
            if open_count >= count.get() {
                Admission::Close
            } else {
                Admission::Serve
            }
        }
        MaxConnections::Reply { count, message } => {
            if open_count >= count.get() {
                Admission::Reply(message)
            } else {
                Admission::Serve
            }
        }
    }
}

fn admit<S: Service>(accepted: Accepted, service: &std::rc::Rc<S>, config: &Config) {
    let Accepted { conn, peer } = accepted;
    let admission = REGISTRY
        .with_borrow(|registry| decide(&config.max_connections, registry.connections.len()));

    match admission {
        Admission::Serve => {
            crate::budget::mark_foreground();
            let service = std::rc::Rc::clone(service);
            let nodelay = config.nodelay;
            compio::runtime::spawn(serve(Accepted { conn, peer }, service, nodelay)).detach();
        }
        Admission::Close => {
            REGISTRY.with_borrow_mut(|registry| registry.rejected_count += 1);
            tracing::debug!(%peer, "connection rejected: shard at its connection limit");
        }
        Admission::Reply(message) => {
            REGISTRY.with_borrow_mut(|registry| registry.rejected_count += 1);
            tracing::debug!(%peer, "connection rejected with a message");
            compio::runtime::spawn(reject(conn, message)).detach();
        }
    }
}

#[tracing::instrument(level = "debug", skip_all)]
async fn serve<S: Service>(
    accepted: Accepted,
    service: std::rc::Rc<S>,
    nodelay: bool,
) -> std::io::Result<()> {
    let Accepted { conn, peer } = accepted;
    // `OnConnectionStart` / `OnConnectionClose` are this guard's construction
    // and `Drop`, so an early return cannot leak a registry slot.
    let _registered = Registered::open(peer);

    if let Err(error) = conn.set_nodelay(nodelay) {
        tracing::warn!(error = ?error, %peer, "setting TCP_NODELAY");
    } else {
        // Applied.
    }

    let result = service.serve(conn, peer).await;
    crate::budget::mark_foreground();
    if let Err(error) = &result {
        tracing::debug!(error = ?error, %peer, "connection ended with an error");
    } else {
        tracing::debug!(%peer, "connection closed");
    }
    result
}

async fn reject(mut conn: compio::net::TcpStream, message: &'static [u8]) {
    let compio::BufResult(result, _message) = conn.write_all(message).await;
    if let Err(error) = result {
        tracing::debug!(error = ?error, "writing the overload reply");
    } else {
        // Written; dropping `conn` closes it.
    }
}

/// The shard's connection registry. Thread-local by construction: no shard ever
/// reads another shard's registry, which is what deletes helio's split
/// migration/traversal counter.
struct Registry {
    connections: slab::Slab<Conn>,
    accepted_count: u64,
    rejected_count: u64,
}

struct Conn {
    peer: std::net::SocketAddr,
    since: std::time::Instant,
}

thread_local! {
    static REGISTRY: std::cell::RefCell<Registry> = const {
        std::cell::RefCell::new(Registry {
            connections: slab::Slab::new(),
            accepted_count: 0,
            rejected_count: 0,
        })
    };
}

/// RAII registration of one live connection.
struct Registered {
    key: usize,
}

impl Registered {
    fn open(peer: std::net::SocketAddr) -> Self {
        let key = REGISTRY.with_borrow_mut(|registry| {
            registry.accepted_count += 1;
            registry.connections.insert(Conn {
                peer,
                since: std::time::Instant::now(),
            })
        });
        Self { key }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        REGISTRY.with_borrow_mut(|registry| {
            registry.connections.remove(self.key);
        });
    }
}

pub(crate) fn counts() -> Counts {
    REGISTRY.with_borrow(|registry| Counts {
        open_count: registry.connections.len(),
        accepted_count: registry.accepted_count,
        rejected_count: registry.rejected_count,
    })
}

pub(crate) fn clients(shard: crate::shard::ShardId) -> Vec<Client> {
    REGISTRY.with_borrow(|registry| {
        registry
            .connections
            .iter()
            .map(|(_key, conn)| Client {
                shard,
                peer: conn.peer,
                connected_for: conn.since.elapsed(),
            })
            .collect()
    })
}

/// Wait for open connections to close, giving up after `within`. Returns the
/// number still open, which is zero on a clean drain.
///
/// ponytail: polls the registry instead of cancelling each connection, so a
/// peer that never sends holds its slot until the deadline. Give `Registered` a
/// `CancelToken` when a stuck client actually delays a shutdown.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn drain(within: std::time::Duration) -> usize {
    let deadline = std::time::Instant::now() + within;
    loop {
        let open_count = REGISTRY.with_borrow(|registry| registry.connections.len());
        if open_count == 0 {
            return 0;
        } else if std::time::Instant::now() >= deadline {
            return open_count;
        } else {
            compio::runtime::time::sleep(DRAIN_POLL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn unlimited_always_serves() {
        let decision = super::decide(&super::MaxConnections::Unlimited, usize::MAX);
        pretty_assertions::assert_eq!(decision, super::Admission::Serve);
    }

    #[test]
    fn close_applies_at_the_limit_not_after_it() {
        let two = std::num::NonZeroUsize::new(2).expect("literal");
        let max_connections = super::MaxConnections::Close(two);
        pretty_assertions::assert_eq!(super::decide(&max_connections, 1), super::Admission::Serve);
        pretty_assertions::assert_eq!(super::decide(&max_connections, 2), super::Admission::Close);
    }

    #[test]
    fn reply_carries_the_message_once_full() {
        let one = std::num::NonZeroUsize::new(1).expect("literal");
        let max_connections = super::MaxConnections::Reply {
            count: one,
            message: b"-ERR max number of clients reached\r\n",
        };
        pretty_assertions::assert_eq!(super::decide(&max_connections, 0), super::Admission::Serve);
        pretty_assertions::assert_eq!(
            super::decide(&max_connections, 1),
            super::Admission::Reply(b"-ERR max number of clients reached\r\n")
        );
    }
}
