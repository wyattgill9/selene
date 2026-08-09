//! End-to-end tests against a real shard pool: real threads, real
//! `SO_REUSEPORT` listeners, real sockets.

use compio::io::AsyncRead as _;
use compio::io::AsyncWriteExt as _;
use std::io::Read as _;
use std::io::Write as _;

const SHARDS_COUNT: u16 = 2;
const CLIENTS_COUNT: usize = 8;

struct Echo;

impl selene::listener::Service for Echo {
    async fn serve(
        &self,
        mut conn: compio::net::TcpStream,
        _peer: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        let mut buffer = Vec::with_capacity(64);
        loop {
            let compio::BufResult(read, returned) = conn.read(buffer).await;
            buffer = returned;
            if read? == 0 {
                return Ok(());
            } else {
                // Something to echo.
            }
            let compio::BufResult(written, returned) = conn.write_all(buffer).await;
            buffer = returned;
            written?;
            buffer.clear();
        }
    }
}

/// A port nobody is listening on yet. Racy in principle, which is why each test
/// takes a fresh one rather than sharing a constant.
fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("binding an ephemeral port");
    probe.local_addr().expect("the probe is bound").port()
}

fn config() -> selene::shard::Config {
    selene::shard::Config {
        count: selene::shard::Count::Exactly(
            std::num::NonZeroU16::new(SHARDS_COUNT).expect("literal"),
        ),
        // Off, so the test does not fight the CI runner's scheduler.
        pin: selene::shard::Affinity::Off,
        watchdog: selene::watchdog::Policy::On {
            stall_after: std::time::Duration::from_millis(500),
            sample_every: std::time::Duration::from_millis(50),
        },
        drain: std::time::Duration::from_secs(2),
    }
}

fn listen(port: u16) -> selene::listener::Config {
    selene::listener::Config {
        addr: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        backlog: std::num::NonZeroU32::new(128).expect("literal"),
        nodelay: true,
        max_connections: selene::listener::MaxConnections::Unlimited,
    }
}

fn round_trip(addr: std::net::SocketAddr, message: &[u8]) -> std::net::TcpStream {
    let mut conn = std::net::TcpStream::connect(addr).expect("connecting to the shards");
    conn.write_all(message).expect("writing the request");
    let mut echoed = vec![0u8; message.len()];
    conn.read_exact(&mut echoed).expect("reading the echo");
    pretty_assertions::assert_eq!(echoed, message);
    conn
}

/// The seam is `async`, so the caller needs a runtime. An application would use
/// its tokio runtime here; the test uses the smallest thing that polls.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    compio::runtime::Runtime::new()
        .expect("building a control-plane runtime")
        .block_on(future)
}

#[test]
fn every_shard_accepts_on_the_same_port_and_echoes() {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let shards = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("starting the shards");
    pretty_assertions::assert_eq!(shards.shards_count(), usize::from(SHARDS_COUNT));

    let held: Vec<std::net::TcpStream> = (0..CLIENTS_COUNT)
        .map(|index| round_trip(addr, format!("hello {index}\n").as_bytes()))
        .collect();

    let stats = block_on(shards.stats()).expect("gathering stats");
    let open_count: usize = stats.iter().map(|shard| shard.connections.open_count).sum();
    let accepted_count: u64 = stats
        .iter()
        .map(|shard| shard.connections.accepted_count)
        .sum();
    pretty_assertions::assert_eq!(stats.len(), usize::from(SHARDS_COUNT));
    pretty_assertions::assert_eq!(open_count, CLIENTS_COUNT);
    pretty_assertions::assert_eq!(
        accepted_count,
        u64::try_from(CLIENTS_COUNT).expect("literal")
    );

    let clients = block_on(shards.clients()).expect("walking the registries");
    pretty_assertions::assert_eq!(clients.len(), CLIENTS_COUNT);

    drop(held);
    shards.shutdown().expect("shutting down cleanly");
}

#[test]
fn closed_connections_leave_the_registry() {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let shards = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("starting the shards");

    drop(round_trip(addr, b"one\n"));
    let empty = block_on(async {
        // The shard notices the close on its own thread, so give it a moment.
        for _ in 0..100u32 {
            compio::runtime::time::sleep(std::time::Duration::from_millis(10)).await;
            let clients = shards.clients().await.expect("walking the registries");
            if clients.is_empty() {
                return true;
            } else {
                // Still registered.
            }
        }
        false
    });
    assert!(empty, "the connection stayed in the registry after closing");

    let stats = block_on(shards.stats()).expect("gathering stats");
    let accepted_count: u64 = stats
        .iter()
        .map(|shard| shard.connections.accepted_count)
        .sum();
    pretty_assertions::assert_eq!(accepted_count, 1);
    shards.shutdown().expect("shutting down cleanly");
}

#[test]
fn a_budgeted_background_task_runs_on_every_shard_without_blocking_the_foreground() {
    const CHUNKS_COUNT: u32 = 20;
    const CHUNK: std::time::Duration = std::time::Duration::from_micros(200);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let shards = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("starting the shards");

    let (finished, finishes) = flume::unbounded();
    shards
        .spawn_on_all(move |_shard| {
            let finished = finished.clone();
            async move {
                let mut budget = selene::budget::Budget::start(selene::budget::Policy {
                    warrant_percent: std::num::NonZeroU8::new(10).expect("literal"),
                    foreground_idle: std::time::Duration::from_millis(5),
                    sleep_max: std::time::Duration::from_micros(1500),
                });
                for _ in 0..CHUNKS_COUNT {
                    let until = std::time::Instant::now() + CHUNK;
                    while std::time::Instant::now() < until {
                        std::hint::spin_loop();
                    }
                    budget.tick().await;
                }
                finished
                    .send(budget.background())
                    .expect("the test still holds the receiver");
            }
        })
        .expect("spawning the background task");

    // The shards are busy with background work; the foreground still answers.
    for index in 0..4 {
        drop(round_trip(addr, format!("still here {index}\n").as_bytes()));
    }

    let mut background: Vec<std::time::Duration> = Vec::new();
    for _ in 0..usize::from(SHARDS_COUNT) {
        background.push(
            finishes
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("every shard finishes its chunks"),
        );
    }
    for accounted in background {
        assert!(
            accounted >= CHUNK * CHUNKS_COUNT,
            "the budget accounted for {accounted:?}, less than the work that ran",
        );
    }
    shards.shutdown().expect("shutting down cleanly");
}

#[test]
fn broadcast_runs_on_every_shard_and_waits_for_all_of_them() {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let shards = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("starting the shards");

    let (visited, visits) = flume::unbounded();
    block_on(shards.broadcast(move |shard| {
        let visited = visited.clone();
        async move {
            compio::runtime::time::sleep(std::time::Duration::from_millis(20)).await;
            visited
                .send(shard)
                .expect("the test still holds the receiver");
        }
    }))
    .expect("broadcasting");

    let mut seen: Vec<u16> = visits.drain().map(|shard| shard.index()).collect();
    seen.sort_unstable();
    pretty_assertions::assert_eq!(seen, vec![0, 1]);
    shards.shutdown().expect("shutting down cleanly");
}

#[test]
fn a_shard_at_its_limit_replies_and_closes() {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let mut listen = listen(addr.port());
    listen.max_connections = selene::listener::MaxConnections::Reply {
        count: std::num::NonZeroUsize::new(1).expect("literal"),
        message: b"-ERR max number of clients reached\r\n",
    };
    // One shard, so the limit is reached deterministically rather than
    // depending on how the kernel spread the connections.
    let mut config = config();
    config.count = selene::shard::Count::Exactly(std::num::NonZeroU16::new(1).expect("literal"));

    let shards =
        selene::shard::Shards::start(config, listen, |_shard| Echo).expect("starting the shard");
    let _held = round_trip(addr, b"first\n");

    let mut rejected = std::net::TcpStream::connect(addr).expect("connecting past the limit");
    let mut message = String::new();
    rejected
        .read_to_string(&mut message)
        .expect("reading the overload reply");
    pretty_assertions::assert_eq!(message, "-ERR max number of clients reached\r\n");

    let stats = block_on(shards.stats()).expect("gathering stats");
    pretty_assertions::assert_eq!(stats[0].connections.rejected_count, 1);
    shards.shutdown().expect("shutting down cleanly");
}

#[test]
fn an_ephemeral_port_is_rejected_because_reuseport_would_not_group_the_shards() {
    let error = selene::shard::Shards::start(config(), listen(0), |_shard| Echo)
        .expect_err("port 0 must not start");
    assert!(
        matches!(
            error,
            selene::shard::Error::Bind {
                source: selene::listener::Error::EphemeralPort { .. },
                ..
            }
        ),
        "unexpected error: {error:?}",
    );
}

#[test]
fn shutdown_is_reported_and_the_port_is_released() {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
    let shards = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("starting the shards");
    shards.shutdown().expect("shutting down cleanly");

    let restarted = selene::shard::Shards::start(config(), listen(addr.port()), |_shard| Echo)
        .expect("the port is free again");
    restarted.shutdown().expect("shutting down cleanly");
}
