//! Echo server. The buffer goes into the read and comes back out, which is the
//! ownership-transfer tax completion-based I/O charges in place of borrowing.
//!
//! Run it, then `nc 127.0.0.1 9000`. Stats are gathered from the main thread,
//! off the shards, which is the seam in miniature.

use compio::io::AsyncRead as _;
use compio::io::AsyncWriteExt as _;

const READ_BUFFER_SIZE: usize = 16 * 1024;
const STATS_EVERY: std::time::Duration = std::time::Duration::from_secs(2);

struct Echo;

impl selene::listener::Service for Echo {
    async fn serve(
        &self,
        mut conn: compio::net::TcpStream,
        _peer: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(READ_BUFFER_SIZE);
        loop {
            let compio::BufResult(read, returned) = conn.read(buffer).await;
            buffer = returned;
            if read? == 0 {
                return Ok(());
            } else {
                selene::budget::mark_foreground();
            }

            let compio::BufResult(written, returned) = conn.write_all(buffer).await;
            buffer = returned;
            written?;
            buffer.clear();
        }
    }
}

fn main() -> Result<(), MainError> {
    tracing_subscriber::fmt::init();

    let shards = selene::shard::Shards::start(
        selene::shard::Config {
            count: selene::shard::Count::PerCore,
            pin: selene::shard::Affinity::Auto,
            watchdog: selene::watchdog::Policy::On {
                stall_after: std::time::Duration::from_millis(200),
                sample_every: std::time::Duration::from_millis(50),
            },
            drain: std::time::Duration::from_secs(5),
        },
        selene::listener::Config {
            addr: std::net::SocketAddr::from(([127, 0, 0, 1], 9000)),
            backlog: std::num::NonZeroU32::new(1024).expect("literal"),
            nodelay: true,
            max_connections: selene::listener::MaxConnections::Unlimited,
        },
        |_shard| Echo,
    )?;

    tracing::info!(cpus = ?shards.affinity().ids(), "echoing on 127.0.0.1:9000");

    // The control plane in miniature: a runtime that is not a shard, asking the
    // shards for their counters. An application would put axum here instead.
    let control = compio::runtime::Runtime::new().map_err(MainError::ControlRuntime)?;
    control.block_on(async {
        loop {
            compio::runtime::time::sleep(STATS_EVERY).await;
            match shards.stats().await {
                Ok(stats) => {
                    let open_count: usize =
                        stats.iter().map(|shard| shard.connections.open_count).sum();
                    tracing::info!(open_count, "connections");
                }
                Err(error) => tracing::error!(error = ?error, "gathering stats"),
            }
        }
    })
}

#[derive(thiserror::Error)]
enum MainError {
    #[error("starting the shards")]
    Shards(#[from] selene::shard::Error),
    #[error("building the control-plane runtime")]
    ControlRuntime(#[source] std::io::Error),
}

// `Termination` prints the `Debug` representation, so render the whole chain.
impl std::fmt::Debug for MainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")?;
        let mut source = std::error::Error::source(self);
        while let Some(error) = source {
            write!(f, "\n  caused by: {error}")?;
            source = std::error::Error::source(error);
        }
        Ok(())
    }
}
