//! RESP ping server, the Phase 1 benchmark target.
//!
//! `redis-benchmark -h 127.0.0.1 -p 6379 -t ping -P 16 -c 64`
//!
//! Pipelined commands are answered in one write: the reply buffer is built for
//! the whole read, not per command.

use compio::io::AsyncRead as _;
use compio::io::AsyncWriteExt as _;

const READ_BUFFER_SIZE: usize = 16 * 1024;
const PONG: &[u8] = b"+PONG\r\n";

struct Ping;

impl selene::listener::Service for Ping {
    async fn serve(
        &self,
        mut conn: compio::net::TcpStream,
        _peer: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(READ_BUFFER_SIZE);
        let mut replies = Vec::with_capacity(READ_BUFFER_SIZE);
        loop {
            let compio::BufResult(read, returned) = conn.read(buffer).await;
            buffer = returned;
            if read? == 0 {
                return Ok(());
            } else {
                selene::budget::mark_foreground();
            }

            let Parsed {
                commands_count,
                consumed_count,
            } = parse(&buffer);
            if commands_count == 0 {
                // A partial command: read more before answering anything.
                continue;
            } else {
                replies.clear();
                for _ in 0..commands_count {
                    replies.extend_from_slice(PONG);
                }
            }

            let compio::BufResult(written, returned) = conn.write_all(replies).await;
            replies = returned;
            written?;
            let _consumed = buffer.split_to(consumed_count);
        }
    }
}

/// How much of a read buffer formed complete commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Parsed {
    commands_count: usize,
    consumed_count: usize,
}

/// Count complete commands, inline or RESP array, without inspecting the verb:
/// this server answers `PONG` to anything, which is what the benchmark sends.
fn parse(buffer: &[u8]) -> Parsed {
    let mut parsed = Parsed {
        commands_count: 0,
        consumed_count: 0,
    };
    while let Some(length) = command_length(&buffer[parsed.consumed_count..]) {
        parsed.commands_count += 1;
        parsed.consumed_count += length;
    }
    parsed
}

/// Length of the complete command at the front of `bytes`, or `None` when it is
/// still partial.
fn command_length(bytes: &[u8]) -> Option<usize> {
    let first = bytes.first()?;
    if *first != b'*' {
        return line_length(bytes);
    } else {
        // A RESP array: a count line, then that many bulk strings.
    }

    let mut at = line_length(bytes)?;
    let arguments_count = number(&bytes[1..at])?;
    for _ in 0..arguments_count {
        let header = line_length(&bytes[at..])?;
        let payload_length = number(&bytes[at + 1..at + header])?;
        at += header + payload_length + 2;
        if at > bytes.len() {
            return None;
        } else {
            // The whole bulk string and its terminator are present.
        }
    }
    Some(at)
}

/// Length of the `\r\n`-terminated line at the front of `bytes`, terminator
/// included.
fn line_length(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .map(|start| start + 2)
}

/// Parse the decimal in a RESP header line, terminator included.
fn number(line: &[u8]) -> Option<usize> {
    let digits = line.strip_suffix(b"\r\n")?;
    if digits.is_empty() {
        return None;
    } else {
        // At least one digit to fold.
    }
    digits.iter().try_fold(0usize, |value, byte| {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            None
        } else {
            value.checked_mul(10)?.checked_add(usize::from(digit))
        }
    })
}

fn main() -> Result<(), MainError> {
    tracing_subscriber::fmt::init();

    let shards = selene::shard::Shards::start(
        selene::shard::Config {
            count: selene::shard::Count::PerCore,
            pin: selene::shard::Affinity::Auto,
            watchdog: selene::watchdog::Policy::Off,
            drain: std::time::Duration::from_secs(5),
        },
        selene::listener::Config {
            addr: std::net::SocketAddr::from(([127, 0, 0, 1], 6379)),
            backlog: std::num::NonZeroU32::new(4096).expect("literal"),
            nodelay: true,
            max_connections: selene::listener::MaxConnections::Reply {
                count: std::num::NonZeroUsize::new(4096).expect("literal"),
                message: b"-ERR max number of clients reached\r\n",
            },
        },
        |_shard| Ping,
    )?;

    tracing::info!(cpus = ?shards.affinity().ids(), "ping on 127.0.0.1:6379");
    std::thread::park();
    shards.shutdown().map_err(MainError::Shards)
}

#[derive(thiserror::Error)]
enum MainError {
    #[error("running the shards")]
    Shards(#[from] selene::shard::Error),
}

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

#[cfg(test)]
mod tests {
    #[test]
    fn an_inline_command_is_one_command() {
        pretty_assertions::assert_eq!(
            super::parse(b"PING\r\n"),
            super::Parsed {
                commands_count: 1,
                consumed_count: 6,
            }
        );
    }

    #[test]
    fn a_resp_array_counts_once_not_once_per_line() {
        pretty_assertions::assert_eq!(
            super::parse(b"*1\r\n$4\r\nPING\r\n"),
            super::Parsed {
                commands_count: 1,
                consumed_count: 14,
            }
        );
    }

    #[test]
    fn pipelined_commands_are_all_counted() {
        pretty_assertions::assert_eq!(
            super::parse(b"*1\r\n$4\r\nPING\r\nPING\r\n*1\r\n$4\r\nPING\r\n"),
            super::Parsed {
                commands_count: 3,
                consumed_count: 34,
            }
        );
    }

    #[test]
    fn a_truncated_array_consumes_nothing() {
        pretty_assertions::assert_eq!(
            super::parse(b"*1\r\n$4\r\nPI"),
            super::Parsed {
                commands_count: 0,
                consumed_count: 0,
            }
        );
    }

    #[test]
    fn a_complete_command_before_a_truncated_one_is_still_answered() {
        pretty_assertions::assert_eq!(
            super::parse(b"PING\r\n*1\r\n$4\r\nPI"),
            super::Parsed {
                commands_count: 1,
                consumed_count: 6,
            }
        );
    }

    #[test]
    fn a_multi_argument_array_is_one_command() {
        pretty_assertions::assert_eq!(
            super::parse(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"),
            super::Parsed {
                commands_count: 1,
                consumed_count: 22,
            }
        );
    }
}
