# selene

A shard-per-core backend framework in Rust. One OS thread per shard, each owning a
[`compio`](https://github.com/compio-rs/compio) runtime, its own `SO_REUSEPORT` listener, and its
own `!Send` connection state. No `Arc`, no `Mutex`, no connection migration.

Selene is an idiomatic rewrite of the load-bearing parts of
[helio](https://github.com/romange/helio), the C++17 fiber-and-proactor framework under DragonflyDB.
See `research/DESIGN_V2.md` for what is deleted and why.

## Layers

- `shard`: the pool. Thread placement, fan-out to every shard, graceful shutdown, and the
  `stats()` / `affinity()` seam the control plane calls across.
- `listener`: the per-shard `SO_REUSEPORT` socket, the `Service` contract, and the thread-local
  connection registry each shard walks on its own thread.
- `budget`: background priority. A warrant over the shard's observed runtime, applied at `.await`
  points a background task chooses, so no runtime is forked.
- `watchdog`: stall detection. Shards tick a counter; a watchdog thread reports one whose event
  loop has stopped turning.

## Control plane

Selene spawns shard threads and nothing else. Metrics, snapshot upload, DNS, and admin HTTP run on
a runtime the application builds and owns, where the tokio ecosystem works unmodified. The seam is
two calls: `stats()` gathers per-shard counters through one message per shard, and `affinity()`
reports the CPUs the shards claimed so the application can keep its own threads off them.

The admission rule for anything else: if it can run late without a client noticing, it does not
belong on a shard.

## Status

Phase 1 of the design: data plane, `Service`, connection registry, graceful shutdown, background
budget, watchdog. TLS (`compio-tls`), the `object_store` cold path, and the `admin` example are
Phase 2 and 3 and are not written yet.

```
cargo nextest run
cargo clippy --workspace --all-targets
cargo run --release --example ping     # RESP, 127.0.0.1:6379
cargo run --release --example echo     # 127.0.0.1:9000
```
