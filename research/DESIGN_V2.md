---
tags:
  - rust
  - cpp
  - architecture
  - performance
  - concurrency
sources:
  - "Raw/Rust/Shard-per-core Rust runtimes - Monoio, Compio, and Glommio compared.md"
  - "Raw/Rust/The blazingly fast Rust crate stack for 2025–2026.md"
  - "helio repository map @ 145a679 (supplied in-session, not in Raw/)"
last_updated: 2026-08-08
---

# Selene — Design Doc

Selene is a proposed idiomatic Rust rewrite of [[helio]], the C++17 fiber-and-proactor
backend framework underneath [[dragonfly|DragonflyDB]]. The central claim of this document
is that **a rewrite should not be a port**: most of helio's ~74k lines exist because
C++17 has no `async`/`await`, no cargo, and no networking ecosystem, and nearly every one of
those lines has a maintained crate behind it. Selene is the part that is actually
load-bearing, sitting on the [[blazingly-fast-rust-crate-stack|Rust crate stack]] for
everything else.

Target: **one crate**, replacing ~45k lines of hand-written non-test C++.

## 1. What helio actually is, and why it is that big

Helio is one OS thread per core; each thread owns a `Proactor` ([[io-uring]] or epoll/kqueue)
and a `Scheduler` (stackful fiber ready/sleep/terminate queues). Blocking is per-*fiber*,
never per-*thread*. It is a good design, faithfully executed. It is also enormous, and the
size is almost entirely a language artifact:

| Helio subsystem | Lines | Why it exists |
|---|---:|---|
| `util/fibers/` | 14,292 | C++ has no coroutine runtime, so helio forked Boost.Fiber |
| `base/` | 26,080 (≈16k vendored) | C++ has no `bytes`, no `crossbeam`, no `tracing`, no `hdrhistogram` |
| `util/http/` | 8,041 | Beast needs an Asio shim; the status page is hand-rolled HTML/JS |
| `util/tls/` | 5,438 | OpenSSL has no sans-io mode, so helio built a BIO-pair state machine |
| `util/cloud/` + `util/aws/` | 5,922 | **Two** independent S3 stacks, plus GCS and Azure, hand-rolled |
| `io/` | 2,369 | C++ has no `std::io::Read`/`Write` |
| `cmake/` + `blaze.sh` | ~1,300 | Includes a 200-line download-retry engine because ExternalProject flakes |
| `tools/gdb_fibers.py` | 535 | Decodes `boost::context` register frames out of core dumps |

None of these are bad code. `download_retry.cmake` with exponential backoff and SHA
verification is a sensible response to a real problem. It is simply a problem `Cargo.lock`
does not have.

## 2. Architecture

Two planes, two runtimes. This is the one structural departure from helio, and it is what
buys the ecosystem back.

```
┌─ shard 0 … N-1  ([[compio]], pinned, !Send) ────────────────┐
│  SO_REUSEPORT listener → connection tasks → shard state    │   DATA PLANE
│  thread-local counters, RefCell state, no Arc, no Mutex    │   hot, io_uring
└──────────────────────────┬─────────────────────────────────┘
                           │  flume channel, cold path only
┌──────────────────────────▼─────────────────────────────────┐
│  control runtime ([[tokio]] multi_thread, 1–2 threads)      │   CONTROL PLANE
│  axum: /metrics /healthz /pprof   ·  object_store: snapshots│   cold, epoll
│  hickory-resolver: DNS            ·  tracing-subscriber      │
└─────────────────────────────────────────────────────────────┘
```

Helio puts its HTTP admin server on the same proactors as the data path, which is why it
needed `AsioStreamAdapter` to make Beast speak fiber-socket. Selene puts admin, metrics, DNS
and cloud storage on a small boring tokio runtime and lets them use the whole tokio
ecosystem unmodified. The data plane never touches it except to hand over a stats snapshot.
Cost: two extra threads and one channel hop on a cold path. Benefit: `util/http`,
`util/cloud`, `util/aws`, `util/metrics` and `util/html` all become dependencies rather than
source — 19,885 lines deleted for the price of a `flume::Sender`.

### 2.1 Runtime choice: compio

Per [[shard-per-core-runtimes-compared]], [[compio]] is the standing recommendation for new
[[io-uring]] projects, and helio's requirement set does not change that:

| Requirement | [[monoio]] | [[compio]] | [[glommio]] | tokio TPC |
|---|---|---|---|---|
| Shard-per-core, `!Send` state | ✓ | ✓ | ✓ | ✓ |
| Broad io_uring feature coverage | slab, zero-alloc | **broadest** | triple-ring | none |
| macOS/FreeBSD (helio supports these) | partial | **✓ (IOCP + polling)** | ✗ Linux only | ✓ |
| Non-uring fallback for seccomp'd containers | FusionDriver | **✓** | ✗ | ✓ native |
| Priority/budget scheduling | ✗ | ✗ | **✓** | ✗ |
| Actively maintained (2026) | slowing | **✓** | ✗ | ✓ |
| Enables [[deterministic-simulation-testing]] | ✗ | **✓ (driver/executor split)** | ✗ | partial |

Compio's per-operation boxing is the one loss against monoio's slab; [[apache-iggy]] measured
it as negligible under [[mimalloc]], which Selene uses as its `#[global_allocator]` anyway.
Glommio's proportional-share scheduler is the only thing Selene genuinely wants from another
runtime, and §3.2 shows it can be rebuilt above the runtime rather than by adopting an
unmaintained one.

**No abstraction layer over the runtime.** A trait with one implementation is a maintenance
tax for a swap that may never happen. If compio has to go, that is a mechanical refactor of
one module, and compio's driver/executor separation means the likely escape hatch is keeping
their driver under our own executor — which is exactly what it was designed for.

### 2.2 Connection distribution: SO_REUSEPORT deletes a protocol

Helio runs one accept loop on one proactor, then hands each socket to a proactor chosen by
`PickConnectionProactor()`. Because connections then live on a thread other than the one that
walks them, helio needs `migrate_traversal_state_` — a single `atomic_uint64_t` split into a
high-32-bit migration counter and a low-32-bit traversal counter, with deliberately
asymmetric progress rules so rare traversals do not starve under frequent migrations.

Selene: every shard opens its own listener with `SO_REUSEPORT`; the kernel distributes.
Connections are born on the shard that will serve them.

- `--use_incoming_cpu` → `SO_INCOMING_CPU`, or CBPF steering via `SO_ATTACH_REUSEPORT_CBPF`.
- Traversal (list clients, kill clients) → send a message to each shard; each walks its own
  `Slab<ConnHandle>` **on its own thread**. Zero atomics, zero protocol.
- Migration → if rebalancing ever proves necessary, send the raw fd over a channel and
  re-register on the target shard. Measure before building it.

The 64-bit split-counter protocol, its starvation-avoidance asymmetry, and the header comment
explaining both are deleted outright. This is the shape of most of Selene's wins: the
complexity was downstream of a structural choice, not inherent to the problem.

## 3. Module layout

```
selene/
  Cargo.toml              features: tls, admin (both default)
  src/
    lib.rs
    shard.rs      shard pool, pinning, fan-out, graceful shutdown
    listener.rs   SO_REUSEPORT accept, Service trait, conn registry
    budget.rs     background priority / warrant scheduling
    watchdog.rs   stall detection + backtrace dump
    admin.rs      control-plane tokio runtime: axum, metrics, pprof
    uring.rs      raw opcodes only if compio lacks a feature (see §7)
  examples/       echo, ping (RESP), tls_echo
  tests/          ported Python integration tests
```

One crate. Feature flags, not sub-crates — a workspace split is warranted when compile times
or independent versioning demand it, and neither does yet (see [[rust-workspace-patterns]]).

### 3.1 The shard pool

Helio's `ProactorPool` exposes a four-way fan-out matrix, SFINAE-gated on callback signature:
`DispatchBrief` / `AwaitBrief` (run on the I/O loop, must not fiber-block) versus
`DispatchOnAll` / `AwaitFiberOnAll` (run in a fiber, may block). That distinction does not
exist in Rust — every `async fn` is a task, and a task that awaits does not stall the loop.
Four methods collapse to two.

```rust
let shards = Shards::builder()
    .count(ShardCount::PerCore)     // sched_getaffinity, so taskset is respected
    .pin(Affinity::Auto)
    .build()?;

shards.spawn_on_all(|id| async move { warm(id).await });   // fire and forget
shards.broadcast(|id| async move { flush(id).await }).await;  // join all
```

Per-shard state is a plain `RefCell<ShardState>` in a `thread_local!` — no `Arc`, no `Mutex`.
The [[thread-per-core#failure-modes|RefCell footgun]] applies: never hold a borrow across an
`.await`. Where that gets awkward, decompose struct-of-arrays as [[apache-iggy]] did
([[soa-vs-aos]]).

Helio's `fu2::function_base<capacity_fixed<16,8>>` task type, with its
`static_assert(sizeof(Tasklet) == 32)` and the compile error you get for capturing more than
16 bytes, has no analogue. Async blocks are compiler-sized state machines.

### 3.2 Background priority — the one thing Selene must actually write

Helio's scheduler implements a real policy: `NORMAL` fibers get a 1 ms round-robin budget with
the dispatcher inserted into the round; `BACKGROUND` fibers get 50 µs and are allowed to run
only while under a 10% warrant of total observed runtime, with probabilistic sleeping when the
system looks idle and a "punish the hog" counter reset at 50 ms instead of 10 ms if background
overran normal. Dragonfly depends on this. Compio and monoio have **FIFO with no fairness**.

Do not fork the runtime. Rust's `.await` points are explicit, so the policy can live in a
guard that background tasks await:

```rust
// selene::budget — per-shard, thread-local, TSC-timed via quanta
loop {
    do_a_chunk();
    budget.tick().await;
}
```

`tick()` compares accumulated background cycles against foreground cycles and:

- **under warrant** → returns `Poll::Ready` immediately, no yield, no context switch
  (helio makes the same optimisation explicitly in `Preempt`'s BACKGROUND branch);
- **over warrant** → `yield_now().await`;
- **over warrant and foreground active in the last 5 ms** → sleeps `min(1.5 ms, last chunk)`.

Same policy, no runtime fork, and the yield points are visible in the source instead of
implicit at every fiber suspension.

Two helio concepts die here. `FiberAtomicGuard` — the no-preempt marker that logs `DFATAL`
with a stack trace if you suspend inside it — is unnecessary, because code between `.await`
points is already atomic with respect to the executor. And the `AGENTS.md` mandate "never use
`std::mutex`, `std::condition_variable`, `std::this_thread::sleep_for`" becomes clippy's
`await_holding_lock` plus the fact that `std::thread::sleep` in an async fn is a code smell
everyone already recognises. A social rule becomes a lint.

### 3.3 Stall detection

Compio has none, and [[thread-per-core]] names head-of-line blocking as the pattern's primary
failure mode. Helio's answer is `PrintAllFiberStackTraces()`, which suspends onto the
dispatcher and walks every fiber's stack — plus `tools/gdb_fibers.py`, 535 lines that decode
`boost::context`'s saved-register frame layouts for x86_64 and aarch64 directly out of core
dumps.

Selene has no fiber stacks to decode. A watchdog thread samples a per-shard `AtomicU64`
heartbeat; if a shard has not ticked in N ms it signals that thread and captures a backtrace
via the `pprof` crate. Replaces all 535 lines plus the in-process dumper, and works the same
in a core dump because it is just a thread backtrace.

### 3.4 The Service trait

Helio's `ListenerInterface` has seven virtual hooks (`PreAcceptLoop`, `ConfigureServerSocket`,
`OnConnectionStart`, `OnConnectionClose`, `OnMaxConnectionsReached`, `PreShutdown`,
`PostShutdown`) plus a `Connection` base class with three more.

```rust
pub trait Service: 'static {
    async fn serve(&self, conn: TcpStream, peer: SocketAddr) -> io::Result<()>;
    fn on_shutdown(&self) {}
}
```

Two. `ConfigureServerSocket` and `PreAcceptLoop` are configuration, so they move to the
builder as a `SocketConfig`. `OnConnectionStart`/`OnConnectionClose` are RAII, so they are the
top and the `Drop` of a guard inside `serve`. `OnMaxConnectionsReached` is a policy enum on the
builder. Note that `async fn` in traits is not dyn-compatible ([[rust-async-evolution]]) —
fine here, because the shard is generic over `S: Service` and never needs `dyn`.

## 4. The deletion table

| Helio | Lines | Selene |
|---|---:|---|
| `util/fibers/` — scheduler, proactors, fiber_interface, sync | 14,292 | [[compio]] + `shard.rs` + `budget.rs` |
| `base/` (≈16k vendored SIMD/utility headers) | 26,080 | [[bytes]], [[crossbeam-array-queue]], [[hdrhistogram]], [[quanta]], [[tracing]], [[foldhash]], [[bumpalo]], [[rustix]], [[socket2]] |
| `util/http/` — Beast, AsioStreamAdapter, status pages | 8,041 | axum on the control plane |
| `util/tls/` — BIO-pair engine, TlsSocket, async req machinery | 5,438 | [[rustls]] via `compio-tls` |
| `util/cloud/` + `util/aws/` — two S3 stacks, GCS, Azure | 5,922 | `object_store` |
| `examples/` | 5,221 | echo, ping, tls_echo |
| `io/` — Source/Sink/Result/adapters/files | 2,369 | `std::io` + [[bytes]] + compio-fs |
| `cmake/` + `blaze.sh` + `install-dependencies.sh` | ~1,300 | `Cargo.toml` |
| `tools/gdb_fibers.py` | 535 | `watchdog.rs` |
| `strings/` | 489 | `humansize`, `percent-encoding` |
| `util/metrics/` + `varz` + `util/html/` | ~900 | `metrics` + `metrics-exporter-prometheus` |
| `tests/` (Python integration) | 1,007 | kept, ported |

Every row on the right is either a dependency or a module small enough to hold in one head.
How small is not knowable until Phase 1 exists; §4.1 is the part of this that does not depend
on guessing.

### 4.1 The honest caveat

Most of that reduction is a **maintenance transfer, not a magic trick**. Selene trades 45k
lines it owns for roughly sixty crates it does not. The real gains are that
[[rustls]] is memory-safe and audited where a hand-rolled OpenSSL BIO-pair state machine is
neither, that `object_store` is maintained by the Arrow project where two divergent in-house
S3 clients are maintained by nobody, and that nobody gets paged for `download_retry.cmake`
again. The real costs are supply-chain surface — mitigated but not eliminated by
[[cargo-deny]] — and less direct control over the io_uring hot path. Section 7 gates on
exactly that.

One asymmetry worth stating plainly: helio's bus factor is already ~1 (517 of ~700 commits
from a single author). Compio's is also ~1. Adopting it is a lateral move on that axis, not a
regression.

### 4.2 Synchronization primitives

Helio ships `Mutex`, `CondVar`, `CondVarAny`, `SharedMutex`, `Done`, `BlockingCounter`,
`FiberBlockingCounter`, `Barrier`, `EventCount`, `WaitQueue` and `FiberAtomicGuard` — roughly
2k lines. On a single-threaded shard, most of them have nothing to synchronize.

| helio | Selene |
|---|---|
| `fb2::Mutex` | `RefCell` (never borrowed across `.await`) |
| `fb2::CondVar` | channel receive, or a `Notify` equivalent |
| `fb2::Done` | `oneshot` channel |
| `BlockingCounter` / `FiberBlockingCounter` | `FuturesUnordered` / `join_all` |
| `fb2::Barrier` | rarely needed; `broadcast()` covers the fan-out/join case |
| `EventCount` | gone — it existed to make notification lock-free from the I/O loop, which is what a `Waker` already is |
| `FiberAtomicGuard` | gone — §3.2 |
| cross-shard messaging | `flume` (sync `send`, async `recv_async`, works across both runtimes) |

`FiberBlockingCounter` deserves a note: it is 8 bytes, backed by a counter embedded in the
constructing fiber, zero-allocation, and only valid for a tight construct→dispatch→`Wait()`
round — overlapping rounds on one fiber corrupt each other, DCHECK'd in debug. That is a
genuinely clever piece of engineering that exists solely because C++ made the obvious version
allocate. It has no Rust counterpart because the obvious version does not allocate.

## 5. Idioms

**Errors.** `nonstd::expected` → `std::io::Result` for OS errors, [[thiserror]] for Selene's
own enum, no [[anyhow]] in the library ([[rust-error-handling]]). The `RETURN_ERROR` and
`RETURN_UNEXPECTED` macros become `?`. `ABSL_MUST_USE_RESULT` becomes the default.

**Buffers.** `base::IoBuf`'s `[consumed | unread | append room]` layout is `BytesMut` plus
`split_to`. Helio's `ProvidedBuffer` — a tagged union of a heap pointer and a
`{buf_id, buf_pos}` pair for io_uring buffer-ring receives — dissolves into compio's buffer
pool handle. The ownership-transfer read loop is the tax:

```rust
let mut buf = BytesMut::with_capacity(16 * 1024);
loop {
    let BufResult(n, b) = sock.read(buf).await;
    buf = b;                       // buffer goes in, comes back out
    if n? == 0 { break }
    parse(&buf)?;
    buf.clear();
}
```

**Observability.** `logging.h`'s `#ifdef USE_ABSL_LOG` seam and `file_log_sink.cc` — ~600
lines whose entire purpose is to be swappable — become [[tracing]] plus
`tracing-subscriber`, where swappability is the design. Per-connection spans come free;
helio cannot do them at all. `/flagz`'s only real use, changing log levels without a restart,
is `tracing_subscriber::reload`.

**Unsafe.** `#![deny(unsafe_code)]` crate-wide, allowed only in `uring.rs` if §7 finds a gap.
Helio's fiber runtime is unsafe by construction — the `FiberInterface` is placement-new'd at
the top of its own stack, and `intrusive_ptr_release` moves the `fiber_context` out, runs the
destructor, then resumes into the tail of `Terminate()` on the dying fiber's own stack so the
stack can free itself. That entire category is gone: futures are stackless state machines.
Per-connection memory drops from kilobytes of stack to hundreds of bytes of state machine, and
`HELIO_INSTRUMENT_STACK` has nothing to instrument.

## 6. Hard rules

1. **No `select!` on io_uring I/O futures.** Dropping an in-flight future is a use-after-free
   in kernel space; [[io-uring#the-cancellation-safety-crisis|every io_uring runtime leaks TCP
   connections]] when `select!` is used for timeouts. Timeouts go *into* the operation, which
   is what helio already does via `FiberCall fc(proactor, timeout_ms)`. Not a regression —
   but Rust invites the mistake in a way C++ did not, so it needs a lint and a review rule.
2. **No `RefCell` borrow held across `.await`.**
3. **No new abstraction with one implementation**, the runtime wrapper included.
4. **Every simplification with a known ceiling carries a comment naming the ceiling.**

## 7. Phasing

**Phase 0 — bake-off (1 week, hard gate).** Port `echo_server` to compio, monoio, and tokio
`current_thread`-per-core. Measure against helio's `echo_server` on the same box: throughput,
p99, p99.99, bytes per connection. Verify compio exposes buffer rings/pools, **multishot
recv**, **direct/fixed FDs**, and `SO_REUSEPORT` accept — the wiki confirms buffer pools and
"broadest io_uring feature coverage," but multishot and direct FDs are unverified and helio
uses both. *If compio lands more than 10% behind helio, stop and reconsider before writing
anything else.* Helio's `MainLoop` is 270 lines of carefully ordered submit/reap/run with a
jump-attribution histogram and a 500 µs task budget; a generic loop is not obviously its equal.

**Phase 1 — data plane.** Shard pool, listener, `Service`, connection registry, graceful
shutdown, tracing, `budget.rs`, `watchdog.rs`. Port `ping_iouring_server` (RESP). Gate: match
helio's ping benchmark.

**Phase 2 — TLS and control plane.** rustls via compio-tls; tokio control runtime with axum,
`metrics-exporter-prometheus`, and a `pprof` handler. Gate: TLS echo parity, and a `/metrics`
scrape that does not move shard p99.

**Phase 3 — cold path.** `object_store` snapshots, `hickory-resolver`, port the Python
integration tests (they drive binaries, so they port nearly unchanged). Build the
[[deterministic-simulation-testing]] harness on compio's driver/executor split — a capability
helio has no equivalent of, and the one place Selene should be strictly *better* rather than
merely smaller.

**Phase 4 — audit.** Confirm the deletion table against reality, `cargo deny` clean,
[[iai-callgrind]] instruction-count gates in CI (helio has no regression gate at all).

CI collapses from 5 configs × 2 arches of cmake matrix to `cargo nextest`, `cargo clippy
-D warnings`, `cargo deny check`, and `cargo miri` on `uring.rs`. Containers still need
`--security-opt seccomp=unconfined`, and the non-uring fallback path must be exercised in CI
— that constraint is inherited from io_uring, not from helio.

## 8. Risks

| Risk | Mitigation |
|---|---|
| Compio underperforms helio's hand-tuned loop | Phase 0 gate; escape hatch is a custom executor over compio's driver, which the split was designed for |
| io_uring cancellation UAF | Rule 6.1, enforced by lint and review |
| Two runtimes in one process | Real but small: two timer wheels, more threads. Buys the entire tokio ecosystem for the cold path |
| Background policy less faithful than helio's | Cooperative either way — a background chunk that never awaits stalls a helio fiber too. Parity, not regression |
| Compio bus factor | Lateral move (§4.1); driver is small enough to fork |
| Container seccomp blocks io_uring | Same exposure as helio's `--force_epoll`; must be a CI job |
| Ecosystem gaps on compio | Sidestepped by design: anything needing the tokio ecosystem lives on the control plane |

## 9. Not doing

No cloud storage clients. No custom logging facade. No `-fno-builtin-malloc` dance —
[[mimalloc]] as `#[global_allocator]`. No SIMD translation shims, no cuckoo map, no varint
encoder, no SSO string view, no fixed-capacity callable type. No runtime abstraction trait.
No built-in HTML status page in v1 — Grafana reads `/metrics`; add one when somebody actually
asks. No PGO plumbing in the build; `cargo-pgo` when a benchmark justifies it
([[cargo-profile-optimization]]).

## 10. Open questions

- Does compio expose multishot recv and direct/fixed FDs? Phase 0 blocks on this.
- Does Dragonfly's workload actually need connection migration, or does `SO_INCOMING_CPU`
  steering at accept time cover every case `Migrate()` currently serves?
- Is the 10% background warrant a tuned constant or a Dragonfly-specific one? Selene should
  expose it as config rather than inherit the magic number.
- Does the [[thread-per-core#the-queueing-theory-penalty|3× overprovisioning penalty]] apply
  to Dragonfly's access pattern? Helio committed to shard-per-core without publishing that
  analysis; a rewrite is the moment to check whether the premise still holds.
