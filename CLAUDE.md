Legend: `>` prefer, `=` is, `¬` not, `→` leads to, `≠` not equal.

## Priorities

- `safety > performance > developer_experience`. `correctness > tiny diffs > compatibility `.
- Zero users. Zero debt. Do it right.

## Ownership / canonical types

- 1 type per domain concept in 1 owner crate/module. ¬ duplicated struct/enum/newtype/wire/DB row across crates. Multi-consumer → shared owner crate.
- **Single source of truth per domain.** 1 canonical store per kind of data; everything else = read-only projection / cache.
- Boundary adapter OK iff semantics differ. Thin, 1-way. Wrong arch → rewrite, ¬ paper over with adapters.
- Shared runtime clients (HTTP/RPC/DB pools) built once at composition root, threaded. `Clone` = cheap.
- 1 crate = 1 concept. ¬ god crates. Shared crates = canonical concepts, ¬ grab-bag.
- Split small crates by boundary, ¬ convenience. Give every crate the smallest possible scope.
- Target-specific → small platform modules at the owner > scattered `#[cfg]`.
- No wrapper methods that just forward. Repeated literal/path/flag → 1 named owner.

## Imports / names

- ¬ `use` imports. Full paths (`crate::foo::Bar`, `std::collections::HashMap`). Exception: `use some::Trait as _;`. ¬ glob, ¬ `super::`.
- ¬ `pub use` re-exports, except when a module's primary export shares the module name: `mod open; pub use open::open;`.
- Visibility: `pub` / `pub(crate)` / private.
- Owner-local short names: `client::Client`, `vm::Config`, `id::Id`, `Error` / `Args` / `State` / `Request` / `Response`. ¬ repeat the domain when the module implies it.
- Most-descriptive-correct > short-vague: `rootfs_derived_cache_hash` > `cache_hash`. Banned vague names when role is knowable: `data` / `value` / `info` / `obj` / `ctx` / `res` / `tmp` / `base`.
- ¬ leading-underscore on used names. `_name` is only for keepalive guards.
- Param structs short + scoped: `xattr::Req`, ¬ `xattr::SetxattrReq`.
- Get the nouns and verbs right. Names are the essence. ¬ abbreviations.
- Put units and qualifiers last, in descending significance: `latency_ms_max`.
- Related names use the same character count: `source` / `target`, ¬ `src` / `dest`.
- `index` is 0-based. `index + 1 = count`; `count × unit = size`. Put units in names.
- Put important things first. Put `main` first. Order everything else alphabetically.

## Code shape

- Hard 100-column limit. Files ~300 lines target, 1000 hard max. Filenames no underscores. Dir hierarchy > compound names. No `mod.rs`; use `foo.rs` + `foo/bar.rs`.
- Functions ≤70 lines. Each fn = 1 named thing, ~5–40 lines, testable alone.
- Mixable arguments → options struct. Max 3 fn params; >5 is always a design failure. Shallow paths, short locals, blank lines between logical steps. Destructure > field access: `let X { a, b } = …?;`.
- ¬ anonymous tuples as domain types, even pairs. `HashMap<(u8, u8, u8), _>` / `fn f((a, b, c): (T, U, V))` / `-> (Foo, Bar)` → named struct with named fields. Tuple OK only for trivially ordered iterator items such as `(key, value)` and throwaway boundary adapters.
- Invariants in types/newtypes/enums/ctors/RAII. ¬ bool/sentinel/str/tuple/magic#.
- Code > comments: prefer a typed map like `HashMap<ComputeId, Entry>` (with `ComputeId(Uuid)`) over `HashMap<Uuid, Entry>` + a comment. Comments = non-obvious why/contract only.
- Derive > boilerplate. `Display` / `From` / `Into` / `AsRef` → `derive_more`. ¬ new macros. ¬ shorthand decl macros bundling derives.
- No enum↔str match ladders. Use `strum` (`AsRefStr` / `Display` / `EnumString`) + `#[strum(serialize_all = "snake_case")]`. Non-returning fns → `-> !`.
- UUID identity → newtype. Hashes → BLAKE3 newtype. URLs → `url::Url` (parse at constructor).
- Explicit, simple control flow. ¬ recursion. Each abstraction earns its keep.
- Bound every loop, queue, retry, and work period. Exceeding a fixed upper bound → fail fast.
- Split compound conditions into nested `if` / `else`. Every `if` gets a matching `else`.
- Push `if`s up and `for`s down. Parent functions centralize control; leaf functions perform pure computation.
- Long `match` / `if` ladders → extract named fns returning typed decisions (`enum Decision { A, B(K) }` > `(bool, Option<K>)`). Glue = flat sequence of named steps; logic lives inside steps. ¬ round-trip re-derivation. ¬ variable duplication. Calculate close to use. Rewrite if nesting > 2.

## Config

- ¬ sentinel values (`0 = disabled`, empty-string toggles, `u32::MAX` / `-1` conversions). Use an enum policy, `Option`, or a validated newtype.
- Optional positive N → `Option<NonZeroU{32,64}>`. Compile-time known → `const`.
- ¬ hidden runtime defaults: no `Default` for runtime-significant cfg, no `#[serde(default)]`, no env/hardcoded fallback.
- Service cfg strict: `#[serde(deny_unknown_fields)]`. Every runtime value explicit.
- Env vars only for cross-cutting ops (tracing filters, Sentry DSN, injected secrets).

## Errors

- `thiserror` only. Banned: `anyhow`, `eyre`, `color_eyre`, `Result<_, String>`, `Result<_, &str>`, `Box<dyn Error>` as `source`, stringly variants `#[error("{0}")] Other(String)`, blanket `#[from]` on ubiquitous types (`io::Error`) into a god enum. `#[error(transparent)]` legal ONLY for the opaque-newtype public API boundary — ¬ as an "anything-else" catch-all.
- Wire boundary = the only place a String error is allowed. Internal chain stays typed; convert via `Display` only at the wire adapter. Touching `Result<_, String>` → convert on sight.
- Generic trait sources → generic param `E: core::error::Error + Send + Sync + 'static` on the enum + `#[source] E` / `#[from] E` variant. thiserror auto-infers `E: Error + 'static` on the generated `impl Error` and `E: Display` on `impl Display`; keep the bound on the type so `#[derive(Debug)]` holds.
- Each `?` = deliberate, explicit context. Pure pass-through (variant adds nothing) → `#[from]` + bare `?`. Adds context → `.map_err(Error::Variant)?` (tuple-variant ctor as a fn) or `.map_err(|e| Error::Variant { source: e, field })?`. This covers **every** fallible call.
- ¬ lossy or context-free conversion. `map_err` is the mechanism, but it must carry a typed source into a named variant. Banned without exception:
  - `.map_err(|e| Error::X(e.to_string()))` → `#[source]` field + `.map_err(Error::X)?`
  - `.map_err(|_| Error::X)` discarding the source → `#[source]` field + `.map_err(Error::X)?`
  - one blanket `#[from] io::Error` reused where call-sites mean different things → distinct variants (`OpenFile` / `ReadFileContents`) + `.map_err(Error::OpenFile)?`
- ¬ `.map_err(|_| …)` to discard the source.
- Every error variant carries its upstream failure as a typed `#[source]` / `#[from]` field (`#[from]` implies `#[source]`). ¬ flattened error strings. ¬ fieldless variants when a source exists. ¬ re-stringified sources.
- Clippy `cast_{possible_truncation,precision_loss,sign_loss}` = a missing fallible boundary. Fix = `try_from` + `{ #[from] source: TryFromIntError }` (or `#[source]`) + `?`. Never `#[allow]` / `#[expect]`, bare `as`, `.unwrap_or(*)`, or saturating casts. Compile-time infallible → `const { assert!(…) }`.
- `-> ()` that logs and drops the error = bug. Scope error enums per-fn / per-mod, located near the unit of fallibility. ¬ god error objects (`crate::Error` / `errors.rs` = antipattern). ¬ branch on error strings — `match` on variants / walk `source()`.
- ¬ lossy: no `.to_string()` on errors, no `format!("{e}")` baked into a variant; never format a `#[source]` field into your own `Display`.
- Bin `main()`: typed `MainError`, `main() -> Result<(), MainError>`. `Termination` prints the `Debug` repr → give `MainError` a hand-rolled `Debug` that walks `source()` and renders the full chain (or wrap in nightly `std::error::Report::new(e).pretty(true)`). Backtraces via `RUST_BACKTRACE=1` + `#[backtrace]` / `std::backtrace::Backtrace` field.
- ¬ `panic!` / `unwrap` / `expect` / `.ok()` / `.unwrap_or(0)` / `.unwrap_or_default()` in prod. ¬ suppression at canonical boundaries: `do -i`, `|| true`, `2>/dev/null`, `| ignore`.

## Failure policy

- init / bootstrap / migration / oneshot → fail fast at the first canonical boundary, typed.
- Long-running services → crash on broken invariants, bad cfg, trust failure, durable-state corruption. Isolate transient peer / request / connection faults, surface, continue.
- Retries and recovery = explicit policy: bounded, observable, idempotent, 1 owner. Simplest mechanism at the real owner. Authoritative layer exposes the failure signal → bounded retry there, not caches / locks / reservations / lockfiles.
- Batch inbound events. Process them on an internal schedule with bounded work per period. ¬ direct reaction to external events.
- Optimistic-at-boundary > pre-coordination. Coordinate only when the boundary can't express the conflict or correctness needs serialized intent.

## Performance

- Design is where 1000× wins live. Back-of-envelope every design across `{network, disk, memory, CPU} × {bandwidth, latency}`.
- Optimize the slowest resource first: network → disk → memory → CPU.
- Amortize work through batching. Prefer sequential access and large chunks. ¬ zig-zagging.
- Minimize allocations. `&str` > `String`; `Cow` > cloning; stack > heap for bounded data.

## Rust

- Proc macros: absolute extern paths, never `crate::` across crates. `prettyplease` format. Fallible macros → `::core::compile_error!`, never `expect` / `unwrap` / `panic!`.
- Async fn: `#[tracing::instrument(level = "debug", skip_all)]` by default. `info` for public boundaries / lifecycle; `debug` for internals; `trace` for per-packet hot loops. ¬ committed debug prints (use `tracing`; CLI stdout = the only `println!` exception).
- Trace errors with `error = ?error` (Debug), ¬ `%error` (Display loses the source chain).
- `#[must_use]` on inherently must-use types; per-method for ctors, getters, pure fns. Every `Result` / `#[must_use]` handled.
- ¬ bare numeric `as`. `try_from` / `try_into` + typed propagation. `?` exits the fn, not a loop. Loop → `match` + `continue`; retain the last error and report it after exhaustion. `let Some(v) = … else { … };` > redundant pre-check.
- ¬ turbofish (`::<…>`). Annotate the binding instead: `let xs: Vec<Foo> = it.collect();`, `let none: Option<&CStr> = None;`. If only an argument needs the type, bind it first or pass a typed local. Exception: a free function with no binding and no other inference site (rare) — restructure before reaching for `::<…>`.
- Large strings → `include_str!` / `include_bytes!`.
- Heap alloc = cost. Borrowed views, stack arrays, `SmallVec` / `ArrayVec`, `Box<[T]>`, reuse. `impl Iterator` / `Display` / `Future` > premature collect / format / box. Collect only for random access, length, or whole-buffer use. Zero-copy default. `bytes::Bytes` across async boundaries. `Vec<u8>` only for mutation / local ownership.
- `swrite::swrite!` / `swriteln!` > `write!` / `writeln!` on `String`. Keep `write!` in `Display` impls. ¬ `format!` / `to_string()` in hot paths.
- ¬ `async_trait`. Native `async fn` in traits, or erased + `BoxFuture` for `dyn`. Free fn when there is no instance state.
- Randomness: `rand::rng().random_range(…)`. File lock: `std::fs::File::{lock, try_lock, lock_shared, try_lock_shared, unlock}`. Channels: `oneshot` for one-off / shutdown, `broadcast` for fan-out, `mpsc` for work queues.
- Concurrent map: `papaya::HashMap` > `dashmap::DashMap` > locked `HashMap`. Never `RwLock<HashMap>`. Hashers: `rapidhash::RapidHashMap` > `rustc_hash::FxBuildHasher`. Prefer unstable std variants unless stable iteration order matters. Use the largest fitting `Duration` constructor.
- Defensive asserts: `debug_assert!` / `more_asserts` for invariants and bounds.
- TDD for debugging. Failing test first. Exact structural asserts: scalar / short → `pretty_assertions::assert_eq`; multi-line next to the test → `expect_test::expect![[r#"..."#]]` (`UPDATE_EXPECT=1`); large / nested diff → `insta::assert_{snapshot, debug_snapshot, json_snapshot}!` (`cargo insta review`). ¬ partial `len` / `contains` when exact output is knowable.

## Binaries

- Split client / server / protocol into crates.
- Rust bins stay thin. `main.rs` ≈ entrypoint + CLI parse + single lib call. Behaviour lives in `lib.rs`.
- Keep dependencies low. Use library calls instead of shelling out.
- Arg parsing uses `clap` with `#[derive(Parser)]`. ¬ hand-rolled `std::env::args` + index loops + `--` flags.
- Env vars = typed through a parser. Every env-driven input declared on a `#[derive(clap::Parser)]` struct with `#[arg(env = "FOO")]` (or on a `#[derive(serde::Deserialize)]` config loaded from file / `envy`). ¬ `std::env::var("FOO")` scattered through `main` / lib code.
- Absolute binary paths in `exec` / `Command::new`. `#!/usr/bin/env` only in shebangs.

## Build / toolchain

- Rust Edition 2024. Nightly features are allowed only for concrete wins and are gated at the crate root.
- Pinned toolchain first; drift → fix `nix` / `direnv`.
- **Lint gate = `cargo clippy`, ¬ `cargo check`. Test gate = `cargo nextest run`, ¬ `cargo test`. Format gate = `cargo fmt`.** Fix every warning. ¬ suppression. `cargo clippy --tests` pre-push.
- `bun` > `npm` / `npx` for the web subtree. Published type packages > hand-rolled `.d.ts`.
- Helper scripts = Python (stdlib first), under `scripts/`, run via `direnv exec . python3` (interpreter lives in the nix devshell). Python > Bash for repo tooling: real data structures, typed JSON compare, no quoting / `set -u` / subshell footguns. 1 script per job, ¬ a Bash twin of the same logic.

## Investigation / debug

- Order: deployed state + incident artifacts → owning source → hypothesis. ¬ guess flags / paths / attrs.
- Once you've identified the exact binary + version + cfg, read that version's source. Third-party = upstream at the pinned version.
- Unfamiliar reference impl? `git clone --depth 1 <url> /tmp/<name>` is a disposable scratch. Speculating from memory / blog posts is the anti-pattern.
- First principles, lowest relevant layer up. Cross-layer bug → verify prereqs at the same layer before blaming a layer up.
- Test the hypothesis against real runtime before stating it. Actually read the state the suspect process would see, ¬ speculate from config.

## Validation

- Fast feedback: use the nearest real harness so environment breakage is separable from regressions. Library entrypoints / current-tree bins > installed host bins unless the deployed shape is the point.
- Preserve failure artifacts at a stable repo-owned path. Improve diagnostics before rerunning. Test timeouts → diagnostics first.
- Validate semantics, ¬ existence. ¬ claim "E2E works" without a real runtime pass; green compile + tests ≠ E2E proof.
- Verify end to end. Fix root causes, ¬ patches.
- **Push-down: integration failure → unit / property test before fix.** (1) repro, (2) narrow the minimum input, (3) `#[test]` or `hegel` property in the owner crate, (4) confirm it fails pre-fix, (5) fix, (6) confirm green. Commit the test with the fix. Property > case when the bug is a shape.

## Prose for humans

- ¬ em-dashes (`—`) and ¬ double-hyphen `--` substitutes. Both read as LLM. Replace with a period, a colon, or parentheses, or split into two sentences. Applies to commit messages, PR / issue bodies, docs, review comments, and this file.
- ¬ rhetorical tics that pattern-match as LLM output: "not just X but Y", "here's the thing", "it's worth noting", "I'd be happy to", overuse of "—". Say the thing directly.
- Long compound clauses with parentheticals → split. Colons and full stops > dashes.
- ¬ decorative emoji in prose unless explicitly asked. Status glyphs in operator docs (`✓`, `💥`) are OK.
- ¬ vague qualifiers without a named mechanism in the same clause: "at scale", "blazing", "world-class", "robust", "powerful", "seamless", "pragmatic", "first-class". If the adjective's meaning isn't made concrete next to it, cut it.
- ¬ comparative framing as hype: "instead of X", "unlike Y", "X over Y", "not just A but B", "better than Z", "real X instead of fake X". State what the thing is, ¬ what it beats. Interface statements ("accepts `nix` invocations unchanged", "drop-in compatible") describe behavior and are fine.
- ¬ performative value claims: "not a religion", "it just works", "built different", "designed for humans". Name the mechanism or cut the sentence.
- DRY prose. 1 concept = 1 place. Tagline ≠ goals ≠ architecture ≠ license section. ¬ restate composability / stack / license across sections. If a bullet and a paragraph cover the same ground, merge or delete one.
- Broad strokes > exhaustive lists in top-level docs. README architecture = named layers with 1-line each, ¬ every crate. Full inventory lives in code / `Cargo.toml`.

## Autonomy

- Default = act, ¬ ask. Tool available + op non-destructive → run.
- `git commit` and `git push` are allowed without asking when the change satisfies the repo's gates locally first: `cargo fmt --check`, `cargo clippy --workspace`, and `cargo nextest run` (on the affected packages). No green → don't commit, don't push.
- Still ask: force-push, history rewrite, `git reset --hard`, `git clean -f`, branch deletion; deleting PRs / issues / pages / messages; paid actions.
- Ambiguous → pick the reversible default, commit, report. Batch / coalesce rate-limited ops. User approving a risky op once = approval for that op, ¬ standing authorization.

## Overrides

- Repo-wide: `thiserror` for errors, `cargo clippy` as the lint gate, `cargo nextest run` as the test gate, `cargo fmt` as the format gate.
- Request conflicts with this file → stop, name the conflict, recommend the repo-consistent path, wait.
