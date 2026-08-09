# rust-nix-template

Rust workspace with a Nix-provided toolchain. Cargo for the dev loop, Nix for reproducible builds.

## Setup

Requires [Nix](https://nixos.org/download) with flakes.

```bash
git clone https://github.com/your-org/your-repo
cd your-repo
direnv allow   # or: nix develop
```

## Commands

```bash
cargo build        # dev loop — incremental
cargo test         # nextest
nix build          # hermetic, sandboxed
nix flake check    # clippy, fmt, tests
nix fmt            # rust, nix, toml
```

Use Cargo inside the shell. Nix only provides the environment. Do not route `cargo check` through `nix build` — it discards incremental compilation.

## Layout

```
flake.nix              # imports nix/ modules
rust-toolchain.toml    # Rust version pin, single source of truth
crates/cli/            # default crate
nix/
  toolchain.nix        # rust-overlay + crane
  packages.nix         # crane builds
  devshell.nix         # dev shell
  fmt.nix              # treefmt
```

## Common changes

**Rust version** — edit `rust-toolchain.toml`. Keep `rust-src`; rust-analyzer needs it.

**New crate** — create `crates/my-crate/`, add it to `members` in the root `Cargo.toml`, then add `my-crate = mkCrate "my-crate";` to `nix/packages.nix`.

**Native library** — add it to `buildInputs` in *both* `nix/packages.nix` and `nix/devshell.nix`. `pkg-config` is already present.

**New system** — add to `systems` in `flake.nix` (`x86_64-linux`, `aarch64-darwin` by default).

**Binary cache** — fill in `nixConfig.extra-substituters` and `extra-trusted-public-keys` in `flake.nix`.

## Build model

`buildDepsOnly` → `cargoArtifacts` (rebuilds only on `Cargo.lock` change) → `buildPackage` → binary. Source changes only rebuild the second derivation.
</content>
</invoke>
