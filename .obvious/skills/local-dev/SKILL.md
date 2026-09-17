---
name: local-dev
version: 1.0.0
description: Durable local-dev onboarding record for devrandom-labs/bombay-communication. How to bring the Rust workspace up from a bare Linux sandbox, the verified command set, and the snapshot state. Loaded by future workers instead of re-discovering setup.
category: autobuild
triggers:
  - local-dev
  - bombay-communication setup
  - rust workspace onboarding
author: obvious-team
created: 2026-09-17
---

## Prerequisites

- Linux x86_64; ~3 GB free disk (toolchain ≈ 1.3 GB + `target/` ≈ 1 GB); 8 cores / 8 GB RAM verified sufficient.
- Repo pins Rust **1.95.0** in `rust-toolchain.toml` (edition 2024). Any rustup-wrapped cargo invocation inside the repo auto-honors it.
- The onboarding sandbox image has **no Nix**. README's `nix develop` / `nix flake check` path only works on Nix hosts; the rustup path below is the verified equivalent. TODO(confirm): on a Nix host, prefer `nix develop` (adds bacon, cargo-nextest, cargo-deny, cargo-audit, taplo, rust-analyzer).

## Install commands

Exact sequence that produced `dev_stack_healthy: true` (2026-09-17):

```bash
curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal
source "$HOME/.cargo/env"
rustup toolchain install 1.95.0 --profile minimal --component rustfmt --component clippy --component llvm-tools-preview
cargo fetch                      # pre-download deps (Cargo.lock is committed)
cargo install taplo-cli --locked # optional — only for local TOML fmt check; CI covers it via nix
```

## Environment setup

- No `.env`, no env vars, no secrets, no DB/services/queues/ports. Pure library workspace.
- CI-only secrets (`CARGO_REGISTRY_SECRET`, `RELEASE_PLZ_APP_ID`, `RELEASE_PLZ_APP_PRIVATE_KEY`) are never needed locally — do not request them.
- `.envrc` contains `use flake` (direnv) — relevant only on Nix hosts; harmless elsewhere.

## Start commands

- There is no server. The dev loop is `cargo check` / `cargo test` (fast: warm full suite < 1 s).
- Runnable binary (primary CLI surface): `cargo run -p bombay-communication-perf --release` — prints the card-3 metric lines (`DIRECT_THROUGHPUT_OPS`, `ANCHOR_THROUGHPUT_OPS`, `ANCHOR_OVERHEAD_NS`, `CONTROL_LATENCY_NS`, `DRAIN_THROUGHPUT_OPS`, `SCORE`, `ANCHOR_CONTENTION_OPS`, `CLOSE_RACE_UPGRADE_OK`) and exits 0.
- API docs: `RUSTDOCFLAGS="--cfg docsrs" cargo doc -p bombay-communication --no-deps`.

## Primary flow (exercised 2026-09-17)

1. `cargo test --workspace --all-targets` — **77 passed, 0 failed** across `communication` (54: edge cases, lane lifecycle, mailbox retirement, property suite, proptest interleavings, teardown oracle, alloc/leak guards) and `communication-reference` (23).
2. README example run against the crate via a path dependency from a scratch crate — output `control: shutdown` then `message: work item`, demonstrating control-lane priority over an older queued user value.
3. `cargo run -p bombay-communication-perf --release` — two runs, consistent metrics (8-core sandbox): DIRECT ≈ 6.7–7.3e7 ops/s, ANCHOR ≈ 3.6–4.2e7, ANCHOR_OVERHEAD ≈ 20–26 ns, CONTROL_LATENCY ≈ 56–64 ns, DRAIN ≈ 5.3–5.7e7, SCORE ≈ 5.6e5–7.5e5.
4. Evidence logs at `/tmp/setup-evidence/` (outside the repo; intentionally not in the PR).

## Verification

- Typecheck: `cargo check --workspace --all-targets` — verified.
- Lint: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `taplo format --check $(git ls-files '*.toml')` — all verified clean.
- Test: `cargo test --workspace --all-targets` — verified.
- Scoped: `cargo check -p bombay-communication`, `cargo fmt --check -- <file>`, `cargo test -p bombay-communication --test edge_cases` — verified.
- Deep check (not in CI): `RUSTFLAGS="--cfg loom" cargo test -p bombay-communication --test loom` — 11 exhaustive model-check tests; 2 passed within ~10 min, the rest were still exploring interleavings at snapshot time. Run detached under tmux and expect long wall-time. TODO(confirm): full-suite duration on this hardware.

## Sandbox snapshot

- **Snapshot ID:** `iw59s5n1btb8i7q6gp1mi` — sandbox `cmp_Cqoj9SZL`, E2B template `rl8r48yfpik8ht918rvr:default`, captured 2026-09-17T15:27:54.769Z.
- Restores: Rust 1.95.0 toolchain (+fmt/clippy/llvm-tools), cargo registry cache, warm `target/` build cache for all four crates, `taplo` in `~/.cargo/bin`.

## Known blockers and notes

- **None fatal** — no typed blockers; `dev_stack_healthy: true`.
- `nix` unavailable in the sandbox image → rustup path used (above). `nix flake check` (the CI gate) runs on GitHub runners, not locally here.
- `cargo-audit` / `cargo-deny` (flake checks) not run locally — advisory-db/deny fetch not attempted during onboarding; CI covers both.
- Loom suite is long-running by design (exhaustive interleaving exploration); it is neither in CI nor in the README's canonical commands.
