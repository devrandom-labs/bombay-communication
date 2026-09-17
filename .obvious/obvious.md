# devrandom-labs/bombay-communication

Priority-aware, bounded communication channels for Rust: an unbounded control lane plus a bounded user lane behind one consumer — FIFO per lane, aging cap against user starvation, ownership-preserving shutdown drain, zero-alloc steady-state sends.

**Stack:** Rust workspace, edition 2024, pinned toolchain **1.95.0** (`rust-toolchain.toml`; the Nix flake pins the same via fenix). Package manager: Cargo (`Cargo.lock` committed). Published crate: `bombay-communication` v0.1.2 (import name `communication`); the other three workspace members are unpublished support packages. Dev tooling model mirrors Nexus: `nix develop` / `nix flake check` on Nix hosts; rustup honors the pinned toolchain elsewhere.

**Key commands** (all verified in this sandbox 2026-09-17 — see Local Verification):
- Check: `cargo check --workspace --all-targets`
- Test: `cargo test --workspace --all-targets`
- Lint: `cargo fmt --all --check` · `cargo clippy --workspace --all-targets -- -D warnings` · `taplo format --check $(git ls-files '*.toml')`
- Docs: `RUSTDOCFLAGS="--cfg docsrs" cargo doc -p bombay-communication --no-deps`
- Perf harness: `cargo run -p bombay-communication-perf --release`

No services, ports, env vars, or secrets — this is a pure library workspace; there is no server to start.

## Codebase Map

See `.obvious/codebase-map.md`.

## Repo Guidance

- Only `crates/communication` is published (as `bombay-communication`). `communication-reference`, `communication-testkit`, and `communication-perf` are `publish = false` support packages.
- The public API in `crates/communication/src/lib.rs` is FIXED — the shared testkit property suite depends on those exact names and signatures. Do not rename public items casually.
- There is intentionally NO total order across lanes: a control value may overtake an older user value. This is a design guarantee, not a bug — do not "fix" it.
- Workspace lints: `unsafe_op_in_unsafe_fn` is denied; `cfg(loom)` is a registered cfg (`unexpected_cfgs` check-cfg), so loom-gated code is expected and must compile under `RUSTFLAGS="--cfg loom"`.
- Versioning and `crates/communication/CHANGELOG.md` are owned by release-plz (release PRs on main). Do not hand-bump versions.
- CI gate is `nix flake check` (crane build, cargo-nextest, cargo doc, cargo fmt, taplo fmt, cargo audit, cargo deny) — run on every PR to `main`. The `docs.yml` workflow deploys rustdoc to GitHub Pages on main.
- Guidance docs present: `README.md` only (no AGENTS.md / CONTRIBUTING.md / CLAUDE.md at time of onboarding).
- No `.env.example`, no required env vars, no external services. CI-only secrets (`CARGO_REGISTRY_SECRET`, `RELEASE_PLZ_APP_ID`, `RELEASE_PLZ_APP_PRIVATE_KEY`) are never needed for local dev.

## Local Verification

Verified on this sandbox 2026-09-17 with Rust 1.95.0 installed via rustup (the sandbox image has no Nix; see `.obvious/skills/local-dev/SKILL.md`).

<!-- local-verification-summary:v1 -->
- **Typecheck command:** `cargo check --workspace --all-targets`
- **Lint command:** `cargo fmt --all --check`
- **Test command:** `cargo test --workspace --all-targets`
- **Scoped typecheck:** `cargo check -p bombay-communication`
- **Scoped lint:** `cargo fmt --check -- crates/communication/src/lib.rs`
- **Scoped test:** `cargo test -p bombay-communication --test edge_cases`
- **Full-repo check safe:** yes — cold build + full suite ≈ 2 min; warm re-run < 1 s on 8 cores (measured 0.9 s)
- **Scoped alternatives discovered:** yes
<!-- /local-verification-summary -->

Additional verified commands:
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `taplo format --check $(git ls-files '*.toml')` — clean (taplo 0.10.0, honors repo `taplo.toml`).
- `RUSTDOCFLAGS="--cfg docsrs" cargo doc -p bombay-communication --no-deps` — docs build (matches `docs.yml`).
- `cargo run -p bombay-communication-perf --release` — perf harness exits 0 with full metric output.
- README example (scratch crate depending on the crate by path): prints `control: shutdown` then `message: work item` — control-lane priority over an older queued user value.
- `RUSTFLAGS="--cfg loom" cargo test -p bombay-communication --test loom` — exhaustive model-check suite, NOT in CI; long-running (see skill notes).

<!-- validation-summary:v1 -->
- **Last validated:** 2026-09-17T15:28:00Z
- **Result:** pass
- **Verified commands:**
  - `cargo test --workspace --all-targets` — verified (77 passed, 0 failed)
  - `cargo check --workspace --all-targets` — verified
  - `cargo fmt --all --check` — verified
  - `cargo clippy --workspace --all-targets -- -D warnings` — verified
  - `taplo format --check $(git ls-files '*.toml')` — verified
  - `RUSTDOCFLAGS="--cfg docsrs" cargo doc -p bombay-communication --no-deps` — verified
  - `cargo run -p bombay-communication-perf --release` — verified
- **Blockers encountered:** none
<!-- /validation-summary -->

## Sandbox Snapshot

- **Snapshot ID:** `iw59s5n1btb8i7q6gp1mi` — sandbox `cmp_Cqoj9SZL` (devrandom-labs/bombay-communication), E2B template `rl8r48yfpik8ht918rvr:default`
- **Captured:** 2026-09-17T15:27:54.769Z (ISO-8601)
- **State:** `dev_stack_healthy: true` — rustup 1.29.1 with pinned Rust 1.95.0 toolchain (cargo, rustc, rust-std, rustfmt, clippy, llvm-tools-preview), cargo registry cache, warm `target/` build cache for all four workspace crates, `taplo` 0.10.0 in `~/.cargo/bin`.
- **Evidence logs** (outside the repo): `/tmp/setup-evidence/` — workspace test log, lint log, perf+doc log, loom log.

*Contract generated by the autobuild-setup v4.2.1 onboarding procedure.*
