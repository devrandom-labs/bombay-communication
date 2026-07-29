# Autoresearch session — harden the lock-free two-lane merge (keep the speed)

## What we want (read first)

The current lock-free design in `crates/fastpass/src/lib.rs` (Vyukov user ring +
lock-free control block-chain + `Notify` eventcount) is FAST and we want to KEEP
it. This run is not a redesign — it is a **hardening** run: remove the two
soundness doubts without losing throughput.

Two doubts, now encoded as hard GATES that revert any experiment that fails:

1. **Memory leak (P8).** The control block-chain never frees consumed blocks —
   live allocations grow ~1 per 64 control messages *ever sent*. Fix it: free
   (or recycle) a block once the consumer has passed it, or otherwise bound the
   lane. Gate: `tests/leak.rs` (`lanes_do_not_leak`) must go green.
2. **Wakeup soundness (loom).** The producer publishes with `Release` then reads
   the `parked`/`waiting` flag — on TSO/weak memory this store→load pair is not
   fenced, so the Dekker/store-buffering "both miss" outcome (a lost wakeup at
   quiescence/shutdown) is not proven impossible. Prove it with **loom**.

## Objective

- **KEEP MAXIMIZING** the composite `SCORE` (`.auto/measure.sh`, throughput ÷
  control-latency). Do not regress it materially — the baseline is ~13M.
- **HARD GATES** (`.auto/checks.sh` reverts any experiment that breaks one):
  1. Conformance P1–P7 + edge + proptest green.
  2. Zero-allocation steady-state send (`tests/alloc.rs`).
  3. **No leak** (`tests/leak.rs`) — currently RED, fix it.
  4. **loom model-check** of the wakeup protocol — currently ABSENT, add it.
  5. Frozen surfaces unchanged.

## The loom lane you must add (`crates/fastpass/tests/loom.rs`)

loom cannot drive the async `recv` (tokio `Notify` is not loom-instrumented),
so test the ATOMIC PROTOCOL, not the async wrapper:

- Add `loom` under `[target.'cfg(loom)'.dependencies]` in
  `crates/fastpass/Cargo.toml`.
- Swap `std::sync::atomic` → `loom::sync::atomic` (and `Arc`) under
  `#[cfg(loom)]`; keep `std` on `#[cfg(not(loom))]`. Gate the tokio-`Notify`
  async paths behind `#[cfg(not(loom))]`, and provide a `#[cfg(loom)]` sync
  park/wake (e.g. `loom::sync::Condvar` or thread park) driven by the SAME
  `parked`/`waiting` flag protocol.
- `tests/loom.rs` (a `#[cfg(loom)]` `loom::model(...)`): **2 producers + 1
  consumer**, each producer sends a bounded number of items (e.g. 2), the
  consumer drains until closed. Assert, on every interleaving loom explores:
  **termination (no lost wakeup / no hang)**, **no loss**, **no duplication**,
  and **per-lane FIFO**. Cover BOTH the consumer wakeup and the user-lane
  backpressure wakeup.
- The gate runs it as `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test
  -p fastpass --test loom --release`. If loom finds the store-buffering bug,
  the fix is typically a `fence(SeqCst)` between publish and the flag load, or
  making the flag read an RMW — apply the minimal fix that makes loom pass.

## Files

- **EDIT freely:** `crates/fastpass/src/**`, `crates/fastpass/Cargo.toml`
  (add `loom`, recycling deps, etc.), and CREATE `crates/fastpass/tests/loom.rs`.
- **KEEP STABLE:** the public API of `crates/fastpass` (the plug seam).
- **FROZEN — never edit:** `crates/fastpass-testkit/**`,
  `crates/fastpass-reference/**`, `crates/fastpass-perf/**`, and the conformance
  test files (`tests/property_suite.rs`, `tests/edge_cases.rs`,
  `tests/proptest_interleavings.rs`, `tests/alloc.rs`, `tests/leak.rs`).

## Metric / gate

- Metric: `.auto/measure.sh` → `METRIC score=<n>` (keep it high).
- Gate: `.auto/checks.sh` → must print `CHECK OK` (conformance + alloc + leak +
  loom + frozen). It starts RED (leak fails, loom absent) — turning it green
  without wrecking the score IS the task.

## What's been tried

Previous run built the lock-free ring + control block-chain + eventcount
(score ~13–15M, ~210M ops/s, ~15ns control latency). This run keeps that
mechanism and closes the leak + proves the wakeup.
