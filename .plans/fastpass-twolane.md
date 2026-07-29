# fastpass — harden the lock-free two-lane merge (bombay card #225)

## Context

The lock-free design (Vyukov user ring + lock-free control block-chain +
`Notify` eventcount) is fast (~13–15M score) and is being KEPT. This run closes
the two soundness gaps that block plugging it into bombay, without regressing
throughput.

## The contract (frozen — must always hold)

P1 priority · P2 per-lane FIFO · P3 progress + anti-starvation · P4 no loss ·
P5 no lost wakeups · P6 teardown drain · P7 zero-alloc send · overtake · stable
public API. NEW: **P8 no memory leak** (`tests/leak.rs`).

## This run's two tasks

1. **Fix the control-lane leak (P8).** Consumed blocks in the control chain are
   never freed. Recycle or free a block once the consumer's cursor has passed
   its last slot (single-consumer, so reclamation is straightforward), or bound
   the lane. Must keep zero-alloc steady state (P7) and not regress score.
   Verify: `cargo test -p fastpass --test leak`.
2. **Prove the wakeup with loom.** Add `crates/fastpass/tests/loom.rs` per the
   prompt: cfg(loom) atomic swap + a sync 2-producer/1-consumer harness that
   loom explores exhaustively, asserting termination + no-loss + FIFO on both
   wakeup paths. Fix any store-buffering hole loom finds (likely a `fence(SeqCst)`
   between publish and the flag load).
   Verify: `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test -p fastpass --test loom --release`.

SEQUENTIAL — both tasks edit `crates/fastpass/src/**`.

## Objective (autoresearch)

Keep maximizing `SCORE` (`.auto/measure.sh`), subject to `.auto/checks.sh`
(conformance + zero-alloc + **no-leak** + **loom** + frozen). The gate starts
RED; turning it green while holding the score is the win.

## Optional cleanups (only if free)

- Stale module doc says the control lane is a `Mutex<VecDeque>` — it is the
  lock-free chain now; correct it.
- Document that `Config::new(n)` capacity is a *minimum* (rounded up to a power
  of two, floor 2), so the semantics are explicit rather than surprising.

## Out of scope

- Editing frozen crates/tests, or changing the public API signatures.
- Throwing away the lock-free mechanism — this run hardens it, not replaces it.
