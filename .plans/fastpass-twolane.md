# fastpass — invent a fast two-lane priority-merge black box (bombay card #225)

## Context

A black box merging a **control** lane and a **user** lane into one consumer,
served control-first, that plugs into bombay's mailbox unchanged. The algorithm
question is settled (2-class strict priority + anti-starvation aging); the open
question is **mechanism**: how to implement the merge with the highest
throughput, the lowest control-latency-under-backlog, zero steady-state
allocation, and the fewest bug classes.

The shipped `crates/fastpass/src/lib.rs` (two flume channels + biased `select!`)
is the **baseline floor**. Beat it with a novel design.

## The contract (frozen conformance suite — must always hold)

- **P1** control served before a full user backlog (O(1), depth-independent).
- **P2** FIFO within each lane.
- **P3** progress + anti-starvation: a waiting user served within `aging_cap` K
  controls under a control flood.
- **P4** no loss (concurrent producers, exactly-once).
- **P5** no lost wakeups (parked consumer woken on either lane; recv is
  cancel-safe — droppable as one arm of a larger select).
- **P6** teardown drain returns queued items per lane, FIFO.
- **P7** zero-allocation steady-state user send.
- **Overtake** a control enqueued after a user may run first (accepted).
- Public API stable (the plug seam).

## Objective (autoresearch)

Maximize `SCORE = throughput / control_latency_ns` (`.auto/measure.sh`), subject
to the gate `.auto/checks.sh` (conformance green + zero-alloc + frozen
unchanged). Full freedom on internals and dependencies.

## Design directions (inspiration, not prescription)

1. Single lock-free MPSC + priority bit — one structure, one disconnect edge.
2. User ring buffer + fixed control sideband slot.
3. Eventcount / futex wakeup — one waiter, lost-wakeup impossible by construction.
4. Epoch / hazard-pointer reclamation for zero-alloc churn.
5. Single-allocation, cache-line-packed two-lane state.

## Verification

- `cargo run -p fastpass-perf --release` — the SCORE.
- `cargo test -p fastpass --tests` — conformance + zero-alloc.
- Do not run the full workspace test under a sandboxed shell (hangs); the loop
  drives these unsandboxed.

## Out of scope

- Editing frozen crates/tests (`fastpass-testkit`, `fastpass-reference`,
  `fastpass-perf`, `crates/fastpass/tests/**`).
- Changing the public API signatures.
- Weakening anti-starvation — it is part of the contract, not optional.
