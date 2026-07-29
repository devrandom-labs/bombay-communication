# Autoresearch session — invent a fast, novel two-lane priority-merge black box

## What we want (read this first)

A **black box** that merges a **control** lane and a **user** lane into one
consumer, plugs into bombay's mailbox unchanged, and is **as fast and as
allocation-free as possible** — beating the naive two-channel merge that ships
today. The goal is a *novel mechanism*, not the obvious one.

You OWN the internals. Invent. The two-flume-channel + biased-select design in
`crates/fastpass/src/lib.rs` right now is a **BASELINE FLOOR to beat**, not the
intended answer. A ground-up redesign is encouraged over micro-tweaks.

## Objective

- **MAXIMIZE** the composite perf **SCORE** = throughput ÷ control-latency-under-backlog
  (`.auto/measure.sh` → `crates/fastpass-perf`). Higher throughput and/or lower
  control latency both raise it.
- **HARD GATES** (`.auto/checks.sh` reverts any experiment that breaks one):
  1. The full conformance suite stays green (P1–P7 + edge + proptest).
  2. The steady-state user send stays **zero-allocation**.
  3. Frozen surfaces stay byte-identical (see below).

## Full freedom

Replace anything behind the public API. Drop flume. Drop tokio internals. Add
any dependency to `crates/fastpass/Cargo.toml`. Candidate directions — as
inspiration, pick/combine/ignore:

- **Single lock-free MPSC with an embedded priority bit** — one structure, one
  disconnect edge (kills the two-channel double-close bug class), control
  dequeued by a priority flag rather than a second queue.
- **Ring buffer + control sideband** — a fixed-size control staging slot beside
  a user ring, so the hot user path is a bare ring and control still jumps ahead
  (the ring-can't-reorder problem, solved by not putting control in the ring).
- **Eventcount / futex-style wakeup** (folly-style) instead of per-lane channel
  wakers — makes the lost-wakeup class *structurally* impossible, one waiter.
- **Epoch / hazard-pointer reclamation** — zero-alloc steady state under churn.
- **Single-allocation two-lane state** — both lanes' hot fields in one cache
  line, fewer atomics, better locality.

Anti-starvation aging (a waiting user forced through after K consecutive
controls) is part of the CONTRACT (the conformance suite tests it) — carry it
into whatever design you build.

## Files

- **EDIT freely:** `crates/fastpass/src/**` (all internals) and
  `crates/fastpass/Cargo.toml` (new deps welcome).
- **KEEP STABLE:** the public API of `crates/fastpass` — `channel`, `Config`,
  `Received`, `Drained`, `ControlSender`, `UserSender`, `Consumer`, the error
  types, and their signatures. It is the plug seam into bombay; the frozen
  conformance tests will not compile if you change it, and the gate fails.
- **FROZEN — never edit (checks.sh reverts you):**
  `crates/fastpass-testkit/**`, `crates/fastpass-reference/**`,
  `crates/fastpass-perf/**`, `crates/fastpass/tests/**`.

## Metric / gate commands

- Metric: `.auto/measure.sh` → `METRIC score=<n>`.
- Gate: `.auto/checks.sh` → must print `CHECK OK`.

## What's been tried

(empty — first perf-objective run; the baseline is the shipped two-channel merge)
