# Autoresearch session — fastpass two-lane priority merge

## Objective

Grow `crates/fastpass/src/lib.rs`'s `Consumer::recv` dequeue policy until the
entire property suite in the `fastpass` crate passes. Start from the naive
user-biased stub; reach strict-priority-with-anti-starvation-aging (the gold
behaviour in `crates/fastpass-reference`).

## Metric

- **Name:** `tests_passing` — number of passing tests in the `fastpass` crate.
- **Direction:** maximize (target = every test green).
- **Command:** `.auto/measure.sh` (runs `cargo test -p fastpass`, sums passes).

A compile break yields 0 passing → the experiment auto-reverts. `.auto/checks.sh`
additionally blocks any `keep` that modified the protected oracle files or broke
the reference suite.

## Files in scope

- EDIT: `crates/fastpass/src/lib.rs` — only the `Consumer::recv` body and the
  `consec_control`/`aging_cap` bookkeeping it needs.
- READ (do not edit): `crates/fastpass-reference/src/lib.rs` for the target
  algorithm's shape, `crates/fastpass-testkit/src/lib.rs` for what each property
  asserts, `.plans/fastpass-twolane.md` for the step order.
- PROTECTED (never edit — checks.sh enforces): `crates/fastpass-testkit/**`,
  `crates/fastpass-reference/**`.

## Properties to earn (currently failing on the stub)

- P1 priority: control served before a full user backlog.
- Overtake: a control enqueued after a user runs first.
- P3 strong (anti-starvation): a waiting user served within `aging_cap` K
  controls under a control flood.

## Already passing on the stub (do not regress)

- P2 FIFO per lane, P3 basic progress, P4 no-loss, P5 wakeups, P6 drain,
  P7 zero-alloc send.

## What's been tried

(empty — first run)
