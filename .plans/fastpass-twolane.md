# fastpass — two-lane priority merge (bombay card #225)

## Context

One consumer, two FIFO producer lanes — **control** (runtime signals) and
**user** (domain messages). The consumer must serve control ahead of a user
backlog while keeping every other good property. This crate is the standalone,
transport-agnostic distillation so it can be tested exhaustively, then plugged
into bombay's mailbox.

The public API in `crates/fastpass/src/lib.rs` is FIXED (the `fastpass-testkit`
property suite depends on the exact names/signatures). The ONLY thing you change
is the dequeue policy in `Consumer::recv` (and the counter it uses). Ship the
naive stub → grow it to full green.

Invariants the final `recv` must satisfy (each has a test in the suite):

- **P1** priority — control served before a full user backlog (no head-of-line block).
- **P2** FIFO within each lane.
- **P3** progress — users flow when control is idle; and (strong) a waiting
  user is served within `aging_cap` K control dequeues even under a control flood.
- **P4** no loss — concurrent producers, every item delivered exactly once.
- **P5** no lost wakeups — a parked consumer wakes on either lane.
- **P6** teardown — `drain()` returns queued items on both lanes in FIFO order.
- **P7** hot path — a steady-state `try_send` allocates zero times.
- **Overtake** — a control enqueued after a user MAY run first (accepted relaxation).

The target algorithm (see the gold `crates/fastpass-reference`, which you must
NOT read-copy wholesale but may consult for shape): **strict priority + an
anti-starvation aging cap**. After K consecutive control dequeues, force one
waiting user through; K is never reached in normal (rate-bounded) operation, so
P1 holds exactly, and P3 holds unconditionally under flood.

## Steps

1. **P1 + overtake** — make `recv` prefer control: `try_recv` control before
   user, and in the parked `select!` put the control arm first under `biased`.
   Expected: `p1_*`, `overtake_*` pass; `p2/p3/p4/p5/p6` stay green.
   Verify: `cargo test -p fastpass p1 overtake`.
2. **Anti-starvation** — use `aging_cap` + `consec_control`: after K controls,
   serve one waiting user, reset the counter. Increment only while below the cap
   so it cannot overflow (no bare/`saturating` arithmetic on the count path).
   Expected: `anti_starvation_*` passes; nothing regresses.
   Verify: `cargo test -p fastpass anti_starvation`.
3. **P7** — ensure the steady-state `try_send` path allocates nothing (it should
   already, via flume's bounded ring; do not add per-send allocation).
   Verify: `cargo test -p fastpass --test alloc`.

SEQUENTIAL — all three steps edit the same file (`crates/fastpass/src/lib.rs`);
no parallel fan-out.

## Verification

- `cargo build -p fastpass` (compile gate — a break scores 0 and auto-reverts).
- `cargo test -p fastpass` — all property + alloc tests green.
- `cargo test -p fastpass-reference` — reference stays green (oracle integrity).
- Do NOT run the full workspace test under a sandboxed shell (it can hang); the
  autoresearch loop drives these itself, unsandboxed.

## Out of scope

- `crates/fastpass-testkit/**` and `crates/fastpass-reference/**` — PROTECTED.
  Editing them fails `.auto/checks.sh`. The tests are the oracle; do not weaken
  them to pass.
- The public API surface (types, fn names, signatures) — fixed.
- Benches (`crates/fastpass/benches/`) — optional to touch; correctness first.
