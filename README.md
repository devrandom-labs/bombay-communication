# fastpass

A generic **priority merge of two FIFO streams into one consumer** — the
standalone, transport-agnostic distillation of [bombay](../bombay) card #225
("control-signal lane"): runtime control signals must not queue behind a user
message backlog.

## The problem

One consumer, two producer lanes:

- a **control** lane (runtime signals: watch/unwatch/supervision) — unbounded,
  never blocks, rate-bounded by contract;
- a **user** lane (domain messages) — bounded, with backpressure.

We want *all* of these at once, with no residual downside:

| Property | Guarantee |
|---|---|
| P1 priority | control recv latency is independent of user-queue depth |
| P2 ordering | FIFO within each lane |
| P3 progress | user lane never starves — even under a control flood (aging cap) |
| P4 safety | no loss; every item delivered exactly once |
| P5 liveness | a parked consumer wakes on arrival in either lane (no lost wakeup) |
| P6 lifecycle | teardown `drain()` returns queued items on both lanes, in FIFO order |
| P7 hot path | steady-state `try_send` allocates zero times |

The single, deliberately-accepted relaxation: **there is no cross-lane total
order** — a control item may overtake an earlier user item. That *is* the
feature (cf. Erlang/OTP 28 EEP-76 "Priority Messages"). User FIFO-per-sender is
untouched.

## Public API

```rust
use fastpass::{channel, Config, Received};

let (ctl, usr, mut rx) = channel::<Ctl, Msg>(Config::new(1024).with_aging_cap(1024));
ctl.send(sig)?;              // never blocks
usr.send(msg).await?;        // bounded backpressure
usr.try_send(msg)?;          // non-blocking
match rx.recv().await {      // control-first
    Some(Received::Control(c)) => { /* ... */ }
    Some(Received::User(u))    => { /* ... */ }
    None => { /* both lanes closed and drained */ }
}
let leftover = rx.drain();   // Drained { control, user }, FIFO
```

## The "best possible outcome" algorithm

**Strict priority + anti-starvation aging.** Strict priority gives P1 exactly;
its only textbook downside — starving the user lane — is neutralised by an aging
cap `K`: after `K` consecutive control dequeues, one waiting user is forced
through. Because control is rate-bounded by contract, `K` is never reached in
normal operation (so P1 is exact), yet the cap bounds a user's worst-case wait
to `K` control items under an adversarial flood (so P3 is unconditional). Set
`aging_cap = 0` for pure strict priority.

Built on two flume channels + a biased `select!`, so the wakeup/disconnect
machinery (P5) is not hand-rolled (bombay ADR-0001).

## Layout

- `crates/fastpass` — the public crate; `Consumer::recv` is the policy under
  research.
- `crates/fastpass-reference` — the gold implementation (proves the suite is
  satisfiable; the benchmark baseline).
- `crates/fastpass-testkit` — the shared P1–P7 property suite, run against both.

## Test & research

```bash
cargo test                      # whole workspace (reference + target suites)
cargo test -p fastpass          # the target crate's suite
cargo bench -p fastpass         # P1 latency-under-backlog + drain throughput
```

The `Consumer::recv` policy is optimised via an autoresearch loop (see `.auto/`):
try an idea → `cargo test -p fastpass` → keep if more tests pass, revert if not
→ repeat. `.auto/checks.sh` forbids weakening the oracle. Once green, the
component plugs into bombay's mailbox: control lane carries
watch/unwatch/supervision, user lane carries messages and the in-band stop.

## License

MIT OR Apache-2.0.
