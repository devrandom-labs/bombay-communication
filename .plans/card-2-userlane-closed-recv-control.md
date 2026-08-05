# Card 2 — fastpass asks: `Received::UserLaneClosed` + `recv_control`

> **Context:** approved in
> `~/Code/devrandom/actorpass/docs/2026-08-04-actorpass-create-at-address-design.md`
> (fork rulings C and D; the coverage audit). These are the TWO driver-needs
> changes actorpass's spine requires. Work in
> `~/Code/devrandom/fastpass`. The broader interface distillation (renames,
> `Config` reshaping, weak senders, slot budget) is QUEUED separately —
> this card is deliberately minimal.

## Why (the two call sites, so the semantics can't drift)

1. **Drain-stop observability (fork C).** actorpass collects an actor when
   its last user handle drops (bombay ADR-0010). Today `recv` returns
   `None` only when BOTH lanes are closed and drained — but the child's
   supervisor holds a `ControlSender`, pinning the merged `recv` open
   forever. The user lane's death is unobservable. The fix is a one-shot
   leg telling the consumer the user lane is terminally closed.
2. **Parked-restart reads (fork D).** A crashed child keeps its mailbox
   (keep-address) and parks awaiting `Control::Restart` or teardown. While
   parked it must read the control lane WITHOUT consuming user items (a
   local stash would defeat the bounded ring's backpressure). The fix is a
   control-only receive.

## Non-negotiable constraints

1. **Additive semantics; no regression to the eight properties** (P1–P8:
   control-first priority, per-lane FIFO, no loss, no lost wakeup, clean
   teardown drain, zero-alloc steady state, no user starvation, no leak).
2. The `Received` enum stays `Copy + PartialEq + Eq` (the new leg is a
   unit variant).
3. The wakeup protocol changes NOTHING: one shared `Notify` gated by
   `parked`; registration enable-then-recheck; a spurious wake costs one
   extra loop turn and is always acceptable; lost wakeup is never
   acceptable.
4. Both the async path AND the `cfg(loom)` blocking twin are implemented;
   the loom model in `crates/fastpass/tests/loom.rs` gains coverage for
   both additions.
5. **Frozen surfaces thaw deliberately, once:** `.auto/checks.sh` freezes
   the testkit, the reference, and the conformance test files — this card
   edits them WITH user approval, then re-points `.auto/BASELINE`. No
   other frozen edits.
6. clippy law as in the workspace (`all` deny + `pedantic` warn): zero
   errors, zero new warnings. No new dependencies. No new `unsafe` (the
   additions are consumer-side logic over existing lane primitives).

## Target files

- `crates/fastpass/src/lib.rs` — the two additions (below).
- `crates/fastpass/tests/loom.rs` — loom model coverage for both.
- `crates/fastpass/tests/lane_lifecycle.rs` — NEW file (not in the FROZEN
  list): the SUT-level conformance tests for both additions.
- `crates/fastpass-reference/src/lib.rs` — mirror BOTH additions (the
  reference runs the same property suite; it must expose the same API).
- `crates/fastpass-testkit/src/lib.rs` — extend the property suite with
  the new invariants (below); the suite then pins them for BOTH
  implementations.
- `.auto/BASELINE` — re-point after green (final step).

**Non-goals:** renames/distillation (queued); weak senders (queued);
slot-size budget (queued); `try_recv_control` (no caller); changing
`recv`'s priority/aging policy; touching `Drained`.

## Change 1 — `Received::UserLaneClosed` (fork C)

```rust
pub enum Received<C, U> {
    /// A control-lane item.
    Control(C),
    /// A user-lane item.
    User(U),
    /// The user lane is TERMINALLY closed: every `UserSender` is gone and
    /// the ring is drained. Delivered EXACTLY ONCE, in the user stream's
    /// FIFO position (after the last user item), subject to the same
    /// control-first priority and aging as a user item. Terminal by
    /// construction: no `UserSender` source exists besides `channel()`
    /// and `Clone`, so a zero count can never rise again.
    UserLaneClosed,
}
```

Implementation in `Consumer`:

- One new field, `usr_closed_reported: bool` (plain consumer-side latch —
  the consumer is single-owner; no atomics).
- In `recv`'s loop, after the control pop and before/around the user pop:
  when `self.usr.closed() && !self.usr_closed_reported` and the ring pops
  empty, deliver `UserLaneClosed` through the SAME path a user item would
  take (it resets the aging streak like a user item — it IS the user
  stream's end-marker), set the latch, and continue. Control items keep
  their priority over it; aging forces it through exactly as for users.
- The leg must be observable from the PARKED path too: the last-sender
  drop already calls `wake_consumer()` (see `UserSender::drop`), so the
  existing enable-then-recheck protocol covers it — verify with a test
  that parks `recv` on an empty channel and then drops the last
  `UserSender` while a `ControlSender` lives: `recv` must wake and return
  `UserLaneClosed`, NOT hang and NOT return `None`.
- `recv`'s `None` condition is unchanged: both lanes closed and drained
  (the leg having been delivered already if the user lane died first).
- Mirror in `recv_blocking` (the `cfg(loom)` twin) with the same latch.

## Change 2 — `recv_control` (fork D)

```rust
/// Receive the next CONTROL item only, never consuming the user lane.
/// `None` once the control lane is closed and drained. Cancel-safe under
/// the same registration protocol as `recv`; a user-lane push may cause a
/// spurious wake (one extra loop turn), never a lost wakeup and never a
/// consumed user item.
#[cfg(not(loom))]
pub async fn recv_control(&mut self) -> Option<C>;
/// The `cfg(loom)` blocking twin (drives the loom model).
#[cfg(loom)]
#[doc(hidden)]
pub fn recv_control_blocking(&mut self) -> Option<C>;
```

Implementation notes:

- Mirror `recv`'s loop MINUS all user-lane interaction: pop control; if
  empty and `self.ctl.closed()`, apply the SAME double-sweep discipline
  `recv` documents for the both-closed path (departing control senders'
  publishes), then `None`; otherwise park with the identical
  enable-then-recheck `parked` protocol and loop.
- Do NOT call `usr.release_one_waiter()` from this path: it consumes no
  user items, so releasing a producer parked on a full ring would just
  re-park it — and the semantics WANT user producers to stay parked while
  the process is parked-for-restart (that is the backpressure).
- The aging streak (`consec_control`) is untouched by this path.

## Change 3 — the reference twin + testkit properties

`fastpass-reference`: mirror both additions with its simple internals
(mutex/vec — semantics identical, trivially).

New testkit properties (pinned against BOTH implementations):

1. `UserLaneClosed` fires AT MOST once per channel.
2. It fires only after every sent user item was received (no user item
   ever appears after it).
3. It never fires while any `UserSender` (or clone) is alive.
4. After it fires, `recv` keeps serving control items; `None` only after
   the control lane is also closed and drained.
5. `recv_control` returns control items in ticket (FIFO) order and never
   reduces the count of user items the consumer later observes.
6. `recv_control` returns `None` only when the control lane is closed and
   drained (drop the last `ControlSender` with an empty queue → `None`).

New SUT conformance tests (`tests/lane_lifecycle.rs`): the parked-`recv`
last-sender-drop wake case above; `recv_control` with a full user ring
(user items untouched, control flows); parked-for-restart scenario shape
(drop all `UserSender`s, read leg, send `Control`, read it via
`recv_control`, drop `ControlSender`, get `None`).

## Acceptance (all must hold)

1. `cargo test --workspace --tests --no-fail-fast` green (all four
   crates), including the NEW tests and properties.
2. `.auto/checks.sh` gate 3 equivalent, green:
   `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test -p fastpass --test loom --release`
   with the new model cases (the leg's latch + `recv_control`'s parking
   under interleavings).
3. `cargo clippy --workspace --all-targets`: zero errors, zero new
   warnings.
4. Zero-alloc guard (`tests/alloc.rs`) and no-leak guard (`tests/leak.rs`)
   still green — the new paths allocate nothing.
5. **Mutation probes**: (a) drop the latch → "fires at most once" fails;
   (b) fire the leg with a non-empty ring → "only after all user items"
   fails; (c) make `recv_control` pop one user item → the user-count
   property fails. Flip, watch fail, revert, tabulate.
6. Re-point `.auto/BASELINE` to the change HEAD; run `.auto/checks.sh`;
   `CHECK OK`. Note the `.auto/measure.sh` score before/after in the
   report (benches must still build: `cargo build --benches`).

## Report back

- Per-file diff summary.
- Mutation-probe table.
- measure.sh score before/after.
- Divergences from this card (quote the card text, state what the code
  forced) — never silent improvisation.
