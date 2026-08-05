# Autoresearch — actorpass-compatible user anchoring

Read `.plans/card-3-user-anchor-actorpass.md` completely before changing code.
The card is the semantic contract; this file defines the iterative research
loop. The current Vyukov user ring, lock-free control block chain, and shared
Notify protocol are the retained baseline. Change them only when a measured
hypothesis requires it and every correctness gate remains green.

## Objective

Add the non-owning `UserAnchor<U>` capability required by actorpass, then
maximize the actorpass-shaped composite score reported by `.auto/measure.sh`.
An address table holds an anchor without keeping the user lane alive. Each
anchor delivery must first atomically acquire a temporary live `UserSender`;
therefore a delivery racing the last real sender linearizes entirely before
lane closure or fails with its payload entirely after closure.

Do not optimize only the new path. Direct user throughput, control latency
under a saturated user backlog, drain throughput, allocation behavior, and
memory reclamation remain first-class constraints.

## Required public interface

```rust
pub struct UserAnchor<U> { /* private */ }

impl<U> Clone for UserAnchor<U>;

impl<U> UserSender<U> {
    pub fn anchor(&self) -> UserAnchor<U>;
}

impl<U> UserAnchor<U> {
    pub fn upgrade(&self) -> Option<UserSender<U>>;
    pub async fn send(&self, item: U) -> Result<(), UserClosed<U>>;
    pub fn try_send(&self, item: U) -> Result<(), TrySendError<U>>;
}
```

Names and signatures are frozen after the contract-landing iteration. Do not
change `channel`'s existing three-value return shape. `anchor()` is additive
and lets actorpass create the address-table endpoint from its initial counting
sender.

## Semantic gates

All existing P1–P8 and card-2 lifecycle semantics remain unchanged. Add and
freeze tests proving:

1. Cloned anchors do not delay `UserLaneClosed` or final `recv() -> None`.
2. An anchor upgrades and sends while at least one counting sender is live.
3. After the last counting sender drops, upgrade returns `None`; `send` and
   `try_send` return the original payload as closed.
4. A last-sender-drop racing anchor delivery has exactly two legal outcomes:
   the item precedes `UserLaneClosed`, or delivery fails and no item appears.
   An item after the marker is forbidden.
5. A blocked anchor `send` counts as live until it completes or is cancelled;
   closure cannot overtake an acquired delivery.
6. Cancelling a blocked anchor send releases its temporary liveness count.
7. Anchors fail once the consumer is dropped and never resurrect a lane.
8. Anchor steady-state `try_send` is allocation-free.
9. Reference and optimized implementations pass the same property suite.
10. Loom covers upgrade-vs-last-drop, blocked-send cancellation, and the
    marker ordering race over the real counter protocol.

Mutation probes for the contract-landing iteration:

- replace the nonzero conditional increment with load-then-increment: the race
  model must fail;
- make an anchor increment permanent: the closure test must hang/fail;
- decrement temporary liveness before publication: marker ordering must fail;
- let upgrade succeed from zero: post-closure resurrection must fail.

## Contract landing, once

The first coherent iteration may deliberately thaw the reference, testkit,
semantic tests, Loom model, and perf harness to land this contract. Implement
the simple reference twin first, add the frozen oracle, then implement the
optimized crate. Run the mutation probes, commit the coherent contract, write
that commit to `.auto/BASELINE`, and record its stable AC-powered performance
in `.auto/PERF_BASELINE`. From that point onward those surfaces are frozen and
only the optimized implementation and dependency manifest may change.

## Iterative optimization cycle

For each experiment:

1. State one concrete bottleneck and predicted affected metric.
2. Make one mechanism-level change in `crates/fastpass/src/**` (and its
   manifest only when needed).
3. Run `.auto/checks.sh`. Any failure means revert the experiment.
4. Run `.auto/measure.sh` on AC power with no competing benchmark process.
5. Keep only a repeatable score improvement that respects every per-metric
   floor. Re-run candidates interleaved A/B when the gain is below 3%.
6. Record kept and rejected results, including raw metrics and machine state,
   in the research log. A rejected experiment is still a result.

Never weaken a test, shrink a workload, loosen a threshold, change the score
formula, or re-point either baseline to make an experiment pass.

## Completion

Finish only when the API and all mutation probes are pinned, `.auto/checks.sh`
prints `CHECK OK`, deep Loom and Miri are green, and three stable measurements
show no remaining low-risk improvement. Report the final and baseline metrics,
all rejected mechanisms, allocations, retained memory, and actorpass wiring.
