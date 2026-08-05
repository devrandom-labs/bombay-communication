# Card 3 — `UserAnchor`: address-table delivery without liveness ownership

## Integration problem

Actorpass gives edge callers counting `UserSender<Envelope>` handles and stores
a typed delivery endpoint in addresspass. Storing a normal sender in the
address table would keep the lane open forever, preventing reference-driven
drain-stop. Storing no endpoint would make pure address-emitted sends
unresolvable. Fastpass therefore supplies a weak delivery capability:
`UserAnchor<U>`.

```text
actorpass Handle          -> UserSender<Envelope>  (counts as ownership)
addresspass endpoint      -> UserAnchor<Envelope>  (does not count)
actor driver              -> Consumer<Control, Envelope>
parent child table        -> ControlSender<Control> plus deliberate user owner
```

The anchor is local mailbox plumbing. It does not know addresses, actors,
behaviors, supervision, or addresspass.

## Required interface

```rust
pub struct UserAnchor<U> { /* private lane Arc */ }

impl<U> Clone for UserAnchor<U>;

impl<U> UserSender<U> {
    #[must_use]
    pub fn anchor(&self) -> UserAnchor<U>;
}

impl<U> UserAnchor<U> {
    #[must_use]
    pub fn upgrade(&self) -> Option<UserSender<U>>;

    pub async fn send(&self, item: U) -> Result<(), UserClosed<U>>;

    pub fn try_send(&self, item: U) -> Result<(), TrySendError<U>>;
}
```

`channel()` remains `(ControlSender<C>, UserSender<U>, Consumer<C, U>)`.
Actorpass derives the anchor from the initial user sender before handing out or
moving that sender.

## Load-bearing implementation rule

`upgrade` must conditionally increment the live-sender count only while it is
nonzero, using one atomic read-modify-write loop (`fetch_update` or equivalent).
It must never increment zero. A separate `closed` flag is unnecessary because
zero is terminal: no constructor or clone may recreate a counting sender.

Anchor `send` and `try_send` compose through `upgrade` and the existing sender
operations. The temporary sender remains alive across a blocked async send and
is dropped on success, error, or future cancellation. This gives one
linearization rule:

```text
upgrade wins first    -> delivery owns liveness; item is before closure marker
last sender hits zero -> upgrade fails; payload is returned; no resurrection
```

A load-then-increment implementation is forbidden: last-drop can reach zero
between the two operations and the anchor could publish after
`UserLaneClosed`.

## Actorpass wiring

At birth actorpass should:

1. call `fastpass::channel`;
2. derive `let anchor = user_sender.anchor()`;
3. claim the derived actor address in addresspass with the typed anchor;
4. retain/count the appropriate edge or parent user ownership;
5. spawn the process with the consumer;
6. resolve address-emitted sends to anchors, then await `anchor.send(envelope)`.

Dropping the last real handle makes the count zero even though addresspass
still holds the anchor. The consumer drains already-linearized messages,
emits `Received::UserLaneClosed` exactly once, and the actor can terminate.
Address retirement then drops the anchor; its presence never affected closure.

## Correctness suite

Add shared reference/SUT properties and focused optimized tests for:

- anchor clone/drop neutrality;
- live upgrade and delivery;
- post-close failure with payload preservation;
- consumer-drop failure;
- upgrade racing last sender drop;
- blocked delivery racing last sender drop;
- cancellation of a blocked anchor send;
- no user item after `UserLaneClosed`;
- anchor plus control-lane operation after user closure;
- zero-allocation anchor `try_send` steady state;
- no retained allocations after anchors, senders, and consumer drop.

Loom must model the production conditional-increment protocol, not a simplified
stand-in. Proptest should generate anchor/send/drop/recv schedules and compare
them with a small state machine whose user-lane states are Open(count>0),
Closing(in-flight temporary owners), and Closed(0, terminal).

## Performance workload

Extend `fastpass-perf` to report, on one line each:

```text
DIRECT_THROUGHPUT_OPS=<f64>
ANCHOR_THROUGHPUT_OPS=<f64>
ANCHOR_OVERHEAD_NS=<f64>
CONTROL_LATENCY_NS=<f64>
DRAIN_THROUGHPUT_OPS=<f64>
SCORE=<f64>
```

Measurements:

- direct pipeline: existing `UserSender::try_send` + receive workload;
- anchor pipeline: address-table-shaped `anchor.try_send` + receive workload;
- anchor contention: 1/2/4/8 producer anchors to one consumer;
- close race: repeated upgrade versus last-sender drop, reported separately
  and excluded from the scalar score;
- saturated user ring plus control delivery latency;
- mixed 90% user / 10% control drain throughput;
- allocation and retained-block counts remain correctness gates, not score
  inputs.

Use fixed workloads, direct binary execution, one discarded warm-up, and
best-of-three for loop decisions. Record medians/p50/p99 in the final Criterion
confirmation. The score is the minimum normalized improvement across direct
throughput, anchor throughput, inverse control latency, and drain throughput;
using the minimum prevents a large gain in one path from hiding damage to
another. No component may fall below 95% of the contract baseline; control
latency may not exceed 110% because it is noisier.

## Verification

Every iteration runs formatting, workspace tests, strict Clippy, the allocation
and leak tests, frozen-surface comparison, and bounded Loom. Before completion
also run Loom at preemption 7, Miri over the library/tests, Criterion
confirmation, and the mutation probes listed in `.auto/prompt.md`.
