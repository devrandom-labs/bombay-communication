# Mailbox admission

`mailbox_channel` separates permission to begin a user delivery from the raw
user lane's sender liveness. `MailboxOwner` owns that permission;
`MailboxRef` is a cloneable, non-owning address. Raw `channel`, `UserSender`,
and `UserAnchor` do not acquire mailbox admission and retain their existing
counting, backpressure, and closure laws.

## Closure and exact recovery

The private admission state is `Open(UserSender<U>)` or `Closed`, shared
through one mutex. A reference promotes its weak admission reference and,
under the mutex, acquires a temporary existing `UserSender` only while open.
The guard and promoted admission reference are released before delivery waits
for queue capacity. Constructing a `MailboxRef::send` future does not acquire
permission; its first poll performs admission acquisition.

Owner closure replaces `Open` with `Closed` under the same mutex. It drops the
retired sender after releasing the guard, so queued payload destruction,
wakeup, or payload reentry cannot run under the admission mutex. Dropping the
owner performs the same closure, including when another operation has
promoted the weak admission reference. Reference allocation liveness cannot
keep admission open.

An operation that acquired its temporary sender before closure may finish.
A later acquisition fails and returns the original payload through
`UserClosed` or `TrySendError::Closed`, without cloning or reconstructing
the rejected payload. Queue publication remains the message acceptance point; permission acquisition can precede a backpressure wait.
If the consumer has been dropped, the existing delivery error still returns
the payload.

Cancellation after acquisition drops that operation's temporary sender and
payload; it does not close the owner's admission. Cancelling an unpolled
future never acquires a sender. After every counting sender has retired and
the queue drains, the consumer emits exactly one `UserLaneClosed`. Control
traffic retains its independent lane and ordering.

The critical section contains only the closed-state match, existing sender
clone, or state replacement. It performs no payload callbacks, payload drops,
or awaits. Mutex poisoning indicates an internal invariant failure; there is
no added payload recovery policy for poisoning. Lock scheduling depends on
the operating system; this implementation promises no bounded closure time.

## Verification

The owning regressions observe complete typed traces and exact move-only
payload allocations. They cover a pending pre-close operation followed by a
new post-close operation, stale references, unpolled future closure, pending
future cancellation, payload destructor reentry, and a `Send` but non-`Sync`
`Cell` payload shared through a mailbox reference. No `U: Sync` bound is added.
Loom explores acquire/close interleavings with the same explicit closed state.
The original implementation fails the pending-operation regression in both
debug and optimized builds: the retained sender incorrectly permits a new
operation after owner closure. The correction preserves the pre-close
operation and returns the exact post-close payload.

## Allocation and ordinary Rust comparison

The mailbox adds one shared admission allocation at construction. After
construction, steady-state `try_send`, async send, and closed rejection do not
allocate. Raw user-lane construction and send paths are unchanged.

An ordinary `RwLock` over the same closed sum preserves the contract and the
`U: Send` interface. Both variants pass the same correctness, cancellation,
reentry, allocation, and `Cell` witnesses. The mutex keeps the acquisition and
closure order explicit and performs better for the measured concurrent
producer workload. Neither lock supplies an operating-system-independent
writer fairness guarantee.

Criterion measurements use the same benchmark bodies, 10 samples, 0.5-second
warmup, and 0.5-second measurement periods on a shared host. The batched
1,024-send and control-latency workloads exclude construction and return their
owned channels for teardown after timing. The two-producer workload includes
construction, task spawning and joining, owner closure, and terminal drain.
Its 40,000 messages comprise 20,000 user deliveries and 20,000 control messages,
rather than 40,000 admission acquisitions. These are measurements, not latency
bounds or universal performance guarantees:

| Workload | Mutex | RwLock |
| --- | ---: | ---: |
| One producer, 1,024 mailbox sends | 13.232 us | 12.086 us |
| Mailbox, two producers, 40,000 mixed messages | 1.3779 ms | 1.6827 ms |
| Raw two-lane channel, two producers, 40,000 mixed messages | 798.83 us | 829.61 us |

The original mailbox measured approximately 9 us per 1,024 sends; the corrected
mutex mailbox measured approximately 13 us. A separate paired run measured
1.3417 ms originally and 1.4433 ms with the mutex for the two-producer workload.
The correction has a material admission cost; zero steady-state allocations
do not remove that cost. Raw anchor and control measurements are retained as
comparison workloads rather than attributed to the admission change.

A packed phase/count model establishes only representation feasibility. It
does not prove preservation of the actual raw sender-count overflow domain or
all raw anchor, clone, wakeup, and ownership paths. An independent atomic
phase with a second check can reject an already acquired temporary sender
after closure; that changes the established pre-close permission law. Neither
alternative is substituted for the owning contract.
