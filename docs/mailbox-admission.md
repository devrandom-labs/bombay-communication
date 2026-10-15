# Mailbox admission

## Capacity observation — current verification

The source candidate observes readiness without reserving capacity or a sender.
A dequeued user original remains owned through notification panic, retry and drain.
Native controls pass 21/21 in debug and optimized builds, including actual Tokio
capacity tasks; four bounded Loom models also pass in both profiles. Intentional
cuts fail their targeted custody/wake laws. Pinned Nix passes all seven Darwin
checks (101 workspace tests); strict Clippy passes.
Consumer remains `Unpin` even with `!Unpin` payloads; it does not pin them.
Package tests run in separate Nextest processes, followed by Cargo doctests.
The earlier single-allocation libtest failure is retained; its cause is unproven.
The isolated observer/delivery allocation window records zero successful allocations.
On pinned aarch64 Rust 1.95, Consumer sizes are 72 bytes for Box<u64> and
80 bytes for u64, versus 64 bytes each at 24ad2b9; all have alignment 8.
Two paired same-host runs with unchanged Criterion settings measured:

- Drain: 66.5–66.8 µs at 24ad2b9 versus 123.8–124.7 µs here (86–87% slower).
- 1,024 owner-reference rounds: 13.0–13.1 µs versus 18.8–19.0 µs (45% slower).
- 1,024 raw-anchor rounds: 8.9–9.0 µs versus 14.5 µs (61–63% slower).
- Observer round: 43.5 ns without registration versus 131.1–131.7 ns registered.

These costs are accepted for this correctness milestone. They measure whole
workloads, not an isolated cause, cross-thread latency or an Application service;
normal OS activity and sequential run order remain measurement limitations.
No performance SLO is claimed. Tokio-hosted tasks prove wake progress on dequeue,
admission closure and consumer disposal under nonpanicking executor wakers.
Arbitrary foreign-waker panic may prevent sibling scheduling; that progress is
unsupported, while closure and exact original-payload custody remain required.
The broader ba015 sibling-progress expectation failed in both profiles; its red
evidence is preserved, not reclassified as a passing proof.
Latest-source Linux PR CI, review, merge, publication and Bombay adoption remain open.

## Historical preparation

The current supported contract explicitly reconciles the earlier broader panic
gate above. It adds no notification mechanism or panic-discharge policy.

Before native dequeue-custody repair, preserve healthy0449 and the actual
10f7407c panic regression. The selected existing Consumer owns one Option<U>
from queue removal through successful wake return and caller transfer. Retry
wake hints from the previous head ticket before transferring a retained original;
no new dequeue occurs. Drain prepends it, control-only receive leaves it alone,
and whole Consumer surrender drops it once. No catch/discharge policy, second
queue, task, Arc, public type, manifest or dependency change is selected.
First correct the incompatible drain[61] characterization to complete[59,61],
and add cancelled borrowed receive/retry FIFO and whole-owner surrender tests.
Freeze/review those tests twice before the actual pre-fix debug/optimized runs.
Carry136 production introductions and858 authored/494 retained verification;
selected owner ceilings220 production,700 retained/2000 authored verification.
Close/teardown panic and cost remain separate unproved boundaries.
Before freezing tests, remove panic-payload downcasts: retain the actual caught
Err opaquely through cleanup, observe its occurrence and exact native wake count.
Extend the retry case with a third original refilling the released slot and a
fresh counting observer. Retrying retained first must publish a wake hint even
while the ring remains full, then complete the exact three-original FIFO and
terminal trace. This selected retry law adds no delivery or capacity reservation.
Before custody inversions, freeze f07fda17 and tests8d0b9f06 after actual
three native controls pass debug and optimized. First take the retained original
into a stack local before wake calls: all three after-cleanup custody oracles
must fail. Separately omit drain's retained-original prepend: the complete
original drain slice must fail. Finally return a retained original without
replaying wake hints: the fresh observer's saved wake delta must fail while
its actual capacity remains Pending. Run each selected test/filter in both
profiles, retain exact mutant patches/hashes and actual Cargo exits, restore
byte-exact f07 between variants, then full15 native tests and four focused
race models in each profile. No caught-panic conversion, new state, dependency,
close/teardown panic policy, cost or release claim follows.

At exact24ad2b9, add owning tests before implementing the coordinator-selected
`MailboxRef::wait_for_capacity -> Capacity::{Available, Closed}`. It observes
capacity without taking a message, acquiring a sender or reserving a slot.
Use existing actual mailbox/ring state and separate capacity notifications;
existing sends and their preclose permit law remain distinct. No API is
implemented by this checkpoint. First expected compilation must fail for the
missing enum/method, rather than a fixture setup error.

Four finite single-poll tests cover an outside original and no insertion,
non-stride dequeue notification, closure while an actual preclose send survives,
consumer teardown, observer withdrawal, another sender taking capacity and
cancelled borrowed observation. Counting wakers and complete bounded receive
traces reject missing notifications without hanging. No sleeps, timeouts,
self-wakes or unbounded polling. Preserve all existing tests. Forecast at most
350 retained/1000 chronological new test lines; no production, manifest, lock,
version, dependency or CI edit at this stage. Owner source inversions, native
Loom, allocation/performance measurements and release verification follow
separately after source eligibility; this record claims none executed.

Before extending the close test, retain its Pending capacity future unpolled
through finite terminal/EOF observations, then release it and finish cleanup.
This rejects an observation that secretly owns a counting sender. Existing
strided sender-waker panic exposure remains source-only, outside this test
patch and without a selected repair. The new observer-notification seam needs
its own promised receive/custody contract before a panic observation can be
classified as a defect; no catch/discharge policy is selected here.

Before production at the same baseline, coordinator selects the actual owner
comparison: one Capacity enum and MailboxRef::wait_for_capacity, one separate
UserLane Notify, guarded existing ring-ticket observation, and wake publication
after every pop, admission close and consumer teardown. Reuse existing lane
storage, admission mutex and slot tickets; no sender/permit across await, new
dependency, default, producer-protocol change or lifecycle product. Limit this
candidate to120 introduced production lines, with the existing350 retained/
1000 chronological test allowance carried. Only isolated pinned1.95 focused
debug/optimized controls follow; Loom, measured costs, races, notification-panic
custody and release remain unproved. A notification may invoke a panicking
waker after a slot was removed; this comparison selects no catch/discard policy
and cannot yet establish the final readiness/receive-panic contract.

Before causal mutations, keep healthy production4cd1f3d7/tests058b immutable.
Run six exact temporary cuts: remove pop broadcast; restrict it to the old
stride; remove owner-close broadcast while a preclose sender survives; remove
consumer-teardown broadcast; hold a counting sender through Pending; and make
the observer register on existing send_notify without adding a notify_one.
The first two target the non-stride wake oracle, closure/teardown target their
named wake counts, the sender lifetime cut targets terminal/EOF while the
observer is still Pending, and shared notification targets independent
observer/sender wakes. Every test finishes finite cleanup before its oracle.
Execute the selected named test in debug/optimized for each cut; require one
actual intended failure, not compilation, timeout or zero matching tests.
Restore exact4cd bytes between variants, then execute all12 mailbox-retirement
tests in both profiles. Carry75 production and497 conservative test
introductions; forecast at most7 further temporary production introductions,
within120, with no test changes. Preserve logs and exact mutant source bytes.
Loom, performance, panic policy and release remain separate pending gates.

Before the race-model stage, coordinator selects one shared private guarded
Poll<Capacity> probe for the native async path and private blocking models in
the existing mailbox_admission_tests host. Extend only the existing Loom
Notify stand-in with broadcast generation, snapshot and wait-while-unchanged;
keep producer permit/enabled/woken behavior. Add dequeue, preclose-owner,
consumer-drain and broadcast/competitor-refill models before the missing
probe/model interface compilation, then implement the actual shared owner.
No public model method/type, queue, trait, executor, dependency or version.
Carry82 production/497 conservative test introductions and all source copies;
select160 cumulative production introductions,500 retained/1600 authored
verification lines. Focused model/native debug and optimized controls, then
actual model defects and exact restoration precede broader gates. A branch
limit is an incomplete exploration, not a semantic failure. Native waker panic
and cost remain separate unresolved contracts; no policy is selected here.

Before model counterexamples, freeze healthy0449aade. Omit pop, owner-close
and consumer-teardown broadcasts separately and run their corresponding
private model in each profile. Separately omit the stand-in's broadcast
generation increment: the competing-refill model must detect the lost
pre-registration event. The latter is model-fidelity evidence, not a native
Tokio defect. Capture the first actual Loom cause (deadlock versus a branch
limit or compilation failure), restore exact healthy bytes between cuts, and
run all four private models sequentially after restoration. Preemption bound3
and default branch bound1000 limit this model claim; no timing, wake-panic,
arbitrary fairness or whole-service conclusion follows.

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

### User-closure diagnostic correction

The diagnostic law is that a rejected user delivery must report closure without
claiming that the consumer was dropped. Explicit mailbox-admission closure can
return `UserClosed` or `TrySendError::Closed` while the same consumer drains the
entire accepted prefix and then its terminal marker.

Before the production edit, the bounded correction covers only the two user
error descriptions and their Display strings in `crates/communication/src/lib.rs`
(at most eight net Rust documentation/text lines), the existing closure
regression in `crates/communication/tests/mailbox_retirement.rs` (at most 45 net
test lines), and this owning record. The original test will assert both strings
as `user lane closed` after observing the complete live-consumer drain. Restoring
each original string separately must fail its intended assertion in debug and
optimized builds, followed by byte-exact source restoration and passing controls.

The existing error variants, original payload recovery, admission owner,
mailbox, and complete receive trace are reused. No public type, dependency,
delivery implementation, resource limit, or version changes. `ControlClosed`
keeps its consumer-disappearance wording. Compilation, runtime tests, formatting,
and strict linting use Bombay's pinned Nix shell; owning Linux CI remains required
before delivery. The selected baseline is commit
`272a2343187b40615ab26c2d0d2e136010a16e77`.

Verification on 2026-10-10 establishes the original diagnostic failure in both
debug and optimized builds, after the complete accepted-prefix drain. Corrected
controls pass in both builds. Restoring only `UserClosed`'s original Display
fails its own assertion; restoring only `TrySendError::Closed`'s original Display
fails the other assertion, in both builds. Each mutation is followed by
byte-exact production-source restoration and passing controls. The restored
source SHA-256 is
`003b01d35f0589a799ebcf24dde53637b8de0fe3f7423dd542e18f0a8928870e`.

Bombay's pinned Rust 1.99 runs all 84 workspace tests successfully in debug and
optimized builds; the optimized command selects libraries, integration tests,
and binaries. Workspace documentation testing succeeds with zero executed
tests and one existing ignored example. An attempted optimized all-target run
was interrupted while its inherited `OneStruct` Criterion comparison stalled;
that attempt is incomplete, not a passing benchmark campaign. No benchmark
code or delivery behavior was changed.

The owner's exact pinned Nix Rust 1.95.0 passes workspace formatting and strict
Clippy across all targets without Rust warnings. Rust 1.99's strict lint attempt
fails the inherited `AtomicUsize::fetch_update` deprecation, which was also
present before this correction. No suppression, atomic implementation change,
minimum-version change, or workflow change was made. Required Linux Nix CI and
independent review remain necessary; this record does not claim merge or release.

Complete tracked and untracked accounting: three existing changed paths and
no untracked paths; production Rust documentation/text +6/-4/net +2; owning
tests +24/-10/net +14; public types +0/-0; manifests, locks, and version unchanged.
Owning guidance adds 54 lines; there are no other retained source changes.
Only the two user-error diagnostics and their documentation changed in production.

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

## Control conversion and rejection custody

`ControlSender::send_map(input, map)` checks the existing consumer-disappearance
condition before converting the input into the concrete control protocol.
Closed admission returns `ControlClosed(input)` without calling the conversion.
After admission, conversion precedes queue-slot allocation and follows the exact
existing `send` publication and wakeup sequence. Closure during conversion keeps
the existing accepted-tail contract; acceptance transfers custody to the queue
and does not promise consumption. A conversion panic claims no queue slot.

This derived ownership law is required by Bombay's independent early-stop
witness: forward-only event ingress consumes a move-only child report, so a
failed later send cannot reconstruct that report. All rejection-bearing values
belong in `input`; captures of the conversion are outside this recovery promise.
The conversion runs synchronously and may itself block or panic. No inverse
ingress, new actor protocol, closure phase, allocation or queue policy is added.
Ordinary direct sends delegate to the same operation with identity conversion.

The pre-edit law and bounded source scope were recorded in the owning
Behavior developer-experience PRD before production changes. The focused
regression precedes implementation; the corrected original-source inverse
fails solely on five missing-method diagnostics. Four tests pass in both debug
and release: exact original boxed input on closed admission, mixed direct/mapped
FIFO, accepted-tail custody during consumer closure, and no unpublished slot
after conversion panic. The initial test attempted a Loom-only blocking receive;
correcting it to the public async receive changed tests only. It is not counted
as an admission-law failure.

Compared with owner 77bed281, production is +21/-2 (including documentation) in
one existing module; the focused test is 103 lines in one new integration file.
Public methods +1, public types/modules/dependencies/versions +0. Existing control
liveness states, queue representation, rejection alternatives and atomic/wakeup
operations are unchanged. No subordinate state or result is introduced. The
surviving rejection owns the original input; the accepted queue owns the mapped
control signal. No history, duplicate cause, nested transition authority or
structural application syntax is added. Cross-checks use the existing control
closure contract and Bombay's complete report-rejection law. Strict pinned
Rust 1.95 workspace/all-target Clippy passes. All seven native Nix gates pass
with 88 tests and zero skipped; all twelve existing Loom models pass with
LOOM_MAX_PREEMPTIONS=3 in 210.51s. The real Bombay single-report-stop journey
passes both profiles using this exact source: the later child retires with its
original rejected report and ClosedParentControlLane, the earlier accepted
report remains in the parent result, and both committed origins remain exact.
Restoring only the original native report interpreter reproduces its control
closure panic in both profiles. Disposition: pass for this bounded transport
law. Owning Linux CI, downstream wrapper proof and release delivery remain open.

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
