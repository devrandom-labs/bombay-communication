//! Two-lane priority merge (bombay card #225) — a ring + control-sideband design.
//!
//! Merge a **control** lane and a **user** lane into one [`Consumer`] so that
//! control signals are served ahead of a user backlog, while keeping FIFO
//! per lane, no loss, no lost wakeups, a clean teardown drain, a zero-alloc
//! steady-state send, and no user starvation under a control flood.
//!
//! # Mechanism
//!
//! - **User lane** — a preallocated bounded Vyukov MPSC ring (power-of-two
//!   slots, one sequence atomic per slot, claim via a single `fetch_add`).
//!   Backpressure lives IN the ring: a producer that finds the head slot
//!   occupied parks on a `send_notify` eventcount and is woken one-per-pop by
//!   the consumer. A steady-state `try_send` is a handful of atomics and
//!   never allocates (P7).
//! - **Control lane** — an unbounded lock-free MPSC chain of single-use
//!   64-slot blocks: producers claim a global ticket with one `fetch_add`
//!   and publish into their slot; the single consumer pops in ticket order.
//!   Consumed blocks are reclaimed by the consumer once no producer can
//!   hold a block hint into them (an `in_flight` registration brackets each
//!   push's hint window), so the lane does not leak (P8). Control never
//!   shares the user ring, so a full ring can never delay it.
//! - **Wakeup** — one shared [`tokio::sync::Notify`] gated by a `parked`
//!   flag. A producer pays a single atomic load when the consumer is active;
//!   a parked consumer is woken by `notify_one` after registering with
//!   enable-then-recheck, so the lost-wakeup class is impossible by
//!   construction and `recv` stays cancel-safe (no item is moved before the
//!   consumer is known to be awake to take it). The flag protocol is
//!   model-checked under `cfg(loom)` (`tests/loom.rs`).
//!
//! The public API below is FIXED — the property suite in `fastpass-testkit`
//! depends on these exact names and signatures.

use std::fmt;
use std::mem::MaybeUninit;

#[cfg(not(loom))]
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering};

#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering};

#[cfg(not(loom))]
use tokio::sync::Notify;

/// Synchronous stand-in for `tokio::sync::Notify`, used ONLY under
/// `cfg(loom)`: models tokio's registration semantics explicitly so the
/// `parked`/`waiting` flag protocol exercised by the loom model matches the
/// async build. `enable()` arms a registration (the async twins call
/// `Notified::enable` at the same points); `notify_one` wakes one
/// registered waiter or stores a single permit; `notify_waiters` wakes all
/// registered waiters without storing one. Wake credits are pooled
/// (`woken`), so a registration abandoned by an early return can only
/// cause a spurious extra loop turn, never a lost wakeup — matching
/// tokio's `Notify`.
#[cfg(loom)]
mod sync_notify {
    use loom::sync::atomic::Ordering;

    pub struct Notify {
        state: loom::sync::Mutex<State>,
        cv: loom::sync::Condvar,
    }

    struct State {
        /// Stored permit (notify with no registered waiter), at most one.
        permit: bool,
        /// Armed registrations not yet consumed by a `wait`.
        enabled: usize,
        /// Wake credits granted by `notify_one`/`notify_waiters`.
        woken: usize,
    }

    impl Notify {
        pub fn new() -> Self {
            Self {
                state: loom::sync::Mutex::new(State {
                    permit: false,
                    enabled: 0,
                    woken: 0,
                }),
                cv: loom::sync::Condvar::new(),
            }
        }

        /// Arm a registration, mirroring `Notified::enable`. ALSO the
        /// synchronization point of the loom protocol: the caller announces
        /// its flag (`parked`/`waiting`) BEFORE this call, so the mutex
        /// release here publishes the announcement to any subsequent
        /// `notify_if*` lock.
        pub fn enable(&self) {
            self.state.lock().unwrap().enabled += 1;
        }

        /// Wake one waiter iff `flag` is set. The flag check happens UNDER
        /// the lock: if this lock follows the waiter's `enable` in the
        /// mutex order, the check synchronizes-with the waiter's
        /// announcement and must observe it; otherwise the waiter's
        /// post-enable re-check observes whatever this caller published
        /// before locking. A plain atomic check outside the lock has no
        /// happens-before edge to the announcement, and loom (correctly
        /// over-approximating hardware) explores the lost wakeup.
        pub fn notify_if(&self, flag: &loom::sync::atomic::AtomicBool) {
            let mut g = self.state.lock().unwrap();
            if !flag.load(Ordering::SeqCst) {
                return;
            }
            if g.enabled > 0 {
                g.enabled -= 1;
                g.woken += 1;
                self.cv.notify_one();
            } else {
                g.permit = true;
            }
        }

        /// Wake one waiter iff `n` is nonzero — see `notify_if`.
        pub fn notify_if_nonzero(&self, n: &loom::sync::atomic::AtomicUsize) {
            let mut g = self.state.lock().unwrap();
            if n.load(Ordering::SeqCst) == 0 {
                return;
            }
            if g.enabled > 0 {
                g.enabled -= 1;
                g.woken += 1;
                self.cv.notify_one();
            } else {
                g.permit = true;
            }
        }

        pub fn notify_one(&self) {
            let mut g = self.state.lock().unwrap();
            if g.enabled > 0 {
                g.enabled -= 1;
                g.woken += 1;
                self.cv.notify_one();
            } else {
                g.permit = true;
            }
        }

        pub fn notify_waiters(&self) {
            let mut g = self.state.lock().unwrap();
            g.woken += g.enabled;
            g.enabled = 0;
            self.cv.notify_all();
        }

        /// Park until a wake credit or stored permit is available, then
        /// consume it.
        pub fn wait(&self) {
            let mut g = self.state.lock().unwrap();
            if g.permit {
                g.permit = false;
                return;
            }
            while g.woken == 0 {
                g = self.cv.wait(g).unwrap();
            }
            g.woken -= 1;
        }

        /// Lock+unlock with no other effect: a happens-before edge to every
        /// `notify_if*` that ran before it in the mutex order. The closed
        /// sweep uses this to observe a departing sender's publishes — the
        /// sender's pre-decrement `wake_consumer` (a `notify_if` lock) is
        /// program-ordered before the decrement the consumer observed.
        pub fn sync_point(&self) {
            let _g = self.state.lock().unwrap();
        }
    }
}

#[cfg(loom)]
use sync_notify::Notify;

/// Consumer configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    user_capacity: usize,
    aging_cap: usize,
}

impl Config {
    /// A config with the given bounded user-lane capacity and no aging.
    ///
    /// The capacity is a MINIMUM: the ring rounds it up to a power of two
    /// (floor 2), and the rounded size is the effective capacity.
    #[must_use]
    pub const fn new(user_capacity: usize) -> Self {
        Self { user_capacity, aging_cap: 0 }
    }

    /// Force one waiting user through after `k` consecutive control dequeues.
    /// `0` disables aging.
    #[must_use]
    pub const fn with_aging_cap(mut self, k: usize) -> Self {
        self.aging_cap = k;
        self
    }
}

/// The item handed back by [`Consumer::recv`], tagged by its lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received<C, U> {
    /// A control-lane item.
    Control(C),
    /// A user-lane item.
    User(U),
}

/// Items still queued when the consumer tears down (see [`Consumer::drain`]).
#[derive(Debug)]
pub struct Drained<C, U> {
    /// Queued control items, in FIFO order.
    pub control: Vec<C>,
    /// Queued user items, in FIFO order.
    pub user: Vec<U>,
}

/// The control lane closed because the [`Consumer`] was dropped.
#[derive(Debug, thiserror::Error)]
#[error("control lane closed: consumer dropped")]
pub struct ControlClosed<T>(pub T);

/// The user lane closed because the [`Consumer`] was dropped.
#[derive(Debug, thiserror::Error)]
#[error("user lane closed: consumer dropped")]
pub struct UserClosed<T>(pub T);

/// A non-blocking user send could not be enqueued.
#[derive(Debug, thiserror::Error)]
pub enum TrySendError<T> {
    /// The bounded user lane is at capacity.
    #[error("user lane full")]
    Full(T),
    /// The consumer was dropped.
    #[error("user lane closed: consumer dropped")]
    Closed(T),
}

/// State shared by both lanes and the consumer: the single wakeup point and
/// the consumer-liveness flag.
struct Core {
    /// Consumer wakeup. Single waiter (the consumer), `notify_one` suffices.
    notify: Notify,
    /// Set while the consumer is parked in `notify`; gates the wake call so a
    /// producer with an active consumer pays one atomic load, nothing more.
    parked: AtomicBool,
    consumer_gone: AtomicBool,
}

impl Core {
    /// Wake the consumer iff it is parked. The check is an RMW, not a plain
    /// load: absent a happens-before edge, a load may legally miss the
    /// consumer's earlier `parked` store (store buffering — loom exhibits
    /// the lost wakeup), while an RMW always observes the newest value in
    /// the modification order. The consumer's post-store re-check covers
    /// the other Dekker direction.
    #[cfg(not(loom))]
    #[inline]
    fn wake_consumer(&self) {
        if self.parked.load(Ordering::SeqCst) {
            self.notify.notify_one();
        }
    }

    /// Wake the consumer iff it is parked — loom variant: the flag check is
    /// taken under the `Notify` stand-in's lock so it synchronizes with the
    /// consumer's announcement (see `sync_notify::Notify::notify_if`).
    #[cfg(loom)]
    fn wake_consumer(&self) {
        self.notify.notify_if(&self.parked);
    }
}

/// One slot of the user ring. `seq` is the Vyukov ticket, stored as `u32`
/// (all ticket arithmetic is mod 2^32; the live-ticket window is bounded by
/// `ring.len() < 2^31`, so truncation is sound): it equals the slot's
/// position when free and `position + 1` once published. The narrow ticket
/// halves slot memory for small `U`.
struct Slot<U> {
    seq: AtomicU32,
    val: std::cell::UnsafeCell<MaybeUninit<U>>,
}

impl<U> fmt::Debug for Slot<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

/// The bounded user lane: a Vyukov MPSC ring with in-ring backpressure.
struct UserLane<U> {
    /// Ring slots, `ring.len()` is the capacity (min 1) rounded up to a
    /// power of two.
    ring: Box<[Slot<U>]>,
    /// Next claim ticket for producers (multi-producer CAS).
    tail: AtomicUsize,
    /// Next pop ticket; written only by the single consumer.
    head: AtomicUsize,
    /// Parked-producer wakeup; gated by `waiting`.
    send_notify: Notify,
    /// Number of producers parked on a full ring (guards `send_notify`).
    waiting: AtomicUsize,
    /// Live [`UserSender`] handles; lane is closed when this reaches 0.
    senders: AtomicUsize,
    core: Arc<Core>,
}

// SAFETY: the ring protocol is the standard bounded-queue ticket discipline —
// a slot's value is written before its `seq` is published (Release) and read
// only after observing the published `seq` (Acquire); the consumer alone
// advances `head`, producers alone advance `tail`, and a slot is reused only
// after the consumer re-tickets it. `U` crosses threads only through that
// protocol, so `U: Send` suffices.
unsafe impl<U: Send> Send for UserLane<U> {}
unsafe impl<U: Send> Sync for UserLane<U> {}

impl<U> UserLane<U> {
    #[inline]
    fn mask(&self) -> usize {
        self.ring.len() - 1
    }

    /// Try to enqueue `v`; on failure hands `v` back (ring full).
    #[inline]
    fn try_push(&self, v: U) -> Result<(), U> {
        let mask = self.mask();
        let mut pos = self.tail.load(Ordering::Acquire);
        loop {
            // SAFETY: `pos & mask` is always in bounds.
            let slot = unsafe { self.ring.get_unchecked(pos & mask) };
            let seq = slot.seq.load(Ordering::Acquire);
            let pos32 = pos as u32;
            let diff = seq.wrapping_sub(pos32) as i32;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // SAFETY: we claimed cycle `pos` of this slot; the
                        // free `seq` ticket proves the previous occupant was
                        // consumed, so we hold exclusive access.
                        unsafe { slot.val.get().write(MaybeUninit::new(v)) };
                        slot.seq.store(pos32.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                return Err(v); // slot still occupied → full
            } else {
                pos = self.tail.load(Ordering::Acquire);
            }
        }
    }

    /// Dequeue the head item, if one is published. Consumer-only.
    #[inline]
    fn pop(&self) -> Option<U> {
        let head = self.head.load(Ordering::Relaxed);
        // SAFETY: `head & mask` is always in bounds.
        let slot = unsafe { self.ring.get_unchecked(head & self.mask()) };
        let head32 = head as u32;
        if slot.seq.load(Ordering::Acquire) != head32.wrapping_add(1) {
            return None;
        }
        // SAFETY: the published `seq` (Acquire) orders the producer's write
        // before this read; only the consumer pops, and the slot is not
        // reused until re-ticketed below.
        let v = unsafe { slot.val.get().read().assume_init() };
        slot.seq
            .store(head32.wrapping_add(self.ring.len() as u32), Ordering::Release);
        self.head.store(head.wrapping_add(1), Ordering::Relaxed);
        // Wake one parked producer, if any — checked every 4th pop only;
        // a parked producer is also released by the consumer's pre-park
        // check, so it can never sleep past an empty ring.
        if head % 4 == 0 {
            self.release_one_waiter();
        }
        Some(v)
    }

    /// Release one parked producer, if any is waiting.
    #[cfg(not(loom))]
    #[inline]
    fn release_one_waiter(&self) {
        if self.waiting.load(Ordering::SeqCst) != 0 {
            self.send_notify.notify_one();
        }
    }

    /// Release one parked producer, if any — loom variant: the `waiting`
    /// check is taken under the `Notify` stand-in's lock (see
    /// `sync_notify::Notify::notify_if`).
    #[cfg(loom)]
    fn release_one_waiter(&self) {
        self.send_notify.notify_if_nonzero(&self.waiting);
    }

    /// True once every [`UserSender`] is gone.
    #[inline]
    fn closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }
}

impl<U> Drop for UserLane<U> {
    fn drop(&mut self) {
        // Quiescent: all senders and the consumer are gone, so `head..tail`
        // are exactly the published-but-unconsumed items.
        let head = self.head.load(Ordering::Relaxed); // quiescent: &mut self
        let tail = self.tail.load(Ordering::Relaxed); // quiescent: &mut self
        for pos in head..tail {
            let slot = &self.ring[pos & self.mask()];
            if slot.seq.load(Ordering::Relaxed) == (pos as u32).wrapping_add(1) {
                // SAFETY: published and never consumed; exclusive `&mut self`.
                unsafe { slot.val.get().drop_in_place() };
            }
        }
    }
}

/// Slots per control block. Blocks are single-use (never reused), so a slot
/// needs no Vyukov ticket — just a publish flag. Under `cfg(loom)` the
/// block is shrunk so the tiny model crosses block boundaries (linking,
/// hint advance, and reclamation are all exercised).
#[cfg(not(loom))]
const CBLOCK: usize = 64;
/// `CBLOCK` under loom — see above.
#[cfg(loom)]
const CBLOCK: usize = 2;

/// One control slot: written once, published with `ready`.
struct CSlot<C> {
    ready: AtomicBool,
    val: std::cell::UnsafeCell<MaybeUninit<C>>,
}

/// A linked block of the control queue.
struct CBlock<C> {
    /// Global block index (`block_base = idx * CBLOCK`).
    idx: usize,
    slots: [CSlot<C>; CBLOCK],
    next: AtomicPtr<CBlock<C>>,
}

impl<C> CBlock<C> {
    fn new(idx: usize) -> Box<Self> {
        Box::new(Self {
            idx,
            slots: std::array::from_fn(|_| CSlot {
                ready: AtomicBool::new(false),
                val: std::cell::UnsafeCell::new(MaybeUninit::uninit()),
            }),
            next: AtomicPtr::new(std::ptr::null_mut()),
        })
    }
}

/// The unbounded control sideband: a lock-free MPSC chain of single-use
/// blocks. Producers claim a global ticket with one `fetch_add` and publish
/// into their slot; the single consumer pops in ticket order lock-free.
/// Control never shares the user ring, so a full ring can never delay it.
struct ControlLane<C> {
    /// Next claim ticket (multi-producer `fetch_add`; unbounded).
    tail: AtomicUsize,
    /// Highest linked block (best-effort hint; producers walk forward from
    /// it). Never points behind `first_block` (reclaim's `min` bound), so
    /// any value read here is a live block.
    tail_block: AtomicPtr<CBlock<C>>,
    /// Live frontier: the first block a push may walk from. Monotonic,
    /// consumer-only writer, published BEFORE the corresponding reclamation
    /// check so a push that registers in-flight afterwards is guaranteed
    /// (SeqCst chain) to read a frontier above the freed prefix.
    first_block: AtomicPtr<CBlock<C>>,
    /// First unfreed block. Blocks in `[free_anchor, first_block)` are
    /// retired (fully consumed) but not yet freed — freeing waits for an
    /// `in_flight == 0` crossing. Consumer-only writer, read at lane drop.
    free_anchor: AtomicPtr<CBlock<C>>,
    /// Slow (block-crossing) pushes between their in-flight registration
    /// and slot publish. The consumer frees retired blocks only while this
    /// is zero, which proves no push can be walking the freed prefix.
    in_flight: AtomicUsize,
    /// Tickets consumed, published by the consumer ONLY at teardown so the
    /// lane's `Drop` reclaims exactly the published-but-unconsumed items.
    consumed: AtomicUsize,
    /// Live [`ControlSender`] handles; lane is closed when this reaches 0.
    senders: AtomicUsize,
    core: Arc<Core>,
}

// SAFETY: a slot's value is written before `ready` is set (Release) and read
// only after observing `ready` (Acquire); each ticket is claimed by exactly
// one producer; blocks are linked before any of their slots are published
// and freed only when all senders and the consumer are gone. `C` crosses
// threads only through that protocol, so `C: Send` suffices.
unsafe impl<C: Send> Send for ControlLane<C> {}
unsafe impl<C: Send> Sync for ControlLane<C> {}

impl<C> ControlLane<C> {
    /// Claim the next ticket and publish `item` into its slot.
    fn push(&self, item: C) {
        // Load the hint BEFORE claiming the ticket. `tail_block` always
        // points at a block containing an already-claimed ticket `t`, and
        // any ticket claimed after this load satisfies `pos >= tail > t`,
        // so `hint.idx <= t / CBLOCK <= pos / CBLOCK == want`: the hint's
        // block is never BEYOND the ticket's block. (Claim-first ordering
        // lets a preempted push resume with a hint beyond its ticket's
        // block and publish into the wrong slot.)
        let hint = self.tail_block.load(Ordering::SeqCst);
        let pos = self.tail.fetch_add(1, Ordering::AcqRel);
        let want = pos / CBLOCK;
        // SAFETY: `hint` is a live block: it contains a claimed ticket and
        // `hint.idx <= want` by the ordering above.
        if unsafe { &*hint }.idx == want {
            // FAST PATH (steady state): the ticket lives in the hinted
            // block, no walk. The block cannot be reclaimed before our
            // publish: `reclaim` only frees blocks the consumer has fully
            // crossed, and our ticket in it is not even published yet, so
            // the consumer's cursor cannot pass it.
            let slot = unsafe { &(*hint).slots[pos % CBLOCK] };
            unsafe { slot.val.get().write(MaybeUninit::new(item)) };
            slot.ready.store(true, Ordering::Release);
            return;
        }
        // SLOW PATH (block crossing): the walk passes through intermediate
        // blocks, which the consumer may reclaim once it has crossed them —
        // register in-flight so `reclaim` provably holds off (see
        // `reclaim`), THEN take a provably-live hint. The inc is ordered
        // before the hint loads below, so any hint this push walks from is
        // covered by the in_flight guard; the initial `hint` may be stale
        // and is never dereferenced here.
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let mut b = self.tail_block.load(Ordering::SeqCst);
        // SAFETY: `b` is live: `tail_block` never points behind the
        // consumer's reclaim frontier, and `in_flight >= 1` blocks new
        // frees for the rest of this push.
        if unsafe { &*b }.idx > want {
            // Another producer advanced `tail_block` past our ticket's
            // block; walk from the reclaim frontier instead (also live,
            // via the SeqCst chain documented in `reclaim`).
            b = self.first_block.load(Ordering::SeqCst);
        }
        // Advance the block chain to the block containing `pos`, linking
        // fresh blocks as needed. Racing linkers: exactly one CAS wins per
        // `next` pointer; losers free their spare block.
        while unsafe { &*b }.idx < want {
            let next = unsafe { &*b }.next.load(Ordering::Acquire);
            if next.is_null() {
                let fresh = Box::into_raw(CBlock::new(unsafe { &*b }.idx + 1));
                match unsafe { &*b }.next.compare_exchange(
                    std::ptr::null_mut(),
                    fresh,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let _ = self.tail_block.compare_exchange(
                            b,
                            fresh,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
                    }
                    Err(_) => drop(unsafe { Box::from_raw(fresh) }),
                }
                continue;
            }
            let _ = self
                .tail_block
                .compare_exchange(b, next, Ordering::SeqCst, Ordering::SeqCst);
            b = next;
        }
        // SAFETY: `b` is the block containing `pos` (the walk terminates
        // with `b.idx == want`); `b` and every block the walk touched are
        // live under the in_flight guard; the ticket is unique to this
        // push, so we hold exclusive access to the slot.
        let slot = unsafe { &(*b).slots[pos % CBLOCK] };
        unsafe { slot.val.get().write(MaybeUninit::new(item)) };
        slot.ready.store(true, Ordering::Release);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Free fully-consumed blocks behind `cursor` (the consumer's current
    /// block). Consumer-only. Two-phase:
    ///
    /// 1. PUBLISH the live frontier (`first_block`) to the first block at
    ///    or after `min(cursor, tail_block)`. A block below `cursor` was
    ///    fully consumed (values moved out); a block below `tail_block`
    ///    can never be a push's walk start. The `min` is load-bearing: a
    ///    freshly LINKED block briefly lags `tail_block` while its
    ///    linker's advance CAS is in flight.
    /// 2. FREE the retired prefix `[free_anchor, frontier)` — only if
    ///    `in_flight == 0`; otherwise freeing catches up at a later
    ///    crossing (no leak: `free_anchor` only advances on real frees).
    ///
    /// Ordering (Dekker over SeqCst): the frontier store (p1) precedes the
    /// `in_flight` read (p2) in program order. A slow push's increment
    /// (p3) precedes its frontier/hint loads (p4). If p2 observes zero,
    /// any push still walking has p3 AFTER p2 in the SeqCst order, hence
    /// p4 > p3 > p2 > p1: it reads a frontier >= the one published here,
    /// and `tail_block` >= `first_block` always, so no push ever walks a
    /// block this call frees. FAST pushes (`hint.idx == want`) write into
    /// a block containing their own unpublished ticket, which `cursor`
    /// can never have crossed — they need no guard.
    fn reclaim(&self, cursor: *const CBlock<C>) {
        // Under loom the block chain is never reclaimed: loom's visibility
        // model (deliberately) allows a load to observe a cell's INITIAL
        // value absent a happens-before edge, so the SeqCst Dekker chain
        // that makes this safe on hardware cannot be model-checked — a
        // stale hint read would manufacture a use-after-free the real
        // protocol forbids. The wakeup/FIFO/no-loss properties the loom
        // lane exists to prove are unaffected; the leak gate (`tests/
        // leak.rs`) covers reclamation on hardware.
        if cfg!(loom) {
            return;
        }
        let tb = self.tail_block.load(Ordering::SeqCst);
        // SAFETY: `tb` and `cursor` are live blocks (reclamation only ever
        // frees strictly behind both).
        let bound = unsafe { &*tb }.idx.min(unsafe { &*cursor }.idx);
        // Advance the frontier to the first block at or after `bound`.
        let mut stop = self.first_block.load(Ordering::Relaxed);
        while !stop.is_null() && unsafe { &*stop }.idx < bound {
            stop = unsafe { &*stop }.next.load(Ordering::Acquire);
        }
        self.first_block.store(stop, Ordering::SeqCst);
        if self.in_flight.load(Ordering::SeqCst) != 0 {
            return;
        }
        let mut b = self.free_anchor.load(Ordering::Relaxed);
        // Amortize: free in batches — the retired prefix keeps the leak
        // bounded by a few blocks, and bulk-freeing avoids per-crossing
        // dealloc churn in the drain hot path.
        if unsafe { &*stop }.idx - unsafe { &*b }.idx < 4 {
            return;
        }
        while !b.is_null() && b != stop {
            let next = unsafe { &*b }.next.load(Ordering::Acquire);
            // SAFETY: the consumer crossed every block below `stop`, so
            // all their slots were consumed (values moved out, nothing to
            // drop); `in_flight == 0` proves no push is walking them;
            // behind `tail_block`, so no new push reaches them.
            drop(unsafe { Box::from_raw(b) });
            b = next;
        }
        self.free_anchor.store(b, Ordering::Relaxed);
    }

    /// True once every [`ControlSender`] is gone.
    #[inline]
    fn closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }
}

impl<C> Drop for ControlLane<C> {
    fn drop(&mut self) {
        // Quiescent: all senders and the consumer are gone. Reclaim every
        // published-but-unconsumed slot, then free the remaining chain
        // (blocks the consumer already freed are behind `free_anchor`).
        let consumed = self.consumed.load(Ordering::Relaxed); // quiescent: &mut self
        let tail = self.tail.load(Ordering::Relaxed); // quiescent: &mut self
        let mut b = self.free_anchor.load(Ordering::Relaxed); // quiescent: &mut self
        while !b.is_null() {
            let block = unsafe { Box::from_raw(b) };
            let base = block.idx * CBLOCK;
            for pos in base.max(consumed)..(base + CBLOCK).min(tail) {
                let slot = &block.slots[pos % CBLOCK];
                if slot.ready.load(Ordering::Relaxed) {
                    // SAFETY: published and never consumed; exclusive ownership.
                    unsafe { slot.val.get().drop_in_place() };
                }
            }
            b = block.next.load(Ordering::Relaxed); // quiescent: &mut self
        }
    }
}

/// Cloneable handle to the control lane. `send` never blocks.
pub struct ControlSender<C> {
    lane: Arc<ControlLane<C>>,
}

impl<C> Clone for ControlSender<C> {
    fn clone(&self) -> Self {
        self.lane.senders.fetch_add(1, Ordering::Relaxed);
        Self { lane: self.lane.clone() }
    }
}

impl<C> Drop for ControlSender<C> {
    fn drop(&mut self) {
        // Wake BEFORE decrementing: under loom this mutex-synchronizes the
        // departing sender's publishes with a consumer that later observes
        // the lane closed (see `sync_notify`); in production it is a cheap
        // gated check and a harmless spurious wake.
        self.lane.core.wake_consumer();
        if self.lane.senders.fetch_sub(1, Ordering::Release) == 1 {
            // Last sender: wake a parked consumer so `recv` can observe the
            // closure and return `None` once both lanes are drained.
            self.lane.core.wake_consumer();
        }
    }
}

impl<C> ControlSender<C> {
    /// Enqueue a control signal. Never blocks (the lane is unbounded).
    ///
    /// # Errors
    /// Returns [`ControlClosed`] carrying `item` if the consumer is gone.
    pub fn send(&self, item: C) -> Result<(), ControlClosed<C>> {
        if self.lane.core.consumer_gone.load(Ordering::Acquire) {
            return Err(ControlClosed(item));
        }
        self.lane.push(item);
        self.lane.core.wake_consumer();
        Ok(())
    }
}

/// Cloneable handle to the bounded user lane.
pub struct UserSender<U> {
    lane: Arc<UserLane<U>>,
}

impl<U> Clone for UserSender<U> {
    fn clone(&self) -> Self {
        self.lane.senders.fetch_add(1, Ordering::Relaxed);
        Self { lane: self.lane.clone() }
    }
}

impl<U> Drop for UserSender<U> {
    fn drop(&mut self) {
        // Wake BEFORE decrementing — see `ControlSender::drop`.
        self.lane.core.wake_consumer();
        if self.lane.senders.fetch_sub(1, Ordering::Release) == 1 {
            self.lane.core.wake_consumer();
        }
    }
}

impl<U> UserSender<U> {
    /// Enqueue a user message, awaiting capacity (backpressure).
    ///
    /// # Errors
    /// Returns [`UserClosed`] carrying `item` if the consumer is gone.
    #[cfg(not(loom))]
    pub async fn send(&self, item: U) -> Result<(), UserClosed<U>> {
        let lane = &self.lane;
        let mut item = item;
        loop {
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                return Err(UserClosed(item));
            }
            match lane.try_push(item) {
                Ok(()) => {
                    lane.core.wake_consumer();
                    return Ok(());
                }
                Err(v) => item = v,
            }
            // Ring full: register, announce, re-check, then park. The
            // announce (SeqCst) pairs with the consumer's SeqCst `waiting`
            // load, so a pop racing this park either wakes us or is seen by
            // the re-check below.
            let notified = lane.send_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            lane.waiting.fetch_add(1, Ordering::SeqCst);
            match lane.try_push(item) {
                Ok(()) => {
                    lane.waiting.fetch_sub(1, Ordering::Relaxed);
                    lane.core.wake_consumer();
                    return Ok(());
                }
                Err(v) => item = v,
            }
            // Park-gating check (RMW — a plain load may miss the consumer's
            // earlier teardown store; see `wake_consumer`).
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                lane.waiting.fetch_sub(1, Ordering::Relaxed);
                // Teardown released us: the send reports success and the item
                // is discarded — the pinned teardown seam (see `drain`).
                return Ok(());
            }
            notified.await;
            lane.waiting.fetch_sub(1, Ordering::Relaxed);
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                return Ok(());
            }
        }
    }

    /// Blocking twin of `send`, compiled ONLY under `cfg(loom)`: the same
    /// announce/re-check/park protocol over `waiting`, with the sync
    /// `Notify` stand-in's `wait()` in place of the `Notified` future.
    /// Drives the loom model in `tests/loom.rs`.
    #[cfg(loom)]
    #[doc(hidden)]
    pub fn send_blocking(&self, item: U) -> Result<(), UserClosed<U>> {
        let lane = &self.lane;
        let mut item = item;
        loop {
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                return Err(UserClosed(item));
            }
            match lane.try_push(item) {
                Ok(()) => {
                    lane.core.wake_consumer();
                    return Ok(());
                }
                Err(v) => item = v,
            }
            // Ring full: announce (SeqCst), re-check, then park. The
            // stand-in's `enable` lock publishes the announcement; the
            // SECOND re-check after it catches any pop the first missed
            // (the consumer's `notify_if_nonzero` lock covers the other
            // direction).
            lane.waiting.fetch_add(1, Ordering::SeqCst);
            match lane.try_push(item) {
                Ok(()) => {
                    lane.waiting.fetch_sub(1, Ordering::Relaxed);
                    lane.core.wake_consumer();
                    return Ok(());
                }
                Err(v) => item = v,
            }
            // Park-gating check (RMW — see `wake_consumer`).
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                lane.waiting.fetch_sub(1, Ordering::Relaxed);
                // Teardown released us: the send reports success and the
                // item is discarded — the pinned teardown seam.
                return Ok(());
            }
            lane.send_notify.enable();
            match lane.try_push(item) {
                Ok(()) => {
                    lane.waiting.fetch_sub(1, Ordering::Relaxed);
                    lane.core.wake_consumer();
                    return Ok(());
                }
                Err(v) => item = v,
            }
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                lane.waiting.fetch_sub(1, Ordering::Relaxed);
                return Ok(());
            }
            lane.send_notify.wait();
            lane.waiting.fetch_sub(1, Ordering::Relaxed);
            if lane.core.consumer_gone.load(Ordering::Acquire) {
                return Ok(());
            }
        }
    }

    /// Enqueue a user message without blocking.
    ///
    /// # Errors
    /// [`TrySendError::Full`] if the lane is at capacity, [`TrySendError::Closed`]
    /// if the consumer is gone — each carrying `item` back.
    #[inline]
    pub fn try_send(&self, item: U) -> Result<(), TrySendError<U>> {
        if self.lane.core.consumer_gone.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(item));
        }
        match self.lane.try_push(item) {
            Ok(()) => {
                self.lane.core.wake_consumer();
                Ok(())
            }
            Err(v) => Err(TrySendError::Full(v)),
        }
    }
}

/// Pop the next published control in ticket order, advancing the consumer's
/// block cursor and reclaiming consumed blocks on each crossing. Expands to
/// a block expression evaluating to `Option<C>`; takes the consumer
/// identifier so it can run under `recv`'s split borrows.
macro_rules! pop_control {
    ($slf:ident) => {{
        let mut block = $slf.ctl_block;
        // SAFETY: `ctl_block` is always a live block (reclamation frees
        // only strictly behind it; the chain anchor lives until lane drop,
        // and the consumer holds an Arc on the lane).
        let b = unsafe { &*block };
        if $slf.ctl_head == (b.idx + 1) * CBLOCK {
            // Crossed a block boundary: the next block is linked before any
            // of its slots are published, so `null` means the lane is empty.
            let next = b.next.load(Ordering::Acquire);
            if next.is_null() {
                None
            } else {
                $slf.ctl_block = next;
                // The block just left is fully consumed; reclaim it (and
                // anything older) once no push can hold a hint into it.
                $slf.ctl.reclaim(next);
                block = next;
                // SAFETY: as above; `ctl_head % CBLOCK` is in bounds.
                let slot = unsafe { &(*block).slots[$slf.ctl_head % CBLOCK] };
                if slot.ready.load(Ordering::Acquire) {
                    // SAFETY: `ready` (Acquire) orders the producer's write
                    // before this read; only the consumer pops, and each
                    // slot is published once.
                    let v = unsafe { slot.val.get().read().assume_init() };
                    $slf.ctl_head = $slf.ctl_head.wrapping_add(1);
                    Some(v)
                } else {
                    None
                }
            }
        } else {
            // SAFETY: as above; `ctl_head % CBLOCK` is in bounds.
            let slot = unsafe { &(*block).slots[$slf.ctl_head % CBLOCK] };
            let __r = slot.ready.load(Ordering::Acquire);
            if __r {
                // SAFETY: as above.
                let v = unsafe { slot.val.get().read().assume_init() };
                $slf.ctl_head = $slf.ctl_head.wrapping_add(1);
                Some(v)
            } else {
                None
            }
        }
    }};
}

/// The single consumer that merges the two lanes.
pub struct Consumer<C, U> {
    ctl: Arc<ControlLane<C>>,
    usr: Arc<UserLane<U>>,
    core: Arc<Core>,
    aging_cap: usize,
    /// Consecutive control dequeues since the last user dequeue (the aging
    /// streak; never exceeds `aging_cap`).
    consec_control: usize,
    /// Consumer-side control pop ticket (plain counter, no atomic).
    ctl_head: usize,
    /// Block containing `ctl_head`; consumer-only. Blocks strictly behind
    /// it are reclaimed by `ControlLane::reclaim` as the cursor crosses
    /// boundaries; the cursor block itself is freed at lane drop.
    ctl_block: *mut CBlock<C>,
}

// SAFETY: `ctl_block` is dereferenced only by the consumer that owns it, and
// the pointee outlives the consumer (freed in `ControlLane::drop`, and the
// consumer holds an `Arc<ControlLane<C>>`).
unsafe impl<C: Send, U: Send> Send for Consumer<C, U> {}

impl<C, U> Consumer<C, U> {
    /// Pop the next published control in ticket order. Consumer-only.
    #[inline]
    fn pop_control(&mut self) -> Option<C> {
        pop_control!(self)
    }

    /// Receive the next item.
    ///
    /// Returns `None` once both lanes are closed and empty.
    ///
    /// POLICY: strict control-first priority with an anti-starvation aging
    /// cap. Control is dequeued before any waiting user (P1, overtake); after
    /// `aging_cap` consecutive control dequeues one waiting user is forced
    /// through (P3 under flood). Users reset the streak; a cap of 0 disables
    /// aging entirely.
    #[cfg(not(loom))]
    pub async fn recv(&mut self) -> Option<Received<C, U>> {
        // Split borrows: lanes + streak + control cursor are disjoint fields.
        let usr = &self.usr;
        let core = &self.core;
        let aging_cap = self.aging_cap;
        let streak = &mut self.consec_control;

        macro_rules! take_control {
            ($c:expr) => {{
                // Guarded increment: streak never exceeds the cap, so it
                // cannot overflow.
                if aging_cap != 0 && *streak < aging_cap {
                    *streak += 1;
                }
                return Some(Received::Control($c));
            }};
        }
        macro_rules! take_user {
            ($u:expr) => {{
                *streak = 0;
                return Some(Received::User($u));
            }};
        }

        loop {
            // Aging safety net: the streak reached the cap and a user is
            // waiting — serve it before any more control.
            if aging_cap != 0 && *streak >= aging_cap {
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
            }
            if let Some(c) = pop_control!(self) {
                take_control!(c);
            }
            if let Some(u) = usr.pop() {
                take_user!(u);
            }

            let ctl_closed = self.ctl.closed();
            let usr_closed = usr.closed();
            if ctl_closed && usr_closed {
                // Both lanes closed: no item can arrive anymore. Sweep TWICE:
                // items pushed by departing senders are visible here via the
                // `senders` RMW release chain; under loom's (deliberately
                // over-approximating) visibility model a single read may
                // still come back stale, and a stale read cannot repeat
                // within one activation. The second pass is free on
                // hardware — it happens once per channel, at teardown.
                if let Some(c) = pop_control!(self) {
                    take_control!(c);
                }
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
                if let Some(c) = pop_control!(self) {
                    take_control!(c);
                }
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
                return None;
            }

            // Park. Register FIRST, announce parked (SeqCst — pairs with the
            // producers' SeqCst `parked` load), then re-check: any push or
            // last-sender drop after the re-check either wakes this
            // registration or is seen by the re-check itself.
            let notified = core.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            core.parked.store(true, Ordering::SeqCst);

            if let Some(c) = pop_control!(self) {
                core.parked.store(false, Ordering::Relaxed);
                take_control!(c);
            }
            if let Some(u) = usr.pop() {
                core.parked.store(false, Ordering::Relaxed);
                take_user!(u);
            }
            if self.ctl.closed() && usr.closed() {
                core.parked.store(false, Ordering::Relaxed);
                continue; // loop around to the sweep-and-`None` path
            }
            // Release one parked producer before sleeping: with the strided
            // in-pop `waiting` check, this is what guarantees a producer
            // parked on a just-drained ring is always woken (its push then
            // wakes us via `parked`).
            usr.release_one_waiter();
            notified.await;
            core.parked.store(false, Ordering::Relaxed);
        }
    }

    /// Blocking twin of `recv`, compiled ONLY under `cfg(loom)`: the same
    /// policy loop and the same `parked`-flag Dekker protocol, with the
    /// sync `Notify` stand-in's `wait()` in place of the `Notified`
    /// future. Drives the loom model in `tests/loom.rs`.
    #[cfg(loom)]
    #[doc(hidden)]
    pub fn recv_blocking(&mut self) -> Option<Received<C, U>> {
        let usr = &self.usr;
        let core = &self.core;
        let aging_cap = self.aging_cap;
        let streak = &mut self.consec_control;

        macro_rules! take_control {
            ($c:expr) => {{
                if aging_cap != 0 && *streak < aging_cap {
                    *streak += 1;
                }
                return Some(Received::Control($c));
            }};
        }
        macro_rules! take_user {
            ($u:expr) => {{
                *streak = 0;
                return Some(Received::User($u));
            }};
        }

        loop {
            if aging_cap != 0 && *streak >= aging_cap {
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
            }
            if let Some(c) = pop_control!(self) {
                take_control!(c);
            }
            if let Some(u) = usr.pop() {
                take_user!(u);
            }

            let ctl_closed = self.ctl.closed();
            let usr_closed = usr.closed();
            if ctl_closed && usr_closed {
                // Both lanes closed. Synchronize with the departing
                // senders' `notify_if` locks so their publishes are visible
                // to the sweep (loom's visibility model does not honor the
                // atomic release chain alone); then sweep TWICE (see the
                // async `recv` for why).
                core.notify.sync_point();
                if let Some(c) = pop_control!(self) {
                    take_control!(c);
                }
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
                if let Some(c) = pop_control!(self) {
                    take_control!(c);
                }
                if let Some(u) = usr.pop() {
                    take_user!(u);
                }
                return None;
            }

            // Park: announce parked FIRST (SeqCst), then register — the
            // stand-in's `enable` lock is the synchronization point that
            // publishes the announcement to `notify_if` (and vice versa
            // for the re-checks below).
            core.parked.store(true, Ordering::SeqCst);
            core.notify.enable();

            if let Some(c) = pop_control!(self) {
                core.parked.store(false, Ordering::Relaxed);
                take_control!(c);
            }
            if let Some(u) = usr.pop() {
                core.parked.store(false, Ordering::Relaxed);
                take_user!(u);
            }
            if self.ctl.closed() && usr.closed() {
                core.parked.store(false, Ordering::Relaxed);
                continue;
            }
            usr.release_one_waiter();
            core.notify.wait();
            core.parked.store(false, Ordering::Relaxed);
        }
    }

    /// Consume the consumer and return everything still queued on both lanes,
    /// in FIFO order (P6 teardown seam).
    ///
    /// # Teardown race (known limitation)
    ///
    /// Only *queued* items are returned. A `UserSender::send` parked on a full
    /// lane at this moment completes `Ok` — teardown wakes it and it either
    /// pushes into the orphaned ring or discards its item — but it appears
    /// neither here nor back at the sender. Callers needing hard delivery
    /// guarantees across teardown must stop accepting sends BEFORE draining
    /// (or treat a send racing `drain` as maybe-undelivered). Pinned by
    /// `edge_cases::drain_teardown_race_discards_blocked_sender_item`.
    #[must_use]
    pub fn drain(mut self) -> Drained<C, U> {
        self.teardown();
        let mut control: Vec<C> = Vec::new();
        while let Some(c) = self.pop_control() {
            control.push(c);
        }
        let mut user = Vec::new();
        let mut head = self.usr.head.load(Ordering::Relaxed);
        let tail = self.usr.tail.load(Ordering::Acquire);
        while head < tail {
            // SAFETY: `head & mask` is always in bounds.
            let slot = unsafe { self.usr.ring.get_unchecked(head & self.usr.mask()) };
            if slot.seq.load(Ordering::Acquire) != (head as u32).wrapping_add(1) {
                break;
            }
            // SAFETY: published and unconsumed; the consumer is being torn
            // down, so no other popper exists.
            user.push(unsafe { slot.val.get().read().assume_init() });
            head = head.wrapping_add(1);
        }
        self.usr.head.store(head, Ordering::Relaxed);
        // Republish the consumed ticket past everything collected above, so
        // the lane's `Drop` does not reclaim items we just moved out.
        self.ctl.consumed.store(self.ctl_head, Ordering::Release);
        Drained { control, user }
    }

    /// Idempotent teardown: flag the consumer gone, publish the consumed
    /// control ticket so the lane's `Drop` reclaims exactly the unconsumed
    /// tail, then release parked user senders (their pending park resolves
    /// to `Ok`, item discarded) and wake any parked `recv`.
    fn teardown(&self) {
        self.ctl.consumed.store(self.ctl_head, Ordering::Release);
        if self.core.consumer_gone.swap(true, Ordering::AcqRel) {
            return;
        }
        self.usr.send_notify.notify_waiters();
        self.core.notify.notify_one();
    }
}

impl<C, U> Drop for Consumer<C, U> {
    fn drop(&mut self) {
        self.teardown();
    }
}

/// Build a two-lane priority channel: an unbounded control lane and a bounded
/// user lane feeding one [`Consumer`].
#[must_use]
pub fn channel<C, U>(cfg: Config) -> (ControlSender<C>, UserSender<U>, Consumer<C, U>) {
    // A capacity-0 (rendezvous) config is served by buffer slots: the FIFO
    // and no-loss semantics are identical, and the send side still paces on
    // the consumer taking items. The ring rounds capacity UP to a power of
    // two with a floor of 2 — the Vyukov ticket for "free next cycle"
    // (`pos + ring_len`) must be distinct from "published" (`pos + 1`),
    // which collapses at ring_len == 1 — and the rounded size IS the
    // effective capacity (in-ring backpressure).
    let capacity = cfg.user_capacity.max(2).next_power_of_two();
    assert!(
        capacity < (1 << 31),
        "user_capacity {capacity} exceeds the u32 ticket window"
    );
    let ring_len = capacity;
    let ring = (0..ring_len)
        .map(|i| Slot {
            seq: AtomicU32::new(i as u32),
            val: std::cell::UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let core = Arc::new(Core {
        notify: Notify::new(),
        parked: AtomicBool::new(false),
        consumer_gone: AtomicBool::new(false),
    });
    let first_block = Box::into_raw(CBlock::new(0));
    let ctl = Arc::new(ControlLane {
        tail: AtomicUsize::new(0),
        tail_block: AtomicPtr::new(first_block),
        first_block: AtomicPtr::new(first_block),
        free_anchor: AtomicPtr::new(first_block),
        in_flight: AtomicUsize::new(0),
        consumed: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        core: core.clone(),
    });
    let usr = Arc::new(UserLane {
        ring,
        tail: AtomicUsize::new(0),
        head: AtomicUsize::new(0),
        send_notify: Notify::new(),
        waiting: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        core: core.clone(),
    });
    (
        ControlSender { lane: ctl.clone() },
        UserSender { lane: usr.clone() },
        Consumer {
            ctl,
            usr,
            core,
            aging_cap: cfg.aging_cap,
            consec_control: 0,
            ctl_head: 0,
            ctl_block: first_block,
        },
    )
}
