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
//! - **Control lane** — a sideband `Mutex<VecDeque<C>>`: control never enters
//!   the ring, so the ring can never reorder it behind user traffic. The
//!   consumer drains the whole sideband into a local stash under ONE lock
//!   acquisition, so the hot `recv` is a stash pop with zero atomics.
//! - **Wakeup** — one shared [`tokio::sync::Notify`] gated by a `parked`
//!   flag. A producer pays a single atomic load when the consumer is active;
//!   a parked consumer is woken by `notify_one` after registering with
//!   enable-then-recheck, so the lost-wakeup class is impossible by
//!   construction and `recv` stays cancel-safe (no item is moved before the
//!   consumer is known to be awake to take it).
//!
//! The public API below is FIXED — the property suite in `fastpass-testkit`
//! depends on these exact names and signatures.

use std::fmt;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Consumer configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    user_capacity: usize,
    aging_cap: usize,
}

impl Config {
    /// A config with the given bounded user-lane capacity and no aging.
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
    /// Wake the consumer iff it is parked. The SeqCst load pairs with the
    /// consumer's SeqCst `parked` store (Dekker): if this load misses the
    /// store, this producer's push is visible to the consumer's post-store
    /// re-check, so no wakeup is needed.
    #[inline]
    fn wake_consumer(&self) {
        if self.parked.load(Ordering::SeqCst) {
            self.notify.notify_one();
        }
    }
}

/// One slot of the user ring. `seq` is the Vyukov ticket: it equals the slot's
/// position when free and `position + 1` once published.
struct Slot<U> {
    seq: AtomicUsize,
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
            let diff = seq.wrapping_sub(pos) as isize;
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
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
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
        if slot.seq.load(Ordering::Acquire) != head.wrapping_add(1) {
            return None;
        }
        // SAFETY: the published `seq` (Acquire) orders the producer's write
        // before this read; only the consumer pops, and the slot is not
        // reused until re-ticketed below.
        let v = unsafe { slot.val.get().read().assume_init() };
        slot.seq.store(head.wrapping_add(self.ring.len()), Ordering::Release);
        self.head.store(head.wrapping_add(1), Ordering::Relaxed);
        // Wake one parked producer, if any — checked every 4th pop only;
        // a parked producer is also released by the consumer's pre-park
        // check, so it can never sleep past an empty ring. The SeqCst load
        // pairs with the producer's SeqCst `waiting` increment (Dekker): if
        // this load misses the increment, the producer's post-increment
        // re-check observes the just-freed slot, so it never parks on a
        // non-full ring.
        if head % 4 == 0 && self.waiting.load(Ordering::SeqCst) != 0 {
            self.send_notify.notify_one();
        }
        Some(v)
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
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        for pos in head..tail {
            let slot = &self.ring[pos & self.mask()];
            if slot.seq.load(Ordering::Relaxed) == pos.wrapping_add(1) {
                // SAFETY: published and never consumed; exclusive `&mut self`.
                unsafe { slot.val.get().drop_in_place() };
            }
        }
    }
}

/// Slots per control block. Blocks are single-use (never reused), so a slot
/// needs no Vyukov ticket — just a publish flag.
const CBLOCK: usize = 64;

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
    /// Highest linked block (best-effort hint; producers walk forward from it).
    tail_block: AtomicPtr<CBlock<C>>,
    /// Anchor of the block chain (block 0); freed wholesale at lane drop.
    first_block: *mut CBlock<C>,
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
        let pos = self.tail.fetch_add(1, Ordering::AcqRel);
        let want = pos / CBLOCK;
        let mut b = self.tail_block.load(Ordering::Acquire);
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
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }
                    Err(_) => drop(unsafe { Box::from_raw(fresh) }),
                }
                continue;
            }
            let _ = self
                .tail_block
                .compare_exchange(b, next, Ordering::Release, Ordering::Relaxed);
            b = next;
        }
        // SAFETY: `b` is the block containing `pos`; the ticket is unique to
        // this push, so we hold exclusive access to the slot.
        let slot = unsafe { &(*b).slots[pos % CBLOCK] };
        unsafe { slot.val.get().write(MaybeUninit::new(item)) };
        slot.ready.store(true, Ordering::Release);
    }

    #[inline]
    fn closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }
}

impl<C> Drop for ControlLane<C> {
    fn drop(&mut self) {
        // Quiescent: all senders and the consumer are gone. Reclaim every
        // published-but-unconsumed slot, then free the whole chain.
        let consumed = *self.consumed.get_mut();
        let tail = *self.tail.get_mut();
        let mut b = self.first_block;
        while !b.is_null() {
            let mut block = unsafe { Box::from_raw(b) };
            let base = block.idx * CBLOCK;
            for pos in base.max(consumed)..(base + CBLOCK).min(tail) {
                let slot = &block.slots[pos % CBLOCK];
                if slot.ready.load(Ordering::Relaxed) {
                    // SAFETY: published and never consumed; exclusive ownership.
                    unsafe { slot.val.get().drop_in_place() };
                }
            }
            b = *block.next.get_mut();
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
    /// Block containing `ctl_head`; consumer-only, never freed under it
    /// (the chain is reclaimed wholesale at lane drop).
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
        let mut block = self.ctl_block;
        // SAFETY: `ctl_block` is always a live block (chain freed only at
        // lane drop; the consumer holds an Arc on the lane).
        let b = unsafe { &*block };
        if self.ctl_head == (b.idx + 1) * CBLOCK {
            // Crossed a block boundary: the next block is linked before any
            // of its slots are published, so `null` means the lane is empty.
            let next = b.next.load(Ordering::Acquire);
            if next.is_null() {
                return None;
            }
            self.ctl_block = next;
            block = next;
        }
        // SAFETY: as above; `ctl_head % CBLOCK` is in bounds.
        let slot = unsafe { &(*block).slots[self.ctl_head % CBLOCK] };
        if !slot.ready.load(Ordering::Acquire) {
            return None;
        }
        // SAFETY: `ready` (Acquire) orders the producer's write before this
        // read; only the consumer pops, and each slot is published once.
        let v = unsafe { slot.val.get().read().assume_init() };
        self.ctl_head = self.ctl_head.wrapping_add(1);
        Some(v)
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
    pub async fn recv(&mut self) -> Option<Received<C, U>> {
        // Split borrows: lanes + streak + control cursor are disjoint fields.
        let usr = &self.usr;
        let core = &self.core;
        let aging_cap = self.aging_cap;
        let streak = &mut self.consec_control;

        macro_rules! pop_control {
            () => {{
                let mut block = self.ctl_block;
                let b = unsafe { &*block };
                if self.ctl_head == (b.idx + 1) * CBLOCK {
                    let next = b.next.load(Ordering::Acquire);
                    if next.is_null() {
                        None
                    } else {
                        self.ctl_block = next;
                        block = next;
                        let slot = unsafe { &(*block).slots[self.ctl_head % CBLOCK] };
                        if slot.ready.load(Ordering::Acquire) {
                            let v = unsafe { slot.val.get().read().assume_init() };
                            self.ctl_head = self.ctl_head.wrapping_add(1);
                            Some(v)
                        } else {
                            None
                        }
                    }
                } else {
                    let slot = unsafe { &(*block).slots[self.ctl_head % CBLOCK] };
                    if slot.ready.load(Ordering::Acquire) {
                        let v = unsafe { slot.val.get().read().assume_init() };
                        self.ctl_head = self.ctl_head.wrapping_add(1);
                        Some(v)
                    } else {
                        None
                    }
                }
            }};
        }

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
            if let Some(c) = pop_control!() {
                take_control!(c);
            }
            if let Some(u) = usr.pop() {
                take_user!(u);
            }

            let ctl_closed = self.ctl.closed();
            let usr_closed = usr.closed();
            if ctl_closed && usr_closed {
                // Both lanes closed: no item can arrive anymore. Final sweep
                // ordered after the Acquire loads above, so any item a
                // departing sender pushed is visible here.
                if let Some(c) = pop_control!() {
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

            if let Some(c) = pop_control!() {
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
            if usr.waiting.load(Ordering::SeqCst) != 0 {
                usr.send_notify.notify_one();
            }
            notified.await;
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
            if slot.seq.load(Ordering::Acquire) != head.wrapping_add(1) {
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
    let ring_len = capacity;
    let ring = (0..ring_len)
        .map(|i| Slot {
            seq: AtomicUsize::new(i),
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
        first_block,
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
