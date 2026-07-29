//! Two-lane priority merge (bombay card #225) — **kimi's target crate**.
//!
//! Merge a **control** lane and a **user** lane into one [`Consumer`] so that
//! control signals are served ahead of a user backlog, while keeping FIFO
//! per lane, no loss, no lost wakeups, a clean teardown drain, a zero-alloc
//! steady-state send, and (the upgrade) no user starvation under a control
//! flood.
//!
//! The public API below is FIXED — the property suite in `fastpass-testkit`
//! depends on these exact names and signatures. The one piece that must change
//! is the dequeue policy in [`Consumer::recv`], which currently ships a naive
//! **user-biased** stub. Grow it until every property passes; see
//! `.plans/fastpass-twolane.md` and the gold `fastpass-reference` for the
//! target behaviour (strict priority + anti-starvation aging cap).

use std::marker::PhantomData;

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

/// Cloneable handle to the control lane. `send` never blocks.
pub struct ControlSender<C> {
    tx: flume::Sender<C>,
}

impl<C> Clone for ControlSender<C> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<C> ControlSender<C> {
    /// Enqueue a control signal. Never blocks (the lane is unbounded).
    ///
    /// # Errors
    /// Returns [`ControlClosed`] carrying `item` if the consumer is gone.
    pub fn send(&self, item: C) -> Result<(), ControlClosed<C>> {
        self.tx.send(item).map_err(|flume::SendError(v)| ControlClosed(v))
    }
}

/// Cloneable handle to the bounded user lane.
pub struct UserSender<U> {
    tx: flume::Sender<U>,
}

impl<U> Clone for UserSender<U> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<U> UserSender<U> {
    /// Enqueue a user message, awaiting capacity (backpressure).
    ///
    /// # Errors
    /// Returns [`UserClosed`] carrying `item` if the consumer is gone.
    pub async fn send(&self, item: U) -> Result<(), UserClosed<U>> {
        self.tx.send_async(item).await.map_err(|flume::SendError(v)| UserClosed(v))
    }

    /// Enqueue a user message without blocking.
    ///
    /// # Errors
    /// [`TrySendError::Full`] if the lane is at capacity, [`TrySendError::Closed`]
    /// if the consumer is gone — each carrying `item` back.
    pub fn try_send(&self, item: U) -> Result<(), TrySendError<U>> {
        self.tx.try_send(item).map_err(|e| match e {
            flume::TrySendError::Full(v) => TrySendError::Full(v),
            flume::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
    }
}

/// The single consumer that merges the two lanes.
pub struct Consumer<C, U> {
    crx: flume::Receiver<C>,
    urx: flume::Receiver<U>,
    aging_cap: usize,
    consec_control: usize,
    ctl_closed: bool,
    usr_closed: bool,
    _marker: PhantomData<(C, U)>,
}

impl<C, U> Consumer<C, U> {
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
        loop {
            // Aging safety net: the streak reached the cap and a user is
            // waiting — serve it before any more control.
            if self.aging_cap != 0 && self.consec_control >= self.aging_cap {
                match self.urx.try_recv() {
                    Ok(u) => {
                        self.consec_control = 0;
                        return Some(Received::User(u));
                    }
                    Err(flume::TryRecvError::Disconnected) => self.usr_closed = true,
                    Err(flume::TryRecvError::Empty) => {}
                }
            }
            match self.crx.try_recv() {
                Ok(c) => {
                    // Guarded increment: streak never exceeds the cap, so it
                    // cannot overflow.
                    if self.aging_cap != 0 && self.consec_control < self.aging_cap {
                        self.consec_control += 1;
                    }
                    return Some(Received::Control(c));
                }
                Err(flume::TryRecvError::Disconnected) => self.ctl_closed = true,
                Err(flume::TryRecvError::Empty) => {}
            }
            match self.urx.try_recv() {
                Ok(u) => {
                    self.consec_control = 0;
                    return Some(Received::User(u));
                }
                Err(flume::TryRecvError::Disconnected) => self.usr_closed = true,
                Err(flume::TryRecvError::Empty) => {}
            }
            if self.ctl_closed && self.usr_closed {
                return None;
            }
            tokio::select! {
                biased;
                c = self.crx.recv_async(), if !self.ctl_closed => match c {
                    Ok(c) => {
                        if self.aging_cap != 0 && self.consec_control < self.aging_cap {
                            self.consec_control += 1;
                        }
                        return Some(Received::Control(c));
                    }
                    Err(_) => self.ctl_closed = true,
                },
                u = self.urx.recv_async(), if !self.usr_closed => match u {
                    Ok(u) => {
                        self.consec_control = 0;
                        return Some(Received::User(u));
                    }
                    Err(_) => self.usr_closed = true,
                },
            }
        }
    }

    /// Consume the consumer and return everything still queued on both lanes,
    /// in FIFO order (P6 teardown seam).
    ///
    /// # Teardown race (known limitation)
    ///
    /// Only *queued* items are returned. A `UserSender::send` parked on a full
    /// lane at this moment completes `Ok` — flume pulls parked sends into the
    /// queue on disconnect — but that item is stranded in the receiverless
    /// queue: it appears neither here nor back at the sender. Callers needing
    /// hard delivery guarantees across teardown must stop accepting sends
    /// BEFORE draining (or treat a send racing `drain` as maybe-undelivered).
    /// Pinned by `edge_cases::drain_teardown_race_discards_blocked_sender_item`.
    #[must_use]
    pub fn drain(self) -> Drained<C, U> {
        let control = self.crx.drain().collect();
        let user = self.urx.drain().collect();
        Drained { control, user }
    }
}

/// Build a two-lane priority channel: an unbounded control lane and a bounded
/// user lane feeding one [`Consumer`].
#[must_use]
pub fn channel<C, U>(cfg: Config) -> (ControlSender<C>, UserSender<U>, Consumer<C, U>) {
    let (ctx, crx) = flume::unbounded();
    let (utx, urx) = flume::bounded(cfg.user_capacity);
    (
        ControlSender { tx: ctx },
        UserSender { tx: utx },
        Consumer {
            crx,
            urx,
            aging_cap: cfg.aging_cap,
            consec_control: 0,
            ctl_closed: false,
            usr_closed: false,
            _marker: PhantomData,
        },
    )
}
