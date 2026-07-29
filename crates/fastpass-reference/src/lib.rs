//! Gold reference for the two-lane priority merge (bombay card #225).
//!
//! # The problem, in the abstract
//!
//! One consumer, two FIFO producers: a **control** lane (runtime signals) and a
//! **user** lane (domain messages). We want *all* of:
//!
//! - **P1** control latency independent of user-queue depth (priority),
//! - **P2** FIFO within each lane,
//! - **P3** the user lane always makes progress (starvation-freedom),
//! - **P4** no loss, **P5** no lost wakeups, **P6** a clean teardown drain,
//! - **P7** a zero-alloc steady-state hot path,
//!
//! with the single, deliberately-accepted relaxation that a control item may
//! **overtake** an earlier user item (there is no cross-lane total order — that
//! *is* the feature; see EEP-76, Erlang/OTP 28).
//!
//! # Why "all upsides, no downsides" is achievable here
//!
//! Strict priority queueing is textbook (network QoS) and gives P1 exactly, but
//! its textbook downside is that a busy high-priority class *starves* the low
//! one — breaking P3. We neutralise that with an **aging cap** `K`: after `K`
//! consecutive control dequeues, one waiting user is forced through. This is a
//! degenerate deficit-round-robin (Shreedhar & Varghese, SIGCOMM '95) tuned so
//! the priority class effectively always wins:
//!
//! - In normal operation the control lane is **rate-bounded by contract** (a
//!   watcher registers ≤1 signal; a supervisor emits O(actions)), so `K` is
//!   never reached and P1 holds *exactly* — control is served immediately.
//! - Under an adversarial control flood the cap still bounds a user's wait to
//!   `K` control items, so P3 holds *unconditionally*. The only cost is that
//!   one control item per `K` may wait behind a single user service — a
//!   negligible, tunable price paid only in a regime the contract forbids.
//!
//! Set `aging_cap = 0` for pure strict priority (P3 downgrades to "progress
//! only when control is idle").
//!
//! # Mechanism
//!
//! Two flume channels (control unbounded, user bounded) drained by a biased
//! `try_recv`-then-`select!` loop. Flume owns the wakeup/disconnect machinery
//! (P5), so nothing is hand-rolled here (bombay ADR-0001: flume is the seam).

use std::marker::PhantomData;

/// Consumer configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    user_capacity: usize,
    aging_cap: usize,
}

impl Config {
    /// A config with the given bounded user-lane capacity and pure strict
    /// priority (no aging).
    #[must_use]
    pub const fn new(user_capacity: usize) -> Self {
        Self { user_capacity, aging_cap: 0 }
    }

    /// Force one waiting user through after `k` consecutive control dequeues.
    /// `0` disables aging (pure strict priority).
    #[must_use]
    pub const fn with_aging_cap(mut self, k: usize) -> Self {
        self.aging_cap = k;
        self
    }
}

/// The item handed back by [`Consumer::recv`], tagged by its lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received<C, U> {
    /// A control-lane item (served with priority).
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

/// The control lane closed because the [`Consumer`] was dropped. Carries the
/// undelivered item back to the caller.
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

/// The single consumer. Serves control ahead of user, with an aging safety net.
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
    // Record a control dequeue, saturating at the aging cap so the counter can
    // never overflow: the guard keeps `consec_control < aging_cap < usize::MAX`,
    // so the `+= 1` is provably safe (no checked arithmetic needed). When aging
    // is disabled the counter is inert.
    fn note_control(&mut self) {
        if self.aging_cap != 0 && self.consec_control < self.aging_cap {
            self.consec_control += 1;
        }
    }

    /// Receive the next item, control-first.
    ///
    /// Returns `None` only once both lanes are closed (all senders dropped) and
    /// both are empty.
    pub async fn recv(&mut self) -> Option<Received<C, U>> {
        loop {
            // Aging safety net: after K back-to-back controls, let one user
            // through if one is waiting.
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

            // Strict priority: drain control first.
            match self.crx.try_recv() {
                Ok(c) => {
                    self.note_control();
                    return Some(Received::Control(c));
                }
                Err(flume::TryRecvError::Disconnected) => self.ctl_closed = true,
                Err(flume::TryRecvError::Empty) => {}
            }

            // Control empty: serve a ready user.
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

            // Both lanes momentarily empty: park until either delivers. `biased`
            // keeps control preferred if both wake together; flume's
            // `recv_async` provides the wakeup (P5) and disconnect signal.
            tokio::select! {
                biased;
                c = self.crx.recv_async(), if !self.ctl_closed => match c {
                    Ok(c) => { self.note_control(); return Some(Received::Control(c)); }
                    Err(_) => self.ctl_closed = true,
                },
                u = self.urx.recv_async(), if !self.usr_closed => match u {
                    Ok(u) => { self.consec_control = 0; return Some(Received::User(u)); }
                    Err(_) => self.usr_closed = true,
                },
            }
        }
    }

    /// Consume the consumer and return everything still queued on both lanes,
    /// in FIFO order. This is the teardown seam: a caller that stops the
    /// consumer can still answer queued control signals (P6).
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
