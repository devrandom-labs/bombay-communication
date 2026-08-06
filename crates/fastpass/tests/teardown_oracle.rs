//! Frozen correctness oracle: blocked-producer teardown payload recovery.
//!
//! When a user send is blocked on a FULL ring and the consumer is dropped,
//! the send must resolve `Err(UserClosed(payload))` with its EXACT move-only
//! payload — never `Ok(())` with the payload discarded (the historical
//! "pinned teardown seam" this oracle replaces). Every test here uses
//! barriers, yields, and protocol state — never sleeps or timing guesses.
//!
//! # The linearization rule (the reference model)
//!
//! A send's linearization point is the ring-slot publish: the `Release`
//! store of the slot ticket inside `UserLane::try_push`.
//!
//! - **Enqueued** — the publish completed before the send observed
//!   teardown: the send resolves `Ok(())` and ownership of the payload
//!   transfers to the mailbox. Teardown either returns it from
//!   [`Consumer::drain`] or drops it with the lane. It is never returned
//!   to the sender, never leaked, never double-dropped.
//! - **Returned** — no publish happened: the send must resolve
//!   `Err(UserClosed(payload))` with its exact payload, exactly once,
//!   whether the consumer was already gone when the send started, the
//!   teardown raced the waiter-registration seam, released a parked
//!   waiter, or landed immediately after a wakeup.
//!
//! [`teardown_model`] encodes this rule for the deterministic scenario each
//! test drives; every assertion compares the SUT against the model. The
//! interleaving-heavy race windows (registration/recheck seam, post-wakeup)
//! are model-checked exhaustively in `loom.rs`; ownership, drop counts, and
//! reclamation are additionally interpreted under Miri.

use fastpass::{Config, Received, TrySendError, UserClosed, channel};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::yield_now;
use tokio::time::timeout;

const GUARD: Duration = Duration::from_secs(5);

/// Shared drop accounting for the move-only probe payloads.
#[derive(Debug, Default)]
struct Drops {
    /// Constructed minus dropped: must return to 0 after full shutdown.
    live: AtomicUsize,
    /// Dropped, ever: proves exactly-once drop when it equals constructions.
    total: AtomicUsize,
}

/// A move-only, drop-counted payload: deliberately not `Clone`/`Copy`, so
/// every ownership transfer is exact and every construction/drop is counted.
#[derive(Debug)]
struct Probe {
    id: u64,
    drops: Arc<Drops>,
}

impl Probe {
    fn new(id: u64, drops: &Arc<Drops>) -> Self {
        drops.live.fetch_add(1, Ordering::SeqCst);
        Self {
            id,
            drops: drops.clone(),
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.live.fetch_sub(1, Ordering::SeqCst);
        self.drops.total.fetch_add(1, Ordering::SeqCst);
    }
}

/// How a send racing consumer teardown must resolve (the reference model's
/// per-send classification — see the module docs for the rule).
#[derive(Debug, PartialEq, Eq)]
enum Fate {
    /// Linearized into the ring before teardown: resolves `Ok(())`; the
    /// mailbox owns the payload (drained or dropped with the lane).
    Enqueued(u64),
    /// Never linearized: must resolve `Err(UserClosed(payload))` with this
    /// exact payload id, exactly once.
    Returned(u64),
}

/// The teardown reference model for a ring pre-filled to capacity with no
/// consumer pops: every pre-filled send is `Enqueued`, every blocked send is
/// `Returned` — no blocked send can publish without a pop freeing a slot.
fn teardown_model(prefilled: &[u64], blocked: &[u64]) -> Vec<Fate> {
    prefilled
        .iter()
        .map(|&id| Fate::Enqueued(id))
        .chain(blocked.iter().map(|&id| Fate::Returned(id)))
        .collect()
}

/// Drive a current-thread runtime until every spawned send has run to its
/// park point. One yield per scheduled wakeup: the sends have no pending
/// external event, so a bounded yield count is deterministic — not a sleep.
async fn settle(tasks: &[tokio::task::JoinHandle<Result<(), UserClosed<Probe>>>]) {
    for _ in 0..8 {
        yield_now().await;
    }
    assert!(
        tasks.iter().all(|t| !t.is_finished()),
        "a send resolved before teardown — the scenario is not the blocked race it pins"
    );
}

/// 1+2. A producer blocked by a full user ring receives
/// `Err(UserClosed(payload))` when the consumer drops; the exact move-only
/// payload is returned once — neither leaked nor double-dropped.
#[tokio::test]
async fn blocked_send_on_full_ring_recovers_exact_payload_on_consumer_drop() {
    let drops = Arc::new(Drops::default());
    let (ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.try_send(Probe::new(1, &drops)).expect("ring empty");
    usr.try_send(Probe::new(2, &drops)).expect("capacity 2");

    let barrier = Arc::new(Barrier::new(2));
    let blocked = tokio::spawn({
        let usr = usr.clone();
        let barrier = barrier.clone();
        let drops = drops.clone();
        async move {
            barrier.wait().await;
            usr.send(Probe::new(3, &drops)).await
        }
    });
    barrier.wait().await;
    settle(std::slice::from_ref(&blocked)).await;

    drop(rx); // consumer teardown: releases the parked producer
    let err = timeout(GUARD, blocked)
        .await
        .expect("blocked sender never released — lost wakeup at teardown")
        .expect("producer task panicked")
        .expect_err("teardown must reject the blocked send, not report Ok");
    assert_eq!(
        teardown_model(&[1, 2], &[3]),
        vec![
            Fate::Enqueued(1),
            Fate::Enqueued(2),
            Fate::Returned(err.0.id)
        ],
        "send fates diverge from the teardown model"
    );
    drop(err); // the returned payload, dropped exactly once here

    drop((ctl, usr)); // full shutdown: the lane drops the two enqueued payloads
    assert_eq!(
        drops.total.load(Ordering::SeqCst),
        3,
        "payload dropped more or fewer than once"
    );
    assert_eq!(
        drops.live.load(Ordering::SeqCst),
        0,
        "payload leaked across shutdown"
    );
}

/// 3. A send that linearized into the ring before teardown resolves `Ok(())`
/// and teardown owns the payload: `drain()` returns it, or the lane drops it.
#[tokio::test]
async fn enqueued_before_teardown_stays_ok_and_teardown_owns_the_payload() {
    let drops = Arc::new(Drops::default());

    // Drain path: the queued payload is RETURNED by drain.
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.send(Probe::new(1, &drops))
        .await
        .expect("consumer alive");
    let mut drained = rx.drain();
    assert_eq!(drained.user.len(), 1, "drain lost the enqueued payload");
    assert_eq!(drained.user.pop().expect("len checked").id, 1);
    drop((drained, usr));

    // Drop path: the queued payload is dropped with the lane, exactly once.
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.send(Probe::new(2, &drops))
        .await
        .expect("consumer alive");
    drop((rx, usr)); // no drain: the lane's Drop reclaims the queued payload

    assert_eq!(drops.total.load(Ordering::SeqCst), 2);
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}

/// 4. A send that starts after teardown (or before waiter registration can
/// matter) never linearizes and returns its payload — synchronously for
/// `try_send`, on the first poll for `send`.
#[tokio::test]
async fn send_starting_after_teardown_returns_its_payload() {
    let drops = Arc::new(Drops::default());
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    drop(rx);

    let err = usr
        .send(Probe::new(1, &drops))
        .await
        .expect_err("consumer is gone");
    assert_eq!(err.0.id, 1);
    match usr.try_send(Probe::new(2, &drops)) {
        Err(TrySendError::Closed(p)) => assert_eq!(p.id, 2),
        other => panic!("expected Closed with the payload, got {other:?}"),
    }
    drop((err, usr));

    assert_eq!(drops.total.load(Ordering::SeqCst), 2);
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}

/// 5+9. Multiple producers blocked on the same full ring are ALL released by
/// teardown, each with its own exact payload — no lost wakeup, no producer
/// left parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_blocked_producer_recovers_its_own_payload() {
    const PRODUCERS: u64 = 4;
    let drops = Arc::new(Drops::default());
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.try_send(Probe::new(1, &drops)).expect("ring empty");
    usr.try_send(Probe::new(2, &drops)).expect("capacity 2");

    let barrier = Arc::new(Barrier::new(usize::try_from(PRODUCERS).expect("small") + 1));
    let mut tasks = Vec::new();
    for id in 10..10 + PRODUCERS {
        let usr = usr.clone();
        let barrier = barrier.clone();
        let drops = drops.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            (id, usr.send(Probe::new(id, &drops)).await)
        }));
    }
    barrier.wait().await;
    // All four must reach the park before teardown — a settled (finished)
    // task here would mean the ring admitted a fifth item or the send
    // resolved spuriously, both contract violations.
    for _ in 0..8 {
        yield_now().await;
    }
    assert!(
        tasks.iter().all(|t| !t.is_finished()),
        "a producer resolved before teardown"
    );

    drop(rx);
    let mut returned = Vec::new();
    for task in tasks {
        let (id, result) = timeout(GUARD, task)
            .await
            .expect("a producer stayed parked past teardown — lost wakeup")
            .expect("producer task panicked");
        let err = result.expect_err("teardown must reject every blocked send");
        assert_eq!(err.0.id, id, "a producer got another producer's payload");
        returned.push(err.0.id);
    }
    returned.sort_unstable();
    let expected: Vec<Fate> = teardown_model(&[1, 2], &[10, 11, 12, 13])
        .into_iter()
        .filter(|f| matches!(f, Fate::Returned(_)))
        .collect();
    assert_eq!(
        returned,
        expected
            .iter()
            .map(|f| match f {
                Fate::Returned(id) => *id,
                Fate::Enqueued(_) => unreachable!("filtered above"),
            })
            .collect::<Vec<_>>(),
        "returned payload ids diverge from the teardown model"
    );

    drop(usr);
    assert_eq!(
        drops.total.load(Ordering::SeqCst),
        2 + usize::try_from(PRODUCERS).expect("small")
    );
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}

/// 6. `UserAnchor::send` — through its temporary liveness upgrade — has the
/// same blocked-teardown behavior: released with its exact payload while the
/// upgrade is held, and the temporary liveness is released afterwards.
#[tokio::test]
async fn blocked_anchor_send_recovers_payload_on_consumer_drop() {
    let drops = Arc::new(Drops::default());
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    let anchor = usr.anchor();
    usr.try_send(Probe::new(1, &drops)).expect("ring empty");
    usr.try_send(Probe::new(2, &drops)).expect("capacity 2");

    let barrier = Arc::new(Barrier::new(2));
    let blocked = tokio::spawn({
        let anchor = anchor.clone();
        let barrier = barrier.clone();
        let drops = drops.clone();
        async move {
            barrier.wait().await;
            anchor.send(Probe::new(3, &drops)).await
        }
    });
    barrier.wait().await;
    settle(std::slice::from_ref(&blocked)).await;

    drop(rx);
    let err = timeout(GUARD, blocked)
        .await
        .expect("blocked anchor send never released")
        .expect("producer task panicked")
        .expect_err("teardown must reject the blocked anchor send");
    assert_eq!(err.0.id, 3);
    drop(err);

    // The temporary upgrade was released: once the last counting sender is
    // gone, the anchor can never upgrade again.
    drop(usr);
    assert!(
        anchor.upgrade().is_none(),
        "the blocked send leaked its temporary liveness"
    );
    drop(anchor);
    assert_eq!(drops.total.load(Ordering::SeqCst), 3);
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}

/// 8 (cancellation safety). Cancelling a blocked send drops its payload
/// exactly once — the new teardown path changes nothing about futures dropped
/// mid-park.
#[tokio::test]
async fn cancelled_blocked_send_drops_payload_exactly_once() {
    let drops = Arc::new(Drops::default());
    let (_ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.try_send(Probe::new(1, &drops)).expect("ring empty");
    usr.try_send(Probe::new(2, &drops)).expect("capacity 2");

    let barrier = Arc::new(Barrier::new(2));
    let blocked = tokio::spawn({
        let usr = usr.clone();
        let barrier = barrier.clone();
        let drops = drops.clone();
        async move {
            barrier.wait().await;
            usr.send(Probe::new(3, &drops)).await
        }
    });
    barrier.wait().await;
    settle(std::slice::from_ref(&blocked)).await;

    blocked.abort();
    assert!(
        blocked.await.unwrap_err().is_cancelled(),
        "abort must cancel the parked send"
    );
    // The parked future owned the payload; cancelling drops it in place.
    assert_eq!(drops.total.load(Ordering::SeqCst), 1, "cancel dropped it");
    assert_eq!(drops.live.load(Ordering::SeqCst), 2);

    drop((rx, usr));
    assert_eq!(drops.total.load(Ordering::SeqCst), 3);
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}

/// 7. Last-sender closure and the `UserLaneClosed` end-marker are unchanged:
/// a consumer left alive still drains the ring, delivers the marker exactly
/// once, and only then reports `None` with both lanes closed.
#[tokio::test]
async fn last_sender_closure_and_user_lane_closed_marker_unchanged() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(4));
    usr.send(1).await.expect("consumer alive");
    usr.send(2).await.expect("consumer alive");
    drop(usr); // last counting sender: the lane closes terminally

    assert_eq!(
        timeout(GUARD, rx.recv()).await.expect("recv hung"),
        Some(Received::User(1))
    );
    assert_eq!(
        timeout(GUARD, rx.recv()).await.expect("recv hung"),
        Some(Received::User(2))
    );
    assert_eq!(
        timeout(GUARD, rx.recv()).await.expect("recv hung"),
        Some(Received::UserLaneClosed),
        "the end-marker must follow the last user item exactly once"
    );
    // Control still flows after the marker.
    ctl.send(9).expect("consumer alive");
    assert_eq!(
        timeout(GUARD, rx.recv()).await.expect("recv hung"),
        Some(Received::Control(9))
    );
    drop(ctl);
    assert_eq!(timeout(GUARD, rx.recv()).await.expect("recv hung"), None);
}

/// 8 (ordering/priority/capacity at teardown). Draining with a producer
/// blocked on the full ring: queued control stays FIFO and ahead of nothing
/// it shouldn't be, queued users stay FIFO, and the blocked producer is
/// released with its payload. Capacity accounting is exact: the drained
/// items plus the returned payload are everything the channel ever accepted.
#[tokio::test]
async fn drain_with_blocked_producer_preserves_ordering_and_releases_payload() {
    let drops = Arc::new(Drops::default());
    let (ctl, usr, rx) = channel::<u32, Probe>(Config::new(2));
    usr.try_send(Probe::new(1, &drops)).expect("ring empty");
    usr.try_send(Probe::new(2, &drops)).expect("capacity 2");
    assert!(
        matches!(usr.try_send(Probe::new(99, &drops)), Err(TrySendError::Full(p)) if p.id == 99),
        "capacity accounting: a full ring must report Full with the payload"
    );

    let barrier = Arc::new(Barrier::new(2));
    let blocked = tokio::spawn({
        let usr = usr.clone();
        let barrier = barrier.clone();
        let drops = drops.clone();
        async move {
            barrier.wait().await;
            usr.send(Probe::new(3, &drops)).await
        }
    });
    barrier.wait().await;
    settle(std::slice::from_ref(&blocked)).await;

    ctl.send(7).expect("consumer alive");
    ctl.send(8).expect("consumer alive");

    let drained = rx.drain();
    let user_ids: Vec<u64> = drained.user.iter().map(|p| p.id).collect();
    assert_eq!(user_ids, vec![1, 2], "drained users must be FIFO");
    assert_eq!(drained.control, vec![7, 8], "drained control must be FIFO");

    let err = timeout(GUARD, blocked)
        .await
        .expect("blocked sender never released")
        .expect("producer task panicked")
        .expect_err("teardown must reject the blocked send");
    assert_eq!(err.0.id, 3);
    drop((drained, err, ctl, usr));

    assert_eq!(drops.total.load(Ordering::SeqCst), 4);
    assert_eq!(drops.live.load(Ordering::SeqCst), 0);
}
