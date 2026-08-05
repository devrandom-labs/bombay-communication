//! Edge-case pins for the two-lane merge — the hazards the property suite does
//! not exercise (review 2026-07-29). Each test here defends an observable
//! contract and fails on a plausible regression:
//!
//! 1. `cap_zero_rendezvous_loses_nothing` — `Config::new(0)` makes the user
//!    lane a rendezvous channel, the one configuration where `select!`
//!    cancellation could theoretically drop a value handed straight to a
//!    receiver. Verified against flume 0.12.0 source: a value leaves shared
//!    state only inside a poll that returns `Ready` (`Shared::recv` pops under
//!    the channel lock; `pull_pending` moves parked sends into the queue under
//!    the same lock), and `RecvFut::drop` re-wakes another receiver if its
//!    hook had already fired (`reset_hook`). This test pins that guarantee
//!    empirically so a flume upgrade that breaks it fails loudly.
//! 2. `recv_future_cancellation_loses_nothing` — bombay's mailbox will
//!    `select!` over `recv()` plus a shutdown arm, dropping the `recv` future
//!    mid-park. No item may be lost or duplicated across cancellations.
//! 3. `drain_teardown_race_discards_blocked_sender_item` — `drain()` returns
//!    only what is queued (flume `drain` uses `pull_pending(false)`, so a full
//!    lane never absorbs a parked send). But flume's `disconnect_all` runs
//!    `pull_pending(false)` AGAIN on receiver drop: a send parked on a full
//!    lane at teardown completes `Ok(())` while its item is moved into the
//!    receiverless queue — stranded, neither delivered nor returned. This is a
//!    flume-level semantic `Consumer` cannot intercept (verified against flume
//!    0.12.0: `Shared::send` parks, `disconnect_all` pulls, `SendFut::poll`
//!    reports `Ok` on an emptied hook). The test pins the seam so a flume
//!    behaviour change is noticed; the limitation is documented on `drain()`
//!    and recorded in the ADR.
//! 4. `default_config_is_pure_strict_priority` — `Config::new` ships
//!    `aging_cap = 0`: under a control flood users starve. That is a
//!    deliberate, oracle-fixed default (the P1 test depends on it) — this pins
//!    the semantics so the tradeoff is explicit, not accidental.
//! 5. `closed_empty_recv_returns_none_repeatedly` — pins the load-bearing
//!    early return before the parked `select!`: with both lanes closed both
//!    arms are disabled, and `tokio::select!` with no enabled arm and no
//!    `else` panics. Since the lane-lifecycle addition the one-shot
//!    `UserLaneClosed` end-marker precedes the first `None`.

use fastpass::{Config, Received, channel};
use std::time::Duration;
use tokio::time::timeout;

const GUARD: Duration = Duration::from_secs(5);

#[tokio::test]
async fn cap_zero_rendezvous_loses_nothing() {
    const N: u32 = 200;
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(0).with_aging_cap(3));
    let producer = tokio::spawn(async move {
        for i in 0..N {
            // Rendezvous lane: each send parks until the consumer takes it.
            usr.send(i).await.unwrap();
            ctl.send(i).unwrap();
        }
    });

    let mut got_u = Vec::new();
    let mut got_c = Vec::new();
    while got_u.len() + got_c.len() < (2 * N) as usize {
        match timeout(GUARD, rx.recv())
            .await
            .expect("rendezvous recv stalled")
        {
            Some(Received::User(u)) => got_u.push(u),
            Some(Received::Control(c)) => got_c.push(c),
            // End-marker (post-#225 lane-lifecycle addition): not a payload
            // item; the loop bound counts only payload.
            Some(Received::UserLaneClosed) => {}
            None => break,
        }
    }
    producer.await.unwrap();

    assert_eq!(
        got_u,
        (0..N).collect::<Vec<_>>(),
        "user lane: loss/dup/reorder at cap 0"
    );
    assert_eq!(
        got_c,
        (0..N).collect::<Vec<_>>(),
        "control lane: loss/dup/reorder at cap 0"
    );
}

#[tokio::test]
async fn recv_future_cancellation_loses_nothing() {
    const N: u32 = 500;
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64).with_aging_cap(4));
    let producer = tokio::spawn(async move {
        for i in 0..N {
            ctl.send(i).unwrap();
            usr.send(i).await.unwrap();
            // Force gaps so the consumer regularly outpaces the producer and
            // parks in the select; the periodic 2ms stall (>> the 50µs cancel
            // arm) guarantees the cancellation path fires.
            tokio::task::yield_now().await;
            if i % 16 == 15 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    });

    let mut got_c = Vec::new();
    let mut got_u = Vec::new();
    let mut cancels = 0u32;
    while got_c.len() + got_u.len() < (2 * N) as usize {
        tokio::select! {
            biased;
            x = rx.recv() => match x {
                Some(Received::Control(c)) => got_c.push(c),
                Some(Received::User(u)) => got_u.push(u),
                // End-marker: possible once the producer's senders drop with
                // the ring drained; not a payload item.
                Some(Received::UserLaneClosed) => {}
                None => break,
            },
            () = tokio::time::sleep(Duration::from_micros(50)) => cancels += 1,
        }
    }
    producer.await.unwrap();

    assert!(
        cancels > 0,
        "cancellation path was never exercised — the test is vacuous"
    );
    assert_eq!(
        got_c,
        (0..N).collect::<Vec<_>>(),
        "control lane: loss/dup across cancel"
    );
    assert_eq!(
        got_u,
        (0..N).collect::<Vec<_>>(),
        "user lane: loss/dup across cancel"
    );
}

#[tokio::test]
async fn drain_teardown_race_discards_blocked_sender_item() {
    let (ctl, usr, rx) = channel::<u32, u32>(Config::new(2));
    usr.send(10).await.unwrap();
    usr.send(11).await.unwrap(); // lane now full
    let blocked = tokio::spawn({
        let usr = usr.clone();
        async move { usr.send(12).await }
    });
    // Let the third send park on the full lane.
    tokio::time::sleep(Duration::from_millis(25)).await;
    ctl.send(7).unwrap();

    let drained = rx.drain();
    assert_eq!(
        drained.user,
        vec![10, 11],
        "drain must return queued users, FIFO"
    );
    assert_eq!(drained.control, vec![7], "drain must return queued control");

    // Teardown seam (flume-level, unfixable from Consumer): the parked send is
    // pulled into the receiverless queue by disconnect_all and reports success;
    // item 12 is neither in Drained nor returned to the sender. Pin the seam.
    let returned = timeout(GUARD, blocked)
        .await
        .expect("blocked sender never released")
        .unwrap();
    assert!(
        returned.is_ok(),
        "flume pulls parked sends into the orphaned queue at disconnect and reports Ok — \
         a send racing teardown is silently discarded (documented on drain())"
    );
}

#[tokio::test]
async fn default_config_is_pure_strict_priority() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64));
    for i in 0..64u32 {
        usr.try_send(i).unwrap();
    }
    for i in 0..32u32 {
        ctl.send(i).unwrap();
    }
    drop(usr);
    drop(ctl);

    let mut saw_user = false;
    let mut controls = 0u32;
    while let Some(x) = timeout(GUARD, rx.recv()).await.expect("recv stalled") {
        match x {
            Received::Control(_) => {
                controls += 1;
                assert!(
                    !saw_user,
                    "user served before the control backlog drained — \
                     aging must stay OFF by default (the P1 oracle depends on it)"
                );
            }
            Received::User(_) => saw_user = true,
            // End-marker: fires after the last user item, so it cannot
            // disturb the strict-priority assertion above.
            Received::UserLaneClosed => {}
        }
    }
    assert_eq!(controls, 32);
}

#[tokio::test]
async fn closed_empty_recv_returns_none_repeatedly() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(1));
    drop(ctl);
    drop(usr);
    // Since the lane-lifecycle addition the one-shot end-marker comes FIRST
    // (the user lane is terminally closed and drained); `None` follows.
    assert!(matches!(
        timeout(GUARD, rx.recv())
            .await
            .expect("marker recv stalled"),
        Some(Received::UserLaneClosed)
    ));
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("first recv stalled")
            .is_none()
    );
    // Second None call: both closed flags set. The early return must fire before the
    // parked select! — with both arms disabled and no else, select! panics.
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("second recv stalled")
            .is_none()
    );
}
