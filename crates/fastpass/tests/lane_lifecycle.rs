//! SUT-level conformance for the two lane-lifecycle additions (actorpass fork
//! rulings C and D — `.plans/card-2-userlane-closed-recv-control.md`):
//!
//! 1. `parked_recv_wakes_with_user_lane_closed_on_last_sender_drop` — fork C
//!    (drain-stop observability): a consumer PARKED on an empty channel with
//!    the control lane held open must be woken by the last `UserSender` drop
//!    and return `UserLaneClosed` — never hang, never `None`. This is what
//!    lets actorpass collect an actor whose supervisor still holds a
//!    `ControlSender` (the merged `recv` stays open, but the user lane's
//!    death is now observable).
//! 2. `recv_control_flows_with_full_user_ring` — fork D: control-only reads
//!    work with a saturated user ring, consume no user items, and do NOT
//!    release a producer parked on the full ring (that parked producer IS
//!    the backpressure the parked-for-restart shape must preserve).
//! 3. `parked_for_restart_shape` — fork D end-to-end: the user lane dies,
//!    the end-marker is read, control is read via `recv_control`, then the
//!    control lane dies and `recv_control` returns `None`.

use fastpass::{Config, Received, TrySendError, UserClosed, channel};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;

const GUARD: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct MoveOnly(u64);

#[tokio::test]
async fn anchor_clone_supports_move_only_items() {
    let (_control, user, mut consumer) = channel::<(), MoveOnly>(Config::new(2));
    let anchor = user.anchor().clone();
    anchor.send(MoveOnly(7)).await.unwrap();
    let Some(Received::User(item)) = consumer.recv().await else {
        panic!("move-only user item must be delivered");
    };
    assert_eq!(item.0, 7);
}

#[tokio::test]
async fn parked_recv_wakes_with_user_lane_closed_on_last_sender_drop() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
    // Two barriers, no sleeps: the first starts both tasks together, the
    // second releases the drop only after bounded yields on this
    // current-thread runtime have provably parked `recv` (the assert below
    // fails otherwise — never a timing guess).
    let started = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(2));
    let mut recver = tokio::spawn({
        let started = started.clone();
        async move {
            started.wait().await;
            let got = rx.recv().await;
            (got, rx)
        }
    });
    let dropper = tokio::spawn({
        let started = started.clone();
        let release = release.clone();
        async move {
            started.wait().await;
            release.wait().await;
            drop(usr); // last user sender; the control lane stays open
        }
    });
    started.wait().await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !recver.is_finished(),
        "recv resolved before the last-sender drop — this is not the parked path"
    );
    release.wait().await;
    let (got, mut rx) = timeout(GUARD, &mut recver)
        .await
        .expect("recv stalled")
        .expect("recver task panicked");
    assert!(
        matches!(got, Some(Received::UserLaneClosed)),
        "expected UserLaneClosed, got {got:?} — None would mean the user lane's \
         death is unobservable while control lives"
    );
    dropper.await.unwrap();
    // The control lane is still fully usable afterwards.
    ctl.send(1).unwrap();
    let got = timeout(GUARD, rx.recv())
        .await
        .expect("control recv stalled");
    assert!(
        matches!(got, Some(Received::Control(1))),
        "control after the marker: {got:?}"
    );
    drop(ctl);
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("recv stalled")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recv_control_flows_with_full_user_ring() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
    usr.try_send(10).unwrap();
    usr.try_send(11).unwrap(); // ring FULL (capacity rounds to 2)
    let mut blocked = tokio::spawn({
        let usr = usr.clone();
        async move { usr.send(12).await }
    });
    // Let the third send park on the full ring.
    tokio::time::sleep(Duration::from_millis(25)).await;

    ctl.send(1).unwrap();
    ctl.send(2).unwrap();
    assert_eq!(
        timeout(GUARD, rx.recv_control())
            .await
            .expect("recv_control stalled"),
        Some(1)
    );
    assert_eq!(
        timeout(GUARD, rx.recv_control())
            .await
            .expect("recv_control stalled"),
        Some(2)
    );

    // recv_control consumed no user item and released no parked producer:
    // the blocked send is STILL parked. (Deterministic: only a consumer
    // pop or teardown can release it, and neither happened.)
    assert!(
        timeout(Duration::from_millis(100), &mut blocked)
            .await
            .is_err(),
        "recv_control released a producer parked on the full ring — it must not touch the user lane"
    );

    // The user lane drains via `recv` untouched and in order; the parked
    // send then lands and its item follows.
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(10))
    ));
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(11))
    ));
    blocked
        .await
        .unwrap()
        .expect("blocked send resolves once recv makes room");
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(12))
    ));

    drop(usr);
    drop(ctl);
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::UserLaneClosed)
    ));
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("recv stalled")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_for_restart_shape() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(4));
    usr.try_send(1).unwrap();
    usr.try_send(2).unwrap();
    drop(usr); // the actor's user handles are gone — drain-stop

    // Remaining user work drains first, then the one-shot marker lands.
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(1))
    ));
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(2))
    ));
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::UserLaneClosed)
    ));

    // Parked-for-restart: read the control lane WITHOUT touching user state.
    ctl.send(99).unwrap();
    assert_eq!(
        timeout(GUARD, rx.recv_control())
            .await
            .expect("recv_control stalled"),
        Some(99)
    );

    // Teardown instead: the control lane closes; recv_control drains to None.
    drop(ctl);
    assert_eq!(
        timeout(GUARD, rx.recv_control())
            .await
            .expect("recv_control stalled"),
        None
    );
    // And the merged recv agrees: everything is gone.
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("recv stalled")
            .is_none()
    );
}

// ——— UserAnchor: the non-owning address-table endpoint (card 3) ———
//
// The anchor holds no liveness. Every delivery first atomically acquires a
// temporary live `UserSender`; a delivery racing the last sender drop either
// linearizes entirely before `UserLaneClosed` or fails with its payload
// entirely after. These SUT tests pin the lifecycle on the optimized crate;
// the shared suite in fastpass-testkit runs the same shape against both
// implementations.

/// Cloned anchors (the address-table shape) do not delay `UserLaneClosed` or
/// the final `recv() -> None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anchor_clone_does_not_hold_lane_open() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
    let a1 = usr.anchor();
    let a2 = a1.clone();
    let a3 = a1.clone();
    usr.try_send(1).unwrap();
    drop(usr);
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(1))
    ));
    assert!(
        matches!(
            timeout(GUARD, rx.recv()).await.expect("recv stalled"),
            Some(Received::UserLaneClosed)
        ),
        "the end-marker must fire while cloned anchors are still held"
    );
    drop(ctl);
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("recv stalled")
            .is_none(),
        "recv must drain to None while cloned anchors are still held"
    );
    drop((a1, a2, a3));
}

/// An anchor upgrades and delivers while at least one counting sender is
/// live — through `upgrade`, through `send`, and through `try_send`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anchor_send_while_live() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
    let anchor = usr.anchor();
    let upgraded = anchor.upgrade().expect("a counting sender is live");
    upgraded.try_send(10).unwrap();
    drop(upgraded);
    anchor.try_send(11).unwrap();
    anchor.send(12).await.unwrap();
    drop(usr);
    drop(ctl);

    let mut users = Vec::new();
    while let Some(x) = timeout(GUARD, rx.recv()).await.expect("recv stalled") {
        if let Received::User(u) = x {
            users.push(u);
        }
    }
    assert_eq!(
        users,
        vec![10, 11, 12],
        "anchor deliveries must arrive in FIFO order"
    );
}

/// After the last counting sender drops: `upgrade` returns `None`; `send`
/// and `try_send` return the ORIGINAL payload as closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anchor_fails_after_last_sender() {
    let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
    let anchor = usr.anchor();
    drop(usr);
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::UserLaneClosed)
    ));
    assert!(
        anchor.upgrade().is_none(),
        "upgrade resurrected a closed lane"
    );
    match anchor.try_send(99) {
        Err(TrySendError::Closed(v)) => assert_eq!(v, 99, "payload mangled"),
        Err(TrySendError::Full(v)) => panic!("closed lane reported Full({v})"),
        Ok(()) => panic!("try_send succeeded after closure"),
    }
    match timeout(GUARD, anchor.send(100))
        .await
        .expect("anchor send stalled")
    {
        Err(UserClosed(v)) => assert_eq!(v, 100, "payload mangled"),
        Ok(()) => panic!("send succeeded after closure"),
    }
}

/// Anchors fail once the CONSUMER is dropped and never resurrect a lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anchor_fails_after_consumer_drop() {
    let (ctl, usr, rx) = channel::<u32, u32>(Config::new(8));
    let anchor = usr.anchor();
    drop(rx);
    match anchor.try_send(7) {
        Err(TrySendError::Closed(v)) => assert_eq!(v, 7, "payload mangled"),
        Err(TrySendError::Full(v)) => panic!("dropped consumer reported Full({v})"),
        Ok(()) => panic!("try_send succeeded after the consumer dropped"),
    }
    drop(usr);
    drop(ctl);
    assert!(
        anchor.upgrade().is_none(),
        "upgrade resurrected a dead lane"
    );
}

/// A blocked anchor `send` counts as live until it completes or is
/// cancelled: closure cannot overtake an acquired delivery (gate 5). The
/// stream must be 0, 1, the blocked item, THEN the end-marker — never the
/// marker before the in-flight delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_anchor_send_holds_lane_open() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
    let anchor = usr.anchor();
    usr.try_send(0).unwrap();
    usr.try_send(1).unwrap(); // ring FULL
    let handle = tokio::spawn({
        let anchor = anchor.clone();
        async move { anchor.send(2).await }
    });
    // Let the anchor send park on the full ring, then drop the last
    // counting sender: the blocked delivery still owns liveness.
    tokio::time::sleep(Duration::from_millis(25)).await;
    drop(usr);

    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(0))
    ));
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(1))
    ));
    // The consumer's pre-park release wakes the blocked send; its item
    // arrives BEFORE the end-marker — closure cannot overtake the delivery.
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(2))
    ));
    assert!(
        matches!(
            timeout(GUARD, rx.recv()).await.expect("recv stalled"),
            Some(Received::UserLaneClosed)
        ),
        "the end-marker must wait for the in-flight delivery (gate 5)"
    );
    handle.await.unwrap().expect("blocked send resolves");
    drop(ctl);
    assert!(
        timeout(GUARD, rx.recv())
            .await
            .expect("recv stalled")
            .is_none()
    );
}

/// Cancelling a blocked anchor send releases its temporary liveness count
/// (gate 6): the lane can then close normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_anchor_send_releases_liveness() {
    let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
    let anchor = usr.anchor();
    usr.try_send(0).unwrap();
    usr.try_send(1).unwrap(); // ring FULL
    let handle = tokio::spawn({
        let anchor = anchor.clone();
        async move { anchor.send(2).await }
    });
    // Let the send park on the full ring, then CANCEL it: the temporary
    // sender is dropped with the future, releasing its liveness.
    tokio::time::sleep(Duration::from_millis(25)).await;
    handle.abort();

    drop(usr); // last counting sender — the cancelled send must not hold the lane
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(0))
    ));
    assert!(matches!(
        timeout(GUARD, rx.recv()).await.expect("recv stalled"),
        Some(Received::User(1))
    ));
    assert!(
        matches!(
            timeout(GUARD, rx.recv()).await.expect("recv stalled"),
            Some(Received::UserLaneClosed)
        ),
        "the lane never closed — the cancelled send leaked its liveness (gate 6)"
    );
}

/// A last-sender-drop racing an anchor delivery has exactly two legal
/// outcomes (gate 4): the item precedes `UserLaneClosed`, or delivery fails
/// and no item appears. An item after the marker is forbidden.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anchor_racing_last_drop_is_linearizable() {
    for round in 0..300u32 {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
        let anchor = usr.anchor();
        let b = Arc::new(Barrier::new(3));
        let b2 = b.clone();
        let b3 = b.clone();
        let dropper = tokio::spawn(async move {
            b2.wait().await;
            drop(usr);
        });
        let sender = tokio::spawn(async move {
            b3.wait().await;
            anchor.try_send(round)
        });
        b.wait().await;
        let result = sender.await.unwrap();
        dropper.await.unwrap();

        // Drain the user stream; track the marker position.
        drop(ctl);
        let mut saw_user = false;
        let mut marker_seen = false;
        loop {
            match timeout(GUARD, rx.recv()).await.expect("recv stalled") {
                Some(Received::User(u)) => {
                    assert_eq!(u, round, "foreign item in the stream");
                    assert!(
                        !marker_seen,
                        "user item after UserLaneClosed — marker ordering broken"
                    );
                    saw_user = true;
                }
                Some(Received::UserLaneClosed) => marker_seen = true,
                Some(Received::Control(_)) => unreachable!("no control traffic"),
                None => break,
            }
        }
        assert!(
            marker_seen,
            "the end-marker must fire after the last sender drops"
        );
        match result {
            Ok(()) => assert!(saw_user, "try_send Ok but no item arrived"),
            Err(TrySendError::Full(_)) => {
                panic!("unexpected Full on an 8-slot ring with one item")
            }
            Err(TrySendError::Closed(v)) => {
                assert_eq!(v, round, "closed error must carry the original payload");
                assert!(!saw_user, "try_send Closed but an item arrived");
            }
        }
    }
}
