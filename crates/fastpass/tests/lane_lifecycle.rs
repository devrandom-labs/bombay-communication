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

use fastpass::{Config, Received, channel};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;

const GUARD: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_recv_wakes_with_user_lane_closed_on_last_sender_drop() {
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
    let b = Arc::new(Barrier::new(2));
    let b2 = b.clone();
    let dropper = tokio::spawn(async move {
        b2.wait().await;
        // Let `recv` park on the empty channel before the drop lands.
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(usr); // last user sender; the control lane stays open
    });
    b.wait().await;
    let got = timeout(GUARD, rx.recv())
        .await
        .expect("recv parked forever — the last-sender drop did not wake it (lost wakeup)");
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
