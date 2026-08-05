//! Shared property suite for the two-lane priority-merge component.
//!
//! The distilled problem (bombay card #225): merge two FIFO streams — a
//! **control** lane and a **user** lane — into a single consumer such that
//! control is served ahead of a user backlog, without the classic downsides
//! (head-of-line blocking, lost wakeups, item loss, or user starvation).
//!
//! Both crates under test expose the identical public API:
//!
//! ```ignore
//! pub struct Config;                       // Config::new(user_cap).with_aging_cap(k)
//! pub enum   Received<C, U> { Control(C), User(U), UserLaneClosed }
//! pub struct Drained<C, U>  { pub control: Vec<C>, pub user: Vec<U> }
//! pub fn channel<C, U>(cfg: Config) -> (ControlSender<C>, UserSender<U>, Consumer<C, U>);
//! // ControlSender::send(C)              -> Result<(), ControlClosed<C>>   (never blocks)
//! // UserSender::send(U).await           -> Result<(), UserClosed<U>>      (bounded backpressure)
//! // UserSender::try_send(U)             -> Result<(), TrySendError<U>>
//! // Consumer::recv().await              -> Option<Received<C, U>>         (None once drained + closed)
//! // Consumer::recv_control().await      -> Option<C>                     (control-only; None once closed + drained)
//! // Consumer::drain(self)               -> Drained<C, U>
//! ```
//!
//! `Received::UserLaneClosed` is the user stream's one-shot end-marker: it
//! fires exactly once, after the last user item, only once every
//! `UserSender` is gone — subject to the same control-first priority and
//! aging as a user item.
//!
//! Invoke [`property_suite!`] and [`alloc_guard!`] from each subject crate's
//! `tests/` directory, passing the crate name.

/// Emit the full P1–P6 + ordering + anti-starvation + lane-lifecycle property
/// suite against `$fp::channel`. Every awaited `recv` is wrapped in a timeout
/// so a deadlocking or lost-wakeup implementation FAILS the test rather than
/// hanging the whole run — essential for the autoresearch measure loop.
#[macro_export]
macro_rules! property_suite {
    ($fp:ident) => {
        mod fastpass_property_suite {
            use ::std::collections::BTreeSet;
            use ::std::sync::Arc;
            use ::std::time::Duration;
            use ::tokio::sync::Barrier;
            use ::tokio::time::timeout;
            use $fp::{Config, Received, TrySendError, UserClosed, channel};

            const GUARD: Duration = Duration::from_secs(5);

            async fn recv1(rx: &mut $fp::Consumer<u32, u32>) -> Received<u32, u32> {
                match timeout(GUARD, rx.recv()).await {
                    Ok(Some(x)) => x,
                    Ok(None) => panic!("channel closed while an item was expected"),
                    Err(_) => panic!("recv timed out — deadlock or lost wakeup"),
                }
            }

            // P1 — priority / no head-of-line blocking: a control signal must
            // not wait behind a full user backlog.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p1_control_skips_full_user_backlog() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                for i in 0..8u32 {
                    usr.try_send(i).expect("user lane has capacity");
                }
                ctl.send(999).expect("control lane open");
                match recv1(&mut rx).await {
                    Received::Control(c) => assert_eq!(c, 999),
                    Received::User(u) => {
                        panic!("user {u} served before control — head-of-line blocking (P1)")
                    }
                    Received::UserLaneClosed => {
                        panic!("user lane reported closed while a UserSender lives")
                    }
                }
            }

            // P2 — FIFO within each lane, preserved under interleaved sends.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p2_fifo_within_each_lane() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(16));
                ctl.send(1).unwrap();
                usr.try_send(10).unwrap();
                ctl.send(2).unwrap();
                usr.try_send(11).unwrap();
                ctl.send(3).unwrap();
                usr.try_send(12).unwrap();
                drop(ctl);
                drop(usr);

                let (mut controls, mut users) = (Vec::new(), Vec::new());
                while let Some(x) = timeout(GUARD, rx.recv()).await.expect("no timeout") {
                    match x {
                        Received::Control(c) => controls.push(c),
                        Received::User(u) => users.push(u),
                        // The user stream's end-marker: expected here (both
                        // senders dropped), not part of the FIFO multisets.
                        Received::UserLaneClosed => {}
                    }
                }
                assert_eq!(controls, vec![1, 2, 3], "control lane not FIFO (P2)");
                assert_eq!(users, vec![10, 11, 12], "user lane not FIFO (P2)");
            }

            // P3 — progress: with no control traffic, users flow in order.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p3_users_flow_when_control_idle() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                for i in 0..8u32 {
                    usr.try_send(i).unwrap();
                }
                drop(ctl);
                let mut users = Vec::new();
                for _ in 0..8 {
                    match recv1(&mut rx).await {
                        Received::User(u) => users.push(u),
                        Received::Control(c) => panic!("phantom control {c} (P3)"),
                        Received::UserLaneClosed => {
                            panic!("user lane reported closed while a UserSender lives")
                        }
                    }
                }
                assert_eq!(users, (0..8).collect::<Vec<_>>(), "users must flow (P3)");
            }

            // P4 — no loss: concurrent producers on both lanes; every item is
            // delivered exactly once (multiset in == multiset out).
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p4_no_loss_concurrent() {
                const NC: u32 = 500;
                const NU: u32 = 500;
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64));
                let (c2, u2) = (ctl.clone(), usr.clone());
                let hc = tokio::spawn(async move {
                    for i in 0..NC {
                        c2.send(i).unwrap();
                    }
                });
                let hu = tokio::spawn(async move {
                    for i in 0..NU {
                        u2.send(i).await.unwrap();
                    }
                });
                drop(ctl);
                drop(usr);

                let (mut controls, mut users) = (BTreeSet::new(), BTreeSet::new());
                loop {
                    match timeout(GUARD, rx.recv()).await.expect("no timeout") {
                        Some(Received::Control(c)) => {
                            assert!(controls.insert(c), "dup control {c}")
                        }
                        Some(Received::User(u)) => assert!(users.insert(u), "dup user {u}"),
                        // End-marker, delivered once both lanes are closed
                        // and the ring drained; not part of the multisets.
                        Some(Received::UserLaneClosed) => {}
                        None => break,
                    }
                }
                hc.await.unwrap();
                hu.await.unwrap();
                assert_eq!(controls, (0..NC).collect(), "control loss/dup (P4)");
                assert_eq!(users, (0..NU).collect(), "user loss/dup (P4)");
            }

            // P5 — lost-wakeup: a consumer parked on an empty channel must be
            // woken by an arrival on EITHER lane.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p5_wake_on_control() {
                let (ctl, _usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let b = Arc::new(Barrier::new(2));
                let b2 = b.clone();
                let h = tokio::spawn(async move {
                    b2.wait().await;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    ctl.send(7).unwrap();
                });
                b.wait().await;
                assert!(
                    matches!(recv1(&mut rx).await, Received::Control(7)),
                    "consumer not woken by control arrival (P5)"
                );
                h.await.unwrap();
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p5_wake_on_user() {
                let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let b = Arc::new(Barrier::new(2));
                let b2 = b.clone();
                let h = tokio::spawn(async move {
                    b2.wait().await;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    usr.try_send(7).unwrap();
                });
                b.wait().await;
                assert!(
                    matches!(recv1(&mut rx).await, Received::User(7)),
                    "consumer not woken by user arrival (P5)"
                );
                h.await.unwrap();
            }

            // P6 — lifecycle: at teardown, queued items on BOTH lanes are
            // retrievable in FIFO order (the generic form of bombay's
            // synthetic-death-on-drop obligation for queued watch registrations).
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn p6_drain_returns_queued_items() {
                let (ctl, usr, rx) = channel::<u32, u32>(Config::new(8));
                ctl.send(1).unwrap();
                ctl.send(2).unwrap();
                usr.try_send(10).unwrap();
                usr.try_send(11).unwrap();
                let drained = rx.drain();
                assert_eq!(drained.control, vec![1, 2], "drain control not FIFO (P6)");
                assert_eq!(drained.user, vec![10, 11], "drain user not FIFO (P6)");
            }

            // Ordering witness — the deliberate relaxation: a control enqueued
            // AFTER a user may overtake it. This is the one property that is a
            // downside elsewhere and an upside here, scoped to runtime signals.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn overtake_control_beats_earlier_user() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                usr.try_send(1).unwrap(); // enqueued first
                ctl.send(2).unwrap(); // enqueued second, must run first
                assert!(
                    matches!(recv1(&mut rx).await, Received::Control(2)),
                    "control must overtake an earlier user (ordering witness)"
                );
                assert!(matches!(recv1(&mut rx).await, Received::User(1)));
            }

            // P3 (strong) — anti-starvation: under a continuous control flood a
            // waiting user must still be served within the aging cap K. Pure
            // strict priority (aging_cap 0) would starve it forever; this is the
            // "no-downside" upgrade. Discriminates the best-outcome algorithm.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn anti_starvation_user_served_within_aging_cap() {
                const K: usize = 4;
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64).with_aging_cap(K));
                for i in 0..(K as u32 * 3) {
                    ctl.send(i).unwrap();
                }
                usr.try_send(1000).unwrap();
                let mut saw_user = false;
                for _ in 0..=K {
                    if let Received::User(_) = recv1(&mut rx).await {
                        saw_user = true;
                        break;
                    }
                }
                assert!(saw_user, "user starved past aging cap K={K} (P3 strong)");
            }

            // User FIFO-per-sender must survive arbitrary control interleave
            // (card: "user-message FIFO-per-sender unaffected").
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn user_fifo_survives_control_interleave() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(32));
                for i in 0..10u32 {
                    usr.try_send(i).unwrap();
                    if i % 2 == 0 {
                        ctl.send(1000 + i).unwrap();
                    }
                }
                drop(ctl);
                drop(usr);
                let mut users = Vec::new();
                while let Some(x) = timeout(GUARD, rx.recv()).await.expect("no timeout") {
                    if let Received::User(u) = x {
                        users.push(u);
                    }
                }
                assert_eq!(
                    users,
                    (0..10).collect::<Vec<_>>(),
                    "user FIFO broke under interleave"
                );
            }

            // Lifecycle (1) — `UserLaneClosed` fires AT MOST once per channel.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn user_lane_closed_fires_at_most_once() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                usr.try_send(1).unwrap();
                drop(usr);
                assert!(matches!(recv1(&mut rx).await, Received::User(1)));
                assert!(matches!(recv1(&mut rx).await, Received::UserLaneClosed));
                // The latch holds: with the control lane still open, further
                // recvs serve control only — never a second leg.
                ctl.send(7).unwrap();
                assert!(matches!(recv1(&mut rx).await, Received::Control(7)));
                drop(ctl);
                while let Some(x) = timeout(GUARD, rx.recv()).await.expect("recv stalled") {
                    assert!(
                        !matches!(x, Received::UserLaneClosed),
                        "UserLaneClosed fired twice — the one-shot latch is broken"
                    );
                }
            }

            // Lifecycle (2) — the leg fires only after every sent user item
            // was received; no user item ever appears after it.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn user_lane_closed_only_after_all_user_items() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(16));
                for i in 0..10u32 {
                    usr.try_send(i).unwrap();
                }
                ctl.send(999).unwrap(); // control-first: served before the users
                drop(usr);
                drop(ctl);

                let mut users = Vec::new();
                let mut legs = 0u32;
                while let Some(x) = timeout(GUARD, rx.recv()).await.expect("recv stalled") {
                    match x {
                        Received::Control(_) => {}
                        Received::User(u) => {
                            assert_eq!(legs, 0, "user {u} served after UserLaneClosed");
                            users.push(u);
                        }
                        Received::UserLaneClosed => {
                            legs += 1;
                            assert_eq!(
                                users,
                                (0..10).collect::<Vec<_>>(),
                                "UserLaneClosed fired before every user item was received"
                            );
                        }
                    }
                }
                assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
            }

            // Lifecycle (3) — the leg never fires while any `UserSender` (or
            // clone) is alive, even with the ring drained.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn user_lane_closed_never_fires_while_sender_alive() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let usr2 = usr.clone();
                usr.try_send(1).unwrap();
                drop(usr); // one clone remains: the lane is NOT closed

                assert!(matches!(recv1(&mut rx).await, Received::User(1)));
                // Ring drained, a clone alive, control lane open: recv must
                // PARK — no leg, no None. (Deterministic: nothing can wake or
                // satisfy the consumer, so only the timeout fires.)
                assert!(
                    timeout(Duration::from_millis(100), rx.recv())
                        .await
                        .is_err(),
                    "recv returned while a UserSender clone lives (premature leg or None)"
                );
                usr2.try_send(2).unwrap();
                assert!(matches!(recv1(&mut rx).await, Received::User(2)));
                drop(usr2);
                assert!(matches!(recv1(&mut rx).await, Received::UserLaneClosed));
                drop(ctl);
                assert!(
                    timeout(GUARD, rx.recv())
                        .await
                        .expect("recv stalled")
                        .is_none()
                );
            }

            // Lifecycle (4) — after the leg, `recv` keeps serving control
            // items; `None` only once the control lane is also closed and
            // drained.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn user_lane_closed_then_control_continues_until_none() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8).with_aging_cap(1));
                ctl.send(0).unwrap();
                ctl.send(1).unwrap();
                ctl.send(2).unwrap();
                usr.try_send(100).unwrap();
                drop(usr);

                // Aging cap 1 interleaves control and the user stream; the
                // leg takes the user slot right after the last user item.
                let expected = [
                    Received::Control(0),
                    Received::User(100),
                    Received::Control(1),
                    Received::UserLaneClosed,
                    Received::Control(2),
                ];
                for want in expected {
                    assert_eq!(recv1(&mut rx).await, want);
                }
                // Control lane still open: NOT None — a fresh control is served.
                ctl.send(3).unwrap();
                assert_eq!(recv1(&mut rx).await, Received::Control(3));
                drop(ctl);
                assert!(
                    timeout(GUARD, rx.recv())
                        .await
                        .expect("recv stalled")
                        .is_none()
                );
            }

            // Lifecycle (5) — `recv_control` serves control in ticket (FIFO)
            // order and never reduces the user items the consumer later
            // observes.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn recv_control_fifo_and_never_consumes_user_items() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                for i in 0..8u32 {
                    usr.try_send(i).unwrap(); // ring FULL: control reads must not touch it
                }
                for i in 0..4u32 {
                    ctl.send(i).unwrap();
                }
                let mut controls = Vec::new();
                for _ in 0..4 {
                    controls.push(
                        timeout(GUARD, rx.recv_control())
                            .await
                            .expect("recv_control stalled")
                            .expect("control item expected"),
                    );
                }
                assert_eq!(
                    controls,
                    (0..4).collect::<Vec<_>>(),
                    "recv_control not FIFO"
                );
                // Every user item is still there, in order: close the user
                // lane and drain to the end-marker (a stolen item shrinks
                // the drained count, failing the assert — never a stall).
                drop(usr);
                let mut users = Vec::new();
                loop {
                    match recv1(&mut rx).await {
                        Received::User(u) => users.push(u),
                        Received::UserLaneClosed => break,
                        x => panic!("expected a user item or the end-marker, got {x:?}"),
                    }
                }
                assert_eq!(
                    users,
                    (0..8).collect::<Vec<_>>(),
                    "recv_control consumed user items"
                );
            }

            // Lifecycle (6) — `recv_control` returns `None` only when the
            // control lane is closed AND drained; a queued item beats the
            // closure, and an open lane parks instead.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn recv_control_none_only_when_control_closed_and_drained() {
                let (ctl, _usr, mut rx) = channel::<u32, u32>(Config::new(8));
                // Open + empty: parks (no None while a sender lives).
                assert!(
                    timeout(Duration::from_millis(100), rx.recv_control())
                        .await
                        .is_err(),
                    "recv_control returned on an open, empty control lane"
                );
                // Queued item + closed lane: the item comes first, then None
                // (stable).
                ctl.send(1).unwrap();
                drop(ctl);
                assert_eq!(
                    timeout(GUARD, rx.recv_control())
                        .await
                        .expect("recv_control stalled"),
                    Some(1),
                    "queued control lost to the lane closure"
                );
                assert_eq!(
                    timeout(GUARD, rx.recv_control())
                        .await
                        .expect("recv_control stalled"),
                    None,
                    "control lane closed and drained — must be None"
                );
                // And on a fresh channel closed with an EMPTY queue: None at once.
                let (ctl2, _usr2, mut rx2) = channel::<u32, u32>(Config::new(8));
                drop(ctl2);
                assert_eq!(
                    timeout(GUARD, rx2.recv_control())
                        .await
                        .expect("recv_control stalled"),
                    None,
                    "closed empty control lane must yield None"
                );
            }

            // ——— UserAnchor (actorpass address-table endpoint, card 3) ———

            // Anchor clone/drop neutrality: cloned anchors (the address-table
            // shape) must not keep the user lane open — the end-marker fires
            // and `recv` drains to `None` while they are held.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn anchor_shared_clone_neutrality() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let a1 = usr.anchor();
                let a2 = a1.clone();
                usr.try_send(1).unwrap();
                drop(usr);
                assert!(matches!(recv1(&mut rx).await, Received::User(1)));
                assert!(
                    matches!(recv1(&mut rx).await, Received::UserLaneClosed),
                    "the leg must fire with only anchors held"
                );
                // The control lane continues to work past the marker.
                ctl.send(7).unwrap();
                assert!(matches!(recv1(&mut rx).await, Received::Control(7)));
                drop(ctl);
                assert!(
                    timeout(GUARD, rx.recv())
                        .await
                        .expect("recv stalled")
                        .is_none(),
                    "recv must return None once both lanes are closed and drained"
                );
                drop((a1, a2));
            }

            // A live anchor upgrades and delivers while a counting sender
            // lives — both through `upgrade` + the sender ops and through
            // the anchor's own `try_send`.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn anchor_shared_upgrade_and_send_while_live() {
                let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let anchor = usr.anchor();
                let upgraded = anchor.upgrade().expect("sender is live");
                upgraded.try_send(11).unwrap();
                drop(upgraded);
                assert!(matches!(recv1(&mut rx).await, Received::User(11)));
                anchor.try_send(12).unwrap();
                assert!(matches!(recv1(&mut rx).await, Received::User(12)));
                drop(usr);
                drop(ctl);
            }

            // After the last counting sender drops: `upgrade` returns `None`,
            // and `send`/`try_send` return the ORIGINAL payload as closed.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn anchor_shared_fails_after_last_sender() {
                let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let anchor = usr.anchor();
                drop(usr);
                assert!(
                    matches!(recv1(&mut rx).await, Received::UserLaneClosed),
                    "the leg must fire after the last sender drops"
                );
                assert!(
                    anchor.upgrade().is_none(),
                    "upgrade succeeded after the lane closed"
                );
                match anchor.try_send(99) {
                    Err(TrySendError::Closed(v)) => assert_eq!(v, 99, "payload mangled"),
                    Err(TrySendError::Full(v)) => {
                        panic!("closed lane reported Full({v}) — must be Closed")
                    }
                    Ok(()) => panic!("try_send succeeded after the lane closed"),
                }
                let err = timeout(GUARD, anchor.send(100))
                    .await
                    .expect("anchor send stalled")
                    .expect_err("send succeeded after the lane closed");
                match err {
                    UserClosed(v) => assert_eq!(v, 100, "payload mangled"),
                }
            }

            // Anchors fail once the CONSUMER is dropped (never resurrect a
            // lane): delivery reports the payload back as closed, and once
            // the senders are also gone the anchor is permanently dead.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn anchor_shared_fails_after_consumer_drop() {
                let (ctl, usr, rx) = channel::<u32, u32>(Config::new(8));
                let anchor = usr.anchor();
                drop(rx); // consumer gone
                match anchor.try_send(7) {
                    Err(TrySendError::Closed(v)) => assert_eq!(v, 7, "payload mangled"),
                    Err(TrySendError::Full(v)) => {
                        panic!("dropped consumer reported Full({v}) — must be Closed")
                    }
                    Ok(()) => panic!("try_send succeeded after the consumer dropped"),
                }
                // Never resurrects: with the senders gone too, the anchor is
                // dead for good.
                drop(usr);
                drop(ctl);
                assert!(
                    anchor.upgrade().is_none(),
                    "upgrade resurrected a lane with no counting sender"
                );
            }
        }
    };
}

/// Emit the P7 hot-path allocation guard in its own test binary (a global
/// allocator is binary-scoped, so this cannot share a binary with the suite).
#[macro_export]
macro_rules! alloc_guard {
    ($fp:ident) => {
        mod fastpass_alloc_guard {
            use ::std::alloc::{GlobalAlloc, Layout, System};
            use ::std::sync::atomic::{AtomicUsize, Ordering};
            use $fp::{Config, channel};

            static ALLOCS: AtomicUsize = AtomicUsize::new(0);

            struct Counting;
            // SAFETY: forwards verbatim to the System allocator; the only added
            // effect is an atomic increment per allocation.
            unsafe impl GlobalAlloc for Counting {
                unsafe fn alloc(&self, l: Layout) -> *mut u8 {
                    ALLOCS.fetch_add(1, Ordering::Relaxed);
                    unsafe { System.alloc(l) }
                }
                unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
                    unsafe { System.dealloc(p, l) }
                }
            }

            #[global_allocator]
            static GLOBAL: Counting = Counting;

            // P7 — a steady-state user send must not allocate. Warm the lane to
            // its steady size first (so any one-time buffer growth is already
            // paid), then measure a single `try_send`.
            #[test]
            fn user_send_steady_state_is_zero_alloc() {
                let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                for i in 0..8u32 {
                    usr.try_send(i).unwrap();
                }
                let rt = ::tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    for _ in 0..8 {
                        let _ = rx.recv().await;
                    }
                });

                let before = ALLOCS.load(Ordering::Relaxed);
                usr.try_send(42).unwrap();
                let delta = ALLOCS.load(Ordering::Relaxed) - before;
                assert_eq!(delta, 0, "steady-state user send allocated {delta}x (P7)");
            }

            // Card-3 gate 8 — anchor steady-state `try_send` must not
            // allocate: the upgrade is a conditional RMW over the live-sender
            // count (plus, in the optimized crate, a `Weak` bump), and the
            // push goes into the warm ring. Same warm-then-measure discipline
            // as P7.
            #[test]
            fn anchor_try_send_steady_state_is_zero_alloc() {
                let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
                let anchor = usr.anchor();
                for i in 0..8u32 {
                    usr.try_send(i).unwrap();
                }
                let rt = ::tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    for _ in 0..8 {
                        let _ = rx.recv().await;
                    }
                });

                let before = ALLOCS.load(Ordering::Relaxed);
                anchor.try_send(42).unwrap();
                let delta = ALLOCS.load(Ordering::Relaxed) - before;
                assert_eq!(
                    delta, 0,
                    "steady-state anchor try_send allocated {delta}x (card-3 gate 8)"
                );
            }
        }
    };
}
