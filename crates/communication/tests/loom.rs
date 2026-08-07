//! P5 — loom model-check of the wakeup protocol.
//!
//! loom cannot drive the async `recv` (tokio's `Notify` is not
//! loom-instrumented), so the library swaps `std::sync::atomic`/`Arc` for
//! loom's under `cfg(loom)`, gates the tokio-`Notify` async paths, and
//! exposes blocking twins (`send_blocking`/`recv_blocking`) that run the
//! SAME `parked`/`waiting` flag protocol. The flag checks under loom are
//! taken under the sync `Notify` stand-in's lock: absent a happens-before
//! edge loom (correctly, over-approximating hardware) explores
//! store-buffering executions where a flag read misses an earlier-executed
//! announcement; the stand-in's mutex supplies the edge real hardware gets
//! from coherence.
//!
//! Explored properties, on every interleaving loom enumerates:
//! termination (no lost wakeup / no hang — loom fails a deadlocked
//! schedule), no loss, no duplication, per-lane FIFO, the
//! `UserLaneClosed` end-marker's exactly-once latch, `recv_control`'s
//! park/wake discipline with a saturated user ring, and the teardown
//! contract: a producer released by consumer teardown either linearized
//! (its send resolved `Ok`) or gets its exact payload back as
//! `Err(UserClosed)` — across the registration/recheck/park/wakeup race
//! windows and for multiple producers at once (models C, J, K). The
//! card-3 `UserAnchor` models (F–H) check the conditional-increment
//! counter protocol itself:
//! upgrade-vs-last-drop linearizability and marker ordering (F), a blocked
//! anchor send holding liveness until its item lands (G), and the RAII
//! release of an upgrade dropped without delivery (H). The control block
//! size is 2 slots under loom so tiny models cross block boundaries
//! (linking, hint advance, consumer reclamation).
//!
//! SMALL models instead of one large one: exploration cost grows
//! combinatorially with schedule length, so each wakeup path gets its own
//! minimal harness. Models run sequentially from one test binary (parallel
//! `loom::model` executions in one process are not supported).
//!
//! Run: `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test -p
//! communication --test loom --release`.
#![cfg(loom)]

use communication::{Config, Received, channel};
use loom::sync::atomic::Ordering;

#[test]
fn user_backpressure_wakeup() {
    // A — user-lane BACKPRESSURE wakeup (`waiting`): the ring is filled
    // BEFORE the producer starts, so its single send must park until the
    // consumer pops; the consumer's strided and pre-park releases must
    // wake it. Asserts termination, no loss, no duplication, FIFO.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        drop(ctl); // close the control lane: `None` requires BOTH lanes closed
        usr.try_send(0).expect("ring empty");
        usr.try_send(1).expect("capacity 2");
        let p = loom::thread::spawn(move || {
            usr.send_blocking(2).expect("consumer alive until drained");
        });
        let mut got = Vec::new();
        let mut legs = 0u32;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(u) => got.push(u),
                Received::Control(_) => unreachable!("no control traffic"),
                // End-marker: fires once the producer thread's `usr` drops
                // with the ring drained — exactly once.
                Received::UserLaneClosed => legs += 1,
            }
        }
        p.join().unwrap();
        assert_eq!(got, [0, 1, 2], "user lane: loss / duplication / reorder");
        assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
    });
}

#[test]
fn consumer_wakeup_and_control_chain() {
    // B — consumer wakeup (`parked`) + control block-chain: 2 producers
    // race 3 sends across a block boundary (2-slot blocks → claiming,
    // linking, and consumer block-crossing are exercised), then close the
    // lane; the parked consumer must be woken for every push and for the
    // final close.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        drop(usr); // close the user lane: `None` requires BOTH lanes closed
        let ctl2 = ctl.clone();
        let p1 = loom::thread::spawn(move || {
            for i in 0..2 {
                ctl.send(i).expect("consumer alive until drained");
            }
        });
        let p2 = loom::thread::spawn(move || {
            ctl2.send(2).expect("consumer alive until drained");
        });
        let mut got = Vec::new();
        let mut legs = 0u32;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::Control(c) => got.push(c),
                Received::User(_) => unreachable!("no user traffic"),
                // End-marker: the user lane was closed from the start, so it
                // fires once the control backlog drains — exactly once.
                Received::UserLaneClosed => legs += 1,
            }
        }
        p1.join().unwrap();
        p2.join().unwrap();
        got.sort_unstable();
        assert_eq!(got, [0, 1, 2], "control lane: loss / duplication");
        assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
    });
}

#[test]
fn teardown_releases_parked_producer() {
    // C — teardown release: a producer parked on a full ring must be
    // released when the consumer is dropped, and a send that never
    // linearized must come back as `Err(UserClosed(payload))` with its
    // exact payload. loom fails the model on any schedule where the
    // producer stays parked — termination IS the no-lost-wakeup assertion.
    // The consumer pops once before tearing down, so a blocked send CAN
    // still win the freed slot and resolve `Ok` legitimately: the
    // assertion is the Ok-prefix/Err-suffix rule with payload identity,
    // and the discriminating no-pop shape lives in model J.
    loom::model(|| {
        let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let producer = loom::thread::spawn(move || {
            let mut results = Vec::new();
            for i in 0..3u32 {
                match usr.send_blocking(i) {
                    Ok(()) => results.push(Ok(i)),
                    Err(communication::UserClosed(v)) => {
                        assert_eq!(v, i, "returned payload is not the sent one");
                        results.push(Err(i));
                    }
                }
            }
            results
        });
        // Take one item (waits for the first send), then tear down with the
        // ring likely full and the producer likely parked.
        let first = rx.recv_blocking();
        assert!(first.is_some());
        drop(rx);
        let results = producer.join().unwrap();
        // Once a send observes teardown every later send must fail at the
        // same check: an Ok may only precede Errs, never follow them, and
        // each Err carried its exact payload (asserted above).
        let first_err = results.iter().position(Result::is_err);
        assert!(
            first_err.is_none_or(|k| results[k..].iter().all(Result::is_err)),
            "an Ok send followed a failed one after teardown: {results:?}"
        );
    });
}

#[test]
fn teardown_returns_unlinearized_payloads() {
    // J — the discriminating shape: NO consumer pop ever frees a slot, so
    // with capacity 2 at most two sends can linearize; every later send is
    // released BY teardown and must return its exact payload. The payloads
    // are drop-counted: at model end every constructed payload was dropped
    // exactly once (returned ones by the assertion scope, published ones by
    // the lane's Drop). A release-reports-Ok variant fails this model with
    // one preemption (an Ok past the capacity bound).
    loom::model(|| {
        let drops = loom::sync::Arc::new(loom::sync::atomic::AtomicUsize::new(0));
        #[derive(Debug)]
        struct Counted(loom::sync::Arc<loom::sync::atomic::AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (_ctl, usr, rx) = channel::<u32, Counted>(Config::new(2));
        let producer = loom::thread::spawn({
            let drops = drops.clone();
            move || {
                let mut oks = 0usize;
                for _ in 0..3 {
                    match usr.send_blocking(Counted(drops.clone())) {
                        Ok(()) => oks += 1,
                        Err(communication::UserClosed(p)) => drop(p),
                    }
                }
                oks
            }
        });
        drop(rx); // teardown immediately: no pop ever frees a slot
        let oks = producer.join().unwrap();
        assert!(
            oks <= 2,
            "{oks} sends resolved Ok with no consumer pop — capacity 2 bounds linearizations; \
             a teardown release must return Err(UserClosed(payload))"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            3,
            "payload leak or double-drop across teardown"
        );
    });
}

#[test]
fn teardown_releases_multiple_producers_with_their_payloads() {
    // K — multiple producers parked on the SAME full ring are all released
    // by teardown, each with its own payload, and every payload is dropped
    // exactly once. The ring is pre-filled and never popped, so neither
    // producer can linearize: both sends must come back `Err(UserClosed)`.
    loom::model(|| {
        let drops = loom::sync::Arc::new(loom::sync::atomic::AtomicUsize::new(0));
        #[derive(Debug)]
        struct Counted(u32, loom::sync::Arc<loom::sync::atomic::AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (_ctl, usr, rx) = channel::<u32, Counted>(Config::new(2));
        usr.try_send(Counted(1, drops.clone())).expect("ring empty");
        usr.try_send(Counted(2, drops.clone())).expect("capacity 2");
        let p1 = loom::thread::spawn({
            let usr = usr.clone();
            let drops = drops.clone();
            move || usr.send_blocking(Counted(10, drops))
        });
        let p2 = loom::thread::spawn({
            let usr = usr.clone();
            let drops = drops.clone();
            move || usr.send_blocking(Counted(20, drops))
        });
        drop(rx); // teardown with both producers parked or registering
        for (handle, id) in [(p1, 10), (p2, 20)] {
            let err = handle
                .join()
                .unwrap()
                .expect_err("a producer that never linearized must be rejected");
            assert_eq!(err.0.0, id, "a producer got another producer's payload");
            drop(err);
        }
        drop(usr);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            4,
            "payload leak or double-drop across teardown"
        );
    });
}

#[test]
fn user_lane_closed_leg_latch_and_wake() {
    // D — the end-marker's latch + parked-path observability (fork C): the
    // consumer parks on an empty channel with the control lane held open; a
    // racing last-`UserSender` drop must wake it with `UserLaneClosed`
    // (exactly once — the later `None` proves no re-fire), never `None`
    // while control lives, never a hang. loom fails any schedule that
    // deadlocks, so termination IS the no-lost-wakeup assertion.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let dropper = loom::thread::spawn(move || drop(usr));
        match rx.recv_blocking() {
            Some(Received::UserLaneClosed) => {}
            other => panic!("expected UserLaneClosed, got {other:?}"),
        }
        dropper.join().unwrap();
        // Control still flows after the marker.
        ctl.send(7).expect("consumer alive until drained");
        match rx.recv_blocking() {
            Some(Received::Control(7)) => {}
            other => panic!("expected Control(7), got {other:?}"),
        }
        drop(ctl);
        // Both lanes closed and drained, marker already spent: None, and no
        // second marker.
        assert!(rx.recv_blocking().is_none());
    });
}

#[test]
fn recv_control_parking_preserves_user_lane() {
    // E — `recv_control` parking under interleavings (fork D): a racing
    // control producer must wake the control-only consumer for every push
    // and for the final close, while a producer parked on the FULL user
    // ring is never released by this path (no `release_one_waiter` call) —
    // only `recv` popping later releases it. Asserts termination (loom
    // fails a deadlock), control FIFO, and the user lane observed untouched
    // afterwards.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        usr.try_send(0).expect("ring empty");
        usr.try_send(1).expect("capacity 2");
        let parked_producer = loom::thread::spawn(move || {
            usr.send_blocking(2).expect("consumer alive until drained");
        });
        let control_producer = loom::thread::spawn(move || {
            ctl.send(10).expect("consumer alive until drained");
            ctl.send(11).expect("consumer alive until drained");
        });
        // Control-only reads with a saturated user ring in play: FIFO, then
        // None once the control lane is closed and drained.
        assert_eq!(rx.recv_control_blocking(), Some(10));
        assert_eq!(rx.recv_control_blocking(), Some(11));
        assert_eq!(rx.recv_control_blocking(), None);
        // The user lane survived untouched; `recv` drains it (releasing the
        // parked producer), then the marker, then None.
        let mut got = Vec::new();
        let mut legs = 0u32;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(u) => got.push(u),
                Received::UserLaneClosed => legs += 1,
                Received::Control(_) => unreachable!("control lane drained"),
            }
        }
        parked_producer.join().unwrap();
        control_producer.join().unwrap();
        assert_eq!(
            got,
            [0, 1, 2],
            "recv_control consumed or reordered user items"
        );
        assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
    });
}

#[test]
fn anchor_upgrade_vs_last_drop() {
    // F — the load-bearing card-3 race: an anchor `upgrade` racing the last
    // `UserSender` drop, over the REAL conditional-increment counter
    // protocol. Exactly two legal outcomes: the upgrade wins first and the
    // item precedes `UserLaneClosed`, or the drop hits zero first and the
    // upgrade fails with no item. An item AFTER the marker is forbidden.
    // loom fails any schedule where the marker and a delivery disagree —
    // this is the probe that kills load-then-increment, unconditional
    // increment, and decrement-before-publication variants.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let anchor = usr.anchor();
        let dropper = loom::thread::spawn(move || drop(usr));
        let upgrader = loom::thread::spawn(move || match anchor.upgrade() {
            Some(sender) => sender.send_blocking(42).is_ok(),
            None => false,
        });
        drop(ctl); // control closed from the start: the drain ends at None

        let mut items = Vec::new();
        let mut legs = 0u32;
        let mut marker_seen = false;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(u) => {
                    assert!(
                        !marker_seen,
                        "user item after UserLaneClosed — marker ordering broken"
                    );
                    items.push(u);
                }
                Received::UserLaneClosed => {
                    marker_seen = true;
                    legs += 1;
                }
                Received::Control(_) => unreachable!("no control traffic"),
            }
        }
        let delivered = upgrader.join().unwrap();
        dropper.join().unwrap();
        assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
        if delivered {
            assert_eq!(items, vec![42], "delivered item missing from the stream");
        } else {
            assert!(
                items.is_empty(),
                "failed upgrade leaked an item into the stream"
            );
        }
    });
}

#[test]
fn anchor_blocked_send_holds_liveness() {
    // G — a blocked anchor `send` owns temporary liveness (gate 5): with
    // the ring full and the last counting sender dropped, the lane must NOT
    // close; the delivery completes once a consumer pop releases it, its
    // item lands BEFORE the end-marker, and only then does the lane close.
    // A leaked (permanent) increment would leave the lane open forever —
    // the drain parks and loom fails the model on the deadlock.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let anchor = usr.anchor();
        usr.try_send(0).expect("ring empty");
        usr.try_send(1).expect("capacity 2");
        let blocked = loom::thread::spawn(move || {
            // Two legal outcomes: the upgrade wins and the delivery lands
            // before the end-marker, or the last-sender drop wins and the
            // payload returns as closed. Either way the lane must close.
            match anchor.send_blocking(2) {
                Ok(()) => true,
                Err(_) => false,
            }
        });
        drop(usr); // last counting sender — the blocked send holds liveness
        drop(ctl);

        let mut got = Vec::new();
        let mut marker_seen = false;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(u) => {
                    assert!(
                        !marker_seen,
                        "user item after UserLaneClosed — marker ordering broken"
                    );
                    got.push(u);
                }
                Received::UserLaneClosed => marker_seen = true,
                Received::Control(_) => unreachable!("no control traffic"),
            }
        }
        let delivered = blocked.join().unwrap();
        assert!(
            marker_seen,
            "lane never closed after the send resolved — liveness leaked"
        );
        if delivered {
            assert_eq!(got, [0, 1, 2], "delivered item missing from the stream");
        } else {
            assert_eq!(
                got,
                [0, 1],
                "failed delivery leaked an item into the stream"
            );
        }
    });
}

#[test]
fn anchor_upgrade_then_drop_releases_liveness() {
    // H — the RAII side of temporary liveness: an upgraded sender dropped
    // WITHOUT delivering releases its increment. Whichever of the upgrade
    // or the last drop wins, the lane must still close and the end-marker
    // must fire exactly once. A permanent increment deadlocks the drain.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let anchor = usr.anchor();
        let upgrader = loom::thread::spawn(move || {
            // The upgrade may legitimately lose to the drop; if it wins,
            // dropping the sender without delivering must release liveness.
            if let Some(sender) = anchor.upgrade() {
                drop(sender);
            }
        });
        drop(usr); // last counting sender
        drop(ctl);
        upgrader.join().unwrap();

        let mut legs = 0u32;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(_) => unreachable!("no user traffic"),
                Received::UserLaneClosed => legs += 1,
                Received::Control(_) => unreachable!("no control traffic"),
            }
        }
        assert_eq!(legs, 1, "UserLaneClosed must fire exactly once");
    });
}

#[test]
fn anchor_release_before_publish_marker_ordering() {
    // I — the load-bearing publish-before-release program order (gate 3):
    // a delivery must PUBLISH its item before RELEASING its temporary
    // liveness, so the end-marker can never be delivered while the item is
    // still unpublished. One fused thread upgrades, drops the last counting
    // sender, and delivers (the upgrade cannot lose — the sequence is
    // single-threaded, so the marker ordering is the ONLY thing in play);
    // the consumer must observe the item before the end-marker. A
    // decrement-before-publication variant fires the marker in the
    // release→publish window and fails this model with one preemption.
    loom::model(|| {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let anchor = usr.anchor();
        let fused = loom::thread::spawn(move || {
            let sender = anchor.upgrade().expect("sender is live");
            drop(usr); // last counting sender — the upgraded sender holds liveness
            sender
                .send_blocking(42)
                .expect("consumer alive until drained")
        });
        drop(ctl); // control closed from the start: the drain ends at None

        let mut items = Vec::new();
        let mut marker_seen = false;
        while let Some(item) = rx.recv_blocking() {
            match item {
                Received::User(u) => {
                    assert!(
                        !marker_seen,
                        "user item after UserLaneClosed — marker ordering broken"
                    );
                    items.push(u);
                }
                Received::UserLaneClosed => marker_seen = true,
                Received::Control(_) => unreachable!("no control traffic"),
            }
        }
        fused.join().unwrap();
        assert!(marker_seen, "lane never closed");
        assert_eq!(items, vec![42], "delivered item missing from the stream");
    });
}
