//! P5 — loom model-check of the wakeup protocol (see `.auto/prompt.md` and
//! `.plans/fastpass-twolane.md`).
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
//! `UserLaneClosed` end-marker's exactly-once latch, and `recv_control`'s
//! park/wake discipline with a saturated user ring. The control block
//! size is 2 slots under loom so tiny models cross block boundaries
//! (linking, hint advance, consumer reclamation).
//!
//! SMALL models instead of one large one: exploration cost grows
//! combinatorially with schedule length, so each wakeup path gets its own
//! minimal harness. Models run sequentially from one test binary (parallel
//! `loom::model` executions in one process are not supported).
//!
//! Run: `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test -p
//! fastpass --test loom --release` (also wired into `.auto/checks.sh`).
#![cfg(loom)]

use fastpass::{Config, Received, channel};

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
    // released when the consumer is dropped (the pinned seam: the parked
    // send resolves `Ok` and the item is discarded). loom fails the model
    // on any schedule where the producer stays parked — termination IS the
    // assertion.
    loom::model(|| {
        let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(2));
        let producer = loom::thread::spawn(move || {
            for i in 0..3u32 {
                // `Err` only if the teardown beat the first send; either
                // way the send must RESOLVE, never hang.
                let _ = usr.send_blocking(i);
            }
        });
        // Take one item (waits for the first send), then tear down with the
        // ring likely full and the producer likely parked.
        let first = rx.recv_blocking();
        assert!(first.is_some());
        drop(rx);
        producer.join().unwrap();
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
