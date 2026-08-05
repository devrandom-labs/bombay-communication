//! Perf metric harness for the two-lane priority-merge black box.
//!
//! Prints the card-3 contract lines, one per line:
//!
//! ```text
//! DIRECT_THROUGHPUT_OPS=<mixed send+recv ops/sec>
//! ANCHOR_THROUGHPUT_OPS=<address-table-shaped anchor try_send+recv ops/sec>
//! ANCHOR_OVERHEAD_NS=<mean ns per send added by the anchor path>
//! CONTROL_LATENCY_NS=<mean ns to recv one control while the user lane is saturated>
//! DRAIN_THROUGHPUT_OPS=<mixed 90% user / 10% control drain ops/sec>
//! SCORE=<min(direct, anchor, drain) / control_latency_ns>
//! ```
//!
//! plus two informational lines EXCLUDED from the scalar score:
//! `ANCHOR_CONTENTION_OPS` (8 producer anchors racing one consumer through a
//! small ring) and `CLOSE_RACE_UPGRADE_OK` (fraction of upgrades that won a
//! racing last-sender drop — inherently timing-dependent, reported separately
//! per the card).
//!
//! Correctness and zero-allocation are enforced elsewhere (the conformance
//! suite + alloc guard, run by `.auto/checks.sh`), so this file only measures
//! speed. It touches `fastpass` through its public API exclusively — a novel
//! internal mechanism (lock-free, ring, eventcount, …) is measured on the same
//! footing as the baseline.

use fastpass::{Config, Received, channel};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // Warm the allocator/codepaths once so the first-touch cost is not measured.
    rt.block_on(throughput_ops_per_sec());

    let direct = rt.block_on(throughput_ops_per_sec());
    let anchor = rt.block_on(anchor_throughput_ops_per_sec());
    let overhead_ns = anchor_overhead_ns();
    let contention = rt.block_on(anchor_contention_ops_per_sec());
    let latency_ns = rt.block_on(control_latency_ns());
    let drain = rt.block_on(drain_throughput_ops_per_sec());
    let close_ok = rt.block_on(close_race_upgrade_ok_fraction());

    let score = direct.min(anchor).min(drain) / latency_ns;
    println!("DIRECT_THROUGHPUT_OPS={direct:.0}");
    println!("ANCHOR_THROUGHPUT_OPS={anchor:.0}");
    println!("ANCHOR_OVERHEAD_NS={overhead_ns:.2}");
    println!("CONTROL_LATENCY_NS={latency_ns:.2}");
    println!("DRAIN_THROUGHPUT_OPS={drain:.0}");
    println!("SCORE={score:.6}");
    println!("ANCHOR_CONTENTION_OPS={contention:.0}");
    println!("CLOSE_RACE_UPGRADE_OK={close_ok:.6}");
}

/// Mixed send+recv throughput: enqueue K control + K user, then drain all 2K.
/// Both the sends and the receives are inside the timed region — this is the
/// full hot path of the black box.
async fn throughput_ops_per_sec() -> f64 {
    const K: u32 = 100_000;
    let (ctl, usr, mut rx) =
        channel::<u32, u32>(Config::new((2 * K) as usize).with_aging_cap(1024));

    let start = Instant::now();
    for i in 0..K {
        ctl.send(i).expect("control open");
        usr.try_send(i).expect("user capacity");
    }
    drop(ctl);
    drop(usr);
    let mut n: u64 = 0;
    while let Some(x) = rx.recv().await {
        black_box(&x);
        n += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    {
        n as f64 / elapsed
    }
}

/// The same shape through the address-table endpoint: every user enqueue is
/// an `anchor.try_send` (which upgrades to a temporary live sender first).
/// The actorpass wiring keeps this exact ratio — one anchor per actor.
async fn anchor_throughput_ops_per_sec() -> f64 {
    const K: u32 = 100_000;
    let (ctl, usr, mut rx) =
        channel::<u32, u32>(Config::new((2 * K) as usize).with_aging_cap(1024));
    let anchor = usr.anchor();

    let start = Instant::now();
    for i in 0..K {
        ctl.send(i).expect("control open");
        anchor.try_send(i).expect("user capacity");
    }
    drop(ctl);
    drop(usr);
    let mut n: u64 = 0;
    while let Some(x) = rx.recv().await {
        black_box(&x);
        n += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    {
        n as f64 / elapsed
    }
}

/// Mean nanoseconds per send added by the anchor path: time K plain
/// `try_send`s and K `anchor.try_send`s into a ring that never fills (no
/// consumer needed — the ring absorbs them), report the per-send delta.
/// Synchronous, so it runs outside the runtime.
fn anchor_overhead_ns() -> f64 {
    const K: u32 = 500_000;
    let (_ctl, usr, _rx) = channel::<u32, u32>(Config::new((2 * K) as usize));

    let t0 = Instant::now();
    for i in 0..K {
        black_box(usr.try_send(i).is_ok());
    }
    let direct = t0.elapsed().as_secs_f64() / f64::from(K);

    let anchor = usr.anchor();
    let t1 = Instant::now();
    for i in 0..K {
        black_box(anchor.try_send(i).is_ok());
    }
    let anchor_secs = t1.elapsed().as_secs_f64() / f64::from(K);

    (anchor_secs - direct) * 1e9
}

/// Anchor contention: 8 producer anchors racing one consumer through a small
/// ring (real backpressure — every `send` parks on the full ring and is
/// woken by a pop). Informational: excluded from the scalar score.
async fn anchor_contention_ops_per_sec() -> f64 {
    const K: u32 = 25_000;
    const PRODUCERS: usize = 8;
    const RING: usize = 128;
    let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(RING).with_aging_cap(1024));
    let anchor = Arc::new(usr.anchor());
    drop(usr); // only anchors + their temporary senders remain

    let b = Arc::new(Barrier::new(PRODUCERS + 1));
    let mut handles = Vec::new();
    for p in 0..PRODUCERS {
        let anchor = anchor.clone();
        let b = b.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            for i in 0..K {
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "PRODUCERS is 8, so the usize producer index never truncates in u32"
                )]
                let item = (p as u32) * K + i;
                anchor
                    .send(item)
                    .await
                    .expect("consumer alive until drained");
            }
        }));
    }
    let start = Instant::now();
    b.wait().await;
    drop(ctl); // control closed from the start: the drain ends at None
    let mut n: u64 = 0;
    while let Some(x) = rx.recv().await {
        match x {
            Received::User(_) => {
                black_box(&x);
                n += 1;
            }
            Received::UserLaneClosed => {}
            Received::Control(_) => unreachable!("no control traffic"),
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(
        n,
        (PRODUCERS as u64) * u64::from(K),
        "contention workload lost items"
    );
    #[allow(clippy::cast_precision_loss)]
    {
        n as f64 / elapsed
    }
}

/// Control latency under a saturated user backlog (P1): fill the user lane to
/// DEPTH, then time a single control recv. Setup is outside the timed region.
async fn control_latency_ns() -> f64 {
    const DEPTH: usize = 4096;
    const REPS: u32 = 4000;

    let mut total_ns: u128 = 0;
    for _ in 0..REPS {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(DEPTH).with_aging_cap(1024));
        for i in 0..DEPTH {
            #[allow(clippy::cast_possible_truncation)]
            usr.try_send(i as u32).expect("user capacity");
        }
        ctl.send(1).expect("control open");

        let start = Instant::now();
        let got = rx.recv().await;
        total_ns += start.elapsed().as_nanos();

        assert!(
            matches!(got, Some(Received::Control(1))),
            "control must win under backlog"
        );
    }
    #[allow(clippy::cast_precision_loss)]
    {
        total_ns as f64 / f64::from(REPS)
    }
}

/// Mixed 90% user / 10% control drain throughput: enqueue U user + C control,
/// drop both senders, drain everything to `None`. All work is inside the
/// timed region.
async fn drain_throughput_ops_per_sec() -> f64 {
    const U: u32 = 90_000;
    const C: u32 = 10_000;
    let (ctl, usr, mut rx) =
        channel::<u32, u32>(Config::new((U + C) as usize).with_aging_cap(1024));

    let start = Instant::now();
    for i in 0..U {
        usr.try_send(i).expect("user capacity");
    }
    for i in 0..C {
        ctl.send(i).expect("control open");
    }
    drop(ctl);
    drop(usr);
    let mut n: u64 = 0;
    while let Some(x) = rx.recv().await {
        black_box(&x);
        n += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    {
        n as f64 / elapsed
    }
}

/// Close race: repeated `upgrade` versus a racing last-sender drop. The
/// fraction of upgrades that won is timing-dependent by construction —
/// reported separately and excluded from the scalar score (per the card).
async fn close_race_upgrade_ok_fraction() -> f64 {
    const ROUNDS: u32 = 20_000;
    let mut ok: u32 = 0;
    for _ in 0..ROUNDS {
        let (_ctl, usr, _rx) = channel::<u32, u32>(Config::new(4));
        let anchor = usr.anchor();
        let b = Arc::new(Barrier::new(2));
        let b2 = b.clone();
        let dropper = tokio::spawn(async move {
            b2.wait().await;
            drop(usr);
        });
        b.wait().await;
        if anchor.upgrade().is_some() {
            // The temporary sender drops here, releasing its liveness.
            ok += 1;
        }
        dropper.await.unwrap();
    }
    #[allow(clippy::cast_precision_loss)]
    {
        f64::from(ok) / f64::from(ROUNDS)
    }
}
