//! Perf metric harness for the two-lane priority-merge black box.
//!
//! Prints three lines; autoresearch maximizes `SCORE`:
//!
//! ```text
//! THROUGHPUT_OPS=<send+recv ops/sec, mixed load>
//! CONTROL_LATENCY_NS=<mean ns to recv one control while user lane is saturated>
//! SCORE=<throughput_ops / control_latency_ns>   (higher throughput AND lower latency both raise it)
//! ```
//!
//! Correctness and zero-allocation are enforced elsewhere (the conformance
//! suite + alloc guard, run by `.auto/checks.sh`), so this file only measures
//! speed. It touches `fastpass` through its public API exclusively — a novel
//! internal mechanism (lock-free, ring, eventcount, …) is measured on the same
//! footing as the baseline.

use fastpass::{Config, Received, channel};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // Warm the allocator/codepaths once so the first-touch cost is not measured.
    rt.block_on(throughput_ops_per_sec());

    let throughput = rt.block_on(throughput_ops_per_sec());
    let latency_ns = rt.block_on(control_latency_ns());

    let score = throughput / latency_ns;
    println!("THROUGHPUT_OPS={throughput:.0}");
    println!("CONTROL_LATENCY_NS={latency_ns:.2}");
    println!("SCORE={score:.6}");
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

        assert!(matches!(got, Some(Received::Control(1))), "control must win under backlog");
    }
    #[allow(clippy::cast_precision_loss)]
    {
        total_ns as f64 / f64::from(REPS)
    }
}
