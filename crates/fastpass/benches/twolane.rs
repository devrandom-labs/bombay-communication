//! Perf signals for the two-lane merge. The headline number is P1: control
//! recv latency while the user lane is saturated — it must stay flat as the
//! backlog grows. A second bench measures drain throughput.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fastpass::{Config, Received, channel};
use std::hint::black_box;
use tokio::runtime::Runtime;

// P1 metric: with `depth` user messages already queued, how long to receive a
// single control signal? Strict priority keeps this flat; head-of-line blocking
// makes it grow with `depth`.
fn control_latency_under_backlog(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("control_latency_under_backlog");
    for depth in [0usize, 64, 1024, 8192] {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.to_async(&rt).iter_batched(
                || {
                    let (ctl, usr, rx) = channel::<u32, u32>(Config::new(depth.max(1)));
                    for i in 0..depth {
                        usr.try_send(u32::try_from(i).unwrap()).unwrap();
                    }
                    ctl.send(1).unwrap();
                    (ctl, usr, rx)
                },
                |(ctl, usr, mut rx)| async move {
                    let got = rx.recv().await;
                    assert!(matches!(got, Some(Received::Control(1))));
                    black_box((ctl, usr));
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// Throughput: enqueue N on each lane, drain all N*2 via recv.
fn drain_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    const N: u32 = 10_000;
    c.bench_function("drain_throughput_20k", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let (ctl, usr, rx) = channel::<u32, u32>(Config::new(N as usize));
                for i in 0..N {
                    ctl.send(i).unwrap();
                    usr.try_send(i).unwrap();
                }
                (ctl, usr, rx)
            },
            |(ctl, usr, mut rx)| async move {
                drop(ctl);
                drop(usr);
                let mut n = 0u32;
                while let Some(x) = rx.recv().await {
                    black_box(&x);
                    n += 1;
                }
                assert_eq!(n, N * 2);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, control_latency_under_backlog, drain_throughput);
criterion_main!(benches);
