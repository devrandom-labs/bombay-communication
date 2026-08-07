//! Perf signals for the two-lane merge. The headline number is P1: control
//! recv latency while the user lane is saturated — it must stay flat as the
//! backlog grows. A second bench measures drain throughput.

use communication::{Config, Received, channel};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use tokio::runtime::Runtime;

const DRAIN_ITEMS_PER_LANE: u32 = 10_000;
const CONTENTION_ITEMS_PER_LANE: u32 = 10_000;

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
    c.bench_function("drain_throughput_20k", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let (ctl, usr, rx) =
                    channel::<u32, u32>(Config::new(DRAIN_ITEMS_PER_LANE as usize));
                for i in 0..DRAIN_ITEMS_PER_LANE {
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
                    match x {
                        // The user stream's one-shot end-marker is not an item.
                        Received::UserLaneClosed => {}
                        Received::Control(_) | Received::User(_) => n += 1,
                    }
                }
                assert_eq!(n, DRAIN_ITEMS_PER_LANE * 2);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// ADR comparison: one-structure priority wrapper (single Mutex around both
// lanes + Notify) implementing the SAME merge policy. Bench-only prototype —
// it exists so the 2-channel vs 1-structure decision carries numbers
// (bombay card #225), not to be shipped.

struct OneStruct {
    state: parking_lot::Mutex<OneState>,
    notify: tokio::sync::Notify,
}

struct OneState {
    control: std::collections::VecDeque<u32>,
    user: std::collections::VecDeque<u32>,
    cap: usize,
    senders_open: bool,
}

impl OneStruct {
    fn new(cap: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(OneState {
                control: std::collections::VecDeque::new(),
                user: std::collections::VecDeque::new(),
                cap,
                senders_open: true,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn send_control(&self, v: u32) {
        self.state.lock().control.push_back(v);
        self.notify.notify_one();
    }

    fn send_user(&self, v: u32) {
        let mut s = self.state.lock();
        while s.user.len() >= s.cap {
            drop(s);
            std::thread::yield_now();
            s = self.state.lock();
        }
        s.user.push_back(v);
        drop(s);
        self.notify.notify_one();
    }

    fn close_senders(&self) {
        self.state.lock().senders_open = false;
        self.notify.notify_one();
    }

    async fn recv(&self) -> Option<u32> {
        loop {
            {
                let mut s = self.state.lock();
                if let Some(c) = s.control.pop_front() {
                    return Some(c);
                }
                if let Some(u) = s.user.pop_front() {
                    return Some(u);
                }
                if !s.senders_open {
                    return None;
                }
            }
            // notify_one stores a permit when there are no waiters, so a
            // notification landing between the check and the await is not lost.
            self.notify.notified().await;
        }
    }
}

fn control_latency_under_backlog_one_struct(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("control_latency_under_backlog_one_struct");
    for depth in [0usize, 64, 1024, 8192] {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.to_async(&rt).iter_batched(
                || {
                    let q = OneStruct::new(depth.max(1));
                    for i in 0..depth {
                        q.send_user(u32::try_from(i).unwrap());
                    }
                    q.send_control(1);
                    q
                },
                |q| async move {
                    black_box(q.recv().await);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// P7-style contention: 2 producer tasks hammering both lanes while the
// consumer drains 40k items. Two-channel = two independent flume locks;
// one-struct = a single Mutex every send AND recv contends on.
fn producer_contention(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("producer_contention_2p_40k");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    group.bench_function("two_channel", |b| {
        b.to_async(&rt).iter(|| async {
            let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(4096));
            let mut handles = Vec::new();
            for _ in 0..2 {
                let ctl = ctl.clone();
                let usr = usr.clone();
                handles.push(tokio::spawn(async move {
                    for i in 0..CONTENTION_ITEMS_PER_LANE {
                        ctl.send(i).unwrap();
                        usr.send(i).await.unwrap();
                    }
                }));
            }
            drop(ctl);
            drop(usr);
            let mut n = 0u32;
            while let Some(x) = rx.recv().await {
                black_box(&x);
                match x {
                    // The user stream's one-shot end-marker is not an item.
                    Received::UserLaneClosed => {}
                    Received::Control(_) | Received::User(_) => n += 1,
                }
            }
            assert_eq!(n, CONTENTION_ITEMS_PER_LANE * 4);
            for h in handles {
                h.await.unwrap();
            }
        });
    });

    group.bench_function("one_struct", |b| {
        b.to_async(&rt).iter(|| async {
            let q = std::sync::Arc::new(OneStruct::new(4096));
            let mut handles = Vec::new();
            for _ in 0..2 {
                let q = std::sync::Arc::clone(&q);
                handles.push(tokio::spawn(async move {
                    for i in 0..CONTENTION_ITEMS_PER_LANE {
                        q.send_control(i);
                        q.send_user(i);
                    }
                }));
            }
            let consumer = tokio::spawn({
                let q = std::sync::Arc::clone(&q);
                async move {
                    let mut n = 0u32;
                    while let Some(x) = q.recv().await {
                        black_box(&x);
                        n += 1;
                    }
                    n
                }
            });
            for h in handles {
                h.await.unwrap();
            }
            q.close_senders();
            let n = consumer.await.unwrap();
            assert_eq!(n, CONTENTION_ITEMS_PER_LANE * 4);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    control_latency_under_backlog,
    control_latency_under_backlog_one_struct,
    drain_throughput,
    producer_contention
);
criterion_main!(benches);
