//! Requirement 10 — shutdown returns all mailbox allocations and payloads to
//! baseline. A full blocked-producer teardown cycle (channel, saturated ring,
//! parked producers released with their payloads, full handle drop) must not
//! retain allocations: repeating it keeps the live-allocation count flat.
//!
//! One `#[test]` on purpose, in its own binary: the global allocator counter
//! is process-wide, so parallel tests would corrupt each other's measurement
//! (same discipline as `leak.rs`).

use communication::{Config, UserClosed, channel};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);

struct NetAlloc;
// SAFETY: forwards to System; the only added effect is a net live counter.
unsafe impl GlobalAlloc for NetAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(1, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(1, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static GLOBAL: NetAlloc = NetAlloc;

/// Drop-counted payload: pairs the allocation check with an ownership check.
#[derive(Debug)]
struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// One full cycle: channel → saturate the user ring → park two producers →
/// consumer teardown releases both with their payloads → drop everything.
/// Returns the number of payloads the cycle constructed (for drop counting).
async fn teardown_cycle(drops: &Arc<AtomicUsize>) -> usize {
    let (ctl, usr, rx) = channel::<u32, Counted>(Config::new(2));
    usr.try_send(Counted(drops.clone())).expect("ring empty");
    usr.try_send(Counted(drops.clone())).expect("capacity 2");
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let usr = usr.clone();
        let drops = drops.clone();
        tasks.push(tokio::spawn(async move { usr.send(Counted(drops)).await }));
    }
    // Drive the current-thread runtime until both sends are parked.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        tasks.iter().all(|t| !t.is_finished()),
        "a send resolved before teardown"
    );
    drop(rx);
    for task in tasks {
        let err: UserClosed<Counted> = task
            .await
            .expect("producer task panicked")
            .expect_err("teardown must release each blocked producer with its payload");
        drop(err);
    }
    drop((ctl, usr));
    4 // payloads constructed this cycle: 2 enqueued + 2 blocked
}

#[test]
fn shutdown_returns_allocations_to_baseline() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut built = 0usize;
        // Warm up: settle one-time runtime/allocator behaviour before measuring.
        built += teardown_cycle(&drops).await;
        let base = LIVE.load(Ordering::Relaxed);
        for _ in 0..64 {
            built += teardown_cycle(&drops).await;
        }
        let growth = LIVE.load(Ordering::Relaxed) - base;
        assert!(
            growth <= 64,
            "blocked-producer teardown retains allocations: +{growth} live over 64 shutdown cycles"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            built,
            "payload drops must equal constructions — leak or double-drop across teardown"
        );
    });
}
