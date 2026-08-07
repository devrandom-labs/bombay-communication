//! P8 memory-leak gate against the flume reference — proves the gate is
//! satisfiable (a bounded/reclaiming design passes). Self-contained; mirrors
//! `communication`'s `tests/leak.rs`.

use communication_reference::{Config, channel};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

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

const M: u32 = 50_000;
const BOUND: isize = 64;

#[test]
fn lanes_do_not_leak() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
        for i in 0..M {
            ctl.send(i).unwrap();
            let _ = rx.recv().await;
        }
        let base = LIVE.load(Ordering::Relaxed);
        for i in 0..M {
            ctl.send(i).unwrap();
            let _ = rx.recv().await;
        }
        let growth = LIVE.load(Ordering::Relaxed) - base;
        assert!(growth <= BOUND, "control lane leaks: +{growth} (P8)");
        drop((ctl, usr, rx));

        let (_ctl, usr, mut rx) = channel::<u32, u32>(Config::new(8));
        for i in 0..M {
            usr.try_send(i).unwrap();
            let _ = rx.recv().await;
        }
        let base = LIVE.load(Ordering::Relaxed);
        for i in 0..M {
            usr.try_send(i).unwrap();
            let _ = rx.recv().await;
        }
        let growth = LIVE.load(Ordering::Relaxed) - base;
        assert!(growth <= BOUND, "user lane leaks: +{growth} (P8)");
    });
}
