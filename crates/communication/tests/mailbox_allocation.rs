//! Mailbox admission must preserve zero-allocation steady-state delivery.
//! A dedicated test binary keeps global allocation counts independent of
//! concurrent tests. The allocator forwards to System without changing
//! allocation or deallocation behavior.

use communication::{Capacity, Config, Consumer, MailboxRef, Received, mailbox_channel};
use std::alloc::{GlobalAlloc, Layout, System};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct AdmissionAllocationCount;

// SAFETY: every allocation/deallocation is forwarded unchanged to System;
// the independent counter observes successful allocation calls only.
unsafe impl GlobalAlloc for AdmissionAllocationCount {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let allocation = unsafe { System.alloc(layout) };
        if !allocation.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        allocation
    }

    unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
        unsafe { System.dealloc(allocation, layout) }
    }
}

#[global_allocator]
static ADMISSION_ALLOCATIONS: AdmissionAllocationCount = AdmissionAllocationCount;

async fn capacity_round(mailbox: &MailboxRef<u32>, receiver: &mut Consumer<(), u32>) {
    mailbox.try_send(7).unwrap();
    mailbox.try_send(7).unwrap();
    let mut capacity = pin!(mailbox.wait_for_capacity());
    let initial = poll_fn(|cx| Poll::Ready(capacity.as_mut().poll(cx))).await;
    assert_eq!(initial, Poll::Pending);
    for _ in 0..2 {
        let delivered = receiver.recv().await;
        assert_eq!(delivered, Some(Received::User(7)));
    }
    let available = capacity.await;
    assert_eq!(available, Capacity::Available);
}

#[test]
fn mailbox_delivery_and_rejection_allocate_nothing_after_construction() {
    println!(
        "LAYOUT consumer_box,consumer_u64,option_box,option_u64: {:?}",
        [
            Layout::new::<Consumer<(), Box<u64>>>(),
            Layout::new::<Consumer<(), u64>>(),
            Layout::new::<Option<Box<u64>>>(),
            Layout::new::<Option<u64>>(),
        ]
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (control, owner, mailbox, mut receiver) = mailbox_channel::<(), u32>(Config::new(2));
        mailbox.try_send(0).unwrap();
        let warmup = receiver.recv().await;
        assert_eq!(warmup, Some(Received::User(0)));
        capacity_round(&mailbox, &mut receiver).await;
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for value in 1..=4096 {
            mailbox.try_send(value).unwrap();
            let delivered = receiver.recv().await;
            assert_eq!(delivered, Some(Received::User(value)));
            capacity_round(&mailbox, &mut receiver).await;
            mailbox.send(value).await.unwrap();
            let delivered = receiver.recv().await;
            assert_eq!(delivered, Some(Received::User(value)));
        }
        owner.close_admission();
        let rejected = mailbox.send(4097).await.unwrap_err();
        assert_eq!(rejected.0, 4097);
        let after = ALLOCATIONS.load(Ordering::Relaxed);
        assert_eq!(after, before, "steady-state mailbox admission allocated");
        let terminal = receiver.recv().await;
        assert_eq!(terminal, Some(Received::UserLaneClosed));
        drop(control);
        let exhausted = receiver.recv().await;
        assert_eq!(exhausted, None);
    });
}
