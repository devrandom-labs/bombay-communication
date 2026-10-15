use communication::{
    Capacity, Config, MailboxRef, Received, TrySendError, UserClosed, mailbox_channel,
};
use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use tokio::sync::Barrier;

#[derive(Debug, PartialEq, Eq)]
struct MoveOnly(u32);

#[derive(Default)]
struct CapacityWakes(AtomicUsize);

impl Wake for CapacityWakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn capacity_observation_wakes_on_a_non_stride_pop_without_taking_the_original() {
    let (control, owner, mailbox, mut receiver) =
        mailbox_channel::<(), Box<MoveOnly>>(Config::new(2));
    let wakes = Arc::new(CapacityWakes::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut empty = Box::pin(mailbox.wait_for_capacity());
    let initially_available = empty.as_mut().poll(&mut context);
    drop(empty);
    mailbox.try_send(Box::new(MoveOnly(0))).unwrap();
    mailbox.try_send(Box::new(MoveOnly(1))).unwrap();
    let prefix = {
        let mut receiving = pin!(receiver.recv());
        receiving.as_mut().poll(&mut context)
    };
    mailbox.try_send(Box::new(MoveOnly(2))).unwrap();
    let original = Box::new(MoveOnly(3));
    let allocation = ptr::from_ref(original.as_ref());
    let refusal = mailbox.try_send(original);
    let Err(TrySendError::Full(original)) = refusal else {
        panic!("the independent prefix must occupy both slots");
    };
    let mut capacity = Box::pin(mailbox.wait_for_capacity());
    let parked = capacity.as_mut().poll(&mut context);
    let still_parked = capacity.as_mut().poll(&mut context);
    let idle_wakes = wakes.0.load(Ordering::Relaxed);
    let released = {
        let mut receiving = pin!(receiver.recv());
        receiving.as_mut().poll(&mut context)
    };
    let dequeue_wakes = wakes.0.load(Ordering::Relaxed);
    let available = capacity.as_mut().poll(&mut context);
    drop(capacity);
    let retained_allocation = ptr::from_ref(original.as_ref());
    let admitted = mailbox.try_send(original);
    owner.close_admission();
    drop(control);
    let mut tail = Vec::new();
    for _ in 0..4 {
        let mut receiving = pin!(receiver.recv());
        tail.push(receiving.as_mut().poll(&mut context));
    }
    let expected = [
        Poll::Ready(Some(Received::User(Box::new(MoveOnly(2))))),
        Poll::Ready(Some(Received::User(Box::new(MoveOnly(3))))),
        Poll::Ready(Some(Received::UserLaneClosed)),
        Poll::Ready(None),
    ];
    let delivered_allocation = match &tail[1] {
        Poll::Ready(Some(Received::User(delivered))) => Some(ptr::from_ref(delivered.as_ref())),
        _ => None,
    };
    let expected_prefix = Poll::Ready(Some(Received::User(Box::new(MoveOnly(0)))));
    let expected_released = Poll::Ready(Some(Received::User(Box::new(MoveOnly(1)))));
    assert_eq!(prefix, expected_prefix);
    assert_eq!(released, expected_released);
    assert!(matches!(
        &initially_available,
        Poll::Ready(Capacity::Available)
    ));
    assert!(matches!(&parked, Poll::Pending));
    assert!(matches!(&still_parked, Poll::Pending));
    assert_eq!(idle_wakes, 0);
    assert!(dequeue_wakes > idle_wakes);
    assert!(matches!(&available, Poll::Ready(Capacity::Available)));
    assert_eq!(retained_allocation, allocation);
    assert_eq!(delivered_allocation, Some(allocation));
    assert!(matches!(&admitted, Ok(())));
    assert_eq!(tail, expected);
}

#[test]
fn capacity_observation_closes_while_a_preclose_sender_still_owns_its_permit() {
    let (control, owner, mailbox, mut receiver) = mailbox_channel::<(), MoveOnly>(Config::new(2));
    mailbox.try_send(MoveOnly(11)).unwrap();
    mailbox.try_send(MoveOnly(13)).unwrap();
    let mut preclose = Box::pin(mailbox.send(MoveOnly(17)));
    let mut sender_context = Context::from_waker(Waker::noop());
    let sender_parked = preclose.as_mut().poll(&mut sender_context);
    let wakes = Arc::new(CapacityWakes::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut capacity = Box::pin(mailbox.wait_for_capacity());
    let parked = capacity.as_mut().poll(&mut context);
    owner.close_admission();
    let closure_wakes = wakes.0.load(Ordering::Relaxed);
    let first = {
        let mut receiving = pin!(receiver.recv());
        receiving.as_mut().poll(&mut sender_context)
    };
    let preclose_completion = preclose.as_mut().poll(&mut sender_context);
    drop(preclose);
    let mut replay = Box::pin(mailbox.wait_for_capacity());
    let replayed = replay.as_mut().poll(&mut context);
    drop(replay);
    drop(control);
    let mut trace = vec![first];
    for _ in 0..4 {
        let mut receiving = pin!(receiver.recv());
        trace.push(receiving.as_mut().poll(&mut sender_context));
    }
    let closed = capacity.as_mut().poll(&mut context);
    drop(capacity);
    let mut cleanup = Vec::new();
    for _ in 0..2 {
        let mut receiving = pin!(receiver.recv());
        cleanup.push(receiving.as_mut().poll(&mut sender_context));
    }
    let expected = [
        Poll::Ready(Some(Received::User(MoveOnly(11)))),
        Poll::Ready(Some(Received::User(MoveOnly(13)))),
        Poll::Ready(Some(Received::User(MoveOnly(17)))),
        Poll::Ready(Some(Received::UserLaneClosed)),
        Poll::Ready(None),
    ];
    assert!(matches!(&sender_parked, Poll::Pending));
    assert!(matches!(&parked, Poll::Pending));
    assert!(closure_wakes > 0);
    assert!(matches!(&closed, Poll::Ready(Capacity::Closed)));
    assert!(matches!(&replayed, Poll::Ready(Capacity::Closed)));
    assert!(matches!(&preclose_completion, Poll::Ready(Ok(()))));
    assert_eq!(trace, expected);
    assert_eq!(cleanup, [Poll::Ready(None), Poll::Ready(None)]);
}

#[test]
fn capacity_observation_returns_closed_on_consumer_teardown_without_losing_queued_work() {
    let (control, owner, mailbox, receiver) = mailbox_channel::<(), MoveOnly>(Config::new(2));
    mailbox.try_send(MoveOnly(19)).unwrap();
    mailbox.try_send(MoveOnly(23)).unwrap();
    let wakes = Arc::new(CapacityWakes::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut capacity = Box::pin(mailbox.wait_for_capacity());
    let parked = capacity.as_mut().poll(&mut context);
    let drained = receiver.drain();
    let teardown_wakes = wakes.0.load(Ordering::Relaxed);
    let closed = capacity.as_mut().poll(&mut context);
    drop(capacity);
    let rejected = mailbox.try_send(MoveOnly(29));
    owner.close_admission();
    drop(control);
    assert!(matches!(&parked, Poll::Pending));
    assert!(teardown_wakes > 0);
    assert!(matches!(&closed, Poll::Ready(Capacity::Closed)));
    assert_eq!(drained.control, []);
    assert_eq!(drained.user, [MoveOnly(19), MoveOnly(23)]);
    assert!(matches!(&rejected, Err(TrySendError::Closed(MoveOnly(29)))));
}

#[test]
fn capacity_observer_withdrawal_and_competition_preserve_native_sender_progress() {
    let (control, owner, mailbox, mut receiver) = mailbox_channel::<(), MoveOnly>(Config::new(2));
    mailbox.try_send(MoveOnly(31)).unwrap();
    mailbox.try_send(MoveOnly(37)).unwrap();
    let first_wakes = Arc::new(CapacityWakes::default());
    let second_wakes = Arc::new(CapacityWakes::default());
    let sender_wakes = Arc::new(CapacityWakes::default());
    let first_waker = Waker::from(first_wakes.clone());
    let second_waker = Waker::from(second_wakes.clone());
    let sender_waker = Waker::from(sender_wakes.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    let mut sender_context = Context::from_waker(&sender_waker);
    let mut first = Box::pin(mailbox.wait_for_capacity());
    let mut second = Box::pin(mailbox.wait_for_capacity());
    let first_parked = first.as_mut().poll(&mut first_context);
    let second_parked = second.as_mut().poll(&mut second_context);
    let mut sender = Box::pin(mailbox.send(MoveOnly(41)));
    let sender_parked = sender.as_mut().poll(&mut sender_context);
    let prefix = {
        let mut receiving = pin!(receiver.recv());
        receiving.as_mut().poll(&mut sender_context)
    };
    let first_notifications = first_wakes.0.load(Ordering::Relaxed);
    let second_notifications = second_wakes.0.load(Ordering::Relaxed);
    let sender_notifications = sender_wakes.0.load(Ordering::Relaxed);
    drop(first);
    let available = second.as_mut().poll(&mut second_context);
    drop(second);
    let competitor = mailbox.try_send(MoveOnly(43));
    let outside = Box::new(MoveOnly(47));
    let allocation = ptr::from_ref(outside.as_ref());
    let mut cancelled = Box::pin(mailbox.wait_for_capacity());
    let competition = cancelled.as_mut().poll(&mut second_context);
    let cancellation_wakes = second_wakes.0.load(Ordering::Relaxed);
    drop(cancelled);
    let released = {
        let mut receiving = pin!(receiver.recv());
        receiving.as_mut().poll(&mut sender_context)
    };
    let sender_completion = sender.as_mut().poll(&mut sender_context);
    drop(sender);
    owner.close_admission();
    drop(control);
    let mut trace = vec![prefix, released];
    for _ in 0..4 {
        let mut receiving = pin!(receiver.recv());
        trace.push(receiving.as_mut().poll(&mut sender_context));
    }
    let after_cancellation_wakes = second_wakes.0.load(Ordering::Relaxed);
    let retained_allocation = ptr::from_ref(outside.as_ref());
    let expected = [
        Poll::Ready(Some(Received::User(MoveOnly(31)))),
        Poll::Ready(Some(Received::User(MoveOnly(37)))),
        Poll::Ready(Some(Received::User(MoveOnly(43)))),
        Poll::Ready(Some(Received::User(MoveOnly(41)))),
        Poll::Ready(Some(Received::UserLaneClosed)),
        Poll::Ready(None),
    ];
    assert!(matches!(&first_parked, Poll::Pending));
    assert!(matches!(&second_parked, Poll::Pending));
    assert!(matches!(&sender_parked, Poll::Pending));
    assert!(first_notifications > 0);
    assert!(second_notifications > 0);
    assert!(sender_notifications > 0);
    assert!(matches!(&available, Poll::Ready(Capacity::Available)));
    assert!(matches!(&competitor, Ok(())));
    assert!(matches!(&competition, Poll::Pending));
    assert_eq!(after_cancellation_wakes, cancellation_wakes);
    assert_eq!(retained_allocation, allocation);
    assert_eq!(*outside, MoveOnly(47));
    assert!(matches!(&sender_completion, Poll::Ready(Ok(()))));
    assert_eq!(trace, expected);
}

#[tokio::test]
async fn close_rejects_stale_refs_with_the_exact_payload_and_drains_accepted_work() {
    let (control, owner, actor_ref, mut receiver) = mailbox_channel::<(), MoveOnly>(Config::new(8));
    let stale = actor_ref.clone();

    actor_ref.send(MoveOnly(1)).await.unwrap();
    actor_ref.try_send(MoveOnly(2)).unwrap();
    owner.close_admission();

    let try_closed = stale
        .try_send(MoveOnly(3))
        .expect_err("stale ref cannot admit a message after close");
    let try_description = try_closed.to_string();
    match try_closed {
        TrySendError::Closed(value) => assert_eq!(value, MoveOnly(3)),
        TrySendError::Full(_) => panic!("closed mailbox reported full"),
    }
    let user_closed = stale.send(MoveOnly(4)).await.unwrap_err();
    let user_description = user_closed.to_string();
    let UserClosed(value) = user_closed;
    assert_eq!(value, MoveOnly(4));

    let first = receiver.recv().await;
    let second = receiver.recv().await;
    let terminal = receiver.recv().await;
    drop(control);
    let exhausted = receiver.recv().await;
    let trace = [first, second, terminal, exhausted];
    let expected = [
        Some(Received::User(MoveOnly(1))),
        Some(Received::User(MoveOnly(2))),
        Some(Received::UserLaneClosed),
        None,
    ];
    assert_eq!(trace, expected);
    assert_eq!(try_description, "user lane closed");
    assert_eq!(user_description, "user lane closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_racing_delivery_has_one_exact_linearizable_outcome() {
    for value in 0..500 {
        let (control, owner, actor_ref, mut receiver) =
            mailbox_channel::<(), MoveOnly>(Config::new(2));
        let barrier = Arc::new(Barrier::new(3));
        let sender_barrier = barrier.clone();
        let closer_barrier = barrier.clone();

        let sender = tokio::spawn(async move {
            sender_barrier.wait().await;
            actor_ref.try_send(MoveOnly(value))
        });
        let closer = tokio::spawn(async move {
            closer_barrier.wait().await;
            owner.close_admission();
        });
        barrier.wait().await;

        let result = sender.await.unwrap();
        closer.await.unwrap();
        drop(control);

        let first = receiver.recv().await;
        match result {
            Ok(()) => {
                assert_eq!(first, Some(Received::User(MoveOnly(value))));
                assert_eq!(receiver.recv().await, Some(Received::UserLaneClosed));
            }
            Err(TrySendError::Closed(returned)) => {
                assert_eq!(returned, MoveOnly(value));
                assert_eq!(first, Some(Received::UserLaneClosed));
            }
            Err(TrySendError::Full(_)) => panic!("empty mailbox reported full"),
        }
        assert_eq!(receiver.recv().await, None);
    }
}

#[tokio::test]
async fn delivery_admitted_before_close_finishes_before_terminal_marker() {
    let (control, owner, actor_ref, mut receiver) = mailbox_channel::<(), MoveOnly>(Config::new(2));
    actor_ref.try_send(MoveOnly(0)).unwrap();
    actor_ref.try_send(MoveOnly(1)).unwrap();

    let blocked = tokio::spawn(async move { actor_ref.send(MoveOnly(2)).await });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !blocked.is_finished(),
        "send did not block on the full mailbox"
    );

    owner.close_admission();
    assert_eq!(receiver.recv().await, Some(Received::User(MoveOnly(0))));
    assert_eq!(receiver.recv().await, Some(Received::User(MoveOnly(1))));
    assert_eq!(receiver.recv().await, Some(Received::User(MoveOnly(2))));
    blocked.await.unwrap().unwrap();
    assert_eq!(receiver.recv().await, Some(Received::UserLaneClosed));

    drop(control);
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn closed_owner_denies_a_new_delivery_while_a_preclose_permit_survives() {
    let (control, owner, mailbox, mut receiver) =
        mailbox_channel::<(), Box<MoveOnly>>(Config::new(2));
    let first = Box::new(MoveOnly(1));
    let first_allocation = ptr::from_ref(first.as_ref());
    let second = Box::new(MoveOnly(2));
    let second_allocation = ptr::from_ref(second.as_ref());
    mailbox.send(first).await.unwrap();
    mailbox.send(second).await.unwrap();
    let preclose_payload = Box::new(MoveOnly(3));
    let preclose_allocation = ptr::from_ref(preclose_payload.as_ref());
    let mut preclose = pin!(mailbox.send(preclose_payload));
    let mut context = Context::from_waker(Waker::noop());
    let parked = preclose.as_mut().poll(&mut context);
    assert!(matches!(&parked, Poll::Pending));
    owner.close_admission();
    control.send(()).unwrap();
    let postclose_payload = Box::new(MoveOnly(4));
    let postclose_allocation = ptr::from_ref(postclose_payload.as_ref());
    let mut postclose = pin!(mailbox.send(postclose_payload));
    let mut trace = Vec::new();
    let closed_control = receiver.recv_control().await;
    assert_eq!(closed_control, Some(()));
    for _ in 0..2 {
        let event = receiver.recv().await.unwrap();
        let Received::User(payload) = event else {
            panic!("admitted prefix precedes terminal marker");
        };
        trace.push(payload);
    }
    let next_admission = postclose.as_mut().poll(&mut context);
    let preclose_completion = preclose.await;
    match preclose_completion {
        Ok(()) => {}
        Err(UserClosed(payload)) => panic!("preclose permit rejected exact payload {payload:?}"),
    }
    let postclose_completion = match next_admission {
        Poll::Ready(result) => result,
        Poll::Pending => postclose.await,
    };
    loop {
        let event = receiver.recv().await.unwrap();
        match event {
            Received::User(payload) => trace.push(payload),
            Received::UserLaneClosed => break,
            Received::Control(()) => panic!("all control events were accounted for"),
        }
    }
    drop(control);
    let exhausted = receiver.recv().await;
    assert_eq!(exhausted, None);
    assert_eq!(ptr::from_ref(trace[0].as_ref()), first_allocation);
    assert_eq!(ptr::from_ref(trace[1].as_ref()), second_allocation);
    let retained_preclose = trace.iter().find(|payload| payload.0 == 3).unwrap();
    assert_eq!(
        ptr::from_ref(retained_preclose.as_ref()),
        preclose_allocation
    );
    println!("complete owning trace {:?}", trace);
    let trace_values: Vec<_> = trace.iter().map(|payload| payload.0).collect();
    match postclose_completion {
        Err(UserClosed(rejected)) => {
            assert_eq!(rejected.0, 4);
            assert_eq!(ptr::from_ref(rejected.as_ref()), postclose_allocation);
            assert_eq!(trace_values, [1, 2, 3]);
        }
        Ok(()) => {
            assert_eq!(trace_values, [1, 2, 4, 3]);
            assert_eq!(ptr::from_ref(trace[2].as_ref()), postclose_allocation);
            panic!(
                "new delivery acquired admission after owner closed with only a preclose permit alive"
            );
        }
    }
}

#[tokio::test]
async fn constructing_a_delivery_before_close_does_not_acquire_admission() {
    let (control, owner, mailbox, mut receiver) =
        mailbox_channel::<(), Box<MoveOnly>>(Config::new(2));
    let payload = Box::new(MoveOnly(29));
    let allocation = ptr::from_ref(payload.as_ref());
    let delivery = mailbox.send(payload);
    drop(owner);
    let rejected = delivery.await.unwrap_err().0;
    assert_eq!(rejected.0, 29);
    assert_eq!(ptr::from_ref(rejected.as_ref()), allocation);
    let terminal = receiver.recv().await;
    assert_eq!(terminal, Some(Received::UserLaneClosed));
    let replayed = mailbox.try_send(rejected);
    let Err(TrySendError::Closed(rejected)) = replayed else {
        panic!("replayed closed admission accepted the original payload");
    };
    assert_eq!(ptr::from_ref(rejected.as_ref()), allocation);
    drop(control);
    let exhausted = receiver.recv().await;
    assert_eq!(exhausted, None);
}

#[derive(Debug)]
struct DropCountedDelivery {
    message_id: u32,
    drops: Arc<AtomicUsize>,
}

impl Drop for DropCountedDelivery {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn cancelling_a_preclose_delivery_releases_its_sender_without_closing_owner() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (control, owner, mailbox, mut receiver) =
        mailbox_channel::<(), DropCountedDelivery>(Config::new(2));
    for message_id in [31, 37] {
        mailbox
            .try_send(DropCountedDelivery {
                message_id,
                drops: drops.clone(),
            })
            .unwrap();
    }
    let mut cancelled = Box::pin(mailbox.send(DropCountedDelivery {
        message_id: 41,
        drops: drops.clone(),
    }));
    let mut context = Context::from_waker(Waker::noop());
    let parked = cancelled.as_mut().poll(&mut context);
    assert!(matches!(&parked, Poll::Pending));
    drop(cancelled);
    let cancelled_drops = drops.load(Ordering::Relaxed);
    assert_eq!(cancelled_drops, 1);
    let first = receiver.recv().await;
    let second = receiver.recv().await;
    mailbox
        .send(DropCountedDelivery {
            message_id: 43,
            drops: drops.clone(),
        })
        .await
        .unwrap();
    owner.close_admission();
    let third = receiver.recv().await;
    let terminal = receiver.recv().await;
    drop(control);
    let exhausted = receiver.recv().await;
    let trace = [first, second, third, terminal, exhausted];
    let observed: Vec<Option<Received<(), u32>>> = trace
        .iter()
        .map(|event| match event {
            Some(Received::User(payload)) => Some(Received::User(payload.message_id)),
            Some(Received::UserLaneClosed) => Some(Received::UserLaneClosed),
            None => None,
            Some(Received::Control(())) => panic!("no control traffic was offered"),
        })
        .collect();
    let expected = [
        Some(Received::User(31)),
        Some(Received::User(37)),
        Some(Received::User(43)),
        Some(Received::UserLaneClosed),
        None,
    ];
    assert_eq!(observed, expected);
    drop(trace);
    let total_drops = drops.load(Ordering::Relaxed);
    assert_eq!(total_drops, 4);
}

#[derive(Debug, PartialEq, Eq)]
enum ReenteredAdmission {
    Closed(u32),
    Full(u32),
    Accepted,
}

struct ReenteringDelivery {
    message_id: u32,
    mailbox: Option<MailboxRef<ReenteringDelivery>>,
    trace: Arc<Mutex<Vec<ReenteredAdmission>>>,
}

impl Drop for ReenteringDelivery {
    fn drop(&mut self) {
        let Some(mailbox) = self.mailbox.take() else {
            return;
        };
        let reentered = mailbox.try_send(Self {
            message_id: self.message_id,
            mailbox: None,
            trace: self.trace.clone(),
        });
        let admission = match reentered {
            Err(TrySendError::Closed(returned)) => ReenteredAdmission::Closed(returned.message_id),
            Err(TrySendError::Full(returned)) => ReenteredAdmission::Full(returned.message_id),
            Ok(()) => ReenteredAdmission::Accepted,
        };
        self.trace.lock().unwrap().push(admission);
    }
}

#[test]
fn queued_delivery_destruction_can_reenter_closed_admission() {
    let (control, owner, mailbox, receiver) =
        mailbox_channel::<(), ReenteringDelivery>(Config::new(2));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let queued = ReenteringDelivery {
        message_id: 47,
        mailbox: Some(mailbox.clone()),
        trace: trace.clone(),
    };
    let admitted = mailbox.try_send(queued);
    match admitted {
        Ok(()) => {}
        Err(_) => panic!("empty mailbox rejected queued delivery"),
    }
    drop(receiver);
    owner.close_admission();
    let observed = trace.lock().unwrap();
    let expected = [ReenteredAdmission::Closed(47)];
    assert_eq!(observed.as_slice(), expected);
    drop((control, mailbox));
}

#[tokio::test]
async fn admission_accepts_send_payloads_without_a_sync_bound() {
    let (control, owner, mailbox, mut receiver) = mailbox_channel::<(), Cell<u32>>(Config::new(2));
    let admitted = thread::scope(|threads| {
        threads
            .spawn(|| mailbox.try_send(Cell::new(53)))
            .join()
            .unwrap()
    });
    admitted.unwrap();
    owner.close_admission();
    let delivered = receiver.recv().await;
    let Some(Received::User(message)) = delivered else {
        panic!("non-Sync message was not delivered");
    };
    let message_id = message.into_inner();
    assert_eq!(message_id, 53);
    let terminal = receiver.recv().await;
    assert_eq!(terminal, Some(Received::UserLaneClosed));
    drop(control);
    let exhausted = receiver.recv().await;
    assert_eq!(exhausted, None);
}

mod capacity_notification {
    use super::{CapacityWakes, Config, Received, mailbox_channel};
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::pin;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Debug)]
    struct Delivery {
        id: u32,
        dropped: Arc<Mutex<Vec<u32>>>,
    }

    impl Drop for Delivery {
        fn drop(&mut self) {
            self.dropped.lock().unwrap().push(self.id);
        }
    }

    struct Notification(Arc<AtomicUsize>);

    impl Wake for Notification {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("capacity notification failed");
        }
    }

    #[test]
    fn a_capacity_waker_panic_does_not_erase_the_dequeued_original() {
        let dropped = Arc::new(Mutex::new(Vec::with_capacity(2)));
        let [first, second] = [59, 61].map(|id| {
            Box::new(Delivery {
                id,
                dropped: dropped.clone(),
            })
        });
        let first_allocation = ptr::from_ref(first.as_ref());
        let second_allocation = ptr::from_ref(second.as_ref());
        let (control, owner, mailbox, mut receiver) =
            mailbox_channel::<(), Box<Delivery>>(Config::new(2));
        mailbox.try_send(first).unwrap();
        mailbox.try_send(second).unwrap();
        let notifications = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(Notification(notifications.clone())));
        let mut context = Context::from_waker(&waker);
        let mut capacity = Box::pin(mailbox.wait_for_capacity());
        let parked = capacity.as_mut().poll(&mut context);
        let mut receiving_context = Context::from_waker(Waker::noop());
        let acquired = catch_unwind(AssertUnwindSafe(|| {
            let mut receiving = pin!(receiver.recv());
            receiving.as_mut().poll(&mut receiving_context)
        }));
        drop(capacity);
        let dropped_before_ready = dropped.lock().unwrap().clone();
        let drained = receiver.drain();
        owner.close_admission();
        drop(control);
        let dropped_after_cleanup = dropped.lock().unwrap().clone();
        let remaining: Vec<_> = drained
            .user
            .iter()
            .map(|original| (original.id, ptr::from_ref(original.as_ref())))
            .collect();
        assert!(parked.is_pending());
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
        assert!(acquired.is_err());
        assert!(drained.control.is_empty());
        assert_eq!(remaining, [(59, first_allocation), (61, second_allocation)]);
        assert_eq!(dropped_after_cleanup, dropped_before_ready);
        assert!(dropped_before_ready.is_empty());
    }

    #[test]
    fn a_cancelled_receive_after_wake_unwind_preserves_fifo_and_terminal_order() {
        let dropped = Arc::new(Mutex::new(Vec::with_capacity(2)));
        let [first, second] = [71, 73].map(|id| {
            Box::new(Delivery {
                id,
                dropped: dropped.clone(),
            })
        });
        let allocations = [
            ptr::from_ref(first.as_ref()),
            ptr::from_ref(second.as_ref()),
        ];
        let (control, owner, mailbox, mut receiver) =
            mailbox_channel::<u32, Box<Delivery>>(Config::new(2));
        mailbox.try_send(first).unwrap();
        mailbox.try_send(second).unwrap();
        let notifications = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(Notification(notifications.clone())));
        let mut context = Context::from_waker(&waker);
        let mut capacity = Box::pin(mailbox.wait_for_capacity());
        let parked = capacity.as_mut().poll(&mut context);
        let mut receiving_context = Context::from_waker(Waker::noop());
        let mut receiving = Box::pin(receiver.recv());
        let acquired = catch_unwind(AssertUnwindSafe(|| {
            receiving.as_mut().poll(&mut receiving_context)
        }));
        drop(receiving);
        drop(capacity);
        let dropped_before_retry = dropped.lock().unwrap().clone();
        let third = Box::new(Delivery {
            id: 75,
            dropped: dropped.clone(),
        });
        let third_allocation = ptr::from_ref(third.as_ref());
        let refilled = mailbox.try_send(third);
        let retry_notifications = Arc::new(CapacityWakes::default());
        let retry_waker = Waker::from(retry_notifications.clone());
        let mut retry_context = Context::from_waker(&retry_waker);
        let mut retry_capacity = Box::pin(mailbox.wait_for_capacity());
        let retry_parked = retry_capacity.as_mut().poll(&mut retry_context);
        let idle_retry_wakes = retry_notifications.0.load(Ordering::Relaxed);
        let control_admitted = control.send(79);
        let only_control = {
            let mut receiving = pin!(receiver.recv_control());
            receiving.as_mut().poll(&mut receiving_context)
        };
        let mut trace = Vec::new();
        {
            let mut receiving = pin!(receiver.recv());
            trace.push(receiving.as_mut().poll(&mut receiving_context));
        }
        let retried_wakes = retry_notifications.0.load(Ordering::Relaxed);
        let still_full = retry_capacity.as_mut().poll(&mut retry_context);
        drop(retry_capacity);
        owner.close_admission();
        drop(control);
        for _ in 0..4 {
            let mut receiving = pin!(receiver.recv());
            trace.push(receiving.as_mut().poll(&mut receiving_context));
        }
        let drained = receiver.drain();
        let dropped_after_cleanup = dropped.lock().unwrap().clone();
        let delivered: Vec<_> = trace
            .iter()
            .filter_map(|received| match received {
                Poll::Ready(Some(Received::User(original))) => {
                    Some((original.id, ptr::from_ref(original.as_ref())))
                }
                _ => None,
            })
            .collect();
        assert!(parked.is_pending());
        assert!(acquired.is_err());
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
        assert!(dropped_before_retry.is_empty());
        assert!(matches!(refilled, Ok(())));
        assert!(matches!(retry_parked, Poll::Pending));
        assert_eq!(idle_retry_wakes, 0);
        assert!(retried_wakes > idle_retry_wakes);
        assert!(matches!(still_full, Poll::Pending));
        assert!(matches!(control_admitted, Ok(())));
        assert!(matches!(only_control, Poll::Ready(Some(79))));
        assert_eq!(
            delivered,
            [
                (71, allocations[0]),
                (73, allocations[1]),
                (75, third_allocation)
            ]
        );
        assert!(matches!(
            &trace[3],
            Poll::Ready(Some(Received::UserLaneClosed))
        ));
        assert!(matches!(&trace[4], Poll::Ready(None)));
        assert!(drained.control.is_empty());
        assert!(drained.user.is_empty());
        assert!(dropped_after_cleanup.is_empty());
    }

    #[test]
    fn surrendering_the_consumer_after_wake_unwind_drops_each_original_once() {
        let dropped = Arc::new(Mutex::new(Vec::with_capacity(2)));
        let (control, owner, mailbox, mut receiver) =
            mailbox_channel::<(), Box<Delivery>>(Config::new(2));
        for id in [83, 89] {
            let admitted = mailbox.try_send(Box::new(Delivery {
                id,
                dropped: dropped.clone(),
            }));
            admitted.unwrap();
        }
        let notifications = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(Notification(notifications.clone())));
        let mut context = Context::from_waker(&waker);
        let mut capacity = Box::pin(mailbox.wait_for_capacity());
        let parked = capacity.as_mut().poll(&mut context);
        let mut receiving_context = Context::from_waker(Waker::noop());
        let acquired = catch_unwind(AssertUnwindSafe(|| {
            let mut receiving = pin!(receiver.recv());
            receiving.as_mut().poll(&mut receiving_context)
        }));
        drop(capacity);
        let dropped_before_surrender = dropped.lock().unwrap().clone();
        drop(receiver);
        owner.close_admission();
        drop(control);
        drop(mailbox);
        let mut dropped_after_surrender = dropped.lock().unwrap().clone();
        dropped_after_surrender.sort_unstable();
        assert!(parked.is_pending());
        assert!(acquired.is_err());
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
        assert!(dropped_before_surrender.is_empty());
        assert_eq!(dropped_after_surrender, [83, 89]);
    }
}
