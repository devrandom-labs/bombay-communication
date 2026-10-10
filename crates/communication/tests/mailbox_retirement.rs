use communication::{Config, MailboxRef, Received, TrySendError, UserClosed, mailbox_channel};
use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use tokio::sync::Barrier;

#[derive(Debug, PartialEq, Eq)]
struct MoveOnly(u32);

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
