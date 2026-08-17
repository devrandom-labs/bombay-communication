use communication::{Config, Received, TrySendError, UserClosed, mailbox_channel};
use std::sync::Arc;
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

    match stale.try_send(MoveOnly(3)) {
        Err(TrySendError::Closed(value)) => assert_eq!(value, MoveOnly(3)),
        Err(TrySendError::Full(_)) => panic!("closed mailbox reported full"),
        Ok(()) => panic!("stale ref admitted a message after close"),
    }
    let UserClosed(value) = stale.send(MoveOnly(4)).await.unwrap_err();
    assert_eq!(value, MoveOnly(4));

    assert_eq!(receiver.recv().await, Some(Received::User(MoveOnly(1))));
    assert_eq!(receiver.recv().await, Some(Received::User(MoveOnly(2))));
    assert_eq!(receiver.recv().await, Some(Received::UserLaneClosed));

    drop(control);
    assert_eq!(receiver.recv().await, None);
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
