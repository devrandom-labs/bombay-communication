use communication::{Config, ControlClosed, channel};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::timeout;

#[derive(Debug)]
enum ControlNotice {
    Report(Box<[u8]>),
    Stop { nonce: u64 },
}

#[test]
fn closed_control_returns_original_report_without_mapping() {
    let (control, user, consumer) = channel::<ControlNotice, ()>(Config::new(1));
    drop(consumer);
    let report: Box<[u8]> = Box::from([7, 13, 17]);
    let allocation = report.as_ptr();
    let admitted = control.send_map(report, |_| panic!("closed admission must precede mapping"));
    let Err(ControlClosed(report)) = admitted else {
        panic!("closed control returns its original input");
    };
    assert_eq!(report.as_ptr(), allocation);
    assert_eq!(&*report, &[7, 13, 17]);
    drop(user);
}

#[tokio::test]
async fn mapped_and_direct_control_notices_keep_fifo_and_exact_payloads() {
    let (control, user, mut consumer) = channel::<ControlNotice, ()>(Config::new(1));
    let report: Box<[u8]> = Box::from([19, 23]);
    let allocation = report.as_ptr();
    let first = control.send(ControlNotice::Stop { nonce: 5 });
    let second = control.send_map(report, ControlNotice::Report);
    let third = control.send_map(29, |nonce| ControlNotice::Stop { nonce });
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(third.is_ok());
    let first = consumer.recv_control().await;
    let second = consumer.recv_control().await;
    let third = consumer.recv_control().await;
    assert!(matches!(first, Some(ControlNotice::Stop { nonce: 5 })));
    let Some(ControlNotice::Report(report)) = second else {
        panic!("second control notice is the exact mapped report");
    };
    assert_eq!(report.as_ptr(), allocation);
    assert_eq!(&*report, &[19, 23]);
    assert!(matches!(third, Some(ControlNotice::Stop { nonce: 29 })));
    drop((control, user));
    let closed = consumer.recv_control().await;
    assert!(closed.is_none());
}

#[derive(Debug)]
struct ReleaseNotice {
    releases: Arc<AtomicUsize>,
}

impl Drop for ReleaseNotice {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn consumer_closure_during_mapping_keeps_current_accepted_tail_custody() {
    let (control, user, consumer) = channel::<ReleaseNotice, ()>(Config::new(1));
    let releases = Arc::new(AtomicUsize::new(0));
    let notice = ReleaseNotice {
        releases: Arc::clone(&releases),
    };
    let admitted = control.send_map(notice, |notice| {
        drop(consumer);
        notice
    });
    assert!(admitted.is_ok());
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    drop((control, user));
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_mapping_reserves_no_unpublished_control_slot() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (control, user, mut consumer) = channel::<u64, ()>(Config::new(1));
        let input: Box<[u8]> = Box::from([31, 37]);
        let mapping = catch_unwind(AssertUnwindSafe(|| {
            control.send_map(input, |input| {
                drop(input);
                panic!("conversion stopped before enqueue");
            })
        }));
        assert!(mapping.is_err());
        let admitted = control.send(41);
        assert!(admitted.is_ok());
        let received = timeout(Duration::from_secs(2), consumer.recv_control()).await;
        assert_eq!(received.unwrap(), Some(41));
        drop((control, user));
    });
}
