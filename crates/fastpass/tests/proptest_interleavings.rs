//! Proptest over arbitrary lane compositions and interleavings — the upgrade
//! from the suite's 3-hardcoded-sends witnesses (review 2026-07-29).
//!
//! 1. `preloaded_backlog_matches_policy_model` — both lanes fully preloaded,
//!    senders dropped, then drained. With no timing in play the merge is a
//!    pure function of (users, controls, aging_cap), so the expected output is
//!    computed EXACTLY by a model of the documented policy and compared item
//!    for item. This fails on: the user-biased stub, missing aging, an
//!    off-by-one in the streak counter, or a lost/duplicated item.
//! 2. `concurrent_producers_no_loss_fifo` — spawned producers with generated
//!    yield patterns race the consumer; only timing-independent invariants are
//!    asserted (no loss, no duplication, per-lane FIFO), the ones that must
//!    hold under EVERY interleaving.

use fastpass::{Config, Received, channel};
use proptest::prelude::*;
use std::collections::VecDeque;
use std::time::Duration;

const GUARD: Duration = Duration::from_secs(10);

/// The documented dequeue policy, modeled exactly: control-first, with one
/// waiting user forced through after every `k` consecutive control dequeues.
/// Mirrors `Consumer::recv` including the guarded streak increment (streak
/// never exceeds `k`).
fn model(users: &mut VecDeque<u32>, controls: &mut VecDeque<u32>, k: usize) -> Vec<Received<u32, u32>> {
    let mut out = Vec::with_capacity(users.len() + controls.len());
    let mut streak = 0usize;
    while !users.is_empty() || !controls.is_empty() {
        if k != 0 && streak >= k && !users.is_empty() {
            out.push(Received::User(users.pop_front().unwrap()));
            streak = 0;
        } else if !controls.is_empty() {
            out.push(Received::Control(controls.pop_front().unwrap()));
            if k != 0 && streak < k {
                streak += 1;
            }
        } else {
            out.push(Received::User(users.pop_front().unwrap()));
            streak = 0;
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn preloaded_backlog_matches_policy_model(
        k in 0usize..=4,
        users in prop::collection::vec(any::<u32>(), 0..=40),
        controls in prop::collection::vec(any::<u32>(), 0..=40),
    ) {
        let expected = model(&mut users.clone().into(), &mut controls.clone().into(), k);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (ctl, usr, mut rx) =
                channel::<u32, u32>(Config::new(users.len().max(1)).with_aging_cap(k));
            for &u in &users {
                usr.try_send(u).unwrap();
            }
            for &c in &controls {
                ctl.send(c).unwrap();
            }
            drop(usr);
            drop(ctl);

            let mut got = Vec::with_capacity(users.len() + controls.len());
            while let Some(x) = tokio::time::timeout(GUARD, rx.recv())
                .await
                .expect("recv stalled on a preloaded, closed channel")
            {
                got.push(x);
            }
            prop_assert_eq!(got, expected, "merge diverged from the policy model");
            Ok(())
        })?;
    }

    #[test]
    fn concurrent_producers_no_loss_fifo(
        k in 0usize..=4,
        n_users in 0usize..=200,
        n_controls in 0usize..=200,
        // Yield pattern: which iterations each producer yields on — drives
        // different consumer/producer interleavings per case.
        c_yield in any::<u32>(),
        u_yield in any::<u32>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64).with_aging_cap(k));
            let producers = tokio::spawn(async move {
                let cp = tokio::spawn(async move {
                    for i in 0..n_controls as u32 {
                        ctl.send(i).unwrap();
                        if i % 7 == c_yield % 7 {
                            tokio::task::yield_now().await;
                        }
                    }
                });
                let up = tokio::spawn(async move {
                    for i in 0..n_users as u32 {
                        usr.send(i).await.unwrap();
                        if i % 7 == u_yield % 7 {
                            tokio::task::yield_now().await;
                        }
                    }
                });
                cp.await.unwrap();
                up.await.unwrap();
                // Senders drop here, closing both lanes.
            });

            let mut got_c = Vec::new();
            let mut got_u = Vec::new();
            loop {
                match tokio::time::timeout(GUARD, rx.recv()).await.expect("recv stalled") {
                    Some(Received::Control(c)) => got_c.push(c),
                    Some(Received::User(u)) => got_u.push(u),
                    None => break,
                }
            }
            producers.await.unwrap();

            prop_assert_eq!(
                got_c,
                (0..n_controls as u32).collect::<Vec<_>>(),
                "control lane: loss/dup/reorder"
            );
            prop_assert_eq!(
                got_u,
                (0..n_users as u32).collect::<Vec<_>>(),
                "user lane: loss/dup/reorder"
            );
            Ok(())
        })?;
    }
}
