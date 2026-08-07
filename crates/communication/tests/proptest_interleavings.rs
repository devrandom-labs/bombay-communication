//! Proptest over arbitrary lane compositions and interleavings — the upgrade
//! from the suite's 3-hardcoded-sends witnesses (review 2026-07-29).
//!
//! 1. `preloaded_backlog_matches_policy_model` — both lanes fully preloaded,
//!    senders dropped, then drained. With no timing in play the merge is a
//!    pure function of (users, controls, `aging_cap`), so the expected output is
//!    computed EXACTLY by a model of the documented policy and compared item
//!    for item. This fails on: the user-biased stub, missing aging, an
//!    off-by-one in the streak counter, or a lost/duplicated item.
//! 2. `concurrent_producers_no_loss_fifo` — spawned producers with generated
//!    yield patterns race the consumer; only timing-independent invariants are
//!    asserted (no loss, no duplication, per-lane FIFO), the ones that must
//!    hold under EVERY interleaving.
//! 3. `anchor_schedules_match_lane_state_machine` — generated
//!    send/anchor-send/upgrade/drop/recv schedules compared step for step
//!    against the card-3 user-lane state machine (Open / Closing / Closed):
//!    an anchor op succeeds iff the state is Open, closed ops return the
//!    original payload, and the stream ends with the one-shot end-marker.

use communication::{Config, Received, TrySendError, channel};
use proptest::prelude::*;
use std::collections::VecDeque;
use std::time::Duration;

const GUARD: Duration = Duration::from_secs(10);

/// The documented dequeue policy, modeled exactly: control-first, with one
/// waiting user forced through after every `k` consecutive control dequeues.
/// Mirrors `Consumer::recv` including the guarded streak increment (streak
/// never exceeds `k`) and, since the lane-lifecycle addition, the one-shot
/// `UserLaneClosed` end-marker: the senders are dropped before the drain
/// starts, so the marker is deliverable wherever the policy next serves a
/// user with an empty user queue — the aging slot (streak at `k`) or the
/// control-empty slot — and it resets the streak like a user item.
fn model(
    users: &mut VecDeque<u32>,
    controls: &mut VecDeque<u32>,
    k: usize,
) -> Vec<Received<u32, u32>> {
    let mut out = Vec::with_capacity(users.len() + controls.len() + 1);
    let mut streak = 0usize;
    let mut leg_pending = true; // user lane terminally closed from the start
    loop {
        if k != 0 && streak >= k {
            if let Some(u) = users.pop_front() {
                out.push(Received::User(u));
                streak = 0;
                continue;
            }
            if leg_pending {
                out.push(Received::UserLaneClosed);
                leg_pending = false;
                streak = 0;
                continue;
            }
            // Aging pop came up empty and the marker is spent: fall through
            // to control with the streak pinned at the cap.
        }
        if let Some(c) = controls.pop_front() {
            out.push(Received::Control(c));
            if k != 0 && streak < k {
                streak += 1;
            }
            continue;
        }
        if let Some(u) = users.pop_front() {
            out.push(Received::User(u));
            streak = 0;
            continue;
        }
        if leg_pending {
            out.push(Received::UserLaneClosed);
            leg_pending = false;
            streak = 0;
            continue;
        }
        break;
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
    #[allow(
        clippy::cast_possible_truncation,
        reason = "generated counts are bounded to 200 by the proptest strategy"
    )]
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
            let mut legs = 0u32;
            loop {
                match tokio::time::timeout(GUARD, rx.recv()).await.expect("recv stalled") {
                    Some(Received::Control(c)) => got_c.push(c),
                    Some(Received::User(u)) => got_u.push(u),
                    // End-marker: deliverable once the producers' senders
                    // drop with the ring drained; at most once per channel.
                    Some(Received::UserLaneClosed) => legs += 1,
                    None => break,
                }
            }
            producers.await.unwrap();

            prop_assert!(legs <= 1, "UserLaneClosed fired {legs}x — must be at most once");

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

/// One step of a generated anchor schedule. The schedule is sequential, so
/// each op's outcome is a pure function of the model state (card 3).
#[derive(Clone, Copy, Debug)]
enum Op {
    /// `UserSender::try_send` through the counting sender.
    Send(u32),
    /// `UserAnchor::try_send` — succeeds iff the state is Open.
    AnchorSend(u32),
    /// `UserAnchor::upgrade` — `Some` iff the state is Open.
    Upgrade,
    /// Drop the (single) counting sender — moves the state to Closing.
    DropLastSender,
    /// Consume one stream item and compare with the model.
    Recv,
}

/// The user-lane anchor state machine (card 3): `Open(count>0)`,
/// `Closing(count==0, items or the leg still pending)`, and `Closed(0, leg
/// delivered — terminal)`. The queue holds the items published by
/// successful sends in FIFO order; the leg is the stream's terminal item.
struct LaneModel {
    count: usize,
    queue: VecDeque<u32>,
    leg_delivered: bool,
}

impl LaneModel {
    fn new() -> Self {
        Self {
            count: 1,
            queue: VecDeque::new(),
            leg_delivered: false,
        }
    }

    fn anchor_ok(&self) -> bool {
        self.count > 0
    }

    fn drop_sender(&mut self) {
        self.count = 0;
    }

    /// The next stream item the policy must produce, or `None` when recv
    /// would park (open empty lane, or closed empty lane with the marker
    /// already spent).
    fn next_stream(&mut self) -> Option<Received<u32, u32>> {
        if let Some(u) = self.queue.pop_front() {
            return Some(Received::User(u));
        }
        if self.count == 0 && !self.leg_delivered {
            self.leg_delivered = true;
            return Some(Received::UserLaneClosed);
        }
        None
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn anchor_schedules_match_lane_state_machine(
        ops in prop::collection::vec(
            prop_oneof![
                any::<u32>().prop_map(Op::Send),
                any::<u32>().prop_map(Op::AnchorSend),
                Just(Op::Upgrade),
                Just(Op::DropLastSender),
                Just(Op::Recv),
            ],
            0..=60,
        ),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (ctl, usr, mut rx) = channel::<u32, u32>(Config::new(64));
            let anchor = usr.anchor();
            let mut model = LaneModel::new();
            let mut usr = Some(usr); // owned, so DropLastSender can consume it

            for op in ops {
                match op {
                    Op::Send(n) => {
                        if model.count == 0 {
                            continue; // invalid op post-close — skip
                        }
                        usr.as_ref().unwrap().try_send(n).unwrap();
                        model.queue.push_back(n);
                    }
                    Op::AnchorSend(n) => match anchor.try_send(n) {
                        Ok(()) => {
                            prop_assert!(
                                model.anchor_ok(),
                                "anchor send succeeded in the {} state",
                                if model.count > 0 { "Open" } else { "Closed" }
                            );
                            model.queue.push_back(n);
                        }
                        Err(TrySendError::Closed(v)) => {
                            prop_assert_eq!(v, n, "closed anchor send must return the payload");
                            prop_assert!(
                                !model.anchor_ok(),
                                "anchor send failed while the lane is Open"
                            );
                        }
                        Err(TrySendError::Full(_)) => {
                            panic!("capacity 64 cannot fill with <= 60 schedule ops")
                        }
                    },
                    Op::Upgrade => {
                        let upgraded = anchor.upgrade();
                        prop_assert_eq!(
                            upgraded.is_some(),
                            model.anchor_ok(),
                            "upgrade must track the lane state"
                        );
                        drop(upgraded);
                    }
                    Op::DropLastSender => {
                        if model.count == 0 {
                            continue; // invalid op — already closed
                        }
                        drop(usr.take().unwrap());
                        model.drop_sender();
                    }
                    Op::Recv => {
                        // Skip a recv the model says would park: open empty
                        // lane, or closed with the marker already spent.
                        if model.queue.is_empty() && (model.count > 0 || model.leg_delivered) {
                            continue;
                        }
                        let want = model.next_stream();
                        let got = tokio::time::timeout(GUARD, rx.recv())
                            .await
                            .expect("recv stalled");
                        prop_assert_eq!(got, want, "stream diverged from the state machine");
                    }
                }
            }

            // Close both lanes and drain: the model must agree to the end.
            if let Some(usr) = usr.take() {
                drop(usr);
                model.drop_sender();
            }
            drop(ctl);
            loop {
                let want = model.next_stream();
                if let Some(x) = tokio::time::timeout(GUARD, rx.recv())
                    .await
                    .expect("recv stalled on the closed channel")
                {
                    prop_assert_eq!(
                        Some(x),
                        want,
                        "final drain diverged from the state machine"
                    );
                    if want.is_none() {
                        break;
                    }
                } else {
                    prop_assert!(
                        want.is_none(),
                        "premature None while the model still expects a stream item"
                    );
                    break;
                }
            }
            Ok(())
        })?;
    }
}
