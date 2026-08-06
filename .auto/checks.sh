#!/usr/bin/env bash
# Correctness gate for every autoresearch experiment. Performance is evaluated
# separately by measure.sh; a faster candidate never bypasses this script.
set -uo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  for d in /nix/store/*rust-1.96.0/bin; do
    if [ -x "${d}/cargo" ]; then PATH="${d}:${PATH}"; export PATH; break; fi
  done
fi
for d in /nix/store/*libiconv-1.*/lib; do
  if [ -d "${d}" ]; then
    LIBRARY_PATH="${d}${LIBRARY_PATH:+:${LIBRARY_PATH}}"; export LIBRARY_PATH; break
  fi
done

base=$(cat .auto/BASELINE 2>/dev/null || true)
FROZEN=(
  .auto/checks.sh
  .auto/prompt.md
  .auto/measure.sh
  .plans/card-3-user-anchor-actorpass.md
  crates/fastpass-reference
  crates/fastpass-testkit
  crates/fastpass-perf
  crates/fastpass/tests/property_suite.rs
  crates/fastpass/tests/edge_cases.rs
  crates/fastpass/tests/lane_lifecycle.rs
  crates/fastpass/tests/proptest_interleavings.rs
  crates/fastpass/tests/alloc.rs
  crates/fastpass/tests/leak.rs
)
if [ -n "${base}" ] && ! git diff --quiet "${base}" -- "${FROZEN[@]}"; then
  echo "CHECK FAIL: frozen contract/oracle/measurement surface changed"
  echo "Only the one-time contract landing may thaw these files; commit it and re-pin BASELINE before optimization."
  exit 1
fi

cargo fmt --all -- --check || { echo "CHECK FAIL: rustfmt"; exit 1; }
cargo test --workspace --tests --no-fail-fast || { echo "CHECK FAIL: workspace tests"; exit 1; }
cargo clippy --workspace --all-targets -- -D warnings || { echo "CHECK FAIL: strict clippy"; exit 1; }

LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" \
  cargo test -p fastpass --test loom --release || {
    echo "CHECK FAIL: bounded Loom model"; exit 1;
  }

if rg -q 'pub struct UserAnchor' crates/fastpass/src/lib.rs; then
  required=(
    anchor_clone_does_not_hold_lane_open
    anchor_send_while_live
    anchor_fails_after_last_sender
    anchor_racing_last_drop_is_linearizable
    cancelled_anchor_send_releases_liveness
    anchor_try_send_steady_state_is_zero_alloc
  )
  for name in "${required[@]}"; do
    rg -q "${name}" crates/fastpass/tests crates/fastpass-testkit || {
      echo "CHECK FAIL: missing UserAnchor oracle ${name}"; exit 1;
    }
  done
else
  echo "CHECK INFO: UserAnchor contract has not landed yet"
fi

# Frozen blocked-producer teardown contract: a producer blocked on a full
# user ring when the consumer drops must resolve Err(UserClosed(payload))
# with its exact payload — the names below pin the oracle so no experiment
# can quietly delete it.
teardown_required=(
  blocked_send_on_full_ring_recovers_exact_payload_on_consumer_drop
  enqueued_before_teardown_stays_ok_and_teardown_owns_the_payload
  send_starting_after_teardown_returns_its_payload
  every_blocked_producer_recovers_its_own_payload
  blocked_anchor_send_recovers_payload_on_consumer_drop
  cancelled_blocked_send_drops_payload_exactly_once
  last_sender_closure_and_user_lane_closed_marker_unchanged
  drain_with_blocked_producer_preserves_ordering_and_releases_payload
  drain_teardown_race_releases_blocked_sender_with_payload
  shutdown_returns_allocations_to_baseline
  teardown_returns_unlinearized_payloads
  teardown_releases_multiple_producers_with_their_payloads
)
for name in "${teardown_required[@]}"; do
  rg -q "${name}" crates/fastpass/tests || {
    echo "CHECK FAIL: missing blocked-producer teardown oracle ${name}"; exit 1;
  }
done

if [ "${FASTPASS_DEEP:-0}" = 1 ]; then
  LOOM_MAX_PREEMPTIONS=7 RUSTFLAGS="--cfg loom" \
    cargo test -p fastpass --test loom --release || {
      echo "CHECK FAIL: deep Loom model"; exit 1;
    }
  if command -v cargo-miri >/dev/null 2>&1 || rustup component list --installed | rg -q '^miri'; then
    # Proptest's persistence needs filesystem isolation disabled and its
    # randomized state spaces are prohibitively slow under interpretation.
    # They remain mandatory in the native workspace gate above; Miri covers
    # the deterministic lifecycle, property, allocation, and leak suites.
    cargo miri test -p fastpass -- \
      --skip anchor_schedules_match_lane_state_machine \
      --skip concurrent_producers_no_loss_fifo \
      --skip preloaded_backlog_matches_policy_model || {
      echo "CHECK FAIL: Miri"; exit 1;
    }
  else
    echo "CHECK FAIL: FASTPASS_DEEP=1 requires Miri"
    exit 1
  fi
fi

echo "CHECK OK"
