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

if [ "${FASTPASS_DEEP:-0}" = 1 ]; then
  LOOM_MAX_PREEMPTIONS=7 RUSTFLAGS="--cfg loom" \
    cargo test -p fastpass --test loom --release || {
      echo "CHECK FAIL: deep Loom model"; exit 1;
    }
  if command -v cargo-miri >/dev/null 2>&1 || rustup component list --installed | rg -q '^miri'; then
    cargo miri test -p fastpass || { echo "CHECK FAIL: Miri"; exit 1; }
  else
    echo "CHECK FAIL: FASTPASS_DEEP=1 requires Miri"
    exit 1
  fi
fi

echo "CHECK OK"
