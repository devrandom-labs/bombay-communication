#!/usr/bin/env bash
# GATE for the autoresearch loop: runs after a measured experiment. A non-zero
# exit blocks `keep`, so a faster-but-broken (or cheating) design is REVERTED
# even when its SCORE improved. This is what lets Kimi optimize aggressively
# without regressing correctness.
#
# Three gates:
#   1. Frozen surfaces unchanged vs the baseline commit — the oracle
#      (testkit), the reference, the perf harness, and the conformance TEST
#      files (which also pin the public API: the plug seam into bombay). Kimi
#      may rewrite crates/fastpass/src/** and its Cargo.toml (new deps welcome),
#      but not the things that define "correct" or "fast".
#   2. The full conformance suite is green (P1–P7 + edge + proptest).
#   3. The zero-alloc steady-state guard is green (inside that suite).
set -uo pipefail

if ! command -v cargo >/dev/null 2>&1; then
	for d in /nix/store/*rust-1.96.0/bin; do
		if [ -x "${d}/cargo" ]; then
			PATH="${d}:${PATH}"
			export PATH
			break
		fi
	done
fi

# The nix-store rust links via the system clang, which cannot find libiconv
# outside a nix shell; point it at the nix store copy.
for d in /nix/store/*libiconv-1.*/lib; do
	if [ -d "${d}" ]; then
		LIBRARY_PATH="${d}${LIBRARY_PATH:+:${LIBRARY_PATH}}"
		export LIBRARY_PATH
		break
	fi
done

base=$(cat .auto/BASELINE 2>/dev/null || true)
FROZEN=(
	crates/fastpass-testkit
	crates/fastpass-reference
	crates/fastpass-perf
	crates/fastpass/tests
)
if [ -n "${base}" ]; then
	if ! git diff --quiet "${base}" -- "${FROZEN[@]}"; then
		echo "CHECK FAIL: a frozen surface (oracle / reference / perf harness / conformance tests) was modified"
		exit 1
	fi
fi

# Conformance + zero-alloc must stay green. --tests skips the ADR benches.
if ! cargo test -p fastpass --tests --no-fail-fast >/dev/null 2>&1; then
	echo "CHECK FAIL: conformance suite (or zero-alloc guard) is not green"
	exit 1
fi

echo "CHECK OK"
