#!/usr/bin/env bash
# Canonical benchmark entrypoint for the autoresearch loop.
#
# Workload: the fastpass property suite (.auto/measure.sh → cargo test -p fastpass).
# Primary metric: tests_passing (count of passing tests; compile break parses as 0).
# Deterministic: fixed test suite, no network, no time-of-day dependence.
set -uo pipefail

# cargo is not on PATH in the loop's non-interactive shell; fall back to the
# nix store rust 1.96.0 toolchain (matches rust-toolchain.toml).
if ! command -v cargo >/dev/null 2>&1; then
	for d in /nix/store/*rust-1.96.0/bin; do
		if [ -x "${d}/cargo" ]; then
			PATH="${d}:${PATH}"
			break
		fi
	done
fi
if ! command -v cargo >/dev/null 2>&1; then
	echo "harness error: cargo not found" >&2
	exit 1
fi
export PATH

out=$(bash .auto/measure.sh 2>&1) || {
	echo "harness error: measure.sh failed" >&2
	printf '%s\n' "$out" >&2
	exit 1
}

# Normalize "METRIC tests_passing=N unit=count" → "METRIC tests_passing=N".
metric=$(printf '%s\n' "$out" | grep -oE 'tests_passing=[0-9]+' | head -n1)
if [ -z "${metric}" ]; then
	echo "harness error: no tests_passing metric in measure.sh output" >&2
	printf '%s\n' "$out" >&2
	exit 1
fi

echo "METRIC ${metric}"
printf '%s\n' "$out" | grep -E '^info:' || true
exit 0
