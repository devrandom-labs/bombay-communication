#!/usr/bin/env bash
# Metric for the autoresearch loop: number of passing tests in the target crate.
# Maximize toward all-green. A compile break parses as 0 passing, so a bad
# experiment scores 0 and gets auto-reverted.
#
# NOTE: run UNSANDBOXED. cargo test hangs under a sandboxed shell (see the
# kimi-delegate skill); the autoresearch loop launched from a real terminal is
# fine. Every test carries an internal 5s timeout, so a deadlocking policy fails
# rather than hanging the run.
set -uo pipefail

out=$(cargo test -p fastpass --no-fail-fast 2>&1)
passed=$(printf '%s\n' "$out" | grep -oE '[0-9]+ passed' | awk '{s += $1} END {print s + 0}')
failed=$(printf '%s\n' "$out" | grep -oE '[0-9]+ failed' | awk '{s += $1} END {print s + 0}')

echo "METRIC tests_passing=${passed} unit=count"
echo "info: failed=${failed}"
