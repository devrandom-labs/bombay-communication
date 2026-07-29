#!/usr/bin/env bash
# Canonical benchmark entrypoint for the autoresearch loop.
#
# Workload: the fastpass perf harness (.auto/measure.sh →
# `cargo run -q -p fastpass-perf --release`), which merges the control and user
# lanes of the current design and reports throughput plus control-latency-under-
# backlog.
#
# Primary metric:   score            = throughput / control_latency_ns  (MAXIMIZE)
# Secondary metrics: throughput_ops, control_latency_ns (parsed from the perf bin).
#
# Determinism: fixed perf workload, no network, no time-of-day dependence.
# A compile/runtime failure makes measure.sh emit METRIC score=0, which passes
# through unchanged so the loop auto-reverts.
#
# Correctness is NOT measured here; .auto/checks.sh is the hard gate
# (conformance + zero-alloc + frozen files).
#
# Run UNSANDBOXED (cargo hangs under a sandboxed shell).
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

out=$(bash .auto/measure.sh 2>&1)

score=$(printf '%s\n' "$out" | grep -oE '^METRIC score=[0-9.]+' | head -n1 | cut -d= -f2)
if [ -z "${score}" ]; then
	echo "harness error: no score metric in measure.sh output" >&2
	printf '%s\n' "$out" >&2
	exit 1
fi

thr=$(printf '%s\n' "$out" | grep -oE 'throughput_ops=[0-9.]+' | head -n1 | cut -d= -f2)
lat=$(printf '%s\n' "$out" | grep -oE 'control_latency_ns=[0-9.]+' | head -n1 | cut -d= -f2)

echo "METRIC score=${score}"
[ -n "${thr}" ] && echo "METRIC throughput_ops=${thr}"
[ -n "${lat}" ] && echo "METRIC control_latency_ns=${lat}"
exit 0
