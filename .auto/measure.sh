#!/usr/bin/env bash
# METRIC for the autoresearch loop: the composite perf SCORE (throughput divided
# by control-latency-under-backlog). MAXIMIZE. Higher throughput and/or lower
# control latency both raise it.
#
# Correctness and zero-allocation are NOT measured here — they are hard GATES in
# .auto/checks.sh. This script only asks "how fast is the current design?". A
# compile break makes the perf bin fail → SCORE parses as 0 → the experiment is
# auto-reverted.
#
# Run UNSANDBOXED (cargo hangs under a sandboxed shell).
set -uo pipefail

# cargo may be absent from a non-interactive loop shell; fall back to the pinned
# nix-store rust 1.96.0 (matches rust-toolchain.toml).
if ! command -v cargo >/dev/null 2>&1; then
	for d in /nix/store/*rust-1.96.0/bin; do
		if [ -x "${d}/cargo" ]; then
			PATH="${d}:${PATH}"
			export PATH
			break
		fi
	done
fi

out=$(cargo run -q -p fastpass-perf --release 2>&1)
score=$(printf '%s\n' "${out}" | grep -oE 'SCORE=[0-9.]+' | head -1 | cut -d= -f2)
thr=$(printf '%s\n' "${out}" | grep -oE 'THROUGHPUT_OPS=[0-9.]+' | head -1 | cut -d= -f2)
lat=$(printf '%s\n' "${out}" | grep -oE 'CONTROL_LATENCY_NS=[0-9.]+' | head -1 | cut -d= -f2)

if [ -z "${score}" ]; then
	echo "METRIC score=0 unit=composite"
	echo "info: perf bin produced no SCORE (compile/runtime failure) — reverting"
	printf '%s\n' "${out}" | tail -8
	exit 0
fi

echo "METRIC score=${score} unit=composite"
echo "info: throughput_ops=${thr} control_latency_ns=${lat}"
