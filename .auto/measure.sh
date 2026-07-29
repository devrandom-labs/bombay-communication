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

# The nix-store rust links via the system clang, which cannot find libiconv
# outside a nix shell; point it at the nix store copy.
for d in /nix/store/*libiconv-1.*/lib; do
	if [ -d "${d}" ]; then
		LIBRARY_PATH="${d}${LIBRARY_PATH:+:${LIBRARY_PATH}}"
		export LIBRARY_PATH
		break
	fi
done

# Optimize for the host CPU; applies uniformly to every iteration.
export RUSTFLAGS="${RUSTFLAGS:-} -C target-cpu=native"

# Build, then exec the binary directly: `cargo run` measurably depresses and
# destabilizes the workload (~2x slower, higher variance) vs a direct exec.
if ! out_build=$(cargo build -q -p fastpass-perf --release 2>&1); then
	echo "METRIC score=0 unit=composite"
	echo "info: perf bin failed to build — reverting"
	printf '%s\n' "${out_build}" | tail -8
	exit 0
fi
# The first exec after a cargo build is consistently ~2x slower (cold pages
# from cargo's target-dir scan); discard it. Then take the BEST of 3 runs:
# machine-state drift depresses individual runs by ~10%, and the best-of-3
# estimator keeps keep/revert decisions out of the noise. Same workload every
# run; only the estimator is more robust.
./target/release/fastpass-perf >/dev/null 2>&1
best_score=0
for _ in 1 2 3; do
	run_out=$(./target/release/fastpass-perf 2>&1)
	run_score=$(printf '%s\n' "${run_out}" | grep -oE 'SCORE=[0-9.]+' | head -1 | cut -d= -f2)
	if [ -n "${run_score}" ] && awk -v a="${run_score}" -v b="${best_score}" 'BEGIN{exit !(a>b)}'; then
		best_score=${run_score}
		out=${run_out}
	fi
done
score=${best_score}
thr=$(printf '%s\n' "${out:-}" | grep -oE 'THROUGHPUT_OPS=[0-9.]+' | head -1 | cut -d= -f2)
lat=$(printf '%s\n' "${out:-}" | grep -oE 'CONTROL_LATENCY_NS=[0-9.]+' | head -1 | cut -d= -f2)

if [ -z "${score}" ] || [ "${score}" = "0" ]; then
	echo "METRIC score=0 unit=composite"
	echo "info: perf bin produced no SCORE (compile/runtime failure) — reverting"
	printf '%s\n' "${out:-}" | tail -8
	exit 0
fi

echo "METRIC score=${score} unit=composite"
echo "info: throughput_ops=${thr} control_latency_ns=${lat}"
