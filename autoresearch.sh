#!/usr/bin/env bash
# Canonical benchmark entrypoint for the autoresearch loop.
#
# This session's goal is a CORRECTNESS contract: a user producer blocked on a
# full ring when the consumer is dropped must resolve Err(UserClosed(payload))
# with its exact payload, never Ok(()) with the payload discarded (the former
# "pinned teardown seam"). The harness measures that contract first, then the
# standing performance score.
#
# Primary metric:   contract_green — 1 iff the frozen teardown oracle
#                   (crates/fastpass/tests/teardown_oracle.rs +
#                   teardown_alloc.rs) AND the full correctness gate
#                   (.auto/checks.sh: frozen surfaces, rustfmt, workspace
#                   tests, strict clippy, bounded Loom) pass; 0 otherwise.
#                   MAXIMIZE (target 1).
#
# Secondary metric: score — the composite from .auto/measure.sh (best-of-3,
#                   normalized-min vs .auto/PERF_BASELINE with per-metric
#                   floors). Regression guard only: the fix touches no hot
#                   path, so the score must hold its floors, not improve.
#                   Plus the perf surface lines measure.sh echoes.
#
# Determinism: fixed test workloads and fixed perf workload, no network, no
# time-of-day dependence; the oracle uses barriers/yields and protocol state,
# never sleeps.
#
# A harness malfunction exits nonzero; a red contract or a perf build failure
# exits 0 with the metric reporting the failure (contract_green=0 / score=0),
# so the loop auto-reverts.
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
for d in /nix/store/*libiconv-1.*/lib; do
	if [ -d "${d}" ]; then
		LIBRARY_PATH="${d}${LIBRARY_PATH:+:${LIBRARY_PATH}}"; export LIBRARY_PATH
		break
	fi
done

# 1. Correctness contract: the frozen teardown oracle plus the full gate.
green=0
if [ -f crates/fastpass/tests/teardown_oracle.rs ]; then
	if cargo test -q -p fastpass --test teardown_oracle --test teardown_alloc >/dev/null 2>&1 \
		&& bash .auto/checks.sh >/dev/null 2>&1; then
		green=1
	fi
fi
echo "METRIC contract_green=${green}"

# 2. Performance regression guard (same workload as the standing harness).
out=$(bash .auto/measure.sh 2>&1)

score=$(printf '%s\n' "$out" | grep -oE '^METRIC score=[0-9.]+' | head -n1 | cut -d= -f2)
if [ -z "${score}" ]; then
	echo "harness error: no score metric in measure.sh output" >&2
	printf '%s\n' "$out" >&2
	exit 1
fi

echo "METRIC score=${score}"

# Perf-surface secondaries (measure.sh echoes them verbatim).
while IFS=: read -r src dst; do
	val=$(printf '%s\n' "$out" | sed -n "s/^${src}=//p" | head -n1)
	[ -n "${val}" ] && echo "METRIC ${dst}=${val}"
done <<'EOF'
DIRECT_THROUGHPUT_OPS:direct_throughput_ops
ANCHOR_THROUGHPUT_OPS:anchor_throughput_ops
ANCHOR_OVERHEAD_NS:anchor_overhead_ns
CONTROL_LATENCY_NS:control_latency_ns
DRAIN_THROUGHPUT_OPS:drain_throughput_ops
EOF

exit 0
