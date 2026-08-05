#!/usr/bin/env bash
# Canonical benchmark entrypoint for the autoresearch loop.
#
# Workload: the fastpass perf harness (.auto/measure.sh →
# `cargo run -q -p fastpass-perf --release`), which merges the control and user
# lanes of the current design and reports throughput plus control-latency-under-
# backlog.
#
# Primary metric:   score — the composite from .auto/measure.sh (best-of-3,
#                   normalized-min vs .auto/PERF_BASELINE with per-metric floors
#                   once the UserAnchor contract lands; 0 while the contract is
#                   pending, because the perf harness does not emit the
#                   contract workload lines yet). MAXIMIZE.
#
# Secondary metrics: direct_throughput_ops, anchor_throughput_ops,
#                   anchor_overhead_ns, control_latency_ns, drain_throughput_ops
#                   (contract surface, from measure.sh's echoed perf lines);
#                   throughput_ops, control_latency_ns (legacy surface, reported
#                   only while the contract is pending — measure.sh cannot parse
#                   the legacy line names).
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

echo "METRIC score=${score}"

# Contract-surface secondaries (present once fastpass-perf emits the
# UserAnchor workload lines; measure.sh echoes them verbatim).
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

# Legacy-surface fallback: while the contract is pending, measure.sh has no
# contract lines to echo, so report the perf binary's raw legacy lines
# directly (same fixed workload measure.sh just ran).
if ! printf '%s\n' "$out" | grep -q '^DIRECT_THROUGHPUT_OPS='; then
	legacy=$(./target/release/fastpass-perf 2>/dev/null || true)
	while IFS=: read -r src dst; do
		val=$(printf '%s\n' "${legacy}" | sed -n "s/^${src}=//p" | head -n1)
		[ -n "${val}" ] && echo "METRIC ${dst}=${val}"
	done <<'EOF'
THROUGHPUT_OPS:throughput_ops
CONTROL_LATENCY_NS:control_latency_ns
EOF
fi

exit 0
