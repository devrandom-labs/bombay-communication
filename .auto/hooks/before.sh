#!/usr/bin/env bash
# Fires before each iteration; stdout is delivered to the agent as a steer.
# Keep it cheap — just reinforce the guardrails.
echo "STEER: maximize SCORE = throughput / control-latency (.auto/measure.sh); edit crates/fastpass/src/** and its Cargo.toml freely."
echo "STEER: do NOT touch fastpass-testkit / fastpass-reference / fastpass-perf / fastpass/tests (checks.sh reverts you)."
echo "STEER: gate = .auto/checks.sh (conformance P1-P7 + proptest + zero-alloc); anti-starvation aging is contractual."
