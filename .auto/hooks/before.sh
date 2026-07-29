#!/usr/bin/env bash
# Fires before each iteration; stdout is delivered to the agent as a steer.
# Keep it cheap — just reinforce the guardrails.
echo "STEER: edit only Consumer::recv in crates/fastpass/src/lib.rs."
echo "STEER: do NOT touch crates/fastpass-testkit or crates/fastpass-reference (checks.sh will revert you)."
echo "STEER: remaining targets — P1 priority, overtake, anti-starvation aging."
