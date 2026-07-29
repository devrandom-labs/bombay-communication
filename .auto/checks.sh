#!/usr/bin/env bash
# Backpressure gate: runs after a passing measurement. A non-zero exit blocks
# `keep`, so the experiment is reverted even if the metric improved.
#
# Two integrity guards:
#   1. The oracle (testkit) and gold (reference) crates must be byte-identical to
#      the baseline commit — no weakening the tests to "pass".
#   2. The reference suite must stay green.
set -uo pipefail

base=$(cat .auto/BASELINE 2>/dev/null || true)
if [ -n "${base}" ]; then
    if ! git diff --quiet "${base}" -- crates/fastpass-testkit crates/fastpass-reference; then
        echo "CHECK FAIL: protected oracle/reference files were modified"
        exit 1
    fi
fi

if ! cargo test -p fastpass-reference --no-fail-fast >/dev/null 2>&1; then
    echo "CHECK FAIL: reference suite is not green"
    exit 1
fi

echo "CHECK OK"
