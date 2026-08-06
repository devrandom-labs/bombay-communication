#!/usr/bin/env bash
# Actorpass-shaped performance metric. The perf binary owns fixed workloads;
# this wrapper owns warming, repetition, parsing, normalization, and floors.
set -uo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  for d in /nix/store/*rust-1.96.0/bin; do
    if [ -x "${d}/cargo" ]; then PATH="${d}:${PATH}"; export PATH; break; fi
  done
fi
for d in /nix/store/*libiconv-1.*/lib; do
  if [ -d "${d}" ]; then
    LIBRARY_PATH="${d}${LIBRARY_PATH:+:${LIBRARY_PATH}}"; export LIBRARY_PATH; break
  fi
done

export RUSTFLAGS="${RUSTFLAGS:-} -C target-cpu=native"
if ! build_out=$(cargo build -q -p fastpass-perf --release 2>&1); then
  echo "METRIC score=0 unit=normalized_min"
  echo "info: perf harness failed to build"
  printf '%s\n' "${build_out}" | tail -12
  exit 0
fi

bin=./target/release/fastpass-perf
"${bin}" >/dev/null 2>&1
best_score=0
best_out=
for _ in 1 2 3; do
  out=$("${bin}" 2>&1)
  direct=$(printf '%s\n' "${out}" | sed -n 's/^DIRECT_THROUGHPUT_OPS=//p' | head -1)
  anchor=$(printf '%s\n' "${out}" | sed -n 's/^ANCHOR_THROUGHPUT_OPS=//p' | head -1)
  latency=$(printf '%s\n' "${out}" | sed -n 's/^CONTROL_LATENCY_NS=//p' | head -1)
  drain=$(printf '%s\n' "${out}" | sed -n 's/^DRAIN_THROUGHPUT_OPS=//p' | head -1)
  if [ -z "${direct}" ] || [ -z "${anchor}" ] || [ -z "${latency}" ] || [ -z "${drain}" ]; then
    continue
  fi

  if [ -f .auto/PERF_BASELINE ]; then
    # shellcheck disable=SC1091
    . .auto/PERF_BASELINE
    score=$(awk -v d="${direct}" -v a="${anchor}" -v l="${latency}" -v r="${drain}" \
      -v bd="${BASE_DIRECT}" -v ba="${BASE_ANCHOR}" -v bl="${BASE_CONTROL_LATENCY}" -v br="${BASE_DRAIN}" '
      BEGIN {
        rd=d/bd; ra=a/ba; rl=bl/l; rr=r/br;
        min=rd; if (ra<min) min=ra; if (rl<min) min=rl; if (rr<min) min=rr;
        if (rd<0.95 || ra<0.95 || rr<0.95 || l>bl*1.10) print 0; else print min;
      }')
  else
    score=$(awk -v d="${direct}" -v a="${anchor}" -v l="${latency}" -v r="${drain}" \
      'BEGIN { min=d; if (a<min) min=a; if (r<min) min=r; print min/l }')
  fi

  # Retain the first complete sample even when it scores zero, so a genuine
  # floor violation is reported as such rather than mislabeled as incomplete.
  if [ -z "${best_out}" ] || awk -v a="${score}" -v b="${best_score}" 'BEGIN { exit !(a>b) }'; then
    best_score=${score}; best_out=${out}
  fi
done

if [ -z "${best_out}" ]; then
  echo "METRIC score=0 unit=normalized_min"
  echo "info: UserAnchor perf contract is pending or harness output is incomplete"
  exit 0
fi

echo "METRIC score=${best_score} unit=normalized_min"
printf '%s\n' "${best_out}" | rg '^(DIRECT_THROUGHPUT_OPS|ANCHOR_THROUGHPUT_OPS|ANCHOR_OVERHEAD_NS|CONTROL_LATENCY_NS|DRAIN_THROUGHPUT_OPS)='
if [ "${best_score}" = 0 ]; then
  echo "info: candidate violated a per-metric performance floor"
fi
