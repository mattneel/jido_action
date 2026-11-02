#!/usr/bin/env bash
# Fresh VM per case. Unique directory per invocation; no artifact overwrite.
set -u
cd "$(dirname "$0")/.."
run="../../results/integration/frame-bench-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$run" || exit 1
echo "ARTIFACT DIR: $run"

run_case() {
  mode="$1"; backend="$2"; depth="$3"; rep="$4"
  out="$run/${mode}_${backend}_d${depth}_rep${rep}.json"
  echo "CASE $mode $backend depth=$depth rep=$rep"
  MIX_ENV=dev timeout 90 mix run --no-start bench/frame_backend_bench.exs -- \
    --mode "$mode" --state "$backend" --depth "$depth" --rep "$rep" \
    --entries 2000 --out "$out"
  status=$?
  echo "CASE EXIT $status $out"
  return $status
}

fail=0
for backend in original graph_ets; do
  for depth in 5 8; do
    for rep in 1 2; do
      run_case full "$backend" "$depth" "$rep" || fail=1
    done
  done
  run_case full "$backend" 20 1 || fail=1
done

for backend in graph_ets owner_map; do
  for depth in 5 8; do
    for rep in 1 2; do
      run_case continuation "$backend" "$depth" "$rep" || fail=1
    done
  done
  run_case continuation "$backend" 20 1 || fail=1
done

echo "BENCH DIR $run"
echo "FAILURES $fail"
exit "$fail"
