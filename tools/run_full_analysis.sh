#!/usr/bin/env bash
# Run analysis on the full 4080-sat Walker-delta snapshot,
# sweeping d_th, with Dijkstra baseline + queuing.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
else
  PY=python3
fi

# Queuing: 1 Gbps ISL with 200 Mbps background load (rho = 0.2).
LINK=1000
LOAD=200
PAIRS=300

mkdir -p results/analysis/full

echo ""
echo "=================================================================="
echo "Full Walker-delta constellation (4080 sats + 250 gateways)"
echo "=================================================================="

for DTH in 3000 3500 4000 5000; do
  echo ""
  echo "--- d_th=${DTH} km ---"
  "${PY}" tools/analyze_mhr_reliability.py \
    --nodes results/snap_optA_nodes.csv \
    --edges results/snap_optA_edges.csv \
    --meta  results/snap_optA_meta.json \
    --d_th_km "${DTH}" --pairs "${PAIRS}" \
    --link_rate_mbps "${LINK}" --offered_load_mbps "${LOAD}" --packet_bytes 1500 \
    --summary_json "results/analysis/full/dth${DTH}_summary.json" \
    --summary_csv  "results/analysis/full/dth${DTH}_summary.csv"
done

echo ""
echo "=================================================================="
echo "Analysis complete. Results in: results/analysis/full/"
echo "=================================================================="
