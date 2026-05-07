#!/usr/bin/env bash
# Run analysis on both 4080-sat (full Walker-delta) and 600-sat (subsample)
# snapshots, sweeping d_th, with Dijkstra baseline + queuing.
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

# Variants: name, nodes_path, edges_path, meta_path, output_subdir
declare -a VARIANTS=(
  "full results/snap_optA_nodes.csv      results/snap_optA_edges.csv      results/snap_optA_meta.json      results/analysis/full"
  "n600 results/snap_optA_n600_nodes.csv results/snap_optA_n600_edges.csv results/snap_optA_n600_meta.json results/analysis/n600"
)

for V in "${VARIANTS[@]}"; do
  read -r name nodes edges meta outdir <<< "${V}"
  mkdir -p "${outdir}"
  echo ""
  echo "=================================================================="
  echo "Variant: ${name}   nodes=${nodes}"
  echo "=================================================================="

  for DTH in 3000 3500 4000 5000; do
    echo ""
    echo "--- d_th=${DTH} km ---"
    "${PY}" tools/analyze_mhr_reliability.py \
      --nodes "${nodes}" --edges "${edges}" --meta "${meta}" \
      --d_th_km "${DTH}" --pairs "${PAIRS}" \
      --link_rate_mbps "${LINK}" --offered_load_mbps "${LOAD}" --packet_bytes 1500 \
      --summary_json "${outdir}/dth${DTH}_summary.json" \
      --summary_csv  "${outdir}/dth${DTH}_summary.csv"
  done
done

echo ""
echo "=================================================================="
echo "All analyses complete."
echo "Results in: results/analysis/full/  and  results/analysis/n600/"
echo "=================================================================="
