#!/usr/bin/env bash
# Generate the canonical snap_optA network snapshot from local TLE data.
#
# Defaults reproduce the snap_optA dataset used in all reported results:
#   - full constellation, top2_separated shell selection (2 shells, ~56 km gap)
#   - 250 real ground stations (operational_safe CSV)
#   - 10 epochs at 9-minute steps (81 minutes total)
#   - Output prefix: results/snap_optA
#
# Pass --no_live (already the default here) to use the local TLE file.
# Any extra args are forwarded to tle_to_snapshot.py and override defaults.
#
# Examples:
#   bash tools/generate_snapshot.sh                        # reproduce snap_optA
#   bash tools/generate_snapshot.sh --n 400 --epoch_steps 1  # quick test run
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

results_dir="${repo_root}/results"
mkdir -p "${results_dir}"

# Locate Python — prefer repo .venv, then Homebrew, then system python3
if [[ -x "${repo_root}/.venv/bin/python" ]]; then
  python_bin="${repo_root}/.venv/bin/python"
elif [[ -x "${HOME}/ns-3-dev/.venv/bin/python" ]]; then
  python_bin="${HOME}/ns-3-dev/.venv/bin/python"
elif [[ -x "/opt/homebrew/bin/python3.11" ]]; then
  python_bin="/opt/homebrew/bin/python3.11"
elif [[ -x "/opt/homebrew/bin/python3" ]]; then
  python_bin="/opt/homebrew/bin/python3"
else
  python_bin="python3"
fi

echo "Using Python: ${python_bin}"
echo "Output prefix: ${results_dir}/snap_optA"
echo ""

"${python_bin}" "${repo_root}/tools/tle_to_snapshot.py" \
  --tle             "${repo_root}/datasets/starlink.tle" \
  --no_live \
  --n               9999 \
  --seed            42 \
  --shell_select    top2_separated \
  --min_alt_sep_km  50 \
  --starlink_operational \
  --gateways_csv    "${repo_root}/datasets/starlink_ground_stations_hf_operational_safe.csv" \
  --epoch_steps     10 \
  --multi_epoch_seconds 540 \
  --nodes_out       "${results_dir}/snap_optA_nodes.csv" \
  --edges_out       "${results_dir}/snap_optA_edges.csv" \
  --meta_out        "${results_dir}/snap_optA_meta.json" \
  --stats_out       "${results_dir}/snap_optA_stats.csv" \
  "$@"
