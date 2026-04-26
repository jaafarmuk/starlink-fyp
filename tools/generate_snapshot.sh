#!/usr/bin/env bash
# Generate a network snapshot from live CelesTrak TLE data.
#
# By default this fetches fresh Starlink TLEs from CelesTrak at runtime
# (supplemental feed, then GP feed as fallback). If the network is unavailable,
# it automatically falls back to datasets/starlink.tle.
#
# Pass --no_live to skip the network fetch and always use the local file.
# Pass --utc, --n, --seed, --starlink_operational etc. as extra args.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

results_dir="${repo_root}/results"
mkdir -p "${results_dir}"

if [[ -x "${repo_root}/.venv/bin/python" ]]; then
  python_bin="${repo_root}/.venv/bin/python"
elif [[ -x "${HOME}/ns-3-dev/.venv/bin/python" ]]; then
  python_bin="${HOME}/ns-3-dev/.venv/bin/python"
else
  python_bin="python3"
fi

"${python_bin}" "${repo_root}/tools/tle_to_snapshot.py" \
  --tle "${repo_root}/datasets/starlink.tle" \
  --edges_out "${results_dir}/snapshot_edges.csv" \
  --nodes_out "${results_dir}/snapshot_nodes.csv" \
  --stats_out "${results_dir}/topology_stats.csv" \
  --meta_out  "${results_dir}/snapshot_meta.json" \
  "$@"
