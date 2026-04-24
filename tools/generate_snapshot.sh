#!/usr/bin/env bash
# Generate a network snapshot from the checked-in TLE dataset.
#
# The Python generator writes outputs atomically; this wrapper just makes
# sure the target directory exists before the Python call (review item 23)
# and forwards any extra args verbatim so users can pass --utc, --n, etc.
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
