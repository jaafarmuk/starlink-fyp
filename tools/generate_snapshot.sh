#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

if [[ -x "${repo_root}/.venv/bin/python" ]]; then
  python_bin="${repo_root}/.venv/bin/python"
elif [[ -x "${HOME}/ns-3-dev/.venv/bin/python" ]]; then
  python_bin="${HOME}/ns-3-dev/.venv/bin/python"
else
  python_bin="python3"
fi

"${python_bin}" "${repo_root}/tools/tle_to_snapshot.py" \
  --tle "${repo_root}/datasets/starlink.tle" \
  --edges_out "${repo_root}/results/snapshot_edges.csv" \
  --nodes_out "${repo_root}/results/snapshot_nodes.csv" \
  "$@"
