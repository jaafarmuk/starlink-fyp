#!/usr/bin/env bash
# Serve the interactive snapshot visualizer on http://localhost:8000/
# Needs: any generated results/snapshot_nodes.csv and snapshot_edges.csv.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
port="${1:-8000}"

if [[ ! -f "${repo_root}/results/snapshot_nodes.csv" ]]; then
  echo "No snapshot CSVs in ${repo_root}/results/." >&2
  echo "Run tools/generate_snapshot.sh first." >&2
  exit 1
fi

echo "Serving ${repo_root} at http://localhost:${port}/"
echo "Open http://localhost:${port}/tools/visualizer/ in your browser."
exec python -m http.server "${port}" --bind 127.0.0.1 --directory "${repo_root}"
