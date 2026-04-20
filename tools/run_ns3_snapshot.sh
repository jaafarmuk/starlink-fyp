#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

ns3_dir="${NS3_DIR:-${HOME}/ns-3-dev}"
if [[ "${1:-}" == "--ns3-dir" ]]; then
  ns3_dir="$2"
  shift 2
fi

if [[ ! -x "${ns3_dir}/ns3" ]]; then
  echo "ns-3 launcher not found at ${ns3_dir}/ns3" >&2
  echo "Set NS3_DIR or pass --ns3-dir /path/to/ns-3-dev" >&2
  exit 1
fi

mkdir -p "${repo_root}/results" "${ns3_dir}/scratch" "${ns3_dir}/results"

cp "${repo_root}/src/starlink-snapshot.cc" "${ns3_dir}/scratch/starlink-snapshot.cc"

rm -f \
  "${repo_root}/results/flowmon.xml" \
  "${repo_root}/results/per_flow_metrics.csv" \
  "${repo_root}/results/starlink-animation.xml"

rm -f \
  "${ns3_dir}/results/flowmon.xml" \
  "${ns3_dir}/results/per_flow_metrics.csv" \
  "${ns3_dir}/results/snapshot_edges.csv" \
  "${ns3_dir}/results/snapshot_nodes.csv" \
  "${ns3_dir}/results/starlink-animation.xml"

for input_file in snapshot_edges.csv snapshot_nodes.csv; do
  if [[ -f "${repo_root}/results/${input_file}" ]]; then
    cp "${repo_root}/results/${input_file}" "${ns3_dir}/results/${input_file}"
  fi
done

scenario_args=("$@")
has_edges_arg=0
for arg in "${scenario_args[@]}"; do
  if [[ "${arg}" == --edges=* || "${arg}" == "--edges" ]]; then
    has_edges_arg=1
    break
  fi
done

if [[ ${has_edges_arg} -eq 0 ]]; then
  scenario_args=(--edges=results/snapshot_edges.csv "${scenario_args[@]}")
fi

printf -v run_spec '%q ' starlink-snapshot "${scenario_args[@]}"

(
  cd "${ns3_dir}"
  ./ns3 run "${run_spec% }"
)

for output_file in \
  flowmon.xml \
  per_flow_metrics.csv \
  snapshot_edges.csv \
  snapshot_nodes.csv \
  starlink-animation.xml; do
  if [[ -f "${ns3_dir}/results/${output_file}" ]]; then
    cp "${ns3_dir}/results/${output_file}" "${repo_root}/results/${output_file}"
  fi
done

echo "Synced simulation outputs back to ${repo_root}/results"
