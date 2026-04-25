#!/usr/bin/env bash
# Run the ns-3 starlink-snapshot scenario against the CURRENT snapshot.
#
# This wrapper has a few safety properties:
#  * It does NOT delete prior repo outputs up-front. New outputs are built in
#    a temp directory and swapped into results/ only if the run succeeds
#    (review item 21).
#  * If the user passes --edges=..., --nodes=..., --meta=..., those are used
#    as-is. Otherwise the wrapper injects all three consistently so the
#    scenario never sees a custom edges file next to stale node metadata
#    (review item 22).
#  * Snapshot metadata schema is verified by the scenario itself; if it does
#    not match, the run fails fast.
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

<<<<<<< HEAD
snapshot_edges="${repo_root}/results/snapshot_edges.csv"
snapshot_nodes="${repo_root}/results/snapshot_nodes.csv"
snapshot_meta="${repo_root}/results/snapshot_meta.json"

for f in "${snapshot_edges}" "${snapshot_nodes}"; do
  if [[ ! -f "${f}" ]]; then
    echo "Missing required snapshot input: ${f}" >&2
    echo "Run tools/generate_snapshot.sh first." >&2
    exit 2
=======
rm -f \
  "${repo_root}/results/flowmon.xml" \
  "${repo_root}/results/per_flow_metrics.csv"

rm -f \
  "${ns3_dir}/results/flowmon.xml" \
  "${ns3_dir}/results/per_flow_metrics.csv" \
  "${ns3_dir}/results/snapshot_edges.csv" \
  "${ns3_dir}/results/snapshot_nodes.csv"

for input_file in snapshot_edges.csv snapshot_nodes.csv; do
  if [[ -f "${repo_root}/results/${input_file}" ]]; then
    cp "${repo_root}/results/${input_file}" "${ns3_dir}/results/${input_file}"
>>>>>>> 5381ee7 (.)
  fi
done

# Stage inputs to ns-3 working area.
cp "${snapshot_edges}" "${ns3_dir}/results/snapshot_edges.csv"
cp "${snapshot_nodes}" "${ns3_dir}/results/snapshot_nodes.csv"
if [[ -f "${snapshot_meta}" ]]; then
  cp "${snapshot_meta}" "${ns3_dir}/results/snapshot_meta.json"
fi

# Inject CLI args consistently so node/edge/meta always line up.
scenario_args=("$@")
has_edges=0; has_nodes=0; has_meta=0
for arg in "${scenario_args[@]}"; do
  case "${arg}" in
    --edges=*|--edges) has_edges=1 ;;
    --nodes=*|--nodes) has_nodes=1 ;;
    --meta=*|--meta)   has_meta=1 ;;
  esac
done
if [[ ${has_edges} -eq 0 ]]; then
  scenario_args=(--edges=results/snapshot_edges.csv "${scenario_args[@]}")
fi
if [[ ${has_nodes} -eq 0 ]]; then
  scenario_args=(--nodes=results/snapshot_nodes.csv "${scenario_args[@]}")
fi
if [[ ${has_meta} -eq 0 ]]; then
  scenario_args=(--meta=results/snapshot_meta.json "${scenario_args[@]}")
fi

# Run ns-3. Outputs land in ${ns3_dir}/results/ first.
printf -v run_spec '%q ' starlink-snapshot "${scenario_args[@]}"
(
  cd "${ns3_dir}"
  ./ns3 run "${run_spec% }"
)

# On success, copy output products into a staging dir then atomically rename
# them into place so prior outputs are never destroyed by a failed run.
stage_dir="$(mktemp -d "${repo_root}/results/.stage-XXXXXX")"
trap 'rm -rf "${stage_dir}"' EXIT
for output_file in \
<<<<<<< HEAD
    flowmon.xml \
    per_flow_metrics.csv \
    starlink-animation.xml \
    run_meta.json; do
=======
  flowmon.xml \
  per_flow_metrics.csv \
  snapshot_edges.csv \
  snapshot_nodes.csv; do
>>>>>>> 5381ee7 (.)
  if [[ -f "${ns3_dir}/results/${output_file}" ]]; then
    cp "${ns3_dir}/results/${output_file}" "${stage_dir}/${output_file}"
  fi
done
for output_file in "${stage_dir}"/*; do
  [[ -e "${output_file}" ]] || continue
  base="$(basename "${output_file}")"
  mv "${output_file}" "${repo_root}/results/${base}"
done

echo "Simulation outputs synced into ${repo_root}/results"
