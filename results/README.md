# Results Workspace

`results/` is a local working directory for generated experiment outputs.

Typical files written here include:

- snapshot CSV/JSON files from `tools/generate_snapshot.sh`
- reliability CSV/JSON/PNG files from `tools/analyze_mhr_reliability.py`
- ns-3 outputs such as `per_flow_metrics.csv`, `run_meta.json`, and NetAnim XML

These files are intentionally not tracked in git because they change whenever
you rerun experiments.

If you need a reproducible artifact for a report or paper, export it
deliberately instead of relying on the live contents of this folder.
