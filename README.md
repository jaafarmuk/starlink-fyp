# Starlink FYP

A pipeline from real Starlink TLE data to an `ns-3` packet-level simulation of
an inter-satellite-link network. The repository keeps only the project-specific
files; the `ns-3` runtime itself lives in a separate `ns-3-dev` checkout that
you point the scripts at.

## What is here

- `src/starlink-snapshot.cc` — the `ns-3` scenario source.
- `tools/tle_to_snapshot.py` — snapshot / topology generator (TLE → CSV graph).
- `tools/plot_flow_metrics.py` — per-flow plotting helper.
- `tools/generate_snapshot.sh`, `tools/run_ns3_snapshot.sh` — convenience wrappers.
- `datasets/starlink.tle` — TLE input data.
- `results/` — generated CSVs, `flowmon.xml`, NetAnim XML, and plots.

## Requirements

- Linux or macOS (or WSL / Git Bash on Windows) — `ns-3` is not officially
  supported on native Windows.
- Python 3.9+ with a virtual environment.
- A working `ns-3-dev` checkout built with the `flow-monitor` and `netanim`
  modules enabled. Set `NS3_DIR` to point at it, or pass `--ns3-dir` to the
  run wrapper.

## Python setup

From the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Generate topology

From the repository root:

```bash
./tools/generate_snapshot.sh
```

Extra arguments are forwarded to `tle_to_snapshot.py`. Useful examples:

```bash
# Larger constellation, tighter ISL range, more inter-plane links.
./tools/generate_snapshot.sh --n 80 --max_km 4500 --inter_plane 2

# Pin the propagation epoch to a specific UTC instant. If omitted, each
# satellite is evaluated at its own TLE epoch (recommended for freshness).
./tools/generate_snapshot.sh --utc 2026-03-21T12:00:00

# Deterministic random sampling from the dataset instead of taking the first N.
./tools/generate_snapshot.sh --sample random --seed 42 --n 100
```

The generator rejects SGP4 outputs with implausible LEO altitudes and warns
when the resulting topology is sparse or dominated by isolated nodes.

## Run the `ns-3` simulation

```bash
export NS3_DIR=/path/to/ns-3-dev
./tools/run_ns3_snapshot.sh
```

Or pass the directory inline:

```bash
./tools/run_ns3_snapshot.sh --ns3-dir /path/to/ns-3-dev
```

The wrapper copies `src/starlink-snapshot.cc` and the snapshot CSVs into the
`ns-3-dev` tree, runs the scenario, and copies the generated outputs
(`per_flow_metrics.csv`, `flowmon.xml`, NetAnim XML) back into `results/`.

Scenario arguments are forwarded through. Example:

```bash
./tools/run_ns3_snapshot.sh --numFlows=8 --simTime=20 --enableAnim=false
```

## Plot flow metrics

After a simulation run, `results/per_flow_metrics.csv` will be present:

```bash
. .venv/bin/activate
python3 tools/plot_flow_metrics.py
```

This emits per-flow bar charts and CDFs for throughput, mean delay, and loss
rate into `results/`. Use `--input` / `--out_dir` to override the defaults.
