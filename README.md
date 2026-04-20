# Starlink FYP

This repo now keeps the project-specific files only. It uses the local `ns-3-dev` checkout at `/home/jaafarmuk/ns-3-dev` as the simulation runtime.

## What is here

- `src/starlink-snapshot.cc`: the current `ns-3` scenario source
- `tools/tle_to_snapshot.py`: snapshot/topology generator
- `tools/plot_flow_metrics.py`: plotting helper
- `datasets/starlink.tle`: TLE input data
- `results/`: generated CSV, XML, and plots

## Python setup

```bash
cd /home/jaafarmuk/starlink-fyp
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Generate topology

```bash
cd /home/jaafarmuk/starlink-fyp
./tools/generate_snapshot.sh
```

You can pass extra arguments through to `tle_to_snapshot.py`, for example:

```bash
./tools/generate_snapshot.sh --n 80 --max_km 4500 --inter_plane 1
```

## Run the `ns-3` simulation

```bash
cd /home/jaafarmuk/starlink-fyp
./tools/run_ns3_snapshot.sh
```

This script:

- copies `src/starlink-snapshot.cc` into `ns-3-dev/scratch/`
- copies the current snapshot CSVs into `ns-3-dev/results/`
- runs `./ns3 run "starlink-snapshot ..."` inside `ns-3-dev`
- copies generated outputs back into this repo's `results/`

Extra simulation arguments are forwarded directly to the scenario, for example:

```bash
./tools/run_ns3_snapshot.sh --numFlows=8 --simTime=20 --enableAnim=false
```

If your `ns-3` checkout lives somewhere else, set `NS3_DIR` or pass `--ns3-dir`:

```bash
NS3_DIR=/path/to/ns-3-dev ./tools/run_ns3_snapshot.sh
./tools/run_ns3_snapshot.sh --ns3-dir /path/to/ns-3-dev --numFlows=6
```

## Plot flow metrics

If you have `results/flow_metrics.csv`, run:

```bash
cd /home/jaafarmuk/starlink-fyp
. .venv/bin/activate
python3 tools/plot_flow_metrics.py
```
