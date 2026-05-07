# Starlink FYP — BPP Model vs Real TLE Geometry

> **Research question:** How accurately does the analytical multi-hop interruption
> probability model from Wang, Kishk & Alouini (2023) predict routing failures on
> real Starlink TLE-derived geometry, compared to the paper's idealized random
> satellite placement?

---

## What this project does

1. Pulls real Starlink orbital data (TLEs) and builds 10 frozen-time network
   snapshots spanning 81 minutes of constellation movement.
2. Runs the paper's **greedy routing algorithm** on that real geometry and counts
   actual routing failures → **empirical interruption probability**.
3. Computes the paper's **closed-form BPP formula** prediction for the same setup.
4. Compares the two side by side to find where and why the model breaks down.
5. Visualizes the satellite topology and live packet routing in a 3D browser tool.

Paper: *Wang, Kishk & Alouini — "Reliability Analysis of Multi-hop Routing in
Multi-tier LEO Satellite Networks"* (arXiv:2303.02286)

---

## Repository layout

```
starlink-fyp/
├── datasets/
│   ├── starlink.tle                                   # Starlink TLE orbital data
│   └── starlink_ground_stations_hf_operational_safe.csv  # 250 real ground stations
│
├── tools/
│   ├── tle_to_snapshot.py          # TLE → network graph (nodes + edges CSV)
│   ├── analyze_mhr_reliability.py  # Core script: BPP model vs empirical routing
│   ├── plot_mhr_reliability.py     # Plot the comparison results
│   ├── plot_flow_metrics.py        # Plot NS-3 per-flow metrics (if NS-3 used)
│   ├── generate_snapshot.sh        # Wrapper to run tle_to_snapshot.py
│   ├── run_visualizer.sh           # Start the local HTTP server
│   ├── run_ns3_snapshot.sh         # Run NS-3 simulation over the snapshot
│   └── visualizer/
│       ├── index.html              # 3D visualizer UI
│       └── app.js                  # Three.js visualizer logic
│
├── results/
│   ├── snap_optA_nodes.csv         # Canonical snapshot — 600 sats + 250 GW (base epoch)
│   ├── snap_optA_nodes.t1–t9.csv   # 9 additional epochs (9-min steps)
│   ├── snap_optA_edges.csv         # ISL + access links (base epoch)
│   ├── snap_optA_edges.t1–t9.csv   # Edges for each epoch
│   ├── snap_optA_meta.json         # Topology metadata and parameters
│   ├── snap_optA_stats.csv         # Per-epoch topology statistics
│   ├── mhr_optA_summary.json/.csv  # Main experiment results
│   ├── mhr_optA_dth3000/3500/5000_summary.json  # d_th sweep results
│   └── comprehensive_findings.txt  # Full written analysis and conclusions
│
├── src/
│   └── starlink-snapshot.cc        # NS-3 C++ simulation scenario (optional)
│
├── CLAUDE.md                       # Full project reference (AI assistant context)
├── requirements.txt
└── README.md                       # This file
```

---

## Getting started (fresh clone)

### Step 1 — Create a virtual environment and install all dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> **macOS (Homebrew Python):** If `python3` gives a stub error, use:
> `/opt/homebrew/bin/python3 -m venv .venv` then `source .venv/bin/activate`

`requirements.txt` includes everything: `skyfield`, `numpy`, `scipy`, `pandas`,
`matplotlib`, `networkx`, `sgp4`. **Activate the venv in every new terminal session**
with `source .venv/bin/activate` before running any script.

---

### Step 2 — Generate the network snapshot

```bash
bash tools/generate_snapshot.sh
```

This reads `datasets/starlink.tle` (already in the repo — no internet needed) and
produces the canonical 10-epoch topology in `results/`:

```
results/snap_optA_nodes.csv        ← 4,330 nodes (4,080 sats + 250 GW), base epoch
results/snap_optA_nodes.t1–t9.csv  ← 9 more epochs, 9-min steps
results/snap_optA_edges.csv        ← ISL + access links, base epoch
results/snap_optA_edges.t1–t9.csv  ← edges for each epoch
results/snap_optA_meta.json        ← topology metadata
results/snap_optA_stats.csv        ← per-epoch statistics
```

Takes ~30 seconds. Snapshot details:
- 4,080 satellites across 2 shells (43° and 53° inclination, 56 km altitude gap)
- 250 real Starlink ground stations
- 10 epochs × 9-minute steps = 81 minutes of constellation movement

---

### Step 3 — Run the core research analysis

```bash
python3 tools/analyze_mhr_reliability.py \
  --nodes results/snap_optA_nodes.csv \
  --edges results/snap_optA_edges.csv \
  --meta  results/snap_optA_meta.json \
  --d_th_km 4000 --pairs 300 \
  --summary_json results/mhr_optA_summary.json \
  --summary_csv  results/mhr_optA_summary.csv
```

This runs the paper's BPP formula **and** the empirical greedy routing on the same
topology across all 10 epochs and compares them.

| Flag | Default | Meaning |
|------|---------|---------|
| `--d_th_km` | 4000 | Max hop distance in km (paper constraint c3) |
| `--theta_r` | 0.524 (30°) | Direction angle limit (constraint c1) |
| `--theta_s` | 0.314 (18°) | Dome min elevation angle (constraint c2) |
| `--pairs` | 300 | Random src-dst pairs tested per epoch |
| `--summary_json` | results/mhr_optA_summary.json | JSON output path |
| `--summary_csv` | results/mhr_optA_summary.csv | CSV output path |

Outputs: `results/mhr_optA_summary.json` and `results/mhr_optA_summary.csv`

---

### Step 4 — Plot the results

```bash
python3 tools/plot_mhr_reliability.py --summary results/mhr_optA_summary.json
```

Produces PNG plots comparing empirical vs BPP-predicted interruption probability
across epochs and d_th values.

---

### Step 5 — Launch the 3D visualizer

```bash
python3 -m http.server 8080 --directory .
```

Open in browser: **http://localhost:8080/tools/visualizer/**

The visualizer shows the real satellite positions, ISL links, and lets you send
packets using either:
- **Dijkstra routing** — shortest propagation-delay path on pre-built ISL graph
- **BPP Paper routing** — exact greedy algorithm from the paper (c1/c2/c3 constraints,
  scans all 850 nodes, same logic as `analyze_mhr_reliability.py`)

Features: congestion heatmap, M/D/1 queuing delay, 10-epoch time window selector,
per-hop stats table, side-by-side Dijkstra vs BPP comparison panel.

---

### Step 6 — (Optional) NS-3 simulation

If you have NS-3 installed (`~/ns-3-dev` by default):

```bash
bash tools/run_ns3_snapshot.sh
python3 tools/plot_flow_metrics.py
```

---

## Key results

Full Walker-delta constellation — 4,080 satellites (ALL available), 250 gateways,
10 epochs, 300 pairs/epoch:

| d_th (km) | Empirical | BPP Predicted | Abs Error | Rel Error |
|-----------|-----------|---------------|-----------|-----------|
| 3000 | 0.874 | 0.016 | 0.858 | 98.2% |
| 3500 | 0.849 | 0.014 | 0.835 | 98.3% |
| **4000** | **0.786** | **0.014** | **0.772** | **98.2%** ← paper default |
| 5000 | 0.744 | 0.014 | 0.730 | 98.1% |

**Primary finding:** At realistic Starlink satellite density, BPP predicts ~1.4%
interruption probability. The measured reality is 74–87%. The model is wrong by
~98% relative error — essentially useless at this scale.

**Why:** BPP assumes random satellite placement. At 4,080 satellites, random
placement gives near-uniform angular coverage → BPP says routing is trivially easy.
Real Starlink satellites orbit in Walker-delta planes, creating systematic angular
gaps the greedy algorithm cannot bypass → 74–87% actual failure rate.

Full analysis and earlier limited-sample experiments: `results/comprehensive_findings.txt`

---

## Requirements

- Python 3.8+
- `pip install -r requirements.txt`
- Browser with WebGL (for visualizer)
- NS-3 (optional, only for Step 7)
