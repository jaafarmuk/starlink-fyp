# Starlink FYP — Project Reference

## What this project is

A Final Year Project that builds a **real Starlink network simulation from TLE orbital
data** and uses it to conduct the first empirical evaluation of Wang's BPP multi-hop
routing reliability model on a real operational LEO satellite constellation.

**Target paper:**
Wang, Kishk & Alouini (2023) — *"Reliability Analysis of Multi-hop Routing in Multi-tier
LEO Satellite Networks"* — arXiv:2303.02286

---

## Research question

> We present the first empirical evaluation of the Wang (2023) BPP analytical routing
> model on a real operational LEO satellite constellation. Using a simulation framework
> built from real Starlink TLE orbital data, we find the model underestimates routing
> interruption by 98% relative error and characterise the validity boundary across
> multiple parameters.

The paper derives a closed-form formula to predict interruption probability in multi-tier
LEO networks, assuming satellites are randomly scattered — a **Binomial Point Process
(BPP)**. The paper claims the model applies to multi-tier LEO networks generally. Starlink
is a real multi-tier LEO network. We test it. The model predicts 1.4% interruption.
Real Walker-delta geometry produces 78.6%. We characterise exactly where and why it breaks.

---

## Core contribution

1. Builds real Starlink network snapshots from live TLE orbital data (4,080 satellites,
   250 real ground stations, 10 time epochs)
2. Runs the paper's **greedy Wang-style routing algorithm** on real geometry to get the
   empirical (measured) interruption probability
3. Simultaneously computes the paper's **analytical BPP formula** prediction
4. Compares both across multiple parameter sweeps (d_th, satellite count, shell separation)
5. Identifies **where and why the model breaks down** — the validity boundary
6. Adds **constraint failure analysis**: for each interrupted packet, records which
   geometric constraint (c1 direction, c2 dome angle, c3 line-of-sight) was the
   binding bottleneck

**Primary metrics:**
- Absolute error = |empirical − BPP predicted|
- Relative error = absolute error / empirical
- Binding constraint breakdown (% of failures caused by c1 / c2 / c3)

---

## Repository structure

```
starlink-fyp/
├── datasets/
│   ├── starlink.tle                          # Real Starlink TLE orbital data
│   └── starlink_ground_stations_hf_operational_safe.csv   # 250 ground stations
│
├── tools/
│   ├── tle_to_snapshot.py                    # TLE → network graph (nodes + edges CSV)
│   ├── analyze_mhr_reliability.py            # Core research script: BPP model vs empirical
│   ├── plot_mhr_reliability.py               # Plot results from analysis
│   ├── generate_snapshot.sh                  # Shell wrapper for tle_to_snapshot.py
│   └── visualizer/
│       ├── index.html                        # Visualizer UI
│       └── app.js                            # Visualizer logic (Three.js)
│
├── results/
│   ├── snap_optA_nodes.csv                   # Base epoch: 4,330 nodes (4,080 sats + 250 GW)
│   ├── snap_optA_edges.csv                   # Base epoch: ~37,000 ISL + access links
│   ├── snap_optA_meta.json                   # Topology metadata and validation
│   ├── snap_optA_nodes.t1.csv … t9.csv       # 9 additional epochs (9-min intervals)
│   ├── snap_optA_edges.t1.csv … t9.csv       # Edges for each epoch
│   ├── mhr_optA_summary.json / .csv          # Main experiment results
│   ├── mhr_optA_dth3000/3500/5000_summary.*  # d_th sweep results
│   └── comprehensive_findings.txt            # Full written analysis of all experiments
│
├── src/
│   └── starlink-snapshot.cc                  # NS-3 scenario (legacy, not required)
│
├── requirements.txt                          # Python dependencies
└── CLAUDE.md                                 # This file
```

---

## Snapshot topology (snap_optA — main dataset)

| Property | Value |
|---|---|
| TLE source | `datasets/starlink.tle` (9,887 TLEs ingested) |
| Epoch (base) | 2026-03-21 19:20:15 UTC |
| Total epochs | 10 (9-minute steps → 81 minutes total) |
| Total nodes | **4,330 (4,080 satellites + 250 gateways)** |
| Satellites selected | **4,080 — ALL available (full Walker-delta structure)** |
| Shell A | 43° inclination, ~476–500 km altitude, **2,670 satellites** |
| Shell B | 53° inclination, ~528–547 km altitude, **1,410 satellites** |
| Shell separation | ~56 km altitude gap (justified multi-tier experiment) |
| Gateways | 250 from operational Starlink ground station dataset |
| ISL links (base epoch) | ~8,000–9,000 inter-satellite links |
| Total edges (base epoch) | ~37,000–38,000 (ISL + gateway access links) |
| Max ISL distance | 5,000 km |
| ISL topology | ±2 intra-plane + ±2 inter-plane neighbours, degree ≤ 4 |
| Seed | 42 (fixed for reproducibility) |
| Schema version | 2.1.0 |

**Why all 4,080 satellites?** A random subsample (e.g. 600 from 4,080) partially
destroys the Walker-delta orbital-plane structure. Random subsampling makes the geometry
look more like BPP's random-placement assumption — which is why 600-sat runs show only
a 28 percentage-point gap while the full constellation shows a 77 percentage-point gap.
The full set preserves real plane spacing and systematic angular gaps.

**Shell selection mode:** `top2_separated` — picks the two largest shells with ≥ 50 km
altitude separation. Shells closer than ~10 km collapse into one tier and break the BPP
model assumptions.

**Important warnings during generation (normal):**
- `shells=4` — 4 internal shell IDs detected, merged to 2 analytical tiers (Shell A + B)
- `WARNING: Shell 0: very uneven planes (min=1, max=51)` — real Starlink deployment is
  still in progress; some orbital planes are densely populated, others are sparse. This
  uneven distribution is one of the reasons BPP fails — it assumes uniform random scatter.

---

## How to run the project

### Prerequisites
```bash
source .venv/bin/activate   # activate virtual environment
pip install -r requirements.txt
```

### Step 1 — Generate a snapshot (already done, in results/)
```bash
bash tools/generate_snapshot.sh
# Produces: results/snap_optA_nodes.csv, snap_optA_edges.csv, snap_optA_meta.json
# Plus 9 additional epoch files (t1–t9)
```

### Step 2 — Run the analysis
```bash
python3 tools/analyze_mhr_reliability.py \
  --nodes results/snap_optA_nodes.csv \
  --edges results/snap_optA_edges.csv \
  --meta  results/snap_optA_meta.json \
  --d_th_km 4000 --pairs 300 \
  --summary_json results/mhr_optA_summary.json \
  --summary_csv  results/mhr_optA_summary.csv
```

Key parameters:
- `--d_th_km` — max hop distance in km (paper constraint c3). Default 4000.
- `--theta_r` — direction angle limit in radians (constraint c1). Default 0.524 (30°).
- `--theta_s` — dome min angle in radians (constraint c2). Default 0.314 (18°).
- `--pairs` — number of near-antipodal src-dst pairs per epoch. Default 300.
- `--summary_json` / `--summary_csv` — output file paths.

Output now includes constraint failure columns:
- `binding_c1_pct` — % of interrupted packets where direction-angle cone (c1) was the
  binding constraint (Walker-delta plane gap in the required direction)
- `binding_c2_pct` — % where dome-angle threshold (c2) eliminated all candidates
- `binding_los_pct` — % where Earth blocking (line-of-sight) caused the failure

### Step 3 — Plot results
```bash
python3 tools/plot_mhr_reliability.py --summary_csv results/mhr_optA_summary.csv
```

### Step 4 — Run the visualizer
```bash
python3 -m http.server 8080
# Open: http://localhost:8080/tools/visualizer/
```

---

## Core research script: analyze_mhr_reliability.py

### A. Empirical routing (Wang-style greedy on real geometry)
- At each hop: scan all nodes within d_th km (geometric relay model, not graph-constrained)
- Apply **c1**: direction angle ≤ θ_r (30°)
- Apply **c2**: geocentric dome angle between current and candidate ≥ θ_s (18°)
- Apply **c3**: distance ≤ d_th AND line-of-sight (Earth not blocking)
- Pick candidate with smallest geocentric angle to destination (most progress)
- Gateway hops relax c1/c2 (described as "Wang-style with gateway-hop relaxation")
- If no valid candidate: packet **interrupted**

**Note:** This is a geometric relay model — it scans all nodes in range, not just
prebuilt ISL neighbours. This matches Wang's paper assumption of on-demand links.

### B. Analytical BPP prediction (paper's formula)
- Equation (2): P^I_{i,j} — tier-to-tier single-hop interruption probability
- Equation (3): P^S_i — single-hop total interruption probability
- Algorithm 3: augmented Transition Probability Matrix
- Equation (5): N_h — average hops for successful transmission
- Theorem 1 / Equation (7): P̃^M — multi-hop interruption probability

Three strategies: `density`, `single_hop`, `stationary_optimal`.

### C. Constraint failure analysis (new)
For every interrupted packet, a secondary pass counts how many candidates in range
failed each constraint specifically. The binding constraint (the one that eliminated
the last surviving candidates) is recorded. Aggregated as `binding_c1_pct`,
`binding_c2_pct`, `binding_los_pct` in the output.

### D. Dijkstra baseline
Runs unconstrained shortest-path on the prebuilt ISL graph (fixed max 5,000 km).
**Important:** Dijkstra results do NOT change across d_th sweeps because it uses the
fixed snapshot graph, not the greedy relay model. Label it as "fixed-topology
shortest-path baseline" in tables, not a same-constraint comparison.

---

## Key experiment results

### Primary finding: BPP model breaks down at realistic satellite density

Using the full 4,080-satellite Walker-delta constellation, K=3 tiers, 10 epochs:

| d_th (km) | Empirical | BPP Predicted | Abs Error | Rel Error |
|-----------|-----------|---------------|-----------|-----------|
| 3000 | 0.874 | 0.016 | **0.858** | 98.2% |
| 3500 | 0.849 | 0.014 | 0.835 | 98.3% |
| **4000** | **0.786** | **0.014** | **0.772** | **98.2%** ← paper default |
| 5000 | 0.744 | 0.014 | 0.730 | 98.1% |

**The gap is constant across d_th** — this is not a parameter tuning problem. The error
is structural: Walker-delta orbital plane gaps cannot be fixed by adjusting d_th.

**600-satellite artifact:** Random subsampling to 600 from 4,080 gives empirical=0.896,
BPP=0.619, gap=0.277 (28pp). This appears to show the model working better — but it is
a sampling artifact. Subsampling destroys Walker-delta structure, making the geometry
look more like BPP's random-placement assumption. The full constellation shows the true
gap: **77pp**.

### Model validity summary

| Dimension | Condition | BPP accuracy |
|---|---|---|
| Satellite count | Full constellation (4,080) | ~98% relative error |
| Satellite count | Random subsample (600) | ~31% relative error (artifact) |
| d_th | Any value, full constellation | ~98% error (constant) |
| Shell separation | < 10 km | Tier collapse — 53% error |
| Shell separation | > 50 km, full constellation | ~77pp absolute error |
| Population N | < 200 | Routing failure (too sparse) |
| Population N | 800 (imbalanced tiers) | BPP breaks (3.8% vs 63%) |
| Strategy ranking | Full constellation | Indistinguishable (~1.4% all) |

---

## The visualizer (tools/visualizer/)

Interactive 3D demonstration tool built with Three.js. Supports live packet routing
animation, side-by-side algorithm comparison, and route explanation.

### Satellite count selector
Dropdown at the top of the Snapshot panel:
- **200 / 600 / 1500 satellites** — randomly subsamples for faster rendering
- **All 4,080** — full real dataset (default, may be laggy on slow machines)

This is also a live demonstration of the research finding: switching from 600 to 4,080
satellites shows the BPP interruption rate increase as Walker-delta structure is restored.
All 250 gateways are always kept regardless of the count setting.

### "Why this route?" explanation panel
After every BPP route (successful or interrupted), a per-hop breakdown appears:

**Successful hop example:**
> Hop 2 → STARLINK-35995 (4966 km)
> Scanned 423 satellite candidates in range. 312 blocked by Earth (c3).
> 89 failed direction cone 30° (c1). 7 failed dome angle 18° (c2).
> 44 valid candidates. Chose this node — geocentric angle to destination 8.4°
> (smallest = most geometric progress).

**Interrupted hop example:**
> ✗ Hop 3 — STUCK
> Scanned 285 candidates. 87 failed direction cone (c1). 0 remained.
> This is a Walker-delta orbital plane gap — no satellite lies in the required direction.

Dijkstra gets a single explanatory paragraph about shortest-path routing.

### Routing modes

**Dijkstra:** Unconstrained shortest-delay path on prebuilt ISL graph. Represents
realistic graph-based routing. Gateway access is maximally connected (~115 links per
gateway) — Dijkstra's 0% interruption rate reflects this permissive model, not real
Starlink operational routing.

**BPP Paper routing:** Exact Wang-style greedy geometric relay:
- Scans ALL nodes within d_th km at each hop (not just ISL neighbours)
- Applies c1 (direction ≤ θ_r), c2 (geocentric dome ≥ θ_s), c3 (distance + LoS)
- c2 geometry: `acos(dot(cur_pos, cand_pos) / (|cur| × |cand|))` — geocentric angle
  between position vectors (matches Python `candidate_constraints_ok` exactly)
- Selection metric: geocentric angle between candidate and destination positions
- Gateway hops relax c1/c2 (gateways are at altitude 0; paper geometry breaks down)
- Non-ISL hops drawn as purple synthetic links

### Delay model
- **Propagation**: distance_km / 299,792.458 km/s
- **Serialization**: packet_bytes × 8 / link_rate_bps
- **Queuing (M/D/1)**: ρ / (2(1−ρ)) × T_serialization

### Known design choices (document in thesis)
- Greedy routing scans all nodes in range, not graph edges — correct for Wang's model
- Dijkstra uses fixed 5,000 km graph regardless of d_th sweep — label as baseline
- Pair sampling for 250 gateways is deterministic near-antipodal, not random
- Gateway access links are uncapped (permissive model)
- 4 internal shell IDs merged to 2 analytical tiers

---

## What is NOT part of this project

- **NS-3**: `src/starlink-snapshot.cc` exists but is legacy. All simulation is in Python.
- **Real Starlink internals**: We test Wang's model on TLE geometry. We do not model
  Starlink's internal production routing software.

---

## Python dependencies

```
skyfield        # TLE propagation (ECEF position computation)
pandas          # CSV I/O and data manipulation
numpy           # Numerical computations
scipy           # Matrix operations (stationary distribution)
matplotlib      # Plotting
networkx        # Graph connectivity validation
requests        # Live TLE download (optional)
```
