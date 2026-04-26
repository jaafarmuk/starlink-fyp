# Starlink FYP: Multi-tier LEO Reliability vs. Real Starlink TLE Snapshots

This project compares the analytical multi-hop interruption probability from
Wang, Kishk, and Alouini (arXiv:2303.02286, 2023) against empirical multi-hop
routing measured on real Starlink TLE snapshots. The accompanying ns-3 TCP
simulation and Three.js visualizer are kept as supporting tooling.

The project is not a reverse-engineered model of the real Starlink production
network. Real Starlink ISL scheduling, gateway/PoP choices, beam assignment,
capacity sharing, and routing policies are proprietary. The built-in gateways
are clearly labelled `GW-DEMO-*` and are demo placeholders. Public TLEs plus
documented geometric assumptions are the only inputs.

## Research Contribution

The research question is:

> How well does the BPP analytical interruption-probability model from
> Wang et al. (2023) predict empirical multi-hop routing reliability on
> real Starlink TLE snapshots?

The pipeline answers this in four steps:

1. Pull a recent Starlink TLE batch (live CelesTrak feed, or
   `datasets/starlink.tle`).
2. Use SGP4 to propagate satellites to one or more common epochs spanning
   roughly an orbital period (`--epoch_steps`, `--multi_epoch_seconds`).
3. For each epoch, run `tools/analyze_mhr_reliability.py` to:
   - infer Wang-style tiers (gateway tier + per-altitude satellite tiers)
     from the snapshot,
   - evaluate the analytical model end-to-end: tier-to-tier interruption
     (eq. 2), single-hop interruption (eq. 3), TPMs T^(1)/T~^(2)/T^^(3),
     stationary distribution v, mu_1 (eq. 4), N_h (eq. 5), and the
     multi-hop interruption probability P~^M (eq. 7),
   - empirically simulate Wang-style greedy multi-hop forwarding across
     random (src, dst) pairs on the real geometry,
   - report empirical vs analytical interruption probability, absolute
     and relative error, mean success hops, and the interrupted-hop index
     per priority strategy and epoch.
4. Plot the comparison with `tools/plot_mhr_reliability.py`.

## What The Project Does

1. Reads Starlink TLE records from `datasets/starlink.tle`.
2. Optionally filters the TLE set to a more operational-looking Starlink subset.
3. Propagates satellites to one (or several) common epochs using SGP4.
4. Converts orbital positions into ECI/ECEF/geodetic coordinates.
5. Groups satellites into shells and planes.
6. Builds inter-satellite links and gateway access links.
7. Writes snapshot files in `results/`.
8. Computes the BPP analytical reliability (Wang et al.) and the matching
   empirical reliability over the snapshot, and emits CSV/JSON/PNG comparison output.
9. Optionally runs a frozen-time ns-3 TCP simulation over the topology.
10. Optionally serves a Three.js visualizer for educational inspection.

## Main Files

- `tools/tle_to_snapshot.py`: TLE parser, satellite propagation, shell/plane grouping, link generation, validation, and CSV/JSON output. Supports multi-epoch generation through `--epoch_steps` and `--multi_epoch_seconds`.
- `tools/analyze_mhr_reliability.py`: implements the Wang et al. (arXiv:2303.02286) analytical model and the matching empirical multi-hop simulator; emits the comparison CSV/JSON.
- `tools/plot_mhr_reliability.py`: matplotlib plots of empirical vs analytical interruption probability, absolute error, and per-strategy comparison.
- `src/starlink-snapshot.cc`: ns-3 scenario that reads the snapshot and simulates TCP flows.
- `tools/visualizer/index.html` and `tools/visualizer/app.js`: optional educational 3D topology and packet/path visualizer.
- `tools/generate_snapshot.sh`: wrapper for generating snapshot files.
- `tools/run_ns3_snapshot.sh`: wrapper for running the ns-3 scenario.
- `tools/run_visualizer.sh`: serves the browser visualizer locally.
- `tools/plot_flow_metrics.py`: plots per-flow ns-3 metrics.
- `results/`: generated snapshot, reliability comparison, and simulation output files.

## What We Used

- Python for data processing and topology generation.
- `sgp4` for propagating TLE orbital elements.
- `numpy` and `pandas` for math, filtering, and CSV generation.
- ns-3 for packet/network simulation.
- C++ for the ns-3 scenario.
- Three.js for the browser-based 3D visualizer.
- Matplotlib for plotting per-flow metrics.
- Public TLE data as the satellite source.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

`requirements.txt` only installs the Python packages. ns-3 must be installed
separately, and the visualizer loads Three.js in the browser from a CDN.

## Basic Usage

### 1. Prepare Python

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell, activate with:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Get Or Update Starlink TLE Data

The repository already includes `datasets/starlink.tle`. To refresh it from
CelesTrak's current Starlink GP/TLE feed, use one of these commands.

PowerShell:

```powershell
Invoke-WebRequest "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle" -OutFile datasets\starlink.tle
```

Bash:

```bash
curl -L "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle" -o datasets/starlink.tle
```

CelesTrak also provides supplemental Starlink GP data derived from SpaceX
public ephemeris data, but this project currently consumes standard TLE/3LE
text files.

### 3. Generate A Realistic Limited-Size Snapshot

The recommended command filters the raw TLE file to a cleaner operational
Starlink-like subset, selects one coherent shell, and then samples from that
shell. This avoids the unrealistic mistake of mixing many shells into one
small fragmented topology.

```bash
tools/generate_snapshot.sh --n 400 --sample random --seed 1 --starlink_operational
```

Equivalent PowerShell command:

```powershell
python tools\tle_to_snapshot.py --tle datasets\starlink.tle --n 400 --sample random --seed 1 --starlink_operational --edges_out results\snapshot_edges.csv --nodes_out results\snapshot_nodes.csv --stats_out results\topology_stats.csv --meta_out results\snapshot_meta.json
```

This creates:

```text
results/snapshot_nodes.csv
results/snapshot_edges.csv
results/snapshot_meta.json
results/topology_stats.csv
```

By default, `--shell_select largest` is enabled. Use `--shell_select none`
only if you intentionally want a multi-shell population and understand that
the resulting graph may fragment.

For multi-epoch sweeps over roughly one orbital period, add
`--epoch_steps` and `--multi_epoch_seconds`. The recommended
research-grade command (single-shell, ~1600 satellites, 10 epochs over 90
minutes) is:

```bash
tools/generate_snapshot.sh --no_live --n 1600 --sample random --seed 1 \
    --starlink_operational --shell_select largest \
    --epoch_steps 10 --multi_epoch_seconds 540
```

This produces the base files plus `snapshot_nodes.t1.csv` ..
`snapshot_nodes.t9.csv` and the matching `snapshot_edges.t*.csv`. All epoch
metadata is recorded in `snapshot_meta.json` under `validation_per_step`.

### 4. Run The MHR Reliability Comparison

```bash
python tools/analyze_mhr_reliability.py --pairs 200 --seed 1
```

The analyser auto-discovers all epoch CSVs (`snapshot_nodes.csv` plus
`snapshot_nodes.t1.csv` and so on). Outputs:

```text
results/mhr_reliability_summary.csv      one row per (strategy, epoch)
results/mhr_reliability_per_epoch.csv    same data, organised by epoch
results/mhr_reliability_summary.json     full nested summary
results/mhr_reliability_per_pair.csv     only with --write-per-pair
```

Useful flags:

```bash
# Different physical assumptions:
python tools/analyze_mhr_reliability.py --theta_r 0.6 --theta_s 0.3 --d_th_km 5000

# Use the paper's fixed theta_m (default uses the observed mean of sampled pairs):
python tools/analyze_mhr_reliability.py --theta_m 3.14159 --theta_m_mode fixed

# Per-pair audit:
python tools/analyze_mhr_reliability.py --pairs 100 --write-per-pair
```

Then plot:

```bash
python tools/plot_mhr_reliability.py
```

This writes:

```text
results/mhr_reliability_overview.png       empirical vs analytical, per epoch
results/mhr_reliability_error.png          absolute error per epoch
results/mhr_reliability_by_strategy.png    bar chart, mean over epochs
```

The comparison is independent of ns-3. ns-3 still runs over the same
snapshot for end-to-end TCP results.

### 5. Run With ns-3

Install/build ns-3 first. By default, the wrapper expects the ns-3 checkout at:

```text
~/ns-3-dev
```

Then run:

```bash
tools/run_ns3_snapshot.sh
```

If ns-3 is somewhere else:

```bash
tools/run_ns3_snapshot.sh --ns3-dir /path/to/ns-3-dev
```

or:

```bash
NS3_DIR=/path/to/ns-3-dev tools/run_ns3_snapshot.sh
```

The wrapper copies `src/starlink-snapshot.cc` into `ns-3-dev/scratch/`, runs
the scenario, and syncs outputs back into `results/`.

Useful scenario overrides:

```bash
tools/run_ns3_snapshot.sh --numFlows=8 --simTime=20 --flowPattern=gateway
tools/run_ns3_snapshot.sh --rate=1Gbps --accessRate=1Gbps --queueSize=1000p
```

### 6. Run The Visualizer (optional, educational)

Serve the repository locally:

```bash
tools/run_visualizer.sh
```

Then open:

```text
http://localhost:8000/tools/visualizer/
```

The visualizer reads the generated `results/snapshot_nodes.csv`,
`results/snapshot_edges.csv`, and `results/snapshot_meta.json`. It is
provided as an educational view of the snapshot. Its queueing model is
simplified and is not part of the research comparison.

### 7. Plot ns-3 Flow Metrics

After running ns-3:

```bash
python tools/plot_flow_metrics.py
```

## Generated Outputs

Snapshot generator (per epoch step k; t0 omits the suffix):

- `results/snapshot_nodes.csv` (and `.t1.csv`, `.t2.csv`, ...): satellite and gateway nodes.
- `results/snapshot_edges.csv` (and `.t1.csv`, ...): ISL and ground access links.
- `results/snapshot_meta.json`: generator settings, schema, filters, caveats, and per-step validation.
- `results/topology_stats.csv`: connectivity and distance summary, one row per epoch.

MHR reliability comparison:

- `results/mhr_reliability_summary.csv`: one row per (strategy, epoch).
- `results/mhr_reliability_per_epoch.csv`: same data, organised by epoch.
- `results/mhr_reliability_summary.json`: nested summary including parameters.
- `results/mhr_reliability_per_pair.csv`: per-pair success/interrupt log (only with `--write-per-pair`).
- `results/mhr_reliability_overview.png`, `mhr_reliability_error.png`, `mhr_reliability_by_strategy.png`: plots.

ns-3:

- `results/per_flow_metrics.csv`: ns-3 per-flow results.
- `results/run_meta.json`: ns-3 run metadata.
- `results/flowmon.xml`: ns-3 FlowMonitor output.

## Important Modeling Notes

- The topology is a frozen-time snapshot. Satellite movement during the ns-3 run is not modeled. The Wang reliability comparison uses several frozen snapshots across an orbital period instead.
- Built-in gateways are demo locations named `GW-DEMO-*`, not official Starlink gateway sites. The Wang ground tier inherits these placeholders unless a real `--gateways_csv` is provided.
- Edge delay is one-way vacuum propagation delay, not real ping latency.
- The default ns-3 traffic pattern is gateway-to-gateway because random satellite-to-satellite user flows are not realistic for Starlink user traffic.
- The visualizer is educational. Its queueing and retransmission behavior are simplified and are not part of the research comparison.
- Strict validation is enabled by default so obviously bad topologies fail instead of silently producing misleading results.

## Equations Used

This section summarizes the main equations and models used in the project.

### 1. TLE Propagation

Satellite position and velocity are propagated with SGP4:

```text
(r_eci, v_eci) = SGP4(TLE, epoch)
```

where:

- `r_eci` is the satellite position in Earth-centered inertial coordinates.
- `v_eci` is velocity in the same frame.
- `epoch` is one common timestamp for all satellites.

### 2. Julian Date Conversion

UTC time is converted into Julian date components:

```text
JD = JD0 + fractional_day
```

The fractional day is:

```text
fractional_day = (hour + minute/60 + second/3600) / 24
```

### 3. ECI to ECEF Rotation

The generator rotates ECI coordinates into Earth-fixed coordinates using GMST:

```text
theta = GMST(JD)

x_ecef =  cos(theta) * x_eci + sin(theta) * y_eci
y_ecef = -sin(theta) * x_eci + cos(theta) * y_eci
z_ecef =  z_eci
```

### 4. WGS84 Geodetic Conversion

The Earth is modeled with WGS84 parameters:

```text
a = 6378.137 km
f = 1 / 298.257223563
e^2 = f * (2 - f)
```

These are used to convert ECEF coordinates into latitude, longitude, and
altitude.

### 5. Orbital Elements From State Vectors

The generator derives classical orbital elements from position and velocity:

```text
h = r x v
n = k x h
e_vec = (v x h) / mu - r / |r|
energy = |v|^2 / 2 - mu / |r|
a = -mu / (2 * energy)
```

where:

- `h` is specific angular momentum.
- `e_vec` is the eccentricity vector.
- `mu = 398600.4418 km^3/s^2` is Earth's gravitational parameter.
- `a` is semi-major axis.

Argument of latitude is:

```text
u = arg_perigee + true_anomaly
```

This is used to order satellites along the same orbital plane.

### 6. Distance Between Nodes

For two nodes with position vectors `r1` and `r2`:

```text
d = |r2 - r1|
```

where `d` is in kilometers.

### 7. Line of Sight

For an inter-satellite segment:

```text
p(t) = r1 + t * (r2 - r1), 0 <= t <= 1
```

The closest point on the segment to Earth center is checked. A link is allowed
only if the segment does not intersect Earth.

### 8. Propagation Delay

Per-edge propagation delay is:

```text
prop_delay_ms = (distance_km / c_km_s) * 1000
```

where:

```text
c_km_s = 299792.458 km/s
```

This is one-way vacuum speed-of-light delay.

### 9. Ground Station Elevation

For a gateway and satellite:

```text
los = sat_ecef - gateway_ecef
elevation = asin((los dot up) / |los|)
```

The access link is accepted only if elevation is above the configured minimum.

### 10. Shortest-Delay Routing

The simulator and visualizer use Dijkstra's algorithm with propagation delay
as edge weight:

```text
path = argmin sum(edge_prop_delay_ms)
```

ns-3 interface metrics are set proportional to delay so global routing follows
the minimum-delay path instead of the minimum-hop path.

### 11. Serialization Delay

In the visualizer:

```text
serialization_ms = (packet_bytes * 8 / link_rate_bps) * 1000
```

### 12. Queueing Approximation

The visualizer uses a simple M/M/1-style approximation:

```text
rho = offered_bps / link_rate_bps
queue_ms = (rho / (1 - rho)) * serialization_ms
```

The result is capped by the configured finite queue size:

```text
queue_ms <= queue_packets * serialization_ms
```

This is an approximation for teaching and exploration, not a full queueing
simulation.

### 13. TCP Goodput

For a flow in ns-3:

```text
goodput_mbps = (rx_bytes * 8) / active_duration_seconds / 1e6
```

### 14. TCP Byte Efficiency

The project reports TCP byte efficiency as:

```text
tcp_byte_efficiency_percent = 100 * rx_bytes / tx_bytes
```

This is not the same as packet delivery probability. TCP can retransmit bytes,
so application-level delivery may still succeed even when extra bytes are sent.

### 15. Retransmission Overhead

```text
tcp_retrans_overhead_percent =
    100 * max(tx_bytes - rx_bytes, 0) / rx_bytes
```

This estimates how much extra TCP/IP traffic was transmitted relative to
delivered bytes.

### 16. Aggregate Capacity Indicator

The run metadata includes:

```text
aggregate_cap_utilization_percent =
    100 * aggregate_tx_load_mbps / installed_capacity_mbps
```

This is only a coarse network-wide indicator. Per-link utilization is more
important for finding bottlenecks.

### 17. Wang BPP Multi-tier Reliability

For tiers indexed 1..K with R_i the orbital radius, N_i the number of
relays in tier i, and threshold parameters theta_r (max direction angle),
theta_s (min dome angle), d_th (max reliable distance):

```text
theta_{i,j} = max(theta_s, min(arccos((R_i^2 + R_j^2 - d_th^2) / (2 R_i R_j)),
                                arccos(R_1/R_i) + arccos(R_1/R_j)))            (eq. 1)

P^I_{i,j}   = (1 - (theta_r / (4*pi)) * (cos(theta_s) - cos(theta_{i,j})))^N    (eq. 2)
              N = N_j   if i != j
              N = N_i-1 if i == j

P^S_i       = prod_j P^I_{i,j}                                                  (eq. 3)
mu_i        = 1 + sum_j T~^(2)_{i,j} * mu_j   on transient states               (eq. 4)
N_h         = round(theta_m / theta_o)                                          (eq. 5)
P~^M        = e_1 * (T~^(2))^(N_h - 2) * T^^(3) * e_{K+1}^T                     (eq. 7)
```

Where T^(1), T~^(2), T^^(3) are the priority-strategy-conditioned
transition matrices from Wang algorithms 1, 2, 3, and v is the stationary
distribution of T^(1). The empirical interruption probability is the
fraction of simulated (src, dst) routes whose Wang-style greedy forwarding
fails before reaching the receiver. Comparison metrics:

```text
absolute_error = |P_empirical - P~^M|
relative_error = absolute_error / P_empirical   (when P_empirical > 0)
```

## Limitations

- No real Starlink routing policy is known or modeled.
- No real beam scheduling or phased-array radio model is included.
- No dynamic topology changes during an ns-3 run.
- Built-in gateways are demo placeholders.
- Weather, obstruction, spectrum sharing, and real user terminal behavior are outside the scope.

## Recommended Run

Research-grade comparison of the BPP analytical model against real Starlink
TLE snapshots, sampled over roughly one orbital period:

```bash
tools/generate_snapshot.sh --no_live --n 1600 --sample random --seed 1 \
    --starlink_operational --shell_select largest \
    --epoch_steps 10 --multi_epoch_seconds 540
python tools/analyze_mhr_reliability.py --pairs 200 --seed 1
python tools/plot_mhr_reliability.py
```

Quick sanity check (single epoch, ~400 satellites, no orbital sweep):

```bash
tools/generate_snapshot.sh --no_live --n 400 --sample random --seed 1 --starlink_operational
python tools/analyze_mhr_reliability.py --pairs 100 --seed 1
python tools/plot_mhr_reliability.py
```

Optional ns-3 traffic simulation over the same snapshot:

```bash
tools/run_ns3_snapshot.sh
python tools/plot_flow_metrics.py
```

Exploratory multi-shell run that may fragment:

```bash
tools/generate_snapshot.sh --n 400 --sample random --seed 1 --starlink_operational --shell_select none --no_strict
```

## Limitations Of The Comparison

- The Wang model assumes a homogeneous spherical binomial point process per
  tier. Real Starlink uses inclined Walker constellations, so the empirical
  geometry deviates systematically (larger gaps near the equator and seam).
- The empirical simulator relaxes the direction angle (`theta_r`) and
  minimum dome angle (`theta_s`) only on uplink/downlink hops, because a
  ground gateway has every satellite within ~22 degrees of dome and the
  paper's constraints would forbid every uplink. Hops between satellites
  use the unrelaxed Wang constraints (c1, c2, c3).
- The built-in gateways are 10 demo placeholders. For a quantitative study
  of ground-tier coverage, supply real gateway locations through
  `--gateways_csv`.
- The empirical comparison uses geometric forwarding (Wang-style greedy),
  not the precomputed ISL graph from the snapshot. This is intentional, so
  that the empirical and analytical models share the same constraint set.
