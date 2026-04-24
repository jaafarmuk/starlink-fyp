# Starlink FYP: LEO Network Snapshot and ns-3 Simulation

This project builds a physically plausible Starlink-like low Earth orbit
network snapshot from TLE data, converts that snapshot into a network topology,
and runs traffic simulations over it with ns-3. It also includes a browser
visualizer for inspecting satellites, gateways, links, routes, and approximate
per-hop delay.

The project is not a reverse-engineered model of the real Starlink production
network. Real Starlink ISL scheduling, gateway/PoP choices, beam assignment,
capacity sharing, and routing policies are proprietary. This project uses
public TLE data plus documented geometric and network-simulation assumptions.

## What The Project Does

1. Reads Starlink TLE records from `datasets/starlink.tle`.
2. Optionally filters the TLE set to a more operational-looking Starlink subset.
3. Propagates satellites to one common epoch using SGP4.
4. Converts orbital positions into ECI/ECEF/geodetic coordinates.
5. Groups satellites into shells and planes.
6. Builds inter-satellite links and gateway access links.
7. Writes snapshot files in `results/`.
8. Runs a frozen-time ns-3 TCP simulation over the generated topology.
9. Provides a Three.js visualizer for interactive inspection.

## Main Files

- `tools/tle_to_snapshot.py`: TLE parser, satellite propagation, shell/plane grouping, link generation, validation, and CSV/JSON output.
- `src/starlink-snapshot.cc`: ns-3 scenario that reads the snapshot and simulates TCP flows.
- `tools/visualizer/index.html` and `tools/visualizer/app.js`: interactive 3D topology and packet/path visualizer.
- `tools/generate_snapshot.sh`: wrapper for generating snapshot files.
- `tools/run_ns3_snapshot.sh`: wrapper for running the ns-3 scenario.
- `tools/run_visualizer.sh`: serves the browser visualizer locally.
- `tools/plot_flow_metrics.py`: plots per-flow ns-3 metrics.
- `results/`: generated snapshot and simulation output files.

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

### 2. Generate A Snapshot

The recommended command filters the raw TLE file to a cleaner operational
Starlink-like subset:

```bash
tools/generate_snapshot.sh --n 400 --sample random --seed 1 --starlink_operational
```

This creates:

```text
results/snapshot_nodes.csv
results/snapshot_edges.csv
results/snapshot_meta.json
results/topology_stats.csv
```

### 3. Run With ns-3

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

### 4. Run The Visualizer

Serve the repository locally:

```bash
tools/run_visualizer.sh
```

Then open:

```text
http://localhost:8000/tools/visualizer/
```

The visualizer reads the generated `results/snapshot_nodes.csv`,
`results/snapshot_edges.csv`, and `results/snapshot_meta.json`.

### 5. Plot ns-3 Flow Metrics

After running ns-3:

```bash
python tools/plot_flow_metrics.py
```

## Generated Outputs

- `results/snapshot_nodes.csv`: satellite and gateway nodes.
- `results/snapshot_edges.csv`: ISL and ground access links.
- `results/snapshot_meta.json`: generator settings, schema, filters, caveats, and validation.
- `results/topology_stats.csv`: connectivity and distance summary.
- `results/per_flow_metrics.csv`: ns-3 per-flow results.
- `results/run_meta.json`: ns-3 run metadata.
- `results/flowmon.xml`: ns-3 FlowMonitor output.

## Important Modeling Notes

- The topology is a frozen-time snapshot. Satellite movement during the ns-3 run is not modeled.
- Built-in gateways are demo locations named `GW-DEMO-*`, not official Starlink gateway sites.
- Edge delay is one-way vacuum propagation delay, not real ping latency.
- The default traffic pattern is gateway-to-gateway because random satellite-to-satellite user flows are not realistic for Starlink user traffic.
- The visualizer is educational. Its queueing and retransmission behavior are simplified and should not replace ns-3 results.
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

## Limitations

- No real Starlink routing policy is known or modeled.
- No real beam scheduling or phased-array radio model is included.
- No dynamic topology changes during an ns-3 run.
- Built-in gateways are demo placeholders.
- Weather, obstruction, spectrum sharing, and real user terminal behavior are outside the scope.

## Recommended Run For Cleaner Results

For a more reasonable demo snapshot:

```bash
tools/generate_snapshot.sh --n 400 --sample random --seed 1 --starlink_operational
tools/run_ns3_snapshot.sh
python tools/plot_flow_metrics.py
```

For exploratory runs that may produce fragmented or unrealistic topologies:

```bash
tools/generate_snapshot.sh --n 200 --sample random --seed 1 --no_strict
```
