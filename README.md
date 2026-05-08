# Starlink FYP

## How To Run

### 1. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On macOS, if the default `python3` is not usable, try:

```bash
/opt/homebrew/bin/python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate the Starlink snapshot

```bash
bash tools/generate_snapshot.sh
```

This reads:

```text
datasets/starlink.tle
datasets/starlink_ground_stations_hf_operational_safe.csv
```

and writes:

```text
results/snap_optA_nodes.csv
results/snap_optA_edges.csv
results/snap_optA_meta.json
results/snap_optA_stats.csv
results/snap_optA_nodes.t1.csv ... t9.csv
results/snap_optA_edges.t1.csv ... t9.csv
```

The main snapshot has 4,080 satellites and 250 gateways.

### 3. Run one analysis

```bash
python3 tools/analyze_mhr_reliability.py \
  --nodes results/snap_optA_nodes.csv \
  --edges results/snap_optA_edges.csv \
  --meta  results/snap_optA_meta.json \
  --d_th_km 4000 \
  --pairs 300 \
  --summary_json results/mhr_optA_summary.json \
  --summary_csv  results/mhr_optA_summary.csv
```

Important parameters:

| Parameter | Meaning |
|---|---|
| `--d_th_km` | Maximum allowed hop distance, in km |
| `--theta_r` | Maximum direction angle, default `pi/6` |
| `--theta_s` | Minimum dome angle, default `pi/10` |
| `--pairs` | Source-destination pairs tested per epoch |
| `--endpoint_kind` | `auto`, `gateway`, or `satellite` |

### 4. Run the full experiment sweep

```bash
bash tools/run_full_analysis.sh
```

This runs both:

```text
full constellation: 4,080 satellites + 250 gateways
n600 subsample:     600 satellites + 250 gateways
```

for:

```text
d_th = 3000, 3500, 4000, 5000 km
```

Outputs are written under:

```text
results/analysis/full/
results/analysis/n600/
```

### 5. Make plots and tables

```bash
python3 tools/make_plots.py
python3 tools/make_tables.py
```

Outputs:

```text
results/plots/
results/tables/
```

### 6. Start the visualizer

```bash
python3 -m http.server 8080 --directory .
```

Open:

```text
http://localhost:8080/tools/visualizer/
```

The visualizer lets you:

- load 500, 1000, 2000, or all 4,080 satellites
- pick source and destination nodes
- send packets using Dijkstra or the Wang/BPP greedy route
- turn background traffic on or off
- see delay, queueing, packet loss, throughput, hop count, and link heat

The visualizer traffic model is a simplified simulator. It shows realistic
network effects, but it is not a proprietary Starlink SLA or capacity model.

### 7. Optional NS-3 run

If NS-3 is installed:

```bash
bash tools/run_ns3_snapshot.sh
python3 tools/plot_flow_metrics.py
```

## What We Are Doing

In simple terms:

1. We take real Starlink satellite positions from TLE files.
2. We build a frozen network snapshot from those positions.
3. We run the paper's greedy routing rule on the real geometry.
4. We compute the paper's BPP mathematical prediction.
5. We compare the predicted interruption probability with the measured one.
6. We also compare greedy routing with Dijkstra shortest-path routing.

The main question is:

> Does the BPP analytical model predict routing failures correctly when the
> satellites are real Starlink satellites instead of randomly scattered points?

The paper being tested is Wang, Kishk, and Alouini, "Reliability Analysis of
Multi-hop Routing in Multi-tier LEO Satellite Networks", arXiv:2303.02286.

The answer from our experiments is mostly no. The BPP formula works under its
own random-placement assumptions, but real Starlink satellites are arranged in
structured Walker-delta orbital planes. That structure creates directional gaps
that greedy geographic routing often cannot escape.

## Main Data

Main full snapshot:

| Item | Value |
|---|---:|
| Satellites | 4,080 |
| Gateways | 250 |
| Total nodes | 4,330 |
| Base epoch | 2026-03-21 19:20:15 UTC |
| Epoch count | 10 |
| Epoch spacing | 9 minutes |
| Satellite shells used in analysis | 2 satellite tiers + gateway tier |
| Main shell inclinations | about 43 deg and 53 deg |

The generator builds a heuristic ISL/access graph. It is physically plausible,
not a claim about SpaceX's private operational routing or gateway scheduling.

## Equations Used

### 1. Distance

For two nodes with position vectors `r_a` and `r_b`:

```math
d(a,b) = ||r_b - r_a||
```

### 2. Propagation delay

```math
t_prop = d / c
```

where:

```text
c = 299792.458 km/s
```

### 3. Dome angle

The dome angle is the angle between two geocentric position vectors:

```math
\psi(a,b) =
\arccos \left(
  {r_a \cdot r_b \over ||r_a|| ||r_b||}
\right)
```

### 4. Direction angle

At current node `A`, candidate relay `B`, and final destination `D`:

```math
\alpha(A,B,D) =
\arccos \left(
  {(r_B-r_A) \cdot (r_D-r_A)
   \over
   ||r_B-r_A|| ||r_D-r_A||}
\right)
```

The Wang greedy route requires:

```math
\alpha \le \theta_r
```

### 5. Line of sight

For segment `p -> q`, find the closest point on the segment to Earth's center.
There is line of sight if:

```math
\min_{0 \le t \le 1} ||p + t(q-p)|| \ge R_E
```

where `R_E` is the Earth radius used for the line-of-sight check.

### 6. Maximum allowed dome angle between tiers

For tier `i` with radius `R_i` and tier `j` with radius `R_j`:

```math
\theta_{i,j}
=
\max \left(
  \theta_s,
  \min \left[
    \arccos \left(
      {R_i^2 + R_j^2 - d_th^2 \over 2 R_i R_j}
    \right),
    \arccos \left({R_E \over R_i}\right)
    +
    \arccos \left({R_E \over R_j}\right)
  \right]
\right)
```

This combines the maximum distance constraint and Earth blockage.

### 7. Single tier-to-tier interruption probability

For a transmitter in tier `i`, the probability that no relay is available in
tier `j` is:

```math
P^I_{i,j}
=
\left[
  1 -
  {\theta_r \over 4\pi}
  \left(\cos\theta_s - \cos\theta_{i,j}\right)
\right]^{N_j}
```

For `i = j`, the transmitter itself is excluded:

```math
P^I_{i,i}
=
\left[
  1 -
  {\theta_r \over 4\pi}
  \left(\cos\theta_s - \cos\theta_{i,i}\right)
\right]^{N_i-1}
```

### 8. Single-hop total interruption probability

The probability that tier `i` finds no relay in any tier is:

```math
P^S_i = \prod_{j=1}^{K} P^I_{i,j}
```

### 9. Priority transition matrix

A priority strategy `s` ranks tiers. Smaller `s_j` means higher priority.

For a hop starting in tier `i`, the transition probability to tier `j` is:

```math
T^{(1)}_{i,j}
=
{
  (1-P^I_{i,j})
  \prod_{k:s_k<s_j} P^I_{i,k}
  \over
  1-P^S_i
}
```

This means:

1. tier `j` must have an available relay
2. every higher-priority tier must have no available relay
3. the result is normalized by the probability that at least one tier works

### 10. Stationary distribution

The stationary distribution `v` satisfies:

```math
v T^{(1)} = v
```

and:

```math
\sum_i v_i = 1
```

It estimates how often the route is expected to be in each tier.

### 11. Expected hops before interruption

Using the augmented transition matrix `\tilde T^{(2)}` with an absorbing
interruption state:

```math
\mu_i = 1 + \sum_j \tilde T^{(2)}_{i,j} \mu_j
```

In matrix form over transient states:

```math
\mu = (I - Q)^{-1} 1
```

### 12. Average dome progress per hop

The paper approximates the expected progress using a Wallis-style product:

```math
p_N = \prod_{k=1}^{N} {2k-1 \over 2k}
```

For a hop from tier `i` to tier `j`:

```math
\theta_{o,i,j}
=
\arccos \left(
  {2\pi \over \theta_r}
  -
  {2\pi \over \theta_r}\cos(\pi p_N)
  +
  \cos\theta_{i,j}
\right)
```

Then:

```math
\theta_o =
\sum_i v_i \sum_j T^{(1)}_{i,j} \theta_{o,i,j}
```

### 13. Average number of hops for a successful route

```math
N_h = round \left( {\theta_m \over \theta_o} \right)
```

where `\theta_m` is the source-destination dome angle.

### 14. Multi-hop interruption probability

The paper's approximate multi-hop interruption probability is:

```math
\tilde P^M
=
e_src
\left(\tilde T^{(2)}\right)^{N_h-2}
\hat T^{(3)}
e_{K+1}^T
```

where:

- `e_src` starts the route in the source tier
- `\tilde T^{(2)}` includes the absorbing interruption state
- `\hat T^{(3)}` handles the final-hop logic
- `e_{K+1}` extracts the interruption state

### 15. Empirical greedy routing rules

At each hop, the greedy Wang-style simulator checks candidates using:

```math
c1: \alpha \le \theta_r
```

```math
c2: \psi \ge \theta_s
```

```math
c3: d \le d_th \quad \text{and line of sight exists}
```

Among valid candidates, it chooses the candidate with the smallest dome angle
to the final destination. If no candidate exists, the route is interrupted.

Gateway uplink/downlink hops are relaxed in the implementation because the
paper's pure satellite geometry constraints otherwise reject practical gateway
hops too aggressively.

### 16. Dijkstra baseline

Dijkstra uses the generated ISL/access graph and minimizes path cost:

```math
\min_P \sum_{e \in P} t_e
```

For propagation-only routing:

```math
t_e = {d_e \over c}
```

In the visualizer, delay can also include serialization and queueing.

### 17. Serialization delay

For packet size `B` bytes and link rate `C` bits/s:

```math
T_s = {8B \over C}
```

### 18. M/D/1 queueing delay

The queueing estimate is:

```math
W_q = {\rho \over 2(1-\rho)} T_s
```

where:

```math
\rho = {offered\_load \over link\_rate}
```

### 19. Visualizer live traffic load

For each edge, the visualizer keeps an exponential moving estimate of recent
offered bits:

```math
recentBits_e(t+\Delta t)
=
recentBits_e(t)\exp(-\Delta t/\tau)
+ addedBits_e
```

Then:

```math
\rho_e =
{recentBits_e \over \tau C_e}
```

In the visualizer, `C_e` is the selected link rate.

### 20. Visualizer congestion loss curve

The visualizer uses a simple loss curve:

```math
P_{loss}(\rho) =
\begin{cases}
0, & \rho < 0.65 \\
0.02{\rho-0.65 \over 0.20}, & 0.65 \le \rho < 0.85 \\
0.02 + 0.06{\rho-0.85 \over 0.15}, & 0.85 \le \rho \le 1
\end{cases}
```

This is for interactive simulation only.

### 21. Goodput and jitter

Goodput over a time window is:

```math
goodput = {delivered\_bits \over window\_seconds}
```

Jitter is approximated from successive delivered packet delays:

```math
jitter =
{1 \over n-1}
\sum_{k=2}^{n}
|delay_k - delay_{k-1}|
```

## Results

### Main interruption probability

Mean over 10 epochs, 300 source-destination pairs per epoch, density strategy.

| Dataset | `d_th` km | Greedy empirical | BPP analytical | Dijkstra | BPP error |
|---|---:|---:|---:|---:|---:|
| full | 3000 | 0.8743 | 0.0160 | 0.0000 | 0.8584 |
| full | 3500 | 0.8490 | 0.0144 | 0.0000 | 0.8346 |
| full | 4000 | 0.7857 | 0.0144 | 0.0000 | 0.7712 |
| full | 5000 | 0.7443 | 0.0144 | 0.0000 | 0.7299 |
| n600 | 3000 | 0.9687 | 0.9864 | 0.0000 | 0.0178 |
| n600 | 3500 | 0.9250 | 0.7850 | 0.0000 | 0.1400 |
| n600 | 4000 | 0.9063 | 0.6145 | 0.0000 | 0.2919 |
| n600 | 5000 | 0.7897 | 0.5399 | 0.0000 | 0.2498 |

Main reading:

- On the full 4,080-satellite geometry, BPP predicts about 1.4 percent
  interruption at `d_th = 4000 km`.
- The empirical greedy route interrupts about 78.6 percent of the time.
- Dijkstra succeeds on the generated graph for the tested pairs.

### Hop count at `d_th = 4000 km`

Successful routes only.

| Dataset | Algorithm | Mean | P50 | P90 | P95 | P99 |
|---|---|---:|---:|---:|---:|---:|
| full | Greedy | 8.11 | 8.00 | 8.30 | 8.79 | 9.77 |
| full | Dijkstra | 12.65 | 12.40 | 16.80 | 18.21 | 20.60 |
| n600 | Greedy | 9.08 | 8.80 | 10.30 | 10.34 | 10.96 |
| n600 | Dijkstra | 8.51 | 8.30 | 10.00 | 10.50 | 11.00 |

### Route distance at `d_th = 4000 km`

Successful routes only.

| Dataset | Algorithm | Mean km | P50 | P90 | P95 | P99 |
|---|---|---:|---:|---:|---:|---:|
| full | Greedy | 20,943 | 20,565 | 22,211 | 23,438 | 26,574 |
| full | Dijkstra | 20,046 | 19,946 | 20,860 | 21,084 | 21,286 |
| n600 | Greedy | 22,637 | 22,095 | 25,388 | 25,948 | 27,186 |
| n600 | Dijkstra | 20,471 | 20,375 | 21,387 | 21,558 | 21,797 |

### End-to-end latency at `d_th = 4000 km`

This includes propagation plus the analysis queueing model used in the run.
Successful routes only.

| Dataset | Algorithm | Mean ms | P50 | P90 | P95 | P99 |
|---|---|---:|---:|---:|---:|---:|
| full | Greedy | 69.87 | 68.61 | 74.10 | 78.19 | 88.66 |
| full | Dijkstra | 66.89 | 66.55 | 69.60 | 70.35 | 71.02 |
| n600 | Greedy | 75.52 | 73.72 | 84.70 | 86.57 | 90.70 |
| n600 | Dijkstra | 68.30 | 67.98 | 71.35 | 71.92 | 72.72 |

### Strategy comparison at `d_th = 4000 km`

| Dataset | Strategy | Greedy empirical | BPP predicted | Dijkstra | BPP error |
|---|---|---:|---:|---:|---:|
| full | density | 0.7857 | 0.0144 | 0.0000 | 0.7712 |
| full | single_hop | 0.7857 | 0.0144 | 0.0000 | 0.7712 |
| full | stationary_optimal | 0.7857 | 0.0144 | 0.0000 | 0.7712 |
| n600 | density | 0.9063 | 0.6145 | 0.0000 | 0.2919 |
| n600 | single_hop | 0.8640 | 0.6319 | 0.0000 | 0.2321 |
| n600 | stationary_optimal | 0.9063 | 0.6145 | 0.0000 | 0.2919 |

## Main Conclusion

The BPP model is mathematically valid for randomly placed satellites. But real
Starlink satellites are not randomly placed; they are arranged in structured
orbital planes.

For the full Starlink-like snapshot:

- BPP says greedy routing should almost always work.
- The empirical greedy route fails often.
- Dijkstra on the generated graph shows that the geometry is still routable
  when global topology information is used.

So the main result is:

> The paper's BPP formula is not a reliable predictor for Wang-style greedy
> routing on real Walker-delta Starlink geometry.

## Important Caveats

- The real Starlink network is proprietary.
- The ISL and gateway graph here is a geometric heuristic.
- Dijkstra is a baseline on our generated graph, not a claim about SpaceX's
  real routing protocol.
- The visualizer traffic model is simplified and calibrated for realistic
  network effects, not exact Starlink SLA prediction.
- The strongest scientific comparison is between the BPP analytical prediction
  and empirical Wang-style greedy routing on the same TLE geometry.

## Repository Layout

```text
datasets/
  starlink.tle
  starlink_ground_stations_hf_operational_safe.csv

tools/
  tle_to_snapshot.py
  analyze_mhr_reliability.py
  run_full_analysis.sh
  make_plots.py
  make_tables.py
  visualizer/
    index.html
    app.js

results/
  snap_optA_*.csv/json
  analysis/
  plots/
  tables/

src/
  starlink-snapshot.cc
```

## Requirements

- Python 3.8+
- `pip install -r requirements.txt`
- Browser with WebGL for the visualizer
- NS-3 only if running the optional C++ simulation
