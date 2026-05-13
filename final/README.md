# Final Experiment Code

This folder is now organized around five experiment scripts:

## 1. Paper Equation

Analytical interruption probability from the paper.

```bash
python3 final/paper_equation.py
python3 final/paper_equation.py --strategy 3,2,1
```

## 2. Greedy on Random Points

Randomly placed gateways and satellites, then greedy routing with paper-style
constraints.

```bash
python3 final/greedy_random.py --pairs 300
```

## 3. Greedy on Starlink-Like Snapshot

Snapshot built from real orbital data, then greedy routing with paper-style
constraints.

```bash
python3 final/build_snapshot.py
python3 final/greedy_sats.py --pairs 300
```

## 4. Dijkstra on Random Points

This uses a relaxed first hop, then paper-style constraints after that.

```bash
python3 final/dijkstra_random.py --pairs 300
```

## 5. Dijkstra on Starlink-Like Snapshot

This uses a relaxed first hop, then paper-style constraints after that.

```bash
python3 final/dijkstra_sats.py --pairs 300
```

For an upper-bound comparison, run Dijkstra with all generated snapshot edges
allowed and no extra paper-style constraints:

```bash
python3 final/dijkstra_sats.py --pairs 300 --relaxed
```

## Support Files

- `base.py`: shared geometry, graph, and random-topology helpers
- `build_snapshot.py`: generates the local Starlink-like snapshot
- `empirical.py`: shared greedy-routing logic
- `dijkstra.py`: shared Dijkstra logic

## One Command for Everything

To run the analytical result plus the random-topology and Starlink-like
experiments together and print one comparison table, including the relaxed
Starlink Dijkstra upper bound:

```bash
python3 final/run_all.py --pairs 300
```
