HOW TO RUN
----------

1. Set up Python environment

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

   On macOS with Homebrew Python:

    /opt/homebrew/bin/python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt


2. Generate the network snapshot

    bash tools/generate_snapshot.sh

   Reads the TLE file and gateway CSV from datasets/ and writes the
   following to results/:

    snap_optA_nodes.csv          all 4080 satellite + 250 gateway nodes
    snap_optA_edges.csv          ISL and access links for epoch 0
    snap_optA_meta.json          epoch/shell/sampling metadata
    snap_optA_stats.csv          per-shell topology statistics
    snap_optA_nodes.t1–t9.csv    node positions for epochs 1–9
    snap_optA_edges.t1–t9.csv    edge sets for epochs 1–9

   Takes a few minutes. Run once; the outputs are reused by everything else.


3. Run the full analysis sweep

    bash tools/run_full_analysis.sh

   Runs BPP analytical + empirical greedy + Dijkstra routing on the full
   4080-satellite snapshot, sweeping d_th = 3000, 3500, 4000, 5000 km.
   Outputs go to:

    results/analysis/full/

   To run a single configuration instead:

    python3 tools/analyze_mhr_reliability.py \
      --nodes results/snap_optA_nodes.csv \
      --edges results/snap_optA_edges.csv \
      --meta  results/snap_optA_meta.json \
      --d_th_km 4000 --pairs 300 \
      --summary_json results/mhr_summary.json \
      --summary_csv  results/mhr_summary.csv


4. Generate plots and tables

    python3 tools/make_plots.py
    python3 tools/make_tables.py

   Outputs go to results/plots/ and results/tables/.


5. Run the visualizer

    bash tools/run_visualizer.sh

   Then open http://localhost:8000/tools/visualizer/ in a browser.
   Requires WebGL. The visualizer loads the snapshot files from results/
   automatically (click "Load snapshot" in the UI if needed).

   Features: 3D globe, Dijkstra and BPP greedy routing, packet animation,
   live background traffic, congestion heat map, per-hop delay breakdown.


6. NS-3 simulation (optional)

   Requires NS-3 installed separately (not via pip).

    bash tools/run_ns3_snapshot.sh
    python3 tools/plot_flow_metrics.py

   Runs the C++ flow-level simulation and plots per-flow throughput/latency.


-----------------------------------------------------------------------
FILES
-----------------------------------------------------------------------

datasets/
  starlink.tle
    TLE file containing Starlink satellite orbital elements. Used by
    tle_to_snapshot.py to propagate positions to a common epoch.

  starlink_ground_stations_hf_operational_safe.csv
    250 real Starlink gateway/ground-station locations used when building
    the access-link layer of the snapshot.

tools/
  tle_to_snapshot.py
    Propagates TLE data to a common UTC epoch, groups satellites into
    orbital shells, builds a heuristic ISL graph (same-shell near-neighbour
    planes, line-of-sight), attaches gateway access links, and writes the
    nodes/edges/meta CSV+JSON files. Supports multi-epoch output.

  generate_snapshot.sh
    Wrapper that calls tle_to_snapshot.py with the parameters used for the
    reported snap_optA dataset (4080 sats, 250 gateways, 10 epochs).

  analyze_mhr_reliability.py
    Core analysis script. Implements the BPP analytical model from Wang,
    Kishk & Alouini (arXiv:2303.02286) and runs the empirical Wang-style
    greedy routing and Dijkstra baseline on the same snapshot. Outputs
    interruption probability, hop count, and latency statistics.

  run_full_analysis.sh
    Runs analyze_mhr_reliability.py across both snapshot sizes and all
    four d_th values used in the report.

  make_plots.py
    Reads the JSON outputs from the analysis sweep and produces the five
    comparison plots: interruption vs d_th, BPP error gap, latency CDFs,
    hop-count distribution, per-epoch breakdown.

  make_tables.py
    Reads the same JSON outputs and writes markdown comparison tables.

  plot_mhr_reliability.py
    Standalone plot script for a single analysis run (legacy, kept for
    quick inspection of one result file).

  plot_flow_metrics.py
    Reads the CSV output of the NS-3 simulation and plots per-flow
    throughput, latency, and goodput.

  run_visualizer.sh
    Starts a local HTTP server and prints the visualizer URL.

  run_ns3_snapshot.sh
    Runs the NS-3 starlink-snapshot scenario against the current results/
    snapshot and writes flow metrics to results/.

  visualizer/
    index.html   UI shell: controls panel, 3D canvas, packet stats overlay.
    app.js       All visualizer logic: Three.js 3D rendering, Dijkstra and
                 BPP greedy routing, packet animation, EMA traffic engine,
                 M/D/1 queueing model, gateway access link capacity model.

src/
  starlink-snapshot.cc
    NS-3 C++ scenario. Reads the snapshot CSV files, builds a point-to-point
    topology, injects UDP/TCP flows, and writes per-flow metrics to CSV.

results/
  Empty at clone time (contains only .gitkeep).
  Populated by generate_snapshot.sh, run_full_analysis.sh, make_plots.py,
  and make_tables.py as described above.

report/
  main.tex     LaTeX source for the FYP report (compile with pdflatex).

requirements.txt
  Python dependencies: skyfield, numpy, scipy, pandas, matplotlib,
  networkx, sgp4. Install once with: pip install -r requirements.txt


-----------------------------------------------------------------------
REQUIREMENTS
-----------------------------------------------------------------------

  Python 3.8+
  pip install -r requirements.txt
  Browser with WebGL support (for the visualizer)
  NS-3 (optional, only needed for src/starlink-snapshot.cc)
