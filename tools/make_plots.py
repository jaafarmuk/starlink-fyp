"""Generate comparison plots from MHR analysis results.

Reads the d_th sweep JSON outputs in results/analysis/full/ and produces:

  results/plots/01_interruption_vs_dth.png   - greedy / Dijkstra / BPP across d_th
  results/plots/02_latency_distribution.png  - greedy vs Dijkstra latency CDFs
  results/plots/03_hop_distribution.png      - greedy vs Dijkstra hop counts
  results/plots/04_per_epoch_interruption.png - per-epoch breakdown
  results/plots/05_strategy_comparison.png   - density / single_hop / stat_opt
"""
from __future__ import annotations
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


REPO = Path(__file__).resolve().parent.parent
ANALYSIS = REPO / "results" / "analysis" / "full"
PLOTS = REPO / "results" / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

D_THS = [3000, 3500, 4000, 5000]


def load(dth: int) -> dict:
    p = ANALYSIS / f"dth{dth}_summary.json"
    with open(p) as f:
        return json.load(f)


def density_rows(summary: dict) -> list:
    return [r for r in summary["rows"] if r["strategy"] == "density"]


def mean(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None and not (
        isinstance(r[key], float) and np.isnan(r[key]))]
    return float(np.mean(vals)) if vals else float("nan")


# -----------------------------------------------------------------
# Plot 1: Interruption probability vs d_th
# -----------------------------------------------------------------
def plot_interruption_vs_dth():
    emp, bpp, dij = [], [], []
    for dth in D_THS:
        try:
            d = load(dth)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        emp.append(mean(r, "empirical_interruption_probability"))
        bpp.append(mean(r, "bpp_predicted_interruption_probability"))
        dij.append(mean(r, "dijkstra_interruption_probability"))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(D_THS, emp, "o-", lw=2, color="#d62728", label="Greedy (Wang) empirical")
    ax.plot(D_THS, bpp, "s--", lw=2, color="#2ca02c", label="BPP analytical")
    ax.plot(D_THS, dij, "^-", lw=2, color="#1f77b4", label="Dijkstra empirical")
    ax.set_xlabel("d_th (km)")
    ax.set_ylabel("Interruption probability")
    ax.set_ylim(-0.05, 1.05)
    ax.set_title("Interruption probability vs hop-distance limit (d_th)\nFull Walker-delta (4,080 sats)")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out = PLOTS / "01_interruption_vs_dth.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 2: Latency distribution at d_th=4000
# -----------------------------------------------------------------
def plot_latency_distribution():
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    r = density_rows(d)
    labels = ["mean", "p50", "p90", "p95", "p99"]
    gr = [mean(r, f"greedy_total_ms_{l}") for l in labels]
    dj = [mean(r, f"dijkstra_total_ms_{l}") for l in labels]
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - 0.2, gr, 0.4, label="Greedy (Wang)", color="#d62728")
    ax.bar(x + 0.2, dj, 0.4, label="Dijkstra", color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels([l.upper() for l in labels])
    ax.set_ylabel("End-to-end latency (ms, prop+queue)")
    ax.set_title("Latency distribution at d_th=4000 km — successful routes only\nFull Walker-delta (4,080 sats)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = PLOTS / "02_latency_distribution.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 3: Hop count distribution at d_th=4000
# -----------------------------------------------------------------
def plot_hop_distribution():
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    r = density_rows(d)
    labels = ["mean", "p50", "p90", "p95", "p99"]
    gr = [mean(r, f"greedy_hops_{l}") for l in labels]
    dj = [mean(r, f"dijkstra_hops_{l}") for l in labels]
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - 0.2, gr, 0.4, label="Greedy (Wang)", color="#d62728")
    ax.bar(x + 0.2, dj, 0.4, label="Dijkstra", color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels([l.upper() for l in labels])
    ax.set_ylabel("Hop count")
    ax.set_title("Hop count distribution at d_th=4000 km — successful routes only\nFull Walker-delta (4,080 sats)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = PLOTS / "03_hop_distribution.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 4: Per-epoch breakdown at d_th=4000
# -----------------------------------------------------------------
def plot_per_epoch_interruption():
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    rows = density_rows(d)
    rows.sort(key=lambda r: r["epoch_step"])
    steps = [r["epoch_step"] for r in rows]
    emp = [r["empirical_interruption_probability"] for r in rows]
    bpp = [r["bpp_predicted_interruption_probability"] for r in rows]
    dij = [r["dijkstra_interruption_probability"] for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(steps, emp, "o-", color="#d62728", label="Greedy (Wang)")
    ax.plot(steps, bpp, "s--", color="#2ca02c", label="BPP analytical")
    ax.plot(steps, dij, "^-", color="#1f77b4", label="Dijkstra")
    ax.set_xlabel("Epoch (9-min steps)")
    ax.set_ylabel("Interruption probability")
    ax.set_ylim(-0.05, 1.05)
    ax.set_title("Per-epoch interruption probability at d_th=4000 km\nFull Walker-delta (4,080 sats)")
    ax.legend(loc="center right")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out = PLOTS / "04_per_epoch_interruption.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 5: Strategy comparison at d_th=4000
# -----------------------------------------------------------------
def plot_strategy_comparison():
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    strategies = ["density", "single_hop", "stationary_optimal"]
    emp_means, bpp_means, dij_means = [], [], []
    for s in strategies:
        rows = [r for r in d["rows"] if r["strategy"] == s]
        emp_means.append(mean(rows, "empirical_interruption_probability"))
        bpp_means.append(mean(rows, "bpp_predicted_interruption_probability"))
        dij_means.append(mean(rows, "dijkstra_interruption_probability"))

    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.27
    x = np.arange(len(strategies))
    ax.bar(x - width, emp_means, width, label="Greedy (Wang)", color="#d62728")
    ax.bar(x,         bpp_means, width, label="BPP analytical", color="#2ca02c")
    ax.bar(x + width, dij_means, width, label="Dijkstra", color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels(strategies)
    ax.set_ylabel("Interruption probability")
    ax.set_title("Strategy comparison @ d_th=4000 km\nFull Walker-delta (4,080 sats)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = PLOTS / "05_strategy_comparison.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


def main():
    print(f"Generating plots in {PLOTS}/")
    plot_interruption_vs_dth()
    plot_latency_distribution()
    plot_hop_distribution()
    plot_per_epoch_interruption()
    plot_strategy_comparison()
    print("Done.")


if __name__ == "__main__":
    main()
