"""Generate comparison plots from MHR analysis results.

Reads the d_th sweep JSON outputs in results/analysis/full/ and
results/analysis/n600/ and produces:

  results/plots/01_interruption_vs_dth.png       - greedy / Dijkstra / BPP across d_th
  results/plots/02_n600_vs_full_gap.png          - BPP error magnitude in both regimes
  results/plots/03_latency_distribution.png      - greedy vs Dijkstra latency CDFs
  results/plots/04_hop_distribution.png          - greedy vs Dijkstra hop counts
  results/plots/05_per_epoch_interruption.png    - per-epoch breakdown
  results/plots/06_strategy_comparison.png       - density / single_hop / stat_opt

All plots use density strategy as the primary unless otherwise noted.
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


REPO = Path(__file__).resolve().parent.parent
ANALYSIS = REPO / "results" / "analysis"
PLOTS = REPO / "results" / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

D_THS = [3000, 3500, 4000, 5000]
VARIANTS = [("full", "Full Walker-delta (4,080 sats)"),
            ("n600", "Random subsample (600 sats)")]
COLORS = {"full": "#1f77b4", "n600": "#ff7f0e"}


def load(variant: str, dth: int) -> dict:
    p = ANALYSIS / variant / f"dth{dth}_summary.json"
    with open(p) as f:
        return json.load(f)


def density_rows(summary: dict) -> list:
    return [r for r in summary["rows"] if r["strategy"] == "density"]


def mean(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None and not (
        isinstance(r[key], float) and np.isnan(r[key]))]
    return float(np.mean(vals)) if vals else float("nan")


# -----------------------------------------------------------------
# Plot 1: Interruption probability vs d_th — three curves per variant
# -----------------------------------------------------------------
def plot_interruption_vs_dth():
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (variant, title) in zip(axes, VARIANTS):
        emp, bpp, dij = [], [], []
        for dth in D_THS:
            try:
                d = load(variant, dth)
            except FileNotFoundError:
                continue
            r = density_rows(d)
            emp.append(mean(r, "empirical_interruption_probability"))
            bpp.append(mean(r, "bpp_predicted_interruption_probability"))
            dij.append(mean(r, "dijkstra_interruption_probability"))
        ax.plot(D_THS, emp, "o-", lw=2, color="#d62728", label="Greedy (Wang) empirical")
        ax.plot(D_THS, bpp, "s--", lw=2, color="#2ca02c", label="BPP analytical")
        ax.plot(D_THS, dij, "^-", lw=2, color="#1f77b4", label="Dijkstra empirical")
        ax.set_title(title)
        ax.set_xlabel("d_th (km)")
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3)
        ax.legend(loc="center right")
    axes[0].set_ylabel("Interruption probability")
    fig.suptitle("Interruption probability vs hop-distance limit (d_th)", fontsize=13)
    fig.tight_layout()
    out = PLOTS / "01_interruption_vs_dth.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 2: BPP error magnitude — 600 vs full
# -----------------------------------------------------------------
def plot_n600_vs_full_gap():
    fig, ax = plt.subplots(figsize=(9, 5))
    width = 0.38
    x = np.arange(len(D_THS))
    for i, (variant, title) in enumerate(VARIANTS):
        gaps = []
        for dth in D_THS:
            try:
                d = load(variant, dth)
            except FileNotFoundError:
                gaps.append(0)
                continue
            r = density_rows(d)
            emp = mean(r, "empirical_interruption_probability")
            bpp = mean(r, "bpp_predicted_interruption_probability")
            gaps.append(abs(emp - bpp) * 100)
        ax.bar(x + (i - 0.5) * width, gaps, width,
               label=title, color=COLORS[variant])
    ax.set_xticks(x)
    ax.set_xticklabels([f"{d}" for d in D_THS])
    ax.set_xlabel("d_th (km)")
    ax.set_ylabel("Absolute BPP error (percentage points)")
    ax.set_title("BPP prediction error — random subsample vs full constellation")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = PLOTS / "02_n600_vs_full_gap.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 3: Latency distribution (mean + percentiles) at d_th=4000
# -----------------------------------------------------------------
def plot_latency_distribution():
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (variant, title) in zip(axes, VARIANTS):
        try:
            d = load(variant, 4000)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        # Take mean over epochs of each percentile
        labels = ["mean", "p50", "p90", "p95", "p99"]
        gr = [mean(r, f"greedy_total_ms_{l}") for l in labels]
        dj = [mean(r, f"dijkstra_total_ms_{l}") for l in labels]
        x = np.arange(len(labels))
        ax.bar(x - 0.2, gr, 0.4, label="Greedy (Wang)", color="#d62728")
        ax.bar(x + 0.2, dj, 0.4, label="Dijkstra", color="#1f77b4")
        ax.set_xticks(x)
        ax.set_xticklabels([l.upper() for l in labels])
        ax.set_title(title)
        ax.grid(alpha=0.3, axis="y")
        ax.legend()
    axes[0].set_ylabel("End-to-end latency (ms, prop+queue)")
    fig.suptitle("Latency distribution at d_th=4000 km — successful routes only", fontsize=13)
    fig.tight_layout()
    out = PLOTS / "03_latency_distribution.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 4: Hop count distribution at d_th=4000
# -----------------------------------------------------------------
def plot_hop_distribution():
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (variant, title) in zip(axes, VARIANTS):
        try:
            d = load(variant, 4000)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        labels = ["mean", "p50", "p90", "p95", "p99"]
        gr = [mean(r, f"greedy_hops_{l}") for l in labels]
        dj = [mean(r, f"dijkstra_hops_{l}") for l in labels]
        x = np.arange(len(labels))
        ax.bar(x - 0.2, gr, 0.4, label="Greedy (Wang)", color="#d62728")
        ax.bar(x + 0.2, dj, 0.4, label="Dijkstra", color="#1f77b4")
        ax.set_xticks(x)
        ax.set_xticklabels([l.upper() for l in labels])
        ax.set_title(title)
        ax.grid(alpha=0.3, axis="y")
        ax.legend()
    axes[0].set_ylabel("Hop count")
    fig.suptitle("Hop count distribution at d_th=4000 km — successful routes only", fontsize=13)
    fig.tight_layout()
    out = PLOTS / "04_hop_distribution.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 5: Per-epoch breakdown at d_th=4000
# -----------------------------------------------------------------
def plot_per_epoch_interruption():
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (variant, title) in zip(axes, VARIANTS):
        try:
            d = load(variant, 4000)
        except FileNotFoundError:
            continue
        rows = density_rows(d)
        rows.sort(key=lambda r: r["epoch_step"])
        steps = [r["epoch_step"] for r in rows]
        emp = [r["empirical_interruption_probability"] for r in rows]
        bpp = [r["bpp_predicted_interruption_probability"] for r in rows]
        dij = [r["dijkstra_interruption_probability"] for r in rows]
        ax.plot(steps, emp, "o-", color="#d62728", label="Greedy (Wang)")
        ax.plot(steps, bpp, "s--", color="#2ca02c", label="BPP analytical")
        ax.plot(steps, dij, "^-", color="#1f77b4", label="Dijkstra")
        ax.set_title(title)
        ax.set_xlabel("Epoch (9-min steps)")
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3)
        ax.legend(loc="center right")
    axes[0].set_ylabel("Interruption probability")
    fig.suptitle("Per-epoch interruption probability at d_th=4000 km", fontsize=13)
    fig.tight_layout()
    out = PLOTS / "05_per_epoch_interruption.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


# -----------------------------------------------------------------
# Plot 6: Strategy comparison at d_th=4000 — full constellation
# -----------------------------------------------------------------
def plot_strategy_comparison():
    try:
        d = load("full", 4000)
    except FileNotFoundError:
        return
    strategies = ["density", "single_hop", "stationary_optimal"]
    fig, ax = plt.subplots(figsize=(9, 5))
    width = 0.27
    x = np.arange(len(strategies))
    emp_means, bpp_means, dij_means = [], [], []
    for s in strategies:
        rows = [r for r in d["rows"] if r["strategy"] == s]
        emp_means.append(mean(rows, "empirical_interruption_probability"))
        bpp_means.append(mean(rows, "bpp_predicted_interruption_probability"))
        dij_means.append(mean(rows, "dijkstra_interruption_probability"))
    ax.bar(x - width, emp_means, width, label="Greedy (Wang)", color="#d62728")
    ax.bar(x,         bpp_means, width, label="BPP analytical", color="#2ca02c")
    ax.bar(x + width, dij_means, width, label="Dijkstra", color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels(strategies)
    ax.set_ylabel("Interruption probability")
    ax.set_title("Strategy comparison @ d_th=4000 km — full Walker-delta (4,080 sats)")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = PLOTS / "06_strategy_comparison.png"
    fig.savefig(out, dpi=130)
    print(f"  → {out}")


def main():
    print(f"Generating plots in {PLOTS}/")
    plot_interruption_vs_dth()
    plot_n600_vs_full_gap()
    plot_latency_distribution()
    plot_hop_distribution()
    plot_per_epoch_interruption()
    plot_strategy_comparison()
    print("Done.")


if __name__ == "__main__":
    main()
