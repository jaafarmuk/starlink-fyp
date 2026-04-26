"""
Plot empirical vs analytical interruption probability from
analyze_mhr_reliability.py outputs.

Reads results/mhr_reliability_summary.csv and writes:
    results/mhr_reliability_overview.png
    results/mhr_reliability_error.png
    results/mhr_reliability_by_strategy.png

The plots are deliberately simple (matplotlib only). They are intended as a
starting point for the FYP write-up; expect to customise axes, titles, and
colours when going to the report.
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Plot Wang reliability comparison results.")
    ap.add_argument("--summary_csv",
                    default="results/mhr_reliability_summary.csv")
    ap.add_argument("--out_dir", default="results")
    return ap.parse_args()


def _annotate_no_data(ax, msg: str) -> None:
    ax.text(0.5, 0.5, msg, ha="center", va="center",
            transform=ax.transAxes)
    ax.set_xticks([])
    ax.set_yticks([])


def plot_overview(df: pd.DataFrame, path: str) -> None:
    fig, ax = plt.subplots(figsize=(8.0, 5.0))
    if df.empty:
        _annotate_no_data(ax, "no data")
    else:
        for sname, g in df.groupby("strategy"):
            g = g.sort_values("epoch_step")
            x = g["epoch_step"].values
            ax.plot(x, g["empirical_interruption_probability"].values,
                    marker="o", label=f"{sname} (empirical)")
            ax.plot(x, g["bpp_predicted_interruption_probability"].values,
                    marker="x", linestyle="--",
                    label=f"{sname} (BPP analytical)")
        ax.set_xlabel("Epoch step")
        ax.set_ylabel("Interruption probability")
        ax.set_title("Empirical vs analytical (Wang BPP) interruption probability")
        ax.set_ylim(0.0, 1.0)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_error(df: pd.DataFrame, path: str) -> None:
    fig, ax = plt.subplots(figsize=(8.0, 5.0))
    if df.empty:
        _annotate_no_data(ax, "no data")
    else:
        for sname, g in df.groupby("strategy"):
            g = g.sort_values("epoch_step")
            ax.plot(g["epoch_step"].values, g["absolute_error"].values,
                    marker="o", label=sname)
        ax.set_xlabel("Epoch step")
        ax.set_ylabel("|empirical - analytical|")
        ax.set_title("Absolute error of the BPP prediction over epochs")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_by_strategy(df: pd.DataFrame, path: str) -> None:
    fig, ax = plt.subplots(figsize=(8.0, 5.0))
    if df.empty:
        _annotate_no_data(ax, "no data")
    else:
        agg = (df.groupby("strategy")
                 [["empirical_interruption_probability",
                   "bpp_predicted_interruption_probability"]]
                 .mean()
                 .sort_index())
        x = np.arange(len(agg.index))
        w = 0.4
        ax.bar(x - w / 2,
               agg["empirical_interruption_probability"].values,
               width=w, label="empirical")
        ax.bar(x + w / 2,
               agg["bpp_predicted_interruption_probability"].values,
               width=w, label="BPP analytical")
        ax.set_xticks(x)
        ax.set_xticklabels(agg.index, rotation=15)
        ax.set_ylabel("Mean interruption probability")
        ax.set_title("Strategy comparison (averaged over epochs)")
        ax.set_ylim(0.0, 1.0)
        ax.grid(True, alpha=0.3, axis="y")
        ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main() -> int:
    args = parse_args()
    if not os.path.exists(args.summary_csv):
        print(f"ERROR: summary file {args.summary_csv} does not exist. "
              "Run tools/analyze_mhr_reliability.py first.",
              file=sys.stderr)
        return 2

    df = pd.read_csv(args.summary_csv)
    os.makedirs(args.out_dir, exist_ok=True)
    overview = os.path.join(args.out_dir, "mhr_reliability_overview.png")
    error = os.path.join(args.out_dir, "mhr_reliability_error.png")
    by_strategy = os.path.join(args.out_dir, "mhr_reliability_by_strategy.png")
    plot_overview(df, overview)
    plot_error(df, error)
    plot_by_strategy(df, by_strategy)
    print(f"Wrote {overview}")
    print(f"Wrote {error}")
    print(f"Wrote {by_strategy}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
