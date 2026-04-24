"""
Plot per-flow metrics produced by the ns-3 starlink-snapshot scenario.

This file reads both the legacy schema (v1, no transport/goodput split) and
the current schema (v2+, with an explicit `schema_version` preamble line).
Plots are only emitted for columns that are actually present; missing
columns are skipped with a warning (review item 19).
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
import matplotlib.pyplot as plt


SUPPORTED_SCHEMAS = {"1", "2.0.0"}

# Column mapping between v1 (pre-review) and v2 (current) so the plotter can
# consume either file without regeneration. Entries on the left are the names
# the plotter uses internally; the right side is what we look for in the CSV.
CANONICAL_COLUMNS = {
    "throughput": ["goodput_mbps", "throughput_mbps"],
    "mean_delay_ms": ["mean_delay_ms"],
    "mean_jitter_ms": ["mean_jitter_ms"],
    "tcp_retrans_overhead_percent": ["tcp_retrans_overhead_percent"],
    "delivery_ratio_percent": ["delivery_ratio_percent"],
    "hop_count": ["hop_count_unweighted", "hop_count"],
    "shortest_delay_ms": ["shortest_delay_ms"],
    "src_node": ["src_node"],
    "dst_node": ["dst_node"],
    "flow_index": ["flow_index"],
}


def _read_csv_with_optional_header(path: str) -> tuple[pd.DataFrame, str]:
    """Read the CSV, stripping an optional `schema_version=...` preamble."""
    with open(path, "r", encoding="utf-8") as fh:
        first = fh.readline()
    if first.startswith("schema_version="):
        schema = first.strip().split("=", 1)[1]
        df = pd.read_csv(path, skiprows=1)
    else:
        schema = "1"
        df = pd.read_csv(path)
    return df, schema


def canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    out = {}
    for canonical, candidates in CANONICAL_COLUMNS.items():
        for c in candidates:
            if c in df.columns:
                out[canonical] = df[c]
                break
    result = pd.DataFrame(out)
    return result


def plot_bar(df: pd.DataFrame,
             column: str, title: str, ylabel: str,
             out_path: str) -> None:
    if column not in df.columns or df.empty:
        print(f"  skip {column}: column missing")
        return
    labels = [f"{int(r.src_node)}->{int(r.dst_node)}"
              if "src_node" in df.columns and "dst_node" in df.columns
              else str(i)
              for i, r in enumerate(df.itertuples())]
    x = list(range(len(df)))
    fig, ax = plt.subplots(figsize=(max(7.0, 0.5 * len(df) + 3.0), 4.5))
    ax.bar(x, df[column], color="#1f77b4")
    ax.set_title(title)
    ax.set_xlabel("Flow (src -> dst)")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def plot_cdf(df: pd.DataFrame,
             column: str, title: str, xlabel: str,
             out_path: str) -> None:
    if column not in df.columns or df.empty:
        print(f"  skip {column}: column missing")
        return
    values = df[column].dropna().sort_values().to_numpy()
    if values.size == 0:
        return
    y = [(i + 1) / len(values) for i in range(len(values))]
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.step(values, y, where="post", color="#1f77b4", linewidth=2)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.set_ylim(0.0, 1.0)
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def plot_scatter(df: pd.DataFrame,
                 xcol: str, ycol: str,
                 title: str, xlabel: str, ylabel: str,
                 out_path: str) -> None:
    if xcol not in df.columns or ycol not in df.columns or df.empty:
        print(f"  skip {xcol} vs {ycol}: columns missing")
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.scatter(df[xcol], df[ycol], color="#1f77b4", s=60, zorder=3)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="results/per_flow_metrics.csv")
    ap.add_argument("--out_dir", default="results")
    args = ap.parse_args()

    if not os.path.isfile(args.input):
        raise SystemExit(f"Input CSV not found: {args.input}")

    raw_df, schema = _read_csv_with_optional_header(args.input)
    if schema not in SUPPORTED_SCHEMAS:
        print(f"WARNING: unknown schema_version={schema!r}, attempting best-effort read",
              file=sys.stderr)

    df = canonicalize(raw_df)
    if df.empty:
        raise SystemExit(f"Input CSV has no usable rows: {args.input}")

    if "flow_index" in df.columns:
        df = df.sort_values("flow_index").reset_index(drop=True)

    os.makedirs(args.out_dir, exist_ok=True)
    print(f"schema={schema}, rows={len(df)}, columns={list(df.columns)}")

    plot_bar(df, "throughput",
             "Per-flow goodput", "Mbps",
             os.path.join(args.out_dir, "throughput_per_flow.png"))
    plot_bar(df, "mean_delay_ms",
             "Per-flow mean delay", "ms",
             os.path.join(args.out_dir, "mean_delay_per_flow.png"))
    plot_bar(df, "mean_jitter_ms",
             "Per-flow mean jitter", "ms",
             os.path.join(args.out_dir, "jitter_per_flow.png"))
    plot_bar(df, "tcp_retrans_overhead_percent",
             "Per-flow TCP retransmission overhead", "%",
             os.path.join(args.out_dir, "tcp_retrans_overhead_per_flow.png"))
    plot_bar(df, "delivery_ratio_percent",
             "Per-flow byte delivery ratio", "%",
             os.path.join(args.out_dir, "delivery_ratio_per_flow.png"))

    plot_cdf(df, "throughput",
             "Goodput CDF", "Mbps",
             os.path.join(args.out_dir, "throughput_cdf.png"))
    plot_cdf(df, "mean_delay_ms",
             "Mean delay CDF", "ms",
             os.path.join(args.out_dir, "mean_delay_cdf.png"))
    plot_cdf(df, "mean_jitter_ms",
             "Jitter CDF", "ms",
             os.path.join(args.out_dir, "jitter_cdf.png"))
    plot_cdf(df, "tcp_retrans_overhead_percent",
             "TCP retransmission overhead CDF", "%",
             os.path.join(args.out_dir, "tcp_retrans_overhead_cdf.png"))

    plot_scatter(df, "hop_count", "mean_delay_ms",
                 "Mean delay vs hop count (unweighted)",
                 "Hop Count", "Mean Delay (ms)",
                 os.path.join(args.out_dir, "delay_vs_hops.png"))
    plot_scatter(df, "shortest_delay_ms", "mean_delay_ms",
                 "Measured delay vs shortest propagation delay",
                 "Shortest Propagation Delay (ms)",
                 "Measured Mean Delay (ms)",
                 os.path.join(args.out_dir, "delay_vs_shortest.png"))

    print(f"Wrote plots for {len(df)} flows into {args.out_dir}/")


if __name__ == "__main__":
    main()
