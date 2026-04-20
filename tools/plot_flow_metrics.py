import argparse
import os

import pandas as pd
import matplotlib.pyplot as plt


REQUIRED_COLUMNS = {
    "flow_index",
    "src_node",
    "dst_node",
    "throughput_mbps",
    "mean_delay_ms",
    "mean_jitter_ms",
    "loss_rate_percent",
}


def plot_per_flow(df, out_dir):
    labels = [f"{int(r.src_node)}->{int(r.dst_node)}" for r in df.itertuples()]
    x = list(range(len(df)))

    plots = [
        ("throughput_mbps", "Per-flow Throughput", "Mbps", "throughput_per_flow.png"),
        ("mean_delay_ms", "Per-flow Mean Delay", "ms", "mean_delay_per_flow.png"),
        ("mean_jitter_ms", "Per-flow Mean Jitter", "ms", "jitter_per_flow.png"),
        ("loss_rate_percent", "Per-flow Loss Rate", "%", "loss_rate_per_flow.png"),
    ]

    for column, title, ylabel, filename in plots:
        fig, ax = plt.subplots(figsize=(max(7, 0.5 * len(df) + 3), 4.5))
        ax.bar(x, df[column], color="#1f77b4")
        ax.set_title(title)
        ax.set_xlabel("Flow (src -> dst)")
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.grid(True, axis="y", linestyle="--", alpha=0.5)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, filename), dpi=160)
        plt.close(fig)


def plot_cdfs(df, out_dir):
    plots = [
        ("throughput_mbps", "Throughput CDF", "Mbps", "throughput_cdf.png"),
        ("mean_delay_ms", "Mean Delay CDF", "ms", "mean_delay_cdf.png"),
        ("mean_jitter_ms", "Jitter CDF", "ms", "jitter_cdf.png"),
        ("loss_rate_percent", "Loss Rate CDF", "%", "loss_rate_cdf.png"),
    ]

    for column, title, xlabel, filename in plots:
        values = df[column].sort_values().to_numpy()
        if len(values) == 0:
            continue
        y = [(i + 1) / len(values) for i in range(len(values))]

        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.step(values, y, where="post", color="#1f77b4", linewidth=2)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("CDF")
        ax.set_ylim(0.0, 1.0)
        ax.grid(True, linestyle="--", alpha=0.5)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, filename), dpi=160)
        plt.close(fig)


def plot_delay_vs_hops(df, out_dir):
    if "hop_count" not in df.columns:
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.scatter(df["hop_count"], df["mean_delay_ms"], color="#1f77b4", s=60, zorder=3)
    ax.set_title("Mean Delay vs Hop Count")
    ax.set_xlabel("Hop Count")
    ax.set_ylabel("Mean Delay (ms)")
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "delay_vs_hops.png"), dpi=160)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="results/per_flow_metrics.csv",
                    help="Per-flow metrics CSV produced by the ns-3 scenario")
    ap.add_argument("--out_dir", default="results",
                    help="Directory to write plot images into")
    args = ap.parse_args()

    if not os.path.isfile(args.input):
        raise SystemExit(f"Input CSV not found: {args.input}")

    df = pd.read_csv(args.input)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(
            f"Input CSV is missing required columns: {sorted(missing)}. "
            f"Expected columns include {sorted(REQUIRED_COLUMNS)}."
        )

    if df.empty:
        raise SystemExit(f"Input CSV has no rows: {args.input}")

    os.makedirs(args.out_dir, exist_ok=True)
    df = df.sort_values("flow_index").reset_index(drop=True)

    plot_per_flow(df, args.out_dir)
    plot_cdfs(df, args.out_dir)
    plot_delay_vs_hops(df, args.out_dir)

    print(f"Wrote plots for {len(df)} flows into {args.out_dir}/")


if __name__ == "__main__":
    main()
