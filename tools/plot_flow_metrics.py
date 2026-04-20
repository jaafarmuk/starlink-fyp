import pandas as pd
import matplotlib.pyplot as plt


def main():
    df = pd.read_csv("results/flow_metrics.csv")

    plots = [
        ("throughput_mbps", "Throughput", "Mbps", "results/throughput_vs_flows.png"),
        ("mean_delay_ms", "Mean Delay", "ms", "results/mean_delay_vs_flows.png"),
        ("loss_rate_percent", "Loss Rate", "%", "results/loss_rate_vs_flows.png"),
    ]

    for column, title, ylabel, out_path in plots:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot(df["num_flows"], df[column], marker="o", linewidth=2, markersize=7, color="#1f77b4")
        ax.set_title(f"{title} vs Number of Flows")
        ax.set_xlabel("Number of Flows")
        ax.set_ylabel(ylabel)
        ax.set_xticks(df["num_flows"])
        ax.grid(True, linestyle="--", alpha=0.5)
        fig.tight_layout()
        fig.savefig(out_path, dpi=160)
        plt.close(fig)


if __name__ == "__main__":
    main()
