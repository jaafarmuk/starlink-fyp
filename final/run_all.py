"""Run all final experiments and print one comparison summary."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from base import (
    DEFAULT_D_TH_KM,
    DEFAULT_EDGES_FILE,
    DEFAULT_NODES_FILE,
    DEFAULT_THETA_R,
    DEFAULT_THETA_S,
)
from build_snapshot import build_snapshot
from dijkstra import run_dijkstra, run_relaxed_dijkstra
from dijkstra_random import run_random_dijkstra
from empirical import run_empirical
from greedy_random import run_random_greedy
from paper_equation import DEFAULT_THETA_M, PAPER_PM, Tier, interruption_probability, parse_strategy


def summarize_rows(rows: list[dict]) -> tuple[float, float]:
    """Return success rate and interruption rate."""

    total = len(rows)
    success = sum(1 for row in rows if row["success"])
    interruption = total - success
    success_rate = success / total if total else 0.0
    interruption_rate = interruption / total if total else 0.0
    return success_rate, interruption_rate


def ensure_snapshot(build_if_missing: bool,
                    rebuild: bool,
                    seed: int,
                    epoch_steps: int,
                    multi_epoch_seconds: int) -> None:
    """Build the Starlink-like snapshot when requested or missing."""

    snapshot_missing = not DEFAULT_NODES_FILE.exists() or not DEFAULT_EDGES_FILE.exists()
    if not rebuild and not (build_if_missing and snapshot_missing):
        return

    print("Building Starlink-like snapshot for the run-all script...")
    build_snapshot(
        output_dir=DEFAULT_NODES_FILE.parent,
        seed=seed,
        epoch_steps=epoch_steps,
        multi_epoch_seconds=multi_epoch_seconds,
    )


def write_summary_csv(rows: list[dict], output_file: str | Path) -> None:
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "experiment",
            "success_rate",
            "interruption_rate",
            "notes",
        ])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all final experiments and compare them.")
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--strategy", type=parse_strategy, default=parse_strategy("3,2,1"))
    parser.add_argument("--build-snapshot", action="store_true",
                        help="Build the Starlink-like snapshot if it is missing.")
    parser.add_argument("--rebuild-snapshot", action="store_true",
                        help="Always rebuild the Starlink-like snapshot before running.")
    parser.add_argument("--epoch-steps", type=int, default=10)
    parser.add_argument("--multi-epoch-seconds", type=int, default=540)
    parser.add_argument("--out", default=Path("results") / "final_all_experiments_summary.csv")
    args = parser.parse_args()

    ensure_snapshot(
        build_if_missing=args.build_snapshot,
        rebuild=args.rebuild_snapshot,
        seed=args.seed,
        epoch_steps=args.epoch_steps,
        multi_epoch_seconds=args.multi_epoch_seconds,
    )

    summary_rows: list[dict] = []

    tiers = [
        Tier("gateway", 0.0, 300),
        Tier("kepler_like", 575.0, 140),
        Tier("oneweb_like", 1200.0, 720),
    ]
    paper_probability, _estimated_hops = interruption_probability(
        strategy=args.strategy,
        tiers=tiers,
        theta_r=DEFAULT_THETA_R,
        theta_s=DEFAULT_THETA_S,
        d_th_km=args.d_th_km,
        theta_m=DEFAULT_THETA_M,
    )
    summary_rows.append({
        "experiment": f"paper_equation_{list(args.strategy)}",
        "success_rate": 1.0 - paper_probability,
        "interruption_rate": paper_probability,
        "notes": f"paper_PM={PAPER_PM.get(args.strategy, '-')}",
    })

    random_greedy_rows = run_random_greedy(args.pairs, args.seed, args.d_th_km)
    success, interruption = summarize_rows(random_greedy_rows)
    summary_rows.append({
        "experiment": "greedy_random_points",
        "success_rate": success,
        "interruption_rate": interruption,
        "notes": "paper constraints on random points",
    })

    random_dijkstra_rows = run_random_dijkstra(
        pair_count=args.pairs,
        seed=args.seed,
        d_th_km=args.d_th_km,
    )
    success, interruption = summarize_rows(random_dijkstra_rows)
    summary_rows.append({
        "experiment": "dijkstra_random_points",
        "success_rate": success,
        "interruption_rate": interruption,
        "notes": "relaxed first hop, then paper constraints",
    })

    sats_greedy_rows = run_empirical(
        nodes_file=DEFAULT_NODES_FILE,
        edges_file=DEFAULT_EDGES_FILE,
        pair_count=args.pairs,
        d_th_km=args.d_th_km,
        seed=args.seed,
    )
    success, interruption = summarize_rows(sats_greedy_rows)
    summary_rows.append({
        "experiment": "greedy_starlink_like_snapshot",
        "success_rate": success,
        "interruption_rate": interruption,
        "notes": "paper constraints on Starlink-like snapshot",
    })

    sats_dijkstra_rows = run_dijkstra(
        nodes_file=DEFAULT_NODES_FILE,
        edges_file=DEFAULT_EDGES_FILE,
        pair_count=args.pairs,
        seed=args.seed,
        d_th_km=args.d_th_km,
    )
    success, interruption = summarize_rows(sats_dijkstra_rows)
    summary_rows.append({
        "experiment": "dijkstra_starlink_like_snapshot",
        "success_rate": success,
        "interruption_rate": interruption,
        "notes": "relaxed first hop, then paper constraints",
    })

    relaxed_sats_dijkstra_rows = run_relaxed_dijkstra(
        nodes_file=DEFAULT_NODES_FILE,
        edges_file=DEFAULT_EDGES_FILE,
        pair_count=args.pairs,
        seed=args.seed,
    )
    success, interruption = summarize_rows(relaxed_sats_dijkstra_rows)
    summary_rows.append({
        "experiment": "dijkstra_starlink_relaxed",
        "success_rate": success,
        "interruption_rate": interruption,
        "notes": "all generated snapshot edges allowed",
    })

    print("All experiment summary")
    print("----------------------")
    print("experiment                           success  interrupt  notes")
    for row in summary_rows:
        print(
            f"{row['experiment']:<35} "
            f"{row['success_rate']:.3f}    "
            f"{row['interruption_rate']:.3f}     "
            f"{row['notes']}"
        )

    print()
    print("Quick comparison")
    print("----------------")
    print("Random points:")
    print("  Compare paper_equation vs greedy_random_points vs dijkstra_random_points.")
    print("Starlink-like snapshot:")
    print("  Compare greedy_starlink_like_snapshot vs dijkstra_starlink_like_snapshot vs dijkstra_starlink_relaxed.")

    write_summary_csv(summary_rows, args.out)
    print(f"Saved summary to {args.out}")


if __name__ == "__main__":
    main()
