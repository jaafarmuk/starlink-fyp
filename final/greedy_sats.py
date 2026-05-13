"""Greedy routing on the Starlink-like snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from base import DEFAULT_D_TH_KM, DEFAULT_EDGES_FILE, DEFAULT_NODES_FILE
from empirical import print_summary, run_empirical, write_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Greedy routing on the Starlink-like snapshot.")
    parser.add_argument("--nodes", default=DEFAULT_NODES_FILE)
    parser.add_argument("--edges", default=DEFAULT_EDGES_FILE)
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--out", default=Path("results") / "greedy_sats_routes.csv")
    args = parser.parse_args()

    rows = run_empirical(
        nodes_file=args.nodes,
        edges_file=args.edges,
        pair_count=args.pairs,
        d_th_km=args.d_th_km,
        seed=args.seed,
    )
    print_summary(rows, title="Greedy Starlink-like snapshot summary")
    write_csv(rows, args.out)
    print(f"Saved routes to {args.out}")


if __name__ == "__main__":
    main()
