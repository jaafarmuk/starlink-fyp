"""Dijkstra routing on the Starlink-like snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from base import DEFAULT_D_TH_KM, DEFAULT_EDGES_FILE, DEFAULT_NODES_FILE
from dijkstra import (
    print_dijkstra_summary,
    run_dijkstra,
    run_relaxed_dijkstra,
    write_dijkstra_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Constrained Dijkstra on the Starlink-like snapshot.")
    parser.add_argument("--nodes", default=DEFAULT_NODES_FILE)
    parser.add_argument("--edges", default=DEFAULT_EDGES_FILE)
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument(
        "--relaxed",
        action="store_true",
        help="Use every generated snapshot edge, without paper-style constraints.",
    )
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    if args.relaxed:
        rows = run_relaxed_dijkstra(
            nodes_file=args.nodes,
            edges_file=args.edges,
            pair_count=args.pairs,
            seed=args.seed,
        )
        title = "Relaxed Dijkstra Starlink-like snapshot summary"
        output_file = args.out or (Path("results") / "dijkstra_sats_relaxed_routes.csv")
    else:
        rows = run_dijkstra(
            nodes_file=args.nodes,
            edges_file=args.edges,
            pair_count=args.pairs,
            seed=args.seed,
            d_th_km=args.d_th_km,
        )
        title = "Dijkstra Starlink-like snapshot summary"
        output_file = args.out or (Path("results") / "dijkstra_sats_routes.csv")

    print_dijkstra_summary(rows, title)
    write_dijkstra_csv(rows, output_file)
    print(f"Saved routes to {output_file}")


if __name__ == "__main__":
    main()
