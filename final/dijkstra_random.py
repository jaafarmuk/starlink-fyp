"""Dijkstra routing on paper-like random points."""

from __future__ import annotations

import argparse
from pathlib import Path

from base import (
    DEFAULT_D_TH_KM,
    build_graph_from_nodes,
    build_random_nodes,
    choose_gateway_pairs,
    gateway_ids,
)
from dijkstra import dijkstra_path, print_dijkstra_summary, write_dijkstra_csv


def run_random_dijkstra(pair_count: int,
                        seed: int,
                        d_th_km: float = DEFAULT_D_TH_KM) -> list[dict]:
    """Run constrained Dijkstra on one random paper-like topology."""

    nodes = build_random_nodes(seed=seed)
    graph = build_graph_from_nodes(nodes, d_th_km=d_th_km)
    pairs = choose_gateway_pairs(gateway_ids(nodes), pair_count, seed)

    rows: list[dict] = []
    for source, destination in pairs:
        success, path, delay_ms = dijkstra_path(
            graph=graph,
            nodes=nodes,
            source=source,
            destination=destination,
            d_th_km=d_th_km,
        )
        reason = "reached destination" if success else "no constrained graph path"
        rows.append({
            "source": source,
            "destination": destination,
            "success": success,
            "hops": max(0, len(path) - 1),
            "reason": reason,
            "delay_ms": delay_ms,
            "path": " -> ".join(str(node_id) for node_id in path),
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Constrained Dijkstra on random paper-like points.")
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    rows = run_random_dijkstra(
        pair_count=args.pairs,
        seed=args.seed,
        d_th_km=args.d_th_km,
    )
    title = "Dijkstra random-point summary"
    output_file = args.out or (Path("results") / "dijkstra_random_routes.csv")
    print_dijkstra_summary(rows, title)
    write_dijkstra_csv(rows, output_file)
    print(f"Saved routes to {output_file}")


if __name__ == "__main__":
    main()
