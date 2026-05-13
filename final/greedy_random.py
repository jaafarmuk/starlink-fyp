"""Greedy routing on paper-like random points."""

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
from empirical import greedy_route, print_summary, shell_priority_by_count, write_csv


def run_random_greedy(pair_count: int,
                      seed: int,
                      d_th_km: float = DEFAULT_D_TH_KM) -> list[dict]:
    """Run greedy routing on one random paper-like topology."""

    nodes = build_random_nodes(seed=seed)
    graph = build_graph_from_nodes(nodes, d_th_km=d_th_km)
    priority = shell_priority_by_count(nodes)
    pairs = choose_gateway_pairs(gateway_ids(nodes), pair_count, seed)

    rows: list[dict] = []
    for source, destination in pairs:
        route = greedy_route(
            nodes=nodes,
            graph=graph,
            source_id=source,
            destination_id=destination,
            priority=priority,
            d_th_km=d_th_km,
        )
        rows.append({
            "source": source,
            "destination": destination,
            "success": route.success,
            "hops": route.hops,
            "reason": route.reason,
            "path": " -> ".join(str(node_id) for node_id in route.path),
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Greedy routing on random paper-like points.")
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--out", default=Path("results") / "greedy_random_routes.csv")
    args = parser.parse_args()

    rows = run_random_greedy(
        pair_count=args.pairs,
        seed=args.seed,
        d_th_km=args.d_th_km,
    )
    print_summary(rows, title="Greedy random-point summary")
    write_csv(rows, args.out)
    print(f"Saved routes to {args.out}")


if __name__ == "__main__":
    main()
