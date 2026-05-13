"""Starlink greedy experiment, with optional side-by-side Dijkstra comparison."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from base import (
    DEFAULT_D_TH_KM,
    DEFAULT_EDGES_FILE,
    DEFAULT_NODES_FILE,
    DEFAULT_THETA_R,
    DEFAULT_THETA_S,
    Node,
    build_graph,
    can_use_link,
    choose_gateway_pairs,
    dome_angle,
    gateway_ids,
    load_nodes,
)
from dijkstra import dijkstra_path


@dataclass
class RouteResult:
    success: bool
    reason: str
    path: list[int]

    @property
    def hops(self) -> int:
        return max(0, len(self.path) - 1)


def choose_next_node(nodes: dict[int, Node],
                     graph: dict[int, list[tuple[int, float]]],
                     current_id: int,
                     destination_id: int,
                     visited: set[int],
                     priority: dict[int, int] | None,
                     d_th_km: float,
                     theta_r: float,
                     theta_s: float) -> int | None:
    """Pick a relay using tier priority, then closest-to-destination."""

    current = nodes[current_id]
    destination = nodes[destination_id]
    best_by_tier: dict[int, tuple[float, int]] = {}

    for candidate_id, _delay_ms in graph.get(current_id, []):
        if candidate_id in visited or candidate_id == destination_id:
            continue
        candidate = nodes[candidate_id]
        if not can_use_link(current, candidate, destination,
                            d_th_km, theta_r, theta_s):
            continue

        score = dome_angle(candidate, destination)
        tier_id = candidate.shell_id
        previous = best_by_tier.get(tier_id)
        if previous is None or score < previous[0]:
            best_by_tier[tier_id] = (score, candidate_id)

    if not best_by_tier:
        return None

    if priority is None:
        return min(best_by_tier.values(), key=lambda item: item[0])[1]

    best_tier = min(
        best_by_tier,
        key=lambda tier_id: priority.get(tier_id, 9999),
    )
    return best_by_tier[best_tier][1]


def greedy_route(nodes: dict[int, Node],
                 graph: dict[int, list[tuple[int, float]]],
                 source_id: int,
                 destination_id: int,
                 priority: dict[int, int] | None = None,
                 d_th_km: float = DEFAULT_D_TH_KM,
                 theta_r: float = DEFAULT_THETA_R,
                 theta_s: float = DEFAULT_THETA_S,
                 max_hops: int = 50) -> RouteResult:
    """Run one greedy route from source to destination."""

    if source_id not in nodes or destination_id not in nodes:
        return RouteResult(False, "missing source or destination", [])

    path = [source_id]
    visited = {source_id}
    current_id = source_id

    for _ in range(max_hops):
        current = nodes[current_id]
        destination = nodes[destination_id]
        neighbor_ids = {neighbor_id for neighbor_id, _delay_ms in graph.get(current_id, [])}

        if destination_id in neighbor_ids and can_use_link(
            current,
            destination,
            destination,
            d_th_km,
            theta_r,
            theta_s,
        ):
            path.append(destination_id)
            return RouteResult(True, "reached destination", path)

        next_id = choose_next_node(
            nodes=nodes,
            graph=graph,
            current_id=current_id,
            destination_id=destination_id,
            visited=visited,
            priority=priority,
            d_th_km=d_th_km,
            theta_r=theta_r,
            theta_s=theta_s,
        )

        if next_id is None:
            return RouteResult(False, "no reachable relay", path)

        path.append(next_id)
        visited.add(next_id)
        current_id = next_id

    return RouteResult(False, "maximum hops reached", path)

def shell_priority_by_count(nodes: dict[int, Node]) -> dict[int, int]:
    """Priority order: denser satellite shells first, gateways last."""

    counts: dict[int, int] = {}
    for node in nodes.values():
        counts[node.shell_id] = counts.get(node.shell_id, 0) + 1

    satellite_shells = [shell_id for shell_id in counts if shell_id >= 0]
    satellite_shells.sort(key=lambda shell_id: counts[shell_id], reverse=True)
    ordered = satellite_shells + [-1]
    return {shell_id: rank for rank, shell_id in enumerate(ordered, start=1)}


def run_empirical(nodes_file: str | Path = DEFAULT_NODES_FILE,
                  edges_file: str | Path = DEFAULT_EDGES_FILE,
                  pair_count: int = 50,
                  d_th_km: float = DEFAULT_D_TH_KM,
                  seed: int = 42) -> list[dict]:
    """Run greedy routing for many gateway pairs."""

    nodes = load_nodes(nodes_file)
    graph = build_graph(edges_file)
    priority = shell_priority_by_count(nodes)
    gateways = gateway_ids(nodes)
    if len(gateways) < 2:
        raise ValueError("Need at least two gateways in the snapshot.")

    rows: list[dict] = []
    pairs = choose_gateway_pairs(gateways, pair_count, seed)

    for source, destination in pairs:
        route = greedy_route(
            nodes,
            graph,
            source,
            destination,
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


def print_summary(rows: list[dict],
                  title: str = "Empirical greedy routing summary") -> None:
    """Print a short result summary for presentation."""

    total = len(rows)
    success = sum(1 for row in rows if row["success"])
    interrupted = total - success
    interruption_probability = interrupted / total if total else 0.0

    print(title)
    print("-" * len(title))
    print(f"Pairs tested: {total}")
    print(f"Successful routes: {success}")
    print(f"Interrupted routes: {interrupted}")
    print(f"Interruption probability: {interruption_probability:.3f}")


def write_csv(rows: list[dict], output_file: str | Path) -> None:
    """Save per-pair results to CSV."""

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "source",
            "destination",
            "success",
            "hops",
            "reason",
            "path",
        ])
        writer.writeheader()
        writer.writerows(rows)

def run_comparison(nodes_file: str | Path,
                   edges_file: str | Path,
                   pair_count: int,
                   d_th_km: float,
                   seed: int) -> list[dict]:
    """Run greedy and constrained Dijkstra on exactly the same gateway pairs."""

    nodes = load_nodes(nodes_file)
    graph = build_graph(edges_file)
    priority = shell_priority_by_count(nodes)
    pairs = choose_gateway_pairs(gateway_ids(nodes), pair_count, seed)

    rows: list[dict] = []
    for source, destination in pairs:
        greedy = greedy_route(
            nodes=nodes,
            graph=graph,
            source_id=source,
            destination_id=destination,
            priority=priority,
            d_th_km=d_th_km,
        )
        dijkstra_success, dijkstra_path_nodes, dijkstra_delay_ms = dijkstra_path(
            graph=graph,
            nodes=nodes,
            source=source,
            destination=destination,
            d_th_km=d_th_km,
        )
        rows.append({
            "source": source,
            "destination": destination,
            "greedy_success": greedy.success,
            "greedy_hops": greedy.hops,
            "greedy_reason": greedy.reason,
            "greedy_path": " -> ".join(str(node_id) for node_id in greedy.path),
            "dijkstra_success": dijkstra_success,
            "dijkstra_hops": max(0, len(dijkstra_path_nodes) - 1),
            "dijkstra_delay_ms": dijkstra_delay_ms,
            "dijkstra_path": " -> ".join(str(node_id) for node_id in dijkstra_path_nodes),
        })
    return rows


def print_comparison_summary(rows: list[dict]) -> None:
    """Print a compact comparison summary."""

    total = len(rows)
    greedy_success = sum(1 for row in rows if row["greedy_success"])
    dijkstra_success = sum(1 for row in rows if row["dijkstra_success"])
    both_success = sum(
        1 for row in rows
        if row["greedy_success"] and row["dijkstra_success"]
    )
    dijkstra_only = sum(
        1 for row in rows
        if (not row["greedy_success"])
        and row["dijkstra_success"]
    )
    greedy_only = sum(
        1 for row in rows
        if row["greedy_success"]
        and (not row["dijkstra_success"])
    )
    both_fail = sum(
        1 for row in rows
        if (not row["greedy_success"]) and (not row["dijkstra_success"])
    )

    greedy_rate = greedy_success / total if total else 0.0
    dijkstra_rate = dijkstra_success / total if total else 0.0

    print("Starlink greedy vs Dijkstra")
    print("---------------------------")
    print(f"Pairs tested: {total}")
    print(f"Greedy success rate: {greedy_rate:.3f}")
    print(f"Dijkstra success rate: {dijkstra_rate:.3f}")
    print(f"Both succeed: {both_success}")
    print(f"Only Dijkstra succeeds: {dijkstra_only}")
    print(f"Only greedy succeeds: {greedy_only}")
    print(f"Both fail: {both_fail}")


def write_comparison_csv(rows: list[dict], output_file: str | Path) -> None:
    """Save the per-pair comparison rows."""

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "source",
            "destination",
            "greedy_success",
            "greedy_hops",
            "greedy_reason",
            "greedy_path",
            "dijkstra_success",
            "dijkstra_hops",
            "dijkstra_delay_ms",
            "dijkstra_path",
        ])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Starlink greedy experiment, with optional Dijkstra comparison."
    )
    parser.add_argument("--nodes", default=DEFAULT_NODES_FILE)
    parser.add_argument("--edges", default=DEFAULT_EDGES_FILE)
    parser.add_argument("--pairs", type=int, default=50)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        default=Path("results") / "final_empirical_routes.csv",
        help="CSV output file for greedy routes.",
    )
    parser.add_argument(
        "--compare-dijkstra",
        action="store_true",
        help="Also run constrained Dijkstra on the same pairs and print a side-by-side summary.",
    )
    parser.add_argument(
        "--compare-out",
        default=Path("results") / "final_starlink_comparison.csv",
        help="CSV output file for the pair-by-pair greedy vs constrained Dijkstra comparison.",
    )
    args = parser.parse_args()

    if args.compare_dijkstra:
        rows = run_comparison(
            nodes_file=args.nodes,
            edges_file=args.edges,
            pair_count=args.pairs,
            d_th_km=args.d_th_km,
            seed=args.seed,
        )
        print_comparison_summary(rows)
        write_comparison_csv(rows, args.compare_out)
        print(f"Saved comparison rows to {args.compare_out}")
        return

    rows = run_empirical(
        nodes_file=args.nodes,
        edges_file=args.edges,
        pair_count=args.pairs,
        d_th_km=args.d_th_km,
        seed=args.seed,
    )
    print_summary(rows)
    write_csv(rows, args.out)
    print(f"Saved per-pair routes to {args.out}")


if __name__ == "__main__":
    main()
