"""Constrained Dijkstra routing on the Starlink snapshot graph."""

from __future__ import annotations

import argparse
import csv
import heapq
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
    gateway_ids,
    load_nodes,
)


def dijkstra_path(graph: dict[int, list[tuple[int, float]]],
                  nodes: dict[int, Node],
                  source: int,
                  destination: int,
                  d_th_km: float = DEFAULT_D_TH_KM,
                  theta_r: float = DEFAULT_THETA_R,
                  theta_s: float = DEFAULT_THETA_S
                  ) -> tuple[bool, list[int], float]:
    """Shortest path with a relaxed first hop, then paper-style constraints."""

    queue = [(0.0, source)]
    distance = {source: 0.0}
    previous: dict[int, int] = {}
    destination_node = nodes[destination]

    while queue:
        current_distance, node = heapq.heappop(queue)
        if node == destination:
            break
        if current_distance > distance.get(node, float("inf")):
            continue

        current_node = nodes[node]
        for next_node, delay_ms in graph.get(node, []):
            candidate_node = nodes[next_node]
            # Relax the first hop from the source gateway: use any snapshot edge.
            # All later hops must satisfy the paper-style constraints.
            if node != source:
                if not can_use_link(
                    current=current_node,
                    candidate=candidate_node,
                    destination=destination_node,
                    d_th_km=d_th_km,
                    theta_r=theta_r,
                    theta_s=theta_s,
                ):
                    continue
            new_distance = current_distance + delay_ms
            if new_distance < distance.get(next_node, float("inf")):
                distance[next_node] = new_distance
                previous[next_node] = node
                heapq.heappush(queue, (new_distance, next_node))

    if destination not in distance:
        return False, [source], float("inf")

    path = [destination]
    while path[-1] != source:
        path.append(previous[path[-1]])
    path.reverse()
    return True, path, distance[destination]


def relaxed_dijkstra_path(graph: dict[int, list[tuple[int, float]]],
                          source: int,
                          destination: int) -> tuple[bool, list[int], float]:
    """Shortest path using every edge already present in the snapshot graph."""

    queue = [(0.0, source)]
    distance = {source: 0.0}
    previous: dict[int, int] = {}

    while queue:
        current_distance, node = heapq.heappop(queue)
        if node == destination:
            break
        if current_distance > distance.get(node, float("inf")):
            continue

        for next_node, delay_ms in graph.get(node, []):
            new_distance = current_distance + delay_ms
            if new_distance < distance.get(next_node, float("inf")):
                distance[next_node] = new_distance
                previous[next_node] = node
                heapq.heappush(queue, (new_distance, next_node))

    if destination not in distance:
        return False, [source], float("inf")

    path = [destination]
    while path[-1] != source:
        path.append(previous[path[-1]])
    path.reverse()
    return True, path, distance[destination]


def run_dijkstra(nodes_file: str | Path,
                 edges_file: str | Path,
                 pair_count: int,
                 seed: int,
                 d_th_km: float = DEFAULT_D_TH_KM) -> list[dict]:
    """Run constrained Dijkstra for many gateway pairs."""

    nodes = load_nodes(nodes_file)
    pairs = choose_gateway_pairs(gateway_ids(nodes), pair_count, seed)
    graph = build_graph(edges_file)

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
            "path": " -> ".join(str(node_id) for node_id in path),
            "delay_ms": delay_ms,
        })
    return rows


def run_relaxed_dijkstra(nodes_file: str | Path,
                         edges_file: str | Path,
                         pair_count: int,
                         seed: int) -> list[dict]:
    """Run Dijkstra on the snapshot graph without paper-style constraints."""

    nodes = load_nodes(nodes_file)
    pairs = choose_gateway_pairs(gateway_ids(nodes), pair_count, seed)
    graph = build_graph(edges_file)

    rows: list[dict] = []
    for source, destination in pairs:
        success, path, delay_ms = relaxed_dijkstra_path(
            graph=graph,
            source=source,
            destination=destination,
        )
        reason = "reached destination" if success else "no graph path"
        rows.append({
            "source": source,
            "destination": destination,
            "success": success,
            "hops": max(0, len(path) - 1),
            "reason": reason,
            "path": " -> ".join(str(node_id) for node_id in path),
            "delay_ms": delay_ms,
        })
    return rows


def write_dijkstra_csv(rows: list[dict], output_file: str | Path) -> None:
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "source",
            "destination",
            "success",
            "hops",
            "reason",
            "delay_ms",
            "path",
        ])
        writer.writeheader()
        writer.writerows(rows)


def print_dijkstra_summary(rows: list[dict], title: str) -> None:
    """Print a short Dijkstra result summary."""

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Constrained Dijkstra on Starlink-like data.")
    parser.add_argument("--nodes", default=DEFAULT_NODES_FILE)
    parser.add_argument("--edges", default=DEFAULT_EDGES_FILE)
    parser.add_argument("--pairs", type=int, default=50)
    parser.add_argument("--d-th-km", type=float, default=DEFAULT_D_TH_KM)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        default=None,
        help="Optional CSV output path. If omitted, the default constrained-Dijkstra file is used.",
    )
    args = parser.parse_args()

    rows = run_dijkstra(
        args.nodes,
        args.edges,
        args.pairs,
        args.seed,
        d_th_km=args.d_th_km,
    )
    title = "Dijkstra routing summary"
    output_file = args.out or (Path("results") / "final_dijkstra_routes.csv")
    print_dijkstra_summary(rows, title)
    write_dijkstra_csv(rows, output_file)
    print(f"Saved Dijkstra routes to {output_file}")


if __name__ == "__main__":
    main()
