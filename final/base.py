"""Shared data loading and geometry helpers for the final experiments."""

from __future__ import annotations

import csv
import math
from collections import defaultdict
import random
from dataclasses import dataclass
from pathlib import Path


EARTH_RADIUS_KM = 6371.0
DEFAULT_D_TH_KM = 4000.0
DEFAULT_THETA_R = math.pi / 6.0
DEFAULT_THETA_S = math.pi / 10.0

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FINAL_DIR = Path(__file__).resolve().parent
DEFAULT_SNAPSHOT_DIR = FINAL_DIR / "generated"
DEFAULT_RESULTS_DIR = FINAL_DIR / "results"
DEFAULT_NODES_FILE = DEFAULT_SNAPSHOT_DIR / "snapshot_nodes.csv"
DEFAULT_EDGES_FILE = DEFAULT_SNAPSHOT_DIR / "snapshot_edges.csv"


@dataclass(frozen=True)
class Node:
    """One satellite or gateway from the snapshot CSV."""

    id: int
    name: str
    kind: str
    shell_id: int
    x: float
    y: float
    z: float


@dataclass(frozen=True)
class Edge:
    """One undirected link from the snapshot edges CSV."""

    u: int
    v: int
    distance_km: float
    delay_ms: float


def load_nodes(path: str | Path = DEFAULT_NODES_FILE) -> dict[int, Node]:
    """Read the snapshot nodes CSV into a dictionary: node_id -> Node."""

    nodes: dict[int, Node] = {}
    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            node = Node(
                id=int(row["id"]),
                name=row["name"],
                kind=row["kind"],
                shell_id=int(row["shell_id"]),
                x=float(row["eci_x_km"]),
                y=float(row["eci_y_km"]),
                z=float(row["eci_z_km"]),
            )
            nodes[node.id] = node
    return nodes


def load_edges(path: str | Path = DEFAULT_EDGES_FILE) -> list[Edge]:
    """Read the snapshot edges CSV."""

    edges: list[Edge] = []
    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            delay = row.get("prop_delay_ms") or row.get("delay_ms") or "0"
            edges.append(Edge(
                u=int(row["u"]),
                v=int(row["v"]),
                distance_km=float(row["distance_km"]),
                delay_ms=float(delay),
            ))
    return edges


def build_graph(edges_file: str | Path = DEFAULT_EDGES_FILE
                ) -> dict[int, list[tuple[int, float]]]:
    """Build the snapshot graph weighted by propagation delay."""

    graph: dict[int, list[tuple[int, float]]] = defaultdict(list)
    for edge in load_edges(edges_file):
        graph[edge.u].append((edge.v, edge.delay_ms))
        graph[edge.v].append((edge.u, edge.delay_ms))
    return graph


def random_point_on_sphere(radius_km: float,
                           rng: random.Random) -> tuple[float, float, float]:
    """Draw one point uniformly at random on a sphere."""

    z_unit = rng.uniform(-1.0, 1.0)
    phi = rng.uniform(0.0, 2.0 * math.pi)
    r_xy = math.sqrt(max(0.0, 1.0 - z_unit * z_unit))
    return (
        radius_km * r_xy * math.cos(phi),
        radius_km * r_xy * math.sin(phi),
        radius_km * z_unit,
    )


def build_random_nodes(seed: int,
                       gateway_count: int = 300,
                       shell_altitudes_km: tuple[float, ...] = (575.0, 1200.0),
                       shell_counts: tuple[int, ...] = (140, 720)
                       ) -> dict[int, Node]:
    """Build a paper-like random topology: gateways plus random LEO shells."""

    if len(shell_altitudes_km) != len(shell_counts):
        raise ValueError("shell_altitudes_km and shell_counts must match in length")

    rng = random.Random(seed)
    nodes: dict[int, Node] = {}
    next_id = 0

    for gateway_index in range(gateway_count):
        x, y, z = random_point_on_sphere(EARTH_RADIUS_KM, rng)
        nodes[next_id] = Node(
            id=next_id,
            name=f"gateway-{gateway_index}",
            kind="gateway",
            shell_id=-1,
            x=x,
            y=y,
            z=z,
        )
        next_id += 1

    for shell_id, (altitude_km, count) in enumerate(zip(shell_altitudes_km, shell_counts)):
        radius_km = EARTH_RADIUS_KM + altitude_km
        for sat_index in range(count):
            x, y, z = random_point_on_sphere(radius_km, rng)
            nodes[next_id] = Node(
                id=next_id,
                name=f"shell{shell_id}-sat-{sat_index}",
                kind="satellite",
                shell_id=shell_id,
                x=x,
                y=y,
                z=z,
            )
            next_id += 1

    return nodes


def build_graph_from_nodes(nodes: dict[int, Node],
                           d_th_km: float = DEFAULT_D_TH_KM
                           ) -> dict[int, list[tuple[int, float]]]:
    """Build a physical graph from node geometry using distance and LoS."""

    graph: dict[int, list[tuple[int, float]]] = defaultdict(list)
    node_ids = list(nodes.keys())
    for index, left_id in enumerate(node_ids):
        left = nodes[left_id]
        for right_id in node_ids[index + 1:]:
            right = nodes[right_id]
            dist_km = distance_km(left, right)
            if dist_km > d_th_km:
                continue
            if not has_line_of_sight(left, right):
                continue
            delay_ms = (dist_km / 299_792.458) * 1000.0
            graph[left_id].append((right_id, delay_ms))
            graph[right_id].append((left_id, delay_ms))
    return graph


def length(vector: tuple[float, float, float]) -> float:
    """Euclidean length of a 3D vector."""

    x, y, z = vector
    return math.sqrt(x * x + y * y + z * z)


def vector_between(a: Node, b: Node) -> tuple[float, float, float]:
    """Vector from node a to node b."""

    return (b.x - a.x, b.y - a.y, b.z - a.z)


def position(node: Node) -> tuple[float, float, float]:
    """Node position vector from Earth centre."""

    return (node.x, node.y, node.z)


def dot(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    """Dot product for two 3D vectors."""

    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


def distance_km(a: Node, b: Node) -> float:
    """Straight-line distance between two nodes in km."""

    return length(vector_between(a, b))


def angle_between(a: tuple[float, float, float],
                  b: tuple[float, float, float]) -> float:
    """Angle between two vectors in radians."""

    a_len = length(a)
    b_len = length(b)
    if a_len == 0.0 or b_len == 0.0:
        return 0.0

    cos_angle = dot(a, b) / (a_len * b_len)
    cos_angle = max(-1.0, min(1.0, cos_angle))
    return math.acos(cos_angle)

    "This computes angles safely"
    "and avoids numerical errors by clamping the cosine value to the valid range [-1, 1]."


def dome_angle(a: Node, b: Node) -> float:
    """Angle between two nodes as seen from Earth's centre."""

    return angle_between(position(a), position(b))


def direction_angle(current: Node, candidate: Node, destination: Node) -> float:
    """Angle between the candidate direction and final destination direction."""

    to_candidate = vector_between(current, candidate)
    to_destination = vector_between(current, destination)
    return angle_between(to_candidate, to_destination)


def has_line_of_sight(a: Node, b: Node,
                      earth_radius_km: float = EARTH_RADIUS_KM) -> bool:
    """Return True when the link segment does not pass through Earth."""

    ax, ay, az = position(a)
    dx, dy, dz = vector_between(a, b)
    d2 = dx * dx + dy * dy + dz * dz
    if d2 == 0.0:
        return False

    # Closest point on the segment to Earth's centre.
    t = -(ax * dx + ay * dy + az * dz) / d2
    t = max(0.0, min(1.0, t))
    closest = (ax + t * dx, ay + t * dy, az + t * dz)
    return length(closest) >= earth_radius_km


def gateway_ids(nodes: dict[int, Node]) -> list[int]:
    """All gateway node IDs."""

    return [node.id for node in nodes.values() if node.kind == "gateway"]


def choose_gateway_pairs(node_ids: list[int],
                         pair_count: int,
                         seed: int) -> list[tuple[int, int]]:
    """Choose random source/destination gateway pairs."""

    rng = random.Random(seed)
    all_pairs = [
        (source, destination)
        for source in node_ids
        for destination in node_ids
        if source != destination
    ]
    rng.shuffle(all_pairs)
    return all_pairs[:pair_count]


def can_use_link(current: Node,
                 candidate: Node,
                 destination: Node,
                 d_th_km: float = DEFAULT_D_TH_KM,
                 theta_r: float = DEFAULT_THETA_R,
                 theta_s: float = DEFAULT_THETA_S) -> bool:
    """Check the Wang-style constraints for one possible hop."""

    if distance_km(current, candidate) > d_th_km:
        return False
    if not has_line_of_sight(current, candidate):
        return False

    # Gateway hops are relaxed because ground-to-satellite geometry is
    # different from satellite-to-satellite relay geometry.
    gateway_hop = current.kind == "gateway" or candidate.kind == "gateway"
    if gateway_hop:
        return True

    if direction_angle(current, candidate, destination) > theta_r:
        return False
    if dome_angle(current, candidate) < theta_s:
        return False

    return True
