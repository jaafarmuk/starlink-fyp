import argparse
import math
from collections import defaultdict

import numpy as np
import pandas as pd
from sgp4.api import Satrec


EARTH_RADIUS_KM = 6371.0
C_KM_S = 299792.458


def read_tles(path):
    lines = [l.strip() for l in open(path, "r", encoding="utf-8", errors="ignore") if l.strip()]
    sats = []
    i = 0
    while i < len(lines):
        if i + 2 < len(lines) and lines[i + 1].startswith("1 ") and lines[i + 2].startswith("2 "):
            name, l1, l2 = lines[i], lines[i + 1], lines[i + 2]
            sats.append((name, l1, l2))
            i += 3
        elif i + 1 < len(lines) and lines[i].startswith("1 ") and lines[i + 1].startswith("2 "):
            l1, l2 = lines[i], lines[i + 1]
            name = f"SAT_{len(sats)}"
            sats.append((name, l1, l2))
            i += 2
        else:
            i += 1
    return sats


def ang_diff_deg(a, b):
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def tle_raan_deg(line2):
    # TLE line 2 columns 18-25 (1-based), so Python slice [17:25]
    return float(line2[17:25].strip())


def has_line_of_sight(r1, r2, earth_radius_km=EARTH_RADIUS_KM):
    r1 = np.array(r1, dtype=float)
    r2 = np.array(r2, dtype=float)
    d = r2 - r1
    denom = np.dot(d, d)
    if denom == 0:
        return False

    t = -np.dot(r1, d) / denom
    t = max(0.0, min(1.0, t))
    closest = r1 + t * d
    return np.linalg.norm(closest) > earth_radius_km


def distance_km(r1, r2):
    return float(np.linalg.norm(np.array(r1, dtype=float) - np.array(r2, dtype=float)))


def cluster_planes(sats, raan_tol_deg):
    """
    Group satellites into approximate orbital planes using RAAN proximity.
    Returns:
      planes: dict[plane_id] -> list of sat indices
      plane_order: list of plane_ids sorted by mean RAAN
    """
    indexed = sorted(
        [(sat["id"], sat["raan"]) for sat in sats],
        key=lambda x: x[1]
    )

    planes = []
    current = [indexed[0]]

    for item in indexed[1:]:
        _, raan = item
        _, prev_raan = current[-1]
        if ang_diff_deg(raan, prev_raan) <= raan_tol_deg:
            current.append(item)
        else:
            planes.append(current)
            current = [item]
    planes.append(current)

    # Merge first/last cluster if they are close across 0/360 wrap
    if len(planes) > 1:
        first_raan = planes[0][0][1]
        last_raan = planes[-1][-1][1]
        if ang_diff_deg(first_raan, last_raan) <= raan_tol_deg:
            merged = planes[-1] + planes[0]
            planes = [merged] + planes[1:-1]

    plane_map = {}
    plane_means = []

    for pid, plane in enumerate(planes):
        ids = [sid for sid, _ in plane]
        mean_raan = sum(raan for _, raan in plane) / len(plane)
        plane_means.append((pid, mean_raan))
        plane_map[pid] = ids

    plane_order = [pid for pid, _ in sorted(plane_means, key=lambda x: x[1])]

    return plane_map, plane_order


def assign_plane_ids(sats, planes):
    sat_to_plane = {}
    for pid, ids in planes.items():
        for sid in ids:
            sat_to_plane[sid] = pid
    for sat in sats:
        sat["plane_id"] = sat_to_plane[sat["id"]]


def sort_plane_members(sats, planes):
    """
    Sort satellites within each plane using a simple angular ordering in ECI.
    This is an approximation, but good enough for a structured snapshot model.
    """
    sat_by_id = {sat["id"]: sat for sat in sats}
    ordered = {}

    for pid, ids in planes.items():
        members = [sat_by_id[sid] for sid in ids]
        members.sort(key=lambda s: math.atan2(s["y"], s["x"]))
        ordered[pid] = [m["id"] for m in members]

    return ordered


def try_add_edge(u, v, sats_by_id, degree, edge_pairs, edges, max_km, max_degree):
    if u == v:
        return False
    a, b = sorted((u, v))
    if (a, b) in edge_pairs:
        return False
    if degree[a] >= max_degree or degree[b] >= max_degree:
        return False

    r1 = sats_by_id[a]["r"]
    r2 = sats_by_id[b]["r"]

    if not has_line_of_sight(r1, r2):
        return False

    d = distance_km(r1, r2)
    if d > max_km:
        return False

    delay_ms = (d / C_KM_S) * 1000.0
    edge_pairs.add((a, b))
    edges.append((a, b, d, delay_ms))
    degree[a] += 1
    degree[b] += 1
    return True


def ring_neighbors(ordered_ids, sid, count):
    n = len(ordered_ids)
    if n < 2 or count <= 0:
        return []

    idx = ordered_ids.index(sid)
    candidates = []

    offsets = []
    step = 1
    while len(offsets) < count:
        offsets.append(-step)
        if len(offsets) < count:
            offsets.append(step)
        step += 1

    for off in offsets:
        nbr = ordered_ids[(idx + off) % n]
        if nbr != sid and nbr not in candidates:
            candidates.append(nbr)

    return candidates[:count]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tle", required=True)
    ap.add_argument("--edges_out", default="results/snapshot_edges.csv")
    ap.add_argument("--nodes_out", default="results/snapshot_nodes.csv")
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--max_km", type=float, default=5000.0)
    ap.add_argument("--jd", type=float, default=2460000.5)
    ap.add_argument("--fr", type=float, default=0.0)

    ap.add_argument("--max_degree", type=int, default=4)
    ap.add_argument("--intra_plane", type=int, default=2)
    ap.add_argument("--inter_plane", type=int, default=2)
    ap.add_argument("--raan_tol_deg", type=float, default=5.0)

    args = ap.parse_args()

    raw_sats = read_tles(args.tle)[:args.n]
    satrecs = [Satrec.twoline2rv(l1, l2) for _, l1, l2 in raw_sats]

    sats = []
    for i, ((name, l1, l2), s) in enumerate(zip(raw_sats, satrecs)):
        e, r, _ = s.sgp4(args.jd, args.fr)
        if e != 0:
            continue

        try:
            raan = tle_raan_deg(l2)
        except Exception:
            continue

        sats.append({
            "id": len(sats),
            "orig_index": i,
            "name": name,
            "l1": l1,
            "l2": l2,
            "r": np.array(r, dtype=float),
            "x": float(r[0]),
            "y": float(r[1]),
            "z": float(r[2]),
            "raan": raan,
        })

    N = len(sats)
    if N < 2:
        raise SystemExit("Not enough valid satellites.")

    sats_by_id = {sat["id"]: sat for sat in sats}

    planes, plane_order = cluster_planes(sats, args.raan_tol_deg)
    assign_plane_ids(sats, planes)
    ordered_planes = sort_plane_members(sats, planes)

    degree = {sat["id"]: 0 for sat in sats}
    edge_pairs = set()
    edges = []

    # Intra-plane links
    for pid, ordered_ids in ordered_planes.items():
        for sid in ordered_ids:
            nbrs = ring_neighbors(ordered_ids, sid, args.intra_plane)
            for nid in nbrs:
                try_add_edge(
                    sid, nid,
                    sats_by_id, degree, edge_pairs, edges,
                    args.max_km, args.max_degree
                )

    # Inter-plane links: only adjacent planes in RAAN order
    plane_pos = {pid: i for i, pid in enumerate(plane_order)}

    for sat in sats:
        sid = sat["id"]
        pid = sat["plane_id"]
        pos = plane_pos[pid]
        num_planes = len(plane_order)

        if num_planes < 2 or args.inter_plane <= 0:
            continue

        adjacent_pids = set()
        adjacent_pids.add(plane_order[(pos - 1) % num_planes])
        adjacent_pids.add(plane_order[(pos + 1) % num_planes])

        candidates = []
        for adj_pid in adjacent_pids:
            for nid in ordered_planes[adj_pid]:
                if nid == sid:
                    continue
                if degree[sid] >= args.max_degree:
                    break
                if degree[nid] >= args.max_degree:
                    continue

                r1 = sats_by_id[sid]["r"]
                r2 = sats_by_id[nid]["r"]
                if not has_line_of_sight(r1, r2):
                    continue

                d = distance_km(r1, r2)
                if d <= args.max_km:
                    candidates.append((d, nid))

        candidates.sort(key=lambda x: x[0])

        chosen = 0
        for _, nid in candidates:
            added = try_add_edge(
                sid, nid,
                sats_by_id, degree, edge_pairs, edges,
                args.max_km, args.max_degree
            )
            if added:
                chosen += 1
            if chosen >= args.inter_plane:
                break

    edges.sort(key=lambda x: (x[0], x[1]))

    edge_rows = [{
        "u": a,
        "v": b,
        "distance_km": d,
        "delay_ms": delay_ms
    } for a, b, d, delay_ms in edges]

    node_rows = [{
        "id": sat["id"],
        "name": sat["name"],
        "plane_id": sat["plane_id"],
        "raan_deg": sat["raan"],
        "x_km": sat["x"],
        "y_km": sat["y"],
        "z_km": sat["z"],
        "degree": degree[sat["id"]],
    } for sat in sats]

    pd.DataFrame(edge_rows).to_csv(args.edges_out, index=False)
    pd.DataFrame(node_rows).to_csv(args.nodes_out, index=False)

    print(f"Valid satellites: {N}")
    print(f"Planes: {len(planes)}")
    print(f"Edges: {len(edge_rows)}")
    print(f"Wrote {args.edges_out}")
    print(f"Wrote {args.nodes_out}")


if __name__ == "__main__":
    main()
