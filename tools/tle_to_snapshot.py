import argparse
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sgp4.api import Satrec


EARTH_RADIUS_KM = 6371.0
C_KM_S = 299792.458

# LEO sanity band for Starlink-like constellations. Altitudes below ~150 km
# decay very quickly, and Starlink operates well below 2500 km. Any SGP4
# output outside this band is almost certainly a numerical blow-up caused by
# propagating far from the TLE epoch.
MIN_ALTITUDE_KM = 150.0
MAX_ALTITUDE_KM = 2500.0
MIN_RADIUS_KM = EARTH_RADIUS_KM + MIN_ALTITUDE_KM
MAX_RADIUS_KM = EARTH_RADIUS_KM + MAX_ALTITUDE_KM


def utc_to_jd(utc_iso):
    """Convert an ISO-8601 UTC string (e.g. '2026-03-21T12:00:00') to (jd, fr)."""
    dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)

    y, m, d = dt.year, dt.month, dt.day
    if m <= 2:
        y -= 1
        m += 12
    a = y // 100
    b = 2 - a + a // 4
    jd_day = int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + b - 1524
    # JD at 0h UT
    jd0 = float(jd_day) - 0.5
    frac = (dt.hour + dt.minute / 60.0 + (dt.second + dt.microsecond / 1e6) / 3600.0) / 24.0
    return jd0, frac


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
    ap.add_argument(
        "--utc",
        default=None,
        help="UTC timestamp (ISO-8601) to propagate all satellites to. "
             "If omitted, each satellite is evaluated at its own TLE epoch.",
    )
    ap.add_argument("--jd", type=float, default=None,
                    help="Override Julian day (advanced). Used together with --fr.")
    ap.add_argument("--fr", type=float, default=0.0,
                    help="Fractional day offset for --jd (advanced).")

    ap.add_argument("--max_degree", type=int, default=4)
    ap.add_argument("--intra_plane", type=int, default=2)
    ap.add_argument("--inter_plane", type=int, default=2)
    ap.add_argument("--raan_tol_deg", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=None,
                    help="Optional seed for deterministic TLE sampling.")
    ap.add_argument("--sample", choices=["head", "random"], default="head",
                    help="How to select --n TLEs from the dataset.")
    ap.add_argument("--stats_out", default="results/topology_stats.csv",
                    help="CSV file to write topology summary statistics into.")

    args = ap.parse_args()

    # Resolve propagation epoch policy.
    common_jd = None
    common_fr = None
    epoch_mode = "tle"
    if args.jd is not None:
        common_jd = float(args.jd)
        common_fr = float(args.fr)
        epoch_mode = f"jd={common_jd}+{common_fr}"
    elif args.utc is not None:
        common_jd, common_fr = utc_to_jd(args.utc)
        epoch_mode = f"utc={args.utc}"

    all_tles = read_tles(args.tle)
    if args.sample == "random":
        rng = np.random.default_rng(args.seed)
        idx = rng.choice(len(all_tles), size=min(args.n, len(all_tles)), replace=False)
        raw_sats = [all_tles[i] for i in sorted(idx.tolist())]
    else:
        raw_sats = all_tles[:args.n]

    satrecs = [Satrec.twoline2rv(l1, l2) for _, l1, l2 in raw_sats]

    sats = []
    rejected_sgp4 = 0
    rejected_altitude = 0
    for i, ((name, l1, l2), s) in enumerate(zip(raw_sats, satrecs)):
        if common_jd is None:
            jd_eval = s.jdsatepoch
            fr_eval = s.jdsatepochF
        else:
            jd_eval = common_jd
            fr_eval = common_fr

        e, r, _ = s.sgp4(jd_eval, fr_eval)
        if e != 0:
            rejected_sgp4 += 1
            continue

        radius = float(np.linalg.norm(r))
        if not (MIN_RADIUS_KM <= radius <= MAX_RADIUS_KM):
            rejected_altitude += 1
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

    # Connectivity stats (approximate, via BFS over undirected graph).
    adj = defaultdict(list)
    for a, b, _, _ in edges:
        adj[a].append(b)
        adj[b].append(a)

    visited = set()
    largest = 0
    for sid in (sat["id"] for sat in sats):
        if sid in visited:
            continue
        stack = [sid]
        size = 0
        while stack:
            x = stack.pop()
            if x in visited:
                continue
            visited.add(x)
            size += 1
            stack.extend(adj[x])
        if size > largest:
            largest = size

    isolated = sum(1 for sat in sats if degree[sat["id"]] == 0)

    print(f"Epoch mode: {epoch_mode}")
    print(f"Requested TLEs: {len(raw_sats)}")
    print(f"Rejected (SGP4 error): {rejected_sgp4}")
    print(f"Rejected (altitude out of LEO band): {rejected_altitude}")
    print(f"Valid satellites: {N}")
    print(f"Planes: {len(planes)}")
    print(f"Edges: {len(edge_rows)}")
    print(f"Isolated nodes: {isolated}")
    print(f"Largest connected component: {largest} / {N}")
    mean_isl_km = sum(d for _, _, d, _ in edges) / len(edges) if edges else 0.0
    max_isl_km = max((d for _, _, d, _ in edges), default=0.0)
    mean_deg = sum(degree.values()) / N if N > 0 else 0.0
    max_deg = max(degree.values()) if degree else 0

    pd.DataFrame([{
        "num_nodes": N,
        "num_edges": len(edge_rows),
        "num_planes": len(planes),
        "isolated_nodes": isolated,
        "largest_cc_size": largest,
        "mean_degree": round(mean_deg, 3),
        "max_degree": max_deg,
        "mean_isl_distance_km": round(mean_isl_km, 1),
        "max_isl_distance_km": round(max_isl_km, 1),
    }]).to_csv(args.stats_out, index=False)

    print(f"Wrote {args.edges_out}")
    print(f"Wrote {args.nodes_out}")
    print(f"Wrote {args.stats_out}")

    # Warnings. Written to stderr so they stand out but do not break pipelines.
    if rejected_altitude > 0:
        print(
            f"WARNING: {rejected_altitude} satellite(s) rejected for unrealistic "
            f"altitude. Consider using --utc near the TLE epoch, or refreshing "
            f"the TLE dataset.",
            file=sys.stderr,
        )
    if N > 0 and isolated / N > 0.25:
        print(
            f"WARNING: {isolated}/{N} satellites have no links "
            f"({isolated / N:.0%}). The topology is sparse; consider relaxing "
            f"--max_km or raising --max_degree.",
            file=sys.stderr,
        )
    if N > 0 and largest / N < 0.5:
        print(
            f"WARNING: largest connected component covers only "
            f"{largest}/{N} nodes ({largest / N:.0%}). Flow generation in the "
            f"ns-3 scenario will only use that component.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
