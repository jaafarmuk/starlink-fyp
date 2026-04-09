import argparse
import numpy as np
import pandas as pd
from sgp4.api import Satrec

def read_tles(path):
    lines = [l.strip() for l in open(path, "r", encoding="utf-8", errors="ignore") if l.strip()]
    sats = []
    i = 0
    while i < len(lines):
        if i + 2 < len(lines) and lines[i+1].startswith("1 ") and lines[i+2].startswith("2 "):
            name, l1, l2 = lines[i], lines[i+1], lines[i+2]
            sats.append((name, l1, l2))
            i += 3
        elif i + 1 < len(lines) and lines[i].startswith("1 ") and lines[i+1].startswith("2 "):
            l1, l2 = lines[i], lines[i+1]
            name = f"SAT_{len(sats)}"
            sats.append((name, l1, l2))
            i += 2
        else:
            i += 1
    return sats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tle", required=True)
    ap.add_argument("--out", default="results/snapshot_edges.csv")
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--k", type=int, default=4)
    ap.add_argument("--max_km", type=float, default=2000.0)
    ap.add_argument("--jd", type=float, default=2460000.5)
    ap.add_argument("--fr", type=float, default=0.0)
    args = ap.parse_args()

    sats = read_tles(args.tle)[:args.n]
    satrecs = [Satrec.twoline2rv(l1, l2) for _, l1, l2 in sats]

    pos = []
    names = []
    for name, s in zip([x[0] for x in sats], satrecs):
        e, r, _ = s.sgp4(args.jd, args.fr)
        if e == 0:
            pos.append(r)
            names.append(name)

    pos = np.array(pos, dtype=float)
    N = len(pos)
    if N < 2:
        raise SystemExit("Not enough valid satellites.")

    diffs = pos[:, None, :] - pos[None, :, :]
    dist = np.sqrt(np.sum(diffs * diffs, axis=2))
    np.fill_diagonal(dist, np.inf)

    c_km_s = 299792.458
    edges = set()

    for i in range(N):
        nbrs = np.argsort(dist[i])[:args.k]
        for j in nbrs:
            d = dist[i, j]
            if d <= args.max_km:
                a, b = sorted((int(i), int(j)))
                edges.add((a, b, float(d)))

    rows = []
    for a, b, d in sorted(edges):
        rows.append({
            "u": a,
            "v": b,
            "distance_km": d,
            "delay_ms": (d / c_km_s) * 1000.0
        })

    pd.DataFrame(rows).to_csv(args.out, index=False)
    pd.DataFrame({"id": range(N), "name": names}).to_csv("results/snapshot_nodes.csv", index=False)

    print(f"Valid satellites: {N}")
    print(f"Edges: {len(rows)}")
    print(f"Wrote {args.out}")
    print("Wrote results/snapshot_nodes.csv")

if __name__ == "__main__":
    main()
