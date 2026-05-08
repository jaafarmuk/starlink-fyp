"""Generate markdown comparison tables from analysis results."""
from __future__ import annotations
import json
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
ANALYSIS = REPO / "results" / "analysis" / "full"
TABLES = REPO / "results" / "tables"
TABLES.mkdir(parents=True, exist_ok=True)

D_THS = [3000, 3500, 4000, 5000]


def load(dth):
    with open(ANALYSIS / f"dth{dth}_summary.json") as f:
        return json.load(f)


def density_rows(s):
    return [r for r in s["rows"] if r["strategy"] == "density"]


def m(rows, key):
    vals = [r[key] for r in rows
            if r.get(key) is not None
            and not (isinstance(r[key], float) and np.isnan(r[key]))]
    return float(np.mean(vals)) if vals else float("nan")


def fmt(x, fmt_str="{:.4f}"):
    if isinstance(x, float) and np.isnan(x):
        return "—"
    return fmt_str.format(x)


def write_main_comparison():
    """Master table: routing-method comparison across d_th."""
    lines = ["# Main Comparison: Routing Methods × d_th",
             "",
             "Full Walker-delta constellation — 4,080 satellites + 250 gateways.",
             "Mean over 10 epochs, 300 src-dst pairs each, density strategy.",
             "Queuing: 1 Gbps ISL, 200 Mbps offered load (rho=0.2), 1500-byte packets.",
             "",
             "## Interruption probability (lower is better)",
             "",
             "| d_th (km) | Greedy (Wang) | BPP analytical | Dijkstra | BPP error |",
             "|---|---|---|---|---|"]
    for dth in D_THS:
        try:
            d = load(dth)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        emp = m(r, "empirical_interruption_probability")
        bpp = m(r, "bpp_predicted_interruption_probability")
        dij = m(r, "dijkstra_interruption_probability")
        err = abs(emp - bpp)
        lines.append(f"| {dth} | {fmt(emp)} | {fmt(bpp)} | {fmt(dij)} | {fmt(err)} |")

    lines += ["",
              "## Mean hop count (successful routes only)",
              "",
              "| d_th (km) | Greedy hops | Dijkstra hops |",
              "|---|---|---|"]
    for dth in D_THS:
        try:
            d = load(dth)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        gh = m(r, "greedy_hops_mean")
        dh = m(r, "dijkstra_hops_mean")
        lines.append(f"| {dth} | {fmt(gh, '{:.2f}')} | {fmt(dh, '{:.2f}')} |")

    lines += ["",
              "## Mean end-to-end latency in ms (prop + queue, successful routes)",
              "",
              "| d_th (km) | Greedy latency (ms) | Dijkstra latency (ms) | Speed-up |",
              "|---|---|---|---|"]
    for dth in D_THS:
        try:
            d = load(dth)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        gl = m(r, "greedy_total_ms_mean")
        dl = m(r, "dijkstra_total_ms_mean")
        speedup = (gl / dl) if (dl and not np.isnan(dl)) else float("nan")
        lines.append(f"| {dth} | {fmt(gl, '{:.2f}')} | {fmt(dl, '{:.2f}')} | {fmt(speedup, '{:.2f}x')} |")

    out = TABLES / "01_main_comparison.md"
    out.write_text("\n".join(lines))
    print(f"  → {out}")


def write_distributions():
    """Hop and latency distributions at d_th=4000."""
    lines = ["# Distribution Tables — d_th = 4000 km, density strategy",
             "",
             "Full Walker-delta (4,080 sats). Mean over 10 epochs.",
             ""]
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    r = density_rows(d)

    lines += ["## Hop count distribution",
              "",
              "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
              "|---|---|---|---|---|---|"]
    for algo, prefix in [("Greedy (Wang)", "greedy_hops"), ("Dijkstra", "dijkstra_hops")]:
        row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.2f}")
                        for l in ["mean", "p50", "p90", "p95", "p99"]]
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    lines += ["## Distance distribution (km)",
              "",
              "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
              "|---|---|---|---|---|---|"]
    for algo, prefix in [("Greedy (Wang)", "greedy_distance_km"),
                         ("Dijkstra", "dijkstra_distance_km")]:
        row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.0f}")
                        for l in ["mean", "p50", "p90", "p95", "p99"]]
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    lines += ["## End-to-end latency distribution (ms, prop+queue)",
              "",
              "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
              "|---|---|---|---|---|---|"]
    for algo, prefix in [("Greedy (Wang)", "greedy_total_ms"),
                         ("Dijkstra", "dijkstra_total_ms")]:
        row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.2f}")
                        for l in ["mean", "p50", "p90", "p95", "p99"]]
        lines.append("| " + " | ".join(row) + " |")

    out = TABLES / "02_distributions.md"
    out.write_text("\n".join(lines))
    print(f"  → {out}")


def write_strategy_table():
    """Strategy comparison at d_th=4000."""
    lines = ["# Strategy Comparison — d_th = 4000 km",
             "",
             "Full Walker-delta (4,080 sats). Mean over 10 epochs.",
             "",
             "| Strategy | Greedy emp | BPP pred | Dijkstra emp | BPP error |",
             "|---|---|---|---|---|"]
    try:
        d = load(4000)
    except FileNotFoundError:
        return
    for s in ["density", "single_hop", "stationary_optimal"]:
        rows = [r for r in d["rows"] if r["strategy"] == s]
        if not rows:
            continue
        emp = m(rows, "empirical_interruption_probability")
        bpp = m(rows, "bpp_predicted_interruption_probability")
        dij = m(rows, "dijkstra_interruption_probability")
        err = abs(emp - bpp)
        lines.append(f"| {s} | {fmt(emp)} | {fmt(bpp)} | {fmt(dij)} | {fmt(err)} |")

    out = TABLES / "03_strategy_comparison.md"
    out.write_text("\n".join(lines))
    print(f"  → {out}")


def main():
    print(f"Writing tables to {TABLES}/")
    write_main_comparison()
    write_distributions()
    write_strategy_table()
    print("Done.")


if __name__ == "__main__":
    main()
