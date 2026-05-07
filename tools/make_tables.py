"""Generate markdown comparison tables from analysis results."""
from __future__ import annotations
import json
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
ANALYSIS = REPO / "results" / "analysis"
TABLES = REPO / "results" / "tables"
TABLES.mkdir(parents=True, exist_ok=True)

D_THS = [3000, 3500, 4000, 5000]
VARIANTS = [("full", "Full Walker-delta (4,080 sats)"),
            ("n600", "Random subsample (600 sats)")]


def load(variant, dth):
    with open(ANALYSIS / variant / f"dth{dth}_summary.json") as f:
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
    """Master table: routing-method comparison across d_th and N."""
    lines = ["# Main Comparison: Routing Methods × Satellite Count × d_th",
             "",
             "Mean over 10 epochs, 300 src-dst pairs each, density strategy.",
             "Queuing: 1 Gbps ISL, 200 Mbps offered load (rho=0.2), 1500-byte packets.",
             "",
             "## Interruption probability (lower is better)",
             "",
             "| N | d_th (km) | Greedy (Wang) | BPP analytical | Dijkstra | BPP error |",
             "|---|---|---|---|---|---|"]
    for variant, label in VARIANTS:
        for dth in D_THS:
            try:
                d = load(variant, dth)
            except FileNotFoundError:
                continue
            r = density_rows(d)
            emp = m(r, "empirical_interruption_probability")
            bpp = m(r, "bpp_predicted_interruption_probability")
            dij = m(r, "dijkstra_interruption_probability")
            err = abs(emp - bpp)
            lines.append(f"| {variant} | {dth} | {fmt(emp)} | {fmt(bpp)} | "
                         f"{fmt(dij)} | {fmt(err)} |")
        lines.append("|   |   |   |   |   |   |")

    lines += ["",
              "## Mean hop count (successful routes only)",
              "",
              "| N | d_th (km) | Greedy hops | Dijkstra hops |",
              "|---|---|---|---|"]
    for variant, _ in VARIANTS:
        for dth in D_THS:
            try:
                d = load(variant, dth)
            except FileNotFoundError:
                continue
            r = density_rows(d)
            gh = m(r, "greedy_hops_mean")
            dh = m(r, "dijkstra_hops_mean")
            lines.append(f"| {variant} | {dth} | {fmt(gh, '{:.2f}')} | {fmt(dh, '{:.2f}')} |")
        lines.append("|   |   |   |   |")

    lines += ["",
              "## Mean end-to-end latency in ms (prop + queue, successful routes)",
              "",
              "| N | d_th (km) | Greedy latency (ms) | Dijkstra latency (ms) | Speed-up |",
              "|---|---|---|---|---|"]
    for variant, _ in VARIANTS:
        for dth in D_THS:
            try:
                d = load(variant, dth)
            except FileNotFoundError:
                continue
            r = density_rows(d)
            gl = m(r, "greedy_total_ms_mean")
            dl = m(r, "dijkstra_total_ms_mean")
            speedup = (gl / dl) if (dl and not np.isnan(dl)) else float("nan")
            lines.append(f"| {variant} | {dth} | {fmt(gl, '{:.2f}')} | "
                         f"{fmt(dl, '{:.2f}')} | {fmt(speedup, '{:.2f}x')} |")
        lines.append("|   |   |   |   |   |")

    out = TABLES / "01_main_comparison.md"
    out.write_text("\n".join(lines))
    print(f"  → {out}")


def write_distributions():
    """Hop and latency distributions at d_th=4000."""
    lines = ["# Distribution Tables — d_th = 4000 km, density strategy",
             "",
             "Mean over 10 epochs.",
             ""]

    for variant, label in VARIANTS:
        try:
            d = load(variant, 4000)
        except FileNotFoundError:
            continue
        r = density_rows(d)
        lines += [f"## {label}", ""]

        lines += ["### Hop count distribution",
                  "",
                  "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
                  "|---|---|---|---|---|---|"]
        for algo, prefix in [("Greedy (Wang)", "greedy_hops"), ("Dijkstra", "dijkstra_hops")]:
            row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.2f}")
                            for l in ["mean", "p50", "p90", "p95", "p99"]]
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

        lines += ["### Distance distribution (km)",
                  "",
                  "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
                  "|---|---|---|---|---|---|"]
        for algo, prefix in [("Greedy (Wang)", "greedy_distance_km"),
                             ("Dijkstra", "dijkstra_distance_km")]:
            row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.0f}")
                            for l in ["mean", "p50", "p90", "p95", "p99"]]
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

        lines += ["### End-to-end latency distribution (ms, prop+queue)",
                  "",
                  "| Algorithm | Mean | P50 | P90 | P95 | P99 |",
                  "|---|---|---|---|---|---|"]
        for algo, prefix in [("Greedy (Wang)", "greedy_total_ms"),
                             ("Dijkstra", "dijkstra_total_ms")]:
            row = [algo] + [fmt(m(r, f"{prefix}_{l}"), "{:.2f}")
                            for l in ["mean", "p50", "p90", "p95", "p99"]]
            lines.append("| " + " | ".join(row) + " |")
        lines += ["", ""]

    out = TABLES / "02_distributions.md"
    out.write_text("\n".join(lines))
    print(f"  → {out}")


def write_strategy_table():
    """Strategy comparison at d_th=4000."""
    lines = ["# Strategy Comparison — d_th = 4000 km",
             "",
             "Mean over 10 epochs.",
             ""]
    for variant, label in VARIANTS:
        try:
            d = load(variant, 4000)
        except FileNotFoundError:
            continue
        lines += [f"## {label}",
                  "",
                  "| Strategy | Greedy emp | BPP pred | Dijkstra emp | BPP error |",
                  "|---|---|---|---|---|"]
        for s in ["density", "single_hop", "stationary_optimal"]:
            rows = [r for r in d["rows"] if r["strategy"] == s]
            if not rows:
                continue
            emp = m(rows, "empirical_interruption_probability")
            bpp = m(rows, "bpp_predicted_interruption_probability")
            dij = m(rows, "dijkstra_interruption_probability")
            err = abs(emp - bpp)
            lines.append(f"| {s} | {fmt(emp)} | {fmt(bpp)} | {fmt(dij)} | {fmt(err)} |")
        lines.append("")
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
