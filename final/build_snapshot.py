"""Generate the Starlink snapshot used by the final greedy/Dijkstra scripts.

This file owns the final-project snapshot workflow.  It reuses the lower-level
TLE parsing, propagation, geometry, validation, and CSV helpers from
tools/tle_to_snapshot.py, but it builds the snapshot in-process instead of
launching that script through subprocess.
"""

from __future__ import annotations

import argparse
import importlib
import math
import os
import sys
from pathlib import Path

from base import DEFAULT_SNAPSHOT_DIR, PROJECT_ROOT

TOOLS_DIR = PROJECT_ROOT / "tools"
tle_snapshot = None


def add_project_venv_site_packages() -> None:
    """Let direct python3 runs see packages installed in the project venv."""

    site_root = PROJECT_ROOT / ".venv" / "lib"
    if not site_root.exists():
        return
    for site_packages in site_root.glob("python*/site-packages"):
        site_path = str(site_packages)
        if site_path not in sys.path:
            sys.path.insert(0, site_path)


def load_tle_snapshot_module():
    """Import the lower-level TLE helper module only when a build is requested."""

    global tle_snapshot
    if tle_snapshot is not None:
        return tle_snapshot

    add_project_venv_site_packages()
    if str(TOOLS_DIR) not in sys.path:
        sys.path.insert(0, str(TOOLS_DIR))
    tle_snapshot = importlib.import_module("tle_to_snapshot")
    return tle_snapshot


def generator_args(output_dir: Path,
                   seed: int,
                   epoch_steps: int,
                   multi_epoch_seconds: int) -> argparse.Namespace:
    """Return the generator settings used for the final-project snapshot."""

    return argparse.Namespace(
        tle=str(PROJECT_ROOT / "datasets" / "starlink.tle"),
        live=False,
        edges_out=str(output_dir / "snapshot_edges.csv"),
        nodes_out=str(output_dir / "snapshot_nodes.csv"),
        stats_out=str(output_dir / "topology_stats.csv"),
        meta_out=str(output_dir / "snapshot_meta.json"),
        n=9999,
        sample="random",
        shell_select="top2_separated",
        min_alt_sep_km=50.0,
        seed=seed,
        starlink_operational=True,
        min_altitude_km=None,
        max_altitude_km=None,
        max_eccentricity=None,
        min_mean_motion_rev_day=None,
        max_mean_motion_rev_day=None,
        utc=None,
        jd=None,
        fr=0.0,
        allow_tle_epoch=False,
        max_km=5000.0,
        min_isl_km=100.0,
        max_degree=4,
        intra_plane=2,
        inter_plane=2,
        raan_tol_deg=5.0,
        inc_tol_deg=1.0,
        alt_tol_km=25.0,
        disable_isl_above_abs_lat_deg=70.0,
        no_seam_avoidance=False,
        gateways_csv=str(
            PROJECT_ROOT / "datasets" / "starlink_ground_stations_hf_operational_safe.csv"
        ),
        no_gateways=False,
        gs_min_elevation_deg=5.0,
        gs_max_range_km=2000.0,
        gs_max_sats=4,
        gs_max_per_sat=2,
        multi_epoch_seconds=multi_epoch_seconds,
        epoch_steps=epoch_steps,
        min_largest_cc_frac=0.5,
        max_isolated_frac=0.25,
        strict=True,
    )


def load_and_select_satellites(args: argparse.Namespace):
    """Read the TLE file, apply final filters, and choose the shell population."""

    generator = load_tle_snapshot_module()
    all_tles = generator.read_tles(args.tle)
    if not all_tles:
        raise SystemExit(f"No TLEs found in {args.tle}")

    all_satrecs = [generator.Satrec.twoline2rv(l1, l2) for _, l1, l2 in all_tles]
    filt = generator._resolve_filter(args)
    filtered_tles, filtered_srs, filter_reasons = generator.filter_operational(
        all_tles,
        all_satrecs,
        **filt,
    )
    dropped = len(all_tles) - len(filtered_tles)
    if dropped:
        print(
            f"Operational filter dropped {dropped}/{len(all_tles)} TLEs "
            f"({filter_reasons})."
        )
    if not filtered_tles:
        raise SystemExit(
            "All TLEs were filtered out. Loosen altitude, eccentricity, "
            "or mean-motion bounds."
        )

    shell_tles, shell_srs, shell_selection = generator.select_shell_population(
        filtered_tles,
        filtered_srs,
        mode=args.shell_select,
        inc_tol_deg=args.inc_tol_deg,
        alt_tol_km=args.alt_tol_km,
        min_alt_sep_km=args.min_alt_sep_km,
    )
    if args.shell_select != "none":
        selected_shells = shell_selection.get(
            "selected_shells",
            [shell_selection["selected_shell"]],
        )
        desc = "; ".join(
            "inc "
            f"{shell['inc_min_deg']}-{shell['inc_max_deg']} deg, "
            f"perigee alt {shell['perigee_alt_min_km']}-"
            f"{shell['perigee_alt_max_km']} km, count={shell['count']}"
            for shell in selected_shells
        )
        print(
            "Shell selection kept "
            f"{shell_selection['selected_tles']}/{shell_selection['input_tles']} "
            f"post-filter TLEs across {len(selected_shells)} shell(s): {desc}."
        )

    raw_sats, satrecs = generator.sample_selected_population(
        shell_tles,
        shell_srs,
        shell_selection,
        n=args.n,
        sample_mode=args.sample,
        seed=args.seed,
    )
    return raw_sats, satrecs, shell_selection, filt, dropped, filter_reasons, len(all_tles), len(filtered_tles)


def load_gateways(args: argparse.Namespace) -> tuple[list[tuple[str, float, float, float]], str]:
    """Load the gateway list used by the final Starlink-like topology."""

    generator = load_tle_snapshot_module()
    if args.no_gateways:
        return [], "disabled"
    if args.gateways_csv and args.gateways_csv.strip():
        gateways = generator.read_gateway_csv(args.gateways_csv)
        gateway_source = "csv"
    else:
        gateways = list(generator.DEFAULT_GATEWAYS)
        gateway_source = "builtin_demo"
        print(
            "WARNING: using built-in DEMO gateway locations. Pass "
            "--gateways-csv data for real studies.",
            file=sys.stderr,
        )

    for gateway_name, _, _, _ in gateways:
        if "," in gateway_name or '"' in gateway_name:
            raise SystemExit(
                f"Gateway name {gateway_name!r} contains comma/quote; rename it."
            )
    return gateways, gateway_source


def write_snapshot_epochs(args: argparse.Namespace,
                          raw_sats,
                          satrecs,
                          jd: float,
                          fr: float,
                          epoch_mode: str,
                          gateways) -> list[dict]:
    """Build and write the base snapshot plus any requested time steps."""

    generator = load_tle_snapshot_module()
    epoch_steps = max(1, args.epoch_steps)
    step_dt_s = args.multi_epoch_seconds or 0.0
    base_edges_out = args.edges_out
    base_nodes_out = args.nodes_out
    validations: list[dict] = []

    for step in range(epoch_steps):
        if step == 0:
            jd_eval, fr_eval = jd, fr
            edges_out = base_edges_out
            nodes_out = base_nodes_out
        else:
            total_frac = fr + (step * step_dt_s) / 86400.0
            extra_days = math.floor(total_frac)
            jd_eval = jd + extra_days
            fr_eval = total_frac - extra_days
            stem_edges, ext_edges = os.path.splitext(base_edges_out)
            stem_nodes, ext_nodes = os.path.splitext(base_nodes_out)
            edges_out = f"{stem_edges}.t{step}{ext_edges}"
            nodes_out = f"{stem_nodes}.t{step}{ext_nodes}"

        (sats, ground_stations, edges, degree,
         shells, planes_per_shell,
         node_rows, edge_rows) = generator.build_snapshot(
            args,
            raw_sats,
            satrecs,
            jd_eval,
            fr_eval,
            epoch_mode,
            gateways,
        )

        generator.atomic_write_csv(generator.pd.DataFrame(edge_rows), edges_out)
        generator.atomic_write_csv(generator.pd.DataFrame(node_rows), nodes_out)

        validation = generator.validate_topology(
            sats,
            ground_stations,
            edges,
            degree,
            shells,
            planes_per_shell,
            args,
            strict=args.strict,
        )
        validations.append({
            "step": step,
            "jd": jd_eval,
            "fr": fr_eval,
            "iso": generator.jd_to_iso(jd_eval, fr_eval),
            **validation,
        })

        print(
            f"[step {step}] epoch={generator.jd_to_iso(jd_eval, fr_eval)} "
            f"sats={len(sats)} gws={len(ground_stations)} "
            f"shells={len(shells)} edges={len(edges)} "
            f"largest_cc={validation['largest_component_size']} / "
            f"{validation['num_nodes']}"
        )
        for warning in validation["warnings"]:
            print(f"  WARNING: {warning}", file=sys.stderr)
        for issue in validation["issues"]:
            print(f"  ERROR: {issue}", file=sys.stderr)

    return validations


def write_stats(validations: list[dict], stats_out: str) -> None:
    """Write one topology-stat row for each generated epoch."""

    generator = load_tle_snapshot_module()
    stats_rows = []
    for validation in validations:
        stats_rows.append({
            "step": validation["step"],
            "epoch_utc": validation["iso"],
            "num_nodes": validation["num_nodes"],
            "num_satellites": validation["num_satellites"],
            "num_gateways": validation["num_gateways"],
            "num_edges": validation["num_edges"],
            "num_isl": validation["num_isl"],
            "num_access": validation["num_access"],
            "num_shells": validation["num_shells"],
            "num_components": validation["num_components"],
            "largest_cc_size": validation["largest_component_size"],
            "isolated_nodes": validation["isolated_nodes"],
            "mean_isl_km": round(validation["mean_isl_km"], 2),
            "max_isl_km": round(validation["max_isl_km"], 2),
            "min_isl_km": round(validation["min_isl_km"], 2),
            "mean_access_km": round(validation["mean_access_km"], 2),
        })
    generator.atomic_write_csv(generator.pd.DataFrame(stats_rows), stats_out)


def write_metadata(args: argparse.Namespace,
                   jd: float,
                   fr: float,
                   epoch_mode: str,
                   shell_selection: dict,
                   filt: dict,
                   dropped: int,
                   filter_reasons: dict,
                   input_tles: int,
                   post_filter_tles: int,
                   gateways,
                   gateway_source: str,
                   validations: list[dict]) -> None:
    """Write the metadata sidecar used to audit the generated snapshot."""

    generator = load_tle_snapshot_module()
    step_dt_s = args.multi_epoch_seconds or 0.0
    meta = {
        "schema_version": generator.SCHEMA_VERSION,
        "generator": "final/build_snapshot.py",
        "generator_helpers": "tools/tle_to_snapshot.py",
        "epoch_policy": epoch_mode,
        "base_epoch_jd": jd,
        "base_epoch_fr": fr,
        "base_epoch_utc": generator.jd_to_iso(jd, fr),
        "multi_epoch_seconds": step_dt_s,
        "epoch_steps": max(1, args.epoch_steps),
        "delay_model": "propagation_only",
        "serialization_model": "in_ns3",
        "queueing_model": "in_ns3",
        "isl_policy": {
            "max_km": args.max_km,
            "min_km": max(0.0, float(args.min_isl_km)),
            "max_degree": args.max_degree,
            "intra_plane_neighbours": args.intra_plane,
            "inter_plane_offsets_max": args.inter_plane,
            "seam_avoidance_lat_deg": args.disable_isl_above_abs_lat_deg,
            "seam_avoidance_enabled": not args.no_seam_avoidance,
            "raan_tol_deg": args.raan_tol_deg,
            "inc_tol_deg": args.inc_tol_deg,
            "alt_tol_km": args.alt_tol_km,
        },
        "tle_filter": {
            "starlink_operational_preset": bool(args.starlink_operational),
            "effective": filt,
            "dropped": dropped,
            "dropped_reasons": filter_reasons,
            "input_tles": input_tles,
            "post_filter_tles": post_filter_tles,
        },
        "gateway_policy": {
            "enabled": bool(gateways) and not args.no_gateways,
            "min_elevation_deg": args.gs_min_elevation_deg,
            "max_range_km": args.gs_max_range_km,
            "max_sats_per_gs": args.gs_max_sats,
            "max_gs_per_sat": args.gs_max_per_sat,
            "source": gateway_source,
            "is_demo_only": gateway_source == "builtin_demo",
            "count": len(gateways) if not args.no_gateways else 0,
        },
        "delay_field_meaning": (
            "edge delay_ms is one-way vacuum speed-of-light propagation only; "
            "real end-to-end latency also includes serialisation, queueing, "
            "scheduling, gateway/PoP hops and internet transit."
        ),
        "sampling": {
            "mode": args.sample,
            "n": args.n,
            "seed": args.seed,
            "tle_source": args.tle,
            "tle_local_fallback": os.path.abspath(args.tle),
            "shell_selection": shell_selection,
        },
        "cli": {key: getattr(args, key) for key in vars(args)},
        "validation_per_step": validations,
    }
    generator.atomic_write_json(meta, args.meta_out)


def build_snapshot(output_dir: Path,
                   seed: int,
                   epoch_steps: int,
                   multi_epoch_seconds: int) -> None:
    """Build the final-project Starlink-like snapshot from local TLE data."""

    if epoch_steps > 1 and multi_epoch_seconds <= 0:
        raise SystemExit(
            "epoch_steps > 1 requires multi_epoch_seconds > 0; otherwise "
            "every step reports the same epoch."
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    args = generator_args(output_dir, seed, epoch_steps, multi_epoch_seconds)

    (raw_sats, satrecs, shell_selection, filt, dropped,
     filter_reasons, input_tles, post_filter_tles) = load_and_select_satellites(args)

    generator = load_tle_snapshot_module()
    jd, fr, epoch_mode = generator.resolve_common_epoch(args, satrecs)
    if epoch_mode == "per-tle-epoch":
        print(
            "WARNING: per-TLE-epoch mode is physically inconsistent.",
            file=sys.stderr,
        )

    gateways, gateway_source = load_gateways(args)
    validations = write_snapshot_epochs(
        args,
        raw_sats,
        satrecs,
        jd,
        fr,
        epoch_mode,
        gateways,
    )
    write_stats(validations, args.stats_out)
    write_metadata(
        args,
        jd,
        fr,
        epoch_mode,
        shell_selection,
        filt,
        dropped,
        filter_reasons,
        input_tles,
        post_filter_tles,
        gateways,
        gateway_source,
        validations,
    )

    if any(validation["issues"] for validation in validations) and args.strict:
        raise SystemExit("Strict validation failed; see warnings above.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate the Starlink snapshot locally for final/ scripts."
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--epoch-steps", type=int, default=10)
    parser.add_argument("--multi-epoch-seconds", type=int, default=540)
    args = parser.parse_args()

    print(f"Building snapshot into {args.out_dir}")
    build_snapshot(
        output_dir=args.out_dir,
        seed=args.seed,
        epoch_steps=args.epoch_steps,
        multi_epoch_seconds=args.multi_epoch_seconds,
    )
    print("Snapshot build complete.")


if __name__ == "__main__":
    main()
