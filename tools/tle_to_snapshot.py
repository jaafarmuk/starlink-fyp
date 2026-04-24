"""
Starlink / LEO TLE -> network snapshot generator.

Important caveats
-----------------
This tool builds a PHYSICALLY PLAUSIBLE topology from a TLE file. It is
explicitly NOT a model of the real Starlink network:

  * ISL existence/choice is a geometric heuristic (same-shell, near-neighbour
    planes, LoS). Real SpaceX ISL schedules, terminal counts, and beam/slot
    assignments are proprietary and not represented here.
  * Built-in gateway locations are labelled `GW-DEMO-*`. They are NOT a
    claim about real Starlink gateway/PoP sites. Pass --gateways_csv for a
    real gateway list.
  * The emitted per-edge `delay_ms` is ONE-WAY SPEED-OF-LIGHT VACUUM
    propagation. It is not user-perceived latency (which depends on
    serialisation, queueing, scheduling, gateway hops, internet transit,
    retransmissions, etc.).
  * A TLE file conflates operational, orbit-raising, and deorbiting sats.
    Use --starlink_operational (or the fine-grained filters) to restrict to
    the operational Starlink shells.

Design goals
------------
  * Every satellite is propagated to ONE common epoch. --utc is the default
    policy (median TLE epoch if omitted). Per-satellite epoch mode is opt-in
    and emits a warning because it is physically inconsistent for a topology.
  * Plane/shell grouping is derived from propagated orbital elements at the
    evaluation epoch (inclination + RAAN + semi-major axis), never from stale
    TLE text fields.
  * Shell clustering is DIAMETER-BOUNDED in (inclination, altitude): a group
    cannot grow beyond the configured tolerance on either axis, so we do
    not single-linkage-chain across wide spreads.
  * Plane clustering (RAAN within a shell) is mean-shift style on the circle
    with angular tolerance.
  * In-plane ordering uses argument of latitude (u = omega + nu) — a real
    along-track orbital coordinate — not ECI atan2(y,x).
  * Inter-plane ISL candidates are restricted to the SAME SHELL, with
    configurable plane offsets, range, elevation, and latitude constraints
    (seam avoidance for inclined shells).
  * Link delay reported is propagation-only. Serialization / queueing live
    in the ns-3 scenario. This is stated in the generated metadata.
  * ECI->ECEF (via GMST) is computed so that ground gateways can be added.
  * Multi-epoch output is supported; a single snapshot is the default but
    the topology is time-varying in reality, so the metadata records which
    epoch (and how many) were generated.
  * A metadata.json sidecar records schema version, epoch, generator params
    and validation results so downstream tools can detect incompatibilities.
  * The generator refuses to silently emit an obviously-wrong topology:
    isolation, fragmentation, shell-count, and ISL-length distributions are
    validated against configurable thresholds.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from sgp4.api import Satrec


SCHEMA_VERSION = "2.1.0"
# Readers should also accept older schemas known to be compatible.
COMPATIBLE_SCHEMAS = ("2.0.0", "2.1.0")

EARTH_RADIUS_KM = 6378.137            # WGS84 equatorial radius
EARTH_FLATTENING = 1.0 / 298.257223563
EARTH_MU_KM3_S2 = 398600.4418
OMEGA_EARTH_RAD_S = 7.2921150e-5
C_KM_S = 299792.458

# LEO sanity band. Starlink is below ~1200 km; we keep a wide band so the
# validator can still see gross SGP4 blow-ups but legitimate orbits pass.
MIN_ALTITUDE_KM = 150.0
MAX_ALTITUDE_KM = 2500.0
MIN_RADIUS_KM = EARTH_RADIUS_KM + MIN_ALTITUDE_KM
MAX_RADIUS_KM = EARTH_RADIUS_KM + MAX_ALTITUDE_KM


# ---------------------------------------------------------------------------
# Time / coordinate helpers
# ---------------------------------------------------------------------------

def utc_to_jd(utc_iso: str) -> tuple[float, float]:
    """ISO-8601 UTC -> (jd_integer_part_at_0h, fractional_day)."""
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
    jd0 = float(jd_day) - 0.5
    frac = (dt.hour + dt.minute / 60.0 + (dt.second + dt.microsecond / 1e6) / 3600.0) / 24.0
    return jd0, frac


def jd_to_iso(jd: float, fr: float) -> str:
    """(jd, fr) -> ISO-8601 UTC string (best-effort, for metadata)."""
    jdn = jd + fr + 0.5
    Z = math.floor(jdn)
    F = jdn - Z
    if Z < 2299161:
        A = Z
    else:
        alpha = math.floor((Z - 1867216.25) / 36524.25)
        A = Z + 1 + alpha - math.floor(alpha / 4)
    B = A + 1524
    C = math.floor((B - 122.1) / 365.25)
    D = math.floor(365.25 * C)
    E = math.floor((B - D) / 30.6001)

    day = B - D - math.floor(30.6001 * E) + F
    month = E - 1 if E < 14 else E - 13
    year = C - 4716 if month > 2 else C - 4715

    d_int = int(math.floor(day))
    frac_day = day - d_int
    total_sec = frac_day * 86400.0
    hh = int(total_sec // 3600)
    mm = int((total_sec % 3600) // 60)
    ss = total_sec - hh * 3600 - mm * 60
    return f"{year:04d}-{month:02d}-{d_int:02d}T{hh:02d}:{mm:02d}:{ss:06.3f}Z"


def gmst_rad(jd: float, fr: float) -> float:
    """Greenwich Mean Sidereal Time (radians) at UT1~=UTC, IAU 1982 approx."""
    jd_ut1 = jd + fr
    T = (jd_ut1 - 2451545.0) / 36525.0
    gmst_sec = (67310.54841
                + (876600.0 * 3600.0 + 8640184.812866) * T
                + 0.093104 * T * T
                - 6.2e-6 * T * T * T)
    gmst_sec = math.fmod(gmst_sec, 86400.0)
    if gmst_sec < 0:
        gmst_sec += 86400.0
    return (gmst_sec / 86400.0) * 2.0 * math.pi


def eci_to_ecef(r_eci_km: np.ndarray, jd: float, fr: float) -> np.ndarray:
    """Rotate ECI (TEME, good enough for LEO topology) to ECEF."""
    theta = gmst_rad(jd, fr)
    c, s = math.cos(theta), math.sin(theta)
    R = np.array([[c, s, 0.0], [-s, c, 0.0], [0.0, 0.0, 1.0]])
    return R @ np.asarray(r_eci_km, dtype=float)


def ecef_to_geodetic(r_ecef_km: np.ndarray) -> tuple[float, float, float]:
    """ECEF -> (lat_deg, lon_deg, alt_km). WGS84 iterative."""
    x, y, z = r_ecef_km
    a = EARTH_RADIUS_KM
    f = EARTH_FLATTENING
    e2 = f * (2 - f)
    b = a * (1 - f)

    lon = math.atan2(y, x)
    p = math.hypot(x, y)
    if p < 1e-9:
        lat = math.copysign(math.pi / 2.0, z)
        alt = abs(z) - b
        return math.degrees(lat), math.degrees(lon), alt

    lat = math.atan2(z, p * (1 - e2))
    for _ in range(8):
        sin_lat = math.sin(lat)
        N = a / math.sqrt(1 - e2 * sin_lat * sin_lat)
        alt = p / math.cos(lat) - N
        lat_new = math.atan2(z, p * (1 - e2 * N / (N + alt)))
        if abs(lat_new - lat) < 1e-12:
            lat = lat_new
            break
        lat = lat_new
    sin_lat = math.sin(lat)
    N = a / math.sqrt(1 - e2 * sin_lat * sin_lat)
    alt = p / math.cos(lat) - N
    return math.degrees(lat), math.degrees(lon), alt


def geodetic_to_ecef(lat_deg: float, lon_deg: float, alt_km: float) -> np.ndarray:
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    a = EARTH_RADIUS_KM
    f = EARTH_FLATTENING
    e2 = f * (2 - f)
    N = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
    x = (N + alt_km) * math.cos(lat) * math.cos(lon)
    y = (N + alt_km) * math.cos(lat) * math.sin(lon)
    z = (N * (1 - e2) + alt_km) * math.sin(lat)
    return np.array([x, y, z], dtype=float)


def ecef_to_eci(r_ecef_km: np.ndarray, jd: float, fr: float) -> np.ndarray:
    theta = gmst_rad(jd, fr)
    c, s = math.cos(theta), math.sin(theta)
    R = np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])
    return R @ np.asarray(r_ecef_km, dtype=float)


# ---------------------------------------------------------------------------
# Circular statistics
# ---------------------------------------------------------------------------

def circ_mean_deg(values_deg) -> float:
    if len(values_deg) == 0:
        return 0.0
    rad = np.radians(np.asarray(values_deg, dtype=float))
    mx = np.mean(np.cos(rad))
    my = np.mean(np.sin(rad))
    mean = math.degrees(math.atan2(my, mx))
    return mean % 360.0


def ang_diff_deg(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


# ---------------------------------------------------------------------------
# Orbital elements from state vectors
# ---------------------------------------------------------------------------

def classical_elements(r_eci_km: np.ndarray,
                       v_eci_km_s: np.ndarray) -> dict:
    """Convert (r, v) in ECI to classical Keplerian elements.

    Returns degrees for all angles.
    """
    r = np.asarray(r_eci_km, dtype=float)
    v = np.asarray(v_eci_km_s, dtype=float)
    r_mag = np.linalg.norm(r)
    v_mag = np.linalg.norm(v)

    h = np.cross(r, v)
    h_mag = np.linalg.norm(h)

    K = np.array([0.0, 0.0, 1.0])
    n = np.cross(K, h)
    n_mag = np.linalg.norm(n)

    e_vec = (np.cross(v, h) / EARTH_MU_KM3_S2) - r / r_mag
    e = float(np.linalg.norm(e_vec))

    energy = 0.5 * v_mag * v_mag - EARTH_MU_KM3_S2 / r_mag
    a = -EARTH_MU_KM3_S2 / (2.0 * energy) if abs(energy) > 1e-12 else float("inf")

    i = math.degrees(math.acos(max(-1.0, min(1.0, h[2] / h_mag))))

    if n_mag > 1e-9:
        raan = math.degrees(math.acos(max(-1.0, min(1.0, n[0] / n_mag))))
        if n[1] < 0.0:
            raan = 360.0 - raan
    else:
        raan = 0.0  # equatorial

    if n_mag > 1e-9 and e > 1e-9:
        argp = math.degrees(math.acos(
            max(-1.0, min(1.0, float(np.dot(n, e_vec)) / (n_mag * e)))
        ))
        if e_vec[2] < 0.0:
            argp = 360.0 - argp
    else:
        argp = 0.0

    if e > 1e-9:
        nu = math.degrees(math.acos(
            max(-1.0, min(1.0, float(np.dot(e_vec, r)) / (e * r_mag)))
        ))
        if float(np.dot(r, v)) < 0.0:
            nu = 360.0 - nu
    else:
        # Circular orbit: use argument of latitude directly.
        if n_mag > 1e-9:
            u_tmp = math.degrees(math.acos(
                max(-1.0, min(1.0, float(np.dot(n, r)) / (n_mag * r_mag)))
            ))
            if r[2] < 0.0:
                u_tmp = 360.0 - u_tmp
            nu = u_tmp
            argp = 0.0
        else:
            nu = math.degrees(math.atan2(r[1], r[0])) % 360.0
            argp = 0.0

    u = (argp + nu) % 360.0

    return {
        "a_km": float(a),
        "e": float(e),
        "i_deg": float(i),
        "raan_deg": float(raan % 360.0),
        "argp_deg": float(argp),
        "nu_deg": float(nu),
        "u_deg": float(u),
    }


# ---------------------------------------------------------------------------
# TLE parsing
# ---------------------------------------------------------------------------

def read_tles(path: str) -> list[tuple[str, str, str]]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    sats = []
    i = 0
    while i < len(lines):
        if (i + 2 < len(lines)
                and lines[i + 1].startswith("1 ")
                and lines[i + 2].startswith("2 ")):
            name, l1, l2 = lines[i], lines[i + 1], lines[i + 2]
            sats.append((name, l1, l2))
            i += 3
        elif (i + 1 < len(lines)
                and lines[i].startswith("1 ")
                and lines[i + 1].startswith("2 ")):
            l1, l2 = lines[i], lines[i + 1]
            sats.append((f"SAT_{len(sats)}", l1, l2))
            i += 2
        else:
            i += 1
    return sats


def tle_epoch_jd(satrec: Satrec) -> float:
    return float(satrec.jdsatepoch) + float(satrec.jdsatepochF)


# ---------------------------------------------------------------------------
# Operational-satellite filtering. A raw TLE file mixes operational sats
# with orbit-raising (low, climbing) and deorbiting / deorbited (decaying)
# sats. For Starlink, operational shells are within a narrow altitude band
# at low eccentricity; the helper below drops TLEs that fall outside the
# requested envelope BEFORE SGP4 propagation so downstream stats are not
# contaminated.
# ---------------------------------------------------------------------------

# Operational Starlink defaults. Verify against the FCC grant and SpaceX
# filings before citing; these are deliberately conservative:
#   * altitude 400-600 km captures Gen1 (~540-570) and Gen2 lower shells
#     (~525-535). Does NOT include planned high-inclination variants.
#   * eccentricity < 0.01 excludes obvious orbit-raising ellipses.
#   * mean motion 14.9-15.7 rev/day covers ~510-620 km circular orbits.
STARLINK_DEFAULTS = {
    "min_altitude_km": 400.0,
    "max_altitude_km": 600.0,
    "max_eccentricity": 0.01,
    "min_mean_motion_rev_day": 14.9,
    "max_mean_motion_rev_day": 15.7,
}


def filter_operational(
    sats: list[tuple[str, str, str]],
    satrecs: list[Satrec],
    *,
    min_altitude_km: Optional[float],
    max_altitude_km: Optional[float],
    max_eccentricity: Optional[float],
    min_mean_motion_rev_day: Optional[float],
    max_mean_motion_rev_day: Optional[float],
) -> tuple[list[tuple[str, str, str]], list[Satrec], dict]:
    """Drop TLEs outside the operational envelope.

    Returns (kept_sats, kept_satrecs, reasons) where `reasons` is a counter
    of why entries were dropped.
    """
    if all(v is None for v in (
            min_altitude_km, max_altitude_km, max_eccentricity,
            min_mean_motion_rev_day, max_mean_motion_rev_day)):
        return list(sats), list(satrecs), {}

    kept_sats = []
    kept_srs = []
    reasons: Counter = Counter()
    for triple, sr in zip(sats, satrecs):
        # SGP4 exposes mean motion (no_kozai) in rad/min and eccentricity.
        # The semi-major axis can be derived from n via Kepler's third law.
        try:
            n_rad_min = float(sr.no_kozai)
            e = float(sr.ecco)
        except Exception:
            reasons["bad_tle"] += 1
            continue
        if n_rad_min <= 0.0:
            reasons["bad_mean_motion"] += 1
            continue
        n_rev_day = n_rad_min * (1440.0 / (2.0 * math.pi))
        # a = (mu / n^2)^(1/3), with n in rad/s
        n_rad_s = n_rad_min / 60.0
        a_km = (EARTH_MU_KM3_S2 / (n_rad_s * n_rad_s)) ** (1.0 / 3.0)
        perigee_alt = a_km * (1.0 - e) - EARTH_RADIUS_KM

        if min_altitude_km is not None and perigee_alt < min_altitude_km:
            reasons["below_min_altitude"] += 1
            continue
        if max_altitude_km is not None and perigee_alt > max_altitude_km:
            reasons["above_max_altitude"] += 1
            continue
        if max_eccentricity is not None and e > max_eccentricity:
            reasons["eccentric"] += 1
            continue
        if (min_mean_motion_rev_day is not None
                and n_rev_day < min_mean_motion_rev_day):
            reasons["slow_orbit"] += 1
            continue
        if (max_mean_motion_rev_day is not None
                and n_rev_day > max_mean_motion_rev_day):
            reasons["fast_orbit"] += 1
            continue
        kept_sats.append(triple)
        kept_srs.append(sr)
    return kept_sats, kept_srs, dict(reasons)


# ---------------------------------------------------------------------------
# Line-of-sight / distance
# ---------------------------------------------------------------------------

def has_line_of_sight(r1: np.ndarray,
                      r2: np.ndarray,
                      earth_radius_km: float = EARTH_RADIUS_KM) -> bool:
    r1 = np.asarray(r1, dtype=float)
    r2 = np.asarray(r2, dtype=float)
    d = r2 - r1
    denom = float(np.dot(d, d))
    if denom == 0.0:
        return False
    t = -float(np.dot(r1, d)) / denom
    t = max(0.0, min(1.0, t))
    closest = r1 + t * d
    return bool(np.linalg.norm(closest) > earth_radius_km)


def distance_km(r1, r2) -> float:
    return float(np.linalg.norm(np.array(r1, dtype=float) - np.array(r2, dtype=float)))


# ---------------------------------------------------------------------------
# Shell + plane clustering (addresses review items 2, 3, 4, 5, 6)
# ---------------------------------------------------------------------------

def _cluster_1d_tolerance(values_and_ids: list[tuple[float, int]],
                          tol: float) -> list[list[int]]:
    """Diameter-bounded 1-D clustering on a non-angular axis.

    Sorts by value and starts a NEW group whenever adding the next point
    would make the group's total span exceed `tol`. This is stricter than
    adjacent-gap single-linkage: with single-linkage, a long ladder of
    points each within `tol` of the last can produce a group whose first
    and last points differ by arbitrarily much. Here the group span is
    always <= tol, which matches what a shell / orbit family actually
    looks like (a narrow altitude/inclination band).
    """
    if not values_and_ids:
        return []
    ordered = sorted(values_and_ids, key=lambda t: t[0])
    groups: list[list[tuple[float, int]]] = [[ordered[0]]]
    group_min = ordered[0][0]
    for v, sid in ordered[1:]:
        if (v - group_min) > tol:
            groups.append([(v, sid)])
            group_min = v
        else:
            groups[-1].append((v, sid))
    return [[sid for _, sid in g] for g in groups]


def cluster_shells(sats: list[dict],
                   inc_tol_deg: float,
                   alt_tol_km: float) -> dict[int, list[int]]:
    """Group satellites into physical shells by (inclination, altitude).

    Tolerance-based 1-D clustering on each axis so satellites that differ by
    less than tol on both inclination and altitude stay together. Rounding
    into fixed bins is intentionally avoided: two satellites that straddle
    a bin boundary but are physically within `inc_tol_deg` of each other
    would otherwise split into different shells.
    """
    if not sats:
        return {}

    inc_groups = _cluster_1d_tolerance(
        [(s["i_deg"], s["id"]) for s in sats],
        max(inc_tol_deg, 1e-6),
    )

    shells: dict[int, list[int]] = {}
    shell_id = 0
    for ids in inc_groups:
        alt_groups = _cluster_1d_tolerance(
            [(sats[sid]["alt_km"], sid) for sid in ids],
            max(alt_tol_km, 1e-6),
        )
        for g in alt_groups:
            shells[shell_id] = g
            shell_id += 1
    return shells


def cluster_planes_in_shell(shell_sat_ids: list[int],
                            sats_by_id: dict[int, dict],
                            raan_tol_deg: float,
                            max_iter: int = 25) -> dict[int, list[int]]:
    """Cluster a set of satellites in one shell into planes using RAAN only.

    Uses a mean-shift-style iteration on circular RAAN:
      1. Seed centers at every distinct RAAN (rounded).
      2. Assign each sat to nearest center.
      3. Recompute each center as circular mean of members.
      4. Merge centers closer than raan_tol_deg/2.
      5. Repeat until stable or max_iter.
    """
    if len(shell_sat_ids) <= 1:
        return {0: list(shell_sat_ids)}

    raans = np.array([sats_by_id[sid]["raan_deg"] for sid in shell_sat_ids])

    seed_step = max(raan_tol_deg / 2.0, 0.5)
    seeds = np.arange(0.0, 360.0, seed_step)
    centers = seeds.tolist()

    for _ in range(max_iter):
        assigned = defaultdict(list)
        for raan, sid in zip(raans, shell_sat_ids):
            best_c = min(range(len(centers)),
                         key=lambda ci: ang_diff_deg(raan, centers[ci]))
            assigned[best_c].append((sid, raan))

        new_centers: list[float] = []
        for ci, members in assigned.items():
            if not members:
                continue
            mean = circ_mean_deg([m[1] for m in members])
            new_centers.append(mean)

        merged: list[float] = []
        for c in sorted(new_centers):
            if not merged:
                merged.append(c)
                continue
            if ang_diff_deg(c, merged[-1]) <= raan_tol_deg / 2.0:
                merged[-1] = circ_mean_deg([merged[-1], c])
            else:
                merged.append(c)
        if len(merged) >= 2 and ang_diff_deg(merged[0], merged[-1]) <= raan_tol_deg / 2.0:
            merged[0] = circ_mean_deg([merged[0], merged[-1]])
            merged.pop()

        if (len(merged) == len(centers)
                and all(ang_diff_deg(a, b) < 1e-3
                        for a, b in zip(sorted(merged), sorted(centers)))):
            centers = merged
            break
        centers = merged

    planes: dict[int, list[int]] = defaultdict(list)
    for raan, sid in zip(raans, shell_sat_ids):
        best_c = min(range(len(centers)),
                     key=lambda ci: ang_diff_deg(raan, centers[ci]))
        if ang_diff_deg(raan, centers[best_c]) > raan_tol_deg:
            # Not close to any center -> its own plane.
            planes[len(centers) + len(planes)].append(sid)
            continue
        planes[best_c].append(sid)

    return {new_pid: ids for new_pid, (_old, ids) in enumerate(planes.items())}


def order_by_argument_of_latitude(plane_sat_ids: list[int],
                                  sats_by_id: dict[int, dict]) -> list[int]:
    """Sort satellites along-track by argument of latitude u = argp + nu."""
    return sorted(plane_sat_ids, key=lambda sid: sats_by_id[sid]["u_deg"])


# ---------------------------------------------------------------------------
# ISL edge construction (addresses items 7, 8, 24)
# ---------------------------------------------------------------------------

@dataclass
class IslPolicy:
    max_km: float = 5000.0
    min_km: float = 0.0
    max_degree: int = 4
    intra_plane_neighbours: int = 2
    allowed_plane_offsets: tuple = (-2, -1, 1, 2)
    # Minimum angle between the ISL vector and each endpoint's local
    # horizontal plane (the plane orthogonal to that satellite's geocentric
    # position vector). This is an ISL grazing-angle check, not a
    # ground-station elevation mask. -90 disables the check.
    min_isl_grazing_angle_deg: float = -90.0
    disable_above_abs_lat_deg: float = 70.0  # seam avoidance for inclined shells
    apply_seam_avoidance: bool = True


def link_midpoint_subpoint_lat(r1_eci: np.ndarray,
                               r2_eci: np.ndarray,
                               jd: float,
                               fr: float) -> float:
    mid = 0.5 * (r1_eci + r2_eci)
    ecef = eci_to_ecef(mid, jd, fr)
    lat, _, _ = ecef_to_geodetic(ecef)
    return abs(lat)


def link_min_elevation_deg(r1_eci: np.ndarray,
                           r2_eci: np.ndarray) -> float:
    """Minimum local elevation angle along an inter-satellite link, measured
    from each endpoint's local horizontal plane (the plane orthogonal to the
    endpoint's geocentric position vector). For ISLs this is a first-order
    line-of-sight check: if either end sees the other *below* its horizon,
    the link grazes the Earth/atmosphere. Returns the smaller of the two
    endpoint elevation angles, in degrees.
    """
    r1 = np.asarray(r1_eci, dtype=float)
    r2 = np.asarray(r2_eci, dtype=float)
    los = r2 - r1
    dist = float(np.linalg.norm(los))
    if dist <= 0.0:
        return 90.0
    n1 = float(np.linalg.norm(r1))
    n2 = float(np.linalg.norm(r2))
    if n1 <= 0.0 or n2 <= 0.0:
        return 90.0
    up1 = r1 / n1
    up2 = r2 / n2
    sin_e1 = float(np.dot(los, up1)) / dist
    sin_e2 = float(np.dot(-los, up2)) / dist
    sin_e1 = max(-1.0, min(1.0, sin_e1))
    sin_e2 = max(-1.0, min(1.0, sin_e2))
    return math.degrees(min(math.asin(sin_e1), math.asin(sin_e2)))


def try_add_edge(u_id: int,
                 v_id: int,
                 sats_by_id: dict[int, dict],
                 degree: dict[int, int],
                 edge_pairs: set,
                 edges: list,
                 policy: IslPolicy,
                 jd: float,
                 fr: float,
                 kind: str,
                 extra_range_km: Optional[float] = None) -> bool:
    if u_id == v_id:
        return False
    a, b = sorted((u_id, v_id))
    if (a, b) in edge_pairs:
        return False
    if degree[a] >= policy.max_degree or degree[b] >= policy.max_degree:
        return False

    s1 = sats_by_id[a]
    s2 = sats_by_id[b]
    r1, r2 = s1["r_eci"], s2["r_eci"]

    d = distance_km(r1, r2)
    limit = extra_range_km if extra_range_km is not None else policy.max_km
    if d > limit:
        return False
    if d < policy.min_km:
        # Reject implausibly short ISLs — these usually indicate two TLEs
        # for the same physical satellite, a near-collision pair at a
        # plane crossing, or TLE epoch contamination.
        return False
    if not has_line_of_sight(r1, r2):
        return False

    if policy.apply_seam_avoidance:
        mid_lat = link_midpoint_subpoint_lat(r1, r2, jd, fr)
        if mid_lat > policy.disable_above_abs_lat_deg:
            return False

    # Reject ISLs whose endpoints see each other below each one's local
    # horizontal plane (grazing links with implausibly low angles).
    if policy.min_isl_grazing_angle_deg > -90.0:
        min_elev = link_min_elevation_deg(r1, r2)
        if min_elev < policy.min_isl_grazing_angle_deg:
            return False

    delay_ms = (d / C_KM_S) * 1000.0
    edge_pairs.add((a, b))
    edges.append({
        "u": a, "v": b,
        "distance_km": d, "delay_ms": delay_ms,
        "kind": kind,
        "shell_id": s1.get("shell_id", -1) if s1.get("shell_id") == s2.get("shell_id") else -1,
    })
    degree[a] += 1
    degree[b] += 1
    return True


def ring_neighbors(ordered_ids: list[int], sid: int, count: int) -> list[int]:
    n = len(ordered_ids)
    if n < 2 or count <= 0:
        return []
    idx = ordered_ids.index(sid)
    out = []
    step = 1
    while len(out) < count and step <= n // 2 + 1:
        for off in (-step, step):
            nbr = ordered_ids[(idx + off) % n]
            if nbr != sid and nbr not in out:
                out.append(nbr)
                if len(out) >= count:
                    break
        step += 1
    return out[:count]


# ---------------------------------------------------------------------------
# Ground gateways (review items 25, 26)
# ---------------------------------------------------------------------------

# A small built-in set of demo ground-station locations. These coordinates
# are major-city centroids, NOT real Starlink gateway or PoP sites. They
# exist only so a snapshot is not sat-only when no --gateways_csv is given.
# Every entry is prefixed DEMO- so downstream tools and visualisations can
# surface that these are synthetic placeholders.
DEFAULT_GATEWAYS = [
    ("DEMO-LON",   51.5074,   -0.1278, 0.03),
    ("DEMO-NYC",   40.7128,  -74.0060, 0.02),
    ("DEMO-LAX",   33.9416, -118.4085, 0.04),
    ("DEMO-SEA",   47.4502, -122.3088, 0.06),
    ("DEMO-FRA",   50.1109,    8.6821, 0.11),
    ("DEMO-SIN",    1.3521,  103.8198, 0.02),
    ("DEMO-SYD",  -33.8688,  151.2093, 0.06),
    ("DEMO-TYO",   35.6762,  139.6503, 0.04),
    ("DEMO-GRU",  -23.5505,  -46.6333, 0.76),
    ("DEMO-JNB",  -26.2041,   28.0473, 1.75),
]


def read_gateway_csv(path: str) -> list[tuple[str, float, float, float]]:
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("name") or cols.get("id") or df.columns[0]
    out = []
    for _, row in df.iterrows():
        name = str(row[name_col])
        lat = float(row[cols["lat"]])
        lon = float(row[cols["lon"]])
        alt = float(row[cols["alt_km"]]) if "alt_km" in cols else 0.0
        out.append((name, lat, lon, alt))
    return out


def build_access_links(ground_stations: list[dict],
                       sats: list[dict],
                       sats_by_id: dict[int, dict],
                       degree: dict[int, int],
                       edge_pairs: set,
                       edges: list,
                       policy: IslPolicy,
                       min_elevation_deg: float,
                       max_range_km: float,
                       max_sats_per_gs: int,
                       max_gs_per_sat: int,
                       jd: float,
                       fr: float) -> list[tuple[int, int]]:
    """Add ground-to-satellite access links. Elevation check uses ECEF.

    Access links use their own per-satellite degree budget (max_gs_per_sat)
    rather than the shared ISL degree cap so that ISL construction order does
    not crowd out gateway connectivity.
    """
    sat_access_degree: dict[int, int] = defaultdict(int)
    added_pairs = []
    for gs in ground_stations:
        candidates = []
        gs_ecef = gs["r_ecef"]
        # Geodetic up on the WGS84 ellipsoid (ENU "up"), NOT geocentric
        # up. The two differ by a few tenths of a degree at mid-latitudes;
        # elevation thresholds near horizon depend on the geodetic normal.
        lat = math.radians(gs["lat_deg"])
        lon = math.radians(gs["lon_deg"])
        gs_up = np.array([
            math.cos(lat) * math.cos(lon),
            math.cos(lat) * math.sin(lon),
            math.sin(lat),
        ], dtype=float)
        for s in sats:
            sat_ecef = s["r_ecef"]
            los = sat_ecef - gs_ecef
            dist = float(np.linalg.norm(los))
            if dist > max_range_km or dist <= 0:
                continue
            elev = math.degrees(math.asin(max(-1.0, min(1.0,
                float(np.dot(los, gs_up) / dist)))))
            if elev < min_elevation_deg:
                continue
            candidates.append((dist, elev, s["id"]))

        # Sort by distance, then accept the first `max_sats_per_gs` *valid*
        # candidates — skipping (not slicing) those blocked by the per-sat cap
        # or existing edges, so later valid sats are still considered.
        candidates.sort(key=lambda x: x[0])
        gs_id = gs["id"]
        added_for_gs = 0
        for dist, elev, sid in candidates:
            if added_for_gs >= max_sats_per_gs:
                break
            if degree[gs_id] >= max_sats_per_gs:
                break
            if sat_access_degree[sid] >= max_gs_per_sat:
                continue
            pair = tuple(sorted((gs_id, sid)))
            if pair in edge_pairs:
                continue
            delay_ms = (dist / C_KM_S) * 1000.0
            edge_pairs.add(pair)
            edges.append({
                "u": pair[0], "v": pair[1],
                "distance_km": dist, "delay_ms": delay_ms,
                "kind": "access",
                "shell_id": -1,
            })
            degree[gs_id] += 1
            degree[sid] += 1
            sat_access_degree[sid] += 1
            added_pairs.append(pair)
            added_for_gs += 1
    return added_pairs


# ---------------------------------------------------------------------------
# Validation (review item 29)
# ---------------------------------------------------------------------------

def connected_components(num_nodes: int, edges: list[dict]) -> list[set]:
    adj = defaultdict(list)
    for e in edges:
        adj[e["u"]].append(e["v"])
        adj[e["v"]].append(e["u"])
    visited = set()
    comps = []
    for start in range(num_nodes):
        if start in visited:
            continue
        stack = [start]
        comp = set()
        while stack:
            x = stack.pop()
            if x in visited:
                continue
            visited.add(x)
            comp.add(x)
            stack.extend(adj[x])
        comps.append(comp)
    comps.sort(key=len, reverse=True)
    return comps


def validate_topology(sats: list[dict],
                      ground_stations: list[dict],
                      edges: list[dict],
                      degree: dict[int, int],
                      shells: dict[int, list[int]],
                      planes_per_shell: dict[int, dict[int, list[int]]],
                      args,
                      strict: bool) -> dict:
    issues: list[str] = []
    warnings: list[str] = []

    num_nodes = len(sats) + len(ground_stations)
    isl_distances = [e["distance_km"] for e in edges if e["kind"] != "access"]
    access_distances = [e["distance_km"] for e in edges if e["kind"] == "access"]

    comps = connected_components(num_nodes, edges)
    largest = len(comps[0]) if comps else 0
    sat_ids = {s["id"] for s in sats}
    isolated_sats = sum(1 for sid in sat_ids if degree.get(sid, 0) == 0)
    isolated_gws  = sum(1 for gs in ground_stations if degree.get(gs["id"], 0) == 0)
    isolated = isolated_sats + isolated_gws

    if sats and num_nodes > 0 and largest / num_nodes < args.min_largest_cc_frac:
        msg = (f"Largest CC covers only {largest}/{num_nodes} nodes "
               f"(< {args.min_largest_cc_frac*100:.0f}%).")
        (issues if strict else warnings).append(msg)

    if sats and isolated_sats / len(sats) > args.max_isolated_frac:
        msg = (f"{isolated_sats}/{len(sats)} satellites have no links "
               f"(gateways isolated: {isolated_gws}).")
        (issues if strict else warnings).append(msg)

    if isl_distances:
        mean_isl = float(np.mean(isl_distances))
        max_isl = float(np.max(isl_distances))
        if max_isl > args.max_km:
            issues.append(f"ISL length {max_isl:.1f} km exceeds --max_km {args.max_km:.1f}.")
        if mean_isl < 50.0:
            warnings.append(
                f"Mean ISL length {mean_isl:.1f} km is implausibly short.")

    for shell_id, plane_map in planes_per_shell.items():
        pcount = len(plane_map)
        if pcount == 0:
            continue
        sizes = [len(m) for m in plane_map.values()]
        if pcount > 2 * args.n:
            warnings.append(
                f"Shell {shell_id}: {pcount} planes inferred for "
                f"{sum(sizes)} sats — possibly over-clustered.")
        if max(sizes) > 0 and max(sizes) / min(sizes) > 20 and min(sizes) < 2:
            warnings.append(
                f"Shell {shell_id}: very uneven planes (min={min(sizes)}, "
                f"max={max(sizes)}).")

    return {
        "ok": not issues,
        "issues": issues,
        "warnings": warnings,
        "num_nodes": num_nodes,
        "num_satellites": len(sats),
        "num_gateways": len(ground_stations),
        "num_edges": len(edges),
        "num_isl": len(isl_distances),
        "num_access": len(access_distances),
        "num_shells": len(shells),
        "num_components": len(comps),
        "largest_component_size": largest,
        "isolated_nodes": isolated,
        "mean_isl_km": float(np.mean(isl_distances)) if isl_distances else 0.0,
        "max_isl_km": float(np.max(isl_distances)) if isl_distances else 0.0,
        "min_isl_km": float(np.min(isl_distances)) if isl_distances else 0.0,
        "mean_access_km": float(np.mean(access_distances)) if access_distances else 0.0,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build a physically consistent snapshot from TLEs.")
    ap.add_argument("--tle", required=True)
    ap.add_argument("--edges_out", default="results/snapshot_edges.csv")
    ap.add_argument("--nodes_out", default="results/snapshot_nodes.csv")
    ap.add_argument("--stats_out", default="results/topology_stats.csv")
    ap.add_argument("--meta_out", default="results/snapshot_meta.json")

    ap.add_argument("--n", type=int, default=60,
                    help="Number of TLEs to sample (after filtering).")
    ap.add_argument("--sample", choices=["head", "random"], default="random",
                    help="Default is 'random' because 'head' biases toward "
                         "the earliest launch IDs, which massively over-"
                         "represents a handful of shells and creates "
                         "fragmented topologies for small --n.")
    ap.add_argument("--seed", type=int, default=None)

    # Operational-satellite filters (review: TLE dataset mixes operational,
    # orbit-raising and deorbiting sats).
    ap.add_argument("--starlink_operational", action="store_true",
                    help="Apply conservative Starlink operational filters "
                         "(altitude 400-600 km, ecc <0.01, mean motion "
                         "14.9-15.7 rev/day). Individual filters below "
                         "override this.")
    ap.add_argument("--min_altitude_km", type=float, default=None,
                    help="Drop TLEs whose perigee altitude is below this.")
    ap.add_argument("--max_altitude_km", type=float, default=None,
                    help="Drop TLEs whose perigee altitude is above this.")
    ap.add_argument("--max_eccentricity", type=float, default=None,
                    help="Drop TLEs with eccentricity above this threshold.")
    ap.add_argument("--min_mean_motion_rev_day", type=float, default=None)
    ap.add_argument("--max_mean_motion_rev_day", type=float, default=None)

    ap.add_argument("--utc", default=None,
                    help="Common propagation epoch as ISO-8601 UTC. "
                         "If omitted, the median TLE epoch is used.")
    ap.add_argument("--jd", type=float, default=None)
    ap.add_argument("--fr", type=float, default=0.0)
    ap.add_argument("--allow_tle_epoch", action="store_true",
                    help="Allow per-satellite-epoch mode. Physically "
                         "inconsistent; not recommended.")

    ap.add_argument("--max_km", type=float, default=5000.0)
    ap.add_argument("--min_isl_km", type=float, default=100.0,
                    help="Reject ISLs shorter than this (km). Very short "
                         "inter-satellite links usually indicate a TLE epoch "
                         "problem or a near-collision at a plane crossing "
                         "and are not representative of operational ISLs.")
    ap.add_argument("--max_degree", type=int, default=4)
    ap.add_argument("--intra_plane", type=int, default=2)
    ap.add_argument("--inter_plane", type=int, default=2)
    ap.add_argument("--raan_tol_deg", type=float, default=5.0)
    ap.add_argument("--inc_tol_deg", type=float, default=1.0)
    ap.add_argument("--alt_tol_km", type=float, default=25.0)
    ap.add_argument("--disable_isl_above_abs_lat_deg", type=float, default=70.0,
                    help="Seam avoidance: reject ISLs whose subpoint is above "
                         "this absolute latitude (matches how inclined "
                         "Starlink shells cannot cross polar seams cleanly).")
    ap.add_argument("--no_seam_avoidance", action="store_true")

    ap.add_argument("--gateways_csv", default=None,
                    help="CSV with name,lat,lon[,alt_km]. Built-in set used "
                         "if omitted. Pass '' to disable gateways.")
    ap.add_argument("--no_gateways", action="store_true")
    ap.add_argument("--gs_min_elevation_deg", type=float, default=25.0)
    ap.add_argument("--gs_max_range_km", type=float, default=2000.0)
    ap.add_argument("--gs_max_sats", type=int, default=4,
                    help="Max satellites per gateway (gateway degree cap).")
    ap.add_argument("--gs_max_per_sat", type=int, default=2,
                    help="Max gateways that may connect to one satellite "
                         "(separate from the ISL degree cap).")

    ap.add_argument("--multi_epoch_seconds", type=float, default=None,
                    help="If set, also emit snapshots at this cadence for "
                         "--epoch_steps steps, into per-step files.")
    ap.add_argument("--epoch_steps", type=int, default=1)

    ap.add_argument("--min_largest_cc_frac", type=float, default=0.5)
    ap.add_argument("--max_isolated_frac", type=float, default=0.25)
    # Strict is the default: we would rather loudly fail than silently emit
    # a topology where half the nodes are isolated. --no_strict reverts to
    # warning-only for exploratory runs.
    strict_grp = ap.add_mutually_exclusive_group()
    strict_grp.add_argument("--strict", dest="strict", action="store_true",
                            help="Exit non-zero when validation finds issues "
                                 "(default).")
    strict_grp.add_argument("--no_strict", dest="strict", action="store_false",
                            help="Downgrade validation errors to warnings.")
    ap.set_defaults(strict=True)

    return ap.parse_args()


def resolve_common_epoch(args, satrecs) -> tuple[float, float, str]:
    if args.jd is not None:
        return float(args.jd), float(args.fr), f"jd={args.jd}+{args.fr}"
    if args.utc is not None:
        jd, fr = utc_to_jd(args.utc)
        return jd, fr, f"utc={args.utc}"
    if args.allow_tle_epoch:
        # Do not return jd=0/fr=0 — we still need a valid reference epoch for
        # ECI→ECEF conversions and gateway placement. Use the median TLE epoch
        # as a reference; propagate_one will override per-satellite when
        # per_tle_mode=True.
        epochs = [tle_epoch_jd(s) for s in satrecs]
        med = float(np.median(epochs))
        jd = math.floor(med) - 0.5
        fr = med - jd
        if fr >= 1.0:
            jd += 1.0
            fr -= 1.0
        return jd, fr, "per-tle-epoch"
    # default: median TLE epoch of the sampled population
    epochs = [tle_epoch_jd(s) for s in satrecs]
    med = float(np.median(epochs))
    jd = math.floor(med) - 0.5
    fr = med - jd
    if fr >= 1.0:
        jd += 1.0
        fr -= 1.0
    return jd, fr, f"median-tle-epoch={jd_to_iso(jd, fr)}"


def propagate_one(satrec: Satrec, jd: float, fr: float,
                  per_tle: bool) -> Optional[tuple[np.ndarray, np.ndarray]]:
    if per_tle:
        jd_eval, fr_eval = satrec.jdsatepoch, satrec.jdsatepochF
    else:
        jd_eval, fr_eval = jd, fr
    e, r, v = satrec.sgp4(jd_eval, fr_eval)
    if e != 0:
        return None
    return np.array(r, dtype=float), np.array(v, dtype=float)


def build_snapshot(args,
                   raw_sats: list[tuple[str, str, str]],
                   satrecs: list[Satrec],
                   jd: float,
                   fr: float,
                   epoch_mode: str,
                   gateways: list[tuple[str, float, float, float]]):
    per_tle_mode = epoch_mode == "per-tle-epoch"

    sats: list[dict] = []
    rejected_sgp4 = 0
    rejected_altitude = 0

    for i, ((name, l1, l2), sr) in enumerate(zip(raw_sats, satrecs)):
        result = propagate_one(sr, jd, fr, per_tle_mode)
        if result is None:
            rejected_sgp4 += 1
            continue
        r_eci, v_eci = result
        radius = float(np.linalg.norm(r_eci))
        if not (MIN_RADIUS_KM <= radius <= MAX_RADIUS_KM):
            rejected_altitude += 1
            continue

        elements = classical_elements(r_eci, v_eci)
        # Perigee-altitude proxy used only for shell clustering (not exported).
        _perigee_alt_km = elements["a_km"] * (1 - elements["e"]) - EARTH_RADIUS_KM
        # For per-TLE mode use the satellite's own epoch for ECEF rotation so
        # we don't rotate with jd=reference which would be wrong per-satellite.
        if per_tle_mode:
            jd_ecef, fr_ecef = float(sr.jdsatepoch), float(sr.jdsatepochF)
        else:
            jd_ecef, fr_ecef = jd, fr
        r_ecef = eci_to_ecef(r_eci, jd_ecef, fr_ecef)
        lat, lon, geo_alt = ecef_to_geodetic(r_ecef)

        sats.append({
            "id": len(sats),
            "name": name,
            "l1": l1, "l2": l2,
            "r_eci": r_eci,
            "v_eci": v_eci,
            "r_ecef": r_ecef,
            "a_km": elements["a_km"],
            "e": elements["e"],
            "i_deg": elements["i_deg"],
            "raan_deg": elements["raan_deg"],
            "argp_deg": elements["argp_deg"],
            "nu_deg": elements["nu_deg"],
            "u_deg": elements["u_deg"],
            "alt_km": _perigee_alt_km,   # shell clustering proxy (not written to CSV)
            "lat_deg": lat,
            "lon_deg": lon,
            "geo_alt_km": geo_alt,       # true geodetic altitude — written as altitude_km
            "shell_id": -1,
            "plane_id": -1,
        })

    if len(sats) < 2:
        raise SystemExit("Not enough valid satellites after filtering.")

    sats_by_id = {s["id"]: s for s in sats}

    shells = cluster_shells(sats, args.inc_tol_deg, args.alt_tol_km)
    planes_per_shell: dict[int, dict[int, list[int]]] = {}
    plane_counter = 0
    plane_to_shell: dict[int, int] = {}
    for shell_id, ids in shells.items():
        for sid in ids:
            sats_by_id[sid]["shell_id"] = shell_id
        planes_in_shell = cluster_planes_in_shell(
            ids, sats_by_id, args.raan_tol_deg)
        renumbered: dict[int, list[int]] = {}
        for _, plane_ids in planes_in_shell.items():
            new_pid = plane_counter
            plane_counter += 1
            renumbered[new_pid] = plane_ids
            plane_to_shell[new_pid] = shell_id
            for sid in plane_ids:
                sats_by_id[sid]["plane_id"] = new_pid
        planes_per_shell[shell_id] = renumbered

    ordered_planes: dict[int, list[int]] = {}
    for shell_id, plane_map in planes_per_shell.items():
        for pid, ids in plane_map.items():
            ordered_planes[pid] = order_by_argument_of_latitude(ids, sats_by_id)

    policy = IslPolicy(
        max_km=args.max_km,
        min_km=max(0.0, float(args.min_isl_km)),
        max_degree=args.max_degree,
        intra_plane_neighbours=args.intra_plane,
        allowed_plane_offsets=tuple(
            o for o in range(-args.inter_plane, args.inter_plane + 1) if o != 0),
        disable_above_abs_lat_deg=args.disable_isl_above_abs_lat_deg,
        apply_seam_avoidance=not args.no_seam_avoidance,
    )

    ground_stations: list[dict] = []
    if gateways and not args.no_gateways:
        for idx, (gname, lat, lon, alt_km) in enumerate(gateways):
            gs_id = len(sats) + idx
            r_ecef = geodetic_to_ecef(lat, lon, alt_km)
            r_eci = ecef_to_eci(r_ecef, jd, fr)
            ground_stations.append({
                "id": gs_id,
                "name": f"GW-{gname}",
                "kind": "gateway",
                "lat_deg": lat, "lon_deg": lon, "alt_km": alt_km,
                "r_eci": r_eci, "r_ecef": r_ecef,
                "shell_id": -1, "plane_id": -1,
            })

    all_nodes = len(sats) + len(ground_stations)
    degree = {nid: 0 for nid in range(all_nodes)}
    edge_pairs: set[tuple[int, int]] = set()
    edges: list[dict] = []

    # Intra-plane ring links
    for pid, ordered_ids in ordered_planes.items():
        for sid in ordered_ids:
            for nid in ring_neighbors(ordered_ids, sid, args.intra_plane):
                try_add_edge(sid, nid, sats_by_id, degree, edge_pairs, edges,
                             policy, jd, fr, kind="intra_plane")

    # Inter-plane links: same shell, allowed plane offsets only
    for shell_id, plane_map in planes_per_shell.items():
        pids = list(plane_map.keys())
        plane_of_sid = {sid: pid for pid, ids in plane_map.items() for sid in ids}
        if len(pids) < 2:
            continue
        # order planes by mean RAAN within shell (circular)
        mean_raan = {pid: circ_mean_deg([sats_by_id[s]["raan_deg"] for s in ids])
                     for pid, ids in plane_map.items()}
        order_in_shell = sorted(pids, key=lambda p: mean_raan[p])
        pos = {pid: i for i, pid in enumerate(order_in_shell)}
        n_planes = len(order_in_shell)

        for sid, shell_plane_id in plane_of_sid.items():
            p_here = pos[shell_plane_id]
            offsets = [o for o in policy.allowed_plane_offsets
                       if abs(o) <= min(args.inter_plane, n_planes // 2)]
            adj_pids = {order_in_shell[(p_here + o) % n_planes] for o in offsets}

            candidates: list[tuple[float, int]] = []
            for adj_pid in adj_pids:
                for nid in plane_map[adj_pid]:
                    if nid == sid:
                        continue
                    if degree[nid] >= policy.max_degree:
                        continue
                    r1, r2 = sats_by_id[sid]["r_eci"], sats_by_id[nid]["r_eci"]
                    d = distance_km(r1, r2)
                    if (policy.min_km <= d <= policy.max_km
                            and has_line_of_sight(r1, r2)):
                        candidates.append((d, nid))

            candidates.sort(key=lambda x: x[0])
            chosen = 0
            for _, nid in candidates:
                if chosen >= args.inter_plane:
                    break
                if degree[sid] >= policy.max_degree:
                    break
                if try_add_edge(sid, nid, sats_by_id, degree, edge_pairs, edges,
                                policy, jd, fr, kind="inter_plane"):
                    chosen += 1

    # Ground access links
    if ground_stations:
        build_access_links(
            ground_stations, sats, sats_by_id, degree,
            edge_pairs, edges, policy,
            args.gs_min_elevation_deg, args.gs_max_range_km,
            args.gs_max_sats, args.gs_max_per_sat, jd, fr,
        )

    edges.sort(key=lambda e: (e["u"], e["v"]))

    node_rows = [{
        "id": s["id"], "name": s["name"], "kind": "satellite",
        "shell_id": s["shell_id"], "plane_id": s["plane_id"],
        "inclination_deg": round(s["i_deg"], 4),
        "raan_deg": round(s["raan_deg"], 4),
        "argp_deg": round(s["argp_deg"], 4),
        "true_anomaly_deg": round(s["nu_deg"], 4),
        "arg_of_lat_deg": round(s["u_deg"], 4),
        "semi_major_axis_km": round(s["a_km"], 4),
        "altitude_km": round(s["geo_alt_km"], 4),
        "eci_x_km": s["r_eci"][0], "eci_y_km": s["r_eci"][1], "eci_z_km": s["r_eci"][2],
        "ecef_x_km": s["r_ecef"][0], "ecef_y_km": s["r_ecef"][1], "ecef_z_km": s["r_ecef"][2],
        "lat_deg": round(s["lat_deg"], 4),
        "lon_deg": round(s["lon_deg"], 4),
        "degree": degree[s["id"]],
    } for s in sats]

    for gs in ground_stations:
        node_rows.append({
            "id": gs["id"], "name": gs["name"], "kind": "gateway",
            "shell_id": -1, "plane_id": -1,
            "inclination_deg": 0.0, "raan_deg": 0.0,
            "argp_deg": 0.0, "true_anomaly_deg": 0.0, "arg_of_lat_deg": 0.0,
            "semi_major_axis_km": 0.0, "altitude_km": gs["alt_km"],
            "eci_x_km": gs["r_eci"][0], "eci_y_km": gs["r_eci"][1], "eci_z_km": gs["r_eci"][2],
            "ecef_x_km": gs["r_ecef"][0], "ecef_y_km": gs["r_ecef"][1], "ecef_z_km": gs["r_ecef"][2],
            "lat_deg": round(gs["lat_deg"], 4),
            "lon_deg": round(gs["lon_deg"], 4),
            "degree": degree[gs["id"]],
        })

    # prop_delay_ms is the canonical column name (schema 2.1.0): it is one-
    # way vacuum speed-of-light propagation time, NOT latency. delay_ms is
    # a deprecated alias kept so schema-2.0.0 readers do not break.
    edge_rows = [{
        "u": e["u"], "v": e["v"],
        "distance_km": e["distance_km"],
        "prop_delay_ms": e["delay_ms"],
        "delay_ms": e["delay_ms"],
        "kind": e["kind"],
        "shell_id": e["shell_id"],
    } for e in edges]

    return sats, ground_stations, edges, degree, shells, planes_per_shell, node_rows, edge_rows


def atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def atomic_write_json(obj, path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
    os.close(fd)
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def _resolve_filter(args) -> dict:
    """Merge the convenience --starlink_operational preset with any
    per-field overrides. Explicit CLI values always win over the preset.
    """
    base = dict.fromkeys(STARLINK_DEFAULTS.keys(), None)
    if args.starlink_operational:
        base.update(STARLINK_DEFAULTS)
    for k in base.keys():
        v = getattr(args, k, None)
        if v is not None:
            base[k] = v
    return base


def main():
    args = parse_args()

    if args.epoch_steps > 1 and not (
            args.multi_epoch_seconds and args.multi_epoch_seconds > 0.0):
        raise SystemExit(
            "--epoch_steps > 1 requires --multi_epoch_seconds > 0; "
            "otherwise every step reports the same epoch.")

    all_tles = read_tles(args.tle)
    if not all_tles:
        raise SystemExit(f"No TLEs found in {args.tle}")

    # Parse all TLEs, then apply operational filters BEFORE sampling so
    # --n operates on the post-filter population (otherwise a small head
    # sample of a contaminated file stays contaminated).
    all_satrecs = [Satrec.twoline2rv(l1, l2) for _, l1, l2 in all_tles]
    filt = _resolve_filter(args)
    filtered_tles, filtered_srs, filter_reasons = filter_operational(
        all_tles, all_satrecs, **filt)
    dropped = len(all_tles) - len(filtered_tles)
    if dropped:
        print(f"Operational filter dropped {dropped}/{len(all_tles)} TLEs "
              f"({filter_reasons}).")
    if not filtered_tles:
        raise SystemExit(
            "All TLEs were filtered out. Loosen --min_altitude_km / "
            "--max_altitude_km / --max_eccentricity / mean-motion bounds.")

    if args.sample == "random":
        rng = np.random.default_rng(args.seed)
        idx = rng.choice(len(filtered_tles),
                         size=min(args.n, len(filtered_tles)),
                         replace=False)
        raw_sats = [filtered_tles[i] for i in sorted(idx.tolist())]
        satrecs = [filtered_srs[i] for i in sorted(idx.tolist())]
    else:
        raw_sats = filtered_tles[:args.n]
        satrecs = filtered_srs[:args.n]

    jd, fr, epoch_mode = resolve_common_epoch(args, satrecs)
    if epoch_mode == "per-tle-epoch":
        print("WARNING: per-TLE-epoch mode is physically inconsistent. "
              "Distances, LOS and delays compare satellites at different "
              "time instants.", file=sys.stderr)

    # Gateway set
    gateways: list[tuple[str, float, float, float]] = []
    gateway_source = "disabled"
    if args.no_gateways:
        gateways = []
        gateway_source = "disabled"
    elif args.gateways_csv:
        if args.gateways_csv.strip():
            gateways = read_gateway_csv(args.gateways_csv)
            gateway_source = "csv"
    else:
        gateways = list(DEFAULT_GATEWAYS)
        gateway_source = "builtin_demo"
        print(
            "WARNING: using built-in DEMO gateway locations. These are city "
            "centroids, NOT real Starlink gateway sites. Pass --gateways_csv "
            "with authoritative locations for real studies.",
            file=sys.stderr,
        )

    # CSV safety: forbid commas inside gateway names so downstream CSV
    # readers (which are delimiter-splitting, not fully RFC 4180 compliant)
    # cannot be confused by a rogue name.
    for (gname, _, _, _) in gateways:
        if "," in gname or '"' in gname:
            raise SystemExit(
                f"Gateway name {gname!r} contains comma/quote; rename it "
                "so CSV consumers do not need quote-aware parsing.")

    epoch_steps = max(1, args.epoch_steps)
    step_dt_s = args.multi_epoch_seconds or 0.0

    base_edges_out = args.edges_out
    base_nodes_out = args.nodes_out
    all_validations = []

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
         node_rows, edge_rows) = build_snapshot(
            args, raw_sats, satrecs, jd_eval, fr_eval,
            epoch_mode, gateways)

        atomic_write_csv(pd.DataFrame(edge_rows), edges_out)
        atomic_write_csv(pd.DataFrame(node_rows), nodes_out)

        validation = validate_topology(
            sats, ground_stations, edges, degree,
            shells, planes_per_shell, args, strict=args.strict)
        all_validations.append({
            "step": step,
            "jd": jd_eval, "fr": fr_eval,
            "iso": jd_to_iso(jd_eval, fr_eval),
            **validation,
        })

        print(f"[step {step}] epoch={jd_to_iso(jd_eval, fr_eval)} "
              f"sats={len(sats)} gws={len(ground_stations)} "
              f"shells={len(shells)} edges={len(edges)} "
              f"largest_cc={validation['largest_component_size']} / "
              f"{validation['num_nodes']}")
        for w in validation["warnings"]:
            print(f"  WARNING: {w}", file=sys.stderr)
        for iss in validation["issues"]:
            print(f"  ERROR: {iss}", file=sys.stderr)

    # Stats
    stats_rows = []
    for v in all_validations:
        stats_rows.append({
            "step": v["step"],
            "epoch_utc": v["iso"],
            "num_nodes": v["num_nodes"],
            "num_satellites": v["num_satellites"],
            "num_gateways": v["num_gateways"],
            "num_edges": v["num_edges"],
            "num_isl": v["num_isl"],
            "num_access": v["num_access"],
            "num_shells": v["num_shells"],
            "num_components": v["num_components"],
            "largest_cc_size": v["largest_component_size"],
            "isolated_nodes": v["isolated_nodes"],
            "mean_isl_km": round(v["mean_isl_km"], 2),
            "max_isl_km": round(v["max_isl_km"], 2),
            "min_isl_km": round(v["min_isl_km"], 2),
            "mean_access_km": round(v["mean_access_km"], 2),
        })
    atomic_write_csv(pd.DataFrame(stats_rows), args.stats_out)

    meta = {
        "schema_version": SCHEMA_VERSION,
        "generator": "tle_to_snapshot.py",
        "epoch_policy": epoch_mode,
        "base_epoch_jd": jd,
        "base_epoch_fr": fr,
        "base_epoch_utc": jd_to_iso(jd, fr),
        "multi_epoch_seconds": step_dt_s,
        "epoch_steps": epoch_steps,
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
            "input_tles": len(all_tles),
            "post_filter_tles": len(filtered_tles),
        },
        "gateway_policy": {
            "enabled": bool(gateways) and not args.no_gateways,
            "min_elevation_deg": args.gs_min_elevation_deg,
            "max_range_km": args.gs_max_range_km,
            "max_sats_per_gs": args.gs_max_sats,
            "max_gs_per_sat": args.gs_max_per_sat,
            "source": gateway_source,
            "is_demo_only": gateway_source == "builtin_demo",
            "count": len([g for g in gateways]) if not args.no_gateways else 0,
        },
        "delay_field_meaning": (
            "edge delay_ms is one-way vacuum speed-of-light propagation only; "
            "real end-to-end latency also includes serialisation, queueing, "
            "scheduling, gateway/PoP hops and internet transit."),
        "sampling": {
            "mode": args.sample, "n": args.n, "seed": args.seed,
            "tle_file": os.path.abspath(args.tle),
        },
        "cli": {k: getattr(args, k) for k in vars(args)},
        "validation_per_step": all_validations,
    }
    atomic_write_json(meta, args.meta_out)

    any_issue = any(v["issues"] for v in all_validations)
    if any_issue and args.strict:
        raise SystemExit("Strict validation failed; see warnings above.")


if __name__ == "__main__":
    main()
