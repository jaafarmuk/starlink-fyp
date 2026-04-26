"""
MHR (Multi-Hop Routing) reliability analysis vs. Starlink TLE snapshots.

Implements the analytical model from:
    R. Wang, M. A. Kishk, M.-S. Alouini,
    "Reliability Analysis of Multi-hop Routing in Multi-tier LEO Satellite
    Networks", arXiv:2303.02286, 2023.

Compares the paper's analytical interruption probability (computed under
the binomial point process / stochastic-geometry abstraction) against the
empirical interruption probability measured on a real TLE-derived snapshot
of the Starlink constellation produced by tools/tle_to_snapshot.py.

Inputs
------
  * results/snapshot_nodes.csv  (and .t1.csv .. .tN-1.csv if present)
  * results/snapshot_edges.csv  (and .t1.csv .. .tN-1.csv if present)
  * results/snapshot_meta.json  (epoch / shell / sampling metadata)

Outputs
-------
  * results/mhr_reliability_summary.csv      one row per (strategy, epoch)
  * results/mhr_reliability_per_epoch.csv    same data, organised by epoch
  * results/mhr_reliability_summary.json     full nested summary
  * results/mhr_reliability_per_pair.csv     one row per simulated pair
                                             (only with --write-per-pair)

Paper notation used in the code (Wang et al. arXiv:2303.02286):
  theta_r   maximum direction angle (constraint c1)
  theta_s   minimum dome angle      (constraint c2)
  theta_ij  maximum dome angle of relays from tier i and tier j
            (constraint c3, equation (1))
  P^I_{i,j} tier-to-tier single-hop interruption probability  (eq. 2)
  P^S_i     single-hop total interruption probability         (eq. 3)
  T^(1)     tier-to-tier transition probability matrix        (alg. 1)
  T~^(2)    augmented TPM with absorbing interrupt state      (alg. 2)
  T^^(3)    augmented TPM for the last hop                    (alg. 3)
  v         stationary distribution of T^(1)
  mu_i      average number of hops before interruption        (eq. 4)
  N_h       average number of hops for successful transmission(eq. 5)
  P~^M      approximate multi-hop interruption probability    (eq. 7, thm 1)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sys
from itertools import permutations
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Wang et al. use a spherical Earth with R_1 = 6371 km. We use the same
# radius for analytical formulas. The TLE snapshot uses WGS84 (6378.137 km)
# but for the dome-angle reliability analysis the difference is negligible
# (< 0.11 percent on R) and well below the parameter sensitivity.
R_EARTH_KM = 6371.0

# Empirical-routing line-of-sight check. Using 6371 km (mean spherical Earth)
# matches the paper. The snapshot generator stores ECEF positions on the
# WGS84 ellipsoid, so non-equatorial gateways have ECEF radius < 6378.137 km
# and would "fail" LoS against the equatorial radius even when straight up.
R_EARTH_LOS_KM = 6371.0

DEFAULT_THETA_R = math.pi / 6.0      # max direction angle  (Wang p.16)
DEFAULT_THETA_S = math.pi / 10.0     # min dome angle       (Wang p.16)
DEFAULT_D_TH_KM = 4000.0             # max comm. distance   (Wang p.16)
DEFAULT_THETA_M = math.pi            # transmitter/receiver dome angle


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def vec3(row: pd.Series) -> np.ndarray:
    """Return the node ECI position as a 3-vector (km)."""
    return np.array([row["eci_x_km"], row["eci_y_km"], row["eci_z_km"]],
                    dtype=float)


def dome_angle(a: np.ndarray, b: np.ndarray) -> float:
    """Wang Definition 1: the angle between two lines from two devices to
    the centre of the Earth (i.e. the angle between position vectors)."""
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0.0 or nb == 0.0:
        return 0.0
    c = float(np.dot(a, b)) / (na * nb)
    c = max(-1.0, min(1.0, c))
    return math.acos(c)


def direction_angle(current: np.ndarray, candidate: np.ndarray,
                    receiver: np.ndarray) -> float:
    """Wang Definition 2: at relay A=current with candidate B=candidate and
    final receiver D, the direction angle is the angle between (B - A) and
    (D - A). Constraint c1 requires this <= theta_r."""
    ab = candidate - current
    ad = receiver - current
    nab = np.linalg.norm(ab)
    nad = np.linalg.norm(ad)
    if nab == 0.0 or nad == 0.0:
        return 0.0
    c = float(np.dot(ab, ad)) / (nab * nad)
    c = max(-1.0, min(1.0, c))
    return math.acos(c)


def has_line_of_sight(p: np.ndarray, q: np.ndarray,
                      earth_radius_km: float = R_EARTH_LOS_KM) -> bool:
    """The segment p..q does not pass through the Earth."""
    d = q - p
    denom = float(np.dot(d, d))
    if denom == 0.0:
        return False
    t = -float(np.dot(p, d)) / denom
    t = max(0.0, min(1.0, t))
    closest = p + t * d
    return float(np.linalg.norm(closest)) >= earth_radius_km


# ---------------------------------------------------------------------------
# Tier / shell inference
# ---------------------------------------------------------------------------

def infer_tiers(nodes: pd.DataFrame,
                alt_bin_km: float = 30.0) -> List[Dict]:
    """Group snapshot nodes into Wang-style tiers.

    Tier 1 is gateways (kind == 'gateway') if any are present. Satellite
    tiers come next, ordered by ascending altitude. Within satellites, we
    group by shell_id when shell_id >= 0 is meaningful; otherwise we bin by
    altitude (alt_bin_km).
    """
    tiers: List[Dict] = []

    gws = nodes[nodes["kind"] == "gateway"]
    if not gws.empty:
        tiers.append({
            "name": "gateway",
            "kind": "gateway",
            "shell_id": -1,
            "altitude_km": 0.0,
            "R_km": R_EARTH_KM,
            "N": int(len(gws)),
            "node_ids": gws["id"].tolist(),
        })

    sats = nodes[nodes["kind"] == "satellite"].copy()
    if sats.empty:
        return tiers

    if (sats["shell_id"] >= 0).any() and sats["shell_id"].nunique() >= 1:
        groups = []
        for sid, df in sats.groupby("shell_id"):
            if int(sid) < 0 and len(df) < 2:
                # Fall back to altitude binning for stragglers.
                continue
            groups.append((float(df["altitude_km"].mean()), int(sid), df))
        groups.sort(key=lambda t: t[0])
        for mean_alt, sid, df in groups:
            tiers.append({
                "name": f"sat_shell_{sid}",
                "kind": "satellite",
                "shell_id": sid,
                "altitude_km": mean_alt,
                "R_km": R_EARTH_KM + mean_alt,
                "N": int(len(df)),
                "node_ids": df["id"].tolist(),
            })
    else:
        # Altitude bins.
        alt = sats["altitude_km"].values
        lo = float(np.min(alt))
        hi = float(np.max(alt))
        nbins = max(1, int(math.ceil((hi - lo + 1e-6) / alt_bin_km)))
        edges = np.linspace(lo, hi + 1e-6, nbins + 1)
        idx = np.clip(np.searchsorted(edges, alt, side="right") - 1, 0, nbins - 1)
        sats = sats.assign(_bin=idx)
        for b, df in sats.groupby("_bin"):
            tiers.append({
                "name": f"sat_alt_{b}",
                "kind": "satellite",
                "shell_id": -1,
                "altitude_km": float(df["altitude_km"].mean()),
                "R_km": R_EARTH_KM + float(df["altitude_km"].mean()),
                "N": int(len(df)),
                "node_ids": df["id"].tolist(),
            })
        tiers.sort(key=lambda t: t["altitude_km"] if t["kind"] == "satellite" else -1.0)

    # Re-index the gateway-first ordering. Wang uses tier 1 = ground.
    tiers.sort(key=lambda t: (t["kind"] != "gateway", t["altitude_km"]))
    for i, t in enumerate(tiers):
        t["tier_index"] = i
    return tiers


def build_node_to_tier(nodes: pd.DataFrame,
                       tiers: List[Dict]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for t in tiers:
        for nid in t["node_ids"]:
            out[nid] = t["tier_index"]
    return out


# ---------------------------------------------------------------------------
# Wang analytical model: equations (1)-(7)
# ---------------------------------------------------------------------------

def theta_max_dome_ij(R_i: float, R_j: float,
                     theta_s: float, d_th_km: float) -> float:
    """Equation (1): maximum dome angle of relays from tiers i and j,
    bounded above by line-of-sight (Earth blockage) and below by theta_s."""
    earth_block = (math.acos(min(1.0, max(-1.0, R_EARTH_KM / R_i)))
                   + math.acos(min(1.0, max(-1.0, R_EARTH_KM / R_j))))
    arg = (R_i * R_i + R_j * R_j - d_th_km * d_th_km) / (2.0 * R_i * R_j)
    arg = max(-1.0, min(1.0, arg))
    distance_block = math.acos(arg)
    inner_min = min(distance_block, earth_block)
    return max(theta_s, inner_min)


def tier_to_tier_interruption(tiers: List[Dict],
                              theta_r: float, theta_s: float,
                              d_th_km: float) -> np.ndarray:
    """Equation (2): tier-to-tier single-hop interruption probability matrix."""
    K = len(tiers)
    P = np.zeros((K, K), dtype=float)
    for i in range(K):
        for j in range(K):
            theta_ij = theta_max_dome_ij(
                tiers[i]["R_km"], tiers[j]["R_km"], theta_s, d_th_km)
            cos_diff = math.cos(theta_s) - math.cos(theta_ij)
            base = 1.0 - (theta_r / (4.0 * math.pi)) * cos_diff
            base = max(0.0, min(1.0, base))
            if i == j:
                N = max(0, tiers[i]["N"] - 1)
            else:
                N = tiers[j]["N"]
            P[i, j] = base ** N if N > 0 else 1.0
    return P


def single_hop_interruption(P_I: np.ndarray) -> np.ndarray:
    """Equation (3): P^S_i is the product over j of P^I_{i,j}."""
    return np.array([float(np.prod(P_I[i, :])) for i in range(P_I.shape[0])])


def _priority_iter_higher_than(s: List[int], j: int) -> List[int]:
    """Indices k for which s[j] > s[k] (i.e. k strictly higher priority)."""
    return [k for k in range(len(s)) if s[k] < s[j]]


def build_T1(s: List[int], P_I: np.ndarray) -> np.ndarray:
    """Algorithm 1: tier-to-tier TPM under priority strategy s."""
    K = len(s)
    P_S = single_hop_interruption(P_I)
    T = np.zeros((K, K), dtype=float)
    for i in range(K):
        denom = 1.0 - P_S[i]
        for j in range(K):
            if denom <= 0.0:
                T[i, j] = 0.0
                continue
            v = (1.0 - P_I[i, j]) / denom
            for k in _priority_iter_higher_than(s, j):
                v *= P_I[i, k]
            T[i, j] = v
        # Row-normalise to be safe against numerical drift.
        rs = T[i, :].sum()
        if rs > 0.0:
            T[i, :] /= rs
    return T


def build_T2_aug(s: List[int], P_I: np.ndarray) -> np.ndarray:
    """Algorithm 2: augmented TPM with the absorbing interrupt state."""
    K = len(s)
    P_S = single_hop_interruption(P_I)
    T = np.zeros((K + 1, K + 1), dtype=float)
    for i in range(K):
        for j in range(K):
            v = 1.0 - P_I[i, j]
            for k in _priority_iter_higher_than(s, j):
                v *= P_I[i, k]
            T[i, j] = v
        T[i, K] = P_S[i]
    T[K, K] = 1.0
    return T


def build_T3_aug(s: List[int], P_I: np.ndarray) -> np.ndarray:
    """Algorithm 3: augmented TPM for the penultimate hop, where tiers
    that cannot reach tier 1 (gateway / receiver) get absorbed."""
    K = len(s)
    can_reach_first = np.array([P_I[j, 0] < 1.0 for j in range(K)], dtype=bool)
    T = np.zeros((K + 1, K + 1), dtype=float)
    for i in range(K):
        row_sum = 0.0
        for j in range(K):
            if not can_reach_first[j]:
                continue
            v = 1.0 - P_I[i, j]
            for k in _priority_iter_higher_than(s, j):
                if can_reach_first[k]:
                    v *= P_I[i, k]
            T[i, j] = v
            row_sum += v
        T[i, K] = max(0.0, 1.0 - row_sum)
    T[K, K] = 1.0
    return T


def stationary_distribution(T1: np.ndarray) -> np.ndarray:
    """Left eigenvector of T^(1) corresponding to eigenvalue 1."""
    K = T1.shape[0]
    if K == 1:
        return np.array([1.0])
    eigvals, eigvecs = np.linalg.eig(T1.T)
    idx = int(np.argmin(np.abs(eigvals - 1.0)))
    v = np.real(eigvecs[:, idx])
    s = np.sum(v)
    if abs(s) < 1e-12:
        # Fallback: uniform.
        return np.ones(K) / float(K)
    v = v / s
    # Numerical clean-up.
    v = np.where(v < 0.0, 0.0, v)
    s = float(np.sum(v))
    return v / s if s > 0 else np.ones(K) / float(K)


def expected_hops_before_interrupt(T2: np.ndarray) -> np.ndarray:
    """Equation (4): mu_i = 1 + sum_j T2[i,j] * mu_j on transient states."""
    K = T2.shape[0] - 1
    A = np.eye(K) - T2[:K, :K]
    try:
        mu = np.linalg.solve(A, np.ones(K))
    except np.linalg.LinAlgError:
        mu = np.full(K, np.nan)
    return mu


def average_dome_per_step(tiers: List[Dict], T1: np.ndarray, v: np.ndarray,
                          theta_r: float, theta_s: float,
                          d_th_km: float) -> float:
    """Equation (6): average dome angle progressed per hop."""
    K = len(tiers)
    factor = 2.0 * math.pi / theta_r
    th = 0.0
    for i in range(K):
        for j in range(K):
            if T1[i, j] <= 0.0 or v[i] <= 0.0:
                continue
            if i == j:
                N = max(1, tiers[i]["N"] - 1)
            else:
                N = max(1, tiers[j]["N"])
            # Wallis-style product approximation from the paper.
            # For large N this rolls off as 1/sqrt(N); we cap to avoid
            # underflow but leave the analytical value in place.
            log_prod = 0.0
            for k in range(1, N + 1):
                log_prod += math.log((2 * k - 1) / (2 * k))
                if log_prod < -50.0:
                    break
            prod = math.exp(log_prod)
            theta_ij = theta_max_dome_ij(
                tiers[i]["R_km"], tiers[j]["R_km"], theta_s, d_th_km)
            inner = factor - factor * math.cos(math.pi * prod) + math.cos(theta_ij)
            inner = max(-1.0, min(1.0, inner))
            theta_oij = math.acos(inner)
            th += v[i] * T1[i, j] * theta_oij
    return th


def estimate_N_h(theta_m: float, theta_o: float) -> int:
    """Equation (5): N_h = round(theta_m / theta_o)."""
    if theta_o <= 0:
        return 1
    return max(1, int(round(theta_m / theta_o)))


def multi_hop_interruption(s: List[int], P_I: np.ndarray, N_h: int,
                           source_tier: int = 0) -> Tuple[float, np.ndarray, np.ndarray]:
    """Equation (7) / Theorem 1: P~^M for a route starting at `source_tier`."""
    K = len(s)
    T2 = build_T2_aug(s, P_I)
    T3 = build_T3_aug(s, P_I)
    e_src = np.zeros(K + 1)
    e_src[source_tier] = 1.0
    if N_h <= 1:
        m = T3
    elif N_h == 2:
        m = T3
    else:
        m = np.linalg.matrix_power(T2, N_h - 2) @ T3
    p = float((e_src @ m)[K])
    return max(0.0, min(1.0, p)), T2, T3


# ---------------------------------------------------------------------------
# Priority strategies (Wang sec. III-C and V-A)
# ---------------------------------------------------------------------------

def strategy_density(tiers: List[Dict]) -> List[int]:
    """Higher satellite density per unit area => higher priority. Gateway
    tier (kind == 'gateway') is pushed to the lowest priority because it
    cannot directly relay between two ground points (Wang p.21)."""
    K = len(tiers)
    sat_idx = [i for i, t in enumerate(tiers) if t["kind"] != "gateway"]
    gw_idx = [i for i, t in enumerate(tiers) if t["kind"] == "gateway"]

    def density(i: int) -> float:
        t = tiers[i]
        return t["N"] / (4.0 * math.pi * t["R_km"] ** 2)

    sat_idx.sort(key=density, reverse=True)
    ranking = sat_idx + gw_idx
    s = [0] * K
    for prio, idx in enumerate(ranking, start=1):
        s[idx] = prio
    return s


def strategy_single_hop(tiers: List[Dict], P_I: np.ndarray) -> List[int]:
    """Sort tiers by ascending P^S_i, smallest first."""
    K = len(tiers)
    P_S = single_hop_interruption(P_I)
    order = sorted(range(K), key=lambda i: P_S[i])
    s = [0] * K
    for prio, idx in enumerate(order, start=1):
        s[idx] = prio
    return s


def strategy_stationary_optimal(tiers: List[Dict],
                                P_I: np.ndarray) -> Tuple[List[int], float]:
    """Algorithm 4: brute-force search for the priority strategy that
    minimises the weighted single-hop interruption probability under the
    stationary distribution. K! grows quickly; we cap at K <= 7 (5040 perms).
    """
    K = len(tiers)
    if K > 7:
        # Too expensive; fall back to single-hop heuristic.
        return strategy_single_hop(tiers, P_I), float("nan")

    P_S = single_hop_interruption(P_I)
    best_p = math.inf
    best_s: Optional[List[int]] = None
    for perm in permutations(range(1, K + 1)):
        s = list(perm)
        T1 = build_T1(s, P_I)
        v = stationary_distribution(T1)
        # weighted single-hop interruption
        w = float(np.dot(v, P_S))
        if w < best_p:
            best_p = w
            best_s = s
    return best_s if best_s is not None else strategy_single_hop(tiers, P_I), best_p


# ---------------------------------------------------------------------------
# Empirical multi-hop simulator
# ---------------------------------------------------------------------------

def precompute_node_arrays(nodes: pd.DataFrame) -> Dict:
    ids = nodes["id"].tolist()
    pos = np.stack([nodes["eci_x_km"].values,
                    nodes["eci_y_km"].values,
                    nodes["eci_z_km"].values], axis=1).astype(float)
    norms = np.linalg.norm(pos, axis=1)
    kinds = nodes["kind"].tolist()
    return {
        "ids": ids,
        "id_to_idx": {nid: i for i, nid in enumerate(ids)},
        "pos": pos,
        "norms": norms,
        "kinds": kinds,
    }


def candidate_constraints_ok(cur_pos: np.ndarray, cur_norm: float,
                             cand_pos: np.ndarray, cand_norm: float,
                             receiver_pos: np.ndarray,
                             theta_r: float, theta_s: float,
                             d_th_km: float,
                             enforce_min_dome: bool = True,
                             enforce_direction: bool = True
                             ) -> Tuple[bool, float]:
    """Check Wang constraints c1 (direction angle), c2 (min dome angle),
    and c3 (line-of-sight + max distance d_th). Returns (ok, dome_to_receiver).

    When `enforce_min_dome` is False the c2 check is skipped. When
    `enforce_direction` is False the c1 check is skipped. Both relaxations
    are applied to ground-to-satellite (uplink) and satellite-to-ground
    (downlink) hops because the gateway is at altitude 0: every visible
    satellite has dome angle bounded by the local horizon (~22 deg) and
    its (B - A) vector points strongly outward, so theta_s/theta_r as
    defined for satellite-to-satellite hops would forbid every uplink.
    """
    if cur_norm == 0.0 or cand_norm == 0.0:
        return False, math.inf

    dist = float(np.linalg.norm(cand_pos - cur_pos))
    if dist > d_th_km:
        return False, math.inf

    cos_d = float(np.dot(cur_pos, cand_pos)) / (cur_norm * cand_norm)
    cos_d = max(-1.0, min(1.0, cos_d))
    dome = math.acos(cos_d)
    if enforce_min_dome and dome < theta_s:
        return False, math.inf

    if not has_line_of_sight(cur_pos, cand_pos):
        return False, math.inf

    if enforce_direction:
        da = direction_angle(cur_pos, cand_pos, receiver_pos)
        if da > theta_r:
            return False, math.inf

    # Dome angle from the candidate to the final receiver (used for greedy
    # progression toward the destination).
    rec_norm = float(np.linalg.norm(receiver_pos))
    if rec_norm == 0.0:
        return False, math.inf
    cos_r = float(np.dot(cand_pos, receiver_pos)) / (cand_norm * rec_norm)
    cos_r = max(-1.0, min(1.0, cos_r))
    dome_to_rx = math.acos(cos_r)
    return True, dome_to_rx


def simulate_route(node_arr: Dict,
                   node_to_tier: Dict[str, int],
                   tiers: List[Dict],
                   priority: List[int],
                   src_id: str, dst_id: str,
                   theta_r: float, theta_s: float, d_th_km: float,
                   max_hops: int = 50) -> Dict:
    """Greedy Wang-style multi-hop forwarding from src_id to dst_id.

    At each hop we:
      1. Check if the destination is directly reachable (LoS, distance
         <= d_th, direction angle <= theta_r). Min-dome is relaxed for the
         final hop (Wang Sec. IV-A also adjusts the priority strategy in
         the penultimate/last hop for the same reason).
      2. Otherwise gather all snapshot nodes that pass c1/c2/c3 from the
         current node, group them by tier, and choose the highest-priority
         non-empty tier. Within that tier, pick the candidate with the
         smallest dome angle to the receiver.
      3. If no candidate passes, the route is interrupted at this hop.
    """
    pos = node_arr["pos"]
    norms = node_arr["norms"]
    id_to_idx = node_arr["id_to_idx"]
    kinds = node_arr["kinds"]

    if src_id not in id_to_idx or dst_id not in id_to_idx:
        return {
            "success": False, "interrupt": False, "hops": 0,
            "interrupted_hop": -1, "reason": "missing_endpoint",
            "path": [],
        }

    src_idx = id_to_idx[src_id]
    dst_idx = id_to_idx[dst_id]
    receiver_pos = pos[dst_idx]
    rec_norm = float(np.linalg.norm(receiver_pos))

    current_idx = src_idx
    path = [src_id]
    visited = {src_idx}

    for hop in range(1, max_hops + 1):
        cur_pos = pos[current_idx]
        cur_norm = norms[current_idx]

        # Direct hop to receiver?
        dist_dst = float(np.linalg.norm(receiver_pos - cur_pos))
        if dist_dst <= d_th_km and has_line_of_sight(cur_pos, receiver_pos):
            # The very first hop must still respect direction; subsequent
            # ones do too via c1 in candidate_constraints_ok. Direct hop
            # to the destination is allowed regardless of min-dome (the
            # paper's last-hop relaxation, Sec. IV-A).
            path.append(dst_id)
            return {
                "success": True, "interrupt": False, "hops": hop,
                "interrupted_hop": -1, "reason": "reached_destination",
                "path": path,
            }

        # Gather candidates passing c1/c2/c3.
        best_per_tier: Dict[int, Tuple[float, int]] = {}
        rel_pos = pos - cur_pos
        dists = np.linalg.norm(rel_pos, axis=1)
        # Cheap pre-filter on distance to avoid expensive LoS checks for
        # everything in the snapshot.
        candidate_idxs = np.where((dists > 0.0) & (dists <= d_th_km))[0]
        # Relax theta_s on uplink/downlink: a ground gateway has no useful
        # geographic progress threshold to a satellite directly overhead.
        cur_is_ground = (kinds[current_idx] == "gateway")
        for cand_idx in candidate_idxs:
            if cand_idx == current_idx or cand_idx == dst_idx or cand_idx in visited:
                continue
            cand_is_ground = (kinds[cand_idx] == "gateway")
            relax = cur_is_ground or cand_is_ground
            ok, dome_to_rx = candidate_constraints_ok(
                cur_pos, cur_norm, pos[cand_idx], norms[cand_idx],
                receiver_pos, theta_r, theta_s, d_th_km,
                enforce_min_dome=not relax,
                enforce_direction=not relax)
            if not ok:
                continue
            cand_id = node_arr["ids"][cand_idx]
            tier_idx = node_to_tier.get(cand_id, -1)
            if tier_idx < 0:
                continue
            best = best_per_tier.get(tier_idx)
            if best is None or dome_to_rx < best[0]:
                best_per_tier[tier_idx] = (dome_to_rx, cand_idx)

        if not best_per_tier:
            return {
                "success": False, "interrupt": True, "hops": hop,
                "interrupted_hop": hop, "reason": "no_candidates",
                "path": path,
            }

        # Pick the tier with the highest priority (smallest priority value).
        chosen_tier = min(best_per_tier.keys(), key=lambda t: priority[t])
        _, next_idx = best_per_tier[chosen_tier]
        path.append(node_arr["ids"][next_idx])
        visited.add(next_idx)
        current_idx = next_idx

    return {
        "success": False, "interrupt": True, "hops": max_hops,
        "interrupted_hop": max_hops, "reason": "max_hops_exceeded",
        "path": path,
    }


# ---------------------------------------------------------------------------
# Source/destination sampling
# ---------------------------------------------------------------------------

def sample_pairs(nodes: pd.DataFrame, tiers: List[Dict],
                 num_pairs: int, theta_m_min: float,
                 rng: random.Random,
                 endpoint_kind: str = "auto"
                 ) -> List[Tuple[str, str, float]]:
    """Pick `num_pairs` (src, dst, theta_m_pair) triples with dome
    angle >= theta_m_min. Returns the actual observed dome angle so the
    analytical model can be evaluated at the empirically realised theta_m
    instead of the user-supplied target."""
    has_gateway = any(t["kind"] == "gateway" for t in tiers)
    if endpoint_kind == "auto":
        kind = "gateway" if has_gateway else "satellite"
    else:
        kind = endpoint_kind
    pool = nodes[nodes["kind"] == kind].reset_index(drop=True)
    if len(pool) < 2:
        # fall back to satellites
        pool = nodes[nodes["kind"] == "satellite"].reset_index(drop=True)
        kind = "satellite"
    if len(pool) < 2:
        return []

    pos = np.stack([pool["eci_x_km"].values,
                    pool["eci_y_km"].values,
                    pool["eci_z_km"].values], axis=1)
    norms = np.linalg.norm(pos, axis=1)

    pairs: List[Tuple[str, str, float]] = []
    seen = set()
    attempts = 0
    max_attempts = max(2000, num_pairs * 50)
    while len(pairs) < num_pairs and attempts < max_attempts:
        attempts += 1
        i = rng.randrange(len(pool))
        j = rng.randrange(len(pool))
        if i == j:
            continue
        key = (i, j) if i < j else (j, i)
        if key in seen:
            continue
        cos_d = float(np.dot(pos[i], pos[j])) / (norms[i] * norms[j])
        cos_d = max(-1.0, min(1.0, cos_d))
        dome = math.acos(cos_d)
        if dome + 1e-9 < theta_m_min:
            continue
        seen.add(key)
        pairs.append((pool["id"].iloc[i], pool["id"].iloc[j], dome))
    return pairs


# ---------------------------------------------------------------------------
# Multi-epoch discovery
# ---------------------------------------------------------------------------

def discover_epoch_files(nodes_path: str,
                         edges_path: str) -> List[Tuple[int, str, str]]:
    """Return [(step, nodes_path, edges_path), ...] for step 0..N-1."""
    out = []
    if not (os.path.exists(nodes_path) and os.path.exists(edges_path)):
        return out
    out.append((0, nodes_path, edges_path))

    base_nodes_stem, ext_nodes = os.path.splitext(nodes_path)
    base_edges_stem, ext_edges = os.path.splitext(edges_path)
    step = 1
    while True:
        n = f"{base_nodes_stem}.t{step}{ext_nodes}"
        e = f"{base_edges_stem}.t{step}{ext_edges}"
        if not (os.path.exists(n) and os.path.exists(e)):
            break
        out.append((step, n, e))
        step += 1
    return out


# ---------------------------------------------------------------------------
# Per-epoch analysis pipeline
# ---------------------------------------------------------------------------

def analyse_one_epoch(nodes_path: str, edges_path: str,
                      meta_step: Optional[Dict],
                      args, rng: random.Random) -> List[Dict]:
    nodes = pd.read_csv(nodes_path)
    # edges file is loaded for completeness but the empirical routing in
    # this analysis is geometric (it does not follow the precomputed ISL
    # graph; it uses the same Wang constraints the analytical model uses).
    _edges = pd.read_csv(edges_path) if os.path.exists(edges_path) else None

    tiers = infer_tiers(nodes)
    if not tiers:
        return []

    K = len(tiers)
    P_I = tier_to_tier_interruption(
        tiers, args.theta_r, args.theta_s, args.d_th_km)
    P_S = single_hop_interruption(P_I)

    strategies: Dict[str, List[int]] = {
        "density": strategy_density(tiers),
        "single_hop": strategy_single_hop(tiers, P_I),
    }
    if K <= args.max_K_for_optimal:
        s_opt, _ = strategy_stationary_optimal(tiers, P_I)
        strategies["stationary_optimal"] = s_opt

    node_arr = precompute_node_arrays(nodes)
    node_to_tier = build_node_to_tier(nodes, tiers)
    pairs = sample_pairs(nodes, tiers, args.pairs, args.theta_m_min,
                         rng, args.endpoint_kind)

    epoch_label = (meta_step or {}).get("iso") or "unknown"
    step = (meta_step or {}).get("step", 0)

    # Source tier index for the analytical model: pick the tier the source
    # endpoints actually live in (gateway if available, else lowest sat tier).
    source_tier = 0
    if pairs:
        src_id = pairs[0][0]
        source_tier = node_to_tier.get(src_id, 0)

    if pairs:
        observed_theta_m = float(np.mean([p[2] for p in pairs]))
    else:
        observed_theta_m = float("nan")
    if args.theta_m_mode == "fixed" or not pairs:
        analytical_theta_m = args.theta_m
    else:
        analytical_theta_m = observed_theta_m

    rows: List[Dict] = []
    for sname, s in strategies.items():
        T1 = build_T1(s, P_I)
        v_dist = stationary_distribution(T1)
        T2 = build_T2_aug(s, P_I)
        mu = expected_hops_before_interrupt(T2)
        theta_o = average_dome_per_step(
            tiers, T1, v_dist, args.theta_r, args.theta_s, args.d_th_km)
        N_h = estimate_N_h(analytical_theta_m, theta_o) if theta_o > 0 else 1
        p_analytical, _, _ = multi_hop_interruption(s, P_I, N_h, source_tier)

        # Empirical: run the simulator under this priority strategy.
        n_success = 0
        n_interrupt = 0
        success_hops: List[int] = []
        interrupted_hops: List[int] = []
        per_pair_rows: List[Dict] = []
        for (src, dst, theta_m_pair) in pairs:
            r = simulate_route(node_arr, node_to_tier, tiers, s,
                               src, dst,
                               args.theta_r, args.theta_s, args.d_th_km,
                               max_hops=args.max_hops)
            if r["success"]:
                n_success += 1
                success_hops.append(r["hops"])
            elif r["interrupt"]:
                n_interrupt += 1
                interrupted_hops.append(r["interrupted_hop"])
            if args.write_per_pair:
                per_pair_rows.append({
                    "epoch_step": step,
                    "epoch_utc": epoch_label,
                    "strategy": sname,
                    "src": src, "dst": dst,
                    "theta_m_rad": theta_m_pair,
                    "success": int(r["success"]),
                    "interrupt": int(r["interrupt"]),
                    "hops": r["hops"],
                    "interrupted_hop": r["interrupted_hop"],
                    "reason": r["reason"],
                })

        decided = n_success + n_interrupt
        empirical_p = (n_interrupt / decided) if decided > 0 else float("nan")
        abs_err = (abs(empirical_p - p_analytical)
                   if not math.isnan(empirical_p) else float("nan"))
        rel_err = (abs_err / empirical_p
                   if (decided > 0 and empirical_p > 0) else float("nan"))

        row = {
            "epoch_step": step,
            "epoch_utc": epoch_label,
            "strategy": sname,
            "priority_vector": ",".join(str(x) for x in s),
            "K": K,
            "tiers": ";".join(t["name"] for t in tiers),
            "tier_sizes": ";".join(str(t["N"]) for t in tiers),
            "tier_altitudes_km": ";".join(f"{t['altitude_km']:.2f}" for t in tiers),
            "theta_r_rad": args.theta_r,
            "theta_s_rad": args.theta_s,
            "theta_m_input_rad": args.theta_m,
            "theta_m_observed_rad": observed_theta_m,
            "theta_m_used_rad": analytical_theta_m,
            "d_th_km": args.d_th_km,
            "P_S": ";".join(f"{x:.6g}" for x in P_S.tolist()),
            "v_stationary": ";".join(f"{x:.6g}" for x in v_dist.tolist()),
            "mu_1": float(mu[0]) if len(mu) > 0 and not np.isnan(mu[0]) else float("nan"),
            "theta_o_rad": theta_o,
            "N_h": N_h,
            "bpp_predicted_interruption_probability": p_analytical,
            "pairs_total": len(pairs),
            "pairs_decided": decided,
            "pairs_success": n_success,
            "pairs_interrupted": n_interrupt,
            "empirical_interruption_probability": empirical_p,
            "absolute_error": abs_err,
            "relative_error": rel_err,
            "mean_success_hops": (float(np.mean(success_hops))
                                   if success_hops else float("nan")),
            "mean_interrupted_hop": (float(np.mean(interrupted_hops))
                                      if interrupted_hops else float("nan")),
            "per_pair_rows": per_pair_rows if args.write_per_pair else None,
        }
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=("Compare BPP analytical interruption probability "
                     "(Wang/Kishk/Alouini, arXiv:2303.02286) with empirical "
                     "multi-hop routing on a Starlink TLE snapshot."),
    )
    ap.add_argument("--nodes", default="results/snapshot_nodes.csv")
    ap.add_argument("--edges", default="results/snapshot_edges.csv")
    ap.add_argument("--meta",  default="results/snapshot_meta.json")
    ap.add_argument("--summary_csv",
                    default="results/mhr_reliability_summary.csv")
    ap.add_argument("--per_epoch_csv",
                    default="results/mhr_reliability_per_epoch.csv")
    ap.add_argument("--per_pair_csv",
                    default="results/mhr_reliability_per_pair.csv")
    ap.add_argument("--summary_json",
                    default="results/mhr_reliability_summary.json")
    ap.add_argument("--theta_r", type=float, default=DEFAULT_THETA_R,
                    help="max direction angle (rad). default pi/6.")
    ap.add_argument("--theta_s", type=float, default=DEFAULT_THETA_S,
                    help="min dome angle (rad). default pi/10.")
    ap.add_argument("--theta_m", type=float, default=DEFAULT_THETA_M,
                    help="dome angle between transmitter and receiver (rad). default pi (paper-style).")
    ap.add_argument("--theta_m_min", type=float, default=math.pi / 3.0,
                    help=("minimum dome angle when sampling (src, dst) pairs (rad). "
                          "default pi/3 keeps pairs reasonably distant given a "
                          "small gateway list."))
    ap.add_argument("--theta_m_mode", choices=("observed", "fixed"),
                    default="observed",
                    help=("how to set the theta_m used in the analytical model. "
                          "'observed' uses the mean dome angle of sampled pairs "
                          "(makes the analytical / empirical comparison "
                          "apples-to-apples). 'fixed' uses --theta_m verbatim."))
    ap.add_argument("--d_th_km", type=float, default=DEFAULT_D_TH_KM,
                    help="max single-hop reliable distance (km). default 4000.")
    ap.add_argument("--pairs", type=int, default=200,
                    help="number of empirical (src, dst) pairs per epoch.")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--max_hops", type=int, default=50)
    ap.add_argument("--max_K_for_optimal", type=int, default=6,
                    help="skip the brute-force optimal strategy if K exceeds this.")
    ap.add_argument("--endpoint_kind", choices=("auto", "gateway", "satellite"),
                    default="auto",
                    help=("source/destination kind: auto picks gateway when "
                          "available, otherwise satellite."))
    ap.add_argument("--write-per-pair", action="store_true",
                    help="also write a per-pair CSV (large for many pairs).")
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)

    epochs = discover_epoch_files(args.nodes, args.edges)
    if not epochs:
        print(f"ERROR: no snapshot found at {args.nodes} / {args.edges}",
              file=sys.stderr)
        return 2

    meta_steps: Dict[int, Dict] = {}
    if os.path.exists(args.meta):
        try:
            with open(args.meta, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            for v in meta.get("validation_per_step", []) or []:
                meta_steps[int(v.get("step", 0))] = v
        except Exception as exc:
            print(f"WARNING: could not read meta {args.meta}: {exc}",
                  file=sys.stderr)
            meta = {}
    else:
        meta = {}

    print(f"Discovered {len(epochs)} epoch snapshot(s).")
    all_rows: List[Dict] = []
    for step, npath, epath in epochs:
        print(f"[epoch step={step}] nodes={os.path.basename(npath)} "
              f"edges={os.path.basename(epath)}")
        rows = analyse_one_epoch(npath, epath, meta_steps.get(step), args, rng)
        all_rows.extend(rows)

    if not all_rows:
        print("ERROR: no analysis rows produced.", file=sys.stderr)
        return 3

    # Per-pair CSV (optional, large). When not requested, remove any stale
    # per-pair file from a previous run so consumers do not mix epochs.
    if args.write_per_pair:
        per_pair: List[Dict] = []
        for r in all_rows:
            pp = r.pop("per_pair_rows", None) or []
            per_pair.extend(pp)
        os.makedirs(os.path.dirname(args.per_pair_csv) or ".", exist_ok=True)
        pd.DataFrame(per_pair).to_csv(args.per_pair_csv, index=False)
        print(f"Wrote {len(per_pair)} rows to {args.per_pair_csv}")
    else:
        for r in all_rows:
            r.pop("per_pair_rows", None)
        if os.path.exists(args.per_pair_csv):
            try:
                os.remove(args.per_pair_csv)
            except OSError:
                pass

    df = pd.DataFrame(all_rows)
    summary_cols = [
        "epoch_step", "epoch_utc", "strategy", "K",
        "tiers", "tier_sizes", "tier_altitudes_km", "priority_vector",
        "theta_r_rad", "theta_s_rad",
        "theta_m_input_rad", "theta_m_observed_rad", "theta_m_used_rad",
        "d_th_km",
        "P_S", "v_stationary",
        "mu_1", "theta_o_rad", "N_h",
        "bpp_predicted_interruption_probability",
        "empirical_interruption_probability",
        "absolute_error", "relative_error",
        "pairs_total", "pairs_decided", "pairs_success", "pairs_interrupted",
        "mean_success_hops", "mean_interrupted_hop",
    ]
    summary_cols = [c for c in summary_cols if c in df.columns]
    df = df[summary_cols]

    os.makedirs(os.path.dirname(args.summary_csv) or ".", exist_ok=True)
    df.to_csv(args.summary_csv, index=False)
    df.to_csv(args.per_epoch_csv, index=False)
    print(f"Wrote {len(df)} rows to {args.summary_csv}")
    print(f"Wrote {len(df)} rows to {args.per_epoch_csv}")

    summary = {
        "schema_version": "mhr-reliability-1.0",
        "source_meta": meta.get("schema_version") if isinstance(meta, dict) else None,
        "base_epoch_utc": meta.get("base_epoch_utc") if isinstance(meta, dict) else None,
        "epoch_steps": len(epochs),
        "parameters": {
            "theta_r_rad": args.theta_r,
            "theta_s_rad": args.theta_s,
            "theta_m_rad": args.theta_m,
            "theta_m_min_rad": args.theta_m_min,
            "theta_m_mode": args.theta_m_mode,
            "d_th_km": args.d_th_km,
            "pairs": args.pairs,
            "seed": args.seed,
            "endpoint_kind": args.endpoint_kind,
            "max_hops": args.max_hops,
        },
        "rows": json.loads(df.to_json(orient="records")),
    }
    os.makedirs(os.path.dirname(args.summary_json) or ".", exist_ok=True)
    with open(args.summary_json, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
    print(f"Wrote summary to {args.summary_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
