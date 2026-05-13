"""Paper analytical equation for interruption probability."""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from itertools import permutations


EARTH_RADIUS_KM = 6371.0
DEFAULT_THETA_R = math.pi / 6.0
DEFAULT_THETA_S = math.pi / 10.0
DEFAULT_D_TH_KM = 4000.0
DEFAULT_THETA_M = math.pi

PAPER_PM = {
    (1, 2, 3): 0.3432,
    (1, 3, 2): 0.3417,
    (2, 1, 3): 0.2135,
    (2, 3, 1): 0.1122,
    (3, 1, 2): 0.1155,
    (3, 2, 1): 0.1033,
}


@dataclass(frozen=True)
class Tier:
    name: str
    altitude_km: float
    count: int

    @property
    def radius_km(self) -> float:
        return EARTH_RADIUS_KM + self.altitude_km


def zero_matrix(rows: int, cols: int) -> list[list[float]]:
    return [[0.0 for _ in range(cols)] for _ in range(rows)]


def identity_matrix(size: int) -> list[list[float]]:
    matrix = zero_matrix(size, size)
    for index in range(size):
        matrix[index][index] = 1.0
    return matrix


def matrix_multiply(left: list[list[float]],
                    right: list[list[float]]) -> list[list[float]]:
    rows = len(left)
    cols = len(right[0])
    inner = len(right)
    out = zero_matrix(rows, cols)
    for row in range(rows):
        for mid in range(inner):
            if left[row][mid] == 0.0:
                continue
            for col in range(cols):
                out[row][col] += left[row][mid] * right[mid][col]
    return out


def matrix_power(matrix: list[list[float]], exponent: int) -> list[list[float]]:
    out = identity_matrix(len(matrix))
    base = [row[:] for row in matrix]
    power = exponent
    while power > 0:
        if power % 2 == 1:
            out = matrix_multiply(out, base)
        base = matrix_multiply(base, base)
        power //= 2
    return out


def row_vector_times_matrix(vector: list[float],
                            matrix: list[list[float]]) -> list[float]:
    out = [0.0 for _ in range(len(matrix[0]))]
    for index, value in enumerate(vector):
        if value == 0.0:
            continue
        for col, matrix_value in enumerate(matrix[index]):
            out[col] += value * matrix_value
    return out


def theta_max_dome_ij(left: Tier,
                      right: Tier,
                      theta_s: float,
                      d_th_km: float) -> float:
    """Paper equation (1): largest valid dome angle between two tiers."""

    earth_block = (
        math.acos(EARTH_RADIUS_KM / left.radius_km)
        + math.acos(EARTH_RADIUS_KM / right.radius_km)
    )
    arg = (
        left.radius_km ** 2
        + right.radius_km ** 2
        - d_th_km ** 2
    ) / (2.0 * left.radius_km * right.radius_km)
    arg = max(-1.0, min(1.0, arg))
    distance_block = math.acos(arg)
    return max(theta_s, min(distance_block, earth_block))


def tier_to_tier_interruption(tiers: list[Tier],
                              theta_r: float,
                              theta_s: float,
                              d_th_km: float) -> list[list[float]]:
    """Paper equation (2): single-hop interruption matrix P_I."""

    count = len(tiers)
    interruption = zero_matrix(count, count)
    for left_index, left in enumerate(tiers):
        for right_index, right in enumerate(tiers):
            theta_ij = theta_max_dome_ij(left, right, theta_s, d_th_km)
            base = 1.0 - (theta_r / (4.0 * math.pi)) * (
                math.cos(theta_s) - math.cos(theta_ij)
            )
            base = max(0.0, min(1.0, base))
            node_count = right.count if left_index != right_index else max(0, right.count - 1)
            interruption[left_index][right_index] = base ** node_count if node_count else 1.0
    return interruption


def single_hop_interruption(interruption: list[list[float]]) -> list[float]:
    out: list[float] = []
    for row in interruption:
        product = 1.0
        for value in row:
            product *= value
        out.append(product)
    return out


def higher_priority_indices(strategy: tuple[int, ...], tier_index: int) -> list[int]:
    return [other for other in range(len(strategy)) if strategy[other] < strategy[tier_index]]


def build_t1(strategy: tuple[int, ...],
             interruption: list[list[float]]) -> list[list[float]]:
    """Paper algorithm 1."""

    count = len(strategy)
    total_interrupt = single_hop_interruption(interruption)
    matrix = zero_matrix(count, count)
    for row in range(count):
        denominator = 1.0 - total_interrupt[row]
        if denominator <= 0.0:
            continue
        for col in range(count):
            value = (1.0 - interruption[row][col]) / denominator
            for higher in higher_priority_indices(strategy, col):
                value *= interruption[row][higher]
            matrix[row][col] = value
        row_sum = sum(matrix[row])
        if row_sum > 0.0:
            matrix[row] = [value / row_sum for value in matrix[row]]
    return matrix


def build_t2(strategy: tuple[int, ...],
             interruption: list[list[float]]) -> list[list[float]]:
    """Paper algorithm 2: augmented transition matrix."""

    count = len(strategy)
    total_interrupt = single_hop_interruption(interruption)
    matrix = zero_matrix(count + 1, count + 1)
    for row in range(count):
        for col in range(count):
            value = 1.0 - interruption[row][col]
            for higher in higher_priority_indices(strategy, col):
                value *= interruption[row][higher]
            matrix[row][col] = value
        matrix[row][count] = total_interrupt[row]
    matrix[count][count] = 1.0
    return matrix


def build_t3(strategy: tuple[int, ...],
             interruption: list[list[float]]) -> list[list[float]]:
    """Paper algorithm 3: penultimate-hop transition matrix."""

    count = len(strategy)
    can_reach_gateway = [interruption[tier][0] < 1.0 for tier in range(count)]
    matrix = zero_matrix(count + 1, count + 1)
    for row in range(count):
        row_sum = 0.0
        for col in range(count):
            if not can_reach_gateway[col]:
                continue
            value = 1.0 - interruption[row][col]
            for higher in higher_priority_indices(strategy, col):
                if can_reach_gateway[higher]:
                    value *= interruption[row][higher]
            matrix[row][col] = value
            row_sum += value
        matrix[row][count] = max(0.0, 1.0 - row_sum)
    matrix[count][count] = 1.0
    return matrix


def stationary_distribution(t1: list[list[float]]) -> list[float]:
    """Approximate the stationary distribution by iterating v = v T1."""

    count = len(t1)
    vector = [1.0 / count for _ in range(count)]
    for _ in range(5000):
        next_vector = row_vector_times_matrix(vector, t1)
        delta = max(abs(next_vector[index] - vector[index]) for index in range(count))
        vector = next_vector
        if delta < 1e-12:
            break
    total = sum(vector)
    if total <= 0.0:
        return [1.0 / count for _ in range(count)]
    return [value / total for value in vector]


def average_dome_progress(tiers: list[Tier],
                          t1: list[list[float]],
                          stationary: list[float],
                          theta_r: float,
                          theta_s: float,
                          d_th_km: float) -> float:
    """Paper equation (6): average dome-angle progress per successful hop."""

    total = 0.0
    factor = 2.0 * math.pi / theta_r
    for left_index, left in enumerate(tiers):
        for right_index, right in enumerate(tiers):
            if t1[left_index][right_index] <= 0.0:
                continue
            node_count = right.count if left_index != right_index else max(1, right.count - 1)
            log_prod = 0.0
            for k in range(1, node_count + 1):
                log_prod += math.log((2 * k - 1) / (2 * k))
                if log_prod < -50.0:
                    break
            prod = math.exp(log_prod)
            theta_ij = theta_max_dome_ij(left, right, theta_s, d_th_km)
            inner = factor - factor * math.cos(math.pi * prod) + math.cos(theta_ij)
            inner = max(-1.0, min(1.0, inner))
            total += stationary[left_index] * t1[left_index][right_index] * math.acos(inner)
    return total


def estimate_successful_hops(theta_m: float, average_dome: float) -> int:
    if average_dome <= 0.0:
        return 1
    return max(1, int(round(theta_m / average_dome)))


def interruption_probability(strategy: tuple[int, ...],
                             tiers: list[Tier],
                             theta_r: float,
                             theta_s: float,
                             d_th_km: float,
                             theta_m: float) -> tuple[float, int]:
    """Paper theorem 1 / equation (7)."""

    interruption = tier_to_tier_interruption(tiers, theta_r, theta_s, d_th_km)
    t1 = build_t1(strategy, interruption)
    t2 = build_t2(strategy, interruption)
    t3 = build_t3(strategy, interruption)
    stationary = stationary_distribution(t1)
    average_dome = average_dome_progress(tiers, t1, stationary, theta_r, theta_s, d_th_km)
    hop_count = estimate_successful_hops(theta_m, average_dome)

    source = [1.0] + [0.0 for _ in range(len(strategy))]
    matrix = t3 if hop_count <= 2 else matrix_multiply(matrix_power(t2, hop_count - 2), t3)
    probability = row_vector_times_matrix(source, matrix)[len(strategy)]
    return max(0.0, min(1.0, probability)), hop_count


def parse_strategy(text: str) -> tuple[int, ...]:
    numbers = tuple(int(piece.strip()) for piece in text.split(","))
    if set(numbers) != {1, 2, 3}:
        raise ValueError("Strategy must be a permutation of 1,2,3")
    return numbers


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper analytical equation only.")
    parser.add_argument("--strategy", type=parse_strategy, default=None,
                        help="Optional single strategy, for example 3,2,1")
    args = parser.parse_args()

    tiers = [
        Tier("gateway", 0.0, 300),
        Tier("kepler_like", 575.0, 140),
        Tier("oneweb_like", 1200.0, 720),
    ]
    strategies = [args.strategy] if args.strategy else list(permutations([1, 2, 3]))

    print("Paper equation summary")
    print("----------------------")
    print("Tiers:")
    for index, tier in enumerate(tiers, start=1):
        print(f"  tier {index}: {tier.name}, h={tier.altitude_km:.0f} km, N={tier.count}")
    print()
    print("strategy  equation  paper_PM")
    for strategy in strategies:
        probability, hop_count = interruption_probability(
            strategy=strategy,
            tiers=tiers,
            theta_r=DEFAULT_THETA_R,
            theta_s=DEFAULT_THETA_S,
            d_th_km=DEFAULT_D_TH_KM,
            theta_m=DEFAULT_THETA_M,
        )
        paper_value = PAPER_PM.get(strategy)
        paper_text = f"{paper_value:.4f}" if paper_value is not None else "-"
        print(f"{list(strategy)}  {probability:.4f}    {paper_text}")


if __name__ == "__main__":
    main()
