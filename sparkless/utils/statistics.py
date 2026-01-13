"""
Lightweight statistical helpers used across Sparkless.

These implementations avoid heavy optional dependencies such as NumPy so the
core library remains pure Python.  They intentionally mirror the behaviour
expected by PySpark compatibility shims (linear interpolation percentiles and
sample covariance).
"""

from __future__ import annotations

from math import fsum
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from typing import Sequence


def percentile(values: Sequence[float], percent: float) -> float:
    """
    Compute the percentile value using linear interpolation.

    Args:
        values: Ordered or unordered numeric sequence.
        percent: Percentile in the range [0, 100].

    Returns:
        Interpolated percentile result as float.  Returns NaN when the input
        sequence is empty to mirror NumPy's behaviour.
    """
    if not values:
        return float("nan")

    if percent <= 0:
        return float(sorted(values)[0])
    if percent >= 100:
        return float(sorted(values)[-1])

    ordered = sorted(float(v) for v in values)
    position = (len(ordered) - 1) * (percent / 100.0)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = position - lower_index

    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]

    return float(lower_value + (upper_value - lower_value) * weight)


def covariance(xs: Iterable[float], ys: Iterable[float], sample: bool = True) -> float:
    """
    Compute covariance between two numeric sequences.

    Args:
        xs: First sequence of numeric values.
        ys: Second sequence of numeric values.
        sample: When True (default) compute the sample covariance dividing by
                N-1.  When False compute population covariance dividing by N.

    Returns:
        Covariance as float.  Returns 0.0 when fewer than two paired values are
        provided to stay consistent with existing Sparkless semantics.
    """
    x_list = [float(x) for x in xs]
    y_list = [float(y) for y in ys]

    n = min(len(x_list), len(y_list))
    if n == 0:
        return 0.0
    if n == 1:
        return 0.0

    mean_x = fsum(x_list) / n
    mean_y = fsum(y_list) / n

    cov_total = fsum((x - mean_x) * (y - mean_y) for x, y in zip(x_list, y_list))

    divisor = (n - 1) if sample and n > 1 else n
    if divisor == 0:
        return 0.0

    return float(cov_total / divisor)
