"""
Maintainer-facing helpers to pretty-print and inspect Sparkless/Robin logical plans.

Used for debugging the Robin-backed execution path (Phase 4 tooling). See
docs/development/debugging_plans.md for workflow.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


def get_logical_plan(df: DataFrame, format: str = "sparkless") -> List[Any]:
    """Build the logical plan for a DataFrame.

    Args:
        df: Sparkless DataFrame (may have pending operations).
        format: "sparkless" for internal plan format, "robin" for Robin JSON plan.
            For "robin", unsupported operations/expressions may raise ValueError.

    Returns:
        JSON-serializable list of plan entries (each with "op" and "payload").

    Raises:
        ValueError: If format is "robin" and the plan contains operations or
            expressions that the Robin plan builder cannot convert.
    """
    if format == "sparkless":
        from sparkless.dataframe.logical_plan import to_logical_plan

        return to_logical_plan(df)
    if format == "robin":
        from sparkless.dataframe.robin_plan import to_robin_plan

        return to_robin_plan(df)
    raise ValueError(f"Unknown plan format: {format!r}. Use 'sparkless' or 'robin'.")


def pretty_print_logical_plan(
    df: DataFrame,
    format: str = "sparkless",
    return_str: bool = False,
) -> str | None:
    """Pretty-print the logical plan for a DataFrame.

    Args:
        df: Sparkless DataFrame.
        format: "sparkless" or "robin". For "robin", may raise if plan is not convertible.
        return_str: If True, return the formatted string; otherwise print to stdout and return None.

    Returns:
        Formatted plan string if return_str is True, else None.
    """
    plan = get_logical_plan(df, format=format)
    out = json.dumps(plan, indent=2, default=str)
    if return_str:
        return out
    print(out)
    return None
