"""
Query Optimizer for Sparkless.

Provides comprehensive query optimization including filter pushdown,
column pruning, join optimization, and memory management.
"""

from typing import Any, Dict, List, Optional, Set, cast
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from sparkless import config


class OperationType(Enum):
    """Types of DataFrame operations."""

    SELECT = "select"
    FILTER = "filter"
    JOIN = "join"
    GROUP_BY = "group_by"
    ORDER_BY = "order_by"
    LIMIT = "limit"
    WITH_COLUMN = "with_column"
    DROP = "drop"
    UNION = "union"
    WINDOW = "window"
    REPARTITION = "repartition"


@dataclass
class Operation:
    """Represents a DataFrame operation in the query plan."""

    type: OperationType
    columns: List[str]
    predicates: List[Dict[str, Any]]
    join_conditions: List[Dict[str, Any]]
    group_by_columns: List[str]
    order_by_columns: List[str]
    limit_count: Optional[int]
    window_specs: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    def __post_init__(self) -> None:
        if self.predicates is None:
            self.predicates = []  # type: ignore[unreachable]
        if self.join_conditions is None:
            self.join_conditions = []  # type: ignore[unreachable]
        if self.group_by_columns is None:
            self.group_by_columns = []  # type: ignore[unreachable]
        if self.order_by_columns is None:
            self.order_by_columns = []  # type: ignore[unreachable]
        if self.window_specs is None:
            self.window_specs = []  # type: ignore[unreachable]
        if self.metadata is None:
            self.metadata = {}  # type: ignore[unreachable]


class OptimizationRule(ABC):
    """Base class for optimization rules."""

    @abstractmethod
    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Apply optimization rule to operations."""
        pass

    @abstractmethod
    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if rule can be applied to operations."""
        pass


class FilterPushdownRuleLegacy(OptimizationRule):
    """Legacy inline FilterPushdownRule - use FilterPushdownRule from optimization_rules instead."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Move filter operations before other operations when possible."""
        if not self.can_apply(operations):
            return operations

        optimized = []
        filters = []

        for op in operations:
            if op.type == OperationType.FILTER:
                filters.extend(op.predicates)
            else:
                # If we have filters and this is a non-filter operation,
                # try to push filters before it
                if filters and self._can_push_filters_before(op):
                    # Create filter operation
                    filter_op = Operation(
                        type=OperationType.FILTER,
                        columns=[],
                        predicates=filters.copy(),
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                    optimized.append(filter_op)
                    filters = []

                optimized.append(op)

        # Add any remaining filters at the end
        if filters:
            filter_op = Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=filters,
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
            optimized.append(filter_op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if filter pushdown can be applied."""
        filter_ops = [op for op in operations if op.type == OperationType.FILTER]
        return len(filter_ops) > 0

    def _can_push_filters_before(self, op: Operation) -> bool:
        """Check if filters can be pushed before this operation."""
        # Can't push filters before GROUP_BY, ORDER_BY, or WINDOW
        return op.type not in [
            OperationType.GROUP_BY,
            OperationType.ORDER_BY,
            OperationType.WINDOW,
        ]


class ColumnPruningRuleLegacy(OptimizationRule):
    """Remove unnecessary columns from the query plan."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Remove columns that are not needed in the final result."""
        if not self.can_apply(operations):
            return operations

        # Find all columns that are actually needed
        needed_columns = self._find_needed_columns(operations)

        optimized = []
        for op in operations:
            if op.type == OperationType.SELECT:
                # Only select needed columns
                needed_for_op = [col for col in op.columns if col in needed_columns]
                if needed_for_op:
                    optimized_op = Operation(
                        type=op.type,
                        columns=needed_for_op,
                        predicates=op.predicates,
                        join_conditions=op.join_conditions,
                        group_by_columns=op.group_by_columns,
                        order_by_columns=op.order_by_columns,
                        limit_count=op.limit_count,
                        window_specs=op.window_specs,
                        metadata=op.metadata,
                    )
                    optimized.append(optimized_op)
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if column pruning can be applied."""
        select_ops = [op for op in operations if op.type == OperationType.SELECT]
        return len(select_ops) > 0

    def _find_needed_columns(self, operations: List[Operation]) -> Set[str]:
        """Find all columns that are actually needed."""
        needed = set()

        # Start from the end and work backwards
        for op in reversed(operations):
            if op.type == OperationType.SELECT:
                needed.update(op.columns)
            elif op.type == OperationType.FILTER:
                # Add columns used in filter predicates
                for pred in op.predicates:
                    if "column" in pred:
                        needed.add(pred["column"])
            elif op.type == OperationType.GROUP_BY:
                needed.update(op.group_by_columns)
            elif op.type == OperationType.ORDER_BY:
                needed.update(op.order_by_columns)
            elif op.type == OperationType.JOIN:
                # Add columns used in join conditions
                for condition in op.join_conditions:
                    if "left_column" in condition:
                        needed.add(condition["left_column"])
                    if "right_column" in condition:
                        needed.add(condition["right_column"])

        return needed


class JoinOptimizationRuleLegacy(OptimizationRule):
    """Optimize join operations for better performance."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Reorder joins and optimize join conditions."""
        if not self.can_apply(operations):
            return operations

        optimized = []
        join_ops = []
        other_ops = []

        # Separate join operations from others
        for op in operations:
            if op.type == OperationType.JOIN:
                join_ops.append(op)
            else:
                other_ops.append(op)

        # Optimize join order (simple heuristic: smaller tables first)
        if join_ops:
            optimized_joins = self._optimize_join_order(join_ops)
            optimized.extend(optimized_joins)

        optimized.extend(other_ops)
        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if join optimization can be applied."""
        join_ops = [op for op in operations if op.type == OperationType.JOIN]
        return len(join_ops) > 0

    def _optimize_join_order(self, join_ops: List[Operation]) -> List[Operation]:
        """Optimize the order of join operations."""

        # Simple heuristic: sort by estimated size (metadata)
        def get_estimated_size(op: Operation) -> int:
            return cast("int", op.metadata.get("estimated_size", 1000))  # Default size

        return sorted(join_ops, key=get_estimated_size)


class PredicatePushdownRuleLegacy(OptimizationRule):
    """Push predicates down to reduce data early."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Push predicates as early as possible."""
        if not self.can_apply(operations):
            return operations

        # Collect all predicates
        all_predicates = []
        for op in operations:
            if op.predicates:
                all_predicates.extend(op.predicates)

        # Push predicates to the earliest possible operation
        optimized = []
        predicates_pushed = False

        for op in operations:
            if not predicates_pushed and self._can_push_predicates_to(op):
                # Add predicates to this operation
                optimized_op = Operation(
                    type=op.type,
                    columns=op.columns,
                    predicates=all_predicates,
                    join_conditions=op.join_conditions,
                    group_by_columns=op.group_by_columns,
                    order_by_columns=op.order_by_columns,
                    limit_count=op.limit_count,
                    window_specs=op.window_specs,
                    metadata=op.metadata,
                )
                optimized.append(optimized_op)
                predicates_pushed = True
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if predicate pushdown can be applied."""
        return any(op.predicates for op in operations)

    def _can_push_predicates_to(self, op: Operation) -> bool:
        """Check if predicates can be pushed to this operation."""
        # Can push to SELECT, FILTER, but not to GROUP_BY, ORDER_BY, WINDOW
        return op.type in [OperationType.SELECT, OperationType.FILTER]


class ProjectionPushdownRuleLegacy(OptimizationRule):
    """Push column projections as early as possible."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Push column selections as early as possible."""
        if not self.can_apply(operations):
            return operations

        # Find all columns that will be needed
        needed_columns = self._find_final_columns(operations)

        optimized = []
        for op in operations:
            if op.type == OperationType.SELECT:
                # Only select columns that will be needed
                optimized_op = Operation(
                    type=op.type,
                    columns=list(needed_columns),
                    predicates=op.predicates,
                    join_conditions=op.join_conditions,
                    group_by_columns=op.group_by_columns,
                    order_by_columns=op.order_by_columns,
                    limit_count=op.limit_count,
                    window_specs=op.window_specs,
                    metadata=op.metadata,
                )
                optimized.append(optimized_op)
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if projection pushdown can be applied."""
        return any(op.type == OperationType.SELECT for op in operations)

    def _find_final_columns(self, operations: List[Operation]) -> Set[str]:
        """Find columns that will be in the final result."""
        # Start from the last SELECT operation
        for op in reversed(operations):
            if op.type == OperationType.SELECT:
                return set(op.columns)
        return set()


class QueryOptimizer:
    """Main query optimizer that applies multiple optimization rules."""

    def __init__(self) -> None:
        """Initialize optimizer with default rules."""
        # Import here to avoid circular import with optimization_rules
        from .optimization_rules import (
            FilterPushdownRule,
            ColumnPruningRule,
            JoinOptimizationRule,
            PredicatePushdownRule,
            ProjectionPushdownRule,
        )

        self.rules = [
            FilterPushdownRule(),
            ColumnPruningRule(),
            JoinOptimizationRule(),
            PredicatePushdownRule(),
            ProjectionPushdownRule(),
        ]
        self._adaptive_enabled = config.is_feature_enabled(
            "enable_adaptive_execution_simulation"
        )
        self._adaptive_threshold = 2.0
        self._adaptive_max_split_factor = 8
        self._adaptive_settings: Dict[str, Any] = {}

    def optimize(
        self,
        operations: List[Operation],
        runtime_stats: Optional[Dict[str, Any]] = None,
    ) -> List[Operation]:
        """Apply all optimization rules to the query plan."""
        optimized = operations.copy()

        # Apply rules in sequence
        for rule in self.rules:
            if rule.can_apply(optimized):
                optimized = rule.apply(optimized)

        if runtime_stats:
            self._adaptive_settings.update(runtime_stats)

        if self._adaptive_enabled:
            optimized = self._simulate_adaptive_execution(optimized)

        return optimized

    def configure_adaptive_execution(
        self,
        *,
        enabled: Optional[bool] = None,
        skew_threshold: Optional[float] = None,
        max_split_factor: Optional[int] = None,
    ) -> None:
        """Configure adaptive execution simulation parameters."""

        if enabled is not None:
            self._adaptive_enabled = enabled
        if skew_threshold is not None and skew_threshold > 1.0:
            self._adaptive_threshold = float(skew_threshold)
        if max_split_factor is not None and max_split_factor >= 1:
            self._adaptive_max_split_factor = int(max_split_factor)

    def reset_adaptive_state(self) -> None:
        """Clear cached adaptive runtime hints."""

        self._adaptive_settings.clear()

    def _simulate_adaptive_execution(
        self, operations: List[Operation]
    ) -> List[Operation]:
        rewritten: List[Operation] = []

        for op in operations:
            skew_metrics = self._extract_skew_metrics(op)
            if skew_metrics:
                try:
                    skew_ratio = float(skew_metrics.get("max_partition_ratio", 1.0))
                except (TypeError, ValueError):
                    skew_ratio = 1.0
                if skew_ratio >= self._adaptive_threshold:
                    partition_columns = self._resolve_partition_columns(
                        op, skew_metrics
                    )
                    if partition_columns:
                        split_factor = max(int(round(skew_ratio)), 2)
                        split_factor = min(
                            split_factor, self._adaptive_max_split_factor
                        )
                        rewritten.append(
                            Operation(
                                type=OperationType.REPARTITION,
                                columns=partition_columns,
                                predicates=[],
                                join_conditions=[],
                                group_by_columns=[],
                                order_by_columns=[],
                                limit_count=None,
                                window_specs=[],
                                metadata={
                                    "reason": "adaptive_skew_mitigation",
                                    "source_operation": op.type.value,
                                    "target_split_factor": split_factor,
                                    "skew_metrics": skew_metrics,
                                },
                            )
                        )
            rewritten.append(op)

        return rewritten

    def _extract_skew_metrics(self, op: Operation) -> Optional[Dict[str, Any]]:
        metrics = op.metadata.get("skew_metrics") if op.metadata else None
        if isinstance(metrics, dict):
            return metrics

        hints = self._adaptive_settings.get("skew_hints")
        if isinstance(hints, dict):
            identifier = op.metadata.get("plan_node_id") if op.metadata else None
            if (
                identifier
                and identifier in hints
                and isinstance(hints[identifier], dict)
            ):
                return cast("Dict[str, Any]", hints[identifier])
        return None

    def _resolve_partition_columns(
        self, op: Operation, metrics: Dict[str, Any]
    ) -> List[str]:
        columns = metrics.get("partition_columns")
        if isinstance(columns, list) and all(isinstance(col, str) for col in columns):
            return list(columns)

        inferred = []
        if op.group_by_columns:
            inferred.extend(op.group_by_columns)
        elif op.join_conditions:
            for condition in op.join_conditions:
                if isinstance(condition, dict):
                    left = condition.get("left_column")
                    right = condition.get("right_column")
                    if isinstance(left, str):
                        inferred.append(left)
                    if isinstance(right, str):
                        inferred.append(right)
        elif op.columns:
            inferred.extend(op.columns)

        # Deduplicate while preserving order
        seen: Set[str] = set()
        unique_columns = []
        for col in inferred:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)

        return unique_columns

    def add_rule(self, rule: OptimizationRule) -> None:
        """Add a custom optimization rule."""
        self.rules.append(rule)

    def remove_rule(self, rule_class: type) -> None:
        """Remove a rule by class."""
        self.rules = [rule for rule in self.rules if not isinstance(rule, rule_class)]

    def get_optimization_stats(
        self, original: List[Operation], optimized: List[Operation]
    ) -> Dict[str, Any]:
        """Get statistics about the optimization."""
        return {
            "original_operations": len(original),
            "optimized_operations": len(optimized),
            "operations_reduced": len(original) - len(optimized),
            "optimization_ratio": len(optimized) / len(original) if original else 1.0,
            "rules_applied": len(self.rules),
        }
