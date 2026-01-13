"""
Optimization rules for Sparkless query optimizer.

Provides specific optimization rules for different query patterns.
"""

from typing import List, Set, cast
from .query_optimizer import OptimizationRule, Operation, OperationType


class FilterPushdownRule(OptimizationRule):
    """Push filters as early as possible in the query plan."""

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


class ColumnPruningRule(OptimizationRule):
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


class JoinOptimizationRule(OptimizationRule):
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


class PredicatePushdownRule(OptimizationRule):
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


class ProjectionPushdownRule(OptimizationRule):
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


class LimitPushdownRule(OptimizationRule):
    """Push LIMIT operations as early as possible."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Push LIMIT operations to reduce data early."""
        if not self.can_apply(operations):
            return operations

        # Find the smallest LIMIT
        min_limit = None
        for op in operations:
            if op.type == OperationType.LIMIT and op.limit_count is not None:  # noqa: SIM102
                if min_limit is None or op.limit_count < min_limit:
                    min_limit = op.limit_count

        if min_limit is None:
            return operations

        # Push LIMIT to the earliest possible operation
        optimized = []
        limit_pushed = False

        for op in operations:
            if not limit_pushed and self._can_push_limit_to(op):
                # Add LIMIT to this operation
                optimized_op = Operation(
                    type=op.type,
                    columns=op.columns,
                    predicates=op.predicates,
                    join_conditions=op.join_conditions,
                    group_by_columns=op.group_by_columns,
                    order_by_columns=op.order_by_columns,
                    limit_count=min_limit,
                    window_specs=op.window_specs,
                    metadata=op.metadata,
                )
                optimized.append(optimized_op)
                limit_pushed = True
            else:
                optimized.append(op)

        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if limit pushdown can be applied."""
        return any(op.type == OperationType.LIMIT for op in operations)

    def _can_push_limit_to(self, op: Operation) -> bool:
        """Check if LIMIT can be pushed to this operation."""
        # Can push to SELECT, FILTER, but not to GROUP_BY, ORDER_BY, WINDOW
        return op.type in [OperationType.SELECT, OperationType.FILTER]


class UnionOptimizationRule(OptimizationRule):
    """Optimize UNION operations."""

    def apply(self, operations: List[Operation]) -> List[Operation]:
        """Optimize UNION operations by removing duplicates early."""
        if not self.can_apply(operations):
            return operations

        optimized = []
        union_ops = []
        other_ops = []

        # Separate UNION operations from others
        for op in operations:
            if op.type == OperationType.UNION:
                union_ops.append(op)
            else:
                other_ops.append(op)

        # Optimize UNION operations
        if union_ops:
            optimized_unions = self._optimize_unions(union_ops)
            optimized.extend(optimized_unions)

        optimized.extend(other_ops)
        return optimized

    def can_apply(self, operations: List[Operation]) -> bool:
        """Check if union optimization can be applied."""
        union_ops = [op for op in operations if op.type == OperationType.UNION]
        return len(union_ops) > 0

    def _optimize_unions(self, union_ops: List[Operation]) -> List[Operation]:
        """Optimize UNION operations."""
        # Simple optimization: combine consecutive UNIONs
        if len(union_ops) <= 1:
            return union_ops

        # For now, just return as-is
        # More sophisticated optimizations could be added here
        return union_ops
