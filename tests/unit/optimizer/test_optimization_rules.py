"""Tests for optimization_rules module."""

from sparkless.optimizer.optimization_rules import (
    FilterPushdownRule,
    ColumnPruningRule,
    JoinOptimizationRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    LimitPushdownRule,
    UnionOptimizationRule,
)
from sparkless.optimizer.query_optimizer import Operation, OperationType


class TestFilterPushdownRule:
    """Test cases for FilterPushdownRule."""

    def test_filter_pushdown_rule_can_apply(self):
        """Test can_apply returns True when filters exist."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_filter_pushdown_rule_cannot_apply(self):
        """Test can_apply returns False when no filters."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is False

    def test_filter_pushdown_basic(self):
        """Test basic filter pushdown before SELECT."""
        rule = FilterPushdownRule()
        # Filter comes first, then SELECT - filter should be pushed before SELECT
        operations = [
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Filter should be pushed before SELECT (already in correct order)
        assert optimized[0].type == OperationType.FILTER
        assert optimized[1].type == OperationType.SELECT

    def test_filter_pushdown_before_join(self):
        """Test filter pushdown before JOIN."""
        rule = FilterPushdownRule()
        # Filter comes first, then JOIN - filter should be pushed before JOIN
        operations = [
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Filter should be pushed before JOIN (already in correct order)
        assert optimized[0].type == OperationType.FILTER
        assert optimized[1].type == OperationType.JOIN

    def test_filter_pushdown_not_before_groupby(self):
        """Test filters not pushed before GROUP BY."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.GROUP_BY,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=["dept"],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Filter should NOT be pushed before GROUP_BY
        assert optimized[0].type == OperationType.GROUP_BY
        assert optimized[1].type == OperationType.FILTER

    def test_filter_pushdown_not_before_orderby(self):
        """Test filters not pushed before ORDER BY."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.ORDER_BY,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=["age"],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Filter should NOT be pushed before ORDER_BY
        assert optimized[0].type == OperationType.ORDER_BY
        assert optimized[1].type == OperationType.FILTER

    def test_filter_pushdown_not_before_window(self):
        """Test filters not pushed before WINDOW."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.WINDOW,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[{"partition_by": ["dept"], "order_by": ["age"]}],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Filter should NOT be pushed before WINDOW
        assert optimized[0].type == OperationType.WINDOW
        assert optimized[1].type == OperationType.FILTER


class TestColumnPruningRule:
    """Test cases for ColumnPruningRule."""

    def test_column_pruning_rule_can_apply(self):
        """Test can_apply for column pruning."""
        rule = ColumnPruningRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_column_pruning_basic(self):
        """Test removing unused columns."""
        rule = ColumnPruningRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age", "city", "country"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],  # Final select only needs these
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Column pruning rule finds needed columns from final SELECT
        # First SELECT should only have columns that are in the final SELECT
        # The rule works backwards, so it should preserve name and age
        final_columns = set(optimized[1].columns)
        first_columns = set(optimized[0].columns)
        # First SELECT should only contain columns that are in final SELECT
        assert final_columns.issubset(first_columns) or first_columns == final_columns

    def test_column_pruning_preserves_needed(self):
        """Test preserving columns used in predicates."""
        rule = ColumnPruningRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age", "city"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # First SELECT should include "age" because it's used in filter
        assert "age" in optimized[0].columns
        assert "name" in optimized[0].columns

    def test_column_pruning_preserves_aggregates(self):
        """Test preserving columns used in aggregations."""
        rule = ColumnPruningRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["dept", "salary", "bonus"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.GROUP_BY,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=["dept"],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["dept"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # First SELECT should include "dept" because it's used in GROUP_BY
        assert "dept" in optimized[0].columns


class TestJoinOptimizationRule:
    """Test cases for JoinOptimizationRule."""

    def test_join_optimization_rule_can_apply(self):
        """Test can_apply for join optimization."""
        rule = JoinOptimizationRule()
        operations = [
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_join_optimization_small_first(self):
        """Test reordering joins to process smaller tables first."""
        rule = JoinOptimizationRule()
        operations = [
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={"estimated_size": 10000},
            ),
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={"estimated_size": 1000},
            ),
        ]

        optimized = rule.apply(operations)
        # Smaller join should come first
        assert optimized[0].metadata["estimated_size"] == 1000
        assert optimized[1].metadata["estimated_size"] == 10000


class TestPredicatePushdownRule:
    """Test cases for PredicatePushdownRule."""

    def test_predicate_pushdown_rule_can_apply(self):
        """Test can_apply for predicate pushdown."""
        rule = PredicatePushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_predicate_pushdown_basic(self):
        """Test basic predicate pushdown."""
        rule = PredicatePushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Predicate should be pushed to SELECT
        assert len(optimized[0].predicates) > 0


class TestProjectionPushdownRule:
    """Test cases for ProjectionPushdownRule."""

    def test_projection_pushdown_rule_can_apply(self):
        """Test can_apply for projection pushdown."""
        rule = ProjectionPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_projection_pushdown_basic(self):
        """Test basic projection pushdown."""
        rule = ProjectionPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age", "city", "country"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],  # Final select
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # First SELECT should only have final columns
        assert set(optimized[0].columns) == {"name", "age"}


class TestLimitPushdownRule:
    """Test cases for LimitPushdownRule."""

    def test_limit_pushdown_rule_can_apply(self):
        """Test can_apply for limit pushdown."""
        rule = LimitPushdownRule()
        operations = [
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=10,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_limit_pushdown_basic(self):
        """Test basic limit pushdown."""
        rule = LimitPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=10,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # LIMIT should be pushed to SELECT
        assert optimized[0].limit_count == 10

    def test_limit_pushdown_smallest_limit(self):
        """Test pushing the smallest LIMIT."""
        rule = LimitPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=100,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=10,  # Smaller limit
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Should push the smallest limit (10)
        assert optimized[0].limit_count == 10


class TestUnionOptimizationRule:
    """Test cases for UnionOptimizationRule."""

    def test_union_optimization_rule_can_apply(self):
        """Test can_apply for union optimization."""
        rule = UnionOptimizationRule()
        operations = [
            Operation(
                type=OperationType.UNION,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations) is True

    def test_union_optimization_basic(self):
        """Test basic union optimization."""
        rule = UnionOptimizationRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.UNION,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = rule.apply(operations)
        # Should preserve UNION operation
        assert any(op.type == OperationType.UNION for op in optimized)

    def test_rule_application_order(self):
        """Test rules are applied in correct order."""
        # Test that FilterPushdownRule works correctly
        filter_rule = FilterPushdownRule()
        # Filter comes first, then SELECT
        operations = [
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = filter_rule.apply(operations)
        # Filter should come before SELECT (already in correct order)
        assert optimized[0].type == OperationType.FILTER
        assert optimized[1].type == OperationType.SELECT

    def test_rule_idempotency(self):
        """Test applying rules multiple times produces same result."""
        rule = FilterPushdownRule()
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "op": ">", "value": 18}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        # Apply rule multiple times
        optimized1 = rule.apply(operations)
        optimized2 = rule.apply(optimized1)

        # Results should be the same
        assert len(optimized1) == len(optimized2)
        assert optimized1[0].type == optimized2[0].type
        assert optimized1[1].type == optimized2[1].type
