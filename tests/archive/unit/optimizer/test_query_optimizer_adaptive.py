from __future__ import annotations
from typing import List, Union

from sparkless.optimizer.query_optimizer import (
    Operation,
    OperationType,
    QueryOptimizer,
)


def _make_operation(
    op_type: OperationType,
    *,
    columns: List[str] | None = None,
    predicates: List[dict] | None = None,
    join_conditions: List[dict] | None = None,
    group_by_columns: List[str] | None = None,
    order_by_columns: List[str] | None = None,
    limit_count: Union[int, None] = None,
    window_specs: List[dict] | None = None,
    metadata: Union[dict, None] = None,
) -> Operation:
    return Operation(
        type=op_type,
        columns=columns or [],
        predicates=predicates or [],
        join_conditions=join_conditions or [],
        group_by_columns=group_by_columns or [],
        order_by_columns=order_by_columns or [],
        limit_count=limit_count,
        window_specs=window_specs or [],
        metadata=metadata or {},
    )


def test_adaptive_execution_inserts_repartition_before_skewed_join() -> None:
    optimizer = QueryOptimizer()
    optimizer.configure_adaptive_execution(
        enabled=True, skew_threshold=1.5, max_split_factor=4
    )

    join_op = _make_operation(
        OperationType.JOIN,
        join_conditions=[{"left_column": "region", "right_column": "region"}],
        metadata={
            "skew_metrics": {
                "max_partition_ratio": 3.2,
                "partition_columns": ["region"],
                "hot_partitions": ["us-east"],
            }
        },
    )

    plan = optimizer.optimize([join_op])

    assert len(plan) == 2
    repartition = plan[0]
    assert repartition.type == OperationType.REPARTITION
    assert repartition.columns == ["region"]
    assert repartition.metadata["target_split_factor"] == 3
    assert repartition.metadata["reason"] == "adaptive_skew_mitigation"
    assert plan[1] == join_op


def test_adaptive_execution_uses_runtime_hints_when_metadata_missing() -> None:
    optimizer = QueryOptimizer()
    optimizer.configure_adaptive_execution(enabled=True, skew_threshold=1.2)

    join_op = _make_operation(
        OperationType.JOIN,
        join_conditions=[{"left_column": "customer_id", "right_column": "customer_id"}],
        metadata={"plan_node_id": "join-hotspot"},
    )

    runtime_stats = {
        "skew_hints": {
            "join-hotspot": {
                "max_partition_ratio": 5.0,
                "partition_columns": ["customer_id"],
            }
        }
    }

    plan = optimizer.optimize([join_op], runtime_stats=runtime_stats)

    assert len(plan) == 2
    assert plan[0].type == OperationType.REPARTITION
    assert plan[0].columns == ["customer_id"]
    assert plan[0].metadata["target_split_factor"] == 5

    # subsequent optimize calls should keep adaptive state unless reset
    optimizer.reset_adaptive_state()
    plan_after_reset = optimizer.optimize([join_op])
    assert all(op.type != OperationType.REPARTITION for op in plan_after_reset)


def test_adaptive_execution_respects_threshold() -> None:
    optimizer = QueryOptimizer()
    optimizer.configure_adaptive_execution(enabled=True, skew_threshold=4.0)

    join_op = _make_operation(
        OperationType.JOIN,
        join_conditions=[{"left_column": "region", "right_column": "region"}],
        metadata={
            "skew_metrics": {
                "max_partition_ratio": 2.5,
                "partition_columns": ["region"],
            }
        },
    )

    plan = optimizer.optimize([join_op])
    assert all(op.type != OperationType.REPARTITION for op in plan)


def test_adaptive_execution_disabled_by_default() -> None:
    optimizer = QueryOptimizer()  # feature flag disabled by default

    join_op = _make_operation(
        OperationType.JOIN,
        join_conditions=[{"left_column": "region", "right_column": "region"}],
        metadata={
            "skew_metrics": {
                "max_partition_ratio": 10.0,
                "partition_columns": ["region"],
            }
        },
    )

    plan = optimizer.optimize([join_op])
    assert len(plan) == 1
    assert plan[0] == join_op
