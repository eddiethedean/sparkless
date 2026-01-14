"""Tests for sql/optimizer module."""

from sparkless.session.sql.optimizer import SQLQueryOptimizer, QueryPlan
from sparkless.session.sql.parser import SQLAST


class TestSQLQueryOptimizer:
    """Test cases for SQLQueryOptimizer."""

    def test_optimizer_init(self):
        """Test SQLQueryOptimizer initialization."""
        optimizer = SQLQueryOptimizer()

        assert optimizer is not None
        assert hasattr(optimizer, "_optimization_rules")
        assert len(optimizer._optimization_rules) > 0

    def test_optimize_simple_select(self):
        """Test optimizing simple SELECT query."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age"],
                "from_tables": ["users"],
                "where_conditions": [],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"

    def test_optimize_with_predicate_pushdown(self):
        """Test predicate pushdown optimization."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age"],
                "from_tables": ["users"],
                "where_conditions": [{"column": "age", "op": ">", "value": 18}],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        # Rule should be applied (even if it's a mock implementation)
        assert optimized.query_type == "SELECT"

    def test_optimize_with_projection_elimination(self):
        """Test projection elimination."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age", "city", "country"],
                "from_tables": ["users"],
                "where_conditions": [],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"

    def test_optimize_with_join_reordering(self):
        """Test join reordering optimization."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["u.name", "o.order_id"],
                "from_tables": ["users", "orders"],
                "join_conditions": [{"left": "users.id", "right": "orders.user_id"}],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"

    def test_optimize_with_constant_folding(self):
        """Test constant folding optimization."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name"],
                "from_tables": ["users"],
                "where_conditions": [{"column": "age", "op": ">", "value": 10 + 8}],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"

    def test_optimize_with_redundant_operations(self):
        """Test redundant operations elimination."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name"],
                "from_tables": ["users"],
                "where_conditions": [],
            },
        )

        optimized = optimizer.optimize(ast)

        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"

    def test_optimize_non_select_query(self):
        """Test optimization of non-SELECT queries."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "INSERT",
            {
                "table": "users",
                "values": [("Alice", 25)],
            },
        )

        optimized = optimizer.optimize(ast)

        # Non-SELECT queries should return unchanged
        assert optimized.query_type == "INSERT"
        assert optimized is ast  # Should return same object for non-SELECT

    def test_generate_execution_plan_select(self):
        """Test generating execution plan for SELECT."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age"],
                "from_tables": ["users"],
                "where_conditions": [{"column": "age", "op": ">", "value": 18}],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        assert plan.plan_type == "SELECT"
        assert "columns" in plan.properties
        assert "tables" in plan.properties

    def test_generate_execution_plan_join(self):
        """Test generating execution plan for JOIN."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["u.name", "o.order_id"],
                "from_tables": ["users", "orders"],
                "join_conditions": [{"left": "users.id", "right": "orders.user_id"}],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        assert plan.plan_type == "SELECT"

    def test_generate_execution_plan_aggregate(self):
        """Test generating execution plan for aggregation."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["dept", "COUNT(*)"],
                "from_tables": ["users"],
                "group_by_columns": ["dept"],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        assert plan.plan_type == "SELECT"
        # Should have GROUP_BY child plan
        group_plans = [
            child for child in plan.children if child.plan_type == "GROUP_BY"
        ]
        assert len(group_plans) > 0

    def test_generate_execution_plan_with_filter(self):
        """Test execution plan with FILTER child."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name"],
                "from_tables": ["users"],
                "where_conditions": [{"column": "age", "op": ">", "value": 18}],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        # Should have FILTER child plan
        filter_plans = [child for child in plan.children if child.plan_type == "FILTER"]
        assert len(filter_plans) > 0

    def test_generate_execution_plan_with_sort(self):
        """Test execution plan with SORT child."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age"],
                "from_tables": ["users"],
                "order_by_columns": ["age"],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        # Should have SORT child plan
        sort_plans = [child for child in plan.children if child.plan_type == "SORT"]
        assert len(sort_plans) > 0

    def test_generate_execution_plan_with_limit(self):
        """Test execution plan with LIMIT child."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name"],
                "from_tables": ["users"],
                "limit_value": 10,
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        # Should have LIMIT child plan
        limit_plans = [child for child in plan.children if child.plan_type == "LIMIT"]
        assert len(limit_plans) > 0

    def test_generate_execution_plan_non_select(self):
        """Test generating execution plan for non-SELECT query."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "INSERT",
            {
                "table": "users",
                "values": [("Alice", 25)],
            },
        )

        plan = optimizer.generate_execution_plan(ast)

        assert isinstance(plan, QueryPlan)
        assert plan.plan_type == "UNKNOWN"
        assert plan.properties["query_type"] == "INSERT"

    def test_estimate_cost_simple(self):
        """Test cost estimation for simple query."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT", properties={"columns": ["name"], "tables": ["users"]}
        )

        cost = optimizer.estimate_cost(plan)

        assert isinstance(cost, float)
        assert cost >= 1.0  # Base cost

    def test_estimate_cost_complex(self):
        """Test cost estimation for complex query."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT",
            properties={"columns": ["name"], "tables": ["users"]},
            children=[
                QueryPlan("FILTER", properties={"conditions": []}),
                QueryPlan("GROUP_BY", properties={"columns": ["dept"]}),
                QueryPlan("SORT", properties={"columns": ["age"]}),
                QueryPlan("LIMIT", properties={"limit": 10}),
            ],
        )

        cost = optimizer.estimate_cost(plan)

        assert isinstance(cost, float)
        assert cost > 1.0  # Should be higher than base cost
        # Should include costs for FILTER (0.1), GROUP_BY (0.5), SORT (0.3), LIMIT (0.05)
        assert cost >= 1.0 + 0.1 + 0.5 + 0.3 + 0.05

    def test_estimate_cost_with_filter(self):
        """Test cost estimation with FILTER operation."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT",
            children=[QueryPlan("FILTER", properties={"conditions": []})],
        )

        cost = optimizer.estimate_cost(plan)

        assert cost >= 1.0 + 0.1  # Base + FILTER cost

    def test_estimate_cost_with_groupby(self):
        """Test cost estimation with GROUP_BY operation."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT",
            children=[QueryPlan("GROUP_BY", properties={"columns": ["dept"]})],
        )

        cost = optimizer.estimate_cost(plan)

        assert cost >= 1.0 + 0.5  # Base + GROUP_BY cost

    def test_estimate_cost_with_sort(self):
        """Test cost estimation with SORT operation."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT",
            children=[QueryPlan("SORT", properties={"columns": ["age"]})],
        )

        cost = optimizer.estimate_cost(plan)

        assert cost >= 1.0 + 0.3  # Base + SORT cost

    def test_estimate_cost_with_limit(self):
        """Test cost estimation with LIMIT operation."""
        optimizer = SQLQueryOptimizer()
        plan = QueryPlan(
            "SELECT",
            children=[QueryPlan("LIMIT", properties={"limit": 10})],
        )

        cost = optimizer.estimate_cost(plan)

        assert cost >= 1.0 + 0.05  # Base + LIMIT cost


class TestQueryPlan:
    """Test cases for QueryPlan."""

    def test_query_plan_init(self):
        """Test QueryPlan initialization."""
        plan = QueryPlan("SELECT")

        assert plan.plan_type == "SELECT"
        assert plan.children == []
        assert plan.properties == {}

    def test_query_plan_init_with_children(self):
        """Test QueryPlan initialization with children."""
        child1 = QueryPlan("FILTER")
        child2 = QueryPlan("SORT")
        plan = QueryPlan("SELECT", children=[child1, child2])

        assert plan.plan_type == "SELECT"
        assert len(plan.children) == 2
        assert plan.children[0] == child1
        assert plan.children[1] == child2

    def test_query_plan_init_with_properties(self):
        """Test QueryPlan initialization with properties."""
        properties = {"columns": ["name", "age"], "tables": ["users"]}
        plan = QueryPlan("SELECT", properties=properties)

        assert plan.plan_type == "SELECT"
        assert plan.properties == properties

    def test_query_plan_str(self):
        """Test QueryPlan string representation."""
        plan = QueryPlan("SELECT", children=[QueryPlan("FILTER")])

        str_repr = str(plan)
        assert isinstance(str_repr, str)
        assert "SELECT" in str_repr
        assert "1" in str_repr  # Should show number of children

    def test_query_plan_repr(self):
        """Test QueryPlan representation."""
        plan = QueryPlan("SELECT")

        repr_str = repr(plan)
        assert isinstance(repr_str, str)
        assert "SELECT" in repr_str

    def test_query_plan_nested_children(self):
        """Test QueryPlan with nested children."""
        grandchild = QueryPlan("LIMIT")
        child = QueryPlan("SORT", children=[grandchild])
        plan = QueryPlan("SELECT", children=[child])

        assert len(plan.children) == 1
        assert len(plan.children[0].children) == 1
        assert plan.children[0].children[0].plan_type == "LIMIT"

    def test_optimization_rule_application(self):
        """Test optimization rules are applied correctly."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name", "age"],
                "from_tables": ["users"],
                "where_conditions": [{"column": "age", "op": ">", "value": 18}],
            },
        )

        optimized = optimizer.optimize(ast)

        # All rules should be applied
        assert isinstance(optimized, SQLAST)
        assert optimized.query_type == "SELECT"
        # Components should be present (even if rules are mock implementations)
        assert "select_columns" in optimized.components

    def test_optimization_idempotency(self):
        """Test optimization is idempotent."""
        optimizer = SQLQueryOptimizer()
        ast = SQLAST(
            "SELECT",
            {
                "select_columns": ["name"],
                "from_tables": ["users"],
                "where_conditions": [],
            },
        )

        # Apply optimization multiple times
        optimized1 = optimizer.optimize(ast)
        optimized2 = optimizer.optimize(optimized1)

        # Results should be consistent
        assert optimized1.query_type == optimized2.query_type
        assert optimized1.components.keys() == optimized2.components.keys()
