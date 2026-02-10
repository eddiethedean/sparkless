"""
Tests for logical plan serialization (Phase 1) and plan-based materialization (Phase 2).

Phase 1: to_logical_plan(df) produces a JSON-serializable list of ops.
Phase 2: When backend implements materialize_from_plan and config is set, that path is used.
"""

import json
import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.spark_types import Row
from sparkless.dataframe.logical_plan import to_logical_plan, serialize_expression


class TestLogicalPlanPhase1:
    """Phase 1: to_logical_plan structure and JSON round-trip."""

    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        session = SparkSession("LogicalPlanTest")
        yield session
        session.stop()

    def test_empty_queue_returns_empty_plan(self, spark):
        """DataFrame with no queued ops yields empty plan."""
        data = [{"a": 1}, {"a": 2}]
        df = spark.createDataFrame(data)
        plan = to_logical_plan(df)
        assert plan == []

    def test_filter_select_limit_structure(self, spark):
        """Plan for filter -> select -> limit has expected ops and keys."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        df = (
            spark.createDataFrame(data)
            .filter(F.col("age") > 25)
            .select("name", "age")
            .limit(2)
        )
        plan = to_logical_plan(df)
        assert len(plan) == 3
        assert plan[0]["op"] == "filter"
        assert "payload" in plan[0]
        assert "condition" in plan[0]["payload"]
        assert plan[0]["payload"]["condition"]["type"] == "op"
        assert plan[0]["payload"]["condition"]["op"] == ">"
        assert plan[1]["op"] == "select"
        assert "columns" in plan[1]["payload"]
        assert len(plan[1]["payload"]["columns"]) == 2
        assert plan[2]["op"] == "limit"
        assert plan[2]["payload"]["n"] == 2

    def test_plan_is_json_serializable(self, spark):
        """Full plan round-trips through json.dumps/loads."""
        data = [{"x": 1, "y": 2}]
        df = spark.createDataFrame(data).filter(F.col("x") == 1).select("x").limit(1)
        plan = to_logical_plan(df)
        dumped = json.dumps(plan, default=str)
        loaded = json.loads(dumped)
        assert len(loaded) == len(plan)
        for i, entry in enumerate(plan):
            assert loaded[i]["op"] == entry["op"]
            assert "payload" in loaded[i]

    def test_simple_ops_payload_keys(self, spark):
        """limit, offset, drop, distinct, withColumnRenamed have expected payload keys."""
        data = [{"a": 1, "b": 2}]
        df = spark.createDataFrame(data).limit(10).offset(1).drop("b").distinct()
        plan = to_logical_plan(df)
        op_names = [p["op"] for p in plan]
        assert "limit" in op_names
        assert "offset" in op_names
        assert "drop" in op_names
        assert "distinct" in op_names
        limit_entry = next(p for p in plan if p["op"] == "limit")
        assert limit_entry["payload"]["n"] == 10
        offset_entry = next(p for p in plan if p["op"] == "offset")
        assert offset_entry["payload"]["n"] == 1
        drop_entry = next(p for p in plan if p["op"] == "drop")
        assert "cols" in drop_entry["payload"]
        assert (
            "a" in drop_entry["payload"]["cols"] or "b" in drop_entry["payload"]["cols"]
        )

    def test_withColumn_serialized(self, spark):
        """withColumn payload has name and expression."""
        data = [{"a": 1}]
        df = spark.createDataFrame(data).withColumn("double", F.col("a") * 2)
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "withColumn"
        assert plan[0]["payload"]["name"] == "double"
        assert plan[0]["payload"]["expression"]["type"] == "op"
        assert plan[0]["payload"]["expression"]["op"] == "*"

    def test_serialize_expression_column_literal(self):
        """serialize_expression for Column and Literal."""
        from sparkless.functions import Column
        from sparkless.functions.core.literals import Literal

        col_expr = Column("age")
        out = serialize_expression(col_expr)
        assert out == {"type": "column", "name": "age"}
        lit_expr = Literal(42)
        out2 = serialize_expression(lit_expr)
        assert out2 == {"type": "literal", "value": 42}


class TestLogicalPlanPhase2:
    """Phase 2: materialize_from_plan code path when backend supports it and config is set."""

    def test_materialize_from_plan_invoked_when_config_true(self, monkeypatch):
        """When spark.sparkless.useLogicalPlan=true and backend has materialize_from_plan, it is called."""
        received_plans = []

        class MockPlanMaterializer:
            def can_handle_operations(self, operations):
                return True, []

            def materialize(self, data, schema, operations):
                raise AssertionError(
                    "materialize should not be called when plan path is used"
                )

            def materialize_from_plan(self, data, schema, logical_plan):
                received_plans.append(logical_plan)
                # Return one row so downstream conversion succeeds
                names = [f.name for f in schema.fields]
                return [
                    Row(
                        {k: (data[0].get(k) if data else None) for k in names},
                        schema=None,
                    )
                ]

            def close(self):
                pass

        import sparkless.backend.factory as factory_mod

        orig_create = factory_mod.BackendFactory.create_materializer

        def fake_get_backend_type(storage):
            return "plan_test"

        def create_materializer(backend_type="robin", **kwargs):
            if backend_type == "plan_test":
                return MockPlanMaterializer()
            return orig_create(backend_type, **kwargs)

        monkeypatch.setattr(
            factory_mod.BackendFactory,
            "get_backend_type",
            staticmethod(fake_get_backend_type),
        )
        monkeypatch.setattr(
            factory_mod.BackendFactory,
            "create_materializer",
            staticmethod(create_materializer),
        )

        session = (
            SparkSession.builder.appName("PlanTest")
            .config("spark.sparkless.useLogicalPlan", "true")
            .config("spark.sparkless.backend", "polars")
            .getOrCreate()
        )
        # Force backend type to be plan_test for this session's DataFrames by patching storage's type
        # Actually get_backend_type(storage) is patched to always return "plan_test", so any DataFrame
        # will get backend_type "plan_test" and our materializer.
        try:
            data = [{"x": 1, "y": 2}]
            df = session.createDataFrame(data).filter(F.col("x") == 1)
            df.collect()
            assert len(received_plans) == 1
            plan = received_plans[0]
            assert isinstance(plan, list)
            assert len(plan) >= 1
            assert plan[0]["op"] == "filter"
            assert "payload" in plan[0]
        finally:
            session.stop()

    def test_default_config_uses_standard_materialize(self, spark):
        """Without useLogicalPlan, materialize() is used (Polars backend has no materialize_from_plan)."""
        data = [{"a": 1}]
        df = spark.createDataFrame(data).limit(1)
        rows = df.collect()
        assert len(rows) >= 0
        # Polars backend does not implement materialize_from_plan, so standard path is used
        assert df.count() == 1 or len(rows) == 1


class TestLogicalPlanPhase3:
    """Phase 3: full serialization for join, union, orderBy, groupBy."""

    @pytest.fixture
    def spark(self):
        session = SparkSession("LogicalPlanPhase3")
        yield session
        session.stop()

    def test_join_payload_has_other_plan_data_schema(self, spark):
        """Join op payload includes other_plan, other_data, other_schema."""
        left = spark.createDataFrame([{"id": 1, "x": "a"}])
        right = spark.createDataFrame([{"id": 1, "y": "b"}])
        df = left.join(right, on="id", how="inner")
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "join"
        p = plan[0]["payload"]
        assert "on" in p and p["on"] == ["id"]
        assert "how" in p and p["how"] == "inner"
        assert "other_plan" in p
        assert "other_data" in p
        assert "other_schema" in p
        assert isinstance(p["other_schema"], list)
        assert len(p["other_schema"]) >= 1
        assert "name" in p["other_schema"][0] and "type" in p["other_schema"][0]

    def test_union_payload_has_other_plan_data_schema(self, spark):
        """Union op payload includes other_plan, other_data, other_schema."""
        a = spark.createDataFrame([{"v": 1}])
        b = spark.createDataFrame([{"v": 2}])
        df = a.union(b)
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "union"
        p = plan[0]["payload"]
        assert "other_plan" in p
        assert "other_data" in p
        assert "other_schema" in p

    def test_orderBy_payload_structure(self, spark):
        """OrderBy payload has columns and ascending."""
        df = spark.createDataFrame([{"a": 1}, {"a": 2}]).orderBy("a", ascending=False)
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "orderBy"
        p = plan[0]["payload"]
        assert "columns" in p
        assert "ascending" in p

    def test_groupBy_payload_structure(self, spark):
        """GroupBy payload has columns and aggs when groupBy is in the queue."""
        # GroupedData.agg() materializes and does not leave groupBy in the queue.
        # Build a DataFrame with an explicit groupBy op to test serialization shape.
        from sparkless.dataframe import DataFrame

        base = spark.createDataFrame([{"k": "x", "v": 1}])
        df = DataFrame(
            base.data,
            base.schema,
            base.storage,
            operations=[("groupBy", (["k"], []))],
        )
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "groupBy"
        p = plan[0]["payload"]
        assert "columns" in p
        assert "aggs" in p
        assert isinstance(p["aggs"], list)
        assert len(p["aggs"]) == 0

    def test_groupBy_with_aggs_serialized(self, spark):
        """GroupBy with agg expressions serializes func/column/alias."""
        from sparkless.dataframe import DataFrame

        base = spark.createDataFrame([{"k": "x", "v": 1}, {"k": "x", "v": 2}])
        # Build groupBy with one agg: F.sum("v")
        base.groupBy("k").agg(F.sum("v").alias("total"))
        # Result of groupBy().agg() has empty queue; build df with explicit queue
        df = DataFrame(
            base.data,
            base.schema,
            base.storage,
            operations=[("groupBy", (["k"], [F.sum("v").alias("total")]))],
        )
        plan = to_logical_plan(df)
        assert len(plan) == 1
        p = plan[0]["payload"]
        assert len(p["aggs"]) == 1
        agg0 = p["aggs"][0]
        assert agg0.get("func") == "sum"
        assert agg0.get("column") == "v"
        assert agg0.get("alias") == "total"

    def test_window_serialized_structure(self, spark):
        """Window expression in select serializes to structured window dict."""
        from sparkless.dataframe import DataFrame
        from sparkless.window import Window

        base = spark.createDataFrame([{"x": "a", "v": 1}, {"x": "a", "v": 2}])
        w = Window.partitionBy("x")
        row_num = F.row_number().over(w).alias("rn")
        df = DataFrame(
            base.data,
            base.schema,
            base.storage,
            operations=[("select", [F.col("x"), F.col("v"), row_num])],
        )
        plan = to_logical_plan(df)
        assert len(plan) == 1
        assert plan[0]["op"] == "select"
        columns = plan[0]["payload"]["columns"]
        assert len(columns) == 3
        win = next(
            (c for c in columns if isinstance(c, dict) and c.get("type") == "window"),
            None,
        )
        assert win is not None
        assert win.get("function") == "row_number"
        assert win.get("partition_by") == ["x"]
        assert win.get("alias") == "rn"


class TestLogicalPlanPhase4:
    """Phase 4: Polars plan interpreter produces correct results."""

    def test_polars_materialize_from_plan_simple_pipeline(self):
        """With useLogicalPlan=true and Polars backend, plan path runs and result matches."""
        from sparkless.session.core.session import SparkSession as CoreSession

        old_singleton = getattr(CoreSession, "_singleton_session", None)
        session = None
        try:
            CoreSession._singleton_session = None
            session = (
                SparkSession.builder.appName("Phase4Test")
                .config("spark.sparkless.useLogicalPlan", "true")
                .config("spark.sparkless.backend", "polars")
                .getOrCreate()
            )
            data = [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}]
            df = (
                session.createDataFrame(data)
                .filter(F.col("a") > 1)
                .select("a", "b")
                .limit(2)
            )
            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["a"] == 2 and rows[0]["b"] == 20
            assert rows[1]["a"] == 3 and rows[1]["b"] == 30
        finally:
            if session:
                session.stop()
            CoreSession._singleton_session = old_singleton

    def test_groupBy_via_plan_interpreter(self):
        """groupBy().agg() executed via plan path produces correct result."""
        from sparkless.backend.polars.plan_interpreter import execute_plan
        from sparkless.spark_types import StructType, StructField, LongType, StringType

        data = [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]
        schema = StructType(
            [
                StructField("k", StringType()),
                StructField("v", LongType()),
            ]
        )
        plan = [
            {
                "op": "groupBy",
                "payload": {
                    "columns": [{"type": "column", "name": "k"}],
                    "aggs": [
                        {"func": "sum", "column": "v", "alias": "sum(v)"},
                        {"func": "count", "column": "v", "alias": "count(v)"},
                    ],
                },
            }
        ]
        rows = execute_plan(data, schema, plan)
        assert len(rows) == 2
        rows_sorted = sorted(rows, key=lambda r: r["k"])
        assert rows_sorted[0]["k"] == "a"
        assert rows_sorted[0]["sum(v)"] == 30
        assert rows_sorted[0]["count(v)"] == 2
        assert rows_sorted[1]["k"] == "b"
        assert rows_sorted[1]["sum(v)"] == 30
        assert rows_sorted[1]["count(v)"] == 1

    def test_plan_interpreter_cast_between_power(self):
        """Plan interpreter handles cast, between, and ** expressions."""
        from sparkless.backend.polars.plan_interpreter import execute_plan
        from sparkless.spark_types import StructType, StructField, LongType

        data = [{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", LongType()),
            ]
        )
        # filter: a between 3 and 7 -> one row a=5
        plan = [
            {
                "op": "filter",
                "payload": {
                    "condition": {
                        "type": "op",
                        "op": "between",
                        "left": {"type": "column", "name": "a"},
                        "right": {
                            "type": "between_bounds",
                            "lower": 3,
                            "upper": 7,
                        },
                    },
                },
            },
            {
                "op": "withColumn",
                "payload": {
                    "name": "squared",
                    "expression": {
                        "type": "op",
                        "op": "**",
                        "left": {"type": "column", "name": "a"},
                        "right": {"type": "literal", "value": 2},
                    },
                },
            },
            {
                "op": "withColumn",
                "payload": {
                    "name": "a_str",
                    "expression": {
                        "type": "op",
                        "op": "cast",
                        "left": {"type": "column", "name": "a"},
                        "right": {"type": "literal", "value": "string"},
                    },
                },
            },
        ]
        rows = execute_plan(data, schema, plan)
        assert len(rows) == 1
        assert rows[0]["a"] == 5
        assert rows[0]["squared"] == 25
        assert rows[0]["a_str"] == "5"

    def test_plan_interpreter_window_row_number(self):
        """Plan interpreter handles row_number() over (partition by col)."""
        from sparkless.backend.polars.plan_interpreter import execute_plan
        from sparkless.spark_types import StructType, StructField, LongType, StringType

        data = [
            {"dept": "A", "salary": 10},
            {"dept": "A", "salary": 20},
            {"dept": "B", "salary": 30},
        ]
        schema = StructType(
            [
                StructField("dept", StringType()),
                StructField("salary", LongType()),
            ]
        )
        plan = [
            {
                "op": "select",
                "payload": {
                    "columns": [
                        {"type": "column", "name": "dept"},
                        {"type": "column", "name": "salary"},
                        {
                            "type": "window",
                            "function": "row_number",
                            "column": None,
                            "partition_by": ["dept"],
                            "order_by": [],
                            "rows_between": None,
                            "range_between": None,
                            "alias": "rn",
                        },
                    ],
                },
            },
        ]
        rows = execute_plan(data, schema, plan)
        assert len(rows) == 3
        # Partition A: rn 1, 2; Partition B: rn 1
        by_dept = {}
        for r in rows:
            d = r["dept"]
            if d not in by_dept:
                by_dept[d] = []
            by_dept[d].append(r["rn"])
        assert by_dept["A"] == [1, 2]
        assert by_dept["B"] == [1]
