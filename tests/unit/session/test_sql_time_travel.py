"""Tests for Delta Lake Time Travel support in SQL executor."""


class TestTimeTravelBasic:
    """Test basic Time Travel (VERSION AS OF) functionality."""

    def test_version_as_of(self, spark) -> None:
        """Test VERSION AS OF syntax is parsed and query executes."""
        # Create source data
        rows = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("versioned_table")

        # Execute query with VERSION AS OF
        # In mock mode, this returns current data (no real versioning)
        result = spark.sql("""
            SELECT * FROM versioned_table VERSION AS OF 5
        """)

        collected = result.collect()
        # Should return current data (mock behavior)
        assert len(collected) == 3

    def test_version_as_of_with_filter(self, spark) -> None:
        """Test VERSION AS OF with WHERE clause."""
        rows = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 50},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("versioned_filter_table")

        result = spark.sql("""
            SELECT * FROM versioned_filter_table VERSION AS OF 10
            WHERE value > 100
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["value"] == 200


class TestTimeTravelTimestamp:
    """Test TIMESTAMP AS OF functionality."""

    def test_timestamp_as_of_single_quotes(self, spark) -> None:
        """Test TIMESTAMP AS OF with single quotes."""
        rows = [{"id": 1, "name": "test"}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("timestamp_table")

        result = spark.sql("""
            SELECT * FROM timestamp_table TIMESTAMP AS OF '2024-01-01 00:00:00'
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["name"] == "test"

    def test_timestamp_as_of_double_quotes(self, spark) -> None:
        """Test TIMESTAMP AS OF with double quotes."""
        rows = [{"id": 1, "status": "active"}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("timestamp_table2")

        result = spark.sql('''
            SELECT * FROM timestamp_table2 TIMESTAMP AS OF "2024-06-15 12:30:00"
        ''')

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["status"] == "active"


class TestTimeTravelShorthand:
    """Test Databricks shorthand syntax for time travel."""

    def test_at_version_shorthand(self, spark) -> None:
        """Test table@v<version> shorthand syntax."""
        rows = [
            {"id": 1, "amount": 500},
            {"id": 2, "amount": 750},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("shorthand_table")

        # table@v5 syntax
        result = spark.sql("""
            SELECT * FROM shorthand_table@v5
        """)

        collected = result.collect()
        assert len(collected) == 2

    def test_at_timestamp_compact_shorthand(self, spark) -> None:
        """Test table@yyyyMMdd shorthand syntax."""
        rows = [{"id": 1, "type": "A"}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("compact_timestamp_table")

        # table@20240101 syntax (compact timestamp)
        result = spark.sql("""
            SELECT * FROM compact_timestamp_table@20240101
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["type"] == "A"

    def test_at_timestamp_full_shorthand(self, spark) -> None:
        """Test table@yyyyMMddHHmmss shorthand syntax."""
        rows = [{"id": 1, "category": "B"}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("full_timestamp_table")

        # table@20240615123000 syntax (full compact timestamp)
        result = spark.sql("""
            SELECT * FROM full_timestamp_table@20240615123000
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["category"] == "B"


class TestTimeTravelWithOperations:
    """Test time travel combined with other SQL operations."""

    def test_time_travel_with_select_columns(self, spark) -> None:
        """Test time travel with specific column selection."""
        rows = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("columns_table")

        result = spark.sql("""
            SELECT id, name FROM columns_table VERSION AS OF 3
        """)

        collected = result.collect()
        assert len(collected) == 2
        # Should only have id and name columns
        assert "age" not in collected[0].asDict()

    def test_time_travel_with_aggregation(self, spark) -> None:
        """Test time travel with GROUP BY and aggregation."""
        rows = [
            {"category": "A", "amount": 100},
            {"category": "A", "amount": 200},
            {"category": "B", "amount": 150},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("agg_table")

        result = spark.sql("""
            SELECT category, SUM(amount) as total
            FROM agg_table VERSION AS OF 1
            GROUP BY category
        """)

        collected = result.collect()
        assert len(collected) == 2
        totals = {row["category"]: row["total"] for row in collected}
        assert totals["A"] == 300
        assert totals["B"] == 150

    def test_time_travel_with_order_by(self, spark) -> None:
        """Test time travel with ORDER BY."""
        rows = [
            {"id": 3, "score": 75},
            {"id": 1, "score": 90},
            {"id": 2, "score": 85},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("order_table")

        result = spark.sql("""
            SELECT * FROM order_table VERSION AS OF 7
            ORDER BY id
        """)

        collected = result.collect()
        ids = [row["id"] for row in collected]
        assert ids == [1, 2, 3]


class TestTimeTravelParser:
    """Test Time Travel parsing specifically."""

    def test_parser_extracts_version(self, spark) -> None:
        """Test that parser correctly extracts VERSION AS OF info."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = "SELECT * FROM my_table VERSION AS OF 42"
        ast = parser.parse(query)

        # Check that time travel info was extracted
        assert "time_travel" in ast.components
        time_travel = ast.components["time_travel"]
        assert time_travel is not None
        assert time_travel["type"] == "version"
        assert time_travel["value"] == 42
        assert time_travel["table"] == "my_table"

    def test_parser_extracts_timestamp(self, spark) -> None:
        """Test that parser correctly extracts TIMESTAMP AS OF info."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = "SELECT * FROM events TIMESTAMP AS OF '2024-01-15 10:30:00'"
        ast = parser.parse(query)

        time_travel = ast.components["time_travel"]
        assert time_travel is not None
        assert time_travel["type"] == "timestamp"
        assert time_travel["value"] == "2024-01-15 10:30:00"
        assert time_travel["table"] == "events"

    def test_parser_extracts_at_version(self, spark) -> None:
        """Test that parser correctly extracts table@v<n> syntax."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = "SELECT id FROM data@v123"
        ast = parser.parse(query)

        time_travel = ast.components["time_travel"]
        assert time_travel is not None
        assert time_travel["type"] == "version"
        assert time_travel["value"] == 123
        assert time_travel["table"] == "data"

    def test_parser_extracts_at_compact_timestamp(self, spark) -> None:
        """Test that parser correctly extracts table@yyyyMMdd syntax."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = "SELECT * FROM snapshot@20231225"
        ast = parser.parse(query)

        time_travel = ast.components["time_travel"]
        assert time_travel is not None
        assert time_travel["type"] == "timestamp_compact"
        assert time_travel["value"] == "20231225"
        assert time_travel["table"] == "snapshot"

    def test_parser_no_time_travel(self, spark) -> None:
        """Test that normal queries have no time travel info."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = "SELECT * FROM regular_table WHERE id > 5"
        ast = parser.parse(query)

        assert ast.components.get("time_travel") is None

    def test_case_insensitive(self, spark) -> None:
        """Test that time travel keywords are case insensitive."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        # lowercase
        query1 = "select * from tbl version as of 1"
        ast1 = parser.parse(query1)
        assert ast1.components["time_travel"]["type"] == "version"

        # UPPERCASE
        query2 = "SELECT * FROM tbl TIMESTAMP AS OF '2024-01-01'"
        ast2 = parser.parse(query2)
        assert ast2.components["time_travel"]["type"] == "timestamp"

        # MiXeD case
        query3 = "Select * From tbl Version As Of 99"
        ast3 = parser.parse(query3)
        assert ast3.components["time_travel"]["value"] == 99
