"""
Tests for issue #419: ParseException when IN literal list is combined with OR.

Sparkless raised ParseException for expressions like:
    df.filter("Value in ('1234') or (Name == 'Alice')")

Root cause: _split_logical_operator was adding parentheses twice (in the paren
branch and again at the end of the loop), causing malformed OR parts.
"""


class TestIssue419FilterInOrParseException:
    """Test filter with IN + OR no longer raises ParseException."""

    def test_filter_in_or_with_integer_literal_exact_issue(self, spark):
        """Exact scenario from issue #419 - IN + OR (using int literal for type match)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in (1234) or (Name == 'Alice')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1234

    def test_filter_in_or_string_literal_no_parse_exception(self, spark):
        """F.expr parses 'Value in ('1234') or (Name == 'Alice')' without ParseException."""
        from sparkless.functions import F

        # This previously raised ParseException - now parses successfully
        expr = F.expr("Value in ('1234') or (Name == 'Alice')")
        assert expr is not None

    def test_filter_in_or_string_literal_type_coercion_exact_issue(self, spark):
        """Exact issue scenario: int column + string literal in IN + OR (type coercion)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234') or (Name == 'Alice')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1234

    def test_filter_in_or_workaround_still_works(self, spark):
        """Workaround from issue (IN only) still works."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_in_or_with_string_column(self, spark):
        """IN + OR with string column (no type coercion needed)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Code": "A"},
                {"Name": "Bob", "Code": "B"},
                {"Name": "Charlie", "Code": "C"},
            ]
        )
        df = df.filter("Code in ('A') or (Name == 'Charlie')")
        rows = df.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}
