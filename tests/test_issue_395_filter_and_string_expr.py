"""
Tests for issue #395: filter with AND and string literals - operator precedence.

"status == 'Y' and Name is not null" was parsed incorrectly (AND joined to 'Y')
because IS NULL was matched before AND split. Fix: parse AND/OR before IS NULL,
and use "==" for equality split so "a == 'Y'" splits correctly.
"""


class TestIssue395FilterAndStringExpr:
    """Test filter with AND and string literals (issue #395)."""

    def test_filter_and_string_equality_exact_issue(self, spark):
        """Exact scenario from issue #395."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("status == 'Y' and Name is not null")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_and_string_equality_with_show(self, spark):
        """filter + show() as in issue example."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("status == 'Y' and Name is not null")
        df.show()
        rows = df.collect()
        assert len(rows) == 1

    def test_filter_and_is_null_workaround_still_works(self, spark):
        """Parentheses workaround still works."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("(status == 'Y') and (Name is not null)")
        rows = df.collect()
        assert len(rows) == 1

    def test_filter_or_string_equality(self, spark):
        """OR with string equality."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": "Bob", "Status": "N"},
                {"Name": "Charlie", "Status": "Y"},
            ]
        )
        df = df.filter("Status == 'Y' or Status == 'N'")
        rows = df.collect()
        assert len(rows) == 3

    def test_filter_and_string_and_is_null(self, spark):
        """Multiple AND with string and is null."""
        df = spark.createDataFrame(
            [
                {"A": "x", "B": "y", "C": 1},
                {"A": "x", "B": None, "C": 2},
                {"A": "x", "B": "y", "C": None},
            ]
        )
        df = df.filter("A == 'x' and B is not null and C is not null")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["C"] == 1
