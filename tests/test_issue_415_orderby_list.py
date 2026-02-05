"""
Tests for issue #415: DataFrame.orderBy(list of column names) treats list as single column.

PySpark allows df.orderBy(["col1", "col2"]) - equivalent to orderBy(*["a", "b"]).
Sparkless must unpack a single list argument to multiple columns.
"""

import pytest

from sparkless.sql import SparkSession


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("issue-415").getOrCreate()


def test_orderby_with_list_of_column_names(spark):
    """df.orderBy(["a", "b"]) should order by a then b, matching PySpark."""
    df = spark.createDataFrame(
        [(1, 2, 3), (1, 1, 4), (2, 0, 5)],
        ["a", "b", "c"],
    )
    result = df.orderBy(["a", "b"]).collect()
    assert len(result) == 3
    # Sorted by (a, b): (1,1), (1,2), (2,0)
    assert result[0]["a"] == 1 and result[0]["b"] == 1 and result[0]["c"] == 4
    assert result[1]["a"] == 1 and result[1]["b"] == 2 and result[1]["c"] == 3
    assert result[2]["a"] == 2 and result[2]["b"] == 0 and result[2]["c"] == 5


def test_orderby_with_tuple_of_column_names(spark):
    """df.orderBy(("a", "b")) should also work (tuple unpacking)."""
    df = spark.createDataFrame(
        [(1, 2, 3), (1, 1, 4), (2, 0, 5)],
        ["a", "b", "c"],
    )
    result = df.orderBy(("a", "b")).collect()
    assert len(result) == 3
    assert result[0]["a"] == 1 and result[0]["b"] == 1
    assert result[1]["a"] == 1 and result[1]["b"] == 2
    assert result[2]["a"] == 2 and result[2]["b"] == 0


def test_orderby_with_single_column_list(spark):
    """df.orderBy(["a"]) with single-element list should work."""
    df = spark.createDataFrame(
        [(2,), (1,), (3,)],
        ["a"],
    )
    result = df.orderBy(["a"]).collect()
    assert len(result) == 3
    assert result[0]["a"] == 1
    assert result[1]["a"] == 2
    assert result[2]["a"] == 3


def test_orderby_desc_with_list(spark):
    """df.orderBy(["a", "b"], ascending=False) should work."""
    df = spark.createDataFrame(
        [(1, 2, 3), (1, 1, 4), (2, 0, 5)],
        ["a", "b", "c"],
    )
    result = df.orderBy(["a", "b"], ascending=False).collect()
    assert len(result) == 3
    # Descending: (2,0), (1,2), (1,1)
    assert result[0]["a"] == 2 and result[0]["b"] == 0
    assert result[1]["a"] == 1 and result[1]["b"] == 2
    assert result[2]["a"] == 1 and result[2]["b"] == 1
