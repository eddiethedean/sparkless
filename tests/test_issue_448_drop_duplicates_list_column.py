"""Test issue #448: dropDuplicates() with list/array column raises TypeError.

Sparkless previously raised:
  TypeError: unhashable type: 'list'
when calling dropDuplicates() on a DataFrame containing list-typed columns.
The fix uses _make_hashable to convert unhashable values before set membership.

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_448_drop_duplicates_list_column.py -v
  pytest tests/test_issue_448_drop_duplicates_list_column.py -v

https://github.com/eddiethedean/sparkless/issues/448
"""


def test_drop_duplicates_with_list_column_exact_issue_448(spark, spark_backend):
    """Exact scenario from #448 - dropDuplicates with list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Bob", "Value": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}
    for r in rows:
        assert r["Value"] == [1, 2, 3]


def test_distinct_with_list_column(spark, spark_backend):
    """distinct() with list column - same fix path."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [10, 20]},
            {"id": 1, "arr": [10, 20]},
            {"id": 2, "arr": [10, 20]},
        ]
    )
    result = df.distinct()
    rows = result.collect()

    assert len(rows) == 2
    ids = {r["id"] for r in rows}
    assert ids == {1, 2}


def test_drop_duplicates_subset_excludes_list_column(spark, spark_backend):
    """dropDuplicates(subset) when subset excludes the list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [4, 5, 6]},
            {"Name": "Bob", "Value": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates(subset=["Name"])
    rows = result.collect()

    assert len(rows) == 2
    names = [r["Name"] for r in rows]
    assert set(names) == {"Alice", "Bob"}


def test_drop_duplicates_subset_includes_list_column(spark, spark_backend):
    """dropDuplicates(subset) when subset includes the list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [4, 5, 6]},
        ]
    )
    result = df.dropDuplicates(subset=["Name", "Value"])
    rows = result.collect()

    assert len(rows) == 2
    alice_rows = [r for r in rows if r["Name"] == "Alice"]
    assert len(alice_rows) == 2
    values = [tuple(r["Value"]) for r in alice_rows]
    assert [1, 2, 3] in [list(v) for v in values]
    assert [4, 5, 6] in [list(v) for v in values]


def test_drop_duplicates_with_dict_column(spark, spark_backend):
    """dropDuplicates with struct/map-like dict column."""
    df = spark.createDataFrame(
        [
            {"id": 1, "meta": {"a": 1, "b": 2}},
            {"id": 1, "meta": {"a": 1, "b": 2}},
            {"id": 2, "meta": {"a": 1, "b": 2}},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    ids = {r["id"] for r in rows}
    assert ids == {1, 2}


def test_drop_duplicates_empty_list_column(spark, spark_backend):
    """dropDuplicates with empty list values."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": []},
            {"Name": "Alice", "Value": []},
            {"Name": "Bob", "Value": []},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}
    for r in rows:
        assert r["Value"] == []
