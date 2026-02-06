"""Test issue #430: F.posexplode() + alias() must return exploded rows (not None).

Related to #366 and #424. posexplode().alias("Value1", "Value2") must produce
1 row per array element with correct pos and value columns, not 1 row per input
with None in exploded columns.

https://github.com/eddiethedean/sparkless/issues/430
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_posexplode_alias_two_names_returns_exploded_rows(spark, spark_backend):
    """posexplode().alias('Value1', 'Value2') must return 4 rows with correct values (#430)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Values": [10, 20]},
            {"Name": "Bob", "Values": [30, 40]},
        ]
    )
    result = df.select("Name", F_backend.posexplode("Values").alias("Value1", "Value2"))
    rows = result.collect()

    assert "Value1" in result.columns
    assert "Value2" in result.columns
    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"

    # Group by name and check exploded values
    by_name = {}
    for r in rows:
        n = r["Name"]
        if n not in by_name:
            by_name[n] = []
        by_name[n].append((r["Value1"], r["Value2"]))

    assert by_name["Alice"] == [(0, 10), (1, 20)]
    assert by_name["Bob"] == [(0, 30), (1, 40)]


def test_posexplode_alias_no_none_values(spark, spark_backend):
    """posexplode().alias() must not return None in exploded columns (#430)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"x": [1, 2, 3], "y": "ok"},
        ]
    )
    result = df.select("y", F_backend.posexplode("x").alias("pos", "val"))
    rows = result.collect()

    assert len(rows) == 3
    for r in rows:
        assert r["pos"] is not None, f"pos must not be None: {r}"
        assert r["val"] is not None, f"val must not be None: {r}"
    assert [(r["pos"], r["val"]) for r in rows] == [(0, 1), (1, 2), (2, 3)]
