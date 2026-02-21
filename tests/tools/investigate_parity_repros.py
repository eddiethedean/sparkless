#!/usr/bin/env python3
"""
Run minimal repros for documented robin-sparkless/PySpark parity issues.
Reports which still fail with current Sparkless + robin-sparkless (0.15.0).
Set SPARKLESS_TEST_BACKEND=robin (or rely on default for Robin-only install).
"""
from __future__ import annotations

import os
import sys

# Force Robin backend
os.environ["SPARKLESS_TEST_BACKEND"] = "robin"

results: list[tuple[str, str, bool, str]] = []  # (name, short_desc, passed, error_or_ok)


def run(name: str, short_desc: str, fn):
    """Run a repro; record pass/fail."""
    try:
        fn()
        results.append((name, short_desc, True, "OK"))
        return True
    except Exception as e:
        results.append((name, short_desc, False, str(e)))
        return False


def main():
    from sparkless import SparkSession
    from sparkless.sql import functions as F
    from sparkless.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        MapType,
        ArrayType,
    )

    spark = SparkSession.builder.appName("parity_repro").getOrCreate()

    # 1. Map type in createDataFrame
    def repro_map():
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("m", MapType(StringType(), StringType())),
        ])
        data = [{"id": 1, "m": {"a": "x"}}, {"id": 2, "m": {"b": "y"}}]
        spark.createDataFrame(data, schema).collect()

    run("map_type", "create_dataframe_from_rows: unsupported type map<string,string>", repro_map)

    # 2. Array type in createDataFrame
    def repro_array():
        schema = StructType([
            StructField("id", StringType()),
            StructField("arr", ArrayType(IntegerType())),
        ])
        data = [{"id": "x", "arr": [1, 2, 3]}, {"id": "y", "arr": [4, 5]}]
        spark.createDataFrame(data, schema).collect()

    run("array_type", "create_dataframe_from_rows: array column value must be null or array", repro_array)

    # 3. Struct in row (tuple)
    def repro_struct():
        schema = StructType([
            StructField("id", StringType()),
            StructField("nested", StructType([
                StructField("a", IntegerType()),
                StructField("b", StringType()),
            ])),
        ])
        data = [{"id": "x", "nested": (1, "y")}]
        spark.createDataFrame(data, schema).collect()

    run("struct_tuple", "create_dataframe_from_rows: struct value must be object or array", repro_struct)

    # 4. Temp view: createOrReplaceTempView then table()
    def repro_temp_view():
        df = spark.createDataFrame([{"id": 1, "name": "a"}], ["id", "name"])
        df.createOrReplaceTempView("my_view")
        spark.table("my_view").collect()

    run("temp_view", "Table or view not found after createOrReplaceTempView", repro_temp_view)

    # 5. String column vs numeric (between)
    def repro_string_numeric():
        df = spark.createDataFrame([{"id": 1, "val": "5"}, {"id": 2, "val": "15"}])
        df.select(F.col("val").between(1, 10)).collect()

    run("string_numeric", "collect failed: cannot compare string with numeric type", repro_string_numeric)

    # 6. select with column expression (e.g. create_map)
    def repro_select_expression():
        df = spark.createDataFrame([{"a": "x", "b": "y"}])
        df.select(F.create_map(F.lit("k"), F.col("a")).alias("m")).collect()

    run("select_expression", "select expects Column or str", repro_select_expression)

    # 7. unionByName with different column types
    def repro_union_coercion():
        df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
        df2 = spark.createDataFrame([("2", "b")], ["id", "name"])
        df1.unionByName(df2).collect()

    run("union_coercion", "type String is incompatible with expected type Int64", repro_union_coercion)

    # 8. Join with different case column names (ID vs id)
    def repro_join_case():
        df1 = spark.createDataFrame([(1, "x")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "y")], ["ID", "other"])
        j = df1.join(df2, df1["id"] == df2["ID"])
        j.collect()

    run("join_case", "collect failed: not found: ID", repro_join_case)

    # Print summary
    failed = [(n, d, e) for n, d, p, e in results if not p]
    passed = [(n, d) for n, d, p, _ in results if p]

    print("=== Parity repro results ===\n")
    for name, short_desc, passed_ok, msg in results:
        status = "PASS" if passed_ok else "FAIL"
        print(f"  [{status}] {name}: {short_desc}")
        if not passed_ok:
            print(f"           -> {msg[:80]}{'...' if len(msg) > 80 else ''}\n")

    print(f"\nPassed: {len(passed)}, Failed: {len(failed)}")
    if failed:
        print("\nStill failing (candidate for GitHub issues):")
        for n, d, e in failed:
            print(f"  - {n}: {d}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
