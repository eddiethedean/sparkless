from sparkless.sql import SparkSession


def test_join_with_unmaterialized_withcolumn_on_right_regression_281():
    spark = SparkSession.builder.appName("issue-281").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        )

        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value2": "C"},
                {"Name": "Bob", "Period": "2", "Value2": "D"},
            ]
        )

        # Unmaterialized op on right side should not break join materialization.
        df2 = df2.withColumn("ExtraColumn", df2["Value2"])

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (r["Name"], r["Period"], r["Value1"], r["Value2"], r["ExtraColumn"])
            for r in rows
        ] == [
            ("Alice", "1", "A", "C", "C"),
            ("Bob", "2", "B", "D", "D"),
        ]
    finally:
        spark.stop()
