from sparkless.sql import SparkSession


def test_issue_280_join_then_groupby_on_join_keys_no_ambiguity():
    """
    Regression for issue #280.

    Sparkless should not raise an ambiguous column error when doing a join
    `on=[...]` followed by a groupBy on those join keys without an intervening
    materialization step.
    """
    spark = SparkSession.builder.appName("issue-280").getOrCreate()
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

        df = df1.join(df2, on=["Name", "Period"], how="left")
        out = df.groupBy(["Name", "Period"]).count().collect()

        got = {(r["Name"], r["Period"]): r["count"] for r in out}
        assert got == {("Alice", "1"): 1, ("Bob", "2"): 1}
    finally:
        spark.stop()
