## Summary

PySpark has df.selectExpr(*exprs) and F.expr(sql_string) to use SQL expression strings. Robin may treat strings as column names: not found Column 'Name like A%' not found. selectExpr and expr() should parse and evaluate the SQL expression.

**Request:** Implement selectExpr(expr1, expr2, ...) and expr(sql_string) that parse and evaluate SQL expressions for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"Name": "Alice"}, {"Name": "Bob"}], [("Name", "string")])
df.selectExpr("Name", "Name as upper", "upper(Name) as u").collect()
```

Error: Column 'upper(Name) as u' not found or similar

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("Alice",), ("Bob",)], ["Name"])
df.selectExpr("Name", "upper(Name) as u").collect()
spark.stop()
```

PySpark selectExpr and expr() evaluate SQL strings.
