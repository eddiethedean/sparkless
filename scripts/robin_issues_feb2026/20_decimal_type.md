## Summary

PySpark supports DecimalType (e.g. Decimal(10,0)) in schema and expressions. Robin-sparkless may not: ValueError unknown type name: Decimal(10,0).

**Request:** Support Decimal type for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"d": 1.5}], [("d", "Decimal(10,2)")])
df.collect()
```

Error: unknown type name Decimal(10,0) or similar

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1.5,)], ["d"])
spark.stop()
```

PySpark supports Decimal in schema.
