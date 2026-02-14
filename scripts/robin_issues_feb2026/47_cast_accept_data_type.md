## Summary

PySpark Column.cast accepts DataType instance or string. Robin expects only string.

**Request:** Accept DataType instances in cast for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
from pyspark.sql.types import IntegerType
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": "1"}], [("x", "string")])
df.select(rs.col("x").cast(IntegerType())).collect()
```

Error: IntegerType object cannot be converted to PyString

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("1",)], ["x"])
df.select(F.col("x").cast(IntegerType())).collect()
spark.stop()
```

PySpark cast accepts DataType or string.
