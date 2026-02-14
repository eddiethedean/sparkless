## Summary

PySpark Column has `.withField(name, value)` for adding/replacing a struct field. Robin-sparkless Column does not: `AttributeError: 'builtins.Column' object has no attribute 'withField'`.

**Request:** Implement Column.withField(name, value) for struct field update (PySpark 3.x parity).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"s": {"a": 1, "b": 2}}], [("s", "struct<a:int,b:int>")])
df.select(rs.col("s").withField("c", rs.lit(3))).collect()
```

Error: 'builtins.Column' object has no attribute 'withField'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.master("local[1]").getOrCreate()
schema = StructType([StructField("s", StructType([StructField("a", IntegerType()), StructField("b", IntegerType())]))])
df = spark.createDataFrame([((1, 2),)], schema)
df.select(F.col("s").withField("c", F.lit(3))).collect()
spark.stop()
```

PySpark returns struct with new field "c" added.
