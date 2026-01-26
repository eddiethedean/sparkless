
Enable lazy mode and queue operations until an action is called.

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([{ "x": i } for i in range(5)])

lazy_df = df.withLazy(True).filter(F.col("x") > 1).withColumn("y", F.lit(1))
# No execution yet
rows = lazy_df.collect()  # materializes queued operations
```
