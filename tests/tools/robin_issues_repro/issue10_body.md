### Summary
When comparing a datetime (timestamp) column to a date column (e.g. `datetime_col < date_col`), Robin returns 0 rows where PySpark returns the expected row(s). Suggests a difference in date/datetime comparison or type coercion.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, DateType
from datetime import date, datetime

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("ts_col", TimestampType()),
    StructField("date_col", DateType()),
])
# One row: ts_col = 2024-01-14 23:00, date_col = 2024-01-15
df = spark.createDataFrame(
    [(datetime(2024, 1, 14, 23, 0, 0), date(2024, 1, 15))],
    schema,
)
# datetime less than date: 2024-01-14 23:00 < 2024-01-15 -> True
rows = df.filter(df["ts_col"] < df["date_col"]).collect()
print(len(rows), rows)
```

**PySpark output:** `1 [Row(ts_col=..., date_col=...)]` — the row is returned because the timestamp is before the date.

### robin-sparkless (current behavior)
Same pattern returns 0 rows (e.g. `assert 0 == 1` in tests expecting one row).

**Request:** Document or align date/datetime comparison and coercion with PySpark so that expressions like `datetime_col < date_literal` behave consistently.
