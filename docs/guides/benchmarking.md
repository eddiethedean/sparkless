# Benchmarking


Use the session benchmarking API to time operations and inspect results.

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession()

df = spark.createDataFrame([{ "x": i } for i in range(100)])

result_df = spark._benchmark_operation(
    "filter_gt_50",
    lambda: df.filter(F.col("x") > 50)
)

stats = spark._get_benchmark_results()["filter_gt_50"]
print(stats["duration_s"], stats["memory_used_bytes"], stats["result_size"])  # e.g. 0.001 0 49
```
