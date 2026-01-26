# Memory Management


Track and clear session memory for large test suites.

```python
from sparkless.sql import SparkSession

spark = SparkSession()

# after creating multiple DataFrames
print(spark._get_memory_usage())

spark.clear_cache()  # frees memory tracked by the session
print(spark._get_memory_usage())  # 0
```
