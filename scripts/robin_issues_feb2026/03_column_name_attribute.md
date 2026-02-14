## Summary

PySpark's Column object exposes a `.name` attribute for simple column references (e.g. `F.col("x").name` → `"x"`). Robin-sparkless Column does not, causing `AttributeError: 'builtins.Column' object has no attribute 'name'` in code that relies on it for schema/display or compat.

**Request:** Expose a `.name` property on Column when the column is a simple reference (e.g. from `col("x")`), for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
c = rs.col("salary")
name = c.name  # AttributeError: 'builtins.Column' object has no attribute 'name'
```

---

## PySpark (expected)

```python
from pyspark.sql import functions as F

c = F.col("salary")
assert c.name == "salary"
```

PySpark Column has `.name` for simple column reference.
