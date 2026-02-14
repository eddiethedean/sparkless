## Summary

Robin-sparkless SQL currently supports only SELECT and CREATE SCHEMA/DATABASE. DROP TABLE is not supported.

**Request:** Support DROP TABLE (and ideally other common DDL) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
spark.sql("DROP TABLE IF EXISTS my_schema.my_table")
```

Error: SQL only SELECT and CREATE SCHEMA/DATABASE supported, got Drop Table.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
spark.sql("DROP TABLE IF EXISTS my_schema.my_table")
spark.stop()
```

PySpark executes DROP TABLE.
