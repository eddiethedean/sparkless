# [PySpark parity] select() with column names fails: "select payload must be array of column names or {name, expr} objects"

## Summary

When executing `df.select("id", "x").limit(1000).collect()` (column names as positional arguments), the Robin engine rejects the plan with:

```
Robin execute_plan failed: invalid plan: select payload must be array of column names or {name, expr} objects
```

The same code runs successfully in PySpark. This appears to be a plan-format mismatch: the caller (Sparkless) sends a select step with `payload: { "columns": [ {"type": "column", "name": "id"}, {"type": "column", "name": "x"} ] }`; if Robin expects the payload to be the array of column names (or `{name, expr}` objects) at the top level, validation would fail.

## Robin-sparkless reproduction

Execution path: Sparkless v4 builds a logical plan and sends it to the robin-sparkless crate via the PyO3 extension. The following code triggers the error when the plan is executed by Robin.

```python
# Run with: python reproduce_select_parity.py
# (from Sparkless repo root, with Sparkless v4 / Robin extension installed)

from sparkless import SparkSession

spark = SparkSession.builder.appName("select-parity").getOrCreate()
data = [{"id": j, "x": j * 2} for j in range(10_000)]
df = spark.createDataFrame(data)

# Fails with: Robin execute_plan failed: invalid plan: select payload must be
# array of column names or {name, expr} objects
result = df.select("id", "x").limit(1000).collect()

spark.stop()
```

**Observed:** `ValueError: Robin execute_plan failed: invalid plan: select payload must be array of column names or {name, expr} objects`

**Plan shape sent by Sparkless** (for the select step): 
```json
{"op": "select", "payload": {"columns": [{"type": "column", "name": "id"}, {"type": "column", "name": "x"}]}}
```

So the select payload is an object with a `"columns"` key whose value is an array of `{type, name}` objects. If the crate expects the payload to be the array itself (e.g. `["id", "x"]` or `[{"type": "column", "name": "id"}, ...]` at the top level), that would explain the validation error.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("select-parity").getOrCreate()
data = [{"id": j, "x": j * 2} for j in range(10_000)]
df = spark.createDataFrame(data)

# Succeeds
result = df.select("id", "x").limit(1000).collect()
assert len(result) == 1000
assert result[0]["id"] == 0 and result[0]["x"] == 0

spark.stop()
```

**Expected:** Same behavior from Robin: `select("id", "x")` with column names (strings) should be accepted and return the selected columns.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.1 (fixed in **0.11.3**)
- PySpark 3.5.8 (reference)
- Python 3.11

## Suggested fix

Either:

1. **Crate accepts `payload.columns`** – When the select step has `payload: { "columns": [ ... ] }`, treat `payload.columns` as the array of column names or `{name, expr}` objects and accept it; or
2. **Document expected select payload format** – So callers (e.g. Sparkless) can emit the exact shape the crate expects.

## References

- Discovered while running `scripts/benchmark_sparkless_vs_pyspark.py` (simple query step).
- Sparkless logical plan serialization: `sparkless/dataframe/logical_plan.py` builds `{"op": "select", "payload": {"columns": [...]}}`.
