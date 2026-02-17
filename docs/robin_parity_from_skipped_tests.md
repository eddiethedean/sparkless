# Robin–Sparkless / PySpark Parity Issues from V4 Skipped Tests

This document summarizes **Robin-sparkless vs PySpark parity gaps** identified when we skipped tests for the v4 Robin-only migration (backend removal). It does not list skips that are purely “removed code” (e.g. Polars materializer); it focuses on behavior that Robin or the crate should eventually support.

---

## 1. Parquet format table append (schema-from-empty / catalog)

**Source:** `tests/parity/dataframe/test_parquet_format_table_append.py`  
**Skip:** Whole class skipped when `SPARKLESS_TEST_BACKEND=robin`  
**Reason:** `ValueError: can not infer schema from empty dataset`

**What the tests do:**

- Create schema with `StructType`/`StructField`, then:
  - **Empty table:** `spark.createDataFrame([], schema)` → `write.format("parquet").mode("overwrite").saveAsTable(fqn)` → `spark.table(fqn)` should return 0 rows with correct schema.
  - **Append to new table:** `createDataFrame(data, schema)` → `write.format("parquet").mode("append").saveAsTable(fqn)` (table does not exist) → `spark.table(fqn)` should see the data.
  - **Append to existing table:** Same with `mode("append")` to an existing parquet table; multiple appends; visibility across sessions.

**Parity issues implied:**

1. **Empty DataFrame + explicit schema**  
   Some code path in the Robin/session flow ends up inferring schema from data. With empty data, that raises “can not infer schema from empty dataset” (raised from `sparkless/session/services/dataframe_factory.py`). So either:
   - `createDataFrame([], schema)` is not passing `schema` through correctly in some Robin path, or
   - A later step (e.g. table creation, `spark.table()`, or writer) produces/uses an empty dataset without an explicit schema.

2. **Parquet format table create/append and catalog**  
   Tests depend on:
   - Creating a table (possibly from an empty DataFrame with schema) with parquet format.
   - Appending to that table and reading back via `spark.table()` with correct schema and row visibility.

**Action for Robin/crate:** Ensure (a) empty DataFrames with explicit `StructType` schema are supported end-to-end, and (b) parquet-format table create/append and catalog `spark.table()` behavior match the above (including schema preservation and multi-append visibility).

---

## 2. Logical-plan semantics (reference for Robin; tests currently skipped)

These tests were skipped because they called the **removed** Polars `plan_interpreter.execute_plan`. They are not skipped due to a known Robin bug; they are good **parity specs** to re-enable once Robin supports the same plan format.

**Source:** `tests/unit/dataframe/test_logical_plan.py`

### 2.1 `test_groupBy_via_plan_interpreter`

- **Data:** `[{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]`
- **Plan:** `groupBy` on `k` with `sum(v)` and `count(v)`.
- **Expected:** 2 rows; `k="a"` → `sum(v)=30`, `count(v)=2`; `k="b"` → `sum(v)=30`, `count(v)=1`.

**Parity:** Robin’s plan execution should support `groupBy` with multiple aggs (e.g. sum, count) and produce these results.

### 2.2 `test_plan_interpreter_cast_between_power`

- **Data:** `[{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]`
- **Plan:**  
  - Filter: `a between 3 and 7` (one row, `a=5`).  
  - withColumn `squared`: `a ** 2`.  
  - withColumn `a_str`: cast `a` to string.
- **Expected:** 1 row; `a=5`, `squared=25`, `a_str="5"`.

**Parity:** Robin should support in logical plan (or equivalent): `between` (inclusive bounds), `**` (power), and `cast` to string.

### 2.3 `test_plan_interpreter_window_row_number`

- **Data:** 3 rows: dept A (salary 10, 20), dept B (salary 30).
- **Plan:** Select `dept`, `salary`, and `row_number()` over `(partition by dept)` with alias `rn`.
- **Expected:** 3 rows; partition A has `rn` 1, 2; partition B has `rn` 1.

**Parity:** Robin should support window `row_number()` with `partition_by` (and optionally `order_by`) and correct partitioning.

---

## 3. Other v4 skips (no new parity finding)

These were skipped only because the implementing code was removed; they do not add new Robin parity items beyond what’s above:

- **Backend / materializer:** `test_backend_capability_model`, `test_robin_optional`, `test_robin_unsupported_raises`, `test_robin_materializer`, all `test_issue_160_*`.
- **Logical plan config/mock:** `test_materialize_from_plan_invoked_when_config_true` (BackendFactory/mock materializer removed).

---

## Summary

| Area                         | Parity issue / gap                                                                 | Source tests                                      |
|-----------------------------|------------------------------------------------------------------------------------|---------------------------------------------------|
| Empty DataFrame + schema    | Schema not preserved or inference triggered on empty data in some Robin path       | Parquet table append (createDataFrame([], schema))|
| Parquet table / catalog     | Create/append parquet table and read via `spark.table()` with correct schema/visibility | Same                                               |
| groupBy + agg               | groupBy + sum/count (and other aggs) in plan                                       | test_groupBy_via_plan_interpreter                 |
| Expressions in plan        | between, **, cast in logical plan                                                  | test_plan_interpreter_cast_between_power          |
| Window row_number           | row_number() over (partition by col)                                              | test_plan_interpreter_window_row_number           |

The only **new** Robin-sparkless/PySpark parity issues clearly exposed by the skipped tests are **(1) schema-from-empty and (2) parquet format table append / catalog**. The logical-plan tests **(3–5)** are reference specs for Robin plan execution; re-enabling them (against Robin’s executor) will validate parity for groupBy+agg, between/power/cast, and window row_number.
