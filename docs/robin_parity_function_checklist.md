# Robin parity function checklist

Source: [Spark 4.1 Built-in Functions](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html). PySpark reference: [Functions — PySpark 4.1.0](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html).

Repros live in `scripts/robin_parity_repros/`. Run from repo root: `python scripts/robin_parity_repros/<script>.py`.

| Category   | Function(s) | Repro script | Robin result | Issue # |
|-----------|-------------|--------------|--------------|---------|
| Aggregate | approx_count_distinct(expr[, relativeSD]) | 28_approx_count_distinct_rsd.py | FAIL (no symbol/rsd) | #297 |
| Aggregate | approx_percentile | 30_approx_percentile.py | FAIL | #300 |
| Aggregate | any_value(expr[, isIgnoreNull]) | 31_any_value_ignore_nulls.py | FAIL | #301 |
| Aggregate | count_if(expr) | 32_count_if.py | FAIL | #302 |
| Aggregate | max_by(x, y) / min_by(x, y) | 33_max_by_min_by.py | FAIL | #303 |
| Aggregate | string_agg / string_agg_distinct | 34_string_agg.py | FAIL | — (PySpark 3.x has no string_agg) |
| Aggregate | try_sum / try_avg | 35_try_sum.py | FAIL | #304 |
| Aggregate | bit_and / bit_or / bit_xor | 36_bit_agg.py | FAIL (no agg) | #287 |
| Generator | posexplode(column name) | 11_posexplode_array.py | FAIL (string not accepted) | #280 |
| Generator | explode_outer | 37_explode_outer.py | FAIL | #305 |
| Generator | inline (array of structs) | 38_inline.py | FAIL | #306 |
| Generator | stack | 39_stack.py | FAIL | — (both fail) |
| String     | encode / decode (charset) | 40_encode_decode.py | FAIL | #307 |
| String     | ascii / unbase64 | 41_ascii_unbase64.py | PASS | — |
| Window/agg | first_value(col, True) / last_value(col, True) | 26_first_ignore_nulls.py | Check | #293 |

### Second batch (repros 42–57, 2026-02-13)

| Category   | Function(s) | Repro script | Robin result | Issue # |
|-----------|-------------|--------------|--------------|---------|
| Aggregate | collect_list(expr) | 42_collect_list.py | FAIL | #309 |
| Aggregate | collect_set(expr) | 43_collect_set.py | FAIL | #310 |
| Aggregate | corr(expr1, expr2) | 44_corr.py | FAIL | #311 |
| Aggregate | covar_pop(expr1, expr2) | 45_covar_pop.py | FAIL | #312 |
| Aggregate | bool_and(expr) / every(expr) | 46_bool_and_every.py | FAIL | #314 |
| Scalar    | datediff | 47_datediff.py | FAIL | Check (PySpark env) |
| Scalar    | hour(col) | 48_hour.py | FAIL | #313 |
| Scalar    | last_day(col) | 49_last_day.py | FAIL | #315 |
| String    | instr(str, substr) | 50_instr.py | FAIL | — (both fail) |
| Array     | array_remove(col, element) | 51_array_remove.py | FAIL | #316 |
| Array     | flatten(col) | 52_flatten.py | FAIL | #318 |
| Array     | element_at(col, index) | 53_element_at.py | FAIL | #317 |
| Window    | lag(col, offset) / lead(col, offset) | 54_lag_lead.py | FAIL | #319 |
| Window    | dense_rank() | 55_dense_rank.py | FAIL | #320 |
| Aggregate | skewness(col) / kurtosis(col) | 56_skewness_kurtosis.py | FAIL | #321 |
| Scalar    | to_date(col [, format]) | 57_to_date.py | FAIL | #322 |
