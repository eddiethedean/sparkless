# Robin parity repro verification matrix

Repros are run with: `python scripts/robin_parity_repros/<script>.py` from repo root.

**Robin version:** robin-sparkless>=0.8.0 (per pyproject.toml). Last run: 2026-02-12.

| Script | Category | Robin result | PySpark result | Verified Robin issue? |
|--------|----------|--------------|----------------|------------------------|
| 01_join_same_name.py | op_join | OK (on=\["id"\] and on="id") | OK | No – Robin supports both |
| 02_withColumn_expression.py | op_withColumn | OK (cast, when/otherwise) | OK | No – Robin supports these |
| 03_select_alias.py | op_select / col_not_found | OK (alias preserved) | OK | No – Robin preserves alias |
| 04_filter_eqNullSafe.py | op_filter | OK (Robin 0.8+) | OK | No – fixed in 0.8 (issue #248) |
| 05_parity_string_soundex.py | parity_assertion | OK (Robin 0.8+) | OK | No – fixed in 0.8 (issue #249) |
| 06_filter_between.py | op_filter | OK (Robin 0.8+) | OK | No – fixed in 0.8 (issue #250) |
| 07_split_limit.py | other (split limit) | FAILED (split 3-arg not supported) | OK | **Yes – F.split(col, pattern, limit) missing** |

## Verified Robin parity issues (current)

1. **F.split(column, pattern, limit)** – PySpark supports `F.split(col, pattern, limit)`; Robin’s `split` takes 2 args only (TypeError: py_split() takes 2 positional arguments but 3 were given). Issue: [#254](https://github.com/eddiethedean/robin-sparkless/issues/254).

## Previously verified (fixed in Robin 0.8+)

- **Column.eqNullSafe / eq_null_safe** – issue #248; Robin 0.8 has it.
- **soundex()** – issue #249; Robin 0.8 has F.soundex.
- **Column.between(low, high)** – issue #250; Robin 0.8 has it.

## Notes

- SparkUnsupportedOperationError for join/withColumn/select in Sparkless often means Sparkless does not translate the expression to Robin. When the same operation is run with pure robin_sparkless, it may succeed (01, 02, 03).
- Failures that remain when using Robin API directly are documented above as verified Robin issues.
