# Robin parity repro verification matrix

Repros are run with: `python scripts/robin_parity_repros/<script>.py` from repo root.

| Script | Category | Robin result | PySpark result | Verified Robin issue? |
|--------|----------|--------------|----------------|------------------------|
| 01_join_same_name.py | op_join | OK (on=\["id"\] and on="id") | OK | No – Robin supports both |
| 02_withColumn_expression.py | op_withColumn | OK (cast, when/otherwise) | OK | No – Robin supports these |
| 03_select_alias.py | op_select / col_not_found | OK (alias preserved) | OK | No – Robin preserves alias |
| 04_filter_eqNullSafe.py | op_filter | FAILED (eq_null_safe / eqNullSafe not found) | OK | **Yes – Column.eqNullSafe missing** |
| 05_parity_string_soundex.py | parity_assertion | FAILED (F.soundex not found) | OK | **Yes – soundex() function missing** |
| 06_filter_between.py | op_filter | FAILED (col.between not found) | OK | **Yes – Column.between() missing** |

## Verified Robin parity issues (for GitHub)

1. **Column.eqNullSafe / eq_null_safe** – Filter with null-safe equality; Robin Column has no eqNullSafe/eq_null_safe; PySpark has col.eqNullSafe(other).
2. **soundex()** – String function soundex() missing in Robin; PySpark has F.soundex(col).
3. **Column.between(low, high)** – Filter with between; Robin Column has no between(); PySpark has col.between(low, high).

## Notes

- SparkUnsupportedOperationError for join/withColumn/select in Sparkless often means Sparkless does not translate the expression to Robin (e.g. different-name join keys, or complex expressions). When the same operation is run with pure robin_sparkless, it may succeed (01, 02, 03).
- Failures that remain when using Robin API directly are documented above as verified Robin issues.
