# Robin parity repro verification matrix

Repros are run with: `python scripts/robin_parity_repros/<script>.py` from repo root.

**Robin version:** robin-sparkless>=0.8.3 (per pyproject.toml). Last run: 2026-02-12.

| Script | Category | Robin result | PySpark result | Verified Robin issue? |
|--------|----------|--------------|----------------|------------------------|
| 01_join_same_name.py | op_join | OK | OK | No – Robin supports both |
| 02_withColumn_expression.py | op_withColumn | OK | OK | No – Robin supports these |
| 03_select_alias.py | op_select / col_not_found | OK | OK | No – Robin preserves alias |
| 04_filter_eqNullSafe.py | op_filter | OK | OK | No – fixed in 0.8 (issue #248) |
| 05_parity_string_soundex.py | parity_assertion | OK | OK | No – fixed in 0.8 (issue #249) |
| 06_filter_between.py | op_filter | OK | OK | No – fixed in 0.8 (issue #250) |
| 07_split_limit.py | other (split limit) | OK (Robin 0.8.3) | OK | No – fixed in 0.8.3 |
| 08_create_dataframe_array_column.py | other (array schema) | OK (Robin 0.8.3) | OK | No – fixed in 0.8.3 |
| 09_round_string_column.py | other (round string) | FAILED | OK | **Yes – F.round() on string column** |
| 10_array_empty.py | other (array empty) | FAILED | OK | **Yes – F.array() with no args** |
| 11_posexplode_array.py | other (posexplode) | FAILED | OK | **Yes – posexplode() missing** |
| 12_null_casting.py | other (null cast) | OK | OK | No – Robin supports null casting |
| 13_date_string_comparison.py | other (date/string) | FAILED | OK | **Yes – date/datetime vs string comparison** |
| 14_eqnullsafe_type_coercion.py | other (type coercion) | FAILED | OK | **Yes – eqNullSafe type coercion** |

## Verified Robin parity issues (current)

1. **F.round() on string column** – PySpark implicitly casts string to numeric; Robin raises "round can only be used on numeric types". Repro: `scripts/robin_parity_repros/09_round_string_column.py`.
2. **F.array() with no args** – PySpark returns empty array column; Robin raises "array requires at least one column". Repro: `scripts/robin_parity_repros/10_array_empty.py`.
3. **posexplode() missing** – PySpark has F.posexplode(); Robin module has no posexplode attribute. Repro: `scripts/robin_parity_repros/11_posexplode_array.py`.

4. **date/datetime vs string comparison** – PySpark implicitly casts; Robin raises "cannot compare 'date/datetime/time' to a string value". Repro: `scripts/robin_parity_repros/13_date_string_comparison.py`.
5. **eqNullSafe type coercion** – PySpark coerces string vs numeric in eqNullSafe; Robin raises "cannot compare string with numeric type (i32)". Repro: `scripts/robin_parity_repros/14_eqnullsafe_type_coercion.py`.

## Previously verified (fixed in Robin 0.8+)

- **Column.eqNullSafe / eq_null_safe** – issue #248; Robin 0.8 has it.
- **soundex()** – issue #249; Robin 0.8 has F.soundex.
- **Column.between(low, high)** – issue #250; Robin 0.8 has it.

## Notes

- SparkUnsupportedOperationError for join/withColumn/select in Sparkless often means Sparkless does not translate the expression to Robin. When the same operation is run with pure robin_sparkless, it may succeed (01, 02, 03).
- Failures that remain when using Robin API directly are documented above as verified Robin issues.
