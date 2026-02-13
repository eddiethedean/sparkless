# Robin parity repro verification matrix

Repros are run with: `python scripts/robin_parity_repros/<script>.py` from repo root.

**Robin version:** robin-sparkless>=0.8.3 (per pyproject.toml). Last run: 2026-02-13.

| Script | Category | Robin result | PySpark result | Verified Robin issue? |
|--------|----------|--------------|----------------|------------------------|
| 01_join_same_name.py | op_join | OK | OK | No – Robin supports both |
| 02_withColumn_expression.py | op_withColumn | OK | OK | No – Robin supports these |
| 03_select_alias.py | op_select / col_not_found | OK | OK | No – Robin preserves alias |
| 04_filter_eqNullSafe.py | op_filter | OK | OK | No – fixed in 0.8 (issue #248) |
| 05_parity_string_soundex.py | parity_assertion | OK | OK | No – fixed in 0.8 (issue #249) |
| 06_filter_between.py | op_filter | OK | OK | No – fixed in 0.8 (issue #250) |
| 07_split_limit.py | other (split limit) | OK | OK | No – fixed in 0.8.3 |
| 08_create_dataframe_array_column.py | other (array schema) | OK | OK | No – fixed in 0.8.3 |
| 09_round_string_column.py | other (round string) | OK | OK | No – fixed (current Robin) |
| 10_array_empty.py | other (array empty) | OK | OK | No – fixed (current Robin) |
| 11_posexplode_array.py | other (posexplode) | FAILED | OK | **Yes – #280 posexplode() accept string** |
| 12_null_casting.py | other (null cast) | OK | OK | No – Robin supports null casting |
| 13_date_string_comparison.py | other (date/string) | OK | OK | No – fixed (current Robin) |
| 14_eqnullsafe_type_coercion.py | other (type coercion) | OK | OK | No – fixed (current Robin) |

## Verified Robin parity issues (current)

1. **posexplode() does not accept column name (string)** – PySpark accepts `F.posexplode("Values")`; Robin raises `TypeError: argument 'col': 'str' object cannot be converted to 'Column'`. Issue #280. Repro: `scripts/robin_parity_repros/11_posexplode_array.py`.

## Previously verified (filed; may be fixed in current Robin)

- **F.round() on string column** – #262; 09_round_string_column.py now OK on current Robin.
- **F.array() with no args** – #263; 10_array_empty.py now OK on current Robin.
- **posexplode() missing** – #264; 11 now fails with different error (API: accept string) → #280.
- **date/datetime vs string comparison** – #265; 13_date_string_comparison.py now OK on current Robin.
- **eqNullSafe type coercion** – #266; 14_eqnullsafe_type_coercion.py now OK on current Robin.

## Previously verified (fixed in Robin 0.8+)

- **Column.eqNullSafe / eq_null_safe** – issue #248; Robin 0.8 has it.
- **soundex()** – issue #249; Robin 0.8 has F.soundex.
- **Column.between(low, high)** – issue #250; Robin 0.8 has it.

## Notes

- SparkUnsupportedOperationError for join/withColumn/select in Sparkless often means Sparkless does not translate the expression to Robin. When the same operation is run with pure robin_sparkless, it may succeed (01, 02, 03).
- Failures that remain when using Robin API directly are documented above as verified Robin issues.
