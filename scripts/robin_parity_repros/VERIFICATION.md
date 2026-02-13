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
| 19_spark_sql_table.py | SQL/session | FAIL | PASS | Yes – #284 |
| 20_window_accept_str.py | Window | FAIL | PASS | Yes – #288 |
| 21_na_drop_fill.py | NA | FAIL | PASS | Yes – #289 |
| 22_fillna_subset.py | NA | FAIL | PASS | Yes – #290 |
| 23_create_df_empty_schema.py | createDataFrame | PASS | PASS | No – fixed |
| 24_union_by_name_allow_missing.py | Union | FAIL | PASS | Yes – #292 |
| 25_global_agg.py | Aggregates | FAIL | PASS | Yes – #287 |
| 26_first_ignore_nulls.py | First | UNKNOWN | UNKNOWN | Check – #293 |
| 27_get_active_session_aggregate.py | Session/agg | FAIL | PASS | Yes – #286 |
| 28_approx_count_distinct_rsd.py | approx_count_distinct | FAIL | PASS | **Yes – #297** |
| 29_create_dataframe_tuple_rows.py | tuple rows + schema | PASS | PASS | No – Robin supports |
| 30_approx_percentile.py | approx_percentile | FAIL | PASS | **Yes – #300** |
| 31_any_value_ignore_nulls.py | any_value | FAIL | PASS | **Yes – #301** |
| 32_count_if.py | count_if | FAIL | PASS | **Yes – #302** |
| 33_max_by_min_by.py | max_by/min_by | FAIL | PASS | **Yes – #303** |
| 34_string_agg.py | string_agg | FAIL | FAIL | No – PySpark 3.x has no string_agg (4.0+) |
| 35_try_sum.py | try_sum | FAIL | PASS | **Yes – #304** |
| 36_bit_agg.py | bit_and (global agg) | FAIL | PASS | No – same as #287 (DataFrame.agg) |
| 37_explode_outer.py | explode_outer | FAIL | PASS | **Yes – #305** |
| 38_inline.py | inline / array of structs | FAIL | PASS | **Yes – #306** |
| 39_stack.py | stack | FAIL | FAIL | No – both fail (API/signature diff) |
| 40_encode_decode.py | encode/decode | FAIL | PASS | **Yes – #307** |
| 41_ascii_unbase64.py | ascii/unbase64 | PASS | PASS | No – Robin supports |
| 42_collect_list.py | collect_list | FAIL | PASS | **Yes – #309** |
| 43_collect_set.py | collect_set | FAIL | PASS | **Yes – #310** |
| 44_corr.py | corr | FAIL | PASS | **Yes – #311** |
| 45_covar_pop.py | covar_pop | FAIL | PASS | **Yes – #312** |
| 46_bool_and_every.py | bool_and/every | FAIL | PASS | **Yes – #314** |
| 47_datediff.py | datediff | FAIL | varies | Check (PySpark env-dependent) |
| 48_hour.py | hour | FAIL | PASS | **Yes – #313** |
| 49_last_day.py | last_day | FAIL | PASS | **Yes – #315** |
| 50_instr.py | instr | FAIL | FAIL | No – both fail / PySpark result diff |
| 51_array_remove.py | array_remove | FAIL | PASS | **Yes – #316** |
| 52_flatten.py | flatten | FAIL | PASS | **Yes – #318** |
| 53_element_at.py | element_at | FAIL | PASS | **Yes – #317** |
| 54_lag_lead.py | lag/lead | FAIL | PASS | **Yes – #319** |
| 55_dense_rank.py | dense_rank | FAIL | PASS | **Yes – #320** |
| 56_skewness_kurtosis.py | skewness/kurtosis | FAIL | PASS | **Yes – #321** |
| 57_to_date.py | to_date | FAIL | PASS | **Yes – #322** |

**repro_robin_limitations/** (separate dir): 13 round string returns None → FAIL (#272); 14 to_timestamp string → FAIL (#273); 15 join key coercion → FAIL (#274); 16 create_map empty → FAIL (#275); 17 between string numeric → FAIL (#276); 18 to_timestamp timestamp col → PASS. Last run: 2026-02-13.

## Verified Robin parity issues (current)

1. **posexplode() does not accept column name (string)** – PySpark accepts `F.posexplode("Values")`; Robin raises `TypeError: argument 'col': 'str' object cannot be converted to 'Column'`. Issue #280. Repro: `scripts/robin_parity_repros/11_posexplode_array.py`.
2. **approx_count_distinct(column, rsd=...) missing** – Robin has no `approx_count_distinct` in Python API. Issue #297. Repro: `scripts/robin_parity_repros/28_approx_count_distinct_rsd.py`.
3. **approx_percentile missing** – #300. Repro: 30_approx_percentile.py.
4. **any_value missing** – #301. Repro: 31_any_value_ignore_nulls.py.
5. **count_if missing** – #302. Repro: 32_count_if.py.
6. **max_by / min_by missing** – #303. Repro: 33_max_by_min_by.py.
7. **try_sum / try_avg missing** – #304. Repro: 35_try_sum.py.
8. **explode_outer() wrong behavior** (lengths don't match when NULL/empty) – #305. Repro: 37_explode_outer.py.
9. **inline() and createDataFrame array of structs** – #306. Repro: 38_inline.py.
10. **encode / decode missing** – #307. Repro: 40_encode_decode.py.
11. **collect_list missing** – #309. Repro: 42_collect_list.py.
12. **collect_set missing** – #310. Repro: 43_collect_set.py.
13. **corr missing** – #311. Repro: 44_corr.py.
14. **covar_pop missing** – #312. Repro: 45_covar_pop.py.
15. **hour missing** – #313. Repro: 48_hour.py.
16. **bool_and / every missing** – #314. Repro: 46_bool_and_every.py.
17. **last_day missing** – #315. Repro: 49_last_day.py.
18. **array_remove missing** – #316. Repro: 51_array_remove.py.
19. **element_at missing** – #317. Repro: 53_element_at.py.
20. **flatten / array of arrays missing** – #318. Repro: 52_flatten.py.
21. **lag / lead missing** – #319. Repro: 54_lag_lead.py.
22. **dense_rank missing** – #320. Repro: 55_dense_rank.py.
23. **skewness / kurtosis missing** – #321. Repro: 56_skewness_kurtosis.py.
24. **to_date missing** – #322. Repro: 57_to_date.py.

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

## Quick scan (2026-02-13, optional)

- **regexp_extract_all / select with expressions:** Documented in `docs/robin_github_issue_regexp_extract_all_select.md`; no issue number in robin_sparkless_issues.md. May be covered by #182 (select/withColumn expressions) or need a dedicated parity issue.
- **UDF / date_trunc:** Skip-list categories; not re-run this pass. Existing issues and skip list cover these.
