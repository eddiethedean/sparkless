# Repro results with robin-sparkless 0.7.0

| Script | Robin | PySpark | Limitation confirmed | Notes |
|--------|-------|---------|----------------------|-------|
| 01_type_strictness | OK | OK | No | **Fixed in 0.7.0** — string vs numeric coercion now works |
| 02_case_sensitivity | OK | OK | No | |
| 03_row_values_nested | OK | OK | No | |
| 04_case_when | OK | OK | No | **Fixed in 0.7.0** — when().otherwise() now works |
| 05_window_in_select | OK | OK | No | **Fixed in 0.7.0** — Robin uses `partitionBy`/`orderBy` (camelCase like PySpark); repro script updated to use partitionBy/orderBy |
| 06_map_array | OK | OK | No | |
| 07_filter_complex | OK | OK | No | Column-column comparison works |
| 08_concat_literal | OK | OK | No | **Fixed in 0.7.0** — concat with lit works |
| 09_chained_arithmetic | OK | OK | No | |
| 10_datetime_row | OK | OK | No | **Fixed in 0.7.0** — datetime in row now accepted |

## Summary

With robin-sparkless 0.7.0, **all 10 original repros pass**. The 6 limitations confirmed in 0.6.0 (type strictness, CaseWhen, concat, datetime in row, Window API, plus map/array/filter) are now fixed or were due to repro script using snake_case (`partition_by`) instead of Robin's PySpark-compatible camelCase (`partitionBy`).

## New repros (2026-02-11)

| Script | Robin | PySpark | Limitation confirmed | Notes |
|--------|-------|---------|----------------------|-------|
| 11_order_by_nulls | FAILED | OK | Yes | Robin lacks `Column.desc_nulls_last()` → robin-sparkless #245 |
| 12_isin_empty_list | FAILED | OK | Yes | Robin lacks `Column.isin()` → robin-sparkless #244 |
