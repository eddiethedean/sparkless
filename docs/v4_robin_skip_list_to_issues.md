# v4 Skip list → Robin-sparkless issues

This document maps each **skip category** (from [tests/unit/v4_robin_skip_list.txt](../tests/unit/v4_robin_skip_list.txt)) to the corresponding **PySpark behavior**, **Robin gap**, and either an **existing** robin-sparkless issue or a **to-file** issue title. Used to drive new upstream issues so Sparkless tests can be un-skipped as Robin reaches parity.

See [robin_sparkless_issues.md](robin_sparkless_issues.md) for policy and how to file.

---

## SQL / session

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Tests call `spark.sql(query)` or `spark.table(name)`; backend raises NotImplementedError or AttributeError | `SparkSession.sql(query)` returns DataFrame; `SparkSession.table(name)` returns DataFrame for a registered table name | Robin does not expose `sql()` or `table()` on the session | **To file:** [Parity] SparkSession.sql() and SparkSession.table() for PySpark compatibility |
| Tests call `df.createOrReplaceTempView("x")` then `spark.table("x")` | DataFrame can register as temp view; session can resolve it in `sql()`/`table()` | No `create_or_replace_temp_view` (or equivalent) and no resolution in session | **To file:** [Parity] DataFrame.createOrReplaceTempView() and temp view resolution in sql()/table() |

---

## Aggregates / session registry

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Parity aggregation tests fail with "No active SparkSession found" when calling F.sum/avg/count | Aggregate functions work when a session was used to create the DataFrame (session is "active" or reachable) | Robin's Python aggregate path may require a registered/getActiveSession-style session that is not set when using Sparkless wrapper | **To file:** [Parity] getActiveSession or session registry so aggregate functions (sum/avg/count) work |
| Tests call `df.agg(F.sum("x"))` (global aggregation, no groupBy) | `DataFrame.agg(*exprs)` returns a single-row DataFrame | Robin DataFrame may not have `agg()` for global aggregation | **To file:** [Parity] DataFrame.agg(*exprs) for global aggregation (no groupBy) |

---

## Window

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Tests use `Window.partitionBy("dept").orderBy("salary")` (string column names); Robin raises "descriptor orderBy for Window doesn't apply to a 'str' object" | `Window.partitionBy("col1", "col2")` and `Window.orderBy("col")` accept column names (str) | Robin Window expects Column in partitionBy/orderBy, not str | **To file:** [Parity] Window.partitionBy() / orderBy() accept column names (str) not only Column |
| Window arithmetic / select with window expressions | select() accepts Column expressions from F.percent_rank().over(window) etc. | #187 (Window API) resolved in 0.4; str vs Column in Window may still differ. Select already accepts Column (see #182) | Existing: [#187](https://github.com/eddiethedean/robin-sparkless/issues/187). If str not supported in Window, file the issue above. |

---

## NA / null handling

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Tests call `df.na.drop(subset=["col"])`, `df.na.fill(0, subset=["col"])` | `DataFrame.na` exposes `.drop(subset=..., how=..., thresh=...)` and `.fill(value, subset=...)` | Robin may not expose `na` or may have different signature (e.g. no subset) | **To file:** [Parity] DataFrame.na.drop() and na.fill() with subset, how, thresh |
| Tests call `df.fillna(0, subset=["col"])` | `DataFrame.fillna(value, subset=[...])` | Robin fillna() may not accept `subset` | **To file:** [Parity] DataFrame.fillna(value, subset=[...]) (or fold into na.fill issue) |

---

## createDataFrame / create_dataframe_from_rows

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Empty data with schema, or empty schema | PySpark allows `createDataFrame([], schema)` or empty schema for empty DataFrame | Robin raises e.g. "schema must not be empty" or rejects empty data | **To file:** [Parity] create_dataframe_from_rows: allow empty data with schema or empty schema |
| Tuple/list rows with schema (positional) | createDataFrame(list_of_tuples, schema) | — | **Checked:** Robin accepts `create_dataframe_from_rows([("a",1),("b",2)], [("name","string"),("value","int")])`. Repro: `scripts/robin_parity_repros/29_create_dataframe_tuple_rows.py`. #256 is array column type only; tuple rows are supported. No new issue. |

---

## Union / join

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| Tests call `unionByName(other, allowMissingColumns=True)` | `DataFrame.unionByName(other, allowMissingColumns=True)` | Robin union_by_name may not accept allow_missing_columns | **To file:** [Parity] union_by_name(allow_missing_columns=True) |

---

## First / approx_count_distinct

| Why skipped | PySpark behavior | Robin gap | Issue |
|-------------|------------------|-----------|--------|
| test_first_ignorenulls, test_first_method | F.first(col, ignorenulls=True) or GroupedData.first() | first() / first_ignore_nulls may be missing or different | **To file:** [Parity] first() / first_ignore_nulls() aggregate (if missing) |
| test_approx_count_distinct_rsd | F.approx_count_distinct(column, rsd=...) | Robin has no approx_count_distinct in Python API | **Filed:** [#297](https://github.com/eddiethedean/robin-sparkless/issues/297). Repro: `scripts/robin_parity_repros/28_approx_count_distinct_rsd.py`. |

---

## Function checklist (creative parity hunting, 2026-02-13)

Gaps found via Spark built-in function list and repros 30–41 (not from skip list). See [robin_parity_function_checklist.md](robin_parity_function_checklist.md) and [robin_sparkless_issues.md](robin_sparkless_issues.md) (section "Issues from creative parity hunting").

| Function / area | Filed issue |
|-----------------|-------------|
| approx_percentile | [#300](https://github.com/eddiethedean/robin-sparkless/issues/300) |
| any_value | [#301](https://github.com/eddiethedean/robin-sparkless/issues/301) |
| count_if | [#302](https://github.com/eddiethedean/robin-sparkless/issues/302) |
| max_by / min_by | [#303](https://github.com/eddiethedean/robin-sparkless/issues/303) |
| try_sum / try_avg | [#304](https://github.com/eddiethedean/robin-sparkless/issues/304) |
| explode_outer (wrong behavior) | [#305](https://github.com/eddiethedean/robin-sparkless/issues/305) |
| inline / array of structs | [#306](https://github.com/eddiethedean/robin-sparkless/issues/306) |
| encode / decode | [#307](https://github.com/eddiethedean/robin-sparkless/issues/307) |

Repros: `scripts/robin_parity_repros/30_approx_percentile.py` through `41_ascii_unbase64.py`.

### Second batch (repros 42–57, 2026-02-13)

| Function / area | Filed issue |
|-----------------|-------------|
| collect_list | [#309](https://github.com/eddiethedean/robin-sparkless/issues/309) |
| collect_set | [#310](https://github.com/eddiethedean/robin-sparkless/issues/310) |
| corr | [#311](https://github.com/eddiethedean/robin-sparkless/issues/311) |
| covar_pop | [#312](https://github.com/eddiethedean/robin-sparkless/issues/312) |
| hour | [#313](https://github.com/eddiethedean/robin-sparkless/issues/313) |
| bool_and / every | [#314](https://github.com/eddiethedean/robin-sparkless/issues/314) |
| last_day | [#315](https://github.com/eddiethedean/robin-sparkless/issues/315) |
| array_remove | [#316](https://github.com/eddiethedean/robin-sparkless/issues/316) |
| element_at | [#317](https://github.com/eddiethedean/robin-sparkless/issues/317) |
| flatten / array of arrays | [#318](https://github.com/eddiethedean/robin-sparkless/issues/318) |
| lag / lead | [#319](https://github.com/eddiethedean/robin-sparkless/issues/319) |
| dense_rank | [#320](https://github.com/eddiethedean/robin-sparkless/issues/320) |
| skewness / kurtosis | [#321](https://github.com/eddiethedean/robin-sparkless/issues/321) |
| to_date | [#322](https://github.com/eddiethedean/robin-sparkless/issues/322) |

Repros: `scripts/robin_parity_repros/42_collect_list.py` through `57_to_date.py`.

---

## Expression / type parity (existing issues)

These skip reasons are already covered by filed robin-sparkless issues; no new issue needed. Reference only.

| Skip category | Existing issues |
|---------------|------------------|
| cast/astype, CaseWhen, substring, select/withColumn expression evaluation | #182, #200, #195 |
| Type strictness, string vs numeric, coercion | #201, #235, #265, #266 |
| map(), array(), struct(), nested row values | #198, #256, #263, #275 |
| Filter (Column–Column, bool, complex) | #184, #185, #202 |
| Case sensitivity | #194 |
| concat/concat_ws, split limit, lit types | #196, #254, #186 |
| getItem / isin / column resolution | #244, #195, #225–231 tests |
| order_by SortOrder / desc_nulls_last | #257, #245 |
| round/to_timestamp/create_map/between/join coercion | #272, #273, #274, #275, #276 |

---

## No new Robin issue (Sparkless or out of scope)

| Why skipped | Note |
|-------------|------|
| select() items must be str or Column (Sparkless ColumnOperation/WindowFunction) | Sparkless should use Robin F/Window so expressions are Robin Column; Robin already documents str or Column. No Robin change. |
| group_by(str) vs list | Sparkless compat layer passes list; Robin accepts. No new issue unless Robin fails with str in some path. |
| Polars / plan_interpreter / v3 config | Removed in v4; skip intentional. |
| Parity suite (exact schema/result comparison) | Sparkless can relax assertions or skip; no Robin issue unless behavior is wrong vs PySpark. |

---

## Skips addressed by Sparkless (not Robin)

These items were in the v4 skip list but are Sparkless responsibility, obsolete, or by-design. Actions taken:

| Action | Detail |
|--------|--------|
| **Dead reference removed** | `tests/unit/dataframe/test_logical_plan.py::TestLogicalPlanPhase2::...` — file lives only under `tests/archive/`; pattern removed from [tests/unit/v4_robin_skip_list.txt](../tests/unit/v4_robin_skip_list.txt). |
| **Archived (Polars/v3)** | [tests/test_issue_160_lazy_polars_expr.py](../tests/test_issue_160_lazy_polars_expr.py) moved to [tests/archive/test_issue_160_lazy_polars_expr.py](../tests/archive/test_issue_160_lazy_polars_expr.py); pattern removed from skip list. |
| **inferSchema by-design** | [tests/unit/dataframe/test_inferschema_parity.py](../tests/unit/dataframe/test_inferschema_parity.py) — v4 reader is string-only. Tests that require `inferSchema=True` are marked with `@INFER_SCHEMA_V4_SKIP` inside the file. Whole file remains in skip list (spark.read API differs with Robin). |
| **Un-skipped after test update** | [tests/unit/backend/test_robin_unsupported_raises.py](../tests/unit/backend/test_robin_unsupported_raises.py) — test updated to accept either `SparkUnsupportedOperationError` or `TypeError` (Robin may raise at select() for unsupported expressions); removed from skip list. |
| **Still skipped (Sparkless/Robin)** | [tests/integration/test_case_sensitivity.py](../tests/integration/test_case_sensitivity.py) — expects `spark.conf.is_case_sensitive()`; conf API differs with Robin. [tests/unit/dataframe/test_robin_plan.py](../tests/unit/dataframe/test_robin_plan.py) — `to_robin_plan()` expects Sparkless DataFrame with `_operations_queue`; Robin-backed session yields Robin wrapper without that. Remain in skip list. |
