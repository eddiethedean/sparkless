# Robin-sparkless upstream issues

## Division of responsibility

- **robin-sparkless** aims to fully emulate PySpark with as much pure Rust as possible. Gaps in Robin (relative to PySpark) are intended to be closed upstream in robin-sparkless.
- **Sparkless** fills in gaps with Python packages and other Python-specific functionality (e.g. session, catalog, SQL, compatibility shims). When Robin does not yet support something that PySpark supports, we report it to robin-sparkless so Robin can reach parity; we do not treat it as a permanent Sparkless-only workaround.

## Policy: report Robin gaps upstream

If something **PySpark can do** and **robin-sparkless** does not support it (or behaves differently), we **report it** to the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) GitHub repo. We do not tiptoe around differences or accept them silently—they are upstream bugs or missing features and should be filed so Robin can reach PySpark parity.

- **Reproduce** with minimal robin-sparkless code and the same logic in PySpark (baseline).
- **File** via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file <path>` with summary, PySpark expected behavior, Robin actual behavior, and repro steps.
- **Document** the issue number in this file and in [v4_behavior_changes_and_known_differences.md](v4_behavior_changes_and_known_differences.md) where relevant.

Minimal repro scripts live in `scripts/repro_robin_limitations/` and `scripts/robin_parity_repros/`; use them as templates for new gaps.

## Issues created from robin_sparkless_needs.md (2026-02-08)

| # | Title | Link |
|---|-------|------|
| 182 | select()/with_column() resolve Column expressions by name instead of evaluating them | https://github.com/eddiethedean/robin-sparkless/issues/182 |
| 184 | [Enhancement] Filter: support Column–Column comparisons (col_a > col_b) | https://github.com/eddiethedean/robin-sparkless/issues/184 |
| 185 | [Enhancement] filter(condition): document Column-only or accept literal bool | https://github.com/eddiethedean/robin-sparkless/issues/185 |
| 186 | [Enhancement] lit(): extend to date/datetime types for PySpark parity | https://github.com/eddiethedean/robin-sparkless/issues/186 |
| 187 | [Enhancement] Window API for row_number, rank, sum over window, lag, lead | https://github.com/eddiethedean/robin-sparkless/issues/187 |

Created via `gh issue create -R eddiethedean/robin-sparkless` with body files in `docs/robin_issue_*.md`.

## Issues created 2026-02-06 (from ownership analysis)

| # | Title | Link |
|---|-------|------|
| 174 | [Enhancement] Add Python operator overloads to Column for PySpark compatibility | https://github.com/eddiethedean/robin-sparkless/issues/174 |
| 175 | [Enhancement] Join on= parameter: accept string for single column (PySpark compatibility) | https://github.com/eddiethedean/robin-sparkless/issues/175 |

Created via `python scripts/create_robin_github_issues_2026_02.py`

## Issues created from failure catalog (2026-02-10, Phase 8 plan)

Grouped by root cause after running Robin unit+integration and parity tests, verifying features in robin-sparkless source, and classifying failures. One issue per group. See `docs/robin_sparkless_verification_table.md` and `scripts/create_robin_issues_from_catalog.py`.

| # | Title | Link |
|---|-------|------|
| 194 | [Sparkless parity] Column name case sensitivity vs PySpark | https://github.com/eddiethedean/robin-sparkless/issues/194 |
| 195 | [Sparkless parity] Column/expression resolution (not found) | https://github.com/eddiethedean/robin-sparkless/issues/195 |
| 196 | [Sparkless parity] concat/concat_ws with literal separator or mixed literals | https://github.com/eddiethedean/robin-sparkless/issues/196 |
| 197 | [Sparkless] Reader schema inference (string-only in v4) | https://github.com/eddiethedean/robin-sparkless/issues/197 |
| 198 | [Sparkless parity] map(), array(), nested struct/row values | https://github.com/eddiethedean/robin-sparkless/issues/198 |
| 199 | [Sparkless parity] Other expression or result parity | https://github.com/eddiethedean/robin-sparkless/issues/199 |
| 200 | [Sparkless parity] substring/substr or alias/partial resolution | https://github.com/eddiethedean/robin-sparkless/issues/200 |
| 201 | [Sparkless parity] Type strictness (string vs numeric, coercion) | https://github.com/eddiethedean/robin-sparkless/issues/201 |
| 202 | [Sparkless parity] Unsupported filter conditions (complex/column-column) | https://github.com/eddiethedean/robin-sparkless/issues/202 |

Created via `python scripts/create_robin_issues_from_catalog.py` (uses `tests/robin_unit_integration_results.txt`, `tests/robin_parity_full_results.txt`, `tests/pyspark_parity_full_results.txt`).

## Issues created from minimal repros (robin-sparkless 0.6.0, 2026-02-10)

Reproduced with **robin-sparkless 0.6.0** using direct API (no Sparkless). Each limitation was run with Robin and with PySpark; only cases where Robin failed and PySpark succeeded were filed. Repros: `scripts/repro_robin_limitations/01_type_strictness.py` through `10_datetime_row.py`. Results: `scripts/repro_robin_limitations/RESULTS_0.6.0.md`.

| # | Title | Link |
|---|-------|------|
| 235 | [0.6.0 repro] Type strictness: string vs numeric comparison raises RuntimeError | https://github.com/eddiethedean/robin-sparkless/issues/235 |
| 236 | [0.6.0 repro] CaseWhen: Column.otherwise() missing (AttributeError) | https://github.com/eddiethedean/robin-sparkless/issues/236 |
| 237 | [0.6.0 repro] Window/row_number not exposed in Python API | https://github.com/eddiethedean/robin-sparkless/issues/237 |
| 238 | [0.6.0 repro] F.concat not found in Python API | https://github.com/eddiethedean/robin-sparkless/issues/238 |
| 239 | [0.6.0 repro] datetime in row not accepted (row values must be scalar types) | https://github.com/eddiethedean/robin-sparkless/issues/239 |

Created via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file tests/.robin_issue_*.txt`.

## Issues created from minimal repros (robin-sparkless 0.7.0, 2026-02-11)

Reproduced with **robin-sparkless 0.7.0** using direct API (no Sparkless). Repros: `scripts/repro_robin_limitations/11_order_by_nulls.py`, `12_isin_empty_list.py`. Results: `scripts/repro_robin_limitations/RESULTS_0.7.0.md`. All 10 original repros (01–10) now pass with 0.7.0.

| # | Title | Link |
|---|-------|------|
| 244 | [0.7.0 repro] Column.isin() not found | https://github.com/eddiethedean/robin-sparkless/issues/244 |
| 245 | [0.7.0 repro] Column.desc_nulls_last() and nulls ordering methods not found | https://github.com/eddiethedean/robin-sparkless/issues/245 |

Created via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file tests/.robin_issue_*.txt`.

## Issues created from verified parity repros (2026-02-12)

Reproduced with **robin-sparkless** using direct API (no Sparkless). Each gap was run with Robin and with PySpark; only cases where Robin failed and PySpark succeeded were filed. Repros: `scripts/robin_parity_repros/04_filter_eqNullSafe.py`, `05_parity_string_soundex.py`, `06_filter_between.py`, `07_split_limit.py`. See `scripts/robin_parity_repros/VERIFICATION.md`. (04/05/06 fixed in Robin 0.8; #248, #249, #250.)

| # | Title | Link |
|---|-------|------|
| 248 | [Parity] Column.eqNullSafe / eq_null_safe missing for null-safe equality in filter | https://github.com/eddiethedean/robin-sparkless/issues/248 |
| 249 | [Parity] soundex() string function missing | https://github.com/eddiethedean/robin-sparkless/issues/249 |
| 250 | [Parity] Column.between(low, high) missing for range filter | https://github.com/eddiethedean/robin-sparkless/issues/250 |
| 254 | [Parity] F.split(column, pattern, limit) — limit parameter not supported | https://github.com/eddiethedean/robin-sparkless/issues/254 |

Created via `python scripts/create_robin_issues_from_verified_repros.py`.

## Issues created from repros (2026-02-12)

Minimal Robin + PySpark repro in each issue body. Repros: `scripts/robin_parity_repros/08_create_dataframe_array_column.py`, `scripts/repro_robin_limitations/11_order_by_nulls.py`.

| # | Title | Link |
|---|-------|------|
| 256 | [Parity] create_dataframe_from_rows: unsupported type 'list'/'array' for column | https://github.com/eddiethedean/robin-sparkless/issues/256 |
| 257 | [Parity] order_by does not accept Column.desc_nulls_last() result (PySortOrder) | https://github.com/eddiethedean/robin-sparkless/issues/257 |

Created via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file scripts/robin_issue_bodies/issue_*.md`.

## Issues created from verified parity repros (2026-02-12, Robin 0.8.3)

Reproduced with **robin-sparkless 0.8.3** using direct API (no Sparkless). Each gap was run with Robin and with PySpark; only cases where Robin failed and PySpark succeeded were filed. Repros: `scripts/robin_parity_repros/09_round_string_column.py` through `14_eqnullsafe_type_coercion.py`. See `scripts/robin_parity_repros/VERIFICATION.md`.

| # | Title | Link |
|---|-------|------|
| 262 | [Parity] F.round() on string column — Robin raises "round can only be used on numeric types" | https://github.com/eddiethedean/robin-sparkless/issues/262 |
| 263 | [Parity] F.array() with no args — Robin raises "array requires at least one column" | https://github.com/eddiethedean/robin-sparkless/issues/263 |
| 264 | [Parity] posexplode() missing from robin_sparkless module | https://github.com/eddiethedean/robin-sparkless/issues/264 |
| 265 | [Parity] date/datetime vs string comparison — Robin raises "cannot compare date to string" | https://github.com/eddiethedean/robin-sparkless/issues/265 |
| 266 | [Parity] eqNullSafe type coercion — Robin raises "cannot compare string with numeric type" | https://github.com/eddiethedean/robin-sparkless/issues/266 |

Created via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file scripts/robin_issue_bodies/verified_*.md`.

## Issues created from parity repro run (2026-02-13)

Repros run from sparkless repo: `python scripts/robin_parity_repros/run_all_repros.py` and individual scripts. Only gap still showing Robin FAIL + PySpark OK after deduplication: posexplode() API (Robin requires Column, PySpark accepts string column name).

| # | Title | Link |
|---|-------|------|
| 280 | [Parity] posexplode() should accept column name (string) for PySpark compatibility | https://github.com/eddiethedean/robin-sparkless/issues/280 |

Created via `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file scripts/robin_issue_bodies/parity_posexplode_accept_string.md`. Repro: `scripts/robin_parity_repros/11_posexplode_array.py`. Related: #264 (posexplode missing).

## Issues from v4 skip list (Sparkless tests)

Created from [docs/v4_robin_skip_list_to_issues.md](v4_robin_skip_list_to_issues.md). Each gap corresponds to skipped Sparkless tests when running with Robin-backed session. Repros: `scripts/robin_parity_repros/19_*.py` through `27_*.py`. Body files: `scripts/robin_issue_bodies/skip_*.md`.

| # | Title | Link |
|---|-------|------|
| 284 | [Parity] SparkSession.sql() and SparkSession.table() for PySpark compatibility | https://github.com/eddiethedean/robin-sparkless/issues/284 |
| 285 | [Parity] DataFrame.createOrReplaceTempView() and temp view resolution in sql()/table() | https://github.com/eddiethedean/robin-sparkless/issues/285 |
| 286 | [Parity] getActiveSession or session registry so aggregate functions (sum/avg/count) work | https://github.com/eddiethedean/robin-sparkless/issues/286 |
| 287 | [Parity] DataFrame.agg(*exprs) for global aggregation (no groupBy) | https://github.com/eddiethedean/robin-sparkless/issues/287 |
| 288 | [Parity] Window.partitionBy() / orderBy() accept column names (str) not only Column | https://github.com/eddiethedean/robin-sparkless/issues/288 |
| 289 | [Parity] DataFrame.na.drop() and na.fill() with subset, how, thresh | https://github.com/eddiethedean/robin-sparkless/issues/289 |
| 290 | [Parity] DataFrame.fillna(value, subset=[...]) | https://github.com/eddiethedean/robin-sparkless/issues/290 |
| 291 | [Parity] create_dataframe_from_rows: allow empty data with schema or empty schema | https://github.com/eddiethedean/robin-sparkless/issues/291 |
| 292 | [Parity] union_by_name(allow_missing_columns=True) | https://github.com/eddiethedean/robin-sparkless/issues/292 |
| 293 | [Parity] first() / first_ignore_nulls() aggregate | https://github.com/eddiethedean/robin-sparkless/issues/293 |

## Sparkless parity issues created (earlier)

- **#1–#17:** Created from initial subset (join, filter, select, transformations); see `scripts/create_robin_github_issues.py`.
- **104 additional issues:** Created from broad parity run: same tests run in Robin mode (`tests/robin_parity_broad_results.txt`) and PySpark mode (`tests/pyspark_parity_failed_results.txt`); issues opened only for tests that **fail with Robin** and **pass with PySpark**, excluding the 17 above. Script: `scripts/create_robin_github_issues_from_results.py` (uses `--dry-run` to preview).
- **Second batch (19 issues):** From `tests/parity/sql/` and `tests/parity/internal/`. Robin run saved to `tests/robin_parity_sql_internal_results.txt` (23 failed, 32 passed). Those 23 run in PySpark → 19 passed, 4 skipped. Issues created for the 19 parity gaps. Command to reproduce results:
  ```bash
  SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/parity/sql/ tests/parity/internal/ -v --tb=line -q 2>&1 | tee tests/robin_parity_sql_internal_results.txt
  ```
  Then run failed IDs in PySpark and create issues:
  ```bash
  python scripts/create_robin_github_issues_from_results.py \
    --robin-results tests/robin_parity_sql_internal_results.txt \
    --pyspark-results tests/pyspark_parity_sql_internal_results.txt \
    --no-already-filed
  ```
  Use `--dry-run` to preview before creating issues.

---

## Sparkless integration note (no upstream feature request needed)

**Finding:** Robin-sparkless already provides what Sparkless needs:

- **Arbitrary schema:** Use `create_dataframe_from_rows(data, schema)` where `data` is a list of dicts or lists and `schema` is a list of `(column_name, dtype_str)` (e.g. `[("id", "bigint"), ("name", "string")]`). The 3-column restriction applies only to `create_dataframe()`.
- **Operations:** The DataFrame API already has `filter`, `select`, `with_column`, `order_by`, `order_by_exprs`, `group_by`, `limit`, `union`, `union_by_name`, `join`, and `GroupedData` (count, sum, avg, min, max, agg, etc.).

The gap is in **Sparkless**: our Robin materializer currently uses only `create_dataframe` (3-column) and supports only filter/select/limit. We will extend it to use `create_dataframe_from_rows` and to translate more operations to the existing robin-sparkless API. No upstream feature issues are required for “flexible schema” or “more operations.”

---

## Bug report template

**Title:** [Bug] Short description of the bug

**Body:**

**Description**  
[One or two sentences describing the incorrect behavior.]

**To reproduce**  
[Minimal code or steps, e.g. Sparkless snippet that calls robin_sparkless and triggers the bug.]

```python
# Example:
import robin_sparkless
# ...
```

**Expected behavior**  
[What you expect to happen.]

**Actual behavior**  
[What actually happens (error message, wrong result, etc.).]

**Environment**  
- Python version:  
- robin-sparkless version:  
- OS:  

**Additional context**  
[Optional: stack trace, logs, or links to Sparkless integration code.]
