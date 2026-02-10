# Issue Templates for robin-sparkless (Upstream)

The following text can be copied into the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) GitHub repo when opening issues.

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
