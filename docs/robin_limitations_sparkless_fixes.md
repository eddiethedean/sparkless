# Robin limitations: fixes on Sparkless's side

During the robin-sparkless limitations and parity review, the following were identified as **Sparkless-side** (code or tests to fix here, not upstream in robin-sparkless).

## 1. Test / comparison: datetime in parity assertions

**Symptom:** `test_filter_on_table_with_complex_schema` (and any parity test whose result contains `datetime` values) can fail with:

`TypeError: Object of type datetime is not JSON serializable`

**Cause:** Result rows from Robin may contain Python `datetime`/`date` objects. If those values are ever passed into `json.dump`/`json.dumps` without a `default` (e.g. in error reporting, test harness, or assertion introspection), serialization fails.

**Fix (Sparkless):**

- Ensure every `json.dump`/`json.dumps` call that can see row data or assertion payloads uses `default=str` (or a custom default that turns `date`/`datetime` into strings). One place already missing it: `tests/tools/generate_expected_outputs.py` line 1971 (`json.dump(metadata, f, indent=2)`).
- When building assertion error messages that include "actual" or "expected" values, normalize `date`/`datetime` to strings (e.g. `str(v)` or `v.isoformat()`) so no non-JSON-serializable object is passed to code that might serialize it.

**Classification:** Treated as Sparkless/test in the catalog; no upstream robin-sparkless issue filed.

---

## 2. Robin materializer: concat with literal separator → concat_ws

**Symptom:** Expressions like `concat(first_name, lit(" "), last_name)` surface as:

`not found: concat(first_name,  , last_name)`

**Cause:** Robin has both `concat(columns)` and `concat_ws(separator, columns)`. The natural way to express "col1 + sep + col2" in Robin is `concat_ws(sep, col1, col2)`. Sparkless currently builds `concat(col1, lit(sep), col2)` and passes it to Robin; if Robin’s Python API or name resolution doesn’t treat that expression as a single evaluated expression, it can fail.

**Fix (Sparkless):** In the Robin materializer’s expression translation, when the expression is effectively “concat with a single literal separator in the middle” (e.g. `concat(col1, lit(sep), col2)` with `sep` a string), translate it to Robin’s `concat_ws(sep, col1, col2)` instead of `concat(col1, lit(sep), col2)`. That matches Robin’s API and avoids resolution issues.

**Upstream:** robin-sparkless issue #196 (concat/concat_ws with literal separator) was filed for parity; the above Sparkless change would reduce or remove the need for that pattern to rely on upstream changes.

---

## 3. Robin materializer: CaseWhen → when().then().otherwise()

**Symptom:** CaseWhen in select/withColumn fails with:

`not found: CASE WHEN ... THEN ... ELSE ... END`

**Cause:** The Robin materializer has **no** translation for CaseWhen. Robin-sparkless already provides `when(condition).then(value).otherwise(value)`. Sparkless never builds that; it either doesn’t send a Robin `Column` for CaseWhen or sends something that gets stringified and resolved by name.

**Fix (Sparkless):** In `_expression_to_robin` (or equivalent), add a branch for CaseWhen / when-otherwise: map the Sparkless CaseWhen AST to Robin’s `when(cond).then(val).otherwise(val)` and return that `Column`. No upstream feature request is required; this is entirely translation in the materializer.

---

## 4. Robin materializer: window expressions → Robin Window API

**Symptom:** Window expressions in select/withColumn fail with:

`not found: row_number() OVER (WindowSpec(...))` (and similar for sum over window, etc.)

**Cause:** The Robin materializer has **no** translation for window expressions. Robin-sparkless already has a Window API (e.g. `row_number().over(...)`, sum over window); see robin-sparkless tests and issue #187.

**Fix (Sparkless):** In the Robin materializer, add translation from Sparkless window expressions (WindowSpec, row_number, sum over window, etc.) to Robin’s Window API. This is a larger change (expression + possibly plan-level), but it is on Sparkless’s side; no new upstream feature is required.

---

## 5. Parquet / table append tests with PySpark backend

**Symptom:** Tests under `test_parquet_format_table_append.py` and similar fail with:

`AttributeError: 'SparkSession' object has no attribute '_storage'` or similar when the test runs with a PySpark-backed session.

**Cause:** In v4, only the Robin backend is supported; some tests or fixtures may still assume a Sparkless session with `_storage` (or other Sparkless-only attributes). Running with a “PySpark” backend or a bare PySpark session breaks those assumptions.

**Fix (Sparkless):** Either:

- Mark or skip these tests when the backend is not Robin (v4), or
- Provide a test-only path that uses a Sparkless Robin session so that table/parquet tests run only in the supported configuration.

**Classification:** Treated as Sparkless/test env in the catalog; no upstream issue.

---

## 6. “Unsupported backend type: pyspark” in v4

**Symptom:** Some parity or compatibility tests fail with:

`ValueError: Unsupported backend type: pyspark. Available backends: robin`

**Cause:** v4 is Robin-only; requesting the PySpark backend is intentionally invalid.

**Fix (Sparkless):** Treat as by design. Any test that explicitly requests the PySpark backend in v4 should be skipped or adapted (e.g. use Robin for “expected behavior” and skip live PySpark comparison when not available). No upstream change.

---

## Summary

| Item | Where to fix | Upstream issue? |
|------|----------------|------------------|
| Datetime in JSON / assertion payloads | Tests/tools: use `default=str` and normalize date/datetime in error messages | No |
| concat(col, lit(sep), col) → concat_ws | Robin materializer expression translation | #196 filed; Sparkless fix reduces reliance |
| CaseWhen → when/then/otherwise | Robin materializer expression translation | No |
| Window expressions → Robin Window API | Robin materializer (expression + plan) | No (#187 is upstream Window API; we only need to use it) |
| Parquet/table append session assumptions | Test markers / fixtures for v4 Robin-only | No |
| PySpark backend in v4 | Test skip/adapt for Robin-only | No |

So: **yes** — there are several concrete fixes on Sparkless’s end (test serialization, concat_ws translation, CaseWhen and window translation, and test/fixture setup for v4). The verification table and this doc are the references used when creating the upstream robin-sparkless issues.
