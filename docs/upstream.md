# Upstream Coordination Notes

## Robin-sparkless crate version

Sparkless v4 depends on **robin-sparkless 0.11.3** (see `Cargo.toml`). 0.11.3 fixes all reported Sparkless parity issues, including:

- **[#492](https://github.com/eddiethedean/robin-sparkless/issues/492)** — Case-insensitive `orderBy` on mixed-case column names (PySpark parity).
- **[#176](https://github.com/eddiethedean/robin-sparkless/issues/176)** — `select()` with column expressions and `regexp_extract_all` for PySpark compatibility.
- **[#503](https://github.com/eddiethedean/robin-sparkless/issues/503)** — `select()` with column names: accept `payload.columns` (invalid plan error).
- Other parity issues reported from Sparkless v4 testing (e.g. groupBy agg, window, between/power/cast, empty DataFrame parquet).

## Robin-Sparkless PySpark parity

- **Goal:** Sparkless v4 runs entirely on the `robin-sparkless` Rust engine while preserving **PySpark semantics**.
- **Policy:** For any observed behavior difference between Sparkless and PySpark:
  - First, **verify the mismatch against real PySpark** with a minimal, self-contained example.
  - If the minimal example shows that **robin-sparkless** itself disagrees with PySpark (independent of Sparkless glue code), treat this as an **upstream Robin parity gap**.
  - Open an issue in [eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) that:
    - Clearly labels the problem as “PySpark parity” in the title.
    - Includes code samples for both robin-sparkless and PySpark demonstrating the difference.
    - Notes the exact crate/PySpark versions and links back to any Sparkless issue/PR.
  - In Sparkless, keep a short “Robin gaps” list and link each entry to the corresponding upstream issue so we can clean up skips/workarounds once the crate is fixed.

## Delta Schema Evolution
- **Issue:** Polars backend raises `ValueError: type String is incompatible with expected type Null`
  when appending with `mergeSchema=true`.
- **Local Mitigation:** `sparkless/backend/polars/schema_utils.py` now coerces write batches to the
  registered schema and fills new columns with correctly typed nulls.
- **Upstream Ask:** Confirm whether Polars intends to support concatenating `Null` and
  concrete dtypes automatically. If yes, track the fix; if no, document the requirement
  to normalise data before insertion.
- **Artifacts:** Regression test `tests/unit/delta/test_delta_schema_evolution.py` reproduces
  the scenario.

## Datetime Conversions
- **Issue:** `to_date`/`to_timestamp` now return Python `date`/`datetime` objects under
  Polars, breaking substring-based assertions in downstream code.
- **Local Mitigation:** New helpers in `sparkless.compat.datetime` offer column- and
  value-level normalisation.
- **Upstream Ask:** Clarify whether future Polars releases will expose string-returning
  variants or document recommended migration paths.

## Documentation Examples
- **Issue:** Documentation tests execute `python3 examples/...` which resolves to the global
  interpreter and imports the pip-installed `sparkless`. This diverges from the workspace
  version.
- **Proposed Action:** Decide whether to ship a CLI wrapper that sets `PYTHONPATH` before
  running examples or update tests to invoke `sys.executable` instead of `python3`.

