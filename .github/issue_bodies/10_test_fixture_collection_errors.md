## Summary

Four test modules cause **pytest collection errors** when running the full suite (e.g. with Robin backend): `tests/parity/dataframe/test_parquet_format_table_append.py`, `tests/test_backend_capability_model.py`, `tests/test_issue_160_manual_cache_manipulation.py`, `tests/test_issue_160_nested_operations.py`. Fix collection so these modules either skip under Robin or load without requiring Polars/missing _storage.

## Failure (current behavior)

Run:
```bash
SPARKLESS_TEST_BACKEND=robin python -m pytest -n 10
```

Result includes:
```
ERROR tests/parity/dataframe/test_parquet_format_table_append.py
ERROR tests/test_backend_capability_model.py
ERROR tests/test_issue_160_manual_cache_manipulation.py
ERROR tests/test_issue_160_nested_operations.py
```

Collection fails (e.g. fixture or import raises when backend is Robin or when _storage is missing).

## Expected behavior

- No collection errors: these modules should either be skipped at collection time when `SPARKLESS_TEST_BACKEND=robin` (or when backend is Robin-only), or their fixtures/imports should not assume PySpark/Polars or session._storage so they can at least be collected.
- Parquet/table append tests are already skipped when backend is not Robin (conftest); ensure they don’t error during collection (e.g. avoid importing or building fixtures that require _storage before the skip logic runs).
- Backend capability and issue_160 tests: skip module or fix session/backend fixture so they don’t require an unsupported backend or missing attributes.

## Fix (Sparkless-side)

- In conftest or in the affected modules: use pytest.importorskip or skip at collection time for Robin when the test requires _storage, Polars, or PySpark.
- Alternatively, make fixtures lazy or conditional so that the module can be collected without instantiating a session that lacks _storage.
- Add or extend v4_robin_skip_list.txt patterns for tests that are known to fail due to UDF or input_file_name so they are skipped when `SPARKLESS_TEST_BACKEND=robin` and reduce failure noise.

Ref: tests/conftest.py, tests/parity/dataframe/test_parquet_format_table_append.py, tests/test_backend_capability_model.py, tests/test_issue_160_*.py, tests/unit/v4_robin_skip_list.txt.
