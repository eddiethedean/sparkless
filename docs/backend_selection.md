# Backend Selection (v4)

In Sparkless v4 only the **Robin** backend is supported. Polars, memory, file, and DuckDB backends have been removed from the default build.

## v4: Robin only

- **robin** – The only supported backend. Uses the `robin-sparkless` package (>=0.6.0) for execution. Required dependency of Sparkless v4.

`BackendFactory.list_available_backends()` returns `["robin"]`. Passing any other `backend_type` or `SPARKLESS_BACKEND` value raises `ValueError`.

## Creating a session

```python
from sparkless import SparkSession

# Default (Robin)
spark = SparkSession("MyApp")

# Explicit (same in v4)
spark = SparkSession(backend_type="robin")
```

Setting `SPARKLESS_BACKEND=robin` is optional; the default is already Robin.

## Migration from v3

If you were using Polars, memory, file, or DuckDB in v3, see **docs/migration_v3_to_v4.md** for the v3→v4 migration guide.

## Robin backend behaviour

Robin (robin-sparkless) provides the execution engine for DataFrame operations. The materializer supports: **filter**, **select**, **limit**, **orderBy**, **withColumn**, **join**, **union**, **distinct**, and **drop**. **groupBy** + agg and some expressions may raise `SparkUnsupportedOperationError` if not yet supported. For a full list and
recommended Sparkless improvements, see [robin_compatibility_recommendations.md](robin_compatibility_recommendations.md).

When running tests in Robin mode with many parallel workers, you may see worker
crashes ("node down: Not properly terminated") or an INTERNALERROR. Use fewer
workers (e.g. `-n 4`) or run serially (`-n 0`); the test runner defaults to 4
workers for Robin. See [robin_mode_worker_crash_investigation.md](robin_mode_worker_crash_investigation.md).

## Running tests

In v4 the default backend is Robin. To run the full test suite:

```bash
bash tests/run_all_tests.sh
```

Or with pytest (optionally with Robin env set explicitly):

```bash
SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 10
# Serial:
python -m pytest tests/ -v
```

Individual tests can request the Robin backend via the marker:
`@pytest.mark.backend('robin')`.

## Troubleshooting

- **`ValueError: Unsupported backend type`** – In v4 only `robin` is supported. Do not set `SPARKLESS_BACKEND` or `backend_type` to any other value.
- **Robin backend not available** – Install with `pip install robin-sparkless>=0.6.0` (required for v4).
- **Robin: worker crashes or INTERNALERROR** – When using pytest-xdist with Robin, use fewer workers (e.g. `-n 4`) or serial (`-n 0`). See [robin_mode_worker_crash_investigation.md](robin_mode_worker_crash_investigation.md).
- **I pass `-n 10` but tests run sequentially** – Nothing in this repo overrides `-n`. Check: (1) **Environment:** `echo $PYTEST_ADDOPTS` — if it contains `-n 0`, unset it (`unset PYTEST_ADDOPTS`) or remove `-n 0` from wherever it is set (e.g. `.bashrc`, `.zshrc`, IDE env). (2) **IDE:** If you run tests from Cursor/VS Code, the test runner may be invoking pytest with its own args; check the Python/Testing settings and any run configuration for extra pytest options like `-n 0`. Running `python -m pytest tests/ -n 10` from the project root in a terminal should always respect `-n 10`.

