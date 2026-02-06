# Troubleshooting

## Native dependency crashes (Polars / Arrow)

Sparkless uses Polars (and optionally Arrow) for execution. On some environments you may see process crashes (e.g. segmentation fault, exit code 139) during tests or when running scripts.

### What to try

1. **Isolate the failure**  
   Run a minimal script that only creates a session and one DataFrame operation. If that crashes, the issue is likely in the native toolchain (Polars/Arrow/Rust) or the Python version.

2. **Pin Polars / PyArrow**  
   If you recently upgraded Polars or PyArrow, try pinning to a known-good version (see `pyproject.toml` or `docs/requirements.txt` for versions used in CI).

3. **Use a supported Python version**  
   Sparkless supports Python 3.9–3.12. Crashes on older or newer interpreters may be due to ABI or build mismatches.

4. **Run without coverage**  
   Coverage instrumentation can trigger different code paths. Run tests with `--no-cov` to see if the crash disappears.

5. **Avoid Hypothesis in-process DB under load**  
   In some environments, Hypothesis’s in-memory database during collection can contribute to instability. Running a subset of tests (e.g. by path or marker) can help narrow this down.

### Pure-Python fallbacks

Some operations have **pure-Python fallbacks** when the Polars path fails or is unavailable:

- **Percentile / approximate percentile** – If the Polars implementation is not available or raises, the engine may fall back to a Python implementation. Results should still be correct; performance may be lower.
- **Covariance / correlation** – Similarly, covariance and correlation can use Python fallbacks when the native path is not used.

If you rely on these and see crashes, ensure you are on a supported Polars version and Python version; the fallbacks are intended to improve robustness rather than to work around broken native builds.

## Session or catalog errors

- **"No active SparkSession"** – Ensure you create a session (e.g. `SparkSession("MyApp")` or `SparkSession.builder.appName("MyApp").getOrCreate()`) before calling `F.col`, `F.lit`, or other session-dependent functions.
- **Catalog / database not found** – Use `spark.catalog.setCurrentDatabase("your_db")` (or the builder/config equivalent) so that session-aware helpers and schema tracking see the correct database. See [Configuration](configuration.md) and the “Session-aware literals and schema tracking” section in the getting started guide.

## Tests failing only with PySpark or only with mock

- Run with an explicit backend: `MOCK_SPARK_TEST_BACKEND=mock` or `MOCK_SPARK_TEST_BACKEND=pyspark` so behavior is deterministic.
- Some tests are skipped when PySpark or Delta is not installed; see test docstrings and `tests/conftest.py` for markers and skip conditions.

## Getting help

- **Known issues**: [Known issues](../known_issues.md) documents limitations (e.g. Delta schema evolution).
- **GitHub issues**: [github.com/eddiethedean/sparkless/issues](https://github.com/eddiethedean/sparkless/issues) for bugs and feature requests.
