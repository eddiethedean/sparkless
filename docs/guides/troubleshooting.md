# Troubleshooting

## Native extension / Robin (v4)

Sparkless v4 runs on the **Robin (Rust) engine** via a PyO3-built extension. Crashes (e.g. segfault, exit 139) are usually due to the native extension or the Robin crate.

### What to try

1. **Isolate the failure** – Run a minimal script: create a session and one DataFrame action. If it crashes, the issue is likely in the Robin extension or Rust toolchain.
2. **Rebuild the extension** – From the repo: `maturin develop` or `pip install -e .`. Ensure Rust and maturin are up to date.
3. **Use a supported Python version** – Sparkless supports Python 3.8+. Crashes on other versions may be ABI/build related.
4. **Run without coverage** – Use `pytest --no-cov` to rule out coverage-related paths.
5. **Fewer parallel workers** – With pytest-xdist, try `-n 4` or `-n 0` if you see worker crashes.

### Pure-Python fallbacks

Some operations (e.g. percentile, covariance) have pure-Python fallbacks when the engine path fails. Results should remain correct; performance may be lower.

## Session or catalog errors

- **"No active SparkSession"** – Ensure you create a session (e.g. `SparkSession("MyApp")` or `SparkSession.builder.appName("MyApp").getOrCreate()`) before calling `F.col`, `F.lit`, or other session-dependent functions.
- **Catalog / database not found** – Use `spark.catalog.setCurrentDatabase("your_db")` (or the builder/config equivalent) so that session-aware helpers and schema tracking see the correct database. See [Configuration](configuration.md) and the “Session-aware literals and schema tracking” section in the getting started guide.

## Tests failing only with PySpark or only with Robin

- Run with an explicit test backend: `SPARKLESS_TEST_BACKEND=robin` or `MOCK_SPARK_TEST_BACKEND=pyspark` so behavior is deterministic.
- Some tests are skipped when PySpark or Delta is not installed; see test docstrings and `tests/conftest.py` for markers and skip conditions.

## Getting help

- **Known issues**: [Known issues](../known_issues.md) documents limitations (e.g. Delta schema evolution).
- **GitHub issues**: [github.com/eddiethedean/sparkless/issues](https://github.com/eddiethedean/sparkless/issues) for bugs and feature requests.
