# Backend / engine (v4)

**Sparkless v4 has a single execution engine.** There is no backend selection.

## Robin engine (v4)

- Execution is entirely via the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) **Rust crate**, integrated with PyO3 as a native extension.
- The crate is **compiled into Sparkless** when you install from source or from wheels. You do **not** install a separate `robin-sparkless` Python package.
- No `spark.sparkless.backend` config, no `BackendFactory`, no `backend_type` constructor argument.
- Session creation: `SparkSession.builder.appName("MyApp").getOrCreate()`.

## Running tests with Robin

Tests run against the Robin engine by default in v4. To force Robin explicitly (e.g. in CI):

```bash
SPARKLESS_TEST_BACKEND=robin pytest tests/ -n 12 -v --ignore=tests/archive
# Or
SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh
```

## Migration from v3

In v3 you could select backends (polars, memory, file, robin) via config or constructor. In v4:

- **Polars / memory / file backends** – Removed. Execution is Robin-only.
- **Robin** – No longer optional; it is the only engine. You do not need `pip install sparkless[robin]` or `robin-sparkless`; the crate is bundled in the extension.

See [robin_v4_overhaul_plan.md](robin_v4_overhaul_plan.md) for the full v4 design and migration notes.
