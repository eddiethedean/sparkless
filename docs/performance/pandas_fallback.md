# Pandas Fallback


Sparkless ships with a lightweight stub that mimics the minimal slice of the
pandas API that appears in tests and documentation examples. The stub keeps the
default installation lean and avoids the heavy NumPy dependency tree. When
parity with native pandas is needed, you can opt-in to the real implementation
and compare behaviour using the benchmarking harness described below.

## Installing the Native Backend

```bash
pip install .[pandas]
```

The optional extra brings in `pandas` and type stubs only; NumPy wheels are
pulled in transitively where available.

## Switching Backends

Set the `MOCK_SPARK_PANDAS_MODE` environment variable before importing `pandas`:

| Value   | Behaviour                                           |
|---------|-----------------------------------------------------|
| `stub`  | Always use the built-in shim (default)              |
| `native`| Require real pandas; raise if it is missing         |
| `auto`  | Use native pandas when installed, otherwise fallback|

```bash
export MOCK_SPARK_PANDAS_MODE=native   # or stub / auto
```

You can also query the active backend at runtime:

```python
import pandas
print(pandas.get_backend())           # "native" or "stub"
```

## Running the Benchmark Suite

The helper script compares core operations (`DataFrame` construction, `to_dict`,
`concat`, and basic indexing) between the stub and native backends.

```bash
python scripts/benchmark_pandas_fallback.py --rows 50000 --samples 7
```

Example output (M3 Pro laptop, macOS 14.6.1):

```
Backend    | Create (mean ms)   | to_dict (mean ms) | concat (mean ms)  | iloc (mean ms)
------------------------------------------------------------------------
stub       | 4.812              | 2.337             | 3.921             | 0.118
native     | 7.604              | 1.281             | 2.017             | 0.064
```

Export raw metrics for further analysis:

```bash
python scripts/benchmark_pandas_fallback.py --output benchmark.json
```

## Interpreting Results

- **Stub strengths:** predictable performance, zero external dependencies,
  ideal for unit tests and CI pipelines without binary wheels.
- **Native strengths:** faster `to_dict`/`concat` operations on larger datasets,
  full pandas feature coverage for interactive analysis or downstream tooling.

Choose the mode that best matches your scenario:

- For CI and fast feedback loops, keep the default stub enabled.
- For parity investigations or integration environments where pandas is already
  present, enable the native backend with `MOCK_SPARK_PANDAS_MODE=native`.

The benchmarking harness provides a quick way to validate regressions when
upgrading either the stub or native dependency chain.

