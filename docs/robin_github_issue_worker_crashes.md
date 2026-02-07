# [Bug] Worker processes crash ("node down: Not properly terminated") when used from pytest-xdist (forked workers)

## Summary

When running the [Sparkless](https://github.com/eddiethedean/sparkless) test suite in Robin mode with **pytest-xdist** (parallel workers, e.g. `pytest -n 12`), **worker processes sometimes crash** with "node down: Not properly terminated". The process exits abruptly (no Python exception or normal teardown), which suggests a **crash or abort in native/Rust code** when the library is used from forked subprocess workers.

We’re reporting this so robin-sparkless can investigate **fork-safety** and **stability when used from multiprocessing/forked workers**.

## Environment

- **Consumer:** Sparkless, using robin-sparkless as an optional backend (`backend_type="robin"`).
- **Runner:** pytest with pytest-xdist, e.g. `pytest tests/ -n 12 --dist loadfile`.
- **Python:** 3.9 (and others).
- **Observed:** Multiple workers (e.g. gw8, gw10, gw11) report "node down: Not properly terminated", then xdist replaces them; the run can end with an INTERNALERROR (KeyError in xdist’s scheduler when handling the replacement, which is a known xdist bug).

## Observed output

```
[gw8] node down: Not properly terminated
replacing crashed worker gw8
...
[gw10] node down: Not properly terminated
replacing crashed worker gw10
[gw11] node down: Not properly terminated
replacing crashed worker gw11
```

No Python traceback is available for the crash because the worker process dies before Python can report it.

## Likely cause

The only native/runtime component in the test process in Robin mode is **robin-sparkless** (Rust/Python bindings). So we infer that some code path in robin-sparkless is **crashing the process** when run inside a forked worker, e.g.:

- **Fork-safety:** Native state (e.g. Rust runtime, allocator, or global state) that is not safe to use after `fork()`.
- **Resource or threading:** Multiple worker processes using the library in parallel leading to a crash (e.g. shared resources, signals).
- **Bug in native code:** Certain operations causing segfault or abort in the Rust layer.

## Tests running when workers crashed (for context)

The workers that crashed were running tests from modules such as:

- `tests/unit/test_column_case_variations.py` (case sensitivity, select/filter/join/withColumn, etc.)
- `tests/unit/dataframe/test_string_arithmetic.py`
- `tests/parity/functions/test_string.py`
- `tests/parity/sql/test_dml.py`

So the crash is not limited to a single test; it appears when the Robin backend is exercised from multiple forked workers across a variety of operations.

## Suggested investigation

1. **Fork-safety:** Ensure the Rust/native layer is safe when the process was created by `fork()` (e.g. no locks or shared state that become invalid after fork; consider re-initializing or avoiding heavy native init before fork).
2. **Stability under multiprocessing:** Run a small test that spawns several processes (e.g. multiprocessing.Pool or pytest-xdist with 4–8 workers), each creating a Robin session and running a few DataFrame operations; see if any process exits with a non-zero code or is killed by the OS.
3. **Signals and cleanup:** Check that signal handling and cleanup on process exit don’t trigger aborts or double-free in native code.

## Workaround (Sparkless side)

We run Robin mode with fewer workers (e.g. `-n 4`) or serially (`-n 0`) to reduce the chance of worker crashes and to avoid hitting the xdist KeyError when replacing crashed workers. We’ll document this and open this issue so robin-sparkless can address the root cause.

## Context

- **Sparkless Robin integration:** [backend_architecture](https://github.com/eddiethedean/sparkless/blob/main/docs/backend_architecture.md), [backend_selection](https://github.com/eddiethedean/sparkless/blob/main/docs/backend_selection.md).
- **Internal investigation:** [robin_mode_worker_crash_investigation.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_mode_worker_crash_investigation.md).
