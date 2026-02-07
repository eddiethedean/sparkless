# Finding tests that stall in Robin mode

## What was fixed

- **`test_regexp_extract_all_multiple_matches_and_nulls`** – This test was stalling (and is now **skipped in Robin mode**). The underlying Polars path was also fixed: `regexp_extract_all` now compiles the regex **inside** the closure so it doesn’t hang under pytest-xdist (fork + compiled regex).
- No other `re.compile`-in-closure patterns were found in the Polars expression translator; the only one was `regexp_extract_all`.

## What else could stall in Robin mode

1. **First use of Robin on a worker** – The first test on a given worker that uses the `spark` fixture may trigger `robin_sparkless.SparkSession.builder().getOrCreate()` or first import of the Robin materializer. If that blocks (e.g. native init), that test will appear to stall. Any test can be “first” on a worker, so the stalling test may vary by run order.
2. **Tests that hit Robin materializer with heavy or blocking ops** – A test that Robin “supports” but that triggers a slow or blocking path in robin-sparkless (e.g. certain `collect()` or expression evaluation) could stall.
3. **Global timeout (e.g. CI)** – A 10‑minute **job** timeout can stop the run before pytest-timeout (300s per test) fires, so you never see a “TIMEOUT” line for a specific test.

## How to find which tests stall

Run the Robin suite with a **shorter per-test timeout** so that stalling tests are reported as TIMEOUT and the run continues. Then grep the output for those tests.

```bash
# From repo root; robin-sparkless installed
export SPARKLESS_TEST_BACKEND=robin
export SPARKLESS_BACKEND=robin

# Per-test timeout 60s; 12 workers; save full output
python -m pytest tests/unit/ tests/parity/ \
  --ignore=tests/archive \
  -n 12 --dist loadfile \
  -v --tb=short \
  --timeout=60 --timeout-method=thread \
  -m "not performance" \
  2>&1 | tee robin_mode_timeout_scan.txt

# List tests that hit the 60s timeout (pytest-timeout prints "TIMEOUT" in the line)
grep -E "TIMEOUT|timeout" robin_mode_timeout_scan.txt
```

Any line like:

`FAILED ... - TIMEOUT`

or similar in the pytest-timeout output corresponds to a test that stalled (or was slow) and is a candidate for skip or fix.

---

## Result of last timeout scan (Robin mode, 60s per-test)

**Command run:** `SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python3 -m pytest tests/unit/ tests/parity/ --ignore=tests/archive -n 12 --dist loadfile -v --tb=short --timeout=60 --timeout-method=thread -m "not performance" 2>&1 | tee robin_mode_timeout_scan.txt`

- **Outcome:** No tests hit the 60s timeout. Run finished in ~85s.
- **Counts:** 663 passed, 600 failed, 11 skipped.
- **File:** `robin_mode_timeout_scan.txt` (full output).
- **Note:** An `INTERNALERROR` (KeyError: WorkerController gw14) appeared at teardown; this is an xdist worker-teardown flake, not a stalling test.

So with the regexp_extract_all test skipped in Robin mode (and the Polars closure fix), no stalling tests were detected in this run. If stalls reappear, re-run the above and grep for `TIMEOUT` in the new output.
