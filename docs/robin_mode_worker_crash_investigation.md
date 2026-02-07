# Robin mode: worker crashes and INTERNALERROR (KeyError WorkerController)

## What we saw

When running the test suite in Robin mode with 12 workers (`-n 12`), the run ended with:

```
[gw8] node down: Not properly terminated
replacing crashed worker gw8
...
[gw10] node down: Not properly terminated
replacing crashed worker gw10
[gw11] node down: Not properly terminated
replacing crashed worker gw11
INTERNALERROR> KeyError: <WorkerController gw14>
  ...
  File ".../xdist/scheduler/loadscope.py", line 275, in _assign_work_unit
    worker_collection = self.registered_collections[node]
KeyError: <WorkerController gw14>
```

So: **multiple workers crashed** ("Not properly terminated"), then xdist tried to replace them and hit a **KeyError** in the scheduler.

---

## Root cause (two layers)

### 1. Worker crashes (primary)

"Node down: Not properly terminated" means the **worker process died abruptly** (no normal Python teardown). Typical causes:

- **Segfault or abort in native code** – e.g. Rust/C extension used from a forked worker
- **OOM kill**
- **Unhandled fatal signal**

In **Robin mode**, the only native/runtime layer in the test process is **robin-sparkless** (Rust/Python bindings). So the most plausible cause is that some test triggers a code path in robin-sparkless that **crashes the process** when run inside a pytest-xdist worker (forked subprocess). That could be:

- Fork-safety: Rust/native state not safe after fork
- A bug in robin-sparkless that aborts or segfaults on certain operations
- Resource or threading issues when many workers use the library

We don’t get a Python traceback for the crash because the process is killed before Python can report it.

### 2. KeyError in xdist (secondary)

When a worker crashes, pytest-xdist replaces it and continues. There is a **known xdist bug**: during that replacement flow, the scheduler can look up `registered_collections[node]` for a node that is not (or no longer) in the dict, leading to **KeyError: &lt;WorkerController gwX&gt;** (see [pytest-xdist#714](https://github.com/pytest-dev/pytest-xdist/issues/714)). So the INTERNALERROR is a **consequence** of the worker crashes plus xdist’s handling of crashed/replacement workers, not a bug in our tests or in Robin itself.

---

## What we can do

### Sparkless side

1. **Run Robin mode with fewer workers or serial**  
   Reduces concurrency and the chance of hitting crashy paths or stressing robin-sparkless:
   - Fewer workers: `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 4`
   - Serial: `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 0`  
   (Serial also avoids xdist entirely, so no KeyError from replacement workers.)

2. **Document the behavior**  
   In docs/backend_selection or a Robin-specific note: when running with many workers, Robin mode may occasionally show "node down: Not properly terminated" and an INTERNALERROR (KeyError WorkerController). Workaround: use fewer workers or `-n 0` for Robin.

3. **Optionally default Robin to fewer workers**  
   In `tests/run_all_tests.sh`, when `BACKEND=robin`, you could set a lower default worker count (e.g. 4) unless the user overrides with `-n`.

### pytest-xdist

- This is an upstream bug (issue #714; there may be a fix in a PR). We can:
  - Pin or upgrade to a xdist version that fixes the KeyError when it’s released.
  - Not block on it; reducing workers or using `-n 0` for Robin avoids the replacement path that triggers the bug.

### robin-sparkless

- **Yes, we should open an issue.** The report should say:
  - We run the Sparkless test suite with pytest-xdist (multiple forked workers) in Robin mode.
  - Worker processes sometimes crash with "node down: Not properly terminated" (no Python exception, process dies).
  - This suggests a crash or abort in native/Rust code when used from forked subprocess workers.
  - Ask them to investigate: fork-safety, signal handling, and stability when the library is used from multiprocessing/forked workers.
  - Optionally attach: a short log snippet (node down + replacing crashed worker) and the test modules that were running on the crashed workers (e.g. test_column_case_variations, test_string_arithmetic, parity/functions/test_string, parity/sql/test_dml) as potentially triggering tests.

---

## Summary

| Cause | Owner | Action |
|-------|--------|--------|
| Worker process crash ("Not properly terminated") | Likely robin-sparkless (native code in forked workers) | Open robin-sparkless issue; run Robin with fewer workers or `-n 0` in the meantime |
| KeyError: WorkerController in xdist | pytest-xdist (issue #714) | Use fewer workers or serial for Robin to avoid replacement path; track xdist fix |

No change is required inside Sparkless test code; the failures are due to worker crashes and xdist’s handling of them.
