# Debugging logical plans and Robin execution (Phase 4)

Maintainer-facing guide to inspect what Sparkless sends to Robin and what Robin returns. See [sparkless_v4_roadmap.md](../sparkless_v4_roadmap.md) §7.4.

## Pretty-printing logical plans

From a test or REPL you can print the Sparkless internal plan or the Robin-format plan for a DataFrame.

**Sparkless format** (internal logical plan):

```python
from sparkless import SparkSession
import sparkless.sql.functions as F
from sparkless.utils.plan_debug import pretty_print_logical_plan, get_logical_plan

spark = SparkSession("debug")
df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}]).filter(F.col("a") > 1).select("a")

# Print to stdout
pretty_print_logical_plan(df, format="sparkless")

# Or get the list and dump to file
plan = get_logical_plan(df, format="sparkless")
import json
with open("plan.json", "w") as f:
    json.dump(plan, f, indent=2, default=str)
```

**Robin format** (plan sent to Robin when backend is robin). Unsupported operations/expressions will raise `ValueError`:

```python
pretty_print_logical_plan(df, format="robin")
# Or return string
s = pretty_print_logical_plan(df, format="robin", return_str=True)
```

## Dump on materialization (`SPARKLESS_DEBUG_PLAN_DIR`)

When debugging a failing test, set the environment variable to a directory path. On each Robin-path materialization, Sparkless will write:

- **plan.json** – Robin-format logical plan (or Sparkless plan if Robin plan build failed).
- **input_data.json** – Input rows (JSON-safe).
- **schema.json** – Schema (field names and type names).
- **result.json** – Result rows on success.
- **error.txt** – Exception message and traceback on failure.

Multiple materializations in one run use a simple counter/timestamp so later ones do not overwrite (e.g. `plan_001.json`, or a timestamped subdir per materialization).

**Example:**

```bash
SPARKLESS_DEBUG_PLAN_DIR=tests/debug_plan_output pytest tests/unit/dataframe/test_column_astype.py::TestColumnAstype::test_basic_astype_int -x -v
```

Then inspect `tests/debug_plan_output/` for the written files. When the test fails, the last dump corresponds to the failing materialization.

## Dump layout (files written)

| File | Content |
|------|---------|
| `plan.json` | Logical plan (Robin format if built; otherwise Sparkless format or partial). |
| `input_data.json` | List of input rows (dicts or list-of-values). |
| `schema.json` | Schema: list of `{"name": "...", "type": "..."}`. |
| `result.json` | Result rows on success (only if materialization completed). |
| `error.txt` | Exception message and traceback on failure. |

When the Robin plan builder raises (e.g. unsupported op), the dump still writes input_data, schema, and error; plan may be Sparkless format or missing if not yet built.

## Reproducing a failing test with Robin only

1. **Run the failing test with dump enabled:**
   ```bash
   SPARKLESS_DEBUG_PLAN_DIR=tests/debug_plan_output pytest tests/unit/path/to/test.py::test_name -x -v
   ```

2. **Inspect the dump** in `tests/debug_plan_output/`: `plan.json`, `input_data.json`, `schema.json`, and `error.txt` (if it failed).

3. **Use the dump to reproduce:**
   - **Option A**: If Robin-sparkless exposes a plan executor (e.g. `execute_plan(data, schema, plan)`), use [scripts/reproduce_robin_plan.py](../../scripts/reproduce_robin_plan.py) (if present) to load the dump and call the executor, reproducing the failure outside Sparkless.
   - **Option B**: Until Robin plan execution is wired in Sparkless, the dump shows exactly "what Sparkless sent" (plan + input + schema). To reproduce "what Robin returned", run the same operations manually via Robin’s Python API; see [internal/robin_plan_contract.md](../internal/robin_plan_contract.md) for the plan format.

4. **Minimal Robin-only repro**: From the dump, build a small Python script that creates a Robin DataFrame from `input_data.json` + `schema.json`, then applies the operations in `plan.json` (or the equivalent Robin API calls) and prints the result or error.
