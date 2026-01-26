# Profiling


This guide explains how to enable the new hot-path profiling hooks and interpret
the baseline measurements captured for the Polars backend and Python expression
evaluator.

## Enabling Instrumentation

Profiling is disabled by default. Turn it on via environment flags:

- `MOCK_SPARK_PROFILE=1` enables the global profiler.
- `MOCK_SPARK_FEATURE_enable_polars_vectorized_shortcuts=1` caches repeated
  struct field lookups in `PolarsOperationExecutor`.
- `MOCK_SPARK_FEATURE_enable_expression_translation_cache=1` enables the LRU
  cache inside `PolarsExpressionTranslator`.

You can set multiple flags using the JSON-based `MOCK_SPARK_FEATURE_FLAGS`
variable:

```bash
export MOCK_SPARK_FEATURE_FLAGS='{
  "enable_performance_profiling": true,
  "enable_polars_vectorized_shortcuts": true,
  "enable_expression_translation_cache": true
}'
```

## Instrumented Hot Paths

The following entry points now emit profiling samples when instrumentation is
enabled:

- `PolarsOperationExecutor.apply_select/apply_filter/apply_with_column`
- `PolarsOperationExecutor.apply_join/apply_group_by_agg`
- `ExpressionEvaluator.evaluate_expression/_evaluate_column_operation/_evaluate_function_call`

Each sample includes runtime (ms) and captured allocations (KB) based on
`tracemalloc`. Per-thread events are stored in
`sparkless.utils.profiling.collect_events()`.

## Collecting Samples

```python
from sparkless.session import SparkSession
from sparkless.utils import profiling

spark = SparkSession.builder.master("local[1]").getOrCreate()
profiling.clear_events()

df = spark.createDataFrame(
    [(i, f"text-{i % 5}", {"k": i}) for i in range(10_000)],
    ["id", "label", "payload"],
)
(
    df.filter(df.id % 2 == 0)
      .select("id", "label", df.payload["k"].alias("payload_k"))
      .groupBy("label")
      .count()
      .collect()
)

for event in profiling.collect_events():
    print(event)
```

## Baseline Snapshot (2025-11-13)

Measurements were taken on macOS 14.6.1 (M3 Pro, Python 3.11.7) using the sample
workloads above.

| Hot Path                                    | Duration (ms) | Peak (KB) | Notes |
|---------------------------------------------|--------------:|----------:|-------|
| `polars.apply_select`                       | 5.8           | 412.0     | Vectorised cache disabled |
| `polars.apply_select`                       | 4.1           | 414.3     | Vectorised cache enabled |
| `polars.apply_group_by_agg`                 | 7.4           | 155.1     | Aggregating 5 partitions |
| `expression.evaluate_expression`            | 14.9          | 96.2      | 10k rows, mixed arithmetic |
| `expression.evaluate_function_call`         | 9.6           | 60.8      | Map/string heavy workload |

> **Interpretation:** enabling the shortcut + translation caches reduces the
> `apply_select` wall time by ~29% for map-heavy queries, with stable memory
> usage. Expression evaluation remains dominated by user-defined functions;
> additional vectorisation opportunities should focus on Python fallbacks.

## Next Steps

- Use `profiling.collect_events()` at the end of larger integration tests to
  capture regressions over time.
- Wire the feature flags into CI smoke runs once steady-state performance
  thresholds are established.

