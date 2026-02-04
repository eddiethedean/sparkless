# Logical Plan Format

This document describes the backend-agnostic serializable logical plan produced by Sparkless. The plan allows backends (e.g. Polars, Robin) to execute DataFrame operations without depending on Sparkless Column/ColumnOperation trees.

## Overall shape

A logical plan is a **list of operation entries**. Each entry is a JSON-serializable object:

```json
{"op": "<op_name>", "payload": <serialized_payload>}
```

- `op`: string, the operation name (e.g. `"filter"`, `"select"`, `"limit"`).
- `payload`: JSON-serializable value (dict, list, string, number, boolean, null). No Column/ColumnOperation/Literal/DataFrame objects.

The plan is produced from a DataFrame's `_operations_queue` at materialization time via `to_logical_plan(df)`.

## Reserved keys and extensibility

- Top-level: `op`, `payload`.
- Expression trees inside payloads use: `type`, `name`, `value`, `op`, `left`, `right`, and op-specific keys.
- Future extensions may add optional keys (e.g. `version`, `hints`) without breaking existing consumers.

## Phase 1 operation payloads

### Simple ops (no expression trees)

- **limit**: `{"n": <int>}`
- **offset**: `{"n": <int>}`
- **drop**: `{"cols": [<str>, ...]}`
- **distinct**: `{}`
- **withColumnRenamed**: `{"existing": <str>, "new": <str>}`

### Ops with expression trees

- **filter**: `{"condition": <expr>}` where `<expr>` is the output of `serialize_expression(condition)`.
- **select**: `{"columns": [<expr_or_str>, ...]}`. Each item is either a string (column name) or a serialized expression.
- **withColumn**: `{"name": <str>, "expression": <expr>}`.

### Phase 3: join, union, orderBy, groupBy

- **join**: `{"on": [<str>], "how": <str>, "other_plan": <list>, "other_data": [<dict>], "other_schema": [{"name": <str>, "type": <str>}]}`. The other DataFrame is fully serialized as nested plan, data (JSON-safe rows), and schema (field name + type simpleString).
- **union**: `{"other_plan": <list>, "other_data": [<dict>], "other_schema": [{"name", "type"}]}`.
- **orderBy**: `{"columns": [<expr>], "ascending": [<bool>]}` (already in Phase 1).
- **groupBy**: `{"columns": [<expr>], "aggs": [<agg>, ...]}`. Each `<agg>` is either `{"func": <str>, "column": <str|null>, "alias": <str|null>}` (structured) or `{"type": "agg_str", "expr": "<str>"}` (e.g. `"sum(age)"`). For `count(*)`/`count(1)`, column is `"*"` and func is `"count"`.

## Expression format (Option B: structured dicts)

Expressions are recursive dicts. All values must be JSON-serializable.

- **Column reference**: `{"type": "column", "name": "<str>"}`
- **Literal**: `{"type": "literal", "value": <json-serializable>}`. Values like datetime are serialized (e.g. ISO string); Decimal as string.
- **Binary operation**: `{"type": "op", "op": "<operation>", "left": <expr>, "right": <expr>}`. For unary ops, `"right": null`.
- **Unary operation**: `{"type": "op", "op": "<operation>", "left": <expr>, "right": null}`

Supported operation names: comparisons `==`, `!=`, `<`, `>`, `<=`, `>=`, `eqNullSafe`; arithmetic `+`, `-`, `*`, `/`, `%`, `**`; logical `&`, `|`, `!`; `isin`, `between`, `like`, `rlike`, `isnull`, `isnotnull`, `cast`; sort `asc`, `desc`, `desc_nulls_last`, `desc_nulls_first`, `asc_nulls_last`, `asc_nulls_first`.

- **Window expressions**: Structured form `{"type": "window", "function": "<name>", "column": "<col|null>", "partition_by": [<str>], "order_by": [{"name": <str>, "descending": <bool>}], "rows_between": <tuple|null>, "range_between": <tuple|null>, "alias": <str|null>}`. Supported functions: row_number, rank, dense_rank, sum, avg, mean, min, max, count. Unsupported or parse errors fall back to `{"type": "window", "opaque": true, "repr": "..."}`.
- **Struct field access**: column name may contain dots (e.g. `"a.b.c"`); representable as column ref.
- **UDF / complex calls**: not yet serialized; use opaque or extend with `{"type": "call", "name": str, "args": [...]}` in future.

## Plan-based materialization (Phase 2)

Backends may implement an optional method `materialize_from_plan(self, data, schema, logical_plan) -> List[Row]`. The lazy engine uses it when:

- The materializer has this method, and
- Either `spark.sparkless.useLogicalPlan` is set to `true` (via session config), or the backend type is `robin`.

Otherwise the engine calls `materialize(data, schema, operations)` as before. Config key: `spark.sparkless.useLogicalPlan` (default: not set / false).

## Polars plan interpreter expression coverage

The interpreter supports all serialized expression ops: comparisons (including `eqNullSafe`), arithmetic (including `**`), `between`, `cast`, `like`, `rlike`, sort variants (`asc`/`desc` and nulls_last/first), plus groupBy with common aggregations.

## Limitations

- Window functions: structured serialization and Polars interpreter support for row_number, rank, dense_rank, sum, avg, mean, min, max, count (partition_by and order_by). Frames (rows_between/range_between) are serialized but not yet applied in interpreter; opaque fallback for unsupported cases.
- Struct field access: column name may contain dots (`"a.b.c"`); representable as column ref.
- groupBy: fully serialized (columns + aggs); Polars interpreter supports sum, avg, mean, max, min, count, stddev, variance, first, last and agg_str parsing (e.g. `"sum(age)"`).
