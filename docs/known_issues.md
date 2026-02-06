# Known Issues


## Delta Schema Evolution with the Polars Backend

Sparkless 3.0.0 introduced the Polars backend, which enforces strict column dtypes
at the storage layer. Earlier builds failed when appending to Delta tables with
``mergeSchema=true`` because the storage layer attempted to concatenate a
``Null``-typed column (from the existing data) with a concrete dtype (from the
new rows). The storage implementation now reconciles appended batches to the
registered schema before persistence, allowing schema evolution that adds new
columns to succeed.

**Limitations**
- Schema evolution is currently limited to adding new columns. Type-changing
  operations (for example, widening ``INT`` to ``STRING``) will still raise an
  ``AnalysisException``.
- Nested schema reconciliation is shallow—the helper casts the top-level column
  types. For complex restructures, prefer migrating the data offline and
  recreating the table with the desired schema.
- When working with backends other than Polars, behaviour falls back to the
  legacy implementation. Verify your storage backend selection if you continue
  to see mismatches.

**Recommended usage**
- Continue to set ``.option("mergeSchema", "true")`` when appending columns to
  ensure the reconciliation path is activated.
- Keep schema evolution granular; add one change per write to simplify
  debugging and match Delta Lake best practices.
- When downstream consumers expect string-based dates or timestamps, wrap the
  expressions with ``sparkless.compat.datetime.to_date_str`` or
  ``to_timestamp_str`` to obtain stable ISO-formatted text values without
  mutating PySpark behaviour.


## Delta Table: Unsupported Operations (NotImplementedError)

The mock Delta implementation supports a subset of Delta Lake operations. The
following will raise ``NotImplementedError`` with a clear message:

- **DeltaTable.update()** with column expressions: Only literal/value assignments
  are supported; ``Column`` or ``ColumnOperation`` expressions in the update map
  are not supported.
- **Delta merge** with non-equality conditions: Only equality join conditions
  (e.g. ``target.col = source.col``) are supported; other predicates are not.
- **Delta merge** when the join condition does not reference both target and
  source aliases correctly.
- **Delta merge** assignments using column expressions: Only literal/value
  assignments are supported in merge ``whenMatchedUpdate`` / ``whenNotMatchedInsert``
  maps.

Use literals or pre-computed values when using ``DeltaTable.update()`` or merge
assignments in the mock implementation.


## DataFrame.explain() Options

``DataFrame.explain(extended=..., codegen=..., cost=..., ...)`` is implemented
with the following limitations:

- **codegen**: Parameter is accepted for API compatibility but is **not implemented**;
  code generation info is not produced.
- **cost**: Parameter is accepted for API compatibility but is **not implemented**;
  cost estimates are not produced.

``extended`` and ``mode`` (e.g. ``"graph"`` for DOT format) work as documented.


## Deprecations

The following are deprecated; use the recommended replacements to avoid future
removal:

- **sparkless.errors**: Use ``sparkless.core.exceptions`` for exception imports.
- **Functions()** constructor (e.g. ``from sparkless.sql.functions import Functions; F = Functions()``):
  Use ``import sparkless.sql.functions as F`` instead.
- **LazyEvaluationEngine** heuristic methods (e.g. ``_requires_manual_materialization``
  heuristics): Prefer explicit capability checks on the materializer/backend.
- **Function aliases**: Prefer the canonical names; deprecated aliases emit
  ``DeprecationWarning`` or ``FutureWarning``:
  - ``toDegrees`` → ``degrees``
  - ``toRadians`` → ``radians``
  - ``approxCountDistinct`` → ``approx_count_distinct``
  - ``sumDistinct`` → ``sum`` with ``distinct``
  - ``bitwiseNOT`` → ``bitwise_not``
  - ``shiftLeft`` → ``shiftleft``
  - ``shiftRight`` → ``shiftright``
  - ``shiftRightUnsigned`` → ``shiftrightunsigned``

