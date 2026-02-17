# Known Issues


## Delta Schema Evolution (v4 / Robin)

Sparkless v4 uses the Robin engine for execution. When appending to Delta tables with
``mergeSchema=true``, schema evolution that adds new columns is supported when the
robin-sparkless crate is built with the Delta feature. Type-changing operations
(e.g. widening ``INT`` to ``STRING``) may raise ``AnalysisException``.

**Recommended usage**
- Set ``.option("mergeSchema", "true")`` when appending new columns.
- Keep schema evolution granular. When downstream code expects string-based dates or timestamps, use ``sparkless.compat.datetime.to_date_str`` or ``to_timestamp_str`` for stable ISO-formatted values.


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

