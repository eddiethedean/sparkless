
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

