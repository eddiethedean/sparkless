# Robin test failure investigation (Sparkless-side)

Classification of failure categories from `scripts/robin_parity_repros/FAILURE_CATEGORIES.md` and actions. Source: `test_results_full.txt` (937 failures).

## Classification

| Category ID | Count | Likely root | Action |
|-------------|-------|-------------|--------|
| op_withColumn | 326 (+ 24 + 10 chained) | Sparkless: `_expression_to_robin(payload[1])` returns None for expression type | Extend `_expression_to_robin` for unsupported ops (struct with string cols, isnan, UDF→skip, window→partial) |
| op_select | 52 | Sparkless: `_expression_to_robin(expr_to_check)` returns None for one of the select list | Same as above |
| op_filter | 52 | Sparkless: `_simple_filter_to_robin(condition)` returns None | Extend `_simple_filter_to_robin` (e.g. isnan) |
| col_not_found | 100 | Robin case sensitivity or Sparkless sending alias/expr as name | Translate expression to Robin Column; else document/skip |
| op_join | 32 | Join with array_contains or unsupported `on` | Sparkless: extend join if feasible; else Robin or skip |
| parity_assertion | 132 + 71 | Wrong result or schema mismatch | Per test: Sparkless bug vs Robin semantic vs test expectation; fix or skip |
| other (expected List, lengths don't match, etc.) | various | Robin (#254, #256) or engine behavior | Confirm no Sparkless fix; document/skip |

## Expression/op gaps to fix in Sparkless (from tracing and rep tests)

- **struct("Name", "Value")**: struct receives string column names; `_expression_to_robin("Value")` returns None because a plain string is not a Column. Fix: in struct branch, if an element is str, use F.col(name).
- **isnan(col)**: Not handled in `_expression_to_robin` or `_simple_filter_to_robin`. Fix: add op == "isnan" (or "is_nan") and translate to Robin F.isnan(inner) if available.
- **UDF in select/withColumn/filter**: Robin likely does not support UDFs; keep can_handle False, document. No translation.
- **Window expressions**: Partially implemented (WindowFunction branch); many still fail (e.g. window with arithmetic). Robin #187; extend only if Robin API is sufficient.
- **array() empty / array(col1, col2)**: "array requires at least one column" is Robin API; document. Optional: translate F.array() to Robin empty array if API exists.

## Entry points (materializer)

- **Select**: `_can_handle_select` → for each payload item, `_expression_to_robin(expr_to_check)` must be non-None (materializer ~L1234–1236).
- **withColumn**: `_can_handle_with_column` → `_expression_to_robin(payload[1])` must be non-None and not list/tuple (~L1244–1249).
- **Filter**: `_can_handle_filter` → condition is translated; `_simple_filter_to_robin(condition)` must be non-None.

## Prioritized fixes (Phase 2) — completed

1. **struct with string column names** – Done. In struct branch, for each element if isinstance(c, str): use F.col(c). Robin's struct() takes one Sequence; use F.struct(robin_cols) when F.struct(*robin_cols) raises TypeError.
2. **isnan** – Done. Added isnan/is_nan in `_expression_to_robin` (unary_ops) and in `_simple_filter_to_robin` (unary isnan).
3. Remaining: struct result shape/field names may differ (Robin); isnan on string column not supported by Robin (Polars). Document and skip those tests for Robin.
