# Robin Expression Plan Format Mismatch

When Sparkless sends **filter** or **withColumn** steps to the robin-sparkless crate, those steps can fail with:

- `expression must have 'col', 'lit', 'op', or 'fn'`
- `withColumn must have 'expr'`

This document explains the mismatch and the resolution.

---

## Resolution

Sparkless now **adapts** the logical plan to Robin’s format before calling the crate. The adapter lives in `sparkless.robin.plan_adapter` and is invoked from `sparkless.robin.execution` after `to_logical_plan()` and before `execute_plan_via_robin()`. It:

- **Filter:** Unwraps `payload.condition` and recursively converts the expression tree to Robin’s shape (`col`, `lit`, `op` with word names like `gt`/`eq`, and arithmetic as `fn` calls).
- **WithColumn:** Renames `expression` → `expr` in the payload and converts the nested expression to Robin format.

Filter and withColumn plans produced by Sparkless are therefore converted to [LOGICAL_PLAN_FORMAT](https://robin-sparkless.readthedocs.io/en/latest/LOGICAL_PLAN_FORMAT/) (col/lit/op/fn) before execution. The rest of this doc is kept as reference for the original mismatch.

---

## 1. Filter: payload shape

| | Sparkless sends | Robin expects |
|---|-----------------|---------------|
| **Filter payload** | `{"condition": <expr>}` | The expression **itself** (no wrapper) |

Robin does `expr_from_value(&payload)`, so it treats the whole payload as the expression. Sparkless wraps the condition in `payload.condition`, so Robin sees `{"condition": ...}` and finds no `col` / `lit` / `op` / `fn` at the top level → **"expression must have 'col', 'lit', 'op', or 'fn'"**.

**Fix (either):** Sparkless sends `{"op": "filter", "payload": <expr>}` (payload = raw expression), or Robin accepts `payload.condition` when present and uses that as the expression.

---

## 2. WithColumn: expression key name

| | Sparkless sends | Robin expects |
|---|-----------------|---------------|
| **WithColumn payload** | `{"name": "...", "expression": <expr>}` | `{"name": "...", "expr": <expr>}` |

Robin does `payload.get("expr")` and errors with **"withColumn must have 'expr'"** when the key is missing. Sparkless uses the key **"expression"** instead of **"expr"**.

**Fix (either):** Sparkless renames the key to `"expr"`, or Robin also accepts `payload.get("expression")`.

---

## 3. Expression tree format (col / lit / op)

Inside any expression (filter condition, withColumn expr, select computed column), Robin expects the **canonical** shape. Sparkless uses a **different** shape:

| Concept | Sparkless | Robin (LOGICAL_PLAN_FORMAT) |
|---------|-----------|----------------------------|
| **Column reference** | `{"type": "column", "name": "x"}` | `{"col": "x"}` |
| **Literal** | `{"type": "literal", "value": 50000}` | `{"lit": 50000}` |
| **Comparison op** | `{"type": "op", "op": ">", "left": ..., "right": ...}` | `{"op": "gt", "left": ..., "right": ...}` |
| **Op names** | `">"`, `"<"`, `"=="`, `"!="`, `">="`, `"<="` | `"gt"`, `"lt"`, `"eq"`, `"ne"`, `"ge"`, `"le"` |
| **Function call** | `{"type": "fn", "fn": "upper", "args": [...]}` (if used) | `{"fn": "upper", "args": [...]}` |

So even after fixing the filter payload shape and the withColumn `expr` key, the **inner** expression must use `col`/`lit`/`op`/`fn` and Robin’s op names (`gt`, `eq`, etc.), or Robin must accept Sparkless’s `type`/`name`, `type`/`literal`/`value`, and `type`/`op` with symbol names and translate them.

---

## Summary

- **Filter:** Payload should be the expression object, not `{"condition": <expr>}`; and the expression must use Robin’s format (`col`, `lit`, `op` with `gt`/`eq`/…).
- **WithColumn:** Payload must use key `"expr"` (not `"expression"`); and the expression must use Robin’s format.
- **Expressions:** Robin expects `col`, `lit`, `op`, `fn` at the top level and word op names (`gt`, `lt`, `eq`, …). Sparkless currently emits `type`/`column`/`name`, `type`/`literal`/`value`, `type`/`op` with symbols (`>`, `<`, …).

To fix on the Sparkless side you can add a **plan adapter** (e.g. in `sparkless/robin/execution.py`) that, before calling the crate:

1. For **filter**: use `payload = payload.get("condition", payload)` so the crate receives the raw expression; then recursively rewrite expressions to Robin shape (`type`/`column`/`name` → `col`, `type`/`literal`/`value` → `lit`, `type`/`op` with `>`/`<`/… → `op` with `gt`/`lt`/…, and `expression` → `expr` for withColumn).
2. For **withColumn**: rename `expression` → `expr` in the payload.
3. Recursively convert all expression nodes to the canonical form expected by `expr_from_value` (see Robin’s Expression tree section and `expr.rs`).

Alternatively, the robin-sparkless crate can be extended to accept Sparkless’s payload shapes and expression format (e.g. accept `condition` and `expression`, and map `type`/`column` → `col`, `type`/`literal` → `lit`, symbol ops → word ops).
