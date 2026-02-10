# Robin Logical Plan Contract (Sparkless v4)

This document captures the **Robin-sparkless logical plan contract** that Sparkless v4
targets when executing queries through the Robin backend.

The goal is to have a **single, shared plan representation** understood by both
Sparkless and Robin-sparkless, so that:

- Sparkless can build plans without depending on Robin’s Python DataFrame API.
- Robin can execute those plans using its Rust/Polars engine.
- The contract is explicit and versioned, rather than implicit in ad‑hoc adapters.

This document describes the **subset** of the plan format that Sparkless v4 Phase 1
intends to rely on. Future versions may extend coverage; such changes should be
explicitly versioned and documented.

## 1. Overall Shape

A Robin logical plan is a JSON-serializable **list of operations**:

```json
[
  { "op": "filter", "payload": { /* ... */ } },
  { "op": "select", "payload": { /* ... */ } },
  { "op": "limit",  "payload": { /* ... */ } }
]
```

- `op`: string – operation name (`"filter"`, `"select"`, `"limit"`, `"orderBy"`, etc.).
- `payload`: JSON object whose shape depends on the operation.

Sparkless is responsible for **building** this list from a `DataFrame`’s lazy
operations queue. Robin-sparkless is responsible for **interpreting** it.

## 2. Expression Encoding

Operation payloads reference expressions using a compact JSON format. This format
differs from Sparkless’s internal `logical_plan` expressions, which use
`{"type": "...", ...}`.

For Robin plans, expressions are encoded as:

- **Column reference**

  ```json
  { "col": "name" }
  ```

- **Literal**

  ```json
  { "lit": 3 }
  ```

  Values MUST be JSON-serializable (strings, numbers, booleans, null, arrays, objects).

- **Binary / unary operation**

  ```json
  {
    "op": "gt",
    "left": { "col": "age" },
    "right": { "lit": 21 }
  }
  ```

  For Phase 1, Sparkless uses the following operator names when converting from its
  internal representation:

  - Comparisons: `==` → `"eq"`, `!=` → `"ne"`, `>` → `"gt"`, `<` → `"lt"`,
    `>=` → `"ge"`, `<=` → `"le"`, `eqNullSafe` → `"eq_null_safe"`.
  - Arithmetic: `"add"`, `"sub"`, `"mul"`, `"div"` (mapped from `+`, `-`, `*`, `/`).
  - Logical: `"and"`, `"or"`, `"not"` (mapped from `&`, `|`, `!`).

- **Function call** (reserved for future use)

  ```json
  {
    "fn": "upper",
    "args": [
      { "col": "name" }
    ]
  }
  ```

Sparkless v4 Phase 1 may emit only `col`, `lit`, and `op` expressions. Use of
`fn`/`args` is reserved for a later phase once Robin’s function-call plan
language is formally surfaced.

### Unsupported / opaque expressions

When Sparkless encounters an expression that it cannot safely convert into this
format (e.g. complex window expressions), the Robin plan builder will raise
`ValueError` so that the lazy engine falls back to the existing
operation-by-operation path for the Robin backend.

## 3. Operation Payloads (Phase 1)

This section describes the **minimal set of operations** Sparkless intends to
emit in Robin plan form for Phase 1. The shapes are intentionally close to
Sparkless’s existing `logical_plan` payloads so both sides can evolve together.

### 3.1 Simple operations (no expressions)

- **limit**

  ```json
  { "op": "limit", "payload": { "n": 10 } }
  ```

- **offset**

  ```json
  { "op": "offset", "payload": { "n": 5 } }
  ```

- **drop**

  ```json
  { "op": "drop", "payload": { "cols": ["col_a", "col_b"] } }
  ```

- **distinct**

  ```json
  { "op": "distinct", "payload": { } }
  ```

- **withColumnRenamed**

  ```json
  {
    "op": "withColumnRenamed",
    "payload": { "existing": "old_name", "new": "new_name" }
  }
  ```

### 3.2 Operations with expressions

- **filter**

  ```json
  {
    "op": "filter",
    "payload": {
      "condition": {
        "op": "gt",
        "left": { "col": "age" },
        "right": { "lit": 21 }
      }
    }
  }
  ```

- **select**

  ```json
  {
    "op": "select",
    "payload": {
      "columns": [
        { "col": "name" },
        { "col": "age" }
      ]
    }
  }
  ```

- **withColumn**

  ```json
  {
    "op": "withColumn",
    "payload": {
      "name": "double_age",
      "expression": {
        "op": "mul",
        "left": { "col": "age" },
        "right": { "lit": 2 }
      }
    }
  }
  ```

- **orderBy**

  ```json
  {
    "op": "orderBy",
    "payload": {
      "columns": [
        { "col": "age" }
      ],
      "ascending": [true]
    }
  }
  ```

Other operations (`join`, `union`, `groupBy`, etc.) are **out of scope for
Phase 1** of the Robin plan integration in Sparkless. For those, Sparkless will
either:

- Continue to use the existing operation-by-operation Robin backend, or
- Extend this contract in a future phase once the Robin plan interpreter’s
  expectations are fully specified.

## 4. Versioning and Compatibility

- Sparkless v4 assumes a Robin-sparkless version **≥ 0.5.0** that stabilizes
  this plan format and exposes a plan execution entry point (e.g.
  `_execute_plan(data, schema, plan_json)` or equivalent).
- Any breaking changes to the Robin plan format MUST be:
  - Reflected here.
  - Released with a corresponding Sparkless update that knows how to emit the
    new format.
- During Phase 1, Sparkless is allowed to raise `ValueError` from its Robin plan
  builder for unsupported expressions or operations to force fallback to the
  legacy Robin backend behavior.

## 5. Relationship to Existing Sparkless Logical Plan

Sparkless already defines a backend-agnostic logical plan in
`docs/internal/logical_plan_format.md` and
`sparkless/dataframe/logical_plan.py`. That format is used primarily by the
Polars plan interpreter.

The **Robin plan contract** described here is:

- Focused specifically on Robin-sparkless’s needs.
- Implemented by a separate builder (e.g. `sparkless.dataframe.robin_plan`),
  which may internally reuse Sparkless’s existing plan serialization as an
  intermediate step.

Over time, if the Sparkless and Robin plan formats converge completely, this
document should either become the canonical plan spec for both projects or be
merged with the existing logical plan documentation.

