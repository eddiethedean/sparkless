# Research Notes: Making Sparkless a Thin Wrapper over Robin-Sparkless

This document captures **research findings and design explorations** for a potential Sparkless v4.0.0 that is *mostly a wrapper* over `robin-sparkless`, while:

- Preserving Sparkless’s **public PySpark-compatible API** (`sparkless.sql.SparkSession`, `DataFrame`, `functions as F`, `types`, exceptions, etc.).
- Keeping the **existing parity and unit tests** as the behavioral spec.
- Avoiding duplication of core query/execution logic between the two projects.

It is intentionally exploratory rather than a final plan. Some options below conflict with each other; the goal is to enumerate tradeoffs.

---

## 1. Current State: How Sparkless and Robin-Sparkless Relate Today

### 1.1 Sparkless architecture (simplified)

- **Public API layer**
  - `sparkless.sql` and `sparkless` top-level:
    - `SparkSession`, `SparkContext`, `JVMContext`.
    - `DataFrame`, `GroupedData`, `DataFrameWriter`, window APIs.
    - `functions` (`F`), `Column`, `ColumnOperation`.
    - `types` (e.g. `StructType`, `StringType`, `ArrayType`).
    - Exceptions mirroring PySpark (e.g. `AnalysisException`).

- **Lazy DataFrame / logical layer**
  - Each `DataFrame` holds:
    - Underlying **data + schema** (in a storage backend).
    - A lazy **operations queue** `[(op_name, payload), ...]`.
  - Transformations (`select`, `filter`, `join`, etc.) enqueue operations via `_queue_op`.
  - `LazyEvaluationEngine.materialize(df)`:
    - Picks a backend via `BackendFactory`.
    - Asks a backend-specific `DataMaterializer` to run the queue.

- **Backend / execution layer**
  - Historically, Sparkless supported **multiple execution backends** behind a common protocol:
    - `StorageBackend` for tables.
    - `DataMaterializer` with `materialize(data, schema, operations)` and optionally `materialize_from_plan(...)`.
    - Implementations included Polars (default), Robin (optional), and others (memory/file, DuckDB).
  - For a **Sparkless v4.0.0 that is a thin wrapper over Robin-sparkless**, this research assumes a **single execution backend**:
    - **Robin-sparkless becomes the only engine** used for query execution.
    - Existing backend abstractions become primarily an internal historical artifact and/or a migration target to be simplified away.
  - Sparkless already has an **internal logical plan format** and can build a serialized plan; in a Robin-only world this format can focus on feeding the Robin engine rather than multiple backends.

### 1.2 Robin-sparkless architecture (simplified)

- **Core engine**
  - Rust/Polars engine wrapped by Python module `robin_sparkless`.
  - Central abstractions:
    - `SparkSession` / `SparkSessionBuilder` (Rust & Python).
    - `DataFrame`, `Column`, `GroupedData`, `WindowSpec`, etc.
    - Rich function library (string, datetime, aggregates, arrays, maps, etc.).

- **Plan representation**
  - JSON-based **logical plan**:
    - A list of operations like `{"op": "filter", "payload": {...}}`.
    - Payloads reference expressions encoded as JSON trees:
      - `{"col": "name"}` / `{"lit": 3}`.
      - `{"op": "gt", "left": ..., "right": ...}`.
      - `{"fn": "upper", "args": [...]}`.
  - Interpreter in Rust (`src/plan`) walks the plan and applies each op to a Robin `DataFrame`.

- **Execution entry points**
  - DataFrame API: `df.filter(...)`, `df.select(...)`, `df.group_by(...).agg(...)`, etc.
  - Plan API: internal helpers that run a plan over in-memory data:
    - Python helper like `_execute_plan(data, schema, plan_json)` (used in tests/fixtures).

- **Test focus**
  - Extensive parity suite against PySpark and Sparkless fixtures.
  - Many tests already validate:
    - Filter, select, withColumn, joins, groupBy, window functions.
    - Complex expressions and types.

### 1.3 Existing integration

- Sparkless already **has a Robin backend** that:
  - Creates a `robin_sparkless.SparkSession`.
  - Converts Sparkless `Column` trees into Robin `Column`s.
  - Applies operations op-by-op (call Robin `filter`, `select`, `with_column`, etc.).
- Robin-sparkless has multiple docs specifically about Sparkless integration:
  - `SPARKLESS_REFACTOR_PLAN.md`, `READINESS_FOR_SPARKLESS_PLAN.md`, `SPARKLESS_INTEGRATION_ANALYSIS.md`, etc.
- There is **significant overlap** between Sparkless parity tests and Robin parity fixtures.

Conclusion: the two systems are already conceptually aligned (PySpark-like API + parity tests), but they communicate primarily via **Python-level Column trees**, not via a shared logical plan.

---

## 2. Target v4 architecture (single Robin backend)

For Sparkless v4.0.0 we assume a **single, simplified architecture**:

- **Sparkless owns the Python-facing API and planning**
  - Public modules and imports (`sparkless.sql.SparkSession`, `DataFrame`, `functions as F`, `types`, exceptions).
  - Lazy construction of a simple **logical plan** for each `DataFrame` (or equivalent operation list).
  - PySpark-compatibility rules, config surface, and error types that users see.

- **Robin-sparkless is the only execution engine**
  - Sparkless does **not** choose between multiple backends at runtime.
  - All query execution (filters, projections, joins, groupBy/agg, windows, functions, etc.) ultimately runs through Robin-sparkless.

- **Single delegation mechanism**
  - When an action is needed (`collect`, `count`, `show`, etc.), Sparkless:
    1. Serializes the current DataFrame’s logical plan into a format Robin understands (directly or via a small adapter).
    2. Calls a Robin entry point such as `_execute_plan(data, schema, plan_json)` (exact API to be agreed).
    3. Wraps Robin’s results back into Sparkless `Row`/`DataFrame` objects.

This “one front-end, one engine, one execution path” model deliberately ignores alternative architectures (e.g. wrapping Robin’s DataFrame API directly, or supporting multiple engines) to keep v4 conceptually simple.

---

## 3. Key technical questions within this architecture

Even with a single Robin backend and a single delegation path, there are still important design choices to resolve.

### 3.1 Plan format and ownership

- Should Sparkless adopt **Robin’s plan format** directly (JSON ops + JSON expressions) as its logical plan representation?
  - This maximizes reuse and minimizes glue code, at the cost of tighter coupling to Robin’s versioned format.
- Or should Sparkless define its own internal plan format and provide a **simple adapter** to Robin’s plan format at the delegation point?
  - This gives Sparkless more freedom to evolve planning/optimization, at the cost of maintaining a mapping layer.

In both variants, there is **only one execution engine**; the question is whether the “official” logical plan spec lives in Sparkless, Robin, or as a small adapter between them.

### 3.2 Expression semantics and edge cases

- Robin-sparkless already describes its expression coverage and differences from PySpark (see `PYSPARK_DIFFERENCES.md`, `SIGNATURE_GAP_ANALYSIS.md`, etc.).
- Sparkless adds its own compatibility shims and parity expectations via:
  - Expression building logic (`Column`, `functions`).
  - How it serializes expressions for backends.

Questions:

- Do we want Sparkless to **normalize and enforce** PySpark semantics before hitting Robin, or let Robin define semantics and Sparkless just mirror them?
- For edge cases (null handling, overflow, type promotion), do we:
  - Treat Robin as the source of truth, and adjust Sparkless tests/docs.
  - Or add additional layers in Sparkless (post-processing, casting, etc.) to match existing expectations?

This is critical for how thin the wrapper can realistically be.

### 3.3 Scope of Robin-sparkless responsibility

Within this single-backend design, Robin-sparkless is the **only place where queries execute**, but there is still a choice of how much it owns:

- **Core responsibility (minimum)**:
  - Execute filters, projections, joins, groupBy/agg, window functions, and built-in functions over tabular data.
  - Implement the expression language and type system used in the logical plan.

- **Extended responsibility (optional)**:
  - Catalog / storage (persistence, Delta features) beyond what Sparkless already provides.
  - SQL parsing and higher-level optimizer passes.

Sparkless can stay thinner by deferring more of this to Robin, or retain parts of the stack (e.g. catalog and some SQL) where backwards compatibility or ergonomics demand it.

### 3.4 Backwards compatibility and migration to a Robin-only engine

Assuming Sparkless v4.0.0 **drops multiple backends and standardizes on Robin-sparkless**, the open questions shift from “which backends to support” to “how to migrate safely”:

- How do we **deprecate and eventually remove** non-Robin backends (Polars, DuckDB, etc.) without surprising existing users?
- Which features are currently available only in non-Robin backends, and how do we:
  - Implement them (or close enough equivalents) in Robin.
  - Or explicitly document that they are no longer supported in v4.

This affects:

- Configuration flags (e.g. deprecating `SPARKLESS_BACKEND` / `SPARKLESS_ENGINE` in favor of Robin-specific options only).
- Expectations for power users who previously relied on selecting alternative backends or backend-specific behaviors.

---

## 4. Integration Strategies Considered

This section outlines what either project would likely need to change for each broad strategy, without committing to an implementation sequence.

### 4.1 Strategy 1: Sparkless builds a Robin-compatible plan and calls `_execute_plan`

**Concept:** Sparkless stays in charge of building a logical plan. Execution is delegated to Robin’s plan interpreter via a Python entry point.

Likely elements:

- In Sparkless:
  - Ensure the logical plan builder can emit all necessary operations and expressions that Robin supports.
  - Add an adapter that converts Sparkless’s internal representation into Robin’s JSON format.
  - Enhance/extend `LazyEvaluationEngine` and `BackendFactory` to route execution to a `RobinMaterializer` which:
    - Calls `_execute_plan(data, schema, plan_json)`.
    - Converts returned rows into Sparkless’s row objects/DataFrames.
- In Robin-sparkless:
  - Stabilize and document the plan format as a **public contract**, including versioning.
  - Possibly expose a cleaner, documented `_execute_plan`-style API for external engines.

Research observations:

- Robin already has robust plan tests and parity coverage; using its interpreter is attractive.
- Sparkless already has internal docs and code around a logical plan format; some of this overlaps with Robin’s design.

Open questions:

- How to handle operations that exist in Sparkless but not in Robin’s plan language.
- Whether we want to support multiple Robin plan versions or pin Sparkless v4 to a specific Robin major version.

### 4.2 Strategy 2: Sparkless wraps Robin’s DataFrame API directly

**Concept:** Embrace Robin’s Python DataFrame API under the hood, minimize separate Sparkless internals.

Likely elements:

- In Sparkless:
  - `SparkSession` would create a Robin `SparkSession` and hold a reference.
  - `sparkless.DataFrame` would:
    - Hold a reference to a `robin_sparkless.DataFrame`.
    - Methods like `select`, `filter`, `join` would mostly forward.
  - `sparkless.functions` would be thin wrappers around `robin_sparkless.functions`.
- In Robin-sparkless:
  - API might need some small adaptations to fit Sparkless’s existing semantics (e.g., error classes, some type behaviors).

Research observations:

- Robin’s Python API is already close to PySpark’s and is well-tested.
- This path risks **leaking Robin’s internal choices** (e.g., error types, edge-case semantics) into Sparkless in ways that might diverge from existing tests.

Open questions:

- How often do Sparkless and Robin disagree on subtle behavior today?
- Can we layer compatibility fixes on top of Robin’s DataFrame API, or does that reintroduce enough complexity that we are no longer “thin”?

### 4.3 Strategy 3: Shared logical plan library, dual front-ends

**Concept:** Treat the logical plan + interpreter as a shared library; Sparkless and Robin-sparkless are sibling front-ends.

Likely elements:

- Extract or consolidate the **logical plan and interpreter** into a place both projects can depend on (or treat Robin’s plan crate as that library).
- Sparkless would:
  - Contribute to the shared plan spec (operations, expressions).
  - Use that plan spec directly for its own lazy representation.
  - Call into the shared interpreter for execution.
- Robin-sparkless would:
  - Continue to be the canonical implementation of that interpreter (in Rust).

Research observations:

- Robin’s plan/interpreter is already Rust-first; Sparkless is Python-first.
- This strategy is more cross-project than “Sparkless wrapping Robin”, but it could be seen as the cleanest long-term architecture.

Open questions:

- Governance and versioning: who defines and evolves the plan spec?
- How much refactoring of both repos would be needed to converge on a single shared library?

---

## 5. Risks, Unknowns, and Open Research Topics

This section collects the main uncertainties surfaced during exploration.

### 5.1 Behavioral differences and parity guarantees

- Even with aligned tests, there may be **subtle differences**:
  - Null semantics, ordering, floating-point behavior.
  - Type coercion rules (e.g., int vs float, casting failures).
  - Error types and messages.
- If Sparkless promises that “v4 is a drop-in replacement for v3”, using Robin under the hood may force:
  - Extra compatibility layers on top of Robin, or
  - Explicitly documented behavior changes and updated tests.

### 5.2 Evolution of both codebases

- Both Sparkless and Robin-sparkless are actively evolving:
  - New functions, ops, and test cases.
  - Refactors of internal representations.
- Any deep integration (especially around a shared plan) would need a **clear versioning and compatibility story**:
  - E.g., Sparkless v4 supports Robin vX–Y and plan format vN.
  - Breakage across that boundary has to be carefully managed.

### 5.3 Debugging and developer experience

- For maintainers, “wrapper” architectures sometimes make debugging harder:
  - A failing test might involve Sparkless API, Sparkless plan, Robin plan, and Robin execution.
- A good developer story likely needs:
  - Tools to pretty-print plans at both levels.
  - Clear logging of “what we sent to Robin” and “what Robin returned”.
  - Ways to reproduce failures in isolation in the Robin repo using the same plan/fixture.

### 5.4 Migration path from multi-backend to Robin-only

- Making Sparkless “mostly a wrapper over Robin” and **eliminating alternative backends** raises practical migration questions:
  - How to stage deprecation of Polars/DuckDB backends (warnings, feature flags, eventual removal).
  - How to communicate and enforce that v4 always runs on Robin-sparkless under the hood.
- The migration must consider:
  - Existing users who rely on specific non-Robin backends or backend-specific quirks.
  - CI / environments where Robin’s Rust dependency is more complex to install than a pure-Python/Polars stack, and what tooling or documentation is needed to smooth that transition.

---

## 6. Tentative Conclusions (Non-Binding)

Based on the current research:

- There is a **strong conceptual fit** for Sparkless using Robin-sparkless as a primary execution engine:
  - Both projects chase PySpark parity with extensive shared fixtures.
  - Robin’s engine is well-suited to being reused for Sparkless workloads.
- The most **technically coherent “wrapper” story** seems to be:
  - Sparkless retains its public API and much of its lazy/planning layer.
  - A Robin backend consumes a **documented logical plan** (possibly via an adapter) and delegates execution to Robin’s interpreter.
  - Over time, the plan format could be converged or shared to reduce glue.
- However, to stay truly “thin” while preserving all existing behavior, Sparkless may still need:
  - Some compatibility shims around Robin’s semantics.
  - A clear **migration and deprecation story** for existing non-Robin backends and any behavior that previously depended on them.

These findings are meant as a foundation for future design discussions; they do **not** commit the project to a specific integration strategy or release scope.

