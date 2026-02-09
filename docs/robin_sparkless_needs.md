# What robin-sparkless needs (for Sparkless parity)

This document lists **upstream robin-sparkless** needs so that the Sparkless Robin backend can reach better parity with PySpark. It is based on the Robin materializer, parity test runs (`robin_parity_*_results.txt`), and existing docs.

**robin-sparkless 0.4.0** implements the main items below (select/with_column Column expressions, filter Column–Column, filter bool, lit date/datetime, Window API). Sparkless requires `robin-sparkless>=0.4.0` and uses the direct `with_column(name, expr)` and expression-aware `select()` APIs.

---

## 1. Column expressions in `select()` and `with_column()` (critical)

**Status:** Issue [#182](https://github.com/eddiethedean/robin-sparkless/issues/182). **Resolved in robin-sparkless 0.4.0.**

**Problem:** `DataFrame.select(*cols)` and `DataFrame.with_column(name, expr)` resolve arguments as **column names** (string lookup). When a Robin `Column` expression is passed (e.g. `F.col("a") * 2`, `F.lit(2) + F.col("x")`), the engine raises `RuntimeError: not found: (2 + x)` (or similar), i.e. it treats the string form of the expression as a column name.

**PySpark behavior:** Both `select()` and `withColumn()` accept `Column` expressions and evaluate them.

**What robin-sparkless should do:** Accept `Column` (expression) arguments in `select()` and `with_column()` and **evaluate** them, not resolve them by name. This single change would unblock:

- Select with expressions (alias, arithmetic, string/datetime functions, etc.)
- WithColumn with any translatable expression (already built in Sparkless; Robin rejects the result)

---

## 2. Filter: Column comparison and `lit()` type limits

### 2a. Column–Column comparisons in filter

**Status:** Issue [#184](https://github.com/eddiethedean/robin-sparkless/issues/184). **Resolved in robin-sparkless 0.4.0.**

**Observed:** `TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'` when the filter condition has a comparison between two columns (e.g. `col("a") > col("b")`). Sparkless translates both sides to Robin Columns and builds e.g. `left.gt(right)`; if Robin’s Column does not support `.gt(Column)` or the Python `>` operator between two Columns, this fails.

**What robin-sparkless should do:** Support comparisons where both sides are `Column` expressions (e.g. `col_a.gt(col_b)` or `col_a > col_b`), matching PySpark’s `filter()`.

### 2b. Filter condition must be Column, not bool

**Status:** Issue [#185](https://github.com/eddiethedean/robin-sparkless/issues/185). **Resolved in robin-sparkless 0.4.0.**

**Observed:** `TypeError: argument 'condition': 'bool' object cannot be converted to 'Column'` when Sparkless passes a Python `bool` (e.g. from evaluating a constant condition) to `df.filter(condition)`.

**What Sparkless can do:** Avoid passing raw `bool` to Robin’s `filter()` (normalize to a Column or skip the filter). Optionally robin-sparkless could accept a literal `True`/`False` and treat it as “no filter” / “filter none” for convenience.

### 2c. `lit()` supports only None, int, float, bool, str

**Observed:** `TypeError: lit() supports only None, int, float, bool, str` when Sparkless passes a date/datetime (or other type) in a literal used in a filter or expression.

**What Sparkless did:** `_lit_value_for_robin()` now coerces date/datetime to string (e.g. ISO) before calling `F.lit()`. So Sparkless side is adapted.

**What robin-sparkless could do (optional):** Extend `lit()` to accept date/datetime (or more types) for better parity and to avoid loss of type in expressions. Issue [#186](https://github.com/eddiethedean/robin-sparkless/issues/186). **Resolved in robin-sparkless 0.4.0.**

---

## 3. Expression resolution: “not found” for function-style expressions

When **select** (or withColumn) is used with expressions, Sparkless builds Robin `Column` objects and passes them to `select()` / `with_column()`. Because of (1), Robin then resolves by name and fails. In addition, the **string form** of the expression that Robin reports in “not found” often looks like a function call, e.g.:

- `not found: concat(first_name,  , last_name)`
- `not found: format_string('%s-%s', StringValue, IntegerValue)`
- `not found: split(StringValue, ,, 3)` / `split(Value, ,, -1)`
- `not found: substring(proc_date, 1, 10)`
- `not found: to_date(date_of_birth, 'yyyy-MM-dd')` / `to_date(event_date, 'MM/dd/yyyy')`

Fixing (1) so that Robin **evaluates** Column expressions in `select()` and `with_column()` would address these: Sparkless would pass the same Column objects, and Robin would evaluate them instead of resolving by name. No separate “support function X” is required for that path.

If robin-sparkless does add expression support but only for certain functions, then it would need at least:

- **String:** `concat`, `concat_ws`, `split` (with optional limit), `substring`/`substr`, `replace`, `format_string` (printf-style).
- **Datetime:** `to_date`, `to_timestamp` (with optional format string), `year`, `month`, `dayofmonth`, etc.
- **Struct/other:** `struct`, and any other expression types Sparkless emits.

---

## 4. Join: complex `on` and “join not supported”

**Observed:** Some parity tests fail with `SparkUnsupportedOperationError: Operation 'Operations: join' is not supported`. That can be due to:

- Join **on** being a non-trivial expression (e.g. array_contains or other expression) that Sparkless does not map to a list of column names, so `can_handle_join` returns False.
- Or robin-sparkless join API (e.g. `on=` only accepting a list of names, not a Column expression).

**Already filed:** Join `on=` accepting a single string for single-column join: [robin-sparkless #175](https://github.com/eddiethedean/robin-sparkless/issues/175).

**What robin-sparkless might need:** If Sparkless needs join-on-expression (e.g. `df1.join(df2, F.expr("a = b"))`), then Robin would need an API to accept a Column or expression for `on`. For name-only joins, ensuring `on="col"` and `on=["c1","c2"]` work is enough.

---

## 5. Window functions

**Observed:** Parity tests for window operations (row_number, rank, dense_rank, sum over window, lag, lead, etc.) are not supported by the Robin materializer; they raise `SparkUnsupportedOperationError` because the materializer does not implement window ops.

**What robin-sparkless would need (if we add window support in the materializer):** A Window / window-spec API and Column expressions that use it (e.g. `F.row_number().over(Window.partitionBy(...).orderBy(...))`), so that Sparkless can translate window expressions to Robin and call `select()` with them. This depends on (1) for select-with-expressions. Issue [#187](https://github.com/eddiethedean/robin-sparkless/issues/187). **Resolved in robin-sparkless 0.4.0.**

---

## 6. Other operations Sparkless currently does not send to Robin

- **groupBy + agg in the same plan:** Today Sparkless often materializes the DataFrame up to the groupBy and then runs aggregation in Python. If in the future groupBy appears in the materializer queue, Robin would need `group_by(...).agg(...)` (or equivalent); the existence of that API is already noted in the docs.
- **Array/collection functions:** explode, array_contains, size, etc. — would require both expression support in select/withColumn and Robin implementing those functions.
- **Regex / format_string / log / math:** Same as (3); once select/withColumn accept Column expressions, Robin only needs to implement the underlying functions (e.g. `format_string`, `log`, `sqrt`, `round`) in its Column API.

---

## 7. Stability and environment

- **Worker crashes (pytest-xdist):** Documented in [robin_mode_worker_crash_investigation.md](robin_mode_worker_crash_investigation.md); issue [#178](https://github.com/eddiethedean/robin-sparkless/issues/178) (fork-safety / stability). Running with fewer workers or serial avoids the issue; robin-sparkless could improve robustness under fork/multiprocess.

---

## Summary table (robin-sparkless upstream)

| Need | Priority | Issue / note |
|------|----------|--------------|
| **select() / with_column() accept and evaluate Column expressions** | Critical | #182 — **0.4.0** |
| **Filter: Column–Column comparison** (e.g. `col_a > col_b`) | High | #184 — **0.4.0** |
| **filter(condition): Column only or accept bool** | Low | #185 — **0.4.0** |
| **lit() extended to date/datetime (optional)** | Low | #186 — **0.4.0** |
| **Join on= single string** | Medium | #175 |
| **Window API** (if Sparkless adds window in materializer) | Future | #187 — **0.4.0** |
| **Stability under xdist/fork** | Medium | #178 |

**robin-sparkless 0.4.0** implements the critical and high-priority items above. Sparkless targets `robin-sparkless>=0.4.0` and uses `with_column(name, expr)` and expression-aware `select()` accordingly. The single most impactful change was **evaluating Column expressions in `select()` and `with_column()`**; most other “not found” and “select/withColumn not supported” failures in Sparkless would be addressed by that.
