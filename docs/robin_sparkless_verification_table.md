# Robin-sparkless verification table (plan: limitations and parity)

Generated from inspecting [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) source (clone) and Sparkless test failures.

| Category | Present in robin-sparkless | Notes |
|----------|----------------------------|--------|
| **Window / window expressions** | Yes | `tests/python/test_window_pyspark_parity.py`: row_number(), rank(), dense_rank(), lag, lead over partition/order. Rust/API has Window support (#187). Sparkless fails with "not found: row_number() OVER (...)" because we send expression string or materializer does not translate window to Robin API. |
| **CaseWhen (when/otherwise)** | Yes | `test_col_lit_when`, `test_lit_date_and_datetime_in_when`: `rs.when(rs.col("x").gt(rs.lit(15))).then(...).otherwise(...)`. Sparkless fails with "not found: CASE WHEN ..." — we may be passing string form; #182 (select evaluate expressions) would address if we pass Column. |
| **concat / concat_ws** | Yes | `src/functions.rs`: `concat(columns: &[&Column])`, `concat_ws(separator: &str, columns: &[&Column])`. concat_ws accepts literal separator. Failure "not found: concat(first_name,  , last_name)" indicates name resolution (#182); using concat_ws(" ", first_name, last_name) is the right Robin API. |
| **Case sensitivity** | Configurable | `SparkSession.is_case_sensitive()` in .pyi; `src/sql/translator.rs` uses `session.is_case_sensitive()`. Default/behavior may differ from PySpark. |
| **lit() types** | Extended in 0.4.0 | #186: date/datetime in lit. Doc says 0.4.0; tests show `lit(date)`, `lit(datetime)`. |
| **Filter Column–Column, filter(bool)** | Yes (0.4.0) | #184, #185 resolved in 0.4.0. |
| **Select/with_column expression evaluation** | Yes (0.4.0) | #182: select/with_column evaluate Column expressions. |
| **substring / substr** | Yes | Sparkless materializer uses F.substring / F.substr; "not found: partial" may be from alias or different call shape. |
| **getItem** | Yes | Phase 7 in Sparkless; Robin has column[key] for array/map. |
| **map() / array() / nested struct** | Partial / different | Robin row values: scalar types; map/array in plan may differ. |

**Conclusion:** Many failures are due to (1) expression evaluation in select/with_column (#182 — resolved in 0.4.0; if Sparkless uses Column objects, Robin should evaluate), (2) Sparkless not translating to Robin’s API (e.g. window, when/then/otherwise), or (3) behavioral differences (case sensitivity, type strictness). New issues should focus on: **window expression translation** (if Robin’s select receives window expr and still fails), **concat with literal in middle** (concat_ws usage or Robin concat accepting lit()), **case sensitivity** (default vs PySpark), **unsupported filter/join** (complex conditions), and **parity-only** (result/order differences).
