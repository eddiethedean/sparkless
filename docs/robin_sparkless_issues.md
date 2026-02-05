# Issue Templates for robin-sparkless (Upstream)

The following text can be copied into the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) GitHub repo when opening issues. **Do not open issues automatically;** these are for maintainers to paste and adapt as needed.

---

## Sparkless integration note (no upstream feature request needed)

**Finding:** Robin-sparkless already provides what Sparkless needs:

- **Arbitrary schema:** Use `create_dataframe_from_rows(data, schema)` where `data` is a list of dicts or lists and `schema` is a list of `(column_name, dtype_str)` (e.g. `[("id", "bigint"), ("name", "string")]`). The 3-column restriction applies only to `create_dataframe()`.
- **Operations:** The DataFrame API already has `filter`, `select`, `with_column`, `order_by`, `order_by_exprs`, `group_by`, `limit`, `union`, `union_by_name`, `join`, and `GroupedData` (count, sum, avg, min, max, agg, etc.).

The gap is in **Sparkless**: our Robin materializer currently uses only `create_dataframe` (3-column) and supports only filter/select/limit. We will extend it to use `create_dataframe_from_rows` and to translate more operations to the existing robin-sparkless API. No upstream feature issues are required for “flexible schema” or “more operations.”

---

## Bug report template

**Title:** [Bug] Short description of the bug

**Body:**

**Description**  
[One or two sentences describing the incorrect behavior.]

**To reproduce**  
[Minimal code or steps, e.g. Sparkless snippet that calls robin_sparkless and triggers the bug.]

```python
# Example:
import robin_sparkless
# ...
```

**Expected behavior**  
[What you expect to happen.]

**Actual behavior**  
[What actually happens (error message, wrong result, etc.).]

**Environment**  
- Python version:  
- robin-sparkless version:  
- OS:  

**Additional context**  
[Optional: stack trace, logs, or links to Sparkless integration code.]
