## Summary

[Parity] GroupedData.avg() should accept multiple column names, like PySpark's `df.groupBy("x").avg("a", "b")`.

## Expected (PySpark)

- `df.groupBy("dept").avg("salary", "bonus")` returns one row per dept with avg(salary) and avg(bonus).
- PySpark: OK (e.g. 2 rows, Row(dept='B', avg(salary)=150.0, avg(bonus)=15.0)).

## Actual (Robin 0.9.1)

- `TypeError: argument 'cols': Can't extract 'str' to 'Vec'` (or "GroupedData.avg() takes 1 positional arguments but 2 were given" in Sparkless tests).
- Robin's avg() appears to accept only a single column or a different signature.

## Repro

Run from sparkless repo root:

```bash
python scripts/robin_parity_repros_0.9.1/groupeddata_avg_multiple_cols.py
```

### Captured output (2026-02-13)

```
groupeddata_avg_multiple_cols: df.groupBy('dept').avg('salary', 'bonus')
Robin: FAILED ["groupBy.avg(multi): TypeError: argument 'cols': Can't extract `str` to `Vec`"]
PySpark: OK ["groupBy.avg(multi): 2 rows, e.g. Row(dept='B', avg(salary)=150.0, avg(bonus)=15.0)"]
```

## Affected Sparkless tests

- `tests/parity/dataframe/test_grouped_data_mean_parity.py::TestGroupedDataMeanParity::test_grouped_data_mean_multiple_columns`
