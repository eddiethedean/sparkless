"""Tests for SQL CTE and JOIN robustness (issue #354).

Table-prefixed columns in SELECT (e.g. d.dept_name) must resolve against
the join result schema (prefixed column names like d_dept_name).
"""


def test_cte_with_join(spark) -> None:
    """CTE with JOIN: table-prefixed columns in SELECT resolve correctly.

    When a SELECT has a JOIN, columns are renamed with table alias prefix
    (e.g. e_id, d_dept_name). The SELECT clause may reference them with
    table prefix (e.id, d.dept_name). Resolution must use the joined
    DataFrame's schema so these resolve to e_id, d_dept_name.
    """
    try:
        # Employees: id, name, dept_id
        employees = spark.createDataFrame(
            [(1, "Alice", 10), (2, "Bob", 20), (3, "Carol", 10)],
            ["id", "name", "dept_id"],
        )
        employees.write.mode("overwrite").saveAsTable("employees")

        # Departments: id, dept_name
        departments = spark.createDataFrame(
            [(10, "Engineering"), (20, "Sales")],
            ["id", "dept_name"],
        )
        departments.write.mode("overwrite").saveAsTable("departments")

        # SELECT with table-prefixed columns after JOIN; resolution must use
        # join result schema (e_id, e_name, e_dept_id, d_id, d_dept_name).
        result = spark.sql(
            "SELECT e.id, e.name, d.dept_name FROM employees e "
            "JOIN departments d ON e.dept_id = d.id"
        )

        rows = result.collect()
        assert len(rows) == 3

        # Executor renames join columns with alias prefix; SELECT projects
        # those, so result columns are e_id, e_name, d_dept_name (or id, name,
        # dept_name if we alias; we don't alias here so prefixed names).
        assert "e_id" in result.columns or "id" in result.columns
        assert "e_name" in result.columns or "name" in result.columns
        assert "d_dept_name" in result.columns or "dept_name" in result.columns

        # Check content: Alice and Carol in Engineering, Bob in Sales.
        # Use row[key] (PySpark-compatible); PySpark Row has no .get() method.
        def _val(r, *keys):
            for k in keys:
                if k in result.columns:
                    return r[k]
            return None
        by_name = {_val(r, "e_name", "name"): _val(r, "d_dept_name", "dept_name") for r in rows}
        assert by_name["Alice"] == "Engineering"
        assert by_name["Bob"] == "Sales"
        assert by_name["Carol"] == "Engineering"
    finally:
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS departments")
