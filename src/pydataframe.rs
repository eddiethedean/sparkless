//! PyO3 wrapper for robin_sparkless::DataFrame.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use robin_sparkless::dataframe::GroupedData as RobinGroupedData;
use robin_sparkless::dataframe::JoinType as RobinJoinType;
use robin_sparkless::DataFrame as RobinDataFrame;

use crate::pycolumn::{py_any_to_column, py_any_to_select_expr, PyColumn};
use crate::pysortorder::PySortOrder;

/// Python wrapper for Robin's DataFrame.
#[pyclass]
pub struct PyDataFrame {
    pub(crate) inner: RobinDataFrame,
}

impl PyDataFrame {
    pub fn from_robin(df: RobinDataFrame) -> Self {
        Self { inner: df }
    }

    pub fn as_robin(&self) -> &RobinDataFrame {
        &self.inner
    }
}

fn join_type_from_str(how: &str) -> PyResult<RobinJoinType> {
    match how.to_lowercase().as_str() {
        "inner" => Ok(RobinJoinType::Inner),
        "left" => Ok(RobinJoinType::Left),
        "right" => Ok(RobinJoinType::Right),
        "outer" | "full" => Ok(RobinJoinType::Outer),
        "left_semi" | "semi" => Ok(RobinJoinType::LeftSemi),
        "left_anti" | "anti" => Ok(RobinJoinType::LeftAnti),
        _ => Err(PyValueError::new_err(format!(
            "unsupported join type: {how}"
        ))),
    }
}

#[pymethods]
impl PyDataFrame {
    fn filter(&self, condition: &Bound<'_, PyAny>) -> PyResult<Self> {
        let col = py_any_to_column(condition)?;
        let expr = col.into_expr();
        self.inner
            .filter(expr)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("filter failed: {e}")))
    }

    fn select(&self, cols: &Bound<'_, PyList>) -> PyResult<Self> {
        let mut exprs = Vec::new();
        for item in cols.iter() {
            let c = py_any_to_select_expr(&item)?;
            exprs.push(c.into_expr());
        }
        self.inner
            .select_exprs(exprs)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("select failed: {e}")))
    }

    fn with_column(&self, name: &str, col: &Bound<'_, PyAny>) -> PyResult<Self> {
        let c = py_any_to_column(col)?;
        self.inner
            .with_column(name, &c)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("with_column failed: {e}")))
    }

    fn drop(&self, cols: Vec<String>) -> PyResult<Self> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        self.inner
            .drop(refs)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("drop failed: {e}")))
    }

    fn limit(&self, n: usize) -> PyResult<Self> {
        self.inner
            .limit(n)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("limit failed: {e}")))
    }

    fn order_by(&self, cols: Vec<&str>, ascending: Option<bool>) -> PyResult<Self> {
        let asc = ascending.unwrap_or(true);
        let n = cols.len();
        self.inner
            .order_by(cols, vec![asc; n])
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("order_by failed: {e}")))
    }

    /// Order by sort expressions (e.g. from F.desc(col), F.asc(col)). PySpark: orderBy(col("a").desc(), col("b").asc()).
    fn order_by_exprs(&self, sort_orders: &Bound<'_, PyList>) -> PyResult<Self> {
        let mut orders = Vec::with_capacity(sort_orders.len());
        for item in sort_orders.iter() {
            let py_so = item.downcast::<PySortOrder>()?;
            orders.push(py_so.borrow().as_robin().clone());
        }
        self.inner
            .order_by_exprs(orders)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("order_by_exprs failed: {e}")))
    }

    fn group_by(&self, cols: Vec<&str>) -> PyResult<PyGroupedData> {
        let gd = self.inner
            .group_by(cols)
            .map_err(|e| PyValueError::new_err(format!("group_by failed: {e}")))?;
        Ok(PyGroupedData { inner: gd })
    }

    fn join(
        &self,
        other: PyRef<'_, PyDataFrame>,
        on: Vec<&str>,
        how: Option<&str>,
    ) -> PyResult<Self> {
        let how_type = join_type_from_str(how.unwrap_or("inner"))?;
        self.inner
            .join(other.as_robin(), on, how_type)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("join failed: {e}")))
    }

    fn union(&self, other: PyRef<'_, PyDataFrame>) -> PyResult<Self> {
        self.inner
            .union(other.as_robin())
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("union failed: {e}")))
    }

    fn union_by_name(&self, other: PyRef<'_, PyDataFrame>, allow_missing_columns: Option<bool>) -> PyResult<Self> {
        self.inner
            .union_by_name(other.as_robin(), allow_missing_columns.unwrap_or(false))
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("union_by_name failed: {e}")))
    }

    fn distinct(&self) -> PyResult<Self> {
        self.inner
            .distinct(None)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("distinct failed: {e}")))
    }

    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let json_rows = self
            .inner
            .collect_as_json_rows()
            .map_err(|e| PyValueError::new_err(format!("collect failed: {e}")))?;
        crate::rows_to_py(py, json_rows)
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        use pyo3::types::PyDict;
        use pyo3::types::PyList;

        let st = self
            .inner
            .schema()
            .map_err(|e| PyValueError::new_err(format!("schema failed: {e}")))?;
        let schema_vec = crate::robin_schema_to_vec(&st);
        let list = PyList::empty(py);
        for (name_str, type_str) in schema_vec {
            let d = PyDict::new(py);
            d.set_item("name", name_str)?;
            d.set_item("type", type_str)?;
            list.append(d)?;
        }
        Ok(list.into_py(py))
    }
}

/// GroupedData: group_by result; supports agg and pivot.
#[pyclass]
pub struct PyGroupedData {
    pub(crate) inner: RobinGroupedData,
}

#[pymethods]
impl PyGroupedData {
    /// Apply aggregations (e.g. F.sum("a"), F.count("*")). Takes list of Column expressions.
    fn agg(&self, exprs: &Bound<'_, PyList>) -> PyResult<PyDataFrame> {
        let mut polars_exprs = Vec::with_capacity(exprs.len());
        for item in exprs.iter() {
            let py_col = item.downcast::<PyColumn>()?;
            polars_exprs.push(py_col.borrow().to_expr());
        }
        self.inner
            .agg(polars_exprs)
            .map(Self::from_robin_df)
            .map_err(|e| PyValueError::new_err(format!("agg failed: {e}")))
    }

    /// Pivot a column; returns PivotedGroupedData (call .sum(column) etc. on result).
    fn pivot(&self, pivot_col: &str, values: Option<Vec<String>>) -> PyPivotedGroupedData {
        let pivoted = self.inner.pivot(pivot_col, values);
        PyPivotedGroupedData { inner: pivoted }
    }
}

impl PyGroupedData {
    fn from_robin_df(df: RobinDataFrame) -> PyDataFrame {
        PyDataFrame::from_robin(df)
    }
}

/// PivotedGroupedData: result of groupBy(...).pivot(col); has .sum(), .avg(), etc.
#[pyclass]
pub struct PyPivotedGroupedData {
    pub(crate) inner: robin_sparkless::dataframe::PivotedGroupedData,
}

#[pymethods]
impl PyPivotedGroupedData {
    fn sum(&self, column: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sum(column)
            .map(PyDataFrame::from_robin)
            .map_err(|e| PyValueError::new_err(format!("pivot sum failed: {e}")))
    }

    fn avg(&self, column: &str) -> PyResult<PyDataFrame> {
        self.inner
            .avg(column)
            .map(PyDataFrame::from_robin)
            .map_err(|e| PyValueError::new_err(format!("pivot avg failed: {e}")))
    }

    fn min(&self, column: &str) -> PyResult<PyDataFrame> {
        self.inner
            .min(column)
            .map(PyDataFrame::from_robin)
            .map_err(|e| PyValueError::new_err(format!("pivot min failed: {e}")))
    }

    fn max(&self, column: &str) -> PyResult<PyDataFrame> {
        self.inner
            .max(column)
            .map(PyDataFrame::from_robin)
            .map_err(|e| PyValueError::new_err(format!("pivot max failed: {e}")))
    }

    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(PyDataFrame::from_robin)
            .map_err(|e| PyValueError::new_err(format!("pivot count failed: {e}")))
    }
}
