//! PyO3 wrapper for robin_sparkless::column::Column.
//! PyColumn holds a Robin Column; all expression logic is delegated to Robin.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use robin_sparkless::column::Column as RobinColumn;
use robin_sparkless::functions;

/// Python wrapper for Robin's Column. Delegates all logic to Robin.
#[pyclass(subclass)]
pub struct PyColumn {
    pub(crate) inner: RobinColumn,
}

impl PyColumn {
    pub fn from_robin(col: RobinColumn) -> Self {
        Self { inner: col }
    }

    pub fn into_robin(self) -> RobinColumn {
        self.inner
    }

    pub fn as_robin(&self) -> &RobinColumn {
        &self.inner
    }

}

#[pymethods]
impl PyColumn {
    /// PySpark: col.alias(name)
    fn alias(&self, name: &str) -> Self {
        Self::from_robin(self.inner.clone().alias(name))
    }

    /// PySpark: col.cast(type_name)
    fn cast(&self, type_name: &str) -> PyResult<Self> {
        functions::cast(&self.inner, type_name)
            .map(Self::from_robin)
            .map_err(|e| PyValueError::new_err(format!("cast failed: {e}")))
    }

    fn is_null(&self) -> Self {
        Self::from_robin(self.inner.clone().is_null())
    }

    fn is_not_null(&self) -> Self {
        Self::from_robin(self.inner.clone().is_not_null())
    }

    /// PySpark: col.isnull()
    fn isnull(&self) -> Self {
        self.is_null()
    }

    /// PySpark: col.isnotnull()
    fn isnotnull(&self) -> Self {
        self.is_not_null()
    }

    /// PySpark: col.between(lower, upper). Inclusive: lower <= self <= upper.
    fn between(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<Self> {
        let lower_col = py_any_to_column(lower)?;
        let upper_col = py_any_to_column(upper)?;
        let ge_expr = self
            .inner
            .clone()
            .ge_pyspark(&lower_col)
            .expr()
            .clone();
        let le_expr = self
            .inner
            .clone()
            .le_pyspark(&upper_col)
            .expr()
            .clone();
        let combined = ge_expr.and(le_expr);
        Ok(Self::from_robin(RobinColumn::from_expr(combined, None)))
    }

    fn gt(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().gt_pyspark(&other_col)))
    }

    fn ge(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().ge_pyspark(&other_col)))
    }

    fn lt(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().lt_pyspark(&other_col)))
    }

    fn le(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().le_pyspark(&other_col)))
    }

    fn eq(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().eq_pyspark(&other_col)))
    }

    fn ne(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        Ok(Self::from_robin(self.inner.clone().ne_pyspark(&other_col)))
    }

    fn and_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        let combined = self
            .inner
            .expr()
            .clone()
            .and(other_col.expr().clone());
        Ok(Self::from_robin(RobinColumn::from_expr(combined, None)))
    }

    fn or_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let other_col = py_any_to_column(other)?;
        let combined = self
            .inner
            .expr()
            .clone()
            .or(other_col.expr().clone());
        Ok(Self::from_robin(RobinColumn::from_expr(combined, None)))
    }

    fn isin_(&self, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let list = values.downcast::<pyo3::types::PyList>()
            .map_err(|_| PyValueError::new_err("isin requires a list of values"))?;
        if list.is_empty() {
            return Err(PyValueError::new_err("isin requires a non-empty list"));
        }
        // Try i64 first
        let i64_vals: Result<Vec<i64>, _> = list.iter().map(|v| v.extract::<i64>()).collect();
        if let Ok(v) = i64_vals {
            return Ok(Self::from_robin(functions::isin_i64(&self.inner, &v)));
        }
        // Try str
        let str_vals: Result<Vec<String>, _> = list.iter().map(|v| v.extract::<String>()).collect();
        if let Ok(v) = str_vals {
            let refs: Vec<&str> = v.iter().map(String::as_str).collect();
            return Ok(Self::from_robin(functions::isin_str(&self.inner, &refs)));
        }
        Err(PyValueError::new_err(
            "isin requires a list of int or str values",
        ))
    }

    /// Python operator: col > other
    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.gt(other)
    }

    /// Python operator: col >= other
    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.ge(other)
    }

    /// Python operator: col < other
    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.lt(other)
    }

    /// Python operator: col <= other
    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.le(other)
    }

    /// Python operator: col == other
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.eq(other)
    }

    /// Python operator: col != other
    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.ne(other)
    }

    /// Python operator: col & other
    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.and_(other)
    }

    /// Python operator: col | other
    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.or_(other)
    }
}

/// Convert PyAny to column/expression for select. Str = col(name), PyColumn = as-is.
pub fn py_any_to_select_expr(obj: &Bound<'_, PyAny>) -> PyResult<RobinColumn> {
    if let Ok(py_col) = obj.extract::<PyRef<'_, PyColumn>>() {
        return Ok(py_col.as_robin().clone());
    }
    if let Ok(name) = obj.extract::<String>() {
        return Ok(functions::col(&name));
    }
    Err(PyValueError::new_err(
        "select expects Column or str".to_string(),
    ))
}

/// Convert PyAny to Robin Column (expression context). Handles PyColumn, str (as literal), int, float, bool, None.
pub fn py_any_to_column(obj: &Bound<'_, PyAny>) -> PyResult<RobinColumn> {
    if let Ok(py_col) = obj.extract::<PyRef<'_, PyColumn>>() {
        return Ok(py_col.as_robin().clone());
    }
    if obj.is_none() {
        return Ok(RobinColumn::null_boolean());
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(functions::lit_i64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(functions::lit_f64(f));
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(functions::lit_bool(b));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(functions::lit_str(&s));
    }
    // Python datetime.date or datetime.datetime: use isoformat() then lit_str
    if obj.hasattr("isoformat").unwrap_or(false) {
        if let Ok(iso) = obj.call_method0("isoformat") {
            if let Ok(s) = iso.extract::<String>() {
                return Ok(functions::lit_str(&s));
            }
        }
    }
    // Python tuple: convert to string representation for literal (e.g. "(1, 'a')")
    if let Ok(tup) = obj.downcast::<pyo3::types::PyTuple>() {
        let parts: Vec<String> = tup
            .iter()
            .map(|v| {
                if v.is_none() {
                    "None".to_string()
                } else if let Ok(s) = v.extract::<String>() {
                    format!("'{s}'")
                } else if let Ok(i) = v.extract::<i64>() {
                    i.to_string()
                } else if let Ok(f) = v.extract::<f64>() {
                    f.to_string()
                } else if let Ok(b) = v.extract::<bool>() {
                    b.to_string()
                } else {
                    "?".to_string()
                }
            })
            .collect();
        let repr = format!("({})", parts.join(", "));
        return Ok(functions::lit_str(&repr));
    }
    Err(PyValueError::new_err("cannot convert to Column".to_string()))
}
