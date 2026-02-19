//! PyO3 bindings for Robin functions (F.col, F.lit, concat, upper, etc.).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use robin_sparkless::column::Column as RobinColumn;
use robin_sparkless::functions;

use crate::pycolumn::{py_any_to_column, py_any_to_select_expr, PyColumn};
use crate::pysortorder::PySortOrder;

fn cols_from_list(list: &Bound<'_, PyList>) -> PyResult<Vec<RobinColumn>> {
    let mut cols = Vec::with_capacity(list.len());
    for item in list.iter() {
        let c = py_any_to_select_expr(&item)?;
        cols.push(c);
    }
    Ok(cols)
}

fn col_refs(cols: &[RobinColumn]) -> Vec<&RobinColumn> {
    cols.iter().collect()
}

/// F.col(name) - column reference by name.
#[pyfunction]
pub fn col(name: &str) -> PyColumn {
    PyColumn::from_robin(functions::col(name))
}

/// F.lit(value) - literal column. Supports int, float, str, bool, None.
#[pyfunction]
pub fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = crate::pycolumn::py_any_to_column(value)?;
    Ok(PyColumn::from_robin(c))
}

// --- String ---

/// F.concat(*cols) - concatenate string columns.
#[pyfunction]
pub fn concat(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("concat requires at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    Ok(PyColumn::from_robin(functions::concat(&refs)))
}

/// F.concat_ws(separator, *cols)
#[pyfunction]
pub fn concat_ws(separator: &str, cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("concat_ws requires at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    Ok(PyColumn::from_robin(functions::concat_ws(separator, &refs)))
}

/// F.upper(col)
#[pyfunction]
pub fn upper(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::upper(&c)))
}

/// F.lower(col)
#[pyfunction]
pub fn lower(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::lower(&c)))
}

/// F.trim(col)
#[pyfunction]
pub fn trim(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::trim(&c)))
}

/// F.substring(col, start, length=None). start is 1-based.
#[pyfunction]
#[pyo3(signature = (col, start, length=None))]
pub fn substring(col: &Bound<'_, PyAny>, start: i64, length: Option<i64>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::substring(&c, start, length)))
}

/// F.length(col)
#[pyfunction]
pub fn length(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::length(&c)))
}

/// F.regexp_extract(col, pattern, group_index=0)
#[pyfunction]
#[pyo3(signature = (col, pattern, group_index=0))]
pub fn regexp_extract(col: &Bound<'_, PyAny>, pattern: &str, group_index: usize) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::regexp_extract(&c, pattern, group_index)))
}

/// F.regexp_replace(col, pattern, replacement)
#[pyfunction]
pub fn regexp_replace(col: &Bound<'_, PyAny>, pattern: &str, replacement: &str) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::regexp_replace(&c, pattern, replacement)))
}

/// F.split(col, delimiter, limit=None)
#[pyfunction]
#[pyo3(signature = (col, delimiter, limit=None))]
pub fn split(col: &Bound<'_, PyAny>, delimiter: &str, limit: Option<i32>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::split(&c, delimiter, limit)))
}

/// F.lpad(col, length, pad)
#[pyfunction]
pub fn lpad(col: &Bound<'_, PyAny>, length: i32, pad: &str) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::lpad(&c, length, pad)))
}

/// F.rpad(col, length, pad)
#[pyfunction]
pub fn rpad(col: &Bound<'_, PyAny>, length: i32, pad: &str) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::rpad(&c, length, pad)))
}

/// F.contains(col, substring)
#[pyfunction]
pub fn contains(col: &Bound<'_, PyAny>, substring: &str) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::contains(&c, substring)))
}

/// F.like(col, pattern, escape_char=None)
#[pyfunction]
#[pyo3(signature = (col, pattern, escape_char=None))]
pub fn like(col: &Bound<'_, PyAny>, pattern: &str, escape_char: Option<char>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::like(&c, pattern, escape_char)))
}

/// F.format_string(format, *cols)
#[pyfunction]
pub fn format_string(format: &str, cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("format_string needs at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    Ok(PyColumn::from_robin(functions::format_string(format, &refs)))
}

/// F.array(*cols) - create array column from columns. Empty list = column of empty arrays.
#[pyfunction]
pub fn array(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    functions::array(&refs)
        .map(PyColumn::from_robin)
        .map_err(|e| PyValueError::new_err(format!("array failed: {e}")))
}

/// F.create_map(*key_value_cols) - alternating key, value columns. Empty = column of empty maps.
#[pyfunction]
pub fn create_map(key_values: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(key_values)?;
    if !robin_cols.is_empty() && robin_cols.len() % 2 != 0 {
        return Err(PyValueError::new_err(
            "create_map requires an even number of columns (key, value pairs)",
        ));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    functions::create_map(&refs)
        .map(PyColumn::from_robin)
        .map_err(|e| PyValueError::new_err(format!("create_map failed: {e}")))
}

// --- Math ---

/// F.abs(col)
#[pyfunction]
pub fn abs(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::abs(&c)))
}

/// F.ceil(col)
#[pyfunction]
pub fn ceil(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::ceil(&c)))
}

/// F.floor(col)
#[pyfunction]
pub fn floor(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::floor(&c)))
}

/// F.round(col, decimals=0)
#[pyfunction]
#[pyo3(signature = (col, decimals=0))]
pub fn round(col: &Bound<'_, PyAny>, decimals: u32) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::round(&c, decimals)))
}

/// F.sqrt(col)
#[pyfunction]
pub fn sqrt(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::sqrt(&c)))
}

/// F.pow(col, exp) / F.power(col, exp)
#[pyfunction]
pub fn pow(col: &Bound<'_, PyAny>, exp: i64) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::pow(&c, exp)))
}

/// F.exp(col)
#[pyfunction]
pub fn exp(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::exp(&c)))
}

/// F.log(col) - natural log
#[pyfunction]
pub fn log(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::log(&c)))
}

// --- Datetime ---

/// F.year(col)
#[pyfunction]
pub fn year(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::year(&c)))
}

/// F.month(col)
#[pyfunction]
pub fn month(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::month(&c)))
}

/// F.day(col)
#[pyfunction]
pub fn day(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::day(&c)))
}

/// F.hour(col)
#[pyfunction]
pub fn hour(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::hour(&c)))
}

/// F.minute(col)
#[pyfunction]
pub fn minute(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::minute(&c)))
}

/// F.second(col)
#[pyfunction]
pub fn second(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::second(&c)))
}

/// F.to_date(col, format=None)
#[pyfunction]
#[pyo3(signature = (col, format=None))]
pub fn to_date(col: &Bound<'_, PyAny>, format: Option<&str>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    functions::to_date(&c, format)
        .map(PyColumn::from_robin)
        .map_err(|e| PyValueError::new_err(format!("to_date failed: {e}")))
}

/// F.date_format(col, format)
#[pyfunction]
pub fn date_format(col: &Bound<'_, PyAny>, format: &str) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::date_format(&c, format)))
}

/// F.current_timestamp()
#[pyfunction]
pub fn current_timestamp() -> PyColumn {
    PyColumn::from_robin(functions::current_timestamp())
}

/// F.date_add(col, n)
#[pyfunction]
pub fn date_add(col: &Bound<'_, PyAny>, n: i32) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::date_add(&c, n)))
}

/// F.date_sub(col, n)
#[pyfunction]
pub fn date_sub(col: &Bound<'_, PyAny>, n: i32) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::date_sub(&c, n)))
}

// --- Conditional ---

/// F.when(condition).then(then_val).otherwise(else_val) - single call convenience.
#[pyfunction]
pub fn when_otherwise(
    condition: &Bound<'_, PyAny>,
    then_val: &Bound<'_, PyAny>,
    else_val: &Bound<'_, PyAny>,
) -> PyResult<PyColumn> {
    let cond = py_any_to_column(condition)?;
    let then_col = py_any_to_column(then_val)?;
    let else_col = py_any_to_column(else_val)?;
    let w = functions::when(&cond);
    let result = w.then(&then_col).otherwise(&else_col);
    Ok(PyColumn::from_robin(result))
}

/// F.when(condition) - start when-then-otherwise chain. Returns builder with .then() and .otherwise().
#[pyclass]
pub struct PyWhenBuilder {
    condition: RobinColumn,
}

#[pymethods]
impl PyWhenBuilder {
    /// .then(value) -> PyThenBuilder
    fn then(&self, value: &Bound<'_, PyAny>) -> PyResult<PyThenBuilder> {
        let val_col = py_any_to_column(value)?;
        Ok(PyThenBuilder {
            condition: self.condition.clone(),
            then_val: val_col,
        })
    }
}

#[pyclass]
pub struct PyThenBuilder {
    condition: RobinColumn,
    then_val: RobinColumn,
}

#[pymethods]
impl PyThenBuilder {
    /// .otherwise(value) -> PyColumn
    fn otherwise(&self, value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let else_col = py_any_to_column(value)?;
        let w = functions::when(&self.condition);
        let result = w.then(&self.then_val).otherwise(&else_col);
        Ok(PyColumn::from_robin(result))
    }
}

/// F.when(condition) - start when-then-otherwise chain.
#[pyfunction]
pub fn when(condition: &Bound<'_, PyAny>) -> PyResult<PyWhenBuilder> {
    let cond = py_any_to_column(condition)?;
    Ok(PyWhenBuilder { condition: cond })
}

/// F.coalesce(*cols)
#[pyfunction]
pub fn coalesce(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("coalesce requires at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    Ok(PyColumn::from_robin(functions::coalesce(&refs)))
}

/// F.greatest(*cols)
#[pyfunction]
pub fn greatest(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("greatest requires at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    functions::greatest(&refs)
        .map(PyColumn::from_robin)
        .map_err(|e| PyValueError::new_err(e))
}

/// F.least(*cols)
#[pyfunction]
pub fn least(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let robin_cols: Vec<RobinColumn> = cols_from_list(cols)?;
    if robin_cols.is_empty() {
        return Err(PyValueError::new_err("least requires at least one column"));
    }
    let refs: Vec<&RobinColumn> = col_refs(&robin_cols);
    functions::least(&refs)
        .map(PyColumn::from_robin)
        .map_err(|e| PyValueError::new_err(e))
}

// --- Aggregation ---

/// F.sum(col)
#[pyfunction]
pub fn sum(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::sum(&c)))
}

/// F.count(col)
#[pyfunction]
pub fn count(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::count(&c)))
}

/// F.avg(col)
#[pyfunction]
pub fn avg(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::avg(&c)))
}

/// F.min(col)
#[pyfunction]
pub fn min(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::min(&c)))
}

/// F.max(col)
#[pyfunction]
pub fn max(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::max(&c)))
}

/// F.count_distinct(col)
#[pyfunction]
pub fn count_distinct(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::count_distinct(&c)))
}

/// F.mean(col) - alias for avg
#[pyfunction]
pub fn mean(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    avg(col)
}

/// F.power(col, exp) - alias for pow
#[pyfunction]
pub fn power(col: &Bound<'_, PyAny>, exp: i64) -> PyResult<PyColumn> {
    pow(col, exp)
}

/// F.first(col) - first value (aggregate). ignorenulls optional.
#[pyfunction]
#[pyo3(signature = (col, ignorenulls=true))]
pub fn first(col: &Bound<'_, PyAny>, ignorenulls: bool) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(col)?;
    Ok(PyColumn::from_robin(functions::first(&c, ignorenulls)))
}

/// F.rank(col, descending=false) - window rank function.
#[pyfunction]
#[pyo3(signature = (column, descending=false))]
pub fn rank(column: &Bound<'_, PyAny>, descending: bool) -> PyResult<PyColumn> {
    let c = py_any_to_select_expr(column)?;
    Ok(PyColumn::from_robin(functions::rank(&c, descending)))
}

// -----------------------------------------------------------------------------
// Sort order (col.desc(), col.asc(), etc.) for order_by_exprs
// -----------------------------------------------------------------------------

/// F.desc(col) - sort descending, nulls last (Spark default for DESC).
#[pyfunction]
pub fn desc(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::desc(&c)))
}

/// F.asc(col) - sort ascending, nulls first (Spark default for ASC).
#[pyfunction]
pub fn asc(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::asc(&c)))
}

/// F.desc_nulls_last(col) - sort descending, nulls last.
#[pyfunction]
pub fn desc_nulls_last(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::desc_nulls_last(&c)))
}

/// F.asc_nulls_last(col) - sort ascending, nulls last.
#[pyfunction]
pub fn asc_nulls_last(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::asc_nulls_last(&c)))
}

/// F.desc_nulls_first(col) - sort descending, nulls first.
#[pyfunction]
pub fn desc_nulls_first(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::desc_nulls_first(&c)))
}

/// F.asc_nulls_first(col) - sort ascending, nulls first.
#[pyfunction]
pub fn asc_nulls_first(column: &Bound<'_, PyAny>) -> PyResult<PySortOrder> {
    let c = py_any_to_select_expr(column)?;
    Ok(PySortOrder::from_robin(functions::asc_nulls_first(&c)))
}
