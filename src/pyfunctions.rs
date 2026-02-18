//! PyO3 bindings for Robin functions (F.col, F.lit, etc.).

use pyo3::prelude::*;
use robin_sparkless::functions;

use crate::pycolumn::PyColumn;

/// F.col(name) - column reference by name.
#[pyfunction]
pub fn col(name: &str) -> PyColumn {
    PyColumn::from_robin(functions::col(name))
}

/// F.lit(value) - literal column. Supports int, float, str, bool.
#[pyfunction]
pub fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = crate::pycolumn::py_any_to_column(value)?;
    Ok(PyColumn::from_robin(c))
}
