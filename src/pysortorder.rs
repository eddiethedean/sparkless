//! PyO3 wrapper for robin_sparkless::functions::SortOrder.
//! Used by orderBy(col.desc(), ...) / order_by_exprs.

use pyo3::prelude::*;
use robin_sparkless::functions::SortOrder;

/// Python wrapper for Robin's SortOrder (result of col.desc(), col.asc(), etc.).
#[pyclass]
pub struct PySortOrder {
    pub(crate) inner: SortOrder,
}

impl PySortOrder {
    pub fn from_robin(so: SortOrder) -> Self {
        Self { inner: so }
    }

    pub fn as_robin(&self) -> &SortOrder {
        &self.inner
    }
}

#[pymethods]
impl PySortOrder {
    fn __repr__(&self) -> String {
        format!("PySortOrder({:?})", self.inner)
    }
}
