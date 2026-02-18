//! PyO3 wrapper for robin_sparkless::session::SparkSession.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use robin_sparkless::session::SparkSession as InnerSession;
use robin_sparkless::DataFrame as RobinDataFrame;

use crate::pydataframe::PyDataFrame;

/// Builder for PySparkSession (PySpark: SparkSession.builder()).
#[pyclass]
pub struct PySparkSessionBuilder {
    app_name: Option<String>,
}

#[pymethods]
impl PySparkSessionBuilder {
    #[new]
    fn new() -> Self {
        Self { app_name: None }
    }

    fn app_name<'a>(mut self_: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        self_.app_name = Some(name.to_string());
        self_
    }

    fn get_or_create(self_: PyRef<'_, Self>) -> PyResult<PySparkSession> {
        let session = InnerSession::builder()
            .app_name(self_.app_name.as_deref().unwrap_or("sparkless"))
            .get_or_create();
        Ok(PySparkSession {
            inner: session,
        })
    }

    /// Alias for get_or_create (PySpark naming).
    fn getOrCreate(self_: PyRef<'_, Self>) -> PyResult<PySparkSession> {
        Self::get_or_create(self_)
    }
}

/// Python wrapper for Robin's SparkSession.
#[pyclass]
pub struct PySparkSession {
    pub(crate) inner: InnerSession,
}

#[pymethods]
impl PySparkSession {
    /// SparkSession.builder()
    #[classmethod]
    fn builder(_cls: &Bound<'_, pyo3::types::PyType>) -> PySparkSessionBuilder {
        PySparkSessionBuilder {
            app_name: None,
        }
    }

    /// createDataFrame(data, schema=None). Schema must be list of {"name": str, "type": str}.
    fn create_data_frame(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let schema_vec = schema
            .map(|s| crate::parse_schema_from_python(s.as_ref()))
            .transpose()?
            .ok_or_else(|| PyValueError::new_err("createDataFrame requires schema"))?;
        let data_rows = crate::py_rows_to_json(py, data.as_ref(), &schema_vec)?;
        let df = self
            .inner
            .create_dataframe_from_rows(data_rows, schema_vec)
            .map_err(|e| PyValueError::new_err(format!("create_dataframe_from_rows failed: {e}")))?;
        Ok(PyDataFrame::from_robin(df))
    }

    /// Alias for create_data_frame (PySpark: createDataFrame).
    fn createDataFrame(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.create_data_frame(py, data, schema)
    }

    /// Execute SQL query; returns DataFrame.
    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df: RobinDataFrame = self
            .inner
            .sql(query)
            .map_err(|e| PyValueError::new_err(format!("SQL failed: {e}")))?;
        Ok(PyDataFrame::from_robin(df))
    }

    /// Get table/view by name.
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let df: RobinDataFrame = self
            .inner
            .table(name)
            .map_err(|e| PyValueError::new_err(format!("table({name}) failed: {e}")))?;
        Ok(PyDataFrame::from_robin(df))
    }

    /// Read Parquet file(s) at path.
    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let df: RobinDataFrame = self
            .inner
            .read_parquet(path)
            .map_err(|e| PyValueError::new_err(format!("read_parquet failed: {e}")))?;
        Ok(PyDataFrame::from_robin(df))
    }

    /// Read CSV file(s) at path.
    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let df: RobinDataFrame = self
            .inner
            .read_csv(path)
            .map_err(|e| PyValueError::new_err(format!("read_csv failed: {e}")))?;
        Ok(PyDataFrame::from_robin(df))
    }
}
