use once_cell::sync::OnceCell;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use robin_sparkless::delta;
use robin_sparkless::plan;
use robin_sparkless::session::SparkSession as InnerSession;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

static GLOBAL_SESSION: OnceCell<InnerSession> = OnceCell::new();

fn get_or_create_session() -> InnerSession {
    GLOBAL_SESSION
        .get_or_init(|| {
            InnerSession::builder()
                .app_name("sparkless-robin")
                .get_or_create()
        })
        .clone()
}

fn py_to_json_value(value: &PyAny) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(JsonValue::Number(i.into()));
    }
    if let Ok(f) = value.extract::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(num));
        }
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    // Fallback: use string representation
    let s = value.str()?.to_string();
    Ok(JsonValue::String(s))
}

fn py_rows_to_json(
    py: Python<'_>,
    data: &PyAny,
    schema: &[(String, String)],
) -> PyResult<Vec<Vec<JsonValue>>> {
    // Expect a sequence of mapping-like rows; rely on PyO3's extract to handle lists/tuples.
    let rows: Vec<HashMap<String, PyObject>> = data.extract()?;
    let mut out: Vec<Vec<JsonValue>> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut vals: Vec<JsonValue> = Vec::with_capacity(schema.len());
        for (name, _) in schema {
            if let Some(obj) = row.get(name) {
                vals.push(py_to_json_value(obj.as_ref(py))?);
            } else {
                vals.push(JsonValue::Null);
            }
        }
        out.push(vals);
    }

    Ok(out)
}

/// Execute a Robin logical plan via the Rust crate.
///
/// Args:
///     data: list of row dicts or sequences.
///     schema: list of {'name': str, 'type': str} dicts.
///     plan_json: JSON string of the logical plan (LOGICAL_PLAN_FORMAT).
///
/// Returns:
///     list of dict rows.
#[pyfunction]
fn _execute_plan(py: Python<'_>, data: &PyAny, schema: &PyAny, plan_json: &str) -> PyResult<PyObject> {
    // Parse schema from Python into Vec<(name, type)>
    let raw_schema: Vec<HashMap<String, String>> = schema.extract().map_err(|_| {
        PyTypeError::new_err("schema must be a sequence of {'name': str, 'type': str} dicts")
    })?;
    let mut schema_vec: Vec<(String, String)> = Vec::with_capacity(raw_schema.len());
    for entry in raw_schema {
        let name = entry
            .get("name")
            .cloned()
            .ok_or_else(|| PyTypeError::new_err("schema dict missing 'name' key"))?;
        let dtype = entry
            .get("type")
            .cloned()
            .ok_or_else(|| PyTypeError::new_err("schema dict missing 'type' key"))?;
        schema_vec.push((name, dtype));
    }

    // Convert data rows to Vec<Vec<JsonValue>>
    let data_rows = py_rows_to_json(py, data, &schema_vec)?;

    // Parse plan JSON into Vec<Value>
    let plan_value: JsonValue = serde_json::from_str(plan_json).map_err(|e| {
        PyValueError::new_err(format!("failed to parse plan_json: {e}"))
    })?;
    let plan_array = plan_value.as_array().ok_or_else(|| {
        PyTypeError::new_err("plan_json must decode to a JSON array of plan steps")
    })?;

    let session = get_or_create_session();
    let df = plan::execute_plan(&session, data_rows, schema_vec, plan_array).map_err(|e| {
        PyValueError::new_err(format!("Robin execute_plan failed: {e}"))
    })?;

    let json_rows = df
        .collect_as_json_rows()
        .map_err(|e| PyValueError::new_err(format!("collect_as_json_rows failed: {e}")))?;

    rows_to_py(py, json_rows)
}

fn rows_to_py(py: Python<'_>, json_rows: Vec<HashMap<String, JsonValue>>) -> PyResult<PyObject> {
    let out = PyList::empty(py);
    for row in json_rows {
        let dict = PyDict::new(py);
        for (k, v) in row {
            let py_val = match v {
                JsonValue::Null => py.None(),
                JsonValue::Bool(b) => b.into_py(py),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        i.into_py(py)
                    } else if let Some(f) = n.as_f64() {
                        f.into_py(py)
                    } else {
                        n.to_string().into_py(py)
                    }
                }
                JsonValue::String(s) => s.into_py(py),
                JsonValue::Array(arr) => {
                    let inner: Vec<PyObject> = arr
                        .into_iter()
                        .map(|vv| match vv {
                            JsonValue::Null => py.None(),
                            JsonValue::Bool(b) => b.into_py(py),
                            JsonValue::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    i.into_py(py)
                                } else if let Some(f) = n.as_f64() {
                                    f.into_py(py)
                                } else {
                                    n.to_string().into_py(py)
                                }
                            }
                            JsonValue::String(s) => s.into_py(py),
                            other => other.to_string().into_py(py),
                        })
                        .collect();
                    PyList::new(py, inner).into_py(py)
                }
                JsonValue::Object(obj) => {
                    let sub = PyDict::new(py);
                    for (kk, vv) in obj {
                        let sub_val = match vv {
                            JsonValue::Null => py.None(),
                            JsonValue::Bool(b) => b.into_py(py),
                            JsonValue::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    i.into_py(py)
                                } else if let Some(f) = n.as_f64() {
                                    f.into_py(py)
                                } else {
                                    n.to_string().into_py(py)
                                }
                            }
                            JsonValue::String(s) => s.into_py(py),
                            other => other.to_string().into_py(py),
                        };
                        sub.set_item(kk, sub_val)?;
                    }
                    sub.into_py(py)
                }
            };
            dict.set_item(k, py_val)?;
        }
        out.append(dict)?;
    }
    Ok(out.into_py(py))
}

/// Execute a SQL query using the Robin SparkSession and return rows as list[dict].
#[pyfunction]
fn sql(py: Python<'_>, query: &str) -> PyResult<PyObject> {
    let session = get_or_create_session();
    let df = session
        .sql(query)
        .map_err(|e| PyValueError::new_err(format!("Robin SQL failed: {e}")))?;
    let json_rows = df
        .collect_as_json_rows()
        .map_err(|e| PyValueError::new_err(format!("collect_as_json_rows failed: {e}")))?;
    rows_to_py(py, json_rows)
}

/// Read a Delta table at path; returns list[dict] rows.
#[pyfunction]
fn read_delta(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let df = delta::read_delta(path, false).map_err(|e| {
        PyValueError::new_err(format!("Robin read_delta failed: {e}"))
    })?;
    let json_rows = df
        .collect_as_json_rows()
        .map_err(|e| PyValueError::new_err(format!("collect_as_json_rows failed: {e}")))?;
    rows_to_py(py, json_rows)
}

/// Read a Delta table at path at a specific version (time travel); returns list[dict] rows.
#[pyfunction]
fn read_delta_version(py: Python<'_>, path: &str, version: i64) -> PyResult<PyObject> {
    let df = delta::read_delta_with_version(path, Some(version), false).map_err(|e| {
        PyValueError::new_err(format!("Robin read_delta_version failed: {e}"))
    })?;
    let json_rows = df
        .collect_as_json_rows()
        .map_err(|e| PyValueError::new_err(format!("collect_as_json_rows failed: {e}")))?;
    rows_to_py(py, json_rows)
}

/// Build a Polars DataFrame from JSON rows and schema for write_delta.
fn json_rows_to_polars(
    data_rows: &[Vec<JsonValue>],
    schema: &[(String, String)],
) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
    use polars::prelude::*;

    if schema.is_empty() {
        return Ok(DataFrame::default());
    }

    let mut series_vec: Vec<Series> = Vec::with_capacity(schema.len());
    for (col_idx, (name, type_name)) in schema.iter().enumerate() {
        let dtype = match type_name.to_lowercase().as_str() {
            "string" => DataType::String,
            "int" | "integer" => DataType::Int32,
            "long" | "bigint" => DataType::Int64,
            "double" | "float" => DataType::Float64,
            "boolean" | "bool" => DataType::Boolean,
            "date" => DataType::Date,
            "timestamp" => DataType::Datetime(TimeUnit::Microseconds, None),
            _ => DataType::String,
        };

        let series = match dtype {
            DataType::String => {
                let vals: Vec<Option<String>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                Some(v.as_str().unwrap_or(&v.to_string()).to_string())
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
            DataType::Int32 => {
                let vals: Vec<Option<i32>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_i64().map(|n| n as i32)
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_i64()
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_f64()
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_bool()
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
            DataType::Date => {
                let vals: Vec<Option<i32>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_i64().map(|n| n as i32)
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals).cast(&DataType::Date)?
            }
            DataType::Datetime(_, _) => {
                let vals: Vec<Option<i64>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_i64()
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals).cast(&DataType::Datetime(TimeUnit::Microseconds, None))?
            }
            _ => {
                let vals: Vec<Option<String>> = data_rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                Some(v.to_string())
                            }
                        })
                    })
                    .collect();
                Series::new(name.as_str().into(), vals)
            }
        };
        series_vec.push(series);
    }

    let columns: Vec<polars::prelude::Column> = series_vec
        .into_iter()
        .map(polars::prelude::Column::from)
        .collect();
    Ok(DataFrame::new(columns)?)
}

/// Write rows to a Delta table at path. overwrite: true = replace table, false = append.
#[pyfunction]
fn write_delta(
    py: Python<'_>,
    data: &PyAny,
    schema: &PyAny,
    path: &str,
    overwrite: bool,
) -> PyResult<()> {
    let raw_schema: Vec<HashMap<String, String>> = schema.extract().map_err(|_| {
        PyTypeError::new_err("schema must be a sequence of {'name': str, 'type': str} dicts")
    })?;
    let mut schema_vec: Vec<(String, String)> = Vec::with_capacity(raw_schema.len());
    for entry in raw_schema {
        let name = entry
            .get("name")
            .cloned()
            .ok_or_else(|| PyTypeError::new_err("schema dict missing 'name' key"))?;
        let dtype = entry
            .get("type")
            .cloned()
            .ok_or_else(|| PyTypeError::new_err("schema dict missing 'type' key"))?;
        schema_vec.push((name, dtype));
    }

    let data_rows = py_rows_to_json(py, data, &schema_vec)?;
    let pl_df = json_rows_to_polars(&data_rows, &schema_vec).map_err(|e| {
        PyValueError::new_err(format!("build Polars DataFrame for write_delta: {e}"))
    })?;

    delta::write_delta(&pl_df, path, overwrite).map_err(|e| {
        PyValueError::new_err(format!("Robin write_delta failed: {e}"))
    })?;
    Ok(())
}

/// Python module definition: exposes low-level Robin helpers for Sparkless.
#[pymodule]
fn _robin(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_delta, m)?)?;
    m.add_function(wrap_pyfunction!(read_delta_version, m)?)?;
    m.add_function(wrap_pyfunction!(write_delta, m)?)?;
    m.add("__robin_version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

