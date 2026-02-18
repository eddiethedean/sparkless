use once_cell::sync::OnceCell;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyTuple};
use robin_sparkless::delta;
use robin_sparkless::plan;
use robin_sparkless::schema::{DataType as RobinDataType, StructType as RobinStructType};
use robin_sparkless::session::SparkSession as InnerSession;
use robin_sparkless::{DataFrame as RobinDataFrame, SaveMode};
use serde_json::Value as JsonValue;
use spark_ddl_parser::{parse_ddl_schema as parse_ddl_rs, StructType as DDLStructType};
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

/// Parse Python schema (list of {name, type}) into Vec<(String, String)>.
fn parse_schema_from_python(schema: &PyAny) -> PyResult<Vec<(String, String)>> {
    let raw_schema: Vec<HashMap<String, String>> = schema.extract().map_err(|_| {
        PyTypeError::new_err("schema must be a sequence of {'name': str, 'type': str} dicts")
    })?;
    let mut schema_vec = Vec::with_capacity(raw_schema.len());
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
    Ok(schema_vec)
}

/// Convert Robin StructType to schema vec (name, type_str) for Python.
fn robin_schema_to_vec(st: &RobinStructType) -> Vec<(String, String)> {
    st.fields()
        .iter()
        .map(|f| (f.name.clone(), robin_data_type_to_str(&f.data_type)))
        .collect()
}

fn robin_data_type_to_str(dt: &RobinDataType) -> String {
    match dt {
        RobinDataType::String => "string".to_string(),
        RobinDataType::Integer => "int".to_string(),
        RobinDataType::Long => "bigint".to_string(),
        RobinDataType::Double => "double".to_string(),
        RobinDataType::Boolean => "boolean".to_string(),
        RobinDataType::Date => "date".to_string(),
        RobinDataType::Timestamp => "timestamp".to_string(),
        RobinDataType::Array(inner) => format!("array<{}>", robin_data_type_to_str(inner)),
        RobinDataType::Map(k, v) => {
            format!("map<{},{}>", robin_data_type_to_str(k), robin_data_type_to_str(v))
        }
        RobinDataType::Struct(_) => "struct".to_string(),
    }
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
    let schema_vec = parse_schema_from_python(schema)?;
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

/// Register a temp view in Robin's session catalog (PySpark: createOrReplaceTempView).
#[pyfunction]
fn register_temp_view(py: Python<'_>, name: &str, data: &PyAny, schema: &PyAny) -> PyResult<()> {
    let schema_vec = parse_schema_from_python(schema)?;
    let data_rows = py_rows_to_json(py, data, &schema_vec)?;
    let session = get_or_create_session();
    let df = session
        .create_dataframe_from_rows(data_rows, schema_vec)
        .map_err(|e| PyValueError::new_err(format!("Robin create_dataframe_from_rows: {e}")))?;
    session.create_or_replace_temp_view(name, df);
    Ok(())
}

/// Register a global temp view in Robin's catalog (PySpark: createOrReplaceGlobalTempView).
#[pyfunction]
fn register_global_temp_view(py: Python<'_>, name: &str, data: &PyAny, schema: &PyAny) -> PyResult<()> {
    let schema_vec = parse_schema_from_python(schema)?;
    let data_rows = py_rows_to_json(py, data, &schema_vec)?;
    let session = get_or_create_session();
    let df = session
        .create_dataframe_from_rows(data_rows, schema_vec)
        .map_err(|e| PyValueError::new_err(format!("Robin create_dataframe_from_rows: {e}")))?;
    session.create_or_replace_global_temp_view(name, df);
    Ok(())
}

/// Get a table or view from Robin's catalog (PySpark: spark.table(name)). Returns (rows, schema).
#[pyfunction]
fn get_table(py: Python<'_>, name: &str) -> PyResult<PyObject> {
    let session = get_or_create_session();
    let df: RobinDataFrame = session
        .table(name)
        .map_err(|e| PyValueError::new_err(format!("Robin table({name}): {e}")))?;
    let json_rows = df
        .collect_as_json_rows()
        .map_err(|e| PyValueError::new_err(format!("collect_as_json_rows: {e}")))?;
    let schema = df
        .schema()
        .map_err(|e| PyValueError::new_err(format!("Robin schema(): {e}")))?;
    let schema_vec = robin_schema_to_vec(&schema);
    let rows_obj = rows_to_py(py, json_rows)?;
    let schema_list = PyList::empty(py);
    for (name_str, type_str) in schema_vec {
        let d = PyDict::new(py);
        d.set_item("name", name_str)?;
        d.set_item("type", type_str)?;
        schema_list.append(d)?;
    }
    Ok(PyTuple::new(py, [rows_obj, schema_list.into_py(py)]).into_py(py))
}

/// Save data as a table in Robin's catalog (PySpark: saveAsTable). mode: "overwrite"|"append"|"ignore"|"error"
#[pyfunction]
fn save_as_table(
    py: Python<'_>,
    name: &str,
    data: &PyAny,
    schema: &PyAny,
    mode: &str,
) -> PyResult<()> {
    let schema_vec = parse_schema_from_python(schema)?;
    let data_rows = py_rows_to_json(py, data, &schema_vec)?;
    let session = get_or_create_session();
    let df = session
        .create_dataframe_from_rows(data_rows, schema_vec)
        .map_err(|e| PyValueError::new_err(format!("Robin create_dataframe_from_rows: {e}")))?;
    let save_mode = match mode.to_lowercase().as_str() {
        "overwrite" => SaveMode::Overwrite,
        "append" => SaveMode::Append,
        "ignore" => SaveMode::Ignore,
        _ => SaveMode::ErrorIfExists,
    };
    df.write()
        .save_as_table(&session, name, save_mode)
        .map_err(|e| PyValueError::new_err(format!("Robin save_as_table: {e}")))?;
    Ok(())
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

fn ddl_field_to_py(py: Python<'_>, name: &str, data_type: &spark_ddl_parser::DataType) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("name", name)?;
    dict.set_item("data_type", ddl_data_type_to_py(py, data_type)?)?;
    Ok(dict.into_py(py))
}

fn ddl_data_type_to_py(py: Python<'_>, dt: &spark_ddl_parser::DataType) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match dt {
        spark_ddl_parser::DataType::Simple { type_name } => {
            dict.set_item("type", "simple")?;
            dict.set_item("type_name", type_name.as_str())?;
        }
        spark_ddl_parser::DataType::Decimal { precision, scale } => {
            dict.set_item("type", "decimal")?;
            dict.set_item("precision", *precision)?;
            dict.set_item("scale", *scale)?;
        }
        spark_ddl_parser::DataType::Array { element_type } => {
            dict.set_item("type", "array")?;
            dict.set_item("element_type", ddl_data_type_to_py(py, element_type)?)?;
        }
        spark_ddl_parser::DataType::Map {
            key_type,
            value_type,
        } => {
            dict.set_item("type", "map")?;
            dict.set_item("key_type", ddl_data_type_to_py(py, key_type)?)?;
            dict.set_item("value_type", ddl_data_type_to_py(py, value_type)?)?;
        }
        spark_ddl_parser::DataType::Struct(s) => {
            dict.set_item("type", "struct")?;
            let fields_list = PyList::empty(py);
            for f in &s.fields {
                fields_list.append(ddl_field_to_py(py, &f.name, &f.data_type)?)?;
            }
            dict.set_item("fields", fields_list)?;
        }
    }
    Ok(dict.into_py(py))
}

/// Parse a DDL schema string; returns list of {"name": str, "data_type": {...}} (nested for struct/array/map).
#[pyfunction]
fn parse_ddl_schema(py: Python<'_>, ddl: &str) -> PyResult<PyObject> {
    let parsed: DDLStructType = parse_ddl_rs(ddl)
        .map_err(|e| PyValueError::new_err(format!("DDL parse error: {e}")))?;
    let list = PyList::empty(py);
    for f in &parsed.fields {
        list.append(ddl_field_to_py(py, &f.name, &f.data_type)?)?;
    }
    Ok(list.into_py(py))
}

/// Python module definition: exposes low-level Robin helpers for Sparkless.
#[pymodule]
fn _robin(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_ddl_schema, m)?)?;
    m.add_function(wrap_pyfunction!(_execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_delta, m)?)?;
    m.add_function(wrap_pyfunction!(read_delta_version, m)?)?;
    m.add_function(wrap_pyfunction!(write_delta, m)?)?;
    m.add_function(wrap_pyfunction!(register_temp_view, m)?)?;
    m.add_function(wrap_pyfunction!(register_global_temp_view, m)?)?;
    m.add_function(wrap_pyfunction!(get_table, m)?)?;
    m.add_function(wrap_pyfunction!(save_as_table, m)?)?;
    m.add("__robin_version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

