use crate::cli::InputFormatArg;
use crate::error::{LoadKeelError, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime};
use indexmap::{IndexMap, IndexSet};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Date(String),
    Timestamp(String),
    String(String),
    Array(Vec<Value>),
    Object(IndexMap<String, Value>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferredType {
    Bool,
    Int,
    Float,
    Date,
    Timestamp,
    Text,
    Jsonb,
}

impl InferredType {
    pub fn pg_type(self) -> &'static str {
        match self {
            Self::Bool => "boolean",
            Self::Int => "bigint",
            Self::Float => "double precision",
            Self::Date => "date",
            Self::Timestamp => "timestamptz",
            Self::Text => "text",
            Self::Jsonb => "jsonb",
        }
    }

    pub fn unify(self, other: Self) -> Self {
        if self == other {
            return self;
        }
        match (self, other) {
            (Self::Int, Self::Float) | (Self::Float, Self::Int) => Self::Float,
            (Self::Date, Self::Timestamp) | (Self::Timestamp, Self::Date) => Self::Timestamp,
            (Self::Jsonb, _) | (_, Self::Jsonb) => Self::Jsonb,
            _ => Self::Text,
        }
    }

    pub fn from_logical_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "boolean" | "bool" => Self::Bool,
            "integer" | "int" | "bigint" | "smallint" | "int64" | "int32" => Self::Int,
            "number" | "float" | "double" | "double precision" | "decimal" | "numeric" | "real" => {
                Self::Float
            }
            "date" => Self::Date,
            "timestamp" | "datetime" | "timestamptz" => Self::Timestamp,
            "array" | "object" | "json" | "jsonb" => Self::Jsonb,
            _ => Self::Text,
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn inferred_type(&self) -> Option<InferredType> {
        match self {
            Self::Null => None,
            Self::Bool(_) => Some(InferredType::Bool),
            Self::Int(_) => Some(InferredType::Int),
            Self::Float(_) => Some(InferredType::Float),
            Self::Date(_) => Some(InferredType::Date),
            Self::Timestamp(_) => Some(InferredType::Timestamp),
            Self::String(_) => Some(InferredType::Text),
            Self::Array(_) | Self::Object(_) => Some(InferredType::Jsonb),
        }
    }

    pub fn canonical_key(&self) -> String {
        match self {
            Self::Null => "\0NULL\0".to_string(),
            Self::Bool(b) => b.to_string(),
            Self::Int(i) => i.to_string(),
            Self::Float(f) => format!("{f}"),
            Self::Date(s) | Self::Timestamp(s) | Self::String(s) => s.clone(),
            Self::Array(_) | Self::Object(_) => serde_json::to_string(self).unwrap_or_default(),
        }
    }

    /// Format a single cell for PostgreSQL `COPY ... (FORMAT text)`.
    pub fn to_pg_copy_text(&self) -> String {
        match self {
            Self::Null => "\\N".to_string(),
            Self::Bool(b) => if *b { "t" } else { "f" }.to_string(),
            Self::Int(i) => i.to_string(),
            Self::Float(f) => {
                if f.is_finite() {
                    format!("{f}")
                } else {
                    "\\N".to_string()
                }
            }
            Self::Date(s) | Self::Timestamp(s) | Self::String(s) => escape_pg_copy(s),
            Self::Array(_) | Self::Object(_) => {
                let raw = serde_json::to_string(self).unwrap_or_else(|_| "null".to_string());
                escape_pg_copy(&raw)
            }
        }
    }

    pub fn classify_string(s: String) -> Self {
        let t = s.trim();
        if t.len() == 10 && NaiveDate::parse_from_str(t, "%Y-%m-%d").is_ok() {
            return Self::Date(t.to_string());
        }
        if t.len() >= 19
            && t.as_bytes().get(4) == Some(&b'-')
            && (DateTime::parse_from_rfc3339(t).is_ok()
                || NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S").is_ok()
                || NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S").is_ok()
                || NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f").is_ok()
                || NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f").is_ok())
        {
            return Self::Timestamp(t.to_string());
        }
        Self::String(s)
    }
}

pub fn escape_pg_copy(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            c => out.push(c),
        }
    }
    out
}

pub type Row = IndexMap<String, Value>;

#[derive(Debug, Clone)]
pub struct Dataset {
    pub source_name: String,
    pub columns: IndexSet<String>,
    pub rows: Vec<Row>,
}

impl Dataset {
    pub fn from_rows(source_name: String, rows: Vec<Row>) -> Self {
        let mut columns = IndexSet::new();
        for row in &rows {
            for k in row.keys() {
                columns.insert(k.clone());
            }
        }
        Self {
            source_name,
            columns,
            rows,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// Recursively flatten nested `Value::Object` values into snake-ish top-level keys joined by `sep`.
/// Arrays remain intact as `Value::Array` (loaded into Postgres `jsonb`).
pub fn flatten_dataset(dataset: Dataset, sep: &str) -> Dataset {
    let mut flat_rows = Vec::with_capacity(dataset.rows.len());
    for row in dataset.rows {
        let mut out: Row = IndexMap::with_capacity(row.len());
        for (k, v) in row {
            flatten_into(&k, v, sep, &mut out);
        }
        flat_rows.push(out);
    }
    Dataset::from_rows(dataset.source_name, flat_rows)
}

fn flatten_into(prefix: &str, val: Value, sep: &str, out: &mut Row) {
    match val {
        Value::Object(map) if !map.is_empty() => {
            for (k, v) in map {
                let next_key = format!("{prefix}{sep}{k}");
                flatten_into(&next_key, v, sep, out);
            }
        }
        other => {
            out.insert(prefix.to_string(), other);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub pg_type: String,
    pub not_null: bool,
    pub primary_key: bool,
}

/// Infer target Postgres column types from dataset values.
pub fn infer_column_specs(dataset: &Dataset, key_cols: &[String]) -> IndexMap<String, ColumnSpec> {
    let mut specs = IndexMap::new();
    for col_name in &dataset.columns {
        let mut inferred: Option<InferredType> = None;
        let mut has_null = false;
        for row in &dataset.rows {
            let val = row.get(col_name).unwrap_or(&Value::Null);
            if let Some(t) = val.inferred_type() {
                inferred = Some(match inferred {
                    None => t,
                    Some(prev) => prev.unify(t),
                });
            } else {
                has_null = true;
            }
        }
        let is_pk = key_cols.iter().any(|k| k == col_name);
        let pg_type = inferred.unwrap_or(InferredType::Text).pg_type().to_string();
        specs.insert(
            col_name.clone(),
            ColumnSpec {
                name: col_name.clone(),
                pg_type,
                not_null: is_pk || (!has_null && !dataset.rows.is_empty()),
                primary_key: is_pk,
            },
        );
    }
    specs
}

#[derive(Debug, Clone)]
pub struct ContractSchema {
    pub columns: IndexMap<String, ColumnSpec>,
    pub primary_keys: Vec<String>,
}

/// Load column types and primary keys from an ODCS v3 YAML or `datakeel.toml` contract.
pub fn load_contract_schema(path: &Path) -> Result<ContractSchema> {
    let content = fs::read_to_string(path).map_err(|source| LoadKeelError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let src = path.display().to_string();

    let mut columns = IndexMap::new();
    let mut primary_keys = Vec::new();

    match ext.as_str() {
        "yaml" | "yml" => {
            let doc: OdcsDoc =
                serde_yaml_ng::from_str(&content).map_err(|e| LoadKeelError::ContractParse {
                    path: src,
                    detail: e.to_string(),
                })?;
            if let Some(obj) = doc.schema.into_iter().next() {
                for prop in obj.properties {
                    let pg_type = prop.physical_type.unwrap_or_else(|| {
                        InferredType::from_logical_str(&prop.logical_type)
                            .pg_type()
                            .to_string()
                    });
                    if prop.primary_key {
                        primary_keys.push(prop.name.clone());
                    }
                    columns.insert(
                        prop.name.clone(),
                        ColumnSpec {
                            name: prop.name,
                            pg_type,
                            not_null: prop.required || prop.primary_key,
                            primary_key: prop.primary_key,
                        },
                    );
                }
            }
        }
        "toml" => {
            let doc: TomlContract =
                toml::from_str(&content).map_err(|e| LoadKeelError::ContractParse {
                    path: src,
                    detail: e.to_string(),
                })?;
            for (name, col) in doc.columns {
                let pg_type = col.physical_type.unwrap_or_else(|| {
                    InferredType::from_logical_str(&col.logical_type)
                        .pg_type()
                        .to_string()
                });
                if col.primary_key {
                    primary_keys.push(name.clone());
                }
                columns.insert(
                    name.clone(),
                    ColumnSpec {
                        name,
                        pg_type,
                        not_null: col.required || col.primary_key,
                        primary_key: col.primary_key,
                    },
                );
            }
        }
        _ => {
            return Err(LoadKeelError::ContractParse {
                path: src,
                detail: format!("unsupported contract extension '.{ext}' (expected .yaml, .yml, or .toml)"),
            });
        }
    }

    Ok(ContractSchema {
        columns,
        primary_keys,
    })
}

pub fn load_input(
    input: &str,
    explicit_fmt: InputFormatArg,
    json_path: Option<&str>,
) -> Result<Dataset> {
    let fmt = detect_format(input, explicit_fmt)?;

    if fmt == InputFormatArg::Parquet {
        if input == "-" {
            return Err(LoadKeelError::UnsupportedFormat {
                path: "-".to_string(),
                detail: "Parquet requires a seekable file path, not stdin".to_string(),
            });
        }
        return read_parquet(Path::new(input));
    }

    let bytes = if input == "-" {
        let mut buf = Vec::new();
        io::stdin()
            .read_to_end(&mut buf)
            .map_err(|source| LoadKeelError::IoRead {
                path: Path::new("-").to_path_buf(),
                source,
            })?;
        buf
    } else {
        fs::read(input).map_err(|source| LoadKeelError::IoRead {
            path: Path::new(input).to_path_buf(),
            source,
        })?
    };

    match fmt {
        InputFormatArg::Json => read_json(&bytes, input, json_path),
        InputFormatArg::Jsonl => read_jsonl(&bytes, input),
        InputFormatArg::Csv => read_csv(&bytes, input, b','),
        InputFormatArg::Tsv => read_csv(&bytes, input, b'\t'),
        InputFormatArg::Parquet | InputFormatArg::Auto => unreachable!(),
    }
}

fn detect_format(input: &str, explicit: InputFormatArg) -> Result<InputFormatArg> {
    if explicit != InputFormatArg::Auto {
        return Ok(explicit);
    }
    if input == "-" {
        return Ok(InputFormatArg::Jsonl);
    }
    let ext = Path::new(input)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "json" => Ok(InputFormatArg::Json),
        "jsonl" | "ndjson" => Ok(InputFormatArg::Jsonl),
        "csv" => Ok(InputFormatArg::Csv),
        "tsv" => Ok(InputFormatArg::Tsv),
        "parquet" | "pq" => Ok(InputFormatArg::Parquet),
        _ => Err(LoadKeelError::UnsupportedFormat {
            path: input.to_string(),
            detail: format!("unrecognized extension '.{ext}' (pass --input-format)"),
        }),
    }
}

fn read_json(bytes: &[u8], source_name: &str, json_path: Option<&str>) -> Result<Dataset> {
    let parsed: JsonValue =
        serde_json::from_slice(bytes).map_err(|e| LoadKeelError::DataParse {
            path: source_name.to_string(),
            detail: e.to_string(),
        })?;
    let target = if let Some(path) = json_path {
        let mut cursor = &parsed;
        for seg in path.split('.') {
            cursor = cursor
                .get(seg)
                .ok_or_else(|| LoadKeelError::JsonPathNotFound {
                    path: path.to_string(),
                    source_name: source_name.to_string(),
                })?;
        }
        cursor
    } else {
        &parsed
    };
    let arr = target.as_array().ok_or_else(|| {
        if let Some(path) = json_path {
            LoadKeelError::JsonPathNotFound {
                path: path.to_string(),
                source_name: source_name.to_string(),
            }
        } else {
            LoadKeelError::DataParse {
                path: source_name.to_string(),
                detail: "expected top-level JSON array of objects (or pass --json-path)".to_string(),
            }
        }
    })?;
    let mut rows = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        rows.push(json_object_to_row(item, source_name, idx + 1)?);
    }
    Ok(Dataset::from_rows(source_name.to_string(), rows))
}

fn read_jsonl(bytes: &[u8], source_name: &str) -> Result<Dataset> {
    let text = std::str::from_utf8(bytes).map_err(|e| LoadKeelError::DataParse {
        path: source_name.to_string(),
        detail: format!("invalid UTF-8: {e}"),
    })?;
    let mut rows = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let parsed: JsonValue =
            serde_json::from_str(t).map_err(|e| LoadKeelError::DataParse {
                path: source_name.to_string(),
                detail: format!("line {}: {e}", idx + 1),
            })?;
        rows.push(json_object_to_row(&parsed, source_name, idx + 1)?);
    }
    Ok(Dataset::from_rows(source_name.to_string(), rows))
}

fn json_object_to_row(val: &JsonValue, source_name: &str, row_num: usize) -> Result<Row> {
    let map = val.as_object().ok_or_else(|| LoadKeelError::DataParse {
        path: source_name.to_string(),
        detail: format!("record {row_num} must be a JSON object"),
    })?;
    let mut row = IndexMap::with_capacity(map.len());
    for (k, v) in map {
        row.insert(k.clone(), json_to_value(v));
    }
    Ok(row)
}

fn json_to_value(v: &JsonValue) -> Value {
    match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Value::Int(u as i64)
                } else {
                    Value::Float(u as f64)
                }
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        JsonValue::String(s) => Value::classify_string(s.clone()),
        JsonValue::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        JsonValue::Object(obj) => {
            let mut m = IndexMap::with_capacity(obj.len());
            for (k, v) in obj {
                m.insert(k.clone(), json_to_value(v));
            }
            Value::Object(m)
        }
    }
}

fn read_csv(bytes: &[u8], source_name: &str, delimiter: u8) -> Result<Dataset> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_reader(bytes);
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| LoadKeelError::DataParse {
            path: source_name.to_string(),
            detail: format!("CSV headers: {e}"),
        })?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();
    let mut columns = IndexSet::new();
    for h in &headers {
        columns.insert(h.clone());
    }
    let mut rows = Vec::new();
    for (idx, rec_res) in reader.records().enumerate() {
        let rec = rec_res.map_err(|e| LoadKeelError::DataParse {
            path: source_name.to_string(),
            detail: format!("row {}: {e}", idx + 2),
        })?;
        let mut row = IndexMap::with_capacity(headers.len());
        for (ci, col_name) in headers.iter().enumerate() {
            row.insert(col_name.clone(), parse_csv_cell(rec.get(ci).unwrap_or("")));
        }
        rows.push(row);
    }
    Ok(Dataset {
        source_name: source_name.to_string(),
        columns,
        rows,
    })
}

fn parse_csv_cell(raw: &str) -> Value {
    let t = raw.trim();
    if t.is_empty() {
        return Value::Null;
    }
    if t.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    let leading_zero = t.len() > 1 && t.starts_with('0') && !t.starts_with("0.");
    if !leading_zero {
        if let Ok(i) = t.parse::<i64>() {
            return Value::Int(i);
        }
        if let Ok(f) = t.parse::<f64>()
            && f.is_finite()
        {
            return Value::Float(f);
        }
    }
    Value::classify_string(t.to_string())
}

fn read_parquet(path: &Path) -> Result<Dataset> {
    let file = File::open(path).map_err(|source| LoadKeelError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = SerializedFileReader::new(file).map_err(|e| LoadKeelError::DataParse {
        path: path.display().to_string(),
        detail: format!("Parquet open: {e}"),
    })?;
    let mut columns = IndexSet::new();
    for f in reader.metadata().file_metadata().schema().get_fields() {
        columns.insert(f.name().to_string());
    }
    let iter = reader.get_row_iter(None).map_err(|e| LoadKeelError::DataParse {
        path: path.display().to_string(),
        detail: format!("Parquet iter: {e}"),
    })?;
    let mut rows = Vec::new();
    for row_res in iter {
        let pq_row = row_res.map_err(|e| LoadKeelError::DataParse {
            path: path.display().to_string(),
            detail: format!("Parquet row: {e}"),
        })?;
        let mut row = IndexMap::with_capacity(pq_row.len());
        for (name, field) in pq_row.get_column_iter() {
            row.insert(name.clone(), field_to_value(field));
        }
        rows.push(row);
    }
    Ok(Dataset {
        source_name: path.display().to_string(),
        columns,
        rows,
    })
}

fn field_to_value(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(b) => Value::Bool(*b),
        Field::Byte(v) => Value::Int(*v as i64),
        Field::Short(v) => Value::Int(*v as i64),
        Field::Int(v) => Value::Int(*v as i64),
        Field::Long(v) => Value::Int(*v),
        Field::UByte(v) => Value::Int(*v as i64),
        Field::UShort(v) => Value::Int(*v as i64),
        Field::UInt(v) => Value::Int(*v as i64),
        Field::ULong(v) => {
            if *v <= i64::MAX as u64 {
                Value::Int(*v as i64)
            } else {
                Value::Float(*v as f64)
            }
        }
        Field::Float(v) => Value::Float(*v as f64),
        Field::Double(v) => Value::Float(*v),
        Field::Str(s) => Value::classify_string(s.clone()),
        Field::Date(days) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
            Value::Date((epoch + Duration::days(*days as i64)).format("%Y-%m-%d").to_string())
        }
        Field::TimestampMillis(ms) => DateTime::from_timestamp_millis(*ms)
            .map_or(Value::Int(*ms), |dt| Value::Timestamp(dt.to_rfc3339())),
        Field::TimestampMicros(us) => DateTime::from_timestamp_micros(*us)
            .map_or(Value::Int(*us), |dt| Value::Timestamp(dt.to_rfc3339())),
        Field::Group(sub) => {
            let mut m = IndexMap::with_capacity(sub.len());
            for (k, v) in sub.get_column_iter() {
                m.insert(k.clone(), field_to_value(v));
            }
            Value::Object(m)
        }
        Field::ListInternal(list) => {
            Value::Array(list.elements().iter().map(field_to_value).collect())
        }
        other => Value::String(other.to_string()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OdcsDoc {
    #[serde(default)]
    schema: Vec<OdcsObj>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OdcsObj {
    #[serde(default)]
    properties: Vec<OdcsProp>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OdcsProp {
    name: String,
    #[serde(default = "default_str")]
    logical_type: String,
    physical_type: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    primary_key: bool,
}

fn default_str() -> String {
    "string".to_string()
}

#[derive(Debug, Deserialize)]
struct TomlContract {
    #[serde(default)]
    columns: IndexMap<String, TomlCol>,
}

#[derive(Debug, Deserialize)]
struct TomlCol {
    #[serde(rename = "type", default = "default_str")]
    logical_type: String,
    physical_type: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    primary_key: bool,
}
