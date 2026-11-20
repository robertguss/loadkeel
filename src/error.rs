use serde::Serialize;
use std::path::PathBuf;
use thiserror::Error;

pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 2;

#[derive(Debug, Error)]
pub enum LoadKeelError {
    #[error("Failed to read '{path}': {source}")]
    IoRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write '{path}': {source}")]
    IoWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Unsupported input format for '{path}': {detail}")]
    UnsupportedFormat { path: String, detail: String },

    #[error("Failed to parse input data '{path}': {detail}")]
    DataParse { path: String, detail: String },

    #[error("Failed to parse contract '{path}': {detail}")]
    ContractParse { path: String, detail: String },

    #[error("JSON path '{path}' not found or not an array of objects in '{source_name}'")]
    JsonPathNotFound { path: String, source_name: String },

    #[error("Postgres shorthand '{target}' requires DATABASE_URL or LOADKEEL_DATABASE_URL to be set")]
    PgMissingDatabaseUrl { target: String },

    #[error("Postgres target '{target}' is missing a table name (pass `--table schema.table` or `?table=schema.table` or use `pg://schema.table`)")]
    PgMissingTable { target: String },

    #[error("Postgres target table '{table}' does not exist in '{target}'")]
    PgTableNotFound { target: String, table: String },

    #[error("Incoming dataset has columns not present in Postgres table '{table}': {columns}")]
    PgMissingColumns { table: String, columns: String },

    #[error("Upsert mode requires at least one conflict key column")]
    MissingUpsertKey,

    #[error("Conflict key column '{key}' not found in dataset columns")]
    UnknownUpsertKey { key: String },

    #[error("Failed to connect to Postgres '{target}': {detail}")]
    PgConnect { target: String, detail: String },

    #[error("Postgres load failed on '{target}': {detail}")]
    PgExecute { target: String, detail: String },

    #[error("{0}")]
    General(String),
}

impl LoadKeelError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::IoRead { .. } => "LK_IO_READ",
            Self::IoWrite { .. } => "LK_IO_WRITE",
            Self::UnsupportedFormat { .. } => "LK_UNSUPPORTED_FORMAT",
            Self::DataParse { .. } => "LK_DATA_PARSE",
            Self::ContractParse { .. } => "LK_CONTRACT_PARSE",
            Self::JsonPathNotFound { .. } => "LK_JSON_PATH_NOT_FOUND",
            Self::PgMissingDatabaseUrl { .. } => "LK_PG_MISSING_DATABASE_URL",
            Self::PgMissingTable { .. } => "LK_PG_MISSING_TABLE",
            Self::PgTableNotFound { .. } => "LK_PG_TABLE_NOT_FOUND",
            Self::PgMissingColumns { .. } => "LK_PG_MISSING_COLUMNS",
            Self::MissingUpsertKey => "LK_MISSING_UPSERT_KEY",
            Self::UnknownUpsertKey { .. } => "LK_UNKNOWN_UPSERT_KEY",
            Self::PgConnect { .. } => "LK_PG_CONNECT",
            Self::PgExecute { .. } => "LK_PG_EXECUTE",
            Self::General(_) => "LK_ERROR",
        }
    }

    pub fn fix_hint(&self) -> Option<String> {
        match self {
            Self::PgMissingDatabaseUrl { .. } => Some(
                "Set `export DATABASE_URL=postgres://user:pass@host:5432/dbname` (or `LOADKEEL_DATABASE_URL`) and retry."
                    .to_string(),
            ),
            Self::PgMissingTable { .. } => Some(
                "Target a table via `pg://staging.my_table` (with `DATABASE_URL`) or pass `--table staging.my_table`."
                    .to_string(),
            ),
            Self::PgTableNotFound { table, .. } => Some(format!(
                "Pass `--auto-create` to create '{table}' automatically, or generate DDL with `datakeel codegen --target postgres`."
            )),
            Self::PgMissingColumns { .. } => Some(
                "Pass `--evolve` to safely add new nullable columns via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, or filter columns before loading."
                    .to_string(),
            ),
            Self::MissingUpsertKey => Some(
                "Pass `--key <pk_col>` (or pass `--contract <file>` with a `primaryKey: true` column), or use `--mode append`."
                    .to_string(),
            ),
            Self::UnknownUpsertKey { key } => Some(format!(
                "Verify `--key {key}` matches an actual column name (if nested JSON, pass `--flatten`)."
            )),
            Self::PgConnect { .. } => Some(
                "Check Postgres host, port, credentials, and `sslmode` (`?sslmode=disable` for local or `?sslmode=require` for hosted)."
                    .to_string(),
            ),
            Self::PgExecute { .. } => Some(
                "Inspect the Postgres error detail (common causes: missing UNIQUE/PRIMARY KEY index on `--key` columns, or type mismatch with existing table). Run with `--dry-run` to preview the exact SQL plan."
                    .to_string(),
            ),
            _ => None,
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::IoRead { .. } | Self::IoWrite { .. } | Self::PgConnect { .. }
        )
    }

    pub fn to_envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            schema_version: "1.0",
            ok: false,
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
                fix_hint: self.fix_hint(),
                retryable: self.retryable(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub schema_version: &'static str,
    pub ok: bool,
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
    pub retryable: bool,
}

pub type Result<T> = std::result::Result<T, LoadKeelError>;
