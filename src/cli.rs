use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "loadkeel",
    version,
    about = "Blazing-fast, agent-first (DX + AX) Rust JSONL/CSV/Parquet to PostgreSQL COPY + atomic upsert bulk loader.",
    long_about = None
)]
pub struct Cli {
    /// Output format: auto (human in TTY, agent when piped), human, agent, or json
    #[arg(long, global = true, value_enum, default_value_t = OutputFormatArg::Auto)]
    pub format: OutputFormatArg,

    /// Disable ANSI colors in human output
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormatArg {
    Auto,
    Human,
    Agent,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputFormatArg {
    Auto,
    Json,
    Jsonl,
    Csv,
    Tsv,
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LoadModeArg {
    /// Automatically choose upsert if --key or contract primary key is set, else append
    Auto,
    /// Atomic COPY into temp staging table + INSERT ... ON CONFLICT DO UPDATE
    Upsert,
    /// Atomic COPY into temp staging table + INSERT INTO target
    Append,
    /// Atomic TRUNCATE target + COPY/INSERT inside a single transaction
    Replace,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Bulk-load JSON, JSONL, CSV, TSV, Parquet, or stdin into PostgreSQL via COPY + atomic upsert
    Load {
        /// Input file path (or '-' for stdin)
        input: String,

        /// Postgres target: 'pg://[schema.]table' (uses DATABASE_URL) or 'postgres://...'
        target: String,

        /// Explicit input data format (default: auto-detect from extension; stdin defaults to jsonl)
        #[arg(long = "input-format", value_enum, default_value_t = InputFormatArg::Auto)]
        input_format: InputFormatArg,

        /// Dot-separated path to an array of objects inside a JSON envelope (e.g. 'data.students')
        #[arg(long)]
        json_path: Option<String>,

        /// Explicit Postgres target table (`table` or `schema.table`) when passing a `postgres://` URI
        #[arg(long)]
        table: Option<String>,

        /// Load strategy: auto, upsert, append, or replace
        #[arg(long, value_enum, default_value_t = LoadModeArg::Auto)]
        mode: LoadModeArg,

        /// Conflict key column(s) for upsert mode (comma-separated, e.g. 'student_id' or 'student_id,course_id')
        #[arg(long, value_delimiter = ',')]
        key: Vec<String>,

        /// Optional ODCS v3 YAML or datakeel.toml contract defining target types and primary keys
        #[arg(long)]
        contract: Option<PathBuf>,

        /// Recursively flatten nested JSON objects into top-level columns (arrays stay JSONB)
        #[arg(long)]
        flatten: bool,

        /// Separator used when flattening nested JSON keys (default: '_')
        #[arg(long, default_value = "_")]
        flatten_sep: String,

        /// Automatically CREATE SCHEMA and CREATE TABLE IF NOT EXISTS before loading
        #[arg(long)]
        auto_create: bool,

        /// Automatically ALTER TABLE ADD COLUMN IF NOT EXISTS for newly appearing columns
        #[arg(long)]
        evolve: bool,

        /// Disable last-wins in-batch deduplication on upsert conflict keys
        #[arg(long)]
        no_dedupe: bool,

        /// Preview resolved schema, row counts, and exact SQL plan without writing to Postgres
        #[arg(long)]
        dry_run: bool,
    },

    /// Flatten nested JSON/JSONL records into clean top-level JSONL (for previewing or piping)
    Flatten {
        /// Input file path (or '-' for stdin)
        input: String,

        /// Explicit input format
        #[arg(long = "input-format", value_enum, default_value_t = InputFormatArg::Auto)]
        input_format: InputFormatArg,

        /// Dot-separated path to an array of objects inside a JSON envelope
        #[arg(long)]
        json_path: Option<String>,

        /// Separator for flattened nested keys (default: '_')
        #[arg(long, default_value = "_")]
        sep: String,

        /// Write flattened JSONL to a file instead of stdout
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Print or install the bundled SKILL.md agent manual (for Claude Code, Antigravity, Codex, Cursor)
    Skill {
        /// Install SKILL.md into a directory (e.g. .agents/skills/loadkeel)
        #[arg(long)]
        install: Option<PathBuf>,
    },
}
