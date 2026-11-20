use crate::error::{LoadKeelError, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub const SKILL_MD: &str = r#"---
name: loadkeel
description: Bulk-load JSON, JSONL, CSV, TSV, Parquet, or stdin into PostgreSQL via native COPY + atomic staging-table ON CONFLICT upserts. Supports nested JSON flattening, ODCS v3 / datakeel.toml contracts, --auto-create, --evolve, and --dry-run. Use instead of slow pandas.to_sql or hand-written psycopg insert loops.
---

# LoadKeel Agent Manual

## Identity & Scope

`loadkeel` is the high-speed PostgreSQL ingestion leg of the **Keel Data Stack** (`datakeel` validates → `loadkeel` bulk-loads → `sqlkeel` verifies SQL). It streams JSON, JSONL/NDJSON, CSV, TSV, Parquet, or `stdin` directly into PostgreSQL via `COPY ... FROM STDIN` into a temp staging table and atomically commits via `INSERT ... ON CONFLICT DO UPDATE`, `APPEND`, or `REPLACE` inside a single transaction.

## Output & Exit Code Contract

- **TTY-aware dual mode:** Compact single-line JSON (`--format agent`) when piped/non-TTY; human summary in a TTY.
- **Strict stream separation:** `stdout` = pure load report or flattened JSONL; `stderr` = structured JSON `ErrorEnvelope` on errors.
- **Secret hygiene:** Always use `pg://schema.table` with `DATABASE_URL` (or `LOADKEEL_DATABASE_URL`) in the environment instead of embedding passwords in CLI arguments. Passwords are redacted automatically in all logs and errors.
- **Exit codes:** `0` = load/plan succeeded, `2` = IO, parse, schema, or Postgres error (check `error.code` and `error.fix_hint`).

## Core Command Matrix

| Goal | Command |
| :--- | :--- |
| Preview exact DDL & upsert plan offline | `loadkeel load clean.jsonl pg://staging.populi_students --contract contracts/populi_students.yaml --auto-create --dry-run --format agent` |
| Idempotent upsert from JSONL using contract PK | `loadkeel load clean.jsonl pg://staging.populi_students --contract contracts/populi_students.yaml --auto-create --format agent` |
| Flatten nested API JSON & upsert with schema evolution | `loadkeel load api.json --json-path data.items pg://staging.populi_students --flatten --key student_id --auto-create --evolve --format agent` |
| Pipe Python extractor directly into Postgres | `python extract_populi.py \| loadkeel load - pg://staging.populi_students --key student_id --auto-create --format agent` |
| Preview flattened JSONL without touching DB | `loadkeel flatten nested_api.jsonl` |

## Agent Decision Tree for ETL Ingestion

1. **Validate first with `datakeel`:**
   - Run `datakeel check contracts/table.yaml raw.jsonl --clean clean.jsonl --quarantine bad.jsonl --max-quarantine-rate 0.05`.
2. **Dry-run before first table load:**
   - Run `loadkeel load clean.jsonl pg://staging.table --contract contracts/table.yaml --auto-create --dry-run --format agent` to verify column types, conflict keys, and SQL plan.
3. **Load atomically:**
   - Remove `--dry-run` to stream via `COPY` + atomic `ON CONFLICT` upsert.
   - If `LK_PG_MISSING_COLUMNS` fires because the upstream API added a column, inspect the new field, update the contract, and pass `--evolve` to add it safely via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
4. **Verify downstream SQL with `sqlkeel`:**
   - Run `sqlkeel check sql/ --schema contracts/`.

## Gotchas & Anti-Patterns

- `upsert` mode automatically collapses duplicate conflict keys within the same batch (last-wins) so Postgres never fails with `"ON CONFLICT DO UPDATE command cannot affect row a second time"`. Pass `--no-dedupe` if you want duplicate keys to error instead.
- `upsert` requires a `PRIMARY KEY` or `UNIQUE` constraint on `--key` columns in Postgres (`--auto-create` creates the `PRIMARY KEY` automatically).
"#;

pub fn install_skill(target: &Path) -> Result<PathBuf> {
    let dest = if target
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("md"))
    {
        target.to_path_buf()
    } else {
        target.join("SKILL.md")
    };

    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| LoadKeelError::IoWrite {
            path: dest.clone(),
            source,
        })?;
    }

    fs::write(&dest, SKILL_MD).map_err(|source| LoadKeelError::IoWrite {
        path: dest.clone(),
        source,
    })?;

    Ok(dest)
}
