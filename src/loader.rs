use crate::cli::LoadModeArg;
use crate::data::{ColumnSpec, ContractSchema, Dataset, Row, Value, infer_column_specs};
use crate::error::{LoadKeelError, Result};
use indexmap::IndexMap;
use native_tls::TlsConnector;
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::Instant;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    Verify,
}

#[derive(Debug, Clone)]
pub struct PgTarget {
    pub conn_url: Option<String>,
    pub safe_display: String,
    pub schema: String,
    pub table: String,
    pub sslmode: SslMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectiveMode {
    Upsert,
    Append,
    Replace,
}

#[derive(Debug, Clone)]
pub struct LoadOptions {
    pub mode: LoadModeArg,
    pub keys: Vec<String>,
    pub contract: Option<ContractSchema>,
    pub auto_create: bool,
    pub evolve: bool,
    pub dedupe: bool,
    pub dry_run: bool,
    pub explicit_table: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    pub schema_version: &'static str,
    pub command: &'static str,
    pub ok: bool,
    pub dry_run: bool,
    pub target: String,
    pub schema_name: String,
    pub table_name: String,
    pub mode: EffectiveMode,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflict_keys: Vec<String>,
    pub rows_read: usize,
    pub duplicates_collapsed: usize,
    pub rows_staged: usize,
    pub rows_loaded: u64,
    pub columns_loaded: Vec<String>,
    pub table_created: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns_added: Vec<String>,
    pub elapsed_ms: u64,
    pub rows_per_sec: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sql_plan: Vec<String>,
}

pub fn resolve_pg_target(
    input: &str,
    explicit_table: Option<&str>,
    allow_missing_env_for_dry_run: bool,
) -> Result<PgTarget> {
    let lower = input.to_ascii_lowercase();
    let (raw_conn_url, shorthand_table) = if lower.starts_with("pg://") {
        let rest = &input[5..];
        let table_part = rest
            .split('?')
            .next()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let env_url = std::env::var("LOADKEEL_DATABASE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                std::env::var("DATABASE_URL")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            });
        match env_url {
            Some(u) => (
                Some(u),
                if table_part.is_empty() {
                    None
                } else {
                    Some(table_part)
                },
            ),
            None if allow_missing_env_for_dry_run => (
                None,
                if table_part.is_empty() {
                    None
                } else {
                    Some(table_part)
                },
            ),
            None => {
                return Err(LoadKeelError::PgMissingDatabaseUrl {
                    target: input.to_string(),
                });
            }
        }
    } else {
        (Some(input.to_string()), None)
    };

    let mut table_param = shorthand_table;
    let mut sslmode = SslMode::Prefer;
    let mut clean_conn_url: Option<String> = None;
    let mut safe_base = "pg://".to_string();

    if let Some(raw_url) = raw_conn_url {
        let mut url = Url::parse(&raw_url).map_err(|e| LoadKeelError::PgConnect {
            target: redact_uri(&raw_url),
            detail: format!("invalid Postgres URL: {e}"),
        })?;
        let mut kept: Vec<(String, String)> = Vec::new();
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "table" if !v.trim().is_empty() => table_param = Some(v.into_owned()),
                "sslmode" => {
                    sslmode = parse_sslmode(&v);
                    kept.push((k.into_owned(), v.into_owned()));
                }
                _ => kept.push((k.into_owned(), v.into_owned())),
            }
        }
        if kept.is_empty() {
            url.set_query(None);
        } else {
            let mut ser = url.query_pairs_mut();
            ser.clear();
            for (k, v) in &kept {
                ser.append_pair(k, v);
            }
        }
        safe_base = redact_url(&url);
        clean_conn_url = Some(url.to_string());
    }

    if let Some(t) = explicit_table {
        table_param = Some(t.to_string());
    }

    let table_spec = table_param.ok_or_else(|| LoadKeelError::PgMissingTable {
        target: safe_base.clone(),
    })?;
    let (schema, table) = split_schema_table(&table_spec);
    if table.is_empty() {
        return Err(LoadKeelError::PgMissingTable { target: safe_base });
    }

    let safe_display = if safe_base == "pg://" {
        format!("pg://{schema}.{table}")
    } else {
        format!("{safe_base}?table={schema}.{table}")
    };

    Ok(PgTarget {
        conn_url: clean_conn_url,
        safe_display,
        schema,
        table,
        sslmode,
    })
}

pub fn execute_load(dataset: &Dataset, target_uri: &str, opts: &LoadOptions) -> Result<LoadReport> {
    let started = Instant::now();
    let target = resolve_pg_target(target_uri, opts.explicit_table.as_deref(), opts.dry_run)?;

    // Resolve conflict keys from CLI or contract primary keys
    let conflict_keys: Vec<String> = if !opts.keys.is_empty() {
        opts.keys.clone()
    } else if let Some(ref c) = opts.contract {
        c.primary_keys.clone()
    } else {
        Vec::new()
    };

    // Resolve effective load mode
    let mode = match opts.mode {
        LoadModeArg::Auto => {
            if !conflict_keys.is_empty() {
                EffectiveMode::Upsert
            } else {
                EffectiveMode::Append
            }
        }
        LoadModeArg::Upsert => EffectiveMode::Upsert,
        LoadModeArg::Append => EffectiveMode::Append,
        LoadModeArg::Replace => EffectiveMode::Replace,
    };

    if mode == EffectiveMode::Upsert && conflict_keys.is_empty() {
        return Err(LoadKeelError::MissingUpsertKey);
    }

    // Build target column specs (contract columns take precedence, inferred fills the rest)
    let inferred = infer_column_specs(dataset, &conflict_keys);
    let mut desired_cols: IndexMap<String, ColumnSpec> = IndexMap::new();
    if let Some(ref c) = opts.contract {
        for (name, spec) in &c.columns {
            if dataset.columns.contains(name) || dataset.rows.is_empty() {
                desired_cols.insert(name.clone(), spec.clone());
            }
        }
    }
    for (name, spec) in inferred {
        desired_cols.entry(name).or_insert(spec);
    }

    // Verify all upsert key columns exist in desired_cols
    for k in &conflict_keys {
        if !desired_cols.contains_key(k) {
            return Err(LoadKeelError::UnknownUpsertKey { key: k.clone() });
        }
    }

    let col_names: Vec<String> = desired_cols.keys().cloned().collect();
    let rows_read = dataset.row_count();

    // Last-wins in-batch deduplication for upsert mode (prevents Postgres ON CONFLICT duplicate-row error)
    let (staged_rows, duplicates_collapsed): (Vec<&Row>, usize) =
        if mode == EffectiveMode::Upsert && opts.dedupe && !conflict_keys.is_empty() {
            dedupe_rows_last_wins(&dataset.rows, &conflict_keys)
        } else {
            (dataset.rows.iter().collect(), 0)
        };
    let rows_staged = staged_rows.len();

    let qualified = format!(
        "{}.{}",
        quote_ident(&target.schema),
        quote_ident(&target.table)
    );
    let create_schema_sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {};",
        quote_ident(&target.schema)
    );
    let create_table_sql = build_create_table_sql(&qualified, &desired_cols, &conflict_keys);
    let quoted_col_list = col_names
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let stage_table = "\"_loadkeel_stage\"";
    let copy_sql = format!("COPY {stage_table} ({quoted_col_list}) FROM STDIN WITH (FORMAT text)");
    let merge_sql = build_merge_sql(mode, &qualified, stage_table, &col_names, &conflict_keys);

    let mut sql_plan = Vec::new();
    if opts.auto_create {
        sql_plan.push(create_schema_sql.clone());
        sql_plan.push(create_table_sql.clone());
    }
    sql_plan.push(format!(
        "CREATE TEMP TABLE {stage_table} (LIKE {qualified} INCLUDING DEFAULTS) ON COMMIT DROP;"
    ));
    sql_plan.push(format!("{copy_sql}; -- ({rows_staged} row(s))"));
    if mode == EffectiveMode::Replace {
        sql_plan.push(format!("TRUNCATE TABLE {qualified};"));
    }
    sql_plan.push(format!("{merge_sql};"));

    if opts.dry_run {
        let elapsed_ms = started.elapsed().as_millis() as u64;
        return Ok(LoadReport {
            schema_version: "1.0",
            command: "load",
            ok: true,
            dry_run: true,
            target: target.safe_display,
            schema_name: target.schema,
            table_name: target.table,
            mode,
            conflict_keys,
            rows_read,
            duplicates_collapsed,
            rows_staged,
            rows_loaded: 0,
            columns_loaded: col_names,
            table_created: false,
            columns_added: Vec::new(),
            elapsed_ms,
            rows_per_sec: 0.0,
            sql_plan,
        });
    }

    // Live atomic Postgres transaction
    let mut client = connect_postgres(&target)?;
    let mut tx = client.transaction().map_err(|e| LoadKeelError::PgExecute {
        target: target.safe_display.clone(),
        detail: e.to_string(),
    })?;

    // 1. Check if target table exists
    let existing_cols = fetch_existing_columns(&mut tx, &target)?;
    let mut table_created = false;
    let mut columns_added: Vec<String> = Vec::new();

    if existing_cols.is_empty() {
        if !opts.auto_create {
            return Err(LoadKeelError::PgTableNotFound {
                target: target.safe_display.clone(),
                table: format!("{}.{}", target.schema, target.table),
            });
        }
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1))",
            &[&target.schema],
        )
        .map_err(|e| LoadKeelError::PgExecute {
            target: target.safe_display.clone(),
            detail: e.to_string(),
        })?;
        tx.batch_execute(&format!("{create_schema_sql}\n{create_table_sql}"))
            .map_err(|e| LoadKeelError::PgExecute {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })?;
        table_created = true;
    } else {
        // 2. Detect newly appearing columns
        let missing: Vec<&ColumnSpec> = desired_cols
            .values()
            .filter(|c| !existing_cols.contains(&c.name.to_ascii_lowercase()))
            .collect();
        if !missing.is_empty() {
            if !opts.evolve {
                let names = missing
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(LoadKeelError::PgMissingColumns {
                    table: format!("{}.{}", target.schema, target.table),
                    columns: names,
                });
            }
            for col in missing {
                let alter = format!(
                    "ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS {} {}",
                    quote_ident(&col.name),
                    col.pg_type
                );
                tx.execute(&alter, &[])
                    .map_err(|e| LoadKeelError::PgExecute {
                        target: target.safe_display.clone(),
                        detail: e.to_string(),
                    })?;
                columns_added.push(col.name.clone());
            }
        }
    }

    let mut rows_loaded = 0u64;
    if !staged_rows.is_empty() {
        // 3. Create temp staging table & stream via native Postgres COPY FROM STDIN
        tx.execute(
            &format!("CREATE TEMP TABLE {stage_table} (LIKE {qualified} INCLUDING DEFAULTS) ON COMMIT DROP"),
            &[],
        )
        .map_err(|e| LoadKeelError::PgExecute {
            target: target.safe_display.clone(),
            detail: e.to_string(),
        })?;

        {
            let mut writer = tx
                .copy_in(&copy_sql)
                .map_err(|e| LoadKeelError::PgExecute {
                    target: target.safe_display.clone(),
                    detail: e.to_string(),
                })?;
            let mut line_buf = String::with_capacity(256);
            for row in &staged_rows {
                line_buf.clear();
                for (i, col_name) in col_names.iter().enumerate() {
                    if i > 0 {
                        line_buf.push('\t');
                    }
                    let val = row.get(col_name).unwrap_or(&Value::Null);
                    line_buf.push_str(&val.to_pg_copy_text());
                }
                line_buf.push('\n');
                writer
                    .write_all(line_buf.as_bytes())
                    .map_err(|e| LoadKeelError::PgExecute {
                        target: target.safe_display.clone(),
                        detail: format!("COPY stream write failed: {e}"),
                    })?;
            }
            writer.finish().map_err(|e| LoadKeelError::PgExecute {
                target: target.safe_display.clone(),
                detail: format!("COPY finish failed: {e}"),
            })?;
        }

        // 4. Atomic merge into target table
        if mode == EffectiveMode::Replace {
            tx.execute(&format!("TRUNCATE TABLE {qualified}"), &[])
                .map_err(|e| LoadKeelError::PgExecute {
                    target: target.safe_display.clone(),
                    detail: e.to_string(),
                })?;
        }

        rows_loaded = tx
            .execute(&merge_sql, &[])
            .map_err(|e| LoadKeelError::PgExecute {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })?;
    } else if mode == EffectiveMode::Replace {
        tx.execute(&format!("TRUNCATE TABLE {qualified}"), &[])
            .map_err(|e| LoadKeelError::PgExecute {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })?;
    }

    tx.commit().map_err(|e| LoadKeelError::PgExecute {
        target: target.safe_display.clone(),
        detail: e.to_string(),
    })?;

    let elapsed = started.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;
    let secs = elapsed.as_secs_f64();
    let rows_per_sec = if secs > 0.0 {
        (rows_loaded as f64 / secs).round()
    } else {
        rows_loaded as f64
    };

    Ok(LoadReport {
        schema_version: "1.0",
        command: "load",
        ok: true,
        dry_run: false,
        target: target.safe_display,
        schema_name: target.schema,
        table_name: target.table,
        mode,
        conflict_keys,
        rows_read,
        duplicates_collapsed,
        rows_staged,
        rows_loaded,
        columns_loaded: col_names,
        table_created,
        columns_added,
        elapsed_ms,
        rows_per_sec,
        sql_plan: Vec::new(),
    })
}

fn dedupe_rows_last_wins<'a>(rows: &'a [Row], key_cols: &[String]) -> (Vec<&'a Row>, usize) {
    let mut last_idx: HashMap<Vec<String>, usize> = HashMap::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        let key: Vec<String> = key_cols
            .iter()
            .map(|k| row.get(k).unwrap_or(&Value::Null).canonical_key())
            .collect();
        last_idx.insert(key, idx);
    }
    let keep: HashSet<usize> = last_idx.into_values().collect();
    let mut out = Vec::with_capacity(keep.len());
    for (idx, row) in rows.iter().enumerate() {
        if keep.contains(&idx) {
            out.push(row);
        }
    }
    let collapsed = rows.len() - out.len();
    (out, collapsed)
}

fn fetch_existing_columns(
    tx: &mut postgres::Transaction<'_>,
    target: &PgTarget,
) -> Result<HashSet<String>> {
    let rows = tx
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &[&target.schema, &target.table],
        )
        .map_err(|e| LoadKeelError::PgExecute {
            target: target.safe_display.clone(),
            detail: e.to_string(),
        })?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let s: String = r.get(0);
            s.to_ascii_lowercase()
        })
        .collect())
}

fn build_create_table_sql(
    qualified: &str,
    cols: &IndexMap<String, ColumnSpec>,
    pk_cols: &[String],
) -> String {
    let mut defs: Vec<String> = Vec::with_capacity(cols.len() + 1);
    for col in cols.values() {
        let nn = if col.not_null { " NOT NULL" } else { "" };
        defs.push(format!(
            "    {} {}{nn}",
            quote_ident(&col.name),
            col.pg_type
        ));
    }
    if !pk_cols.is_empty() {
        let pks = pk_cols
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        defs.push(format!("    PRIMARY KEY ({pks})"));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {qualified} (\n{}\n);",
        defs.join(",\n")
    )
}

fn build_merge_sql(
    mode: EffectiveMode,
    qualified: &str,
    stage_table: &str,
    col_names: &[String],
    conflict_keys: &[String],
) -> String {
    let cols = col_names
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    match mode {
        EffectiveMode::Append | EffectiveMode::Replace => {
            format!("INSERT INTO {qualified} ({cols}) SELECT {cols} FROM {stage_table}")
        }
        EffectiveMode::Upsert => {
            let keys = conflict_keys
                .iter()
                .map(|k| quote_ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            let non_keys: Vec<String> = col_names
                .iter()
                .filter(|c| !conflict_keys.contains(*c))
                .map(|c| {
                    let q = quote_ident(c);
                    format!("{q} = EXCLUDED.{q}")
                })
                .collect();
            if non_keys.is_empty() {
                format!(
                    "INSERT INTO {qualified} ({cols}) SELECT {cols} FROM {stage_table} ON CONFLICT ({keys}) DO NOTHING"
                )
            } else {
                format!(
                    "INSERT INTO {qualified} ({cols}) SELECT {cols} FROM {stage_table} ON CONFLICT ({keys}) DO UPDATE SET {}",
                    non_keys.join(", ")
                )
            }
        }
    }
}

fn connect_postgres(target: &PgTarget) -> Result<Client> {
    let conn_url =
        target
            .conn_url
            .as_deref()
            .ok_or_else(|| LoadKeelError::PgMissingDatabaseUrl {
                target: target.safe_display.clone(),
            })?;
    match target.sslmode {
        SslMode::Disable => {
            Client::connect(conn_url, NoTls).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })
        }
        SslMode::Prefer => {
            if let Ok(connector) = make_tls(false)
                && let Ok(client) = Client::connect(conn_url, connector)
            {
                return Ok(client);
            }
            Client::connect(conn_url, NoTls).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })
        }
        SslMode::Require => {
            let connector = make_tls(false).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e,
            })?;
            Client::connect(conn_url, connector).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })
        }
        SslMode::Verify => {
            let connector = make_tls(true).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e,
            })?;
            Client::connect(conn_url, connector).map_err(|e| LoadKeelError::PgConnect {
                target: target.safe_display.clone(),
                detail: e.to_string(),
            })
        }
    }
}

fn make_tls(verify_certs: bool) -> std::result::Result<MakeTlsConnector, String> {
    let mut builder = TlsConnector::builder();
    if !verify_certs {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let tls = builder.build().map_err(|e| e.to_string())?;
    Ok(MakeTlsConnector::new(tls))
}

fn parse_sslmode(s: &str) -> SslMode {
    match s.trim().to_ascii_lowercase().as_str() {
        "disable" => SslMode::Disable,
        "require" => SslMode::Require,
        "verify-ca" | "verify-full" => SslMode::Verify,
        _ => SslMode::Prefer,
    }
}

fn split_schema_table(spec: &str) -> (String, String) {
    let cleaned = spec.trim();
    match cleaned.split_once('.') {
        Some((s, t)) => (s.trim().to_string(), t.trim().to_string()),
        None => ("public".to_string(), cleaned.to_string()),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn redact_uri(raw: &str) -> String {
    match Url::parse(raw) {
        Ok(u) => redact_url(&u),
        Err(_) => "<postgres-uri>".to_string(),
    }
}

fn redact_url(url: &Url) -> String {
    let mut cloned = url.clone();
    if cloned.password().is_some() {
        let _ = cloned.set_password(Some("***"));
    }
    cloned.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_keeps_last_row_for_duplicate_keys() {
        let mut r1 = Row::new();
        r1.insert("student_id".into(), Value::String("STU-1".into()));
        r1.insert("gpa".into(), Value::Float(3.0));

        let mut r2 = Row::new();
        r2.insert("student_id".into(), Value::String("STU-2".into()));
        r2.insert("gpa".into(), Value::Float(3.5));

        let mut r3 = Row::new();
        r3.insert("student_id".into(), Value::String("STU-1".into()));
        r3.insert("gpa".into(), Value::Float(3.9));

        let rows = vec![r1, r2, r3];
        let (kept, collapsed) = dedupe_rows_last_wins(&rows, &["student_id".to_string()]);
        assert_eq!(collapsed, 1);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0]["student_id"], Value::String("STU-2".into()));
        assert_eq!(kept[1]["gpa"], Value::Float(3.9));
    }
}
