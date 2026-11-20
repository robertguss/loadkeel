use crate::cli::OutputFormatArg;
use crate::error::{LoadKeelError, Result};
use crate::loader::LoadReport;
use owo_colors::OwoColorize;
use std::io::IsTerminal;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedFormat {
    Human,
    Agent,
    Json,
}

pub fn resolve_format(arg: OutputFormatArg) -> ResolvedFormat {
    match arg {
        OutputFormatArg::Human => ResolvedFormat::Human,
        OutputFormatArg::Agent => ResolvedFormat::Agent,
        OutputFormatArg::Json => ResolvedFormat::Json,
        OutputFormatArg::Auto => {
            if std::io::stdout().is_terminal() {
                ResolvedFormat::Human
            } else {
                ResolvedFormat::Agent
            }
        }
    }
}

pub fn should_use_color(no_color: bool, format: ResolvedFormat) -> bool {
    if no_color || format != ResolvedFormat::Human {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::stdout().is_terminal()
}

pub fn emit_load_report(
    report: &LoadReport,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if report.dry_run {
                if color {
                    " DRY RUN ".on_yellow().black().bold().to_string()
                } else {
                    "[DRY RUN]".to_string()
                }
            } else if color {
                " LOADED ".on_green().black().bold().to_string()
            } else {
                "[LOADED]".to_string()
            };

            println!(
                "{badge} {}.{} ({:?}) — {} read, {} staged, {} loaded in {}ms ({:.0} rows/s)",
                report.schema_name,
                report.table_name,
                report.mode,
                report.rows_read,
                report.rows_staged,
                report.rows_loaded,
                report.elapsed_ms,
                report.rows_per_sec
            );
            if report.duplicates_collapsed > 0 {
                println!(
                    "  dedupe : collapsed {} duplicate key row(s) (last-wins)",
                    report.duplicates_collapsed
                );
            }
            if report.table_created {
                println!(
                    "  schema : created {}.{} ({} columns)",
                    report.schema_name,
                    report.table_name,
                    report.columns_loaded.len()
                );
            }
            if !report.columns_added.is_empty() {
                println!("  evolved: added column(s): {}", report.columns_added.join(", "));
            }
            if !report.sql_plan.is_empty() {
                println!("\n  SQL Plan:");
                for stmt in &report.sql_plan {
                    for line in stmt.lines() {
                        println!("    {line}");
                    }
                }
            }
            Ok(())
        }
        ResolvedFormat::Agent => {
            let summary = if report.dry_run {
                format!(
                    "DRY RUN: would load {} row(s) into {}.{} via {:?} ({} columns).",
                    report.rows_staged,
                    report.schema_name,
                    report.table_name,
                    report.mode,
                    report.columns_loaded.len()
                )
            } else {
                format!(
                    "LOADED: {} row(s) into {}.{} via {:?} in {}ms ({:.0} rows/s).",
                    report.rows_loaded,
                    report.schema_name,
                    report.table_name,
                    report.mode,
                    report.elapsed_ms,
                    report.rows_per_sec
                )
            };
            let payload = serde_json::json!({
                "schema_version": report.schema_version,
                "command": report.command,
                "ok": report.ok,
                "dry_run": report.dry_run,
                "summary": summary,
                "target": report.target,
                "mode": report.mode,
                "rows_read": report.rows_read,
                "duplicates_collapsed": report.duplicates_collapsed,
                "rows_staged": report.rows_staged,
                "rows_loaded": report.rows_loaded,
                "table_created": report.table_created,
                "columns_added": report.columns_added,
                "elapsed_ms": report.elapsed_ms,
                "rows_per_sec": report.rows_per_sec,
                "sql_plan": report.sql_plan,
            });
            let json = serde_json::to_string(&payload)
                .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
        ResolvedFormat::Json => {
            let json = serde_json::to_string_pretty(report)
                .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_flatten_saved(
    rows: usize,
    cols: usize,
    output_path: &Path,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " FLATTENED ".on_green().black().bold().to_string()
            } else {
                "[FLATTENED]".to_string()
            };
            println!(
                "{badge} {rows} row(s), {cols} column(s) -> {}",
                output_path.display()
            );
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let s = serde_json::to_string(&serde_json::json!({
                "schema_version": "1.0",
                "command": "flatten",
                "ok": true,
                "rows": rows,
                "columns": cols,
                "output_path": output_path.display().to_string(),
            }))
            .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{s}");
            Ok(())
        }
    }
}

pub fn emit_skill_installed(
    installed_path: &Path,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " INSTALLED ".on_green().black().bold().to_string()
            } else {
                "[INSTALLED]".to_string()
            };
            println!("{badge} Agent skill written to {}", installed_path.display());
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let s = serde_json::to_string(&serde_json::json!({
                "schema_version": "1.0",
                "command": "skill",
                "ok": true,
                "installed_path": installed_path.display().to_string(),
            }))
            .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{s}");
            Ok(())
        }
    }
}

pub fn emit_error(err: &LoadKeelError, format: ResolvedFormat, color: bool) {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " ERROR ".on_red().white().bold().to_string()
            } else {
                "[ERROR]".to_string()
            };
            eprintln!("{badge} {}: {err}", err.code());
            if let Some(hint) = err.fix_hint() {
                eprintln!("  hint: {hint}");
            }
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let env = err.to_envelope();
            if let Ok(json) = serde_json::to_string_pretty(&env) {
                eprintln!("{json}");
            } else {
                eprintln!("{{\"ok\":false,\"error\":{{\"code\":\"{}\"}}}}", err.code());
            }
        }
    }
}
