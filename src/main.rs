use clap::Parser;
use loadkeel::cli::{Cli, Commands};
use loadkeel::data::{flatten_dataset, load_contract_schema, load_input};
use loadkeel::error::{EXIT_ERROR, EXIT_OK, LoadKeelError, Result};
use loadkeel::loader::{self, LoadOptions};
use loadkeel::output::{self, ResolvedFormat};
use loadkeel::skill;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = output::resolve_format(cli.format);
    let use_color = output::should_use_color(cli.no_color, format);

    match run(cli, format, use_color) {
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            output::emit_error(&err, format, use_color);
            ExitCode::from(EXIT_ERROR as u8)
        }
    }
}

fn run(cli: Cli, format: ResolvedFormat, use_color: bool) -> Result<i32> {
    match cli.command {
        Commands::Load {
            input,
            target,
            input_format,
            json_path,
            table,
            mode,
            key,
            contract,
            flatten,
            flatten_sep,
            auto_create,
            evolve,
            no_dedupe,
            dry_run,
        } => {
            let mut ds = load_input(&input, input_format, json_path.as_deref())?;
            if flatten {
                ds = flatten_dataset(ds, &flatten_sep);
            }
            let contract_schema = match contract {
                Some(ref p) => Some(load_contract_schema(p)?),
                None => None,
            };

            let report = loader::execute_load(
                &ds,
                &target,
                &LoadOptions {
                    mode,
                    keys: key,
                    contract: contract_schema,
                    auto_create,
                    evolve,
                    dedupe: !no_dedupe,
                    dry_run,
                    explicit_table: table,
                },
            )?;

            output::emit_load_report(&report, format, use_color)?;
            Ok(EXIT_OK)
        }

        Commands::Flatten {
            input,
            input_format,
            json_path,
            sep,
            output: out_path,
        } => {
            let ds = load_input(&input, input_format, json_path.as_deref())?;
            let flat = flatten_dataset(ds, &sep);

            if let Some(path) = out_path {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    fs::create_dir_all(parent).map_err(|source| LoadKeelError::IoWrite {
                        path: path.clone(),
                        source,
                    })?;
                }
                let file = File::create(&path).map_err(|source| LoadKeelError::IoWrite {
                    path: path.clone(),
                    source,
                })?;
                let mut w = BufWriter::new(file);
                for row in &flat.rows {
                    serde_json::to_writer(&mut w, row)
                        .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
                    w.write_all(b"\n").map_err(|source| LoadKeelError::IoWrite {
                        path: path.clone(),
                        source,
                    })?;
                }
                w.flush().map_err(|source| LoadKeelError::IoWrite {
                    path: path.clone(),
                    source,
                })?;
                output::emit_flatten_saved(
                    flat.row_count(),
                    flat.columns.len(),
                    &path,
                    format,
                    use_color,
                )?;
            } else {
                let stdout = std::io::stdout();
                let mut w = BufWriter::new(stdout.lock());
                for row in &flat.rows {
                    serde_json::to_writer(&mut w, row)
                        .map_err(|e| LoadKeelError::General(format!("JSON encode error: {e}")))?;
                    w.write_all(b"\n")
                        .map_err(|e| LoadKeelError::General(format!("stdout write error: {e}")))?;
                }
                w.flush()
                    .map_err(|e| LoadKeelError::General(format!("stdout flush error: {e}")))?;
            }
            Ok(EXIT_OK)
        }

        Commands::Skill { install } => {
            if let Some(dir) = install {
                let installed = skill::install_skill(&dir)?;
                output::emit_skill_installed(&installed, format, use_color)?;
            } else {
                print!("{}", skill::SKILL_MD);
            }
            Ok(EXIT_OK)
        }
    }
}
