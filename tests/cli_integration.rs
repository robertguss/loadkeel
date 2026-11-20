use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::path::PathBuf;
use tempfile::tempdir;

fn loadkeel() -> Command {
    Command::cargo_bin("loadkeel").expect("loadkeel binary built")
}

fn example(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/seminary-load")
        .join(rel)
}

#[test]
fn flatten_and_offline_dry_run_work_without_database_url() {
    // 1. Standalone flatten outputs clean JSONL with flattened keys & JSONB arrays intact
    let flat_out = loadkeel()
        .args(["flatten", example("populi_nested.jsonl").to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let lines: Vec<&str> = std::str::from_utf8(&flat_out).unwrap().lines().collect();
    assert_eq!(lines.len(), 3);
    let first: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["academic_program"], "MDiv");
    assert_eq!(first["academic_gpa"], 3.82);
    assert!(first["tags"].is_array());

    // 2. Offline --dry-run works cleanly even without DATABASE_URL and collapses duplicate keys
    let dry_out = loadkeel()
        .env_remove("DATABASE_URL")
        .env_remove("LOADKEEL_DATABASE_URL")
        .args([
            "load",
            example("populi_nested_day2_evolve.jsonl").to_str().unwrap(),
            "pg://staging.populi_students",
            "--flatten",
            "--key",
            "student_id",
            "--auto-create",
            "--dry-run",
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: Value = serde_json::from_slice(&dry_out).expect("valid JSON");
    assert_eq!(report["ok"], true);
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["mode"], "upsert");
    assert_eq!(report["rows_read"], 3);
    assert_eq!(report["duplicates_collapsed"], 1);
    assert_eq!(report["rows_staged"], 2);
}

#[test]
fn error_envelopes_redact_passwords_and_skill_installs() {
    let dir = tempdir().unwrap();

    // Missing DATABASE_URL on live pg:// load -> exit 2 + LK_PG_MISSING_DATABASE_URL
    let stderr = loadkeel()
        .env_remove("DATABASE_URL")
        .env_remove("LOADKEEL_DATABASE_URL")
        .args([
            "load",
            example("populi_nested.jsonl").to_str().unwrap(),
            "pg://staging.populi_students",
            "--format",
            "agent",
        ])
        .assert()
        .code(2)
        .get_output()
        .stderr
        .clone();
    let env: Value = serde_json::from_slice(&stderr).expect("valid JSON error envelope");
    assert_eq!(env["error"]["code"], "LK_PG_MISSING_DATABASE_URL");

    // Password in postgres:// URI is NEVER leaked in stderr
    let stderr2 = loadkeel()
        .args([
            "load",
            example("populi_nested.jsonl").to_str().unwrap(),
            "postgres://alice:super_secret_pw@localhost:5432/seminary",
            "--format",
            "agent",
        ])
        .assert()
        .code(2)
        .get_output()
        .stderr
        .clone();
    let raw = String::from_utf8_lossy(&stderr2);
    assert!(!raw.contains("super_secret_pw"));
    assert!(raw.contains("LK_PG_MISSING_TABLE"));

    // Skill prints and installs
    let skill_dir = dir.path().join(".agents/skills/loadkeel");
    loadkeel()
        .args(["skill", "--install", skill_dir.to_str().unwrap()])
        .assert()
        .success();
    assert!(skill_dir.join("SKILL.md").exists());
}

#[test]
fn live_postgres_copy_upsert_evolve_and_stdin_end_to_end() {
    let Ok(pg_url) = std::env::var("LOADKEEL_TEST_PG_URL") else {
        return;
    };

    // 1. Day 1: flatten + auto-create + COPY + upsert (3 rows)
    let out1 = loadkeel()
        .env("DATABASE_URL", &pg_url)
        .args([
            "load",
            example("populi_nested.jsonl").to_str().unwrap(),
            "pg://staging.populi_students",
            "--flatten",
            "--key",
            "student_id",
            "--auto-create",
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let r1: Value = serde_json::from_slice(&out1).unwrap();
    assert_eq!(r1["ok"], true);
    assert_eq!(r1["table_created"], true);
    assert_eq!(r1["rows_loaded"], 3);

    // 2. Day 2 WITHOUT --evolve fails cleanly with LK_PG_MISSING_COLUMNS (`advisor_email`)
    loadkeel()
        .env("DATABASE_URL", &pg_url)
        .args([
            "load",
            example("populi_nested_day2_evolve.jsonl").to_str().unwrap(),
            "pg://staging.populi_students",
            "--flatten",
            "--key",
            "student_id",
            "--format",
            "agent",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("LK_PG_MISSING_COLUMNS"));

    // 3. Day 2 WITH --evolve adds `advisor_email`, collapses duplicate STU-1004 (last-wins 3.98), updates STU-1003, inserts STU-1004!
    let out2 = loadkeel()
        .env("DATABASE_URL", &pg_url)
        .args([
            "load",
            example("populi_nested_day2_evolve.jsonl").to_str().unwrap(),
            "pg://staging.populi_students",
            "--flatten",
            "--key",
            "student_id",
            "--evolve",
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let r2: Value = serde_json::from_slice(&out2).unwrap();
    assert_eq!(r2["ok"], true);
    assert_eq!(r2["duplicates_collapsed"], 1);
    assert_eq!(r2["rows_loaded"], 2);
    assert_eq!(r2["columns_added"][0], "advisor_email");

    // 4. Stdin JSONL pipe upsert into the same live table!
    loadkeel()
        .env("DATABASE_URL", &pg_url)
        .args([
            "load",
            "-",
            "pg://staging.populi_students",
            "--key",
            "student_id",
            "--format",
            "agent",
        ])
        .write_stdin(
            "{\"student_id\":\"STU-1005\",\"email\":\"lydia@example.edu\",\"academic_program\":\"MABC\",\"academic_gpa\":3.6,\"academic_credits\":30,\"tags\":[\"online\"],\"advisor_email\":\"dr.owen@example.edu\"}\n",
        )
        .assert()
        .success()
        .stdout(predicate::str::contains("\"rows_loaded\":1"));
}
