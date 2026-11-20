# loadkeel

**Blazing-fast, agent-first (DX + AX) Rust JSONL / JSON / CSV / Parquet → PostgreSQL `COPY` + atomic upsert bulk loader.**

Completes the **Keel Data Stack**:
- **[`datakeel`](https://github.com/robertguss/datakeel)** — Validates extracts, catches drift, splits clean vs. quarantined rows
- **`loadkeel`** — Flattens nested API payloads & streams into PostgreSQL via native `COPY` + atomic `ON CONFLICT` upserts
- **[`sqlkeel`](https://github.com/robertguss/sqlkeel)** — Statically verifies downstream SQL views, migrations, and column-level lineage

## Why `loadkeel`?

In Python API ETL pipelines (like Populi SIS + Canvas LMS → Postgres), writing pagination/flattening glue and loading via `pandas.to_sql` or row-by-row `psycopg` loops is slow and repetitive. `loadkeel` replaces that entire load layer with a single compiled Rust binary:

- **Native PostgreSQL `COPY ... FROM STDIN` + atomic temp-table merge:** Streams into an `ON COMMIT DROP` staging table and commits `upsert` (`ON CONFLICT DO UPDATE`), `append`, or `replace` inside a single atomic transaction.
- **Last-wins in-batch deduplication:** Prevents Postgres's infamous `"ON CONFLICT DO UPDATE command cannot affect row a second time"` error when API pagination returns overlapping records.
- **Nested JSON flattening (`--flatten`):** Turns nested SaaS API objects (`academic.gpa` → `academic_gpa`) into clean columns while keeping arrays as `jsonb`.
- **Contract-driven or schema-evolving:** Accepts **ODCS v3 YAML** / **`datakeel.toml`** contracts (`--contract`) to enforce exact Postgres types & PKs, plus `--auto-create` and `--evolve` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).
- **Offline `--dry-run` + First-class AX:** Preview exact DDL & SQL plans before touching Postgres, strict `stdout`/`stderr` split, `pg://schema.table` via `DATABASE_URL` (zero passwords in CLI args), and bundled `SKILL.md` (`loadkeel skill --install`).

## Installation

```bash
# Rust / Cargo
cargo install --git https://github.com/robertguss/loadkeel

# Python / uv (once published to PyPI)
uvx loadkeel --help
uv tool install loadkeel
```

## Quickstart

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/seminary"

# 1. Preview the flattened schema & SQL upsert plan offline (touches nothing)
loadkeel load examples/seminary-load/populi_nested.jsonl pg://staging.populi_students \
  --flatten --key student_id --auto-create --dry-run

# 2. Load day 1 (auto-creates staging.populi_students with PRIMARY KEY (student_id))
loadkeel load examples/seminary-load/populi_nested.jsonl pg://staging.populi_students \
  --flatten --key student_id --auto-create

# 3. Load day 2 with a newly appearing `advisor_email` column + duplicate PK in batch!
loadkeel load examples/seminary-load/populi_nested_day2_evolve.jsonl pg://staging.populi_students \
  --flatten --key student_id --evolve

# 4. Pipe straight from a Python extractor via stdin
python extract_populi.py | loadkeel load - pg://staging.populi_students --key student_id --auto-create

# 5. Install the built-in AI Agent Skill into your repo
loadkeel skill --install .agents/skills/loadkeel
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
