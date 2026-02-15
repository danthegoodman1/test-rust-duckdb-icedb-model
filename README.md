We’re trying to build a **Rust ingestion path using DuckDB** that behaves like a dynamic-schema JSON ingest engine.

### Problem We’re Solving
- Input rows are arbitrary JSON objects and schema can change over time.
- In Python, this was easy via DuckDB replacement scans / in-memory table references.
- In Rust, we want the same behavior **without writing temp files**.

### What We Want in Rust
- Accept in-memory JSON rows (`Vec<serde_json::Value>` or similar).
- Infer a batch schema from those rows.
- Compare against existing table schema.
- Evolve table when needed (`ALTER TABLE ADD COLUMN`, optional widening rules).
- Insert rows efficiently with DuckDB **Appender API**.
- Support sparse rows (only present keys per row) while missing columns become `NULL`/default.
- Keep everything in memory (no NDJSON/parquet staging files).

### Expected Design Pattern
1. **Infer schema in Rust** from JSON values.
2. **Ensure/evolve destination table** in DuckDB.
3. For each row/batch:
   - set active appender columns (`clear_columns` + `add_column`)
   - append only available values (`append_row`)
   - flush.

### Key Constraint
- No Python-style variable replacement scans in Rust binding for this flow, so schema inference and mapping logic must be done explicitly in Rust code.

### Non-Goals
- Not re-implementing full IceDB features (merge/tombstones/MVCC) here.
- Focus is just dynamic in-memory ingest + schema evolution + fast append in Rust.
