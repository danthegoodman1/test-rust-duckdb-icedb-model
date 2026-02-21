We’re trying to build a **Rust ingestion path using DuckDB** that behaves like a dynamic-schema JSON ingest engine.

### Problem We’re Solving
- Input rows are arbitrary JSON objects and schema can change over time.
- In Python, this was easy via DuckDB replacement scans / in-memory table references.
- In Rust, we want the same behavior **without writing temp files**.

### What We Want in Rust
- Accept in-memory JSON rows (`Vec<serde_json::Value>` or similar).
- Infer a batch schema from those rows.
- Compare against existing table schema.
- Evolve table when needed (including type widening and nested struct/list shape growth).
- Query batches directly from in-memory Arrow chunks with DuckDB table function scans.
- Keep everything in memory (no NDJSON/parquet staging files).

### Expected Design Pattern
1. **Infer/validate batch shape in Rust** from JSON values.
2. **Build Arrow record batches** for the incoming JSON payload strings.
3. **Run DuckDB SQL over chunked `arrow(?, ?)` sources** (single query with `UNION ALL` across chunks).
4. **Materialize typed output** with `from_json(...)` using merged structure.

### Key Constraint
- No Python-style variable replacement scans in Rust binding for this flow, so schema inference and mapping logic must be done explicitly in Rust code.

### Non-Goals
- Not re-implementing full IceDB features (merge/tombstones/MVCC) here.
- Focus is just dynamic in-memory ingest + schema evolution + fast append in Rust.

### Perf Snapshot (Current Prototype)
- Ran with `cargo run --release` on this machine.
- Dataset: `10,000` JSON rows per iteration.
- Iterations: `20`.
- Arrow path: chunked Arrow scan with one SQL query across chunks (`chunk_size=1000`), with schema merge derived from the Arrow source.

Latest run:
- `json -> arrow(chunked) -> scan`: total `599.94ms`, avg `30.00ms`
