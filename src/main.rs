use duckdb::Connection;
use duckdb::arrow::array::{ArrayRef, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params_from_iter;
use duckdb::vtab::arrow::ArrowVTab;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

type AppResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn invalid_input(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.into())
}

fn qident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn qliteral(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[derive(Clone, Debug, PartialEq)]
enum JsonShape {
    Unknown,
    Bool,
    BigInt,
    Double,
    Varchar,
    Array(Box<JsonShape>),
    Struct(HashMap<String, JsonShape>),
}

fn json_shape_name(shape: &JsonShape) -> &'static str {
    match shape {
        JsonShape::Unknown => "UNKNOWN",
        JsonShape::Bool => "BOOLEAN",
        JsonShape::BigInt => "BIGINT",
        JsonShape::Double => "DOUBLE",
        JsonShape::Varchar => "VARCHAR",
        JsonShape::Array(_) => "ARRAY",
        JsonShape::Struct(_) => "STRUCT",
    }
}

fn merge_json_shapes(current: &mut JsonShape, incoming: JsonShape, path: &str) -> std::result::Result<(), String> {
    if matches!(current, JsonShape::Unknown) {
        *current = incoming;
        return Ok(())
    }

    if matches!(incoming, JsonShape::Unknown) {
        return Ok(())
    }

    if *current == incoming {
        return Ok(())
    }

    let current_snapshot = current.clone();
    match (current_snapshot, incoming) {
        (JsonShape::BigInt, JsonShape::Double) => {
            *current = JsonShape::Double;
            Ok(())
        }
        (JsonShape::Double, JsonShape::BigInt) => Ok(()),
        (JsonShape::Array(_), JsonShape::Array(incoming_elem)) => {
            let JsonShape::Array(current_elem) = current else {
                return Err(format!("internal merge error at {path}: expected ARRAY"))
            };
            merge_json_shapes(current_elem.as_mut(), *incoming_elem, &format!("{path}[]"))
        }
        (JsonShape::Struct(_), JsonShape::Struct(incoming_fields)) => {
            let JsonShape::Struct(current_fields) = current else {
                return Err(format!("internal merge error at {path}: expected STRUCT"))
            };
            for (key, incoming_field_ty) in incoming_fields {
                match current_fields.get_mut(&key) {
                    Some(current_field_ty) => {
                        merge_json_shapes(
                            current_field_ty,
                            incoming_field_ty,
                            &format!("{path}.{key}")
                        )?;
                    }
                    None => {
                        current_fields.insert(key, incoming_field_ty);
                    }
                }
            }
            Ok(())
        }
        (left, right) => Err(format!(
            "incompatible types at {path}: {} vs {}",
            json_shape_name(&left),
            json_shape_name(&right)
        )),
    }
}

fn infer_json_shape(v: &Value, path: &str) -> std::result::Result<Option<JsonShape>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Bool(_) => Ok(Some(JsonShape::Bool)),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Ok(Some(JsonShape::BigInt))
            } else {
                Ok(Some(JsonShape::Double))
            }
        }
        Value::String(_) => Ok(Some(JsonShape::Varchar)),
        Value::Array(items) => {
            let mut merged = JsonShape::Unknown;
            for item in items {
                if let Some(item_shape) = infer_json_shape(item, path)? {
                    merge_json_shapes(&mut merged, item_shape, &format!("{path}[]"))?;
                }
            }
            Ok(Some(JsonShape::Array(Box::new(merged))))
        }
        Value::Object(obj) => {
            let mut fields = HashMap::new();
            for (k, child) in obj {
                if let Some(child_shape) = infer_json_shape(child, &format!("{path}.{k}"))? {
                    fields.insert(k.clone(), child_shape);
                }
            }
            Ok(Some(JsonShape::Struct(fields)))
        }
    }
}

fn infer_batch_shape(rows: &[Value]) -> AppResult<JsonShape> {
    let mut merged_batch = JsonShape::Unknown;

    for (idx, row) in rows.iter().enumerate() {
        let Some(row_shape) = infer_json_shape(row, &format!("$[{idx}]")).map_err(invalid_input)? else {
            continue;
        };
        merge_json_shapes(&mut merged_batch, row_shape, "$").map_err(invalid_input)?;
    }

    Ok(merged_batch)
}

fn shape_from_type_token(token: &str) -> JsonShape {
    let upper = token.trim().to_ascii_uppercase();
    if upper == "NULL" {
        JsonShape::Unknown
    } else if upper == "BOOLEAN" || upper == "BOOL" {
        JsonShape::Bool
    } else if upper == "BIGINT"
        || upper == "INTEGER"
        || upper == "INT"
        || upper == "SMALLINT"
        || upper == "TINYINT"
        || upper == "UBIGINT"
        || upper == "UINTEGER"
        || upper == "USMALLINT"
        || upper == "UTINYINT"
        || upper == "HUGEINT"
    {
        JsonShape::BigInt
    } else if upper == "DOUBLE"
        || upper == "DOUBLE PRECISION"
        || upper == "FLOAT"
        || upper == "REAL"
        || upper == "DECIMAL"
        || upper.starts_with("DECIMAL(")
    {
        JsonShape::Double
    } else {
        JsonShape::Varchar
    }
}

fn shape_from_structure_value(v: &Value, path: &str) -> std::result::Result<JsonShape, String> {
    match v {
        Value::String(token) => Ok(shape_from_type_token(token)),
        Value::Array(items) => {
            let mut merged = JsonShape::Unknown;
            for item in items {
                let item_shape = shape_from_structure_value(item, &format!("{path}[]"))?;
                merge_json_shapes(&mut merged, item_shape, &format!("{path}[]"))?;
            }
            Ok(JsonShape::Array(Box::new(merged)))
        }
        Value::Object(obj) => {
            let mut fields = HashMap::new();
            for (k, child) in obj {
                let child_shape = shape_from_structure_value(child, &format!("{path}.{k}"))?;
                fields.insert(k.clone(), child_shape);
            }
            Ok(JsonShape::Struct(fields))
        }
        _ => Err(format!("invalid structure JSON at {path}")),
    }
}

fn shape_from_structure_json(structure_json: &str) -> AppResult<JsonShape> {
    let v: Value = serde_json::from_str(structure_json)?;
    let shape = shape_from_structure_value(&v, "$").map_err(invalid_input)?;
    Ok(shape)
}

fn query_first_optional_string(conn: &Connection, sql: &str) -> AppResult<Option<String>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, Option<String>>(0))?;
    for row in rows {
        return Ok(row?);
    }
    Ok(None)
}

fn table_exists(conn: &Connection, table: &str) -> AppResult<bool> {
    let mut stmt = conn.prepare(
        "SELECT count(*)::BIGINT
         FROM information_schema.tables
         WHERE table_name = ?"
    )?;
    let count: i64 = stmt.query_row([table], |r| r.get(0))?;
    Ok(count > 0)
}

fn fetch_table_row_structure_json(conn: &Connection, table: &str) -> AppResult<Option<String>> {
    if !table_exists(conn, table)? {
        return Ok(None)
    }

    let sql = format!(
        "SELECT json_group_structure(to_json(t)) FROM {} t",
        qident(table)
    );
    query_first_optional_string(conn, &sql)
}

fn validate_batch_against_existing_table(
    conn: &Connection,
    table: &str,
    rows: &[Value],
) -> AppResult<bool> {
    let Some(existing_structure_json) = fetch_table_row_structure_json(conn, table)? else {
        return Err(invalid_input(format!(
            "cannot validate against {table}: no existing rows to derive schema"
        ))
        .into())
    };

    let existing_shape = shape_from_structure_json(&existing_structure_json)?;
    let incoming_shape = infer_batch_shape(rows)?;
    let mut merged_shape = existing_shape.clone();
    merge_json_shapes(&mut merged_shape, incoming_shape, "$").map_err(invalid_input)?;
    Ok(merged_shape != existing_shape)
}

fn shape_to_structure_json(shape: &JsonShape) -> Value {
    match shape {
        JsonShape::Unknown => Value::String("NULL".to_string()),
        JsonShape::Bool => Value::String("BOOLEAN".to_string()),
        JsonShape::BigInt => Value::String("BIGINT".to_string()),
        JsonShape::Double => Value::String("DOUBLE".to_string()),
        JsonShape::Varchar => Value::String("VARCHAR".to_string()),
        JsonShape::Array(inner) => Value::Array(vec![shape_to_structure_json(inner)]),
        JsonShape::Struct(fields) => {
            let mut keys: Vec<_> = fields.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                if let Some(field_shape) = fields.get(key) {
                    out.insert(key.clone(), shape_to_structure_json(field_shape));
                }
            }
            Value::Object(out)
        }
    }
}

fn json_rows_to_payload_record_batch(rows: &[Value]) -> AppResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("payload", DataType::Utf8, false)]));
    let payloads: Vec<String> = rows.iter().map(Value::to_string).collect();
    let payload_array: ArrayRef = Arc::new(StringArray::from(payloads));
    Ok(RecordBatch::try_new(schema, vec![payload_array])?)
}

fn run_arrow_query_from_json_batch(conn: &Connection, rows: &[Value], sql: &str) -> AppResult<Vec<RecordBatch>> {
    let batch = json_rows_to_payload_record_batch(rows)?;
    let params = arrow_recordbatch_to_query_params(batch);
    let mut stmt = conn.prepare(sql)?;
    Ok(stmt.query_arrow(params)?.collect())
}

fn run_arrow_query_from_json_batch_chunked(
    conn: &Connection,
    rows: &[Value],
    chunk_size: usize,
    sql_with_arrow_union_source: &str,
) -> AppResult<Vec<RecordBatch>> {
    let (_, params) = build_arrow_union_source_and_params(rows, chunk_size)?;
    let mut stmt = conn.prepare(sql_with_arrow_union_source)?;
    Ok(stmt.query_arrow(params_from_iter(params))?.collect())
}

fn build_arrow_union_source_and_params(rows: &[Value], chunk_size: usize) -> AppResult<(String, Vec<usize>)> {
    if chunk_size == 0 {
        return Err(invalid_input("chunk_size must be > 0").into())
    }

    let chunk_count = rows.chunks(chunk_size).len();
    if chunk_count == 0 {
        return Ok((String::new(), vec![]))
    }

    let union_source = (0..chunk_count)
        .map(|_| "SELECT payload AS payload_text FROM arrow(?, ?)")
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    let mut params = Vec::<usize>::with_capacity(chunk_count * 2);
    for chunk in rows.chunks(chunk_size) {
        let batch = json_rows_to_payload_record_batch(chunk)?;
        let [array_ptr, schema_ptr] = arrow_recordbatch_to_query_params(batch);
        params.push(array_ptr);
        params.push(schema_ptr);
    }

    Ok((union_source, params))
}

fn fetch_merged_json_structure_from_arrow_chunked(
    conn: &Connection,
    rows: &[Value],
    chunk_size: usize,
) -> AppResult<String> {
    if rows.is_empty() {
        return Ok("{}".to_string())
    }

    ensure_arrow_table_function(conn)?;
    let (union_source, params) = build_arrow_union_source_and_params(rows, chunk_size)?;
    let sql = format!(
        "SELECT json_group_structure(json(payload_text)) FROM ({})",
        union_source
    );

    let mut stmt = conn.prepare(&sql)?;
    let structure: Option<String> = stmt.query_row(params_from_iter(params), |r| r.get(0))?;
    Ok(structure.unwrap_or_else(|| "{}".to_string()))
}

fn ensure_arrow_table_function(conn: &Connection) -> AppResult<()> {
    let mut stmt = conn.prepare(
        "SELECT count(*)::BIGINT
         FROM duckdb_functions()
         WHERE function_name = 'arrow' AND function_type = 'table'"
    )?;
    let count: i64 = stmt.query_row([], |r| r.get(0))?;
    if count == 0 {
        conn.register_table_function::<ArrowVTab>("arrow")?;
    }
    Ok(())
}

fn query_arrow_from_json_batch(conn: &Connection, rows: &[Value], sql: &str) -> AppResult<Vec<RecordBatch>> {
    // DuckDB can scan in-memory Arrow directly, which is usually cheaper than
    // inserting JSON into a temporary table first and then scanning it.
    // This still is not fully zero-copy from serde_json::Value because we
    // serialize rows into Arrow UTF8 payload strings and DuckDB parses JSON.
    ensure_arrow_table_function(conn)?;
    run_arrow_query_from_json_batch(conn, rows, sql)
}

fn query_arrow_from_json_batch_chunked(
    conn: &Connection,
    rows: &[Value],
    merged_structure: &str,
    chunk_size: usize,
) -> AppResult<Vec<RecordBatch>> {
    ensure_arrow_table_function(conn)?;
    let (union_source, _) = build_arrow_union_source_and_params(rows, chunk_size)?;
    if union_source.is_empty() {
        return Ok(vec![])
    }
    let sql = format!(
        "SELECT parsed.*
         FROM (SELECT from_json(json(payload_text), {}) AS parsed FROM ({}))",
        qliteral(merged_structure),
        union_source
    );
    run_arrow_query_from_json_batch_chunked(conn, rows, chunk_size, &sql)
}

fn ingest_json_batch_via_arrow(
    conn: &Connection,
    typed_table: &str,
    rows: &[Value],
    chunk_size: usize,
) -> AppResult<String> {
    let _ = infer_batch_shape(rows)?;
    let incoming_structure = fetch_merged_json_structure_from_arrow_chunked(conn, rows, chunk_size)?;
    let incoming_shape = shape_from_structure_json(&incoming_structure)?;

    let merged_structure = if let Some(existing_structure) = fetch_table_row_structure_json(conn, typed_table)? {
        let existing_shape = shape_from_structure_json(&existing_structure)?;
        let mut merged_shape = existing_shape;
        merge_json_shapes(&mut merged_shape, incoming_shape, "$").map_err(invalid_input)?;
        serde_json::to_string(&shape_to_structure_json(&merged_shape))?
    } else {
        incoming_structure
    };

    ensure_arrow_table_function(conn)?;
    let (arrow_union_source, params) = build_arrow_union_source_and_params(rows, chunk_size)?;
    if arrow_union_source.is_empty() {
        return Ok(merged_structure)
    }

    let table_q = qident(typed_table);
    let source_sql = if fetch_table_row_structure_json(conn, typed_table)?.is_some() {
        format!(
            "SELECT CAST(to_json(t) AS VARCHAR) AS payload_text FROM {} t UNION ALL {}",
            table_q,
            arrow_union_source
        )
    } else {
        arrow_union_source
    };

    let sql = format!(
        "CREATE OR REPLACE TABLE {} AS
         SELECT parsed.*
         FROM (
             SELECT from_json(json(payload_text), {}) AS parsed
             FROM ({})
         )",
        table_q,
        qliteral(&merged_structure),
        source_sql
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(params))?;
    Ok(merged_structure)
}

fn export_typed_table_to_parquet(conn: &Connection, typed_table: &str, parquet_path: &str) -> AppResult<()> {
    let _ = std::fs::remove_file(parquet_path);
    let copy_sql = format!(
        "COPY (SELECT * FROM {}) TO {} (FORMAT PARQUET)",
        qident(typed_table),
        qliteral(parquet_path)
    );
    conn.execute_batch(&copy_sql)?;
    Ok(())
}

fn make_benchmark_rows(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "event_id": format!("e{i}"),
                "user": { "id": format!("u{i}"), "score": (i as f64) + 0.5_f64 },
                "items": [{ "sku": format!("sku{i}"), "qty": ((i % 5) as f64) + 1.0_f64 }],
                "meta": { "source": if i % 2 == 0 { "ad" } else { "organic" } }
            })
        })
        .collect()
}

fn benchmark_arrow_chunked_scan(conn: &Connection) -> AppResult<()> {
    let rows = make_benchmark_rows(10_000);
    let iterations = 20;
    let chunk_size = 1_000;
    let merged_structure = fetch_merged_json_structure_from_arrow_chunked(conn, &rows, chunk_size)?;

    ensure_arrow_table_function(conn)?;
    let _ = query_arrow_from_json_batch_chunked(conn, &rows, &merged_structure, chunk_size)?;

    let arrow_start = Instant::now();
    for _ in 0..iterations {
        let _ = query_arrow_from_json_batch_chunked(conn, &rows, &merged_structure, chunk_size)?;
    }
    let arrow_elapsed = arrow_start.elapsed();

    println!(
        "benchmark json->arrow(chunked)->scan: rows={} chunk_size={} total={:.2}ms avg={:.2}ms",
        rows.len(),
        chunk_size,
        arrow_elapsed.as_secs_f64() * 1000.0,
        arrow_elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );

    Ok(())
}

fn debug_dump_table_row_structure(conn: &Connection, table: &str) -> AppResult<()> {
    match fetch_table_row_structure_json(conn, table)? {
        Some(structure) => println!("typed table row structure: {structure}"),
        None => println!("typed table row structure: <no rows>"),
    }
    Ok(())
}

fn main() -> AppResult<()> {
    let conn = Connection::open_in_memory()?;
    let typed_table = "typed_events";
    let chunk_size = 1_000;

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", qident(typed_table)))?;

    let batch1 = vec![
        json!({
            "event_id": "e1",
            "user": { "id": "u1", "score": 1_i64 },
            "items": [{ "sku": "a", "qty": 1_i64 }]
        }),
        json!({
            "event_id": "e2",
            "user": { "id": "u2", "score": 2.5_f64 },
            "items": [{ "sku": "b", "qty": 3.0_f64 }]
        }),
    ];
    let merged1 = ingest_json_batch_via_arrow(&conn, typed_table, &batch1, chunk_size)?;
    println!("merged structure after batch1: {merged1}");
    let arrow_sql = format!(
        "SELECT parsed.*
         FROM (SELECT from_json(json(payload), {}) AS parsed FROM arrow(?, ?))",
        qliteral(&merged1)
    );
    let arrow_result = query_arrow_from_json_batch(&conn, &batch1, &arrow_sql)?;
    let arrow_rows: usize = arrow_result.iter().map(RecordBatch::num_rows).sum();
    println!("arrow query over batch1 rows: {arrow_rows}");

    let batch2 = vec![
        json!({
            "event_id": "e3",
            "user": { "id": "u3", "score": 4.25_f64, "plan": "pro" },
            "items": [{ "sku": "c", "qty": 2.0_f64, "discount": 0.1_f64 }],
            "meta": { "source": "ad" }
        }),
    ];
    let batch2_widens = validate_batch_against_existing_table(&conn, typed_table, &batch2)?;
    println!("batch2 compatible with existing typed table, widens schema: {batch2_widens}");
    let merged2 = ingest_json_batch_via_arrow(&conn, typed_table, &batch2, chunk_size)?;
    println!("merged structure after batch2: {merged2}");

    debug_dump_table_row_structure(&conn, typed_table)?;
    export_typed_table_to_parquet(&conn, typed_table, "target/nested_events.parquet")?;
    benchmark_arrow_chunked_scan(&conn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_typed_events(conn: &Connection, typed_table: &str) {
        let existing_batch = vec![json!({
            "event_id": "e1",
            "user": { "id": "u1", "score": 1_i64 }
        })];
        ingest_json_batch_via_arrow(conn, typed_table, &existing_batch, 1000)
            .expect("ingest existing batch");
    }

    #[test]
    fn supports_within_batch_numeric_widening() {
        let batch = vec![
            json!({"user": {"score": 1_i64}, "items": [{"qty": 1_i64}]}),
            json!({"user": {"score": 1.5_f64}, "items": [{"qty": 2.0_f64}]}),
        ];

        let result = infer_batch_shape(&batch);
        assert!(result.is_ok(), "expected widening to be valid: {result:?}");
    }

    #[test]
    fn rejects_incompatible_nested_shapes_in_batch() {
        let batch = vec![
            json!({"user": {"id": "u1"}}),
            json!({"user": 7_i64}),
        ];

        let result = infer_batch_shape(&batch);
        assert!(result.is_err(), "expected incompatible shapes to fail");
    }

    #[test]
    fn detects_widening_against_existing_table() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        let typed_table = "typed_events";
        seed_typed_events(&conn, typed_table);

        let incoming_batch = vec![json!({
            "event_id": "e2",
            "user": { "id": "u2", "score": 2.5_f64 }
        })];
        let widens = validate_batch_against_existing_table(&conn, typed_table, &incoming_batch)
        .expect("validate incoming batch");

        assert!(widens, "expected incoming batch to require widening");
    }

    #[test]
    fn detects_incompatibility_against_existing_table() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        let typed_table = "typed_events";
        seed_typed_events(&conn, typed_table);

        let incompatible_batch = vec![json!({
            "event_id": "e2",
            "user": 7_i64
        })];
        let result = validate_batch_against_existing_table(&conn, typed_table, &incompatible_batch);

        assert!(result.is_err(), "expected incompatible batch to fail validation");
    }

    #[test]
    fn detects_struct_vs_bool_conflict_on_table_column() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute_batch("CREATE TABLE flags(flag BOOLEAN); INSERT INTO flags VALUES (true)")
            .expect("seed bool table");

        let incoming_values = vec![json!({"flag": {"nested": true}})];
        let result = validate_batch_against_existing_table(&conn, "flags", &incoming_values);

        assert!(result.is_err(), "expected struct vs bool conflict");
    }

    #[test]
    fn can_query_arrow_from_json_payload_batch() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        let rows = vec![
            json!({"event_id": "e1", "value": 1_i64}),
            json!({"event_id": "e2", "value": 2_i64}),
        ];
        let sql = "SELECT count(*)::BIGINT AS c FROM arrow(?, ?)";
        let out = query_arrow_from_json_batch(&conn, &rows, sql).expect("query arrow");
        assert_eq!(out.len(), 1, "expected one output batch");
        assert_eq!(out[0].num_rows(), 1, "expected one aggregate row");
    }

    #[test]
    fn can_query_arrow_from_json_batch_chunked_in_one_query() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");

        let rows = vec![
            json!({"event_id": "e1", "value": 1_i64}),
            json!({"event_id": "e2", "value": 2_i64}),
            json!({"event_id": "e3", "value": 3_i64}),
            json!({"event_id": "e4", "value": 4_i64}),
            json!({"event_id": "e5", "value": 5_i64}),
        ];
        let merged_structure = fetch_merged_json_structure_from_arrow_chunked(&conn, &rows, 2)
            .expect("merged structure");
        let out = query_arrow_from_json_batch_chunked(&conn, &rows, &merged_structure, 2)
            .expect("query chunked arrow");
        let output_rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(output_rows, rows.len(), "expected all input rows in one query result");
    }
}
