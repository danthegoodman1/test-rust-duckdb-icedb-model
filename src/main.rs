use duckdb::Connection;
use serde_json::{json, Value};
use std::collections::HashMap;

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

fn fetch_table_row_structure_json(conn: &Connection, table: &str) -> AppResult<Option<String>> {
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

fn insert_raw_json_rows(conn: &Connection, table: &str, rows: &[Value]) -> AppResult<()> {
    let sql = format!("INSERT INTO {} (payload) VALUES (json(?))", qident(table));
    let mut stmt = conn.prepare(&sql)?;
    for row in rows {
        let payload = row.to_string();
        stmt.execute([payload.as_str()])?;
    }
    Ok(())
}

fn fetch_merged_json_structure(conn: &Connection, table: &str) -> AppResult<String> {
    let sql = format!("SELECT json_group_structure(payload) FROM {}", qident(table));
    Ok(query_first_optional_string(conn, &sql)?.unwrap_or_else(|| "{}".to_string()))
}

fn materialize_nested_typed_table(
    conn: &Connection,
    raw_table: &str,
    typed_table: &str,
    merged_structure: &str,
) -> AppResult<()> {
    let sql = format!(
        "CREATE OR REPLACE TABLE {} AS
         SELECT parsed.*
         FROM (SELECT from_json(payload, ?) AS parsed FROM {})",
        qident(typed_table),
        qident(raw_table)
    );
    conn.execute(&sql, [merged_structure])?;
    Ok(())
}

fn ingest_json_batch(
    conn: &Connection,
    raw_table: &str,
    typed_table: &str,
    rows: &[Value],
) -> AppResult<String> {
    let _ = infer_batch_shape(rows)?;
    insert_raw_json_rows(conn, raw_table, rows)?;
    let merged_structure = fetch_merged_json_structure(conn, raw_table)?;
    materialize_nested_typed_table(conn, raw_table, typed_table, &merged_structure)?;
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

fn debug_dump_table_row_structure(conn: &Connection, table: &str) -> AppResult<()> {
    match fetch_table_row_structure_json(conn, table)? {
        Some(structure) => println!("typed table row structure: {structure}"),
        None => println!("typed table row structure: <no rows>"),
    }
    Ok(())
}

fn main() -> AppResult<()> {
    let conn = Connection::open_in_memory()?;
    let raw_table = "raw_events";
    let typed_table = "typed_events";

    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {} (payload JSON)",
        qident(raw_table)
    ))?;

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
    let merged1 = ingest_json_batch(&conn, raw_table, typed_table, &batch1)?;
    println!("merged structure after batch1: {merged1}");

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
    let merged2 = ingest_json_batch(&conn, raw_table, typed_table, &batch2)?;
    println!("merged structure after batch2: {merged2}");

    debug_dump_table_row_structure(&conn, typed_table)?;
    export_typed_table_to_parquet(&conn, typed_table, "target/nested_events.parquet")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_raw_events_table(conn: &Connection, raw_table: &str) {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {} (payload JSON)",
            qident(raw_table)
        ))
        .expect("create raw table");
    }

    fn seed_typed_events(conn: &Connection, raw_table: &str, typed_table: &str) {
        let existing_batch = vec![json!({
            "event_id": "e1",
            "user": { "id": "u1", "score": 1_i64 }
        })];
        ingest_json_batch(conn, raw_table, typed_table, &existing_batch).expect("ingest existing batch");
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
        let raw_table = "raw_events";
        let typed_table = "typed_events";
        setup_raw_events_table(&conn, raw_table);
        seed_typed_events(&conn, raw_table, typed_table);

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
        let raw_table = "raw_events";
        let typed_table = "typed_events";
        setup_raw_events_table(&conn, raw_table);
        seed_typed_events(&conn, raw_table, typed_table);

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
}
