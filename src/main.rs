use duckdb::{Connection, Result};
use duckdb::types::ToSql;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlTy {
    Bool,
    BigInt,
    Double,
    Varchar,
}

fn qident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn ty_sql(t: SqlTy) -> &'static str {
    match t {
        SqlTy::Bool => "BOOLEAN",
        SqlTy::BigInt => "BIGINT",
        SqlTy::Double => "DOUBLE",
        SqlTy::Varchar => "VARCHAR",
    }
}

fn rank(t: SqlTy) -> u8 {
    match t {
        SqlTy::Bool => 0,
        SqlTy::BigInt => 1,
        SqlTy::Double => 2,
        SqlTy::Varchar => 3,
    }
}

fn promote(a: SqlTy, b: SqlTy) -> SqlTy {
    if rank(a) >= rank(b) { a } else { b }
}

fn infer_value_ty(v: &Value) -> Option<SqlTy> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(SqlTy::Bool),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Some(SqlTy::BigInt)
            } else {
                Some(SqlTy::Double)
            }
        }
        Value::String(_) | Value::Array(_) | Value::Object(_) => Some(SqlTy::Varchar),
    }
}

fn infer_batch_schema(rows: &[Value]) -> HashMap<String, SqlTy> {
    let mut out = HashMap::<String, SqlTy>::new();
    for row in rows {
        let Value::Object(obj) = row else { continue };
        for (k, v) in obj {
            if let Some(t) = infer_value_ty(v) {
                out.entry(k.clone())
                    .and_modify(|cur| *cur = promote(*cur, t))
                    .or_insert(t);
            }
        }
    }
    out
}

fn existing_columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut cols = HashSet::new();
    let sql = format!(
        "SELECT column_name FROM information_schema.columns WHERE table_name = '{}'",
        table.replace('\'', "''")
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    for c in rows {
        cols.insert(c?);
    }
    Ok(cols)
}

fn ensure_table_and_columns(
    conn: &Connection,
    table: &str,
    schema: &HashMap<String, SqlTy>,
) -> Result<()> {
    let table_q = qident(table);
    let existing = existing_columns(conn, table)?;

    if existing.is_empty() {
        let mut cols: Vec<_> = schema.iter().collect();
        cols.sort_by(|a, b| a.0.cmp(b.0));
        let ddl_cols = cols
            .into_iter()
            .map(|(k, t)| format!("{} {}", qident(k), ty_sql(*t)))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("CREATE TABLE {} ({})", table_q, ddl_cols))?;
        return Ok(());
    }

    for (k, t) in schema {
        if !existing.contains(k) {
            conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table_q,
                qident(k),
                ty_sql(*t)
            ))?;
        }
    }

    Ok(())
}

fn append_dynamic_rows(
    conn: &Connection,
    table: &str,
    rows: &[Value],
    schema: &HashMap<String, SqlTy>,
) -> Result<()> {
    let mut app = conn.appender(table)?;

    for row in rows {
        let Value::Object(obj) = row else { continue };

        let mut keys: Vec<&String> = obj.keys().collect();
        keys.sort();

        app.clear_columns()?;

        let mut owned: Vec<Box<dyn ToSql>> = Vec::with_capacity(keys.len());

        for key in keys {
            app.add_column(key)?;
            let t = schema.get(key.as_str()).copied().unwrap_or(SqlTy::Varchar);
            let v = obj.get(key.as_str()).unwrap_or(&Value::Null);

            match t {
                SqlTy::Bool => {
                    owned.push(Box::new(v.as_bool()));
                }
                SqlTy::BigInt => {
                    let val = v.as_i64().or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()));
                    owned.push(Box::new(val));
                }
                SqlTy::Double => {
                    owned.push(Box::new(v.as_f64()));
                }
                SqlTy::Varchar => {
                    let s: Option<String> = match v {
                        Value::Null => None,
                        Value::String(x) => Some(x.clone()),
                        _ => Some(v.to_string()),
                    };
                    owned.push(Box::new(s));
                }
            }
        }

        let refs: Vec<&dyn ToSql> = owned.iter().map(|b| b.as_ref() as &dyn ToSql).collect();
        app.append_row(refs.as_slice())?;
    }

    app.flush()?;
    Ok(())
}

fn debug_dump_table(conn: &Connection, table: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("SELECT * FROM {}", qident(table)))?;
    let mut rows = stmt.query([])?;
    let column_names = rows
        .as_ref()
        .map(|s| s.column_names())
        .unwrap_or_default();

    while let Some(row) = rows.next()? {
        let mut parts = Vec::with_capacity(column_names.len());
        for (i, name) in column_names.iter().enumerate() {
            let value = row.get_ref(i)?.to_owned();
            parts.push(format!("{name}={value:?}"));
        }
        println!("{}", parts.join(", "));
    }

    Ok(())
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    let table = "events";

    let batch1 = vec![
        json!({"ts": 1700000000000_i64, "user_id": "u1", "event": "page_load"}),
        json!({"ts": 1700000001000_i64, "user_id": "u2", "event": "purchase", "amount": 19.99}),
    ];

    let schema1 = infer_batch_schema(&batch1);
    ensure_table_and_columns(&conn, table, &schema1)?;
    append_dynamic_rows(&conn, table, &batch1, &schema1)?;

    let batch2 = vec![
        json!({"ts": 1700000002000_i64, "user_id": "u3", "event": "signup", "plan": "pro"}),
    ];

    let schema2 = infer_batch_schema(&batch2);
    ensure_table_and_columns(&conn, table, &schema2)?;
    append_dynamic_rows(&conn, table, &batch2, &schema2)?;

    debug_dump_table(&conn, table)?;

    Ok(())
}
