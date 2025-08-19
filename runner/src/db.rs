use std::sync::Arc;

use anyhow::{self};
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

use crate::data::structs::FileMeta;

pub fn collect_files(
    db: Arc<Pool<PostgresConnectionManager<NoTls>>>,
) -> anyhow::Result<Vec<FileMeta>> {
    tracing::trace!("loading");

    let mut conn = db.get()?;

    let rows = conn.query(
        "SELECT id, path, duration_in_sec FROM files ORDER BY path",
        &[],
    )?;

    let mut result = Vec::new();
    for row in rows {
        result.push(FileMeta {
            id: row.get("id"),
            path: row.get("path"),
            duration_in_sec: row.get::<_, f64>("duration_in_sec") as f32,
        });
    }
    tracing::info!(len = result.len(), "Files collected from database");

    Ok(result)
}

pub fn save(
    mut conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    file_data: crate::data::structs::File,
) -> anyhow::Result<()> {
    tracing::debug!(file = file_data.id, "Saving file to database");
    conn.execute(
        "INSERT INTO kv (id, type, content) VALUES ($1, $2, $3) 
         ON CONFLICT (id, type) DO UPDATE SET content = EXCLUDED.content",
        &[&file_data.id, &file_data.type_, &file_data.data],
    )?;
    Ok(())
}

pub fn load(
    mut conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    id: &str,
    type_: &str,
) -> anyhow::Result<crate::data::structs::File> {
    tracing::debug!(id, type=type_, "Loading file to database");
    let row = conn.query_one(
        "SELECT content FROM kv WHERE id = $1 AND type = $2",
        &[&id, &type_],
    )?;
    let content: Vec<u8> = row.get("content");
    Ok(crate::data::structs::File {
        id: id.to_string(),
        type_: type_.to_string(),
        data: content,
    })
}
