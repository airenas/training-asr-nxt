use std::sync::Arc;

use anyhow::{self};
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

use crate::data::{errors::RunnerError, structs::FileMeta};

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
    conn: &mut r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
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

pub fn exists(
    conn: &mut r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    id: &str,
    type_: &str,
) -> anyhow::Result<bool> {
    tracing::debug!(file = id, type = type_, "Check file in database");
    let row = conn.query_one(
        "SELECT EXISTS(SELECT 1 FROM kv WHERE id = $1 AND type = $2)",
        &[&id, &type_],
    )?;
    let exists: bool = row.get(0);
    Ok(exists)
}

pub fn load(
    conn: &mut r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    id: &str,
    type_: &str,
) -> anyhow::Result<crate::data::structs::File> {
    tracing::trace!(id, type=type_, "Loading file to database");
    let row_opt: Option<postgres::Row> = conn.query_opt(
        "SELECT content FROM kv WHERE id = $1 AND type = $2",
        &[&id, &type_],
    )?;
    match row_opt {
        Some(row) => {
            let content: Vec<u8> = row.get("content");
            Ok(crate::data::structs::File {
                id: id.to_string(),
                type_: type_.to_string(),
                data: content,
            })
        }
        None => Err(RunnerError::RecordNotFound {
            id: id.to_string(),
            type_: type_.to_string(),
        }
        .into()),
    }
}

pub fn get_pool(
    url: &str,
    workers: u32,
) -> anyhow::Result<Arc<Pool<PostgresConnectionManager<NoTls>>>> {
    let size = if workers > 10 { 10 } else { workers };

    let manager: PostgresConnectionManager<NoTls> =
        PostgresConnectionManager::new(url.parse()?, NoTls);
    let pool = Pool::builder()
        .max_size(size)
        .connection_timeout(std::time::Duration::from_secs(5))
        .build(manager)?;
    Ok(Arc::new(pool))
}
