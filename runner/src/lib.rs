use std::{path::Path, sync::Arc};

use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

use crate::data::structs::FileMeta;

pub mod data;
pub mod files;
pub mod utils;
pub mod db;
pub mod worker;

pub const APP_NAME: &str = "runner";

// #[derive(Clone)]
pub struct Params<'a> {
    pub worker_index: u16,
    pub input_base_dir: &'a str,
    pub cmd: &'a str,
    pub input_files: &'a Vec<String>,
    pub output_files: &'a Vec<String>,
    pub file_meta: &'a  FileMeta,
    pub pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    pub run_f: Box<dyn FnMut(&str, &str, &Path, &Path, &str) -> anyhow::Result<String>+ 'a>,
}

#[derive(Clone)]
pub struct FileParams<'a> {
    pub input_dir: &'a str,
    pub output_dir: &'a str,
    pub file_name: &'a str,
    pub result_file_name: &'a str,
    pub cmd: &'a str,
    pub same_dir: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessStatus {
    Success,
    Skipped,
}
