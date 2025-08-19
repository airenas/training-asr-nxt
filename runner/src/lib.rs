pub mod data;
pub mod files;
pub mod utils;
pub mod db;

pub const APP_NAME: &str = "runner";

#[derive(Clone)]
pub struct Params<'a> {
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
