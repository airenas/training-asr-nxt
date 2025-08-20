use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("KV '{id}:{type_}' not found")]
    RecordNotFound { id: String, type_: String },
}
