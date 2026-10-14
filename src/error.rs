use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AppError {
    #[error("I/O error {0}")]
    IoError(String),
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::IoError(e.to_string())
    }
}
