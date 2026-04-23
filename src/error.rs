use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    NotFound(String),
    #[error("secret handling failed: {0}")]
    Crypto(String),
    #[error("{0}")]
    Internal(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),
}

impl AppError {
    pub fn lock(name: &str) -> Self {
        Self::Internal(format!("{name} lock poisoned"))
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Crypto(_) | Self::Internal(_) | Self::Io(_) | Self::Json(_) | Self::AddrParse(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorPayload {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let payload = ErrorPayload {
            error: self.to_string(),
        };

        (self.status_code(), Json(payload)).into_response()
    }
}
