use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{app::AppSettings, error::{AppError, AppResult}};

pub fn init(settings: &AppSettings) -> AppResult<WorkerGuard> {
    std::fs::create_dir_all(&settings.log_dir)?;

    let file_appender = tracing_appender::rolling::daily(&settings.log_dir, "perp-arb.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false);

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        .try_init()
        .map_err(|error| AppError::Internal(format!("failed to initialize logging: {error}")))?;

    Ok(guard)
}
