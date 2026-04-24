use perp_arb::{
    app,
    cli::{self, Command},
    error::AppError,
    logging,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let command = cli::parse_from_env()?;
    let settings = app::AppSettings::from_env()?;
    let _log_guard = logging::init(&settings)?;

    match command {
        Command::Serve => app::run(settings).await,
        Command::PrintPlan => app::print_plan(settings),
    }
}
