use perp_arb::{
    app,
    cli::{self, Command},
    error::AppError,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let command = cli::parse_from_env()?;
    let settings = app::AppSettings::from_env()?;

    match command {
        Command::Serve => app::run(settings).await,
        Command::PrintPlan => app::print_plan(settings),
    }
}
