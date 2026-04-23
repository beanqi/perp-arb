use crate::{
    error::{AppError, AppResult},
};

#[derive(Clone, Debug)]
pub enum Command {
    Serve,
    PrintPlan,
}

pub fn parse_from_env() -> AppResult<Command> {
    match std::env::args().nth(1).as_deref() {
        None | Some("serve") => Ok(Command::Serve),
        Some("print-plan") => Ok(Command::PrintPlan),
        Some("--help") | Some("-h") | Some("help") => {
            println!("perp-arb [serve|print-plan]");
            std::process::exit(0);
        }
        Some(other) => Err(AppError::Validation(format!(
            "unknown command `{other}`; supported commands: `serve`, `print-plan`"
        ))),
    }
}
