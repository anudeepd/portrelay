use anyhow::{Context, Result};
use clap::Parser;

use portrelay::{
    cli::{Cli, Command},
    config::load_settings,
    server,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => {
            let settings = load_settings(&args).context("failed to load relay configuration")?;
            portrelay::cli::init_logging(&settings.log_level)?;
            server::run_until_signal(settings).await
        }
    }
}
