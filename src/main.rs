use std::error::Error;

use clap::Parser;
use codecrafters_redis::Cli;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    let cli = Cli::parse();
    logging_init();
    codecrafters_redis::run(cli)?;
    Ok(())
}

fn logging_init() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "debug".into()))
        .with_writer(std::io::stderr)
        .init();
}
