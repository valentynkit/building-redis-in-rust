use std::error::Error;

use clap::Parser;
use codecrafters_redis::Cli;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::uptime;

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    let cli = Cli::parse();
    logging_init();
    codecrafters_redis::run(cli)?;
    Ok(())
}

fn logging_init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "codecrafters_redis=debug".into()),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_timer(uptime())
        .with_target(true)
        .with_writer(std::io::stderr)
        .init();
}
