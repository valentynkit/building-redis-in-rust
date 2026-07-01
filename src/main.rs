use std::error::Error;

use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    logging_init();
    codecrafters_redis::run()?;
    Ok(())
}

fn logging_init() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "debug".into()))
        .with_writer(std::io::stderr)
        .init();
}
