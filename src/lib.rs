mod cli;
mod client;
mod command;
mod db;
mod networking;
mod resp;
pub use cli::Cli;
use networking::Server;
use tracing::info;

pub fn run(cli: Cli) -> Result<(), anyhow::Error> {
    info!("Starting Server");
    Server::new(&cli)?.run(cli.port())
}
