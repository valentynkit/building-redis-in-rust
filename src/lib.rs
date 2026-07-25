mod cli;
mod client;
mod command;
mod db;
mod networking;
mod resp;
pub use cli::Cli;
use networking::Server;

pub fn run(cli: Cli) -> Result<(), anyhow::Error> {
    Server::new(&cli)?.run(cli.port())
}
