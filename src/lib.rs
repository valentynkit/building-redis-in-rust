mod client;
mod command;
mod db;
mod networking;
mod resp;
use networking::Server;
use tracing::info;

pub fn run() -> Result<(), anyhow::Error> {
    info!("Starting Server");
    Server::new()?.run()
}
