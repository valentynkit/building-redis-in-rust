mod client;
mod command;
mod db;
mod networking;
mod poll;
mod resp;
use networking::Server;

pub fn run() -> Result<(), anyhow::Error> {
    Server::new()?.run()
}
