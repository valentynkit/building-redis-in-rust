mod client;
mod command;
mod networking;
mod poll;
mod resp;
use networking::Server;
use std::io::{self};

pub fn run() -> Result<(), io::Error> {
    Server::new()?.run()
}
