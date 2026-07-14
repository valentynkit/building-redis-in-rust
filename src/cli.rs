const PORT_RANGE: RangeInclusive<usize> = 1..=65535;
use std::ops::RangeInclusive;

use clap::Parser;
#[derive(Parser, Debug)]
#[command(name = "redis", version, about, long_about = "Long about")]
pub struct Cli {
    /// Path to the vault directory.
    #[arg(value_parser = port_in_range, long, global = true, default_value = "6380")]
    port: u16,
}

impl Cli {
    pub fn get_port(&self) -> u16 {
        self.port
    }
}

fn port_in_range(s: &str) -> Result<u16, String> {
    let port: usize = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a port number"))?;
    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(format!(
            "port not in range {}-{}",
            PORT_RANGE.start(),
            PORT_RANGE.end()
        ))
    }
}
