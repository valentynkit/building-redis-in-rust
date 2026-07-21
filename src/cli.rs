const PORT_RANGE: RangeInclusive<usize> = 1..=65535;
use std::ops::RangeInclusive;

use clap::Parser;
use thiserror::Error;
#[derive(Parser, Debug)]
#[command(name = "redis", version, about, long_about = "Long about")]
pub struct Cli {
    /// Path to the vault directory.
    #[arg(value_parser = port_in_range, long, global = true, default_value = "6379")]
    port: u16,
    #[arg(long, global = true)]
    replicaof: Option<String>,

    #[arg(long, global = true, default_value = ".")]
    dir: String,

    #[arg(long, global = true, default_value = "dump.rdb")]
    dbfilename: String,
}

#[derive(Debug, Error, Clone)]
pub enum CliError {
    #[error("Port not in in range {0}-{1}")]
    PortNotInRange(usize, usize),
    #[error("{0} isn't a number")]
    NotANumber(String),
}

impl Cli {
    pub const fn port(&self) -> u16 {
        self.port
    }

    pub const fn dir(&self) -> &str {
        self.dir.as_str()
    }

    pub const fn dbfilename(&self) -> &str {
        self.dbfilename.as_str()
    }

    pub fn parse_replicaof(&self) -> Result<Option<(String, u16)>, CliError> {
        if let Some(value) = self.replicaof.as_deref()
            && let Some(parts) = value.split_once(' ')
        {
            let (host, port): (&str, &str) = (parts.0, parts.1);
            let port = port_in_range(port)?;
            Ok(Some((host.into(), port)))
        } else {
            Ok(None)
        }
    }
}

fn port_in_range(s: &str) -> Result<u16, CliError> {
    let port: usize = s.parse().map_err(|_| CliError::NotANumber(s.into()))?;
    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(CliError::PortNotInRange(
            PORT_RANGE.start().to_owned(),
            PORT_RANGE.end().to_owned(),
        ))
    }
}
