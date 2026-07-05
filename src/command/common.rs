use std::time::Duration;

use strum::{AsRefStr, EnumString};
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum CommandError {
    #[error("unknown command '{0}'")]
    Unknown(String),
    #[error("wrong number of arguments for '{0}', expected: '{1}'")]
    WrongArity(String, String),
    #[error("wrong argument format: expected number. actual '{0}'")]
    WrongNumber(String),
    #[error("key already exist with different type, expected: '{0}'")]
    WrongType(String),
}

#[derive(AsRefStr, Debug, EnumString)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum ExpCmd {
    Ex,
    Px,
}

impl ExpCmd {
    fn from_bytes(value: &[u8]) -> Option<Self> {
        str::from_utf8(value).ok()?.parse().ok()
    }
}

pub fn get_ttl(cmd: ExpCmd, exp: Option<&Vec<u8>>) -> Result<Option<Duration>, CommandError> {
    let Some(exp) = exp else {
        return Ok(None);
    };

    let number_err = CommandError::WrongNumber(String::from_utf8_lossy(exp).into());

    let n: f64 = str::from_utf8(exp)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(number_err)?;

    // exp_cmd could be EX or PX. EX = seconds, PX = milisseconds.
    let ms = match cmd {
        ExpCmd::Ex => n * (1000f64),
        ExpCmd::Px => n,
    };

    Ok(Some(Duration::from_millis(ms as u64)))
}

pub fn parse_ttl(
    exp_cmd: Option<&Vec<u8>>,
    exp: Option<&Vec<u8>>,
) -> Result<Option<Duration>, CommandError> {
    let (Some(cmd), Some(exp)) = (exp_cmd, exp) else {
        return Ok(None);
    };

    let cmd = ExpCmd::from_bytes(cmd)
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(cmd).into_owned()))?;

    get_ttl(cmd, Some(exp))
}
