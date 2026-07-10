use std::time::Duration;

use strum::{AsRefStr, EnumString};
use thiserror::Error;

use crate::{
    client::ClientMode,
    db::Db,
    resp::{Reply, Resp},
};

pub type HandleCmdResult = Result<Reply, CommandError>;

pub enum BlockMode {
    NotBlocking,
    Forever,
    Timeout(Duration),
}

#[derive(Debug, Error, Clone)]
pub enum CommandError {
    #[error("unknown command '{0}'")]
    Unknown(String),
    #[error("wrong number of arguments for '{0}', expected: '{1}'")]
    WrongArity(String, String),
    #[error("wrong argument format: expected number. actual '{0}'")]
    WrongNumber(String),
    #[error("wrong argument format, could not parse stream: expected stream. actual '{0}'")]
    ParseStream(String),
    #[error("The ID specified in XADD is equal or smaller than the target stream top item")]
    InvalidStream,
    #[error("The ID specified in XADD must be greater than 0-0")]
    InvalidStreamZero,
    #[error("key already exist with different type, expected: '{0}'")]
    WrongType(String),
    #[error("value is not an integer or out of range")]
    NotAnInteger,
    #[error("EXEC without MULTI")]
    ExecTransaction,
    #[error("DISCARD without MULTI")]
    DiscardTransaction,
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

pub fn exec(_db: &mut Db, mode: ClientMode) -> HandleCmdResult {
    // TODO:
    // we should execute all the queud commands and gather their responses into Resp::Array like:
    /*
    > EXEC
    1) OK
    2) (integer) 42
    */
    if mode != ClientMode::Transaction {
        return Err(CommandError::ExecTransaction);
    }

    Ok(Resp::Array(None).into())
}

// just returning initial request, used for Transaction processing, which doesn't execute command,
// but just return the initial request, so the consumer could add it to queue.
pub fn get_initial_request(elems: Vec<Vec<u8>>) -> Reply {
    let request: Resp = elems
        .into_iter()
        .map(|item| Resp::Bulk(Some(item)))
        .collect();
    Reply::AddTransaction(request)
}

pub fn get_ttl(cmd: &ExpCmd, exp: Option<&[u8]>) -> Result<Option<Duration>, CommandError> {
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
    exp_cmd: Option<&[u8]>,
    exp: Option<&[u8]>,
) -> Result<Option<Duration>, CommandError> {
    let (Some(cmd), Some(exp)) = (exp_cmd, exp) else {
        return Ok(None);
    };

    let cmd = ExpCmd::from_bytes(cmd)
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(cmd).into_owned()))?;

    get_ttl(&cmd, Some(exp))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn get_ttl_without_expiry_is_none() {
        assert!(get_ttl(&ExpCmd::Ex, None).unwrap().is_none());
    }

    #[test]
    fn get_ttl_ex_is_seconds() {
        let got = get_ttl(&ExpCmd::Ex, Some(b"10".as_ref())).unwrap().unwrap();
        assert_eq!(got, Duration::from_secs(10));
    }

    #[test]
    fn get_ttl_px_is_milliseconds() {
        let got = get_ttl(&ExpCmd::Px, Some(b"500".as_ref()))
            .unwrap()
            .unwrap();
        assert_eq!(got, Duration::from_millis(500));
    }

    #[test]
    fn get_ttl_rejects_malformed_number() {
        assert!(matches!(
            get_ttl(&ExpCmd::Ex, Some(b"soon".as_ref())),
            Err(CommandError::WrongNumber(_))
        ));
    }

    #[test]
    fn parse_ttl_without_cmd_or_exp_is_none() {
        assert!(parse_ttl(None, None).unwrap().is_none());
        assert!(parse_ttl(Some(b"EX".as_ref()), None).unwrap().is_none());
    }

    #[test]
    fn parse_ttl_accepts_lowercase_ex() {
        let got = parse_ttl(Some(b"ex".as_ref()), Some(b"1".as_ref()))
            .unwrap()
            .unwrap();
        assert_eq!(got, Duration::from_secs(1));
    }

    #[test]
    fn parse_ttl_rejects_unknown_cmd() {
        assert!(matches!(
            parse_ttl(Some(b"BOGUS".as_ref()), Some(b"1".as_ref())),
            Err(CommandError::Unknown(_))
        ));
    }
}
