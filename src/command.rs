use crate::db::{Db, Key, Value};
use crate::resp::{self, ResponseKind};
use std::time::Duration;
use std::{
    env::args,
    io::{self},
};
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
}

#[derive(AsRefStr, EnumString)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Rpush,
}

#[derive(AsRefStr, EnumString)]
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

impl Command {
    fn check_arity(&self, argc: usize) -> Result<(), CommandError> {
        let arity = self.arity();
        if (arity > 0 && argc != arity as usize) || (arity < 0 && argc < (-arity) as usize) {
            return Err(CommandError::WrongArity(
                self.as_ref().to_owned(),
                argc.to_string(),
            ));
        }
        Ok(())
    }

    fn from_bytes(value: &[u8]) -> Option<Self> {
        str::from_utf8(value).ok()?.parse().ok()
    }

    fn arity(&self) -> i32 {
        match self {
            Command::Ping => 1,
            Command::Echo => -1,
            Command::Set => -3,
            Command::Get => 2,
            Command::Rpush => 3,
        }
    }
}

/// All command handling lives here. This is the seam that grows into a Command enum.
pub fn dispatch(db: &mut Db, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<(), CommandError> {
    let name = args
        .first()
        .ok_or_else(|| CommandError::Unknown(String::new()))?;
    let cmd = Command::from_bytes(&args[0])
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(name).into_owned()))?;
    cmd.check_arity(args.len())?;

    match cmd {
        Command::Ping => resp::write_out(ResponseKind::SIMPLE("PONG"), out),
        // TODO after resp update
        Command::Echo => resp::write_out(ResponseKind::BULK(&args[1..].join(&b' ')), out),
        Command::Set => {
            if (args.len() > 3) {
                cmd_setex(db, &args[1], &args[2], &args[3], &args[4])?;
            } else {
                cmd_set(db, &args[1], &args[2]);
            }
            resp::write_out(ResponseKind::SIMPLE_OK, out);
        }
        Command::Get => match cmd_get(db, &args[1]) {
            Some(v) => resp::write_out(ResponseKind::BULK(&v), out),
            None => resp::write_out(ResponseKind::NULL_BULK, out),
        },
        Command::Rpush => {
            let len = cmd_rpush(db, &args[1], &args[2]);
            resp::write_out(ResponseKind::Int(len), out);
        }
    };

    Ok(())
}

// TODO: actual resp value comes in "val" not just val. currently we process only val case without
// ""
fn cmd_rpush(db: &mut Db, key: &Vec<u8>, elem: &Vec<u8>) -> i64 {
    let key: Key = key.into();
    let elem: Value = elem.into();
    db.upsert_elem(key, elem)
}
fn cmd_get(db: &mut Db, key: &Vec<u8>) -> Option<Vec<u8>> {
    let key: Key = key.into();
    let value = db.get(&key)?; // None → key absent → caller writes $-1
    Some(value.into()) // &Value → Vec<u8> via the reverse From impl
}

fn cmd_setex(
    db: &mut Db,
    key: &Vec<u8>,
    value: &Vec<u8>,
    exp_cmd: &Vec<u8>,
    exp: &Vec<u8>,
) -> Result<(), CommandError> {
    let exp_cmd = ExpCmd::from_bytes(exp_cmd)
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(exp_cmd).into_owned()))?;

    let number_err = CommandError::WrongNumber(String::from_utf8_lossy(exp).into());

    let mut exp_number: u64 = String::from_utf8(exp.to_owned())
        .ok()
        .ok_or_else(|| number_err.clone())?
        .parse()
        .ok()
        .ok_or_else(|| number_err.clone())?;

    match exp_cmd {
        ExpCmd::Px => {}
        ExpCmd::Ex => exp_number *= 1000,
    }

    let exp_at: Duration = db.realtime_ms() + Duration::from_millis(exp_number);
    let key: Key = key.into();
    let value: Value = value.into();
    db.setex(key, value, Some(exp_at));
    Ok(())

    // exp_cmd could be EX or PX. EX = seconds, PX = milisseconds.
}
fn cmd_set(db: &mut Db, key: &Vec<u8>, value: &Vec<u8>) {
    let key: Key = key.into();
    let value: Value = value.into();
    db.setex(key, value, None);
}
