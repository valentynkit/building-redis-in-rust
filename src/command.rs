use std::{
    env::args,
    io::{self},
};
use thiserror::Error;

use crate::resp::{write_bulk, write_simple};

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("unknown command '{0}'")]
    Unknown(String),
    #[error("wrong number of arguments for '{0}', expected: '{1}'")]
    WrongArity(String, String),
}

pub enum Command {
    Ping,
    Echo,
}
impl Command {
    pub fn to_string(&self) -> String {
        match self {
            Self::Ping => "PING".to_owned(),
            Self::Echo => "ECHO".to_owned(),
        }
    }
    pub fn from_string(name: &[u8]) -> Option<Self> {
        match name {
            b"PING" => Some(Self::Ping),
            b"ECHO" => Some(Self::Echo),
            _ => None,
        }
    }

    fn check_arity(&self, argc: usize) -> Result<(), CommandError> {
        let arity = self.arity();
        if (arity > 0 && argc != arity as usize) || (arity < 0 && argc < (-arity) as usize) {
            return Err(CommandError::WrongArity(self.to_string(), argc.to_string()));
        }
        Ok(())
    }
    fn arity(&self) -> i32 {
        match self {
            Command::Ping => 1,
            Command::Echo => -1,
        }
    }
}
/// All command handling lives here. This is the seam that grows into a Command enum.
pub fn dispatch(args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<(), CommandError> {
    let name = args
        .first()
        .ok_or_else(|| CommandError::Unknown(String::new()))?;
    let cmd = Command::from_string(&args[0])
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(name).into_owned()))?;
    cmd.check_arity(args.len())?;

    match cmd {
        Command::Ping => write_simple(out, "PONG"),
        // TODO after resp update
        Command::Echo => write_bulk(out, &args[1..].join(&b' ')),
    };

    Ok(())
}
