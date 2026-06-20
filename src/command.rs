use std::{
    env::args,
    io::{self},
};

use crate::command;

pub enum Command {
    Ping,
    Echo,
}
impl Command {
    pub fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"PING" => Some(Self::Ping),
            b"ECHO" => Some(Self::Echo),
            _ => None,
        }
    }

    fn check_arity(&self, argc: usize) -> io::Result<()> {
        let arity = self.arity();
        if (arity > 0 && argc != arity as usize) || (arity < 0 && argc < (-arity) as usize) {
            eprintln!("wrong number of arguments: arity: {arity}, actual argc: {argc}");
            return Err(io::Error::other(""));
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
pub fn dispatch(args: &[Vec<u8>], out: &mut Vec<u8>) -> io::Result<()> {
    if args.is_empty() {
        eprintln!("args empty");
        return Err(io::Error::other("args empty"));
    }
    let cmd = Command::from_name(&args[0]).ok_or_else(|| io::Error::other("unknown command"))?;
    cmd.check_arity(args.len())?;
    out.extend_from_slice(b"+PONG\r\n"); // stub

    Ok(())
}
