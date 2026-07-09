pub mod common;
mod list;
mod stream;
mod string;
use crate::client::ClientId;
use crate::command::common::CommandError;
use crate::command::list::Side;
use crate::db::Db;
use crate::resp::{Reply, Resp};
use strum::{AsRefStr, Display, EnumString};
use tracing::field::Empty;
use tracing::{Span, debug, field, info, instrument};

#[derive(AsRefStr, EnumString, Debug, Display, Clone, Copy)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Rpush,
    Lpush,
    Lrange,
    Llen,
    Lpop,
    Blpop,
    Type,
    Xadd,
    Xrange,
    Xread,
    Incr,
}

impl Command {
    const fn arity(self) -> i32 {
        match self {
            Self::Ping => 1,
            Self::Echo => -1,
            Self::Set => -3,
            Self::Get => 2,
            Self::Rpush | Self::Lpush => -3,
            Self::Lrange => 4,
            Self::Llen => 2,
            Self::Lpop => -2,
            Self::Blpop => -2,
            Self::Type => 2,
            Self::Xadd => 5,
            Self::Xrange => 4,
            Self::Xread => -4,
            Self::Incr => 2,
        }
    }

    fn check_arity(&self, argc: usize) -> Result<(), CommandError> {
        let arity = self.arity();
        if (arity > 0 && argc != arity as usize) || (arity < 0 && argc < (-arity) as usize) {
            debug!(actual = argc, expected = arity, "wrong arity");
            return Err(CommandError::WrongArity(
                self.as_ref().to_owned(),
                argc.to_string(),
            ));
        }
        Ok(())
    }

    fn from_bytes(value: &[u8]) -> Result<Self, CommandError> {
        str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<Self>().ok())
            .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(value).into_owned()))
    }
}

/// All command handling lives here. This is the seam that grows into a Command enum.

#[instrument(skip(frame, db, client_id), fields(cmd = Empty))]
pub fn handle(frame: Resp, db: &mut Db, client_id: ClientId) -> Result<Reply, CommandError> {
    let args: Vec<Vec<u8>> = frame
        .into_args()
        .ok_or_else(|| CommandError::Unknown(String::new()))?;
    let kind: Command = Command::from_bytes(&args[0])?;
    kind.check_arity(args.len())?;
    Span::current().record("cmd", field::display(&kind));
    info!(command = ?kind, "handling cmd");
    match kind {
        Command::Ping => Ok(cmd_ping()),
        Command::Echo => Ok(cmd_echo(&args[1])),
        Command::Get => string::get(db, &args[1]),
        Command::Set => string::set(
            db,
            &args[1],
            &args[2],
            args.get(3).map(Vec::as_slice),
            args.get(4).map(Vec::as_slice),
        ),
        Command::Lpush => list::push(db, &Side::Front, &args[1], &args[2..args.len()]),
        Command::Rpush => list::push(db, &Side::Back, &args[1], &args[2..args.len()]),
        Command::Llen => list::llen(db, &args[1]),
        Command::Lpop => list::lpop(db, &args[1], args.get(2).map(Vec::as_slice)),
        Command::Lrange => list::lrange(db, &args[1], &args[2], &args[3]),
        Command::Blpop => list::blpop(db, &args[1], args.get(2).map(Vec::as_slice), client_id),
        Command::Type => Ok(string::cmd_type(db, &args[1])),
        Command::Xadd => stream::xadd(db, &args[1], &args[2], &args[3..args.len()]),
        Command::Xrange => stream::xrange(db, &args[1], &args[2], &args[3]),
        Command::Xread => stream::xread(db, client_id, &args[1..args.len()]),
        Command::Incr => string::incr(db, &args[1]),
    }
}

fn cmd_ping() -> Reply {
    Resp::Simple("PONG".to_owned()).into()
}

fn cmd_echo(arg: &[u8]) -> Reply {
    Resp::Bulk(Some(arg.into())).into()
}
