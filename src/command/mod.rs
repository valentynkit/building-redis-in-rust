pub mod common;
mod list;
mod stream;
mod string;
use crate::client::{Client, ClientId, ClientMode};
use crate::command::common::CommandError;
use crate::command::list::Side;
use crate::db::Db;
use crate::resp::{Reply, Resp};
use strum::{AsRefStr, Display, EnumString};
use tracing::field::Empty;
use tracing::{Span, debug, field, info, instrument};

#[derive(Copy, Clone)]
pub struct ClientInfo {
    id: ClientId,
    mode: ClientMode,
}
impl ClientInfo {
    pub fn new(id: ClientId, mode: ClientMode) -> Self {
        Self { id, mode }
    }
}
pub struct RequestCmd {
    frame: Resp,
    client: ClientInfo,
}

impl RequestCmd {
    pub fn new(frame: Resp, client: ClientInfo) -> Self {
        Self { frame, client }
    }
    pub fn update_frame(&mut self, frame: Resp) {
        self.frame = frame;
    }
}

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
    Multi,
    Exec,
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
            Self::Multi | Self::Exec => 1,
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
fn handle_normal_mode(
    db: &mut Db,
    kind: Command,
    args: Vec<Vec<u8>>,
    client_id: ClientId,
) -> Result<Reply, CommandError> {
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
        Command::Multi => Ok(Reply::StartTransaction),
        Command::Exec => Err(CommandError::TransactionError),
    }
}

fn handle_transaction_mode(kind: Command, args: Vec<Vec<u8>>) -> Result<Reply, CommandError> {
    // rebuild the request
    match kind {
        Command::Exec => Ok(Reply::ExecTransaction),
        Command::Multi => Err(CommandError::TransactionError),
        _ => Ok(common::get_initial_request(args)),
    }
}

/// All command handling lives here. This is the seam that grows into a Command enum.
#[instrument(skip(request, db), fields(cmd = Empty))]
pub fn handle(db: &mut Db, request: RequestCmd) -> Result<Reply, CommandError> {
    let args: Vec<Vec<u8>> = request
        .frame
        .into_args()
        .ok_or_else(|| CommandError::Unknown(String::new()))?;

    let kind: Command = Command::from_bytes(&args[0])?;
    let client = request.client;
    kind.check_arity(args.len())?;
    Span::current().record("cmd", field::display(&kind));
    info!(command = ?kind, "handling cmd");
    let client_mode = request.client.mode;
    match client_mode {
        ClientMode::Normal => handle_normal_mode(db, kind, args, client.id),
        ClientMode::Transaction => handle_transaction_mode(kind, args),
    }
}

fn cmd_ping() -> Reply {
    Resp::Simple("PONG".to_owned()).into()
}

fn cmd_echo(arg: &[u8]) -> Reply {
    Resp::Bulk(Some(arg.into())).into()
}
