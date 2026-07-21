pub mod common;
mod list;
mod stream;
mod string;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, Read};
use std::mem;
use std::rc::Rc;

use crate::client::{ClientId, ClientMode, ClientRole};
use crate::command::common::{CommandError, HandleCmdResult};
use crate::command::list::Side;
use crate::db::Db;
use crate::networking::ServerInfo;
use crate::resp::{Reply, RespBody};
use strum::{AsRefStr, Display, EnumString};
use tracing::{debug, error, field, info, warn, Span};

#[derive(Clone)]
pub struct ClientInfo {
    id: ClientId,
    mode: ClientMode,
    role: ClientRole,
    server_info: Rc<RefCell<ServerInfo>>,
}

impl ClientInfo {
    pub const fn new(
        id: ClientId,
        mode: ClientMode,
        role: ClientRole,
        server_info: Rc<RefCell<ServerInfo>>,
    ) -> Self {
        Self {
            id,
            mode,
            role,
            server_info,
        }
    }
}

pub struct Command {
    kind: CommandKind,
    pub client: ClientInfo,
    args: Vec<Vec<u8>>,
}

impl Command {
    pub fn new(frame: RespBody, client: ClientInfo) -> Result<Self, CommandError> {
        let args: Vec<Vec<u8>> = frame
            .into_args()
            .ok_or_else(|| CommandError::Unknown(String::new()))?;

        let kind: CommandKind = CommandKind::new(args.len(), &args[0])?;
        Ok(Self { kind, client, args })
    }

    pub fn execute(&mut self, db: &mut Db) -> Result<Reply, CommandError> {
        match self.client.mode {
            ClientMode::Normal => self.handle_normal_mode(db),
            ClientMode::Transaction => self.handle_transaction_mode(db),
        }
    }

    fn handle_transaction_mode(&mut self, db: &mut Db) -> Result<Reply, CommandError> {
        // rebuild the request
        let args = mem::take(&mut self.args);
        let client_id = self.client.id;

        match self.kind {
            CommandKind::Info => common::info(
                client_id,
                args.get(1).map(Vec::as_slice),
                &self.client.server_info.borrow(),
            ),
            CommandKind::Exec => Ok(common::execute_transaction(db, client_id)),
            CommandKind::Multi => Err(CommandError::ExecTransaction),
            CommandKind::Discard => Ok(Reply::DiscardTransaction(None)),
            CommandKind::Watch | CommandKind::Unwatch => Err(CommandError::WatchTransaction),
            CommandKind::Replconf | CommandKind::Psync => Err(CommandError::SlaveUnsupported),
            _ => Ok(common::get_initial_request(args)),
        }
    }

    fn handle_normal_mode(&mut self, db: &mut Db) -> Result<Reply, CommandError> {
        let args = mem::take(&mut self.args);
        let client_id = self.client.id;
        match self.kind {
            CommandKind::Info => common::info(
                client_id,
                args.get(1).map(Vec::as_slice),
                &self.client.server_info.borrow(),
            ),
            CommandKind::Ping => Ok(cmd_ping()),
            CommandKind::Echo => Ok(cmd_echo(&args[1])),
            CommandKind::Get => string::get(db, &args[1]),
            CommandKind::Set => string::set(
                db,
                &args[1],
                &args[2],
                args.get(3).map(Vec::as_slice),
                args.get(4).map(Vec::as_slice),
            ),
            CommandKind::Lpush => list::push(db, &Side::Front, &args[1], &args[2..args.len()]),
            CommandKind::Rpush => list::push(db, &Side::Back, &args[1], &args[2..args.len()]),
            CommandKind::Llen => list::llen(db, &args[1]),
            CommandKind::Lpop => list::lpop(db, &args[1], args.get(2).map(Vec::as_slice)),
            CommandKind::Lrange => list::lrange(db, &args[1], &args[2], &args[3]),
            CommandKind::Blpop => {
                list::blpop(db, &args[1], args.get(2).map(Vec::as_slice), client_id)
            }
            CommandKind::Type => Ok(string::cmd_type(db, &args[1])),
            CommandKind::Xadd => stream::xadd(db, &args[1], &args[2], &args[3..args.len()]),
            CommandKind::Xrange => stream::xrange(db, &args[1], &args[2], &args[3]),
            CommandKind::Xread => stream::xread(db, client_id, &args[1..args.len()]),
            CommandKind::Incr => string::incr(db, &args[1]),
            CommandKind::Multi => Ok(Reply::StartTransaction),
            CommandKind::Exec => Err(CommandError::ExecTransaction),
            CommandKind::Discard => Err(CommandError::DiscardTransaction),
            CommandKind::Watch => Ok(common::watch_keys(db, client_id, &args[1..args.len()])),
            CommandKind::Unwatch => Ok(common::unwatch(db, client_id)),
            CommandKind::Replconf => Ok(repl_conf()),
            CommandKind::Psync => psync(&self.client.server_info.borrow()),
        }
    }
}

#[derive(EnumString, Debug, Display, Clone, Copy)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
enum InfoSection {
    Replication,
}

impl InfoSection {
    fn from_bytes(value: &[u8]) -> Result<Self, CommandError> {
        str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<Self>().ok())
            .ok_or_else(|| CommandError::Info(String::from_utf8_lossy(value).into_owned()))
    }
}
#[derive(AsRefStr, EnumString, Debug, Display, Clone, Copy)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
enum CommandKind {
    Info,
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
    Discard,
    Watch,
    Unwatch,
    Replconf,
    Psync,
}

impl CommandKind {
    fn new(argc: usize, value: &[u8]) -> Result<Self, CommandError> {
        let kind = Self::from_bytes(value)?;
        kind.check_arity(argc)?;

        Span::current().record("cmd", field::display(&kind));
        info!(command = ?kind, "handling cmd");
        Ok(kind)
    }

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
            Self::Multi | Self::Exec | Self::Discard => 1,
            Self::Watch => -2,
            Self::Unwatch => 1,
            Self::Info => -1,
            Self::Replconf => -3,
            Self::Psync => 3,
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

const EMPTY_RDB: &[u8] = include_bytes!("../../empty.rdb");

fn psync(server_info: &ServerInfo) -> HandleCmdResult {
    let repl_id = server_info.master_replid.clone();
    let out = format!("FULLRESYNC {repl_id} 0");
    let path = server_info.rdb_path();
    warn!(?path, "psync");
    let buffer = match File::open(&path) {
        Ok(file) => {
            let mut buf_reader = BufReader::new(file);
            let mut buffer: Vec<u8> = vec![];
            buf_reader.read_to_end(&mut buffer).map_err(|err| {
                error!(?err, "psync could read file");
                CommandError::NoRdbFile
            })?;
            buffer
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            warn!(?path, "no rdb on disk, sending empty rdb");
            EMPTY_RDB.to_vec()
        }
        Err(err) => {
            error!(?err, "psync couldn't open rdb");
            return Err(CommandError::NoRdbFile);
        }
    };

    let rdb = RespBody::RDB(buffer);
    Ok(Reply::Rdb(RespBody::Simple(out), rdb))
}

fn repl_conf() -> Reply {
    RespBody::new_ok().into()
}
fn cmd_ping() -> Reply {
    RespBody::Simple("PONG".to_owned()).into()
}

fn cmd_echo(arg: &[u8]) -> Reply {
    RespBody::Bulk(Some(arg.into())).into()
}
