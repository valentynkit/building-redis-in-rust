use std::time::Duration;

use crate::{
    command::{CommandError, ExpCmd},
    db::{Db, Key},
    resp::{Reply, Resp},
};

pub fn get(db: &mut Db, key: &Vec<u8>) -> Reply {
    let key: Key = key.into();
    let opt_value = db.get(&key).map(Into::into); // None → key absent → caller writes $-1
    Reply::Now(Resp::Bulk(opt_value))
}

pub fn set(
    db: &mut Db,
    key: &Vec<u8>,
    value: &Vec<u8>,
    exp_cmd: Option<&Vec<u8>>,
    exp: Option<&Vec<u8>>,
) -> Result<Reply, CommandError> {
    let expiry = parse_ttl(exp_cmd, exp)?.map(|ttl| db.realtime_ms() + ttl);

    let out = db
        .setex(key.into(), value.into(), expiry)
        .map(|value| value.into());
    Ok(Reply::Now(Resp::Bulk(out)))
}

fn parse_ttl(
    exp_cmd: Option<&Vec<u8>>,
    exp: Option<&Vec<u8>>,
) -> Result<Option<Duration>, CommandError> {
    let (Some(cmd), Some(exp)) = (exp_cmd, exp) else {
        return Ok(None);
    };

    let cmd = ExpCmd::from_bytes(cmd)
        .ok_or_else(|| CommandError::Unknown(String::from_utf8_lossy(cmd).into_owned()))?;

    let number_err = CommandError::WrongNumber(String::from_utf8_lossy(exp).into());

    let n: u64 = str::from_utf8(exp)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| number_err)?;

    // exp_cmd could be EX or PX. EX = seconds, PX = milisseconds.
    let ms = match cmd {
        ExpCmd::Ex => n * 1000,
        ExpCmd::Px => n,
    };
    Ok(Some(Duration::from_millis(ms)))
}
